"""协调管网监测、告警、工单和应急资源分配的应用服务。"""
from __future__ import annotations
import sqlite3,uuid
from .auth import Auth
from .errors import Conflict
from .models import Reading,Segment,as_dict,utcnow
from .risk import leak_probability,score_reading
from .storage import audit,connect,rows,transaction
class NetworkService:
    def __init__(self,database=":memory:"): self.db=connect(database); self.auth=Auth(self.db)
    def bootstrap(self):
        for uid,pwd,role in (("admin","network-admin","admin"),("operator","network-operator","operator")):
            try:self.auth.create_user(uid,pwd,role)
            except Exception:pass
    def register_segment(self,token,segment):
        actor=self.auth.require(token,"admin"); segment.validate(); now=utcnow()
        with transaction(self.db):
            self.db.execute("INSERT INTO segments VALUES(?,?,?,?,?,?,?,?)",(segment.segment_id,segment.district,segment.network_type,segment.length_m,segment.criticality,segment.status,now,now)); audit(self.db,"segment",segment.segment_id,"created",actor.user_id,as_dict(segment))
        return self.segment(token,segment.segment_id)
    def segment(self,token,segment_id):
        self.auth.require(token,"read"); row=self.db.execute("SELECT * FROM segments WHERE segment_id=?",(segment_id,)).fetchone()
        if not row:raise KeyError(segment_id)
        return dict(row)
    def ingest_reading(self,token,reading):
        actor=self.auth.require(token,"measure"); reading.validate(); seg=self.db.execute("SELECT criticality FROM segments WHERE segment_id=?",(reading.segment_id,)).fetchone()
        if not seg:raise KeyError(reading.segment_id)
        fingerprint=reading.business_fingerprint(); observed_at=reading.normalized_observed_at()
        try:
            with transaction(self.db):
                # BEGIN IMMEDIATE 串行化首写：并发重试只会有一个写入者，后到者在锁释放后看到已提交记录。
                existing=self.db.execute("SELECT * FROM readings WHERE reading_id=?",(reading.reading_id,)).fetchone()
                if existing:
                    if existing["payload_fingerprint"]==fingerprint:
                        return self._reading_result(existing,seg[0],duplicate=True)
                    raise Conflict(f"reading_id {reading.reading_id} 已对应不同读数载荷（压力、流量、声学值或采集时刻不一致），编号可能被网关复用")
                owner=self.db.execute("SELECT reading_id FROM readings WHERE segment_id=? AND sensor_id=? AND observed_at=?",(reading.segment_id,reading.sensor_id,observed_at)).fetchone()
                if owner:
                    raise Conflict(f"管段 {reading.segment_id} 传感器 {reading.sensor_id} 在 {observed_at} 的读数已由编号 {owner[0]} 占用，当前编号 {reading.reading_id} 与之冲突")
                self.db.execute("INSERT INTO readings VALUES(?,?,?,?,?,?,?,?)",(reading.reading_id,reading.segment_id,reading.sensor_id,reading.pressure_kpa,reading.flow_lps,reading.acoustic_db,observed_at,fingerprint))
                risk=score_reading(reading.pressure_kpa,reading.flow_lps,reading.acoustic_db,seg[0]); alert_id=None
                if risk.severity in {"high","critical"}:
                    alert_id="alert-"+fingerprint[:18]; self.db.execute("INSERT OR IGNORE INTO alerts VALUES(?,?,?,?,?,?,?,?)",(alert_id,reading.segment_id,fingerprint,risk.severity,risk.score,"open",utcnow(),None))
                audit(self.db,"reading",reading.reading_id,"ingested",actor.user_id,{"fingerprint":fingerprint,"risk":as_dict(risk),"alert_id":alert_id})
        except sqlite3.IntegrityError as exc:
            # 唯一约束兜底（例如极端并发下的业务键竞争）：统一报冲突，绝不写入半成品。
            raise Conflict(f"读数 {reading.reading_id} 与既有记录冲突") from exc
        return {"reading_id":reading.reading_id,"duplicate":False,"risk":as_dict(risk),"alert_id":alert_id,"fingerprint":fingerprint}
    def _reading_result(self,row,criticality,duplicate):
        """按已存储记录重建响应：重放返回的始终是第一次写入的原始结果。"""
        risk=score_reading(row["pressure_kpa"],row["flow_lps"],row["acoustic_db"],criticality)
        alert=self.db.execute("SELECT alert_id FROM alerts WHERE fingerprint=?",(row["payload_fingerprint"],)).fetchone()
        return {"reading_id":row["reading_id"],"duplicate":duplicate,"risk":as_dict(risk),"alert_id":alert[0] if alert else None,"fingerprint":row["payload_fingerprint"]}
    def risk_report(self,token,segment_id):
        self.auth.require(token,"analyze"); readings=rows(self.db,"SELECT * FROM readings WHERE segment_id=? ORDER BY observed_at",(segment_id,))
        alerts=rows(self.db,"SELECT a.*,r.reading_id AS source_reading_id FROM alerts a LEFT JOIN readings r ON r.segment_id=a.segment_id AND r.payload_fingerprint=a.fingerprint WHERE a.segment_id=? ORDER BY a.created_at",(segment_id,))
        return {"segment_id":segment_id,"readings":len(readings),"alerts":alerts,"leak_probability":leak_probability(alerts)}
    def create_work_order(self,token,segment_id,alert_id,assignee,priority=3):
        actor=self.auth.require(token,"work_order")
        if not assignee.strip() or not 1<=priority<=5:raise ValueError("assignee and priority are invalid")
        if not self.db.execute("SELECT 1 FROM alerts WHERE alert_id=? AND segment_id=?",(alert_id,segment_id)).fetchone():raise KeyError(alert_id)
        wid="wo-"+uuid.uuid4().hex[:16]
        with transaction(self.db): self.db.execute("INSERT INTO work_orders VALUES(?,?,?,?,?,?,?,?)",(wid,segment_id,alert_id,assignee,"open",priority,utcnow(),utcnow())); audit(self.db,"work_order",wid,"created",actor.user_id,{"segment_id":segment_id,"alert_id":alert_id})
        return self.work_order(token,wid)
    def work_order(self,token,work_order_id):
        self.auth.require(token,"read"); row=self.db.execute("SELECT * FROM work_orders WHERE work_order_id=?",(work_order_id,)).fetchone()
        if not row:raise KeyError(work_order_id)
        return dict(row)
    def transition_work_order(self,token,work_order_id,target,reason):
        actor=self.auth.require(token,"work_order"); allowed={"open":{"assigned","cancelled"},"assigned":{"in_progress","cancelled"},"in_progress":{"completed","blocked"},"blocked":{"in_progress","cancelled"},"completed":set(),"cancelled":set()}
        if not reason.strip():raise ValueError("transition reason is required")
        with transaction(self.db):
            row=self.db.execute("SELECT status FROM work_orders WHERE work_order_id=?",(work_order_id,)).fetchone()
            if not row:raise KeyError(work_order_id)
            if target not in allowed.get(row[0],set()):raise ValueError("invalid work order transition")
            self.db.execute("UPDATE work_orders SET status=?,updated_at=? WHERE work_order_id=?",(target,utcnow(),work_order_id)); audit(self.db,"work_order",work_order_id,"transition",actor.user_id,{"from":row[0],"to":target,"reason":reason})
        return self.work_order(token,work_order_id)
    def add_resource(self,token,resource_id,kind,district,capacity):
        actor=self.auth.require(token,"admin")
        if capacity<=0 or not kind.strip() or not district.strip():raise ValueError("resource fields are invalid")
        with transaction(self.db):self.db.execute("INSERT INTO resources VALUES(?,?,?,?,?)",(resource_id,kind,district,capacity,capacity)); audit(self.db,"resource",resource_id,"created",actor.user_id,{"kind":kind,"district":district,"capacity":capacity})
        return self.resource(token,resource_id)
    def resource(self,token,resource_id):
        self.auth.require(token,"read"); row=self.db.execute("SELECT * FROM resources WHERE resource_id=?",(resource_id,)).fetchone()
        if not row:raise KeyError(resource_id)
        return dict(row)
    def allocate(self,token,resource_id,work_order_id,quantity):
        actor=self.auth.require(token,"allocate")
        if quantity<=0:raise ValueError("quantity must be positive")
        aid="alloc-"+uuid.uuid4().hex[:16]
        with transaction(self.db):
            resource=self.db.execute("SELECT available FROM resources WHERE resource_id=?",(resource_id,)).fetchone()
            if not resource:raise KeyError(resource_id)
            if not self.db.execute("SELECT 1 FROM work_orders WHERE work_order_id=?",(work_order_id,)).fetchone():raise KeyError(work_order_id)
            if resource[0]<quantity:raise ValueError("resource capacity exceeded")
            old=self.db.execute("SELECT allocation_id FROM allocations WHERE resource_id=? AND work_order_id=?",(resource_id,work_order_id)).fetchone()
            if old:return {"allocation_id":old[0],"duplicate":True}
            self.db.execute("INSERT INTO allocations VALUES(?,?,?,?,?)",(aid,resource_id,work_order_id,quantity,utcnow())); self.db.execute("UPDATE resources SET available=available-? WHERE resource_id=?",(quantity,resource_id)); audit(self.db,"resource",resource_id,"allocated",actor.user_id,{"work_order_id":work_order_id,"quantity":quantity})
        return {"allocation_id":aid,"duplicate":False,"resource_id":resource_id,"quantity":quantity}
    def audit_events(self,token,entity_type,entity_id): self.auth.require(token,"read"); return rows(self.db,"SELECT * FROM audit_events WHERE entity_type=? AND entity_id=? ORDER BY event_id",(entity_type,entity_id))
