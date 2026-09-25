import hashlib,sqlite3,tempfile,threading,unittest
from pathlib import Path
from urban_network.errors import Conflict
from urban_network.models import Reading,Segment
from urban_network.risk import score_reading
from urban_network.service import NetworkService
class UrbanNetworkTests(unittest.TestCase):
    def setUp(self):
        self.s=NetworkService(); self.s.bootstrap(); self.t=self.s.auth.login("admin","network-admin"); self.s.register_segment(self.t,Segment("S1","east","drainage",100,4))
    def reading(self,reading_id="R1",pressure=120,flow=250,db=90,observed="2026-01-01T00:00:00+00:00",sensor="sensor"):
        return Reading(reading_id,"S1",sensor,pressure,flow,db,observed)
    def test_risk_and_idempotent_reading(self):
        r=Reading("R1","S1","sensor",120,250,90,"2026-01-01T00:00:00+00:00"); a=self.s.ingest_reading(self.t,r); b=self.s.ingest_reading(self.t,r); self.assertFalse(a["duplicate"]); self.assertTrue(b["duplicate"]); self.assertEqual(self.s.risk_report(self.t,"S1")["readings"],1)
    def test_work_order_and_allocation(self):
        r=self.s.ingest_reading(self.t,Reading("R2","S1","sensor",100,250,90,"2026-01-01T00:00:00+00:00")); o=self.s.create_work_order(self.t,"S1",r["alert_id"],"crew"); self.s.transition_work_order(self.t,o["work_order_id"],"assigned","crew accepted"); self.s.add_resource(self.t,"R1","pump","east",2); self.assertFalse(self.s.allocate(self.t,"R1",o["work_order_id"],1)["duplicate"]); self.assertEqual(self.s.resource(self.t,"R1")["available"],1)
    def test_risk_validation(self):
        with self.assertRaises(ValueError):score_reading(-1,1,1,2)
    def test_identical_replay_returns_stored_record_without_side_effects(self):
        first=self.s.ingest_reading(self.t,self.reading()); again=self.s.ingest_reading(self.t,self.reading(observed="2026-01-01T08:00:00+08:00"))
        self.assertTrue(again["duplicate"]); self.assertEqual(again["fingerprint"],first["fingerprint"]); self.assertEqual(again["risk"],first["risk"]); self.assertEqual(again["alert_id"],first["alert_id"])
        report=self.s.risk_report(self.t,"S1"); self.assertEqual(report["readings"],1); self.assertEqual(len(report["alerts"]),1)
        self.assertEqual(report["alerts"][0]["reading_id"],"R1"); self.assertEqual(report["alerts"][0]["fingerprint"],first["fingerprint"])
        self.assertEqual(len(self.s.audit_events(self.t,"reading","R1")),1)
    def test_same_id_with_changed_payload_conflicts(self):
        self.s.ingest_reading(self.t,self.reading())
        for changed in (self.reading(pressure=130),self.reading(flow=10),self.reading(observed="2026-01-01T00:00:01+00:00")):
            with self.assertRaises(Conflict):self.s.ingest_reading(self.t,changed)
        report=self.s.risk_report(self.t,"S1"); self.assertEqual(report["readings"],1); self.assertEqual(len(report["alerts"]),1)
        self.assertEqual(len(self.s.audit_events(self.t,"reading","R1")),1)
    def test_sensor_instant_occupied_by_other_id_conflicts(self):
        self.s.ingest_reading(self.t,self.reading("R1"))
        with self.assertRaises(Conflict) as ctx:self.s.ingest_reading(self.t,self.reading("R2"))
        self.assertIn("R1",str(ctx.exception))
        with self.assertRaises(Conflict):self.s.ingest_reading(self.t,self.reading("R3",observed="2026-01-01T08:00:00+08:00"))
        report=self.s.risk_report(self.t,"S1"); self.assertEqual(report["readings"],1); self.assertEqual(len(report["alerts"]),1)
        self.assertEqual(self.s.audit_events(self.t,"reading","R2"),[]); self.assertEqual(self.s.audit_events(self.t,"reading","R3"),[])
    def test_concurrent_first_writes_agree_on_single_winner(self):
        results,conflicts=[],[]
        def worker(i):
            try:results.append(self.s.ingest_reading(self.t,self.reading(pressure=120+i)))
            except Conflict:conflicts.append(i)
        threads=[threading.Thread(target=worker,args=(i,)) for i in range(8)]
        for th in threads:th.start()
        for th in threads:th.join()
        self.assertEqual(len(results),1); self.assertFalse(results[0]["duplicate"]); self.assertEqual(len(conflicts),7)
        self.assertEqual(self.s.risk_report(self.t,"S1")["readings"],1); self.assertEqual(len(self.s.audit_events(self.t,"reading","R1")),1)
    def test_concurrent_identical_replays_all_return_stored_record(self):
        first=self.s.ingest_reading(self.t,self.reading()); results=[]
        def worker():results.append(self.s.ingest_reading(self.t,self.reading()))
        threads=[threading.Thread(target=worker) for _ in range(8)]
        for th in threads:th.start()
        for th in threads:th.join()
        self.assertEqual(len(results),8)
        for r in results:self.assertTrue(r["duplicate"]); self.assertEqual(r["fingerprint"],first["fingerprint"]); self.assertEqual(r["alert_id"],first["alert_id"])
        self.assertEqual(self.s.risk_report(self.t,"S1")["readings"],1); self.assertEqual(len(self.s.audit_events(self.t,"reading","R1")),1)
    def test_restart_keeps_replay_and_conflict_judgments(self):
        with tempfile.TemporaryDirectory() as d:
            path=str(Path(d)/"network.sqlite3"); s1=NetworkService(path); s1.bootstrap(); t1=s1.auth.login("admin","network-admin")
            s1.register_segment(t1,Segment("S1","east","drainage",100,4)); first=s1.ingest_reading(t1,self.reading()); s1.db.close()
            s2=NetworkService(path); t2=s2.auth.login("admin","network-admin")
            replay=s2.ingest_reading(t2,self.reading())
            self.assertTrue(replay["duplicate"]); self.assertEqual(replay["fingerprint"],first["fingerprint"]); self.assertEqual(replay["alert_id"],first["alert_id"])
            with self.assertRaises(Conflict):s2.ingest_reading(t2,self.reading(pressure=130))
            with self.assertRaises(Conflict):s2.ingest_reading(t2,self.reading("R9"))
            report=s2.risk_report(t2,"S1"); self.assertEqual(report["readings"],1); self.assertEqual(report["alerts"][0]["reading_id"],"R1")
            s2.db.close()
    def test_legacy_database_is_migrated_with_fingerprints(self):
        with tempfile.TemporaryDirectory() as d:
            path=str(Path(d)/"legacy.sqlite3"); db=sqlite3.connect(path)
            db.executescript("CREATE TABLE segments(segment_id TEXT PRIMARY KEY,district TEXT NOT NULL,network_type TEXT NOT NULL,length_m REAL NOT NULL,criticality INTEGER NOT NULL,status TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);"
                             "CREATE TABLE readings(reading_id TEXT PRIMARY KEY,segment_id TEXT NOT NULL REFERENCES segments(segment_id),sensor_id TEXT NOT NULL,pressure_kpa REAL NOT NULL,flow_lps REAL NOT NULL,acoustic_db REAL NOT NULL,observed_at TEXT NOT NULL,UNIQUE(segment_id,sensor_id,observed_at));"
                             "CREATE TABLE alerts(alert_id TEXT PRIMARY KEY,segment_id TEXT NOT NULL REFERENCES segments(segment_id),fingerprint TEXT NOT NULL UNIQUE,severity TEXT NOT NULL,score REAL NOT NULL,status TEXT NOT NULL,created_at TEXT NOT NULL,resolved_at TEXT);")
            db.execute("INSERT INTO segments VALUES('S1','east','drainage',100,4,'normal','2026-01-01','2026-01-01')")
            db.execute("INSERT INTO readings VALUES('R1','S1','sensor',120,250,90,'2026-01-01T00:00:00Z')")
            legacy_fp=hashlib.sha256("S1|sensor|2026-01-01T00:00:00Z".encode()).hexdigest()
            db.execute("INSERT INTO alerts VALUES(?,?,?,?,?,?,?,?)",("alert-"+legacy_fp[:18],"S1",legacy_fp,"critical",97.0,"open","2026-01-01",None)); db.commit(); db.close()
            s=NetworkService(path); s.bootstrap(); t=s.auth.login("admin","network-admin")
            replay=s.ingest_reading(t,self.reading(observed="2026-01-01T00:00:00+00:00"))
            self.assertTrue(replay["duplicate"])
            with self.assertRaises(Conflict):s.ingest_reading(t,self.reading(pressure=130,observed="2026-01-01T00:00:00+00:00"))
            with self.assertRaises(Conflict):s.ingest_reading(t,self.reading("R9",observed="2026-01-01T08:00:00+08:00"))
            report=s.risk_report(t,"S1"); self.assertEqual(report["readings"],1); self.assertEqual(report["alerts"][0]["reading_id"],"R1")
            s.db.close()
