"""SQLite 结构、事务和审计事件辅助函数。"""
from __future__ import annotations
import hashlib, json, sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator

from .models import canonical_instant, reading_fingerprint

SCHEMA = """
CREATE TABLE IF NOT EXISTS users(user_id TEXT PRIMARY KEY,role TEXT NOT NULL,salt TEXT NOT NULL,password_hash TEXT NOT NULL,active INTEGER NOT NULL DEFAULT 1,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sessions(token TEXT PRIMARY KEY,user_id TEXT NOT NULL,expires_at TEXT NOT NULL,active INTEGER NOT NULL DEFAULT 1);
CREATE TABLE IF NOT EXISTS segments(segment_id TEXT PRIMARY KEY,district TEXT NOT NULL,network_type TEXT NOT NULL,length_m REAL NOT NULL,criticality INTEGER NOT NULL,status TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS readings(reading_id TEXT PRIMARY KEY,segment_id TEXT NOT NULL REFERENCES segments(segment_id),sensor_id TEXT NOT NULL,pressure_kpa REAL NOT NULL,flow_lps REAL NOT NULL,acoustic_db REAL NOT NULL,observed_at TEXT NOT NULL,fingerprint TEXT NOT NULL,UNIQUE(segment_id,sensor_id,observed_at));
CREATE TABLE IF NOT EXISTS alerts(alert_id TEXT PRIMARY KEY,segment_id TEXT NOT NULL REFERENCES segments(segment_id),reading_id TEXT NOT NULL,fingerprint TEXT NOT NULL UNIQUE,severity TEXT NOT NULL,score REAL NOT NULL,status TEXT NOT NULL,created_at TEXT NOT NULL,resolved_at TEXT);
CREATE TABLE IF NOT EXISTS work_orders(work_order_id TEXT PRIMARY KEY,segment_id TEXT NOT NULL,alert_id TEXT NOT NULL,assignee TEXT NOT NULL,status TEXT NOT NULL,priority INTEGER NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS resources(resource_id TEXT PRIMARY KEY,kind TEXT NOT NULL,district TEXT NOT NULL,capacity INTEGER NOT NULL,available INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS allocations(allocation_id TEXT PRIMARY KEY,resource_id TEXT NOT NULL,work_order_id TEXT NOT NULL,quantity INTEGER NOT NULL,created_at TEXT NOT NULL,UNIQUE(resource_id,work_order_id));
CREATE TABLE IF NOT EXISTS audit_events(event_id INTEGER PRIMARY KEY AUTOINCREMENT,entity_type TEXT NOT NULL,entity_id TEXT NOT NULL,action TEXT NOT NULL,actor TEXT NOT NULL,payload TEXT NOT NULL,created_at TEXT NOT NULL);
"""
INDEXES = """
CREATE UNIQUE INDEX IF NOT EXISTS readings_fingerprint_idx ON readings(fingerprint);
"""
def utcnow() -> str: return datetime.now(timezone.utc).isoformat()
def connect(path: str = ":memory:") -> sqlite3.Connection:
    db=sqlite3.connect(path,timeout=10,check_same_thread=False); db.row_factory=sqlite3.Row; db.execute("PRAGMA foreign_keys=ON"); db.execute("PRAGMA journal_mode=WAL"); db.executescript(SCHEMA); _migrate(db); db.executescript(INDEXES); db.commit(); return db
def _migrate(db: sqlite3.Connection) -> None:
    """为旧库补齐读数指纹和告警来源列，并把采集时刻归一化为 UTC 文本。"""
    reading_cols={r[1] for r in db.execute("PRAGMA table_info(readings)")}
    alert_cols={r[1] for r in db.execute("PRAGMA table_info(alerts)")}
    if "fingerprint" in reading_cols and "reading_id" in alert_cols: return
    if "fingerprint" not in reading_cols: db.execute("ALTER TABLE readings ADD COLUMN fingerprint TEXT NOT NULL DEFAULT ''")
    if "reading_id" not in alert_cols: db.execute("ALTER TABLE alerts ADD COLUMN reading_id TEXT NOT NULL DEFAULT ''")
    with transaction(db):
        legacy={}
        for r in db.execute("SELECT * FROM readings").fetchall():
            legacy[hashlib.sha256(f"{r['segment_id']}|{r['sensor_id']}|{r['observed_at']}".encode()).hexdigest()]=r["reading_id"]
        for a in db.execute("SELECT alert_id,fingerprint FROM alerts WHERE reading_id=''").fetchall():
            if a["fingerprint"] in legacy: db.execute("UPDATE alerts SET reading_id=? WHERE alert_id=?",(legacy[a["fingerprint"]],a["alert_id"]))
        for r in db.execute("SELECT * FROM readings").fetchall():
            instant=canonical_instant(r["observed_at"]); fp=reading_fingerprint(r["segment_id"],r["sensor_id"],instant,r["pressure_kpa"],r["flow_lps"],r["acoustic_db"])
            if r["observed_at"]!=instant or r["fingerprint"]!=fp: db.execute("UPDATE readings SET observed_at=?,fingerprint=? WHERE reading_id=?",(instant,fp,r["reading_id"]))
@contextmanager
def transaction(db: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    try: db.execute("BEGIN IMMEDIATE"); yield db; db.commit()
    except Exception: db.rollback(); raise
def audit(db, entity_type, entity_id, action, actor, payload):
    db.execute("INSERT INTO audit_events(entity_type,entity_id,action,actor,payload,created_at) VALUES(?,?,?,?,?,?)",(entity_type,entity_id,action,actor,json.dumps(payload,ensure_ascii=False,sort_keys=True),utcnow()))
def rows(db, query, args=()): return [dict(r) for r in db.execute(query,args).fetchall()]
