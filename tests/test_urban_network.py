import os, tempfile, threading, time, unittest
from urban_network.errors import Conflict
from urban_network.models import Reading,Segment
from urban_network.risk import score_reading
from urban_network.service import NetworkService


class UrbanNetworkTests(unittest.TestCase):
    def setUp(self):
        self.s=NetworkService(); self.s.bootstrap(); self.t=self.s.auth.login("admin","network-admin"); self.s.register_segment(self.t,Segment("S1","east","drainage",100,4))
    def test_risk_and_idempotent_reading(self):
        r=Reading("R1","S1","sensor",120,250,90,"2026-01-01T00:00:00+00:00"); a=self.s.ingest_reading(self.t,r); b=self.s.ingest_reading(self.t,r); self.assertFalse(a["duplicate"]); self.assertTrue(b["duplicate"]); self.assertEqual(self.s.risk_report(self.t,"S1")["readings"],1)
    def test_work_order_and_allocation(self):
        r=self.s.ingest_reading(self.t,Reading("R2","S1","sensor",100,250,90,"2026-01-01T00:00:00+00:00")); o=self.s.create_work_order(self.t,"S1",r["alert_id"],"crew"); self.s.transition_work_order(self.t,o["work_order_id"],"assigned","crew accepted"); self.s.add_resource(self.t,"R1","pump","east",2); self.assertFalse(self.s.allocate(self.t,"R1",o["work_order_id"],1)["duplicate"]); self.assertEqual(self.s.resource(self.t,"R1")["available"],1)
    def test_risk_validation(self):
        with self.assertRaises(ValueError):score_reading(-1,1,1,2)


class ReadingFingerprintTests(unittest.TestCase):
    def setUp(self):
        self.s=NetworkService(); self.s.bootstrap(); self.t=self.s.auth.login("admin","network-admin")
        self.s.register_segment(self.t,Segment("S1","east","water",100,5))
        self.reading=Reading("R1","S1","sensor-1",160,230,88,"2026-09-24T10:00:00+00:00")
    def _counts(self):
        return (self.s.db.execute("SELECT count(*) FROM readings").fetchone()[0],
                self.s.db.execute("SELECT count(*) FROM audit_events WHERE entity_type='reading'").fetchone()[0],
                self.s.db.execute("SELECT count(*) FROM alerts").fetchone()[0])
    def test_identical_replay_returns_original_record(self):
        first=self.s.ingest_reading(self.t,self.reading)
        replay=self.s.ingest_reading(self.t,self.reading)
        self.assertFalse(first["duplicate"]); self.assertTrue(replay["duplicate"])
        self.assertEqual(first["reading_id"],replay["reading_id"])
        self.assertEqual(first["risk"],replay["risk"])
        self.assertEqual(first["alert_id"],replay["alert_id"])
        self.assertEqual(first["fingerprint"],replay["fingerprint"])
        self.assertEqual(self._counts(),(1,1,1))
    def test_timestamp_spelling_is_same_business_reading(self):
        first=self.s.ingest_reading(self.t,self.reading)
        replay=self.s.ingest_reading(self.t,Reading("R1","S1","sensor-1",160,230,88,"2026-09-24T10:00:00Z"))
        self.assertTrue(replay["duplicate"]); self.assertEqual(first["fingerprint"],replay["fingerprint"])
        stored=self.s.db.execute("SELECT observed_at FROM readings WHERE reading_id='R1'").fetchone()[0]
        self.assertEqual(stored,"2026-09-24T10:00:00+00:00")
    def test_changed_payload_same_id_conflicts_without_side_effects(self):
        self.s.ingest_reading(self.t,self.reading)
        before=self._counts()
        for changed in (Reading("R1","S1","sensor-1",161,230,88,"2026-09-24T10:00:00+00:00"),
                        Reading("R1","S1","sensor-1",160,231,88,"2026-09-24T10:00:00+00:00"),
                        Reading("R1","S1","sensor-1",160,230,89,"2026-09-24T10:00:00+00:00"),
                        Reading("R1","S1","sensor-1",160,230,88,"2026-09-24T10:00:01+00:00")):
            with self.assertRaises(Conflict):self.s.ingest_reading(self.t,changed)
        self.assertEqual(self._counts(),before)
    def test_same_sensor_moment_claimed_by_another_id_conflicts(self):
        self.s.ingest_reading(self.t,self.reading)
        before=self._counts()
        with self.assertRaises(Conflict):
            self.s.ingest_reading(self.t,Reading("R2","S1","sensor-1",160,230,88,"2026-09-24T10:00:00+00:00"))
        self.assertEqual(self._counts(),before)
    def test_distinct_moment_under_same_id_is_not_a_replay(self):
        self.s.ingest_reading(self.t,self.reading)
        with self.assertRaises(Conflict):
            self.s.ingest_reading(self.t,Reading("R1","S1","sensor-1",160,230,88,"2026-09-24T11:00:00+00:00"))
    def test_risk_report_counts_and_alert_sources_are_explainable(self):
        self.s.ingest_reading(self.t,self.reading)
        self.s.ingest_reading(self.t,self.reading)  # replay must not double count
        self.s.ingest_reading(self.t,Reading("R3","S1","sensor-1",160,230,88,"2026-09-24T12:00:00+00:00"))
        report=self.s.risk_report(self.t,"S1")
        self.assertEqual(report["readings"],2)
        self.assertEqual(len(report["alerts"]),2)
        self.assertEqual({a["source_reading_id"] for a in report["alerts"]},{"R1","R3"})


class ReadingConcurrencyAndRestartTests(unittest.TestCase):
    def setUp(self):
        self.dir=tempfile.TemporaryDirectory(); self.path=os.path.join(self.dir.name,"network.sqlite3")
        seed=NetworkService(self.path); seed.bootstrap()
        token=seed.auth.login("admin","network-admin")
        seed.register_segment(token,Segment("C1","north","gas",50,3)); seed.db.close()
    def tearDown(self):self.dir.cleanup()
    def _call(self,reading_id,pressure,results,barrier):
        service=NetworkService(self.path); token=service.auth.login("admin","network-admin")
        barrier.wait(); time.sleep(0.01)
        try:
            results.append("duplicate" if service.ingest_reading(token,Reading(reading_id,"C1","x",pressure,10,50,"2026-09-24T11:00:00Z"))["duplicate"] else "first")
        except Conflict:
            results.append("conflict")
        finally:
            service.db.close()
    def test_concurrent_same_id_replay_has_single_first_write(self):
        results=[]; barrier=threading.Barrier(2)
        threads=[threading.Thread(target=self._call,args=("K1",300,results,barrier)),
                 threading.Thread(target=self._call,args=("K1",300,results,barrier))]
        for thread in threads:thread.start()
        for thread in threads:thread.join()
        self.assertEqual(sorted(results),["duplicate","first"])
        reopened=NetworkService(self.path)
        self.assertEqual(reopened.db.execute("SELECT count(*) FROM readings").fetchone()[0],1)
        self.assertEqual(reopened.db.execute("SELECT count(*) FROM audit_events WHERE entity_type='reading'").fetchone()[0],1)
        token=reopened.auth.login("admin","network-admin")
        self.assertTrue(reopened.ingest_reading(token,Reading("K1","C1","x",300,10,50,"2026-09-24T11:00:00+00:00"))["duplicate"])
        reopened.db.close()
    def test_rival_id_for_same_moment_conflicts_even_under_concurrency(self):
        results=[]; barrier=threading.Barrier(2)
        threads=[threading.Thread(target=self._call,args=("K1",300,results,barrier)),
                 threading.Thread(target=self._call,args=("K2",300,results,barrier))]
        for thread in threads:thread.start()
        for thread in threads:thread.join()
        # 无论哪个编号先拿到写锁，都只有一条读数：胜者 first，占用同一传感器时刻的败者 conflict
        self.assertEqual(sorted(results),["conflict","first"])
        reopened=NetworkService(self.path)
        self.assertEqual(reopened.db.execute("SELECT count(*) FROM readings").fetchone()[0],1)
        self.assertEqual(reopened.db.execute("SELECT count(*) FROM audit_events WHERE entity_type='reading'").fetchone()[0],1)
        winner=reopened.db.execute("SELECT reading_id FROM readings").fetchone()[0]
        token=reopened.auth.login("admin","network-admin")
        with self.assertRaises(Conflict):
            reopened.ingest_reading(token,Reading("K1" if winner=="K2" else "K2","C1","x",300,10,50,"2026-09-24T11:00:00Z"))
        with self.assertRaises(Conflict):
            reopened.ingest_reading(token,Reading(winner,"C1","x",301,10,50,"2026-09-24T11:00:00Z"))
        reopened.db.close()
    def test_legacy_database_is_backfilled_with_fingerprints(self):
        service=NetworkService(self.path); token=service.auth.login("admin","network-admin")
        service.db.execute("ALTER TABLE readings RENAME TO readings_old")
        service.db.execute("CREATE TABLE readings(reading_id TEXT PRIMARY KEY,segment_id TEXT NOT NULL,sensor_id TEXT NOT NULL,pressure_kpa REAL NOT NULL,flow_lps REAL NOT NULL,acoustic_db REAL NOT NULL,observed_at TEXT NOT NULL,UNIQUE(segment_id,sensor_id,observed_at))")
        service.db.execute("INSERT INTO readings VALUES('OLD1','C1','x',300,10,50,'2026-09-24T11:00:00Z')")
        service.db.commit(); service.db.close()
        migrated=NetworkService(self.path)
        row=migrated.db.execute("SELECT observed_at,payload_fingerprint FROM readings WHERE reading_id='OLD1'").fetchone()
        self.assertEqual(row[0],"2026-09-24T11:00:00+00:00"); self.assertTrue(row[1])
        token=migrated.auth.login("admin","network-admin")
        self.assertTrue(migrated.ingest_reading(token,Reading("OLD1","C1","x",300,10,50,"2026-09-24T11:00:00+00:00"))["duplicate"])
        with self.assertRaises(Conflict):
            migrated.ingest_reading(token,Reading("OLD1","C1","x",301,10,50,"2026-09-24T11:00:00Z"))
        migrated.db.close()


if __name__=="__main__":
    unittest.main()
