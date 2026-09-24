"""离线命令行验收入口。"""
from __future__ import annotations
import argparse,json
from .models import Reading,Segment
from .service import NetworkService
def run():
    s=NetworkService(); s.bootstrap(); t=s.auth.login("admin","network-admin"); s.register_segment(t,Segment("SEG-DEMO","north","water",680,5)); r=s.ingest_reading(t,Reading("RD-DEMO","SEG-DEMO","sensor-01",160,230,88,"2026-09-24T10:00:00+00:00")); report=s.risk_report(t,"SEG-DEMO"); order=s.create_work_order(t,"SEG-DEMO",r["alert_id"],"crew-north",1); s.add_resource(t,"PUMP-01","mobile-pump","north",2); allocation=s.allocate(t,"PUMP-01",order["work_order_id"],1); return {"status":"ok","segment":"SEG-DEMO","severity":r["risk"]["severity"],"probability":report["leak_probability"],"allocation":allocation["allocation_id"]}
def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--workspace",default="."); parser.parse_args(); print(json.dumps(run(),ensure_ascii=False))
if __name__=="__main__":main()
