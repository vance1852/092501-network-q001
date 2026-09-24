"""压力、流量和声学信号的确定性风险评分。"""
from __future__ import annotations
import math
from dataclasses import dataclass
@dataclass(frozen=True)
class RiskResult: score: float; severity: str; reasons: tuple[str,...]
def score_reading(pressure_kpa,flow_lps,acoustic_db,criticality):
    if min(pressure_kpa,flow_lps,acoustic_db)<0 or not 1<=criticality<=5: raise ValueError("sensor values are invalid")
    ps=min(1.0,abs(pressure_kpa-350.0)/180.0); fs=min(1.0,flow_lps/240.0); a=max(0.0,min(1.0,(acoustic_db-45.0)/45.0)); c=criticality/5.0
    score=round(100*(.34*ps+.26*fs+.25*a+.15*c),4); reasons=[]
    if ps>=.6: reasons.append("pressure-deviation")
    if fs>=.7: reasons.append("flow-surge")
    if a>=.5: reasons.append("acoustic-anomaly")
    severity="critical" if score>=75 else "high" if score>=50 else "medium" if score>=25 else "low"
    return RiskResult(score,severity,tuple(reasons or ["normal-variance"]))
def leak_probability(history):
    if not history:return 0.0
    scores=[float(x.get("score",0)) for x in history]; tail=scores[-5:]; weighted=sum(s*(i+1) for i,s in enumerate(tail))/sum(range(1,len(tail)+1)); return round(1-math.exp(-weighted/80),6)
