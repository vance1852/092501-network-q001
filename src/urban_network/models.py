"""管段、读数、告警、工单和资源的领域模型。"""
from __future__ import annotations
import hashlib, json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()

def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return (parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)

def canonical_instant(value: str) -> str:
    """把采集时刻归一化为 UTC ISO 文本，同一物理时刻得到同一字符串。"""
    return parse_time(value).isoformat()

def reading_fingerprint(segment_id: str, sensor_id: str, observed_instant: str,
                        pressure_kpa: float, flow_lps: float, acoustic_db: float) -> str:
    """读数的稳定业务指纹：覆盖管段、传感器、归一化时刻和全部测量值。"""
    payload = [segment_id, sensor_id, observed_instant,
               repr(float(pressure_kpa)), repr(float(flow_lps)), repr(float(acoustic_db))]
    return hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()

@dataclass(frozen=True)
class Segment:
    segment_id: str; district: str; network_type: str; length_m: float; criticality: int; status: str = "normal"
    def validate(self) -> None:
        if not self.segment_id.strip() or not self.district.strip(): raise ValueError("segment id and district are required")
        if self.network_type not in {"water", "drainage", "gas"}: raise ValueError("unsupported network type")
        if self.length_m <= 0 or not 1 <= self.criticality <= 5: raise ValueError("segment dimensions are invalid")

@dataclass(frozen=True)
class Reading:
    reading_id: str; segment_id: str; sensor_id: str; pressure_kpa: float; flow_lps: float; acoustic_db: float; observed_at: str
    def validate(self) -> None:
        if not self.reading_id.strip() or not self.segment_id.strip() or not self.sensor_id.strip(): raise ValueError("reading identifiers are required")
        if min(self.pressure_kpa, self.flow_lps, self.acoustic_db) < 0: raise ValueError("reading values cannot be negative")
        parse_time(self.observed_at)

def as_dict(value: Any) -> dict[str, Any]:
    return {name: getattr(value, name) for name in value.__dataclass_fields__} if hasattr(value, "__dataclass_fields__") else dict(value)
