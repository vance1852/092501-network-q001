"""管段、读数、告警、工单和资源的领域模型。"""
from __future__ import annotations
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()

def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return (parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)

def normalize_time(value: str) -> str:
    """把同一时刻的不同写法（Z、带偏移）归一成统一 UTC 文本。"""
    return parse_time(value).isoformat()

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
    def normalized_observed_at(self) -> str:
        return normalize_time(self.observed_at)
    def business_fingerprint(self) -> str:
        """稳定业务指纹：覆盖载荷、来源传感器与归一化时刻，刻意排除 reading_id。

        同一读数的通信重放指纹一致；改动压力/流量/声学值或时刻会产生不同指纹；
        同一 (segment_id, sensor_id, observed_at) 业务键始终归约到同一时刻文本。
        """
        payload = "|".join((
            self.segment_id,
            self.sensor_id,
            self.normalized_observed_at(),
            repr(float(self.pressure_kpa)),
            repr(float(self.flow_lps)),
            repr(float(self.acoustic_db)),
        ))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def as_dict(value: Any) -> dict[str, Any]:
    return {name: getattr(value, name) for name in value.__dataclass_fields__} if hasattr(value, "__dataclass_fields__") else dict(value)
