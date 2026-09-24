"""管网领域服务的可观察错误。"""
from __future__ import annotations


class ServiceError(RuntimeError):
    code = "service_error"
    status = 400


class Conflict(ServiceError):
    """读数编号被不同载荷复用，或同一传感器时刻被其他编号占用。"""

    code = "conflict"
    status = 409
