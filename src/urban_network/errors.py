"""管网服务暴露给 API 层的业务错误。"""
from __future__ import annotations


class ServiceError(RuntimeError):
    """带有 HTTP 状态码的服务错误。"""
    status = 400


class Conflict(ServiceError):
    """请求与已持久化的记录冲突：编号载荷不一致或传感器时刻被占用。"""
    status = 409
