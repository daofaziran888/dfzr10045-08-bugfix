"""查询服务路由：首次查询签发回执，后续按原回执复取冻结快照明细。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.integration.query_service_chain import (
    execute_query_chain,
    refetch_by_receipt,
)

router = APIRouter(prefix="/api/query", tags=["查询服务链路"])

# 业务错误码到 HTTP 状态码的映射；可重试的取证/中断类问题用 409，
# 归属与过期类确定性拒绝用 403/410，未知回执用 404。
_ERROR_STATUS = {
    "receipt_not_found": 404,
    "receipt_owner_mismatch": 403,
    "snapshot_expired": 410,
    "snapshot_not_found": 409,
    "snapshot_evidence_unavailable": 409,
    "segment_interrupted": 409,
}


class QueryPayload(BaseModel):
    endpoint_id: str
    consumer_app_id: str
    filters: dict[str, str] = Field(default_factory=dict)
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=500)


class RefetchPayload(BaseModel):
    consumer_app_id: str
    page_size: int | None = Field(default=None, ge=1, le=500)


def _raise_if_failed(result: dict[str, Any]) -> None:
    if result.get("status") == "ok":
        return
    code = result.get("code", "query_failed")
    status_code = _ERROR_STATUS.get(code, 400)
    raise HTTPException(status_code=status_code, detail=result)


@router.post("/execute")
def execute_query(payload: QueryPayload) -> dict[str, Any]:
    """首次查询：执行查询计划、冻结快照、签发查询回执并交付第一段。"""
    result = execute_query_chain(
        endpoint_id=payload.endpoint_id,
        consumer_app_id=payload.consumer_app_id,
        filters=payload.filters,
        page=payload.page,
        page_size=payload.page_size,
    )
    _raise_if_failed(result)
    return result


@router.post("/receipt/{receipt_no}/refetch")
def refetch_receipt(receipt_no: str, payload: RefetchPayload) -> dict[str, Any]:
    """按原查询回执复取：从冻结快照的未交付位置继续，订阅归属强校验。"""
    result = refetch_by_receipt(
        receipt_no=receipt_no,
        consumer_app_id=payload.consumer_app_id,
        page_size=payload.page_size,
    )
    _raise_if_failed(result)
    return result
