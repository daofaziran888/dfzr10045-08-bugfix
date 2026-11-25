"""查询服务链路：端点查询→计划执行→快照冻结→回执复取→限流配额→用量计量。

结果集快照的持久化链路：

1. 首次查询冻结**当时**的匹配明细，行内容逐行写入快照记录，并签发
   查询回执（query_receipt）；回执与快照编号、查询计划、计量读数一一绑定。
2. 有效期内同一订阅只能凭原回执复取，复取永远从冻结明细里取数，
   源数据随后如何变化都不影响已交付的快照编号与行内容。
3. 回执按订阅（消费方应用）隔离：同端点的另一份订阅即使筛选条件完全
   相同，也不能复取不归它的快照。
4. 分段读取在回执上持久化已交付游标；中断时不推进游标，用原回执重试
   即可从未交付位置继续。无法取证（明细缺失）时返回可等待重试的错误，
   但已交付进度必须保留。
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from app.store import store

RECEIPT_MODULE = "query_receipt"

# 一次性故障注入：按回执编号登记的下一次复取中断，触发后自动解除。
_interrupt_once: set[str] = set()


def arm_next_refetch_interrupt(receipt_no: str) -> None:
    """让该回执的下一次复取中断一次（用于模拟分段读取中断，触发后自动清除）。"""
    _interrupt_once.add(receipt_no)


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _ttl_text(ttl_seconds: int) -> str:
    return (datetime.now() + timedelta(seconds=ttl_seconds)).strftime("%Y-%m-%dT%H:%M:%S")


def execute_query_chain(
    endpoint_id: str,
    consumer_app_id: str,
    filters: dict[str, str] | None = None,
    page: int = 1,
    page_size: int = 20,
) -> dict[str, Any]:
    """链路4：端点查询——从接收查询请求到返回数据结果的全流程。

    输入：端点标识+消费方应用+筛选条件 → 处理：配额检查、查询计划创建执行、
    快照冻结（明细持久化）、回执签发 → 产物：首段结果+查询回执+快照+计量读数。

    后续分段读取必须走 :func:`refetch_by_receipt`，按回执从冻结明细续取，
    再次调用本函数属于一次全新查询，会生成新的计划、快照与回执。
    """
    # 找端点
    endpoints = store.rows("service_endpoint")
    endpoint = None
    for ep in endpoints:
        if ep.get("端点编号") == endpoint_id:
            endpoint = ep
            break
    if not endpoint:
        return {"status": "error", "message": f"端点 {endpoint_id} 不存在"}
    if endpoint.get("status") not in ("已发布", "灰度中"):
        return {"status": "error", "message": f"端点 {endpoint_id} 不可查询"}

    # 配额检查
    quota_result = quota_check_and_degrade(consumer_app_id, endpoint_id)
    degraded = quota_result.get("degraded", False)

    # 创建查询计划
    plan_rows = store.rows("query_plan")
    plan_id = max((int(r.get("id", 0)) for r in plan_rows), default=0) + 1
    plan = {
        "id": plan_id,
        "计划编号": f"QPLN-{plan_id:04d}",
        "所属端点": endpoint_id,
        "筛选条件": str(filters or {}),
        "扫描范围": f"page={page},size={page_size}",
        "预估行数": str(page_size),
        "执行策略": "degraded" if degraded else "full",
        "实际行数": "0",
        "执行耗时": "0",
        "status": "执行中",
        "pending": True,
        "abnormal": False,
    }
    plan_rows.append(plan)

    # 模拟查询执行（读取内存 store 中匹配数据，全量冻结后再分段交付）
    contract_id = endpoint.get("所属契约", "")
    matched_rows = _query_contract_data(contract_id, filters, page=None, page_size=page_size)
    actual_rows = len(matched_rows)

    execute_time = f"{10 + actual_rows * 0.5:.1f}ms"
    plan["实际行数"] = str(actual_rows)
    plan["执行耗时"] = execute_time
    plan["status"] = "已返回"
    plan["pending"] = False

    # 冻结结果集快照（明细持久化，后续复取只读这份冻结数据）
    snapshot = snapshot_with_cache(plan_id=plan_id, endpoint_id=endpoint_id,
                                   contract_id=contract_id, rows=matched_rows)

    # 记录用量，并与本次计划/快照绑定，保证计量与计划逐条对得上
    meter = _record_usage(consumer_app_id, endpoint_id, 1, actual_rows)
    meter["关联计划"] = plan["计划编号"]
    meter["关联快照"] = snapshot["快照编号"]

    # 签发查询回执并交付第一段
    receipt = _create_receipt(
        consumer_app_id=consumer_app_id,
        endpoint_id=endpoint_id,
        contract_id=contract_id,
        plan=plan,
        snapshot=snapshot,
        meter=meter,
        filters=filters or {},
        segment_size=page_size,
    )
    first_segment = _frozen_rows(snapshot)[:page_size]
    receipt["已交付行数"] = str(len(first_segment))
    receipt["待续取行数"] = str(actual_rows - len(first_segment))
    if actual_rows <= len(first_segment):
        receipt["status"] = "已交付完"
        receipt["pending"] = False
    snapshot["引用次数"] = str(int(snapshot.get("引用次数", "0") or "0") + 1)

    return {
        "status": "ok",
        "plan": plan,
        "plan_no": plan["计划编号"],
        "rows": first_segment,
        "actual_count": actual_rows,
        "degraded": degraded,
        "snapshot": snapshot,
        "snapshot_no": snapshot["快照编号"],
        "receipt": receipt,
        "receipt_no": receipt["回执编号"],
        "meter": meter,
        "meter_no": meter["计量编号"],
        "execute_time": execute_time,
        "delivered_position": len(first_segment),
        "has_more": actual_rows > len(first_segment),
    }


def refetch_by_receipt(
    receipt_no: str,
    consumer_app_id: str,
    page_size: int | None = None,
) -> dict[str, Any]:
    """按查询回执复取快照明细的持久化入口。

    - 仅回执所属订阅（消费方应用）可复取，他端订阅同筛选条件也拒绝；
    - 仅在快照有效期内可复取，过期返回 error 且不改动已交付游标；
    - 行内容来自首次查询冻结的明细，源数据变化不影响复取结果；
    - 从回执持久化的已交付游标续取；分段中断时游标不前进，可原回执重试；
    - 冻结明细无法取证时返回 ``retryable`` 错误，可稍后再试，进度保留。
    """
    receipt = _find_receipt(receipt_no)
    if receipt is None:
        return {"status": "error", "code": "receipt_not_found",
                "message": f"查询回执 {receipt_no} 不存在"}

    # 订阅隔离：另一份订阅不能复取不归它的快照
    if receipt.get("消费方应用") != consumer_app_id:
        return {"status": "error", "code": "receipt_owner_mismatch",
                "message": "查询回执不属于该订阅，禁止复取他人快照"}

    snapshot = _find_snapshot(receipt.get("快照编号", ""))
    if snapshot is None:
        return {"status": "error", "code": "snapshot_not_found",
                "message": f"回执 {receipt_no} 关联的快照不存在，无法取证",
                "retryable": True,
                "delivered_position": int(receipt.get("已交付行数", "0") or "0")}

    if _snapshot_expired(snapshot) or snapshot.get("status") in ("已过期", "已失效"):
        return {"status": "error", "code": "snapshot_expired",
                "message": f"快照 {snapshot['快照编号']} 已过有效期，不能按原回执复取",
                "retryable": False,
                "delivered_position": int(receipt.get("已交付行数", "0") or "0")}

    frozen_rows = snapshot.get("冻结明细")
    if frozen_rows is None:
        # 无法取证：允许等待重试，但绝不动已交付进度
        return {"status": "error", "code": "snapshot_evidence_unavailable",
                "message": "快照明细暂不可取证，可稍后凭原回执重试",
                "retryable": True,
                "delivered_position": int(receipt.get("已交付行数", "0") or "0")}

    segment_size = page_size or int(receipt.get("分段大小", "20") or "20")
    position = int(receipt.get("已交付行数", "0") or "0")

    # 一次性中断注入：中断在推进游标之前发生，进度原样保留。
    # 即使回执已交付完，挂起的中断也要先暴露，避免故障被静默吞掉。
    if receipt_no in _interrupt_once:
        _interrupt_once.discard(receipt_no)
        receipt["status"] = "中断待续"
        receipt["abnormal"] = True
        return {
            "status": "error",
            "code": "segment_interrupted",
            "message": "分段读取中断，游标未推进，请用原回执继续复取",
            "retryable": True,
            "receipt_no": receipt_no,
            "snapshot_no": snapshot["快照编号"],
            "plan_no": receipt.get("计划编号"),
            "meter_no": receipt.get("计量编号"),
            "delivered_position": position,
            "rows": [],
        }

    if position >= len(frozen_rows):
        return {
            "status": "ok",
            "receipt_no": receipt_no,
            "snapshot_no": snapshot["快照编号"],
            "plan_no": receipt.get("计划编号"),
            "meter_no": receipt.get("计量编号"),
            "rows": [],
            "actual_count": len(frozen_rows),
            "delivered_position": position,
            "has_more": False,
            "message": "该回执已交付完毕",
        }

    segment = frozen_rows[position:position + segment_size]
    new_position = position + len(segment)
    receipt["已交付行数"] = str(new_position)
    receipt["待续取行数"] = str(len(frozen_rows) - new_position)
    receipt["最近复取"] = _now_text()
    receipt["abnormal"] = False
    has_more = new_position < len(frozen_rows)
    if not has_more:
        receipt["status"] = "已交付完"
        receipt["pending"] = False
    else:
        receipt["status"] = "续取中"
        receipt["pending"] = True
    snapshot["引用次数"] = str(int(snapshot.get("引用次数", "0") or "0") + 1)

    return {
        "status": "ok",
        "receipt_no": receipt_no,
        "snapshot_no": snapshot["快照编号"],
        "plan_no": receipt.get("计划编号"),
        "meter_no": receipt.get("计量编号"),
        "plan": _find_plan(receipt.get("计划编号")),
        "meter": _find_meter(receipt.get("计量编号")),
        "rows": segment,
        "actual_count": len(frozen_rows),
        "delivered_position": new_position,
        "has_more": has_more,
    }


def snapshot_with_cache(
    plan_id: int,
    endpoint_id: str,
    contract_id: str,
    rows: list[dict[str, Any]],
    ttl_seconds: int = 3600,
) -> dict[str, Any]:
    """链路5：结果集快照与缓存策略——冻结查询结果并维护缓存。

    输入：计划标识+端点+数据行 → 处理：快照冻结（含逐行明细持久化）、
    缓存策略校验、有效期设定 → 产物：快照记录+缓存条目。

    快照编号在复取链路中保持不变：同一订阅在有效期内凭回执复取，
    始终读到本快照冻结的明细，不会因源数据变化而换编号、换取值。
    """
    snap_rows = store.rows("result_set")
    snap_id = max((int(r.get("id", 0)) for r in snap_rows), default=0) + 1
    col_keys = sorted(rows[0].keys())[:5] if rows else []
    col_summary = {k: str(rows[0][k])[:20] for k in col_keys} if rows else {}
    now = datetime.now()
    snapshot = {
        "id": snap_id,
        "快照编号": f"SNAP-{snap_id:04d}",
        "所属计划": str(plan_id),
        "绑定版本": contract_id,
        "快照行数": str(len(rows)),
        "列摘要值": str(col_summary),
        "有效期": (now + timedelta(seconds=ttl_seconds)).strftime("%Y-%m-%dT%H:%M:%S"),
        "引用次数": "1",
        "生成时间": now.strftime("%Y-%m-%d %H:%M:%S"),
        # 首次交付明细逐行冻结，复取只允许读取这份持久化数据
        "冻结明细": [dict(row) for row in rows],
        "status": "可读",
        "pending": False,
        "abnormal": False,
    }
    snap_rows.append(snapshot)

    # 校验缓存策略——契约版本切换时旧缓存必须整体失效
    cache_rows = store.rows("cache_policy")
    for cache in cache_rows:
        if cache.get("所属端点") == endpoint_id:
            hit_count = int(cache.get("命中率", "0") or "0")
            cache["命中率"] = str(hit_count + 1)
            cache["最近调整"] = now.strftime("%Y-%m-%d %H:%M:%S")

    return snapshot


def quota_check_and_degrade(
    consumer_app_id: str,
    endpoint_id: str,
) -> dict[str, Any]:
    """链路6：限流配额检查——超额时降级返回而非丢弃已在执行的查询。

    输入：消费方应用+端点 → 处理：配额状态检查、超额处置决策 → 产物：配额状态+降级标记。
    注意：按回执复取属于同一次交付的续读，不再重复扣减配额，配额只在首次查询计一次。
    """
    quota_rows = store.rows("rate_quota")
    for quota in quota_rows:
        if quota.get("消费方应用") == consumer_app_id and quota.get("所属端点") == endpoint_id:
            current = int(quota.get("当前用量", "0") or "0")
            daily_limit = int(quota.get("日级上限", "1000") or "1000")

            if current >= daily_limit:
                quota["status"] = "已超额"
                quota["abnormal"] = True
                disposition = quota.get("超额处置", "降级返回部分数据")
                return {
                    "status": "exceeded",
                    "degraded": True,
                    "disposition": disposition,
                    "current_usage": current,
                    "daily_limit": daily_limit,
                }
            if current >= daily_limit * 0.8:
                quota["status"] = "接近上限"
                quota["abnormal"] = True
            quota["当前用量"] = str(current + 1)
            return {"status": "ok", "degraded": False, "current_usage": current + 1}

    # 无配额记录时创建默认条目
    new_id = max((int(r.get("id", 0)) for r in quota_rows), default=0) + 1
    quota_rows.append({
        "id": new_id,
        "配额编号": f"QUOTA-{new_id:04d}",
        "消费方应用": consumer_app_id,
        "所属端点": endpoint_id,
        "秒级上限": "100",
        "日级上限": "1000",
        "当前用量": "1",
        "超额处置": "降级返回部分数据",
        "计量窗口": datetime.now().strftime("%Y-%m"),
        "status": "充足",
        "pending": True,
        "abnormal": False,
    })
    return {"status": "ok", "degraded": False, "current_usage": 1}


def _query_contract_data(
    contract_id: str,
    filters: dict[str, str] | None = None,
    page: int | None = 1,
    page_size: int = 20,
) -> list[dict[str, Any]]:
    """从 store 中模拟查询契约关联数据。

    page 传 None 时返回全量匹配数据，供快照整体冻结；
    保留分页入参是为了兼容旧调用方的分段读取语义。
    """
    all_rows: list[dict[str, Any]] = []
    for module_name in store.module_names():
        for row in store.rows(module_name):
            # 搜集与契约相关的数据
            row_contract = row.get("所属契约", "")
            if row_contract and contract_id in str(row_contract):
                all_rows.append(row)
    if filters:
        for k, v in filters.items():
            all_rows = [
                r for r in all_rows
                if v.lower() in str(r.get(k, "")).lower()
            ]
    if page is None:
        return all_rows
    start = (page - 1) * page_size
    return all_rows[start:start + page_size]


def _record_usage(
    consumer_app_id: str,
    endpoint_id: str,
    call_count: int,
    row_count: int,
) -> dict[str, Any]:
    """记录用量计量——计量读数与查询计划需逐条对得上才能进入结算。"""
    meter_rows = store.rows("usage_meter")
    new_id = max((int(r.get("id", 0)) for r in meter_rows), default=0) + 1
    meter = {
        "id": new_id,
        "计量编号": f"METER-{new_id:04d}",
        "消费方应用": consumer_app_id,
        "所属端点": endpoint_id,
        "调用次数": str(call_count),
        "返回行数": str(row_count),
        "计量窗口": datetime.now().strftime("%Y-%m-%dT%H"),
        "结算口径": "按调用次数",
        "计量时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "关联计划": "",
        "关联快照": "",
        "status": "计量中",
        "pending": True,
        "abnormal": False,
    }
    meter_rows.append(meter)
    return meter


def _create_receipt(
    *,
    consumer_app_id: str,
    endpoint_id: str,
    contract_id: str,
    plan: dict[str, Any],
    snapshot: dict[str, Any],
    meter: dict[str, Any],
    filters: dict[str, Any],
    segment_size: int,
) -> dict[str, Any]:
    """签发查询回执：把订阅、计划、快照、计量与已交付游标持久化绑定。"""
    receipt_rows = store.rows(RECEIPT_MODULE)
    new_id = max((int(r.get("id", 0)) for r in receipt_rows), default=0) + 1
    total = int(snapshot.get("快照行数", "0") or "0")
    receipt = {
        "id": new_id,
        "回执编号": f"RCPT-{new_id:04d}",
        "消费方应用": consumer_app_id,
        "所属端点": endpoint_id,
        "所属契约": contract_id,
        "计划编号": plan.get("计划编号", ""),
        "快照编号": snapshot.get("快照编号", ""),
        "计量编号": meter.get("计量编号", ""),
        "筛选条件": str(filters or {}),
        "快照有效期": snapshot.get("有效期", ""),
        "分段大小": str(segment_size),
        "总行数": str(total),
        "已交付行数": "0",
        "待续取行数": str(total),
        "签发时间": _now_text(),
        "最近复取": "",
        "status": "续取中",
        "pending": True,
        "abnormal": False,
    }
    receipt_rows.append(receipt)
    return receipt


def _find_receipt(receipt_no: str) -> dict[str, Any] | None:
    for row in store.rows(RECEIPT_MODULE):
        if row.get("回执编号") == receipt_no:
            return row
    return None


def _find_snapshot(snapshot_no: str) -> dict[str, Any] | None:
    for row in store.rows("result_set"):
        if row.get("快照编号") == snapshot_no:
            return row
    return None


def _frozen_rows(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    rows = snapshot.get("冻结明细")
    if rows is None:
        return []
    return rows


def _find_plan(plan_no: str) -> dict[str, Any] | None:
    for row in store.rows("query_plan"):
        if row.get("计划编号") == plan_no:
            return row
    return None


def _find_meter(meter_no: str) -> dict[str, Any] | None:
    for row in store.rows("usage_meter"):
        if row.get("计量编号") == meter_no:
            return row
    return None


def _snapshot_expired(snapshot: dict[str, Any]) -> bool:
    expiry = str(snapshot.get("有效期", ""))
    if not expiry:
        return False
    return expiry < datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
