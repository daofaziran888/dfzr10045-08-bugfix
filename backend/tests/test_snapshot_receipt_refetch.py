"""快照回执复取链路测试：冻结明细持久化、订阅隔离、分段续取与计划/计量对账。

验收主线：
1. 首次查询确认 x=12 并读到第一段；
2. 源数据更正为 x=15 后，凭原回执复取仍读到冻结的 12，快照编号不变；
3. 复取下一段中断一次，游标不前进，原回执可从断点继续；
4. 另一份订阅（同端点、同筛选）不能复取不归它的快照；
5. 另发新查询能读到源数据最新值 15，并生成新的计划/快照/计量。
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from app.integration.query_service_chain import (
    RECEIPT_MODULE,
    arm_next_refetch_interrupt,
    execute_query_chain,
    refetch_by_receipt,
)
from app.store import store

ENDPOINT_ID = "EP-ACC"
CONTRACT_ID = "CTR-ACC"
OWNER_APP = "APP-ANALYTICS"
OTHER_APP = "APP-DASHBOARD"
FILTERS = {"x": "12"}


@pytest.fixture
def endpoint():
    """专用端点：绑定仅含合成源数据的契约，避免与种子表互相干扰。"""
    endpoints = store.rows("service_endpoint")
    endpoint = {
        "id": max((int(r.get("id", 0)) for r in endpoints), default=0) + 1,
        "端点编号": ENDPOINT_ID,
        "所属契约": CONTRACT_ID,
        "端点路径": f"/api/v1/contract/{CONTRACT_ID}/query",
        "支持筛选": "x",
        "分页上限": "500",
        "返回列集合": "x,label",
        "缓存开关": "开启",
        "最近发布": "2026-11-25",
        "status": "已发布",
        "pending": False,
        "abnormal": False,
    }
    endpoints.append(endpoint)
    return endpoint


def _seed_x_rows(values: list[int]) -> None:
    """在独立表中构造绑定契约的源数据，避免与种子数据互相干扰。"""
    table = store.rows("acceptance_source")
    table.clear()
    for idx, value in enumerate(values, start=1):
        table.append({
            "id": idx,
            "所属契约": CONTRACT_ID,
            "x": str(value),
            "label": f"row-{idx}",
        })


def _all_delivered_rows(receipt_no: str, app_id: str, page_size: int = 2) -> list[dict]:
    """持续凭原回执复取直到交付完毕，返回按顺序拼回的全部行。"""
    delivered: list[dict] = []
    for _ in range(20):
        result = refetch_by_receipt(receipt_no, app_id, page_size=page_size)
        assert result["status"] == "ok"
        delivered.extend(result["rows"])
        if not result["has_more"]:
            return delivered
    raise AssertionError("回执始终未交付完毕，可能游标没有推进")


@pytest.fixture
def source_rows(endpoint):
    _seed_x_rows([12, 12, 12, 12, 12])
    yield store.rows("acceptance_source")


class TestSnapshotReceiptRefetch:
    def test_first_query_freezes_detail_and_issues_receipt(self, source_rows):
        """首次查询：冻结明细、签发回执，首段内容与当时源数据一致。"""
        result = execute_query_chain(
            endpoint_id=ENDPOINT_ID,
            consumer_app_id=OWNER_APP,
            filters=FILTERS,
            page_size=2,
        )
        assert result["status"] == "ok"
        receipt = result["receipt"]
        snapshot = result["snapshot"]

        # 首段读到 x=12
        assert len(result["rows"]) == 2
        assert all(row["x"] == "12" for row in result["rows"])
        assert result["has_more"] is True
        assert result["delivered_position"] == 2

        # 回执与快照、计划、计量一一绑定，快照列表编号与回执记录对得上
        assert receipt["回执编号"] == result["receipt_no"]
        assert receipt["快照编号"] == snapshot["快照编号"] == result["snapshot_no"]
        assert receipt["计划编号"] == result["plan_no"]
        assert receipt["计量编号"] == result["meter_no"]
        assert receipt["消费方应用"] == OWNER_APP
        assert int(receipt["已交付行数"]) == 2

        listed = next(s for s in store.rows("result_set")
                      if s["快照编号"] == snapshot["快照编号"])
        assert listed is snapshot
        # 首次交付明细已逐行持久化，共 5 行 x=12
        frozen = snapshot["冻结明细"]
        assert len(frozen) == 5
        assert all(row["x"] == "12" for row in frozen)

    def test_refetch_returns_frozen_rows_after_source_correction(self, source_rows):
        """源数据更正为 x=15 后，原回执复取仍是冻结的 12，且快照编号不变。"""
        first = execute_query_chain(
            endpoint_id=ENDPOINT_ID,
            consumer_app_id=OWNER_APP,
            filters=FILTERS,
            page_size=2,
        )
        receipt_no = first["receipt_no"]
        snapshot_no = first["snapshot_no"]
        first_rows = [dict(r) for r in first["snapshot"]["冻结明细"]]

        # 更正源数据：全部改为 15
        for row in source_rows:
            row["x"] = "15"

        second = refetch_by_receipt(receipt_no, OWNER_APP, page_size=2)
        assert second["status"] == "ok"
        # 快照编号与首次确认一致，没有生成新快照
        assert second["snapshot_no"] == snapshot_no
        assert all(row["x"] == "12" for row in second["rows"])

        delivered = first["rows"] + second["rows"] + _all_delivered_rows(
            receipt_no, OWNER_APP, page_size=2)
        assert len(delivered) == 5
        assert [row["x"] for row in delivered] == ["12"] * 5
        assert delivered == first_rows

        # 复取全程只生成过这一份快照
        owned_snaps = [
            s for s in store.rows("result_set")
            if s["快照编号"] == snapshot_no
        ]
        assert len(owned_snaps) == 1

    def test_other_subscription_cannot_refetch_foreign_snapshot(self, source_rows):
        """同端点另一份订阅，即使筛选条件相同，也不能借用他人快照。"""
        first = execute_query_chain(
            endpoint_id=ENDPOINT_ID,
            consumer_app_id=OWNER_APP,
            filters=FILTERS,
            page_size=2,
        )
        receipt_no = first["receipt_no"]

        denied = refetch_by_receipt(receipt_no, OTHER_APP, page_size=2)
        assert denied["status"] == "error"
        assert denied["code"] == "receipt_owner_mismatch"

        # 被拒绝后回执游标没有变化，属主仍可正常续取
        receipt = next(r for r in store.rows(RECEIPT_MODULE)
                       if r["回执编号"] == receipt_no)
        assert int(receipt["已交付行数"]) == 2
        ok = refetch_by_receipt(receipt_no, OWNER_APP, page_size=2)
        assert ok["status"] == "ok"
        assert len(ok["rows"]) == 2

    def test_interrupted_segment_keeps_position_and_resumes(self, source_rows):
        """分段中断一次：游标不推进，原回执从未交付位置继续。"""
        first = execute_query_chain(
            endpoint_id=ENDPOINT_ID,
            consumer_app_id=OWNER_APP,
            filters=FILTERS,
            page_size=2,
        )
        receipt_no = first["receipt_no"]
        snapshot_no = first["snapshot_no"]

        # 让下一段复取中断
        arm_next_refetch_interrupt(receipt_no)
        interrupted = refetch_by_receipt(receipt_no, OWNER_APP, page_size=2)
        assert interrupted["status"] == "error"
        assert interrupted["code"] == "segment_interrupted"
        assert interrupted["retryable"] is True
        # 进度留住：仍停在首段交付后的位置 2，且没有任何行被错误交付
        assert interrupted["delivered_position"] == 2
        assert interrupted["rows"] == []

        receipt = next(r for r in store.rows(RECEIPT_MODULE)
                       if r["回执编号"] == receipt_no)
        assert int(receipt["已交付行数"]) == 2

        # 原回执继续复取：从中断点拿到第 3、4 行，仍是冻结的 12
        resumed = refetch_by_receipt(receipt_no, OWNER_APP, page_size=2)
        assert resumed["status"] == "ok"
        assert resumed["delivered_position"] == 4
        assert [row["x"] for row in resumed["rows"]] == ["12", "12"]
        assert resumed["snapshot_no"] == snapshot_no

        tail = refetch_by_receipt(receipt_no, OWNER_APP, page_size=2)
        assert tail["status"] == "ok"
        assert tail["has_more"] is False
        assert len(tail["rows"]) == 1
        assert tail["rows"][0]["x"] == "12"

        # 中断注入是一次性的，再复取不会重复中断
        again = refetch_by_receipt(receipt_no, OWNER_APP, page_size=2)
        assert again["status"] == "ok"
        assert again["rows"] == []

    def test_refetch_response_links_plan_and_meter(self, source_rows):
        """复取响应关联首次的查询计划与计量，供订阅人员和计划对账。"""
        first = execute_query_chain(
            endpoint_id=ENDPOINT_ID,
            consumer_app_id=OWNER_APP,
            filters=FILTERS,
            page_size=2,
        )
        receipt_no = first["receipt_no"]
        plan_no = first["plan_no"]
        meter_no = first["meter_no"]

        refetched = refetch_by_receipt(receipt_no, OWNER_APP, page_size=2)
        assert refetched["plan_no"] == plan_no
        assert refetched["meter_no"] == meter_no
        assert refetched["plan"]["计划编号"] == plan_no
        assert refetched["meter"]["计量编号"] == meter_no
        # 计量与计划逐条对得上：计量记录回链同一计划与快照
        assert refetched["meter"]["关联计划"] == plan_no
        assert refetched["meter"]["关联快照"] == first["snapshot_no"]
        assert refetched["meter"]["返回行数"] == "5"

        # 续读不重复计量、不重复扣配额：仍只有一条该回执的计量记录
        meters = [m for m in store.rows("usage_meter")
                  if m["计量编号"] == meter_no]
        assert len(meters) == 1

    def test_new_query_after_correction_reads_current_value(self, source_rows):
        """另发新查询读到源数据最新值 15，并生成全新计划/快照/计量/回执。"""
        first = execute_query_chain(
            endpoint_id=ENDPOINT_ID,
            consumer_app_id=OWNER_APP,
            filters=FILTERS,
            page_size=2,
        )
        old_receipt = first["receipt_no"]
        old_snapshot = first["snapshot_no"]
        old_plan = first["plan_no"]
        old_meter = first["meter_no"]

        for row in source_rows:
            row["x"] = "15"

        # 新查询按最新值 x=15 过滤，确认能读到更正后的源数据
        new_query = execute_query_chain(
            endpoint_id=ENDPOINT_ID,
            consumer_app_id=OWNER_APP,
            filters={"x": "15"},
            page_size=2,
        )
        assert new_query["status"] == "ok"
        assert all(row["x"] == "15" for row in new_query["rows"])
        assert new_query["receipt_no"] != old_receipt
        assert new_query["snapshot_no"] != old_snapshot
        assert new_query["plan_no"] != old_plan
        assert new_query["meter_no"] != old_meter
        assert all(row["x"] == "15" for row in new_query["snapshot"]["冻结明细"])

        # 旧回执复取仍是 12，两条交付链路互不串扰
        old_refetch = refetch_by_receipt(old_receipt, OWNER_APP, page_size=2)
        assert old_refetch["status"] == "ok"
        assert old_refetch["snapshot_no"] == old_snapshot
        assert all(row["x"] == "12" for row in old_refetch["rows"])

    def test_unknown_receipt_and_expired_snapshot_rejected(self, source_rows):
        """未知回执报错；快照过有效期后拒绝复取且保留已交付进度。"""
        missing = refetch_by_receipt("RCPT-9999", OWNER_APP)
        assert missing["status"] == "error"
        assert missing["code"] == "receipt_not_found"

        first = execute_query_chain(
            endpoint_id=ENDPOINT_ID,
            consumer_app_id=OWNER_APP,
            filters=FILTERS,
            page_size=2,
        )
        receipt_no = first["receipt_no"]
        snapshot = first["snapshot"]
        snapshot["有效期"] = (datetime.now() - timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S")

        expired = refetch_by_receipt(receipt_no, OWNER_APP, page_size=2)
        assert expired["status"] == "error"
        assert expired["code"] == "snapshot_expired"
        assert expired["delivered_position"] == 2

        receipt = next(r for r in store.rows(RECEIPT_MODULE)
                       if r["回执编号"] == receipt_no)
        assert int(receipt["已交付行数"]) == 2

    def test_missing_frozen_evidence_is_retryable_and_keeps_progress(self, source_rows):
        """冻结明细无法取证时可等待重试，已交付进度不丢。"""
        first = execute_query_chain(
            endpoint_id=ENDPOINT_ID,
            consumer_app_id=OWNER_APP,
            filters=FILTERS,
            page_size=2,
        )
        receipt_no = first["receipt_no"]
        first["snapshot"]["冻结明细"] = None

        unavailable = refetch_by_receipt(receipt_no, OWNER_APP, page_size=2)
        assert unavailable["status"] == "error"
        assert unavailable["code"] == "snapshot_evidence_unavailable"
        assert unavailable["retryable"] is True
        assert unavailable["delivered_position"] == 2

        # 取证恢复后，凭原回执仍可从位置 2 继续
        first["snapshot"]["冻结明细"] = [
            {"id": i, "所属契约": CONTRACT_ID, "x": "12", "label": f"row-{i}"}
            for i in range(1, 6)
        ]
        resumed = refetch_by_receipt(receipt_no, OWNER_APP, page_size=2)
        assert resumed["status"] == "ok"
        assert resumed["delivered_position"] == 4
