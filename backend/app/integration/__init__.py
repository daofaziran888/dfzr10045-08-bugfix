"""跨模块业务集成层：贯通数据契约全生命周期的真实数据处理链路。

每条链路包含确定性步骤，支持失败恢复、去重、冲突版本、迟到数据与水位推进。
"""

from app.integration.contract_lifecycle import (
    register_contract_chain,
    evolve_contract_version,
    validate_field_promises,
)
from app.integration.query_service_chain import (
    execute_query_chain,
    refetch_by_receipt,
    snapshot_with_cache,
    quota_check_and_degrade,
    arm_next_refetch_interrupt,
)
from app.integration.sla_freshness_chain import (
    track_freshness_and_settle_sla,
    escalate_breach,
)
from app.integration.release_reconcile_chain import (
    release_dataset_with_reconcile,
    dual_run_and_attribute,
)
from app.integration.sample_access_chain import (
    probe_sample_and_grant_access,
    revoke_expired_grants,
)
from app.integration.audit_trail import (
    record_audit_entry,
    verify_audit_chain,
)

__all__ = [
    "register_contract_chain",
    "evolve_contract_version",
    "validate_field_promises",
    "execute_query_chain",
    "refetch_by_receipt",
    "snapshot_with_cache",
    "quota_check_and_degrade",
    "arm_next_refetch_interrupt",
    "track_freshness_and_settle_sla",
    "escalate_breach",
    "release_dataset_with_reconcile",
    "dual_run_and_attribute",
    "probe_sample_and_grant_access",
    "revoke_expired_grants",
    "record_audit_entry",
    "verify_audit_chain",
]