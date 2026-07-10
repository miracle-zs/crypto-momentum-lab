from dataclasses import dataclass
from datetime import datetime

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.domain.execution import ExchangeOrderState
from crypto_momentum_lab.domain.live_rollout import (
    LIVE_APPROVAL_CONFIRMATION,
    LiveGateDecision,
    LiveGateStatus,
    LiveOperatorApproval,
)
from crypto_momentum_lab.domain.risk import (
    RiskConfigSnapshot,
    RiskHalt,
    TradingLease,
    TradingLeaseState,
)
from crypto_momentum_lab.execution_account.orders.state_machine import SubmitPolicy


@dataclass(frozen=True, slots=True)
class LiveGateContext:
    now: datetime
    live_submit_enabled: bool
    account_label: str
    strategy_name: str
    strategy_config_hash: str
    git_commit_hash: str
    database_migration_revision: str
    required_lease_owner: str
    requested_submit_policy: SubmitPolicy
    active_lease: TradingLease | None
    risk_config: RiskConfigSnapshot
    approval: LiveOperatorApproval | None
    account_state: ExecutionAccountStatus
    active_halts: tuple[RiskHalt, ...]
    unresolved_order_states: tuple[ExchangeOrderState, ...]


def evaluate_live_gate(context: LiveGateContext) -> LiveGateDecision:
    reasons: list[str] = []
    if not context.live_submit_enabled:
        reasons.append("live_submit_disabled")
    if not context.account_label.strip():
        reasons.append("missing_account_label")
    if context.requested_submit_policy is not SubmitPolicy.LIVE_SUBMIT:
        reasons.append("submit_policy_not_live")
    _check_lease(context, reasons)
    _check_approval(context, reasons)
    if context.account_state is not ExecutionAccountStatus.READY_READONLY:
        reasons.append("account_not_ready")
    if context.active_halts:
        reasons.append("active_risk_halt")
    if any(not state.terminal for state in context.unresolved_order_states):
        reasons.append("unresolved_order_uncertainty")
    return LiveGateDecision(
        status=LiveGateStatus.BLOCKED if reasons else LiveGateStatus.APPROVED,
        reasons=tuple(reasons),
    )


def _check_lease(context: LiveGateContext, reasons: list[str]) -> None:
    lease = context.active_lease
    if lease is None:
        reasons.append("missing_active_lease")
        return
    if lease.state is not TradingLeaseState.ACTIVE or lease.expires_at <= context.now:
        reasons.append("inactive_or_expired_lease")
    if lease.owner != context.required_lease_owner:
        reasons.append("lease_owner_mismatch")
    if lease.account_label != context.account_label:
        reasons.append("lease_account_mismatch")
    if lease.strategy_name != context.strategy_name:
        reasons.append("lease_strategy_mismatch")


def _check_approval(context: LiveGateContext, reasons: list[str]) -> None:
    approval = context.approval
    if approval is None:
        reasons.append("missing_operator_approval")
        return
    checks = (
        (
            approval.approval_text == LIVE_APPROVAL_CONFIRMATION,
            "approval_text_mismatch",
        ),
        (approval.expires_at > context.now, "approval_expired"),
        (approval.account_label == context.account_label, "approval_account_mismatch"),
        (approval.strategy_name == context.strategy_name, "approval_strategy_mismatch"),
        (
            approval.strategy_config_hash == context.strategy_config_hash,
            "approval_strategy_config_mismatch",
        ),
        (
            approval.risk_config_hash == context.risk_config.config_hash,
            "approval_risk_config_mismatch",
        ),
        (
            approval.git_commit_hash == context.git_commit_hash,
            "approval_commit_mismatch",
        ),
        (
            approval.database_migration_revision
            == context.database_migration_revision,
            "approval_migration_mismatch",
        ),
        (
            context.risk_config.max_order_notional
            <= approval.approved_notional_cap,
            "risk_notional_exceeds_approval",
        ),
        (
            context.risk_config.max_open_positions
            <= approval.approved_max_open_positions,
            "risk_positions_exceed_approval",
        ),
        (
            context.risk_config.max_daily_loss
            <= approval.approved_max_daily_loss,
            "risk_daily_loss_exceeds_approval",
        ),
    )
    reasons.extend(reason for passed, reason in checks if not passed)
