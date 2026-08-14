from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.domain.execution import ExchangeOrderState
from crypto_momentum_lab.domain.live_rollout import (
    LIVE_APPROVAL_CONFIRMATION,
    LiveGateStatus,
    LiveOperatorApproval,
)
from crypto_momentum_lab.domain.risk import (
    RiskConfigSnapshot,
    TradingLease,
    TradingLeaseState,
)
from crypto_momentum_lab.execution_account.orders.state_machine import SubmitPolicy
from crypto_momentum_lab.live_rollout.gates import LiveGateContext, evaluate_live_gate

NOW = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)


def test_live_gate_rejects_without_operator_approval() -> None:
    decision = evaluate_live_gate(replace(_context(), approval=None))

    assert decision.status is LiveGateStatus.BLOCKED
    assert "missing_operator_approval" in decision.reasons


def test_live_gate_rejects_when_live_submit_disabled() -> None:
    decision = evaluate_live_gate(replace(_context(), live_submit_enabled=False))

    assert "live_submit_disabled" in decision.reasons


def test_live_gate_rejects_without_ready_account_sync() -> None:
    decision = evaluate_live_gate(
        replace(_context(), account_state=ExecutionAccountStatus.DEGRADED)
    )

    assert "account_not_ready" in decision.reasons


def test_live_gate_rejects_when_strategy_lease_missing() -> None:
    decision = evaluate_live_gate(replace(_context(), active_lease=None))

    assert "missing_active_lease" in decision.reasons


def test_live_gate_accepts_complete_preflight_context() -> None:
    decision = evaluate_live_gate(_context())

    assert decision.status is LiveGateStatus.APPROVED
    assert decision.reasons == ()


def test_live_gate_allows_confirmed_resting_exit_order() -> None:
    decision = evaluate_live_gate(
        replace(
            _context(),
            unresolved_order_states=(ExchangeOrderState.ACKNOWLEDGED,),
        )
    )

    assert decision.status is LiveGateStatus.APPROVED


def test_live_gate_blocks_cancel_in_flight() -> None:
    decision = evaluate_live_gate(
        replace(
            _context(),
            unresolved_order_states=(ExchangeOrderState.CANCELING,),
        )
    )

    assert decision.status is LiveGateStatus.BLOCKED
    assert "unresolved_order_uncertainty" in decision.reasons


def _context() -> LiveGateContext:
    config = _risk_config()
    return LiveGateContext(
        now=NOW,
        live_submit_enabled=True,
        account_label="primary",
        strategy_name="compression_breakout",
        strategy_config_hash="a" * 64,
        git_commit_hash="abc123",
        database_migration_revision="20260704_0010",
        required_lease_owner="live-worker",
        requested_submit_policy=SubmitPolicy.LIVE_SUBMIT,
        active_lease=TradingLease(
            lease_id="lease-1",
            environment="live",
            account_label="primary",
            strategy_name="compression_breakout",
            owner="live-worker",
            state=TradingLeaseState.ACTIVE,
            acquired_at=NOW - timedelta(minutes=1),
            expires_at=NOW + timedelta(minutes=5),
        ),
        risk_config=config,
        approval=LiveOperatorApproval(
            approval_id="approval-1",
            account_label="primary",
            strategy_name="compression_breakout",
            strategy_config_hash="a" * 64,
            risk_config_hash=config.config_hash,
            git_commit_hash="abc123",
            database_migration_revision="20260704_0010",
            approved_notional_cap=Decimal("25"),
            approved_max_open_positions=1,
            approved_max_daily_loss=Decimal("10"),
            approver_name="operator",
            approval_text=LIVE_APPROVAL_CONFIRMATION,
            expires_at=NOW + timedelta(hours=1),
            created_at=NOW - timedelta(minutes=1),
        ),
        account_state=ExecutionAccountStatus.READY_READONLY,
        active_halts=(),
        unresolved_order_states=(),
    )


def _risk_config() -> RiskConfigSnapshot:
    return RiskConfigSnapshot(
        environment="live",
        account_label="primary",
        max_order_notional=Decimal("25"),
        max_gross_notional=Decimal("25"),
        max_daily_loss=Decimal("10"),
        max_open_positions=1,
        max_market_state_age_seconds=30,
        max_account_state_age_seconds=30,
        allow_reduce_only_while_draining=True,
        created_at=NOW,
    )
