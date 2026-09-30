from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.domain.live_rollout import (
    LIVE_APPROVAL_CONFIRMATION,
    LiveOperatorApproval,
    LiveSessionState,
)
from crypto_momentum_lab.domain.risk import (
    RiskConfigSnapshot,
    TradingLease,
    TradingLeaseState,
)
from crypto_momentum_lab.execution_account.orders.state_machine import SubmitPolicy
from crypto_momentum_lab.live_rollout.gates import LiveGateContext
from crypto_momentum_lab.live_rollout.lease_recovery import (
    maybe_auto_reacquire_live_lease,
)

NOW = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    "state", [None, LiveSessionState.HALTED, LiveSessionState.DRAINING]
)
async def test_inactive_session_cannot_reacquire(state) -> None:
    reader = AsyncMock()
    reader.load_latest_operating_state.return_value = state
    writer = AsyncMock()
    result = await maybe_auto_reacquire_live_lease(
        session_state_reader=reader,
        risk_repository=writer,
        gate_context=replace(_context(), active_lease=None),
        session_id="s1",
        draining=False,
        lease_ttl_seconds=60,
    )
    assert result is None
    writer.acquire_lease.assert_not_awaited()
    reader.load_latest_operating_state.assert_awaited_once_with("s1")


@pytest.mark.parametrize(
    "draining,enabled", [(False, True), (True, True), (False, False)]
)
async def test_recovery_requires_only_missing_lease_gate(draining, enabled) -> None:
    reader = AsyncMock()
    reader.load_latest_operating_state.return_value = LiveSessionState.LIVE_ENABLED
    writer = AsyncMock()
    context = replace(_context(), active_lease=None, live_submit_enabled=enabled)
    result = await maybe_auto_reacquire_live_lease(
        session_state_reader=reader,
        risk_repository=writer,
        gate_context=context,
        session_id="s1",
        draining=draining,
        lease_ttl_seconds=60,
    )
    if draining or not enabled:
        assert result is None
        writer.acquire_lease.assert_not_awaited()
    else:
        assert result is not None
        writer.acquire_lease.assert_awaited_once_with(result)
        assert result.expires_at == context.now + timedelta(seconds=60)
        assert result.owner == context.required_lease_owner
        assert result.code_generation == context.git_commit_hash


async def test_existing_lease_does_not_read_or_write() -> None:
    reader, writer = AsyncMock(), AsyncMock()
    context = _context()
    result = await maybe_auto_reacquire_live_lease(
        session_state_reader=reader,
        risk_repository=writer,
        gate_context=context,
        session_id="s1",
        draining=False,
        lease_ttl_seconds=60,
    )
    assert result is context.active_lease
    reader.load_latest_operating_state.assert_not_awaited()
    writer.acquire_lease.assert_not_awaited()


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
            code_generation="abc123",
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
