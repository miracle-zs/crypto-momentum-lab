"""Startup must preserve authoritative state and reject trace-only recovery."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.domain.decision.decision_engine import PolicyState
from crypto_momentum_lab.domain.decision.policy_transition import (
    compute_policy_state_digest,
    serialize_policy_state,
)
from crypto_momentum_lab.domain.execution.ports import DecisionCommitConflict
from crypto_momentum_lab.persistence.postgres.decision_unit_of_work import (
    AsyncPostgresDecisionUnitOfWork,
    _trade_command_from_payload,
    _trade_command_payload,
)


@asynccontextmanager
async def _transaction():
    yield


@pytest.mark.parametrize("trace_exists", [False, True])
async def test_missing_policy_head_is_fresh_only_without_a_trace(trace_exists):
    session = SimpleNamespace(
        begin=_transaction,
        get=AsyncMock(return_value=None),
        scalar=AsyncMock(
            return_value=SimpleNamespace(decision_id="old-decision")
            if trace_exists
            else None
        ),
    )

    @asynccontextmanager
    async def factory():
        yield session

    uow = AsyncPostgresDecisionUnitOfWork(factory)
    uow._require_durable_commit = AsyncMock()
    uow._lock_policy = AsyncMock()
    if trace_exists:
        with pytest.raises(DecisionCommitConflict, match="no durable policy head"):
            await uow.load_policy_state_for_startup(
                "live/primary/strategy", "strategy", "primary"
            )
    else:
        assert (
            await uow.load_policy_state_for_startup(
                "live/primary/strategy", "strategy", "primary"
            )
            is None
        )


async def test_current_policy_head_preserves_cooldown_and_revision():
    until = datetime(2026, 10, 4, tzinfo=UTC) + timedelta(minutes=15)
    state = PolicyState(cooldown_until_by_symbol={"BTCUSDT": until})
    row = SimpleNamespace(
        state_payload=serialize_policy_state(state),
        state_digest=compute_policy_state_digest(state),
        policy_revision=7,
        last_decision_id="current-decision",
    )
    session = SimpleNamespace(
        begin=_transaction, get=AsyncMock(return_value=row), scalar=AsyncMock()
    )

    @asynccontextmanager
    async def factory():
        yield session

    uow = AsyncPostgresDecisionUnitOfWork(factory)
    uow._require_durable_commit = AsyncMock()
    uow._lock_policy = AsyncMock()
    restored = await uow.load_policy_state_for_startup(
        "live/primary/strategy", "strategy", "primary"
    )
    assert restored.state == state
    assert restored.revision == 7
    assert restored.last_decision_id == "current-decision"
    session.scalar.assert_not_awaited()


@pytest.mark.parametrize("with_allocation", [False, True])
def test_pending_exit_restore_preserves_explicit_nullable_fields(with_allocation):
    from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
    from crypto_momentum_lab.domain.execution.order_state import ExitAllocation
    from crypto_momentum_lab.domain.execution.trade_command import (
        ExitAllocationPlan,
        ExitPolicyMode,
        TradeCommand,
        TradeCommandType,
    )
    from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

    key = ExecutionScope(
        environment="live", account_label="primary", symbol="BTCUSDT"
    ).to_position_key()
    plan = (
        ExitAllocationPlan(
            position_key=key,
            allocations=(ExitAllocation("batch-1", Decimal("1"), Decimal("100")),),
            total_allocated_quantity=Decimal("1"),
            policy=ExitPolicyMode.TARGET_BATCHES_ONLY,
            batch_quantities={"batch-1": Decimal("2")},
        )
        if with_allocation
        else None
    )
    command = TradeCommand(
        command_id="exit-1",
        position_key=key,
        command_type=TradeCommandType.EXIT,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("1"),
        reduce_only=True,
        allocation_plan=plan,
        created_at=datetime(2026, 10, 4, tzinfo=UTC),
    )
    payload = _trade_command_payload(command)
    assert _trade_command_from_payload(payload) == command
    if with_allocation:
        del payload["allocation_plan"]["allocations"][0]["entry_price"]
        with pytest.raises(KeyError, match="entry_price"):
            _trade_command_from_payload(payload)
    else:
        del payload["allocation_plan"]
        with pytest.raises(KeyError, match="allocation_plan"):
            _trade_command_from_payload(payload)
