"""Startup must preserve authoritative state and reject trace-only recovery."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
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
