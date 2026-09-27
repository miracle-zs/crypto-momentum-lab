"""Verify immutable decision references under real PostgreSQL transactions."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.decision.decision_engine import PolicyState
from crypto_momentum_lab.domain.decision.policy_transition import (
    compute_policy_state_digest,
    serialize_policy_state,
)
from crypto_momentum_lab.domain.market.revision_models import (
    DecisionTrace,
    MarketRevisionRef,
    MarketVisibilityMode,
)
from crypto_momentum_lab.persistence.postgres.decision_trace_repository import (
    PostgresDecisionTraceRepository,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
    AsyncPostgresDecisionUnitOfWork,
    DecisionCommit,
    DecisionCommitConflict,
)
from crypto_momentum_lab.persistence.postgres.models import DecisionTraceRow
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)


def _trace(identity: str, decision: str) -> DecisionTrace:
    now = datetime(2026, 9, 27, tzinfo=UTC)
    ref = MarketRevisionRef(
        scope="live",
        symbol="BTCUSDT",
        interval="15s",
        bucket_start=now,
        bucket_end=now + timedelta(seconds=15),
        revision_id=f"authority-ref-{identity}",
        content_hash=identity,
        published_at=now + timedelta(seconds=15),
        source_epoch=identity,
        visibility_mode=MarketVisibilityMode.DECISION_VISIBLE,
        observed_at=now,
    )
    return DecisionTrace(
        decision_id=f"authority-trace-{identity}-{decision}",
        strategy_name="authority-test",
        account_label=f"test-{identity}-{decision}",
        decision_time=now + timedelta(seconds=15),
        evaluated_market_refs=(ref,),
        intent_produced=False,
        intent_id=None,
        rejection_reason="test",
        input_hash=identity,
        frame_digest=identity,
        trace_payload={"market_state": {"symbol": "BTCUSDT", "close_price": "102"}},
    )


@pytest.mark.asyncio
async def test_concurrent_decisions_share_identical_revision(
    async_database_url: str,
) -> None:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    repository = PostgresDecisionTraceRepository(factory)
    identity = uuid4().hex
    traces = (_trace(identity, "one"), _trace(identity, "two"))

    async def persist(trace: DecisionTrace) -> None:
        async with factory() as session, session.begin():
            await repository.save_decision_traces_in_session(session, (trace,))

    try:
        await asyncio.gather(*(persist(trace) for trace in traces))
        assert all(
            [
                await repository.load_decision_trace(trace.decision_id)
                for trace in traces
            ]
        )
        # A batch may refer to the same revision more than once.
        async with factory() as session, session.begin():
            await repository.save_decision_traces_in_session(session, traces)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_conflicting_reference_rolls_back_other_trace(
    async_database_url: str,
) -> None:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    repository = PostgresDecisionTraceRepository(factory)
    identity = uuid4().hex
    original = _trace(identity, "original")
    candidate = _trace(identity, "candidate")
    conflict = replace(
        original,
        trace_payload={"market_state": {"symbol": "BTCUSDT", "close_price": "999"}},
    )
    try:
        async with factory() as session, session.begin():
            await repository.save_decision_traces_in_session(session, (original,))
        with pytest.raises(ValueError, match="conflict"):
            async with factory() as session, session.begin():
                await repository.save_decision_traces_in_session(
                    session, (candidate, conflict)
                )
        async with factory() as session:
            row = await session.scalar(
                select(DecisionTraceRow).where(
                    DecisionTraceRow.decision_id == candidate.decision_id
                )
            )
        assert row is None
        persisted = await repository.load_decision_trace(original.decision_id)
        assert persisted is not None
        assert persisted.trace_payload["market_state"]["close_price"] == "102"
    finally:
        await engine.dispose()


def _commit(identity: str, decision: str) -> DecisionCommit:
    trace = _trace(identity, decision)
    state = PolicyState()
    payload = dict(trace.trace_payload)
    payload.update(
        prior_policy_state=serialize_policy_state(state),
        next_policy_state=serialize_policy_state(state),
        decision_context={
            "position_view": {
                "symbol": "BTCUSDT",
                "position_key": {
                    "environment": "live",
                    "account_label": trace.account_label,
                    "symbol": "BTCUSDT",
                    "position_side": "BOTH",
                },
            }
        },
    )
    return DecisionCommit(
        trace=replace(trace, trace_payload=payload),
        policy_key=f"live/{trace.account_label}/{trace.strategy_name}",
        expected_policy_revision=0,
        expected_prior_digest=compute_policy_state_digest(state),
        prior_policy_state=state,
        next_policy_state=state,
    )


@pytest.mark.asyncio
async def test_noop_policy_still_rejects_concurrent_stale_decision(
    async_database_url: str,
) -> None:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    uow = AsyncPostgresDecisionUnitOfWork(factory)
    first = _commit(uuid4().hex, "shared-account")
    second = replace(
        first, trace=replace(first.trace, decision_id=f"{first.trace.decision_id}-next")
    )
    try:
        results = await asyncio.gather(
            uow.commit_decision(first),
            uow.commit_decision(second),
            return_exceptions=True,
        )
        assert (
            sum(isinstance(result, DecisionCommitConflict) for result in results) == 1
        )
        snapshot = await uow.load_policy_state(first.policy_key)
        assert snapshot is not None
        assert snapshot.revision == 1
        winner = first if not isinstance(results[0], Exception) else second
        receipt = await uow.commit_decision(winner)
        assert receipt.policy_revision == 1
        assert (await uow.load_policy_state(first.policy_key)).revision == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_failure_after_trace_write_rolls_back_trace_and_policy(
    async_database_url: str,
) -> None:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    class FailingTraceRepository(PostgresDecisionTraceRepository):
        async def save_decision_traces_in_session(self, session, traces, **kwargs):
            await super().save_decision_traces_in_session(session, traces, **kwargs)
            raise RuntimeError("injected after durable trace SQL")

    uow = AsyncPostgresDecisionUnitOfWork(
        factory, trace_repository=FailingTraceRepository(factory)
    )
    commit = _commit(uuid4().hex, "rollback-account")
    try:
        with pytest.raises(RuntimeError, match="injected"):
            await uow.commit_decision(commit)
        assert await uow.load_policy_state(commit.policy_key) is None
        assert (
            await PostgresDecisionTraceRepository(factory).load_decision_trace(
                commit.trace.decision_id
            )
            is None
        )
    finally:
        await engine.dispose()
