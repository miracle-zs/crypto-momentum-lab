import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.risk import RiskDecision, RiskEvaluation
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategySide,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ExchangeFillRow,
    ExchangeOrderEventRow,
    ExchangeOrderRow,
    ExecutionCommandRow,
    ExecutionReconciliationEventRow,
    OrderIntentClaimRow,
    OrderIntentExecutionRow,
)
from crypto_momentum_lab.persistence.postgres.order_repository import (
    PostgresOrderRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)

NOW = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)


@pytest.fixture
async def order_repository(
    async_database_url: str,
) -> AsyncIterator[tuple[PostgresOrderRepository, async_sessionmaker[AsyncSession]]]:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        async with session.begin():
            for model in (
                ExchangeFillRow,
                ExchangeOrderEventRow,
                ExchangeOrderRow,
                OrderIntentClaimRow,
                OrderIntentExecutionRow,
                ExecutionCommandRow,
                ExecutionReconciliationEventRow,
            ):
                await session.execute(delete(model))
    yield PostgresOrderRepository(factory), factory
    await engine.dispose()


async def test_claim_intent_allows_one_worker(
    order_repository: tuple[
        PostgresOrderRepository,
        async_sessionmaker[AsyncSession],
    ],
) -> None:
    repository, _ = order_repository
    await _save_intent(repository)

    results = await asyncio.gather(
        repository.claim_intent(
            "candidate-1", "worker-1", NOW, NOW + timedelta(minutes=1)
        ),
        repository.claim_intent(
            "candidate-1", "worker-2", NOW, NOW + timedelta(minutes=1)
        ),
    )

    assert sorted(results) == [False, True]


async def test_save_exchange_order_event_is_idempotent(
    order_repository: tuple[
        PostgresOrderRepository,
        async_sessionmaker[AsyncSession],
    ],
) -> None:
    repository, factory = order_repository
    await _save_intent(repository)
    await repository.save_planned_order(_plan())
    event = ExchangeOrderEvent(
        event_id="event-1",
        client_order_id="cml_12345678901234567890123456789012",
        state=ExchangeOrderState.ACKNOWLEDGED,
        occurred_at=NOW + timedelta(seconds=1),
        exchange_order_id="12345",
        details={"status": "NEW"},
    )

    assert await repository.append_order_event(event) is True
    assert await repository.append_order_event(event) is False

    async with factory() as session:
        count = await session.scalar(select(func.count(ExchangeOrderEventRow.event_id)))
    assert count == 1


async def test_prepare_submission_journals_intent_order_and_event_together(
    order_repository: tuple[
        PostgresOrderRepository,
        async_sessionmaker[AsyncSession],
    ],
) -> None:
    repository, factory = order_repository
    prepared = await repository.prepare_submission(
        intent=_intent(),
        evaluation=RiskEvaluation(
            evaluation_id="evaluation-1",
            candidate_id="candidate-1",
            decision=RiskDecision.APPROVED,
            reason="approved",
            evaluated_at=NOW,
            details={},
        ),
        plan=_plan(),
        prepared_at=NOW + timedelta(milliseconds=2),
    )

    async with factory() as session:
        intent_state = await session.scalar(
            select(OrderIntentExecutionRow.state).where(
                OrderIntentExecutionRow.intent_id == "candidate-1"
            )
        )
        order_state = await session.scalar(
            select(ExchangeOrderRow.state).where(
                ExchangeOrderRow.client_order_id
                == "cml_12345678901234567890123456789012"
            )
        )
        event_states = (
            await session.scalars(
                select(ExchangeOrderEventRow.state).where(
                    ExchangeOrderEventRow.client_order_id
                    == "cml_12345678901234567890123456789012"
                )
            )
        ).all()

    assert prepared.submitting_event.state is ExchangeOrderState.SUBMITTING
    assert intent_state == ExchangeOrderState.SUBMITTING.value
    assert order_state == ExchangeOrderState.SUBMITTING.value
    assert event_states == [ExchangeOrderState.SUBMITTING.value]


async def test_concurrent_prepare_grants_only_one_submission(order_repository) -> None:
    repository, factory = order_repository
    evaluation = RiskEvaluation(
        evaluation_id="evaluation-1",
        candidate_id="candidate-1",
        decision=RiskDecision.APPROVED,
        reason="approved",
        evaluated_at=NOW,
        details={},
    )
    results = await asyncio.gather(
        *(
            repository.prepare_submission(
                intent=_intent(),
                evaluation=evaluation,
                plan=_plan(),
                prepared_at=NOW + timedelta(seconds=index),
            )
            for index in range(2)
        )
    )
    assert sum(result is not None for result in results) == 1
    await repository.append_order_event(
        ExchangeOrderEvent(
            event_id="original-filled",
            client_order_id=_plan().client_order_id,
            state=ExchangeOrderState.FILLED,
            occurred_at=NOW + timedelta(seconds=3),
            exchange_order_id="original-order",
            details={},
        )
    )
    restarted = PostgresOrderRepository(factory)
    assert (
        await restarted.prepare_submission(
            intent=_intent(),
            evaluation=evaluation,
            plan=_plan(),
            prepared_at=NOW + timedelta(hours=8),
        )
        is None
    )
    async with factory() as session:
        row = await session.get(ExchangeOrderRow, _plan().client_order_id)
        assert row.state == ExchangeOrderState.FILLED.value
        assert row.exchange_order_id == "original-order"
        count = await session.scalar(
            select(func.count())
            .select_from(ExchangeOrderEventRow)
            .where(ExchangeOrderEventRow.state == ExchangeOrderState.SUBMITTING.value)
        )
        assert count == 1


async def test_late_ack_cannot_reopen_filled_order(order_repository) -> None:
    repository, factory = order_repository
    await _save_intent(repository)
    await repository.save_planned_order(_plan())
    for event_id, state, seconds in (
        ("filled-first", ExchangeOrderState.FILLED, 3),
        ("late-ack", ExchangeOrderState.ACKNOWLEDGED, 1),
    ):
        await repository.append_order_event(
            ExchangeOrderEvent(
                event_id=event_id,
                client_order_id=_plan().client_order_id,
                state=state,
                occurred_at=NOW + timedelta(seconds=seconds),
                exchange_order_id="original-order",
                details={},
            )
        )
    async with factory() as session:
        row = await session.get(ExchangeOrderRow, _plan().client_order_id)
        assert row.state == ExchangeOrderState.FILLED.value
        assert row.updated_at == NOW + timedelta(seconds=3)


async def test_late_ack_cannot_reopen_canceled_order(order_repository) -> None:
    repository, factory = order_repository
    await _save_intent(repository)
    await repository.save_planned_order(_plan())
    await repository.append_order_event(
        ExchangeOrderEvent(
            event_id="canceled-first",
            client_order_id=_plan().client_order_id,
            state=ExchangeOrderState.CANCELED,
            occurred_at=NOW + timedelta(seconds=3),
            exchange_order_id="original-order",
            details={},
        )
    )
    await repository.append_order_event(
        ExchangeOrderEvent(
            event_id="late-partial",
            client_order_id=_plan().client_order_id,
            state=ExchangeOrderState.PARTIALLY_FILLED,
            occurred_at=NOW + timedelta(seconds=4),
            exchange_order_id="original-order",
            details={},
        )
    )
    async with factory() as session:
        row = await session.get(ExchangeOrderRow, _plan().client_order_id)
        assert row.state == ExchangeOrderState.CANCELED.value
        assert row.updated_at == NOW + timedelta(seconds=3)


async def test_conflicting_exchange_identity_is_journaled_without_overwrite(
    order_repository,
) -> None:
    repository, factory = order_repository
    await _save_intent(repository)
    await repository.save_planned_order(_plan())
    for index, exchange_id in enumerate(("original-order", "different-order")):
        await repository.append_order_event(
            ExchangeOrderEvent(
                event_id=f"identity-{index}",
                client_order_id=_plan().client_order_id,
                state=ExchangeOrderState.FILLED,
                occurred_at=NOW + timedelta(seconds=index),
                exchange_order_id=exchange_id,
                details={"executed_quantity": str(index + 1)},
            )
        )
    async with factory() as session:
        row = await session.get(ExchangeOrderRow, _plan().client_order_id)
        assert row.exchange_order_id == "original-order"
        assert row.executed_quantity == Decimal("1")
        assert (
            await session.scalar(
                select(func.count()).select_from(ExchangeOrderEventRow)
            )
            == 2
        )


async def test_exit_batch_binding_loads_from_durable_intent(order_repository) -> None:
    from dataclasses import replace

    from crypto_momentum_lab.live_rollout.postgres_runtime import _load_exit_batch_ids

    repository, factory = order_repository
    intent = replace(_intent(), reduce_only=True, features={"batch_id": "old-batch"})
    await repository.save_approved_intent(
        intent,
        RiskEvaluation(
            evaluation_id="evaluation-1",
            candidate_id=intent.candidate_id,
            decision=RiskDecision.APPROVED,
            reason="approved",
            evaluated_at=NOW,
            details={},
        ),
    )
    plan = replace(_plan(), reduce_only=True, side="SELL")
    await repository.save_planned_order(plan)
    async with factory() as session:
        orders = (await session.scalars(select(ExchangeOrderRow))).all()
    assert await _load_exit_batch_ids(factory, orders) == {
        plan.client_order_id: "old-batch"
    }


async def test_load_unresolved_orders_returns_unknown_state(
    order_repository: tuple[
        PostgresOrderRepository,
        async_sessionmaker[AsyncSession],
    ],
) -> None:
    repository, _ = order_repository
    await _save_intent(repository)
    await repository.save_planned_order(_plan())
    await repository.append_order_event(
        ExchangeOrderEvent(
            event_id="event-unknown",
            client_order_id="cml_12345678901234567890123456789012",
            state=ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
            occurred_at=NOW + timedelta(seconds=1),
            exchange_order_id=None,
            details={"cause": "submit_timeout"},
        )
    )

    unresolved = await repository.load_unresolved_orders()

    assert len(unresolved) == 1
    assert unresolved[0].state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION


async def _save_intent(repository: PostgresOrderRepository) -> None:
    await repository.save_approved_intent(
        _intent(),
        RiskEvaluation(
            evaluation_id="evaluation-1",
            candidate_id="candidate-1",
            decision=RiskDecision.APPROVED,
            reason="approved",
            evaluated_at=NOW,
            details={},
        ),
    )


def _intent() -> OrderIntentCandidate:
    return OrderIntentCandidate(
        candidate_id="candidate-1",
        signal_id="signal-1",
        run_id="run-1",
        strategy_name="compression_breakout",
        strategy_version="v1",
        config_hash="a" * 64,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        limit_price=None,
        desired_notional=Decimal("100"),
        reduce_only=False,
        expires_at=NOW + timedelta(seconds=30),
        created_at=NOW,
        reason="test",
        features={},
    )


def _plan() -> OrderExecutionPlan:
    return OrderExecutionPlan(
        intent_id="candidate-1",
        run_id="run-1",
        client_order_id="cml_12345678901234567890123456789012",
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("0.001"),
        price=None,
        reduce_only=False,
        created_at=NOW,
        quantized=True,
    )
