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
    )
