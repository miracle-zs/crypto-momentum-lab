import asyncio
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    ExchangeOrderFill,
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.risk import RiskDecision, RiskEvaluation
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategySide,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderPreSubmissionError,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ExchangeFillRow,
    ExchangeOrderEventRow,
    ExchangeOrderRow,
    ExecutionCommandRow,
    ExecutionReconciliationEventRow,
    ExitEpisodeReservationRow,
    LiveExposureClaimRow,
    LiveSessionTransitionRow,
    OrderIntentClaimRow,
    OrderIntentExecutionRow,
    TradingLeaseRow,
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
                ExitEpisodeReservationRow,
                LiveExposureClaimRow,
                OrderIntentClaimRow,
                OrderIntentExecutionRow,
                LiveSessionTransitionRow,
                TradingLeaseRow,
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


async def test_external_order_adoption_uses_normal_cancel_event_journal(
    order_repository,
) -> None:
    repository, factory = order_repository
    plan = _plan()
    await repository.adopt_external_order_for_cancellation(
        plan,
        exchange_order_id="external-order",
        observed_at=NOW,
    )

    persisted = await repository.load_order(plan.client_order_id)
    assert persisted is not None
    assert persisted.state is ExchangeOrderState.SUBMITTED
    assert persisted.exchange_order_id == "external-order"

    assert await repository.append_order_event(
        ExchangeOrderEvent(
            event_id="external-canceled",
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.CANCELED,
            occurred_at=NOW + timedelta(seconds=1),
            exchange_order_id="external-order",
            details={},
        )
    )
    async with factory() as session:
        row = await session.get(ExchangeOrderRow, plan.client_order_id)
    assert row is not None
    assert row.state == ExchangeOrderState.CANCELED.value


async def test_save_fill_deduplicates_exchange_trade_identity(
    order_repository: tuple[
        PostgresOrderRepository,
        async_sessionmaker[AsyncSession],
    ],
) -> None:
    repository, factory = order_repository
    await _save_intent(repository)
    await repository.save_planned_order(_plan())
    fill = ExchangeOrderFill(
        fill_id="fill-1",
        client_order_id=_plan().client_order_id,
        exchange_trade_id="trade-1",
        price=Decimal("100"),
        quantity=Decimal("0.003"),
        fee=Decimal("0.01"),
        fee_asset="USDT",
        filled_at=NOW + timedelta(seconds=1),
        details={},
    )
    duplicate = replace(fill, fill_id="fill-2")

    assert await repository.save_fill(fill) is True
    assert await repository.save_fill(duplicate) is False

    async with factory() as session:
        count = await session.scalar(select(func.count()).select_from(ExchangeFillRow))
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


async def test_prepare_reuses_active_reduce_only_intent_after_reprice(
    order_repository,
) -> None:
    repository, factory = order_repository
    intent = replace(
        _intent(),
        reduce_only=True,
        features={
            "position_side": "LONG",
            "opened_at": NOW.isoformat(),
        },
    )
    evaluation = _evaluation(intent, "evaluation-exit-1")
    first_plan = replace(
        _plan(),
        intent_id=intent.candidate_id,
        client_order_id="cml_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        side="SELL",
        order_type="LIMIT",
        quantity=Decimal("0.001"),
        price=Decimal("100"),
        reduce_only=True,
        position_side=FuturesPositionSide.LONG,
        time_in_force="GTC",
        expires_at=NOW + timedelta(minutes=1),
    )

    assert (
        await repository.prepare_submission(
            intent=intent,
            evaluation=evaluation,
            plan=first_plan,
            prepared_at=NOW + timedelta(seconds=1),
        )
        is not None
    )
    await repository.append_order_event(
        ExchangeOrderEvent(
            event_id="exit-ack",
            client_order_id=first_plan.client_order_id,
            state=ExchangeOrderState.ACKNOWLEDGED,
            occurred_at=NOW + timedelta(seconds=2),
            exchange_order_id="exchange-exit-1",
            details={},
        )
    )

    repriced_plan = replace(
        first_plan,
        order_type="MARKET",
        price=None,
        time_in_force=None,
        expires_at=None,
        created_at=NOW + timedelta(seconds=3),
    )
    assert (
        await repository.prepare_submission(
            intent=intent,
            evaluation=evaluation,
            plan=repriced_plan,
            prepared_at=NOW + timedelta(seconds=3),
        )
        is None
    )

    async with factory() as session:
        row = await session.get(
            ExchangeOrderRow,
            first_plan.client_order_id,
        )
    assert row is not None
    assert row.state == ExchangeOrderState.ACKNOWLEDGED.value
    assert row.price == Decimal("100")
    assert row.exchange_order_id == "exchange-exit-1"


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


async def test_live_entry_exposure_claim_is_atomic_and_released_on_terminal(
    order_repository,
) -> None:
    repository, factory = order_repository
    await _save_live_lease(factory)
    first_intent = _intent()
    first_evaluation = _evaluation(first_intent, "evaluation-entry-1")
    first_plan = _plan()
    second_intent = replace(first_intent, candidate_id="candidate-2")
    second_evaluation = _evaluation(second_intent, "evaluation-entry-2")
    second_plan = replace(
        first_plan,
        intent_id="candidate-2",
        client_order_id="cml_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        symbol="ETHUSDT",
    )
    claim_kwargs = {
        "environment": "live",
        "account_label": "primary",
        "strategy_name": "compression_breakout",
        "required_lease_owner": "worker-1",
        "required_lease_id": "lease-test",
        "required_code_generation": "test-generation",
        "max_open_positions": 5,
        "max_daily_loss": Decimal("1000"),
        "max_gross_exposure": Decimal("150"),
        "current_daily_pnl": Decimal("0"),
        "current_gross_exposure": Decimal("0"),
        "open_position_symbols": frozenset(),
        "exposure_notional": Decimal("100"),
    }

    first = await repository.prepare_submission(
        intent=first_intent,
        evaluation=first_evaluation,
        plan=first_plan,
        prepared_at=NOW + timedelta(seconds=1),
        **claim_kwargs,
    )
    assert first is not None
    with pytest.raises(OrderPreSubmissionError, match="gross exposure"):
        await repository.prepare_submission(
            intent=second_intent,
            evaluation=second_evaluation,
            plan=second_plan,
            prepared_at=NOW + timedelta(seconds=2),
            **claim_kwargs,
        )

    await repository.append_order_event(
        ExchangeOrderEvent(
            event_id="entry-canceled",
            client_order_id=first_plan.client_order_id,
            state=ExchangeOrderState.CANCELED,
            occurred_at=NOW + timedelta(seconds=3),
            exchange_order_id="entry-order",
            details={},
        )
    )
    second = await repository.prepare_submission(
        intent=second_intent,
        evaluation=second_evaluation,
        plan=second_plan,
        prepared_at=NOW + timedelta(seconds=4),
        **claim_kwargs,
    )
    assert second is not None
    async with factory() as session:
        claim = await session.get(LiveExposureClaimRow, second_plan.intent_id)
    assert claim is not None
    assert claim.active is True


async def test_live_submission_rejects_stale_code_generation(
    order_repository,
) -> None:
    repository, factory = order_repository
    await _save_live_lease(factory)

    with pytest.raises(
        OrderPreSubmissionError,
        match="version fencing",
    ):
        await repository.prepare_submission(
            intent=_intent(),
            evaluation=_evaluation(_intent(), "evaluation-stale-generation"),
            plan=_plan(),
            prepared_at=NOW + timedelta(seconds=1),
            environment="live",
            account_label="primary",
            strategy_name="compression_breakout",
            required_lease_owner="worker-1",
            required_lease_id="lease-test",
            required_code_generation="stale-generation",
        )


async def test_live_submission_rejects_draining_session(
    order_repository,
) -> None:
    repository, factory = order_repository
    await _save_live_lease(factory)
    async with factory() as session:
        async with session.begin():
            session.add(
                LiveSessionTransitionRow(
                    transition_id="transition-draining-session",
                    session_id="live-session-fence",
                    state="draining",
                    occurred_at=NOW,
                    operator="operator",
                    strategy_config_hash="strategy-hash",
                    risk_config_hash="risk-hash",
                    reason="operator_disabled_new_entries",
                    details={},
                )
            )

    with pytest.raises(OrderPreSubmissionError, match="session entries"):
        await repository.prepare_submission(
            intent=_intent(),
            evaluation=_evaluation(_intent(), "evaluation-draining-session"),
            plan=_plan(),
            prepared_at=NOW + timedelta(seconds=1),
            environment="live",
            account_label="primary",
            strategy_name="compression_breakout",
            required_lease_owner="worker-1",
            required_lease_id="lease-test",
            required_code_generation="test-generation",
            required_session_id="live-session-fence",
        )


async def test_live_exit_episode_reservation_survives_rolling_workers(
    order_repository,
) -> None:
    repository, factory = order_repository
    await _save_live_lease(factory)
    first_intent = replace(
        _intent(),
        reduce_only=True,
        features={
            "position_side": "LONG",
            "opened_at": NOW.isoformat(),
            "batch_id": "episode-1",
        },
    )
    second_intent = replace(first_intent, candidate_id="candidate-2")
    first_plan = replace(
        _plan(),
        reduce_only=True,
        position_side=FuturesPositionSide.LONG,
        side="SELL",
        client_order_id="cml_cccccccccccccccccccccccccccccccc",
    )
    second_plan = replace(
        first_plan,
        intent_id="candidate-2",
        client_order_id="cml_dddddddddddddddddddddddddddddddd",
    )
    first_evaluation = _evaluation(first_intent, "evaluation-exit-1")
    second_evaluation = _evaluation(second_intent, "evaluation-exit-2")
    fencing_kwargs = {
        "environment": "live",
        "account_label": "primary",
        "strategy_name": "compression_breakout",
        "required_lease_owner": "worker-1",
        "required_lease_id": "lease-test",
        "required_code_generation": "test-generation",
    }
    results = await asyncio.gather(
        repository.prepare_submission(
            intent=first_intent,
            evaluation=first_evaluation,
            plan=first_plan,
            prepared_at=NOW + timedelta(seconds=1),
            **fencing_kwargs,
        ),
        repository.prepare_submission(
            intent=second_intent,
            evaluation=second_evaluation,
            plan=second_plan,
            prepared_at=NOW + timedelta(seconds=2),
            **fencing_kwargs,
        ),
    )
    assert sum(result is not None for result in results) == 1
    winner_plan = first_plan if results[0] is not None else second_plan
    loser_intent = second_intent if results[0] is not None else first_intent
    loser_evaluation = second_evaluation if results[0] is not None else first_evaluation
    loser_plan = second_plan if results[0] is not None else first_plan

    await repository.append_order_event(
        ExchangeOrderEvent(
            event_id="exit-canceled",
            client_order_id=winner_plan.client_order_id,
            state=ExchangeOrderState.CANCELED,
            occurred_at=NOW + timedelta(seconds=3),
            exchange_order_id="exit-order",
            details={},
        )
    )
    retry = await repository.prepare_submission(
        intent=loser_intent,
        evaluation=loser_evaluation,
        plan=loser_plan,
        prepared_at=NOW + timedelta(seconds=4),
        **fencing_kwargs,
    )
    assert retry is not None
    async with factory() as session:
        reservation = await session.scalar(
            select(ExitEpisodeReservationRow).where(
                ExitEpisodeReservationRow.intent_id == loser_plan.intent_id
            )
        )
    assert reservation is not None
    assert reservation.active is True


async def test_prepare_rejects_client_order_id_reused_by_another_intent(
    order_repository,
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
    assert prepared is not None

    conflicting_intent = replace(_intent(), candidate_id="candidate-2")
    conflicting_plan = replace(_plan(), intent_id="candidate-2")
    with pytest.raises(ValueError, match="client order ID"):
        await repository.prepare_submission(
            intent=conflicting_intent,
            evaluation=RiskEvaluation(
                evaluation_id="evaluation-2",
                candidate_id="candidate-2",
                decision=RiskDecision.APPROVED,
                reason="approved",
                evaluated_at=NOW,
                details={},
            ),
            plan=conflicting_plan,
            prepared_at=NOW + timedelta(seconds=1),
        )

    async with factory() as session:
        assert (
            await session.scalar(
                select(OrderIntentExecutionRow).where(
                    OrderIntentExecutionRow.intent_id == "candidate-2"
                )
            )
            is None
        )
        existing = await session.get(
            ExchangeOrderRow,
            _plan().client_order_id,
        )
        assert existing is not None
        assert existing.intent_id == "candidate-1"


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


async def _save_live_lease(factory: async_sessionmaker[AsyncSession]) -> None:
    async with factory() as session:
        async with session.begin():
            session.add(
                TradingLeaseRow(
                    lease_id="lease-test",
                    environment="live",
                    account_label="primary",
                    strategy_name="compression_breakout",
                    owner="worker-1",
                    code_generation="test-generation",
                    state="active",
                    acquired_at=NOW,
                    expires_at=NOW + timedelta(hours=1),
                )
            )


def _evaluation(
    intent: OrderIntentCandidate,
    evaluation_id: str,
) -> RiskEvaluation:
    return RiskEvaluation(
        evaluation_id=evaluation_id,
        candidate_id=intent.candidate_id,
        decision=RiskDecision.APPROVED,
        reason="approved",
        evaluated_at=NOW,
        details={},
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
