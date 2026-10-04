import asyncio
import os
import sys
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderFill,
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderPreSubmissionError,
)
from crypto_momentum_lab.domain.risk import RiskDecision, RiskEvaluation
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategySide,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountFillEventRow,
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
from crypto_momentum_lab.persistence.postgres.order_adoption_repository import (
    PostgresOrderAdoptionRepository,
)
from crypto_momentum_lab.persistence.postgres.order_event_repository import (
    PostgresOrderEventRepository,
)
from crypto_momentum_lab.persistence.postgres.order_read_repository import (
    PostgresOrderReadRepository,
)
from crypto_momentum_lab.persistence.postgres.order_submission_repository import (
    PostgresOrderSubmissionRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)
from tests.fixtures.order_rows import (
    OrderRows,
)

NOW = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)


async def _prepare_submission(repository, **values):
    from crypto_momentum_lab.domain.execution.order_submission import (
        OrderAlreadyPreparedError,
    )

    try:
        async with repository._session_factory() as session:
            async with session.begin():
                return await repository.prepare_submission_in_session(session, **values)
    except OrderAlreadyPreparedError:
        return None


@pytest.mark.parametrize(
    "price_source",
    [
        "event",
        "fills",
        "missing",
        "wrong_quantity",
        "zero_cancel",
        "account_fills",
        "nested_account_fills",
        "wrong_account",
    ],
)
async def test_terminal_receipt_requires_exact_priced_execution_facts(
    order_repository, price_source
):
    plans, _, reads, events, submissions, factory = order_repository
    await _save_intent(submissions)
    plan = _plan()
    await plans.seed_order(plan)
    quantity = Decimal(0) if price_source == "zero_cancel" else plan.quantity
    details = {"executed_quantity": str(quantity)}
    if price_source == "event":
        details["average_price"] = "100"
    elif price_source == "wrong_quantity":
        await events.append_order_event(
            ExchangeOrderEvent(
                "priced-partial",
                plan.client_order_id,
                ExchangeOrderState.PARTIALLY_FILLED,
                NOW,
                "12345",
                {"executed_quantity": str(quantity / 2), "average_price": "100"},
            )
        )
    if price_source == "fills":
        await events.save_fill(
            ExchangeOrderFill(
                "fill-receipt",
                plan.client_order_id,
                "real-trade",
                Decimal(100),
                quantity,
                Decimal(0),
                "USDT",
                NOW,
                {},
            )
        )
    if price_source in {"account_fills", "nested_account_fills", "wrong_account"}:
        account = f"test-receipt-{uuid4().hex}"
        async with factory.begin() as session:
            session.add(
                ExecutionCommandRow(
                    command_id=plan.client_order_id,
                    client_order_id=plan.client_order_id,
                    command="entry",
                    status="unknown",
                    requested_at=NOW,
                    details={
                        "scope": {
                            "environment": "live",
                            "account_label": account,
                            "symbol": plan.symbol,
                            "position_side": plan.position_side.value,
                        }
                    },
                )
            )
            # Same exchange ID in another account must not influence the receipt.
            session.add(
                AccountFillEventRow(
                    environment="live",
                    account_label=account + "-other",
                    symbol=plan.symbol,
                    trade_id="other-account",
                    order_id="12345",
                    side=plan.side,
                    price=Decimal(200),
                    quantity=quantity,
                    realized_pnl=Decimal(0),
                    fee=Decimal(0),
                    fee_asset="USDT",
                    trade_at=NOW,
                    raw_payload={"positionSide": plan.position_side.value},
                )
            )
            if price_source in {"account_fills", "nested_account_fills"}:
                session.add(
                    AccountFillEventRow(
                        environment="live",
                        account_label=account,
                        symbol=plan.symbol,
                        trade_id="actual-trade",
                        order_id="12345",
                        side=plan.side,
                        price=Decimal(100),
                        quantity=quantity,
                        realized_pnl=Decimal(0),
                        fee=Decimal(0),
                        fee_asset="USDT",
                        trade_at=NOW,
                        raw_payload={"event": {"o": {"ps": plan.position_side.value}}}
                        if price_source == "nested_account_fills"
                        else {"positionSide": plan.position_side.value},
                    )
                )
    state = (
        ExchangeOrderState.CANCELED
        if price_source == "zero_cancel"
        else ExchangeOrderState.FILLED
    )
    await events.append_order_event(
        ExchangeOrderEvent(
            "terminal",
            plan.client_order_id,
            state,
            NOW + timedelta(seconds=1),
            None if price_source == "zero_cancel" else "12345",
            details,
        )
    )
    order = await reads.load_order(plan.client_order_id)
    assert not await reads.load_unresolved_orders(plan.run_id)
    if price_source in {"missing", "wrong_quantity", "wrong_account"}:
        assert order.terminal_receipt is None
    else:
        receipt = order.terminal_receipt
        assert receipt.state == state
        assert receipt.executed_quantity == quantity
        assert receipt.average_price == (Decimal(0) if quantity == 0 else Decimal(100))


async def test_restored_unknown_with_terminal_order_replays_through_real_postgres_book(
    order_repository,
):
    from crypto_momentum_lab.domain.account import AccountFillEvent
    from crypto_momentum_lab.domain.execution.command_models import (
        DispatchState,
        ExecutionScope,
    )
    from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
    from crypto_momentum_lab.domain.execution.trade_command import (
        TradeCommand,
        TradeCommandType,
    )
    from crypto_momentum_lab.execution_account.orders.coordinator import (
        OrderExecutionCoordinator,
    )
    from crypto_momentum_lab.live_rollout.command_receipt_recovery import (
        recover_restored_commands,
    )
    from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
        ExecutionBookHeadRow,
    )
    from tests.integration.persistence.test_authority_book_transactions import _book

    plans, _, reads, events, submissions, factory = order_repository
    await _save_intent(submissions)
    plan = _plan()
    await plans.seed_order(plan)
    await events.append_order_event(
        ExchangeOrderEvent(
            "terminal",
            plan.client_order_id,
            ExchangeOrderState.FILLED,
            NOW,
            "12345",
            {"executed_quantity": str(plan.quantity), "average_price": "100"},
        )
    )
    account = f"test-replay-{uuid4().hex}"
    scope = ExecutionScope("live", account, plan.symbol, plan.position_side)
    book = _book(factory)
    await book.restore(account_label=account)
    fill = AccountFillEvent(
        "live",
        account,
        plan.symbol,
        "real-trade",
        "12345",
        "BUY",
        Decimal(100),
        plan.quantity,
        Decimal(0),
        Decimal(0),
        "USDT",
        NOW,
        {"positionSide": plan.position_side.value},
    )
    await book.observe(
        ExecutionEvidence(
            "true-fill", scope, NOW, fill=fill, stream_id="legacy", stream_epoch="one"
        )
    )
    book.register_prepared_command(
        TradeCommand(
            plan.client_order_id,
            scope.to_position_key(),
            TradeCommandType.ENTRY,
            StrategySide.LONG,
            EntryType.LIMIT,
            plan.quantity,
            limit_price=plan.price,
            created_at=NOW,
        ),
        scope,
    )
    await book.mark_dispatching(plan.client_order_id)
    async with factory.begin() as session:
        await session.execute(
            delete(ExecutionBookHeadRow).where(
                ExecutionBookHeadRow.account_label == account
            )
        )
    restored = _book(factory)
    await restored.restore(account_label=account)
    assert restored.command_requires_recovery(plan.client_order_id)
    coordinator = OrderExecutionCoordinator(
        backend=object(),
        account_label=account,
        environment="live",
        execution_book=restored,
    )
    try:
        assert not (
            await recover_restored_commands(
                book=restored, coordinator=coordinator, orders=reads
            )
        )[0]
        assert restored.get_outbox(plan.client_order_id).state == DispatchState.TERMINAL
        restarted = _book(factory)
        await restarted.restore(account_label=account)
        assert not restarted.command_requires_recovery(plan.client_order_id)
        assert (await restarted.read(scope)).total_quantity == plan.quantity
    finally:
        await coordinator.aclose()


@pytest.mark.parametrize("history", ["complete", "partial", "foreign_account"])
async def test_old_trade_settlement_is_independent_of_current_epoch_position_facts(
    order_repository, history
):
    from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
    from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
    from crypto_momentum_lab.domain.execution.trade_command import (
        TradeCommand,
        TradeCommandType,
    )
    from crypto_momentum_lab.execution_account.orders.coordinator import (
        OrderExecutionCoordinator,
    )
    from crypto_momentum_lab.live_rollout.command_receipt_recovery import (
        recover_restored_commands,
    )
    from tests.integration.persistence.test_authority_book_transactions import _book

    plans, _, reads, events, submissions, factory = order_repository
    await _save_intent(submissions)
    plan = _plan()
    await plans.seed_order(plan)
    await events.append_order_event(
        ExchangeOrderEvent(
            "terminal",
            plan.client_order_id,
            ExchangeOrderState.FILLED,
            NOW,
            "12345",
            {"executed_quantity": str(plan.quantity), "average_price": "100"},
        )
    )
    account = f"test-history-{uuid4().hex}"
    scope = ExecutionScope("live", account, plan.symbol, plan.position_side)
    book = _book(factory)
    await book.restore(account_label=account)
    # New stream baseline intentionally has no previous epoch's trade prefix.
    await book.observe(
        ExecutionEvidence(
            "current-stream", scope, NOW, stream_id="hub", stream_epoch="new"
        )
    )
    async with factory.begin() as session:
        session.add(
            AccountFillEventRow(
                environment="live",
                account_label=account
                if history != "foreign_account"
                else account + "-other",
                symbol=plan.symbol,
                trade_id="old-real-trade",
                order_id="12345",
                side=plan.side,
                quantity=plan.quantity if history != "partial" else plan.quantity / 2,
                price=Decimal(100),
                realized_pnl=Decimal(0),
                fee=Decimal(0),
                fee_asset="USDT",
                trade_at=NOW - timedelta(seconds=1),
                raw_payload={"positionSide": plan.position_side.value},
            )
        )
    book.register_prepared_command(
        TradeCommand(
            plan.client_order_id,
            scope.to_position_key(),
            TradeCommandType.ENTRY,
            StrategySide.LONG,
            EntryType.LIMIT,
            plan.quantity,
            limit_price=plan.price,
            created_at=NOW,
        ),
        scope,
    )
    await book.mark_dispatching(plan.client_order_id)
    restored = _book(factory)
    await restored.restore(account_label=account)
    coordinator = OrderExecutionCoordinator(
        backend=object(),
        account_label=account,
        environment="live",
        execution_book=restored,
    )
    try:
        pending = history != "complete"
        assert (
            await recover_restored_commands(
                book=restored, coordinator=coordinator, orders=reads
            )
        )[0] is pending
        assert restored.command_requires_recovery(plan.client_order_id) is pending
        assert (await restored.read(scope)).total_quantity == 0
        assert (
            restored._journals[scope.to_position_key().canonical_id].read_cut().fills
            == ()
        )
        # The head and command transition commit together and survive process loss.
        restarted = _book(factory)
        await restarted.restore(account_label=account)
        assert restarted.command_requires_recovery(plan.client_order_id) is pending
        assert (await restarted.read(scope)).total_quantity == 0
    finally:
        await coordinator.aclose()


@pytest.mark.parametrize(
    "terminal", [ExchangeOrderState.FILLED, ExchangeOrderState.CANCELED]
)
async def test_exchange_terminal_fact_precedes_later_local_ack_clock(
    order_repository, terminal
):
    plans, _, _, events, submissions, factory = order_repository
    await _save_intent(submissions)
    plan = _plan()
    await plans.seed_order(plan)
    await events.append_order_event(
        ExchangeOrderEvent(
            "local-ack",
            plan.client_order_id,
            ExchangeOrderState.ACKNOWLEDGED,
            NOW + timedelta(seconds=3, microseconds=348681),
            "12345",
            {},
        )
    )
    await events.append_order_event(
        ExchangeOrderEvent(
            "exchange-terminal",
            plan.client_order_id,
            terminal,
            NOW + timedelta(seconds=3, microseconds=345000),
            "12345",
            {},
        )
    )
    async with factory() as session:
        row = await session.get(ExchangeOrderRow, plan.client_order_id)
        assert row.state == terminal.value
        assert row.updated_at == NOW + timedelta(seconds=3, microseconds=348681)


@pytest.fixture
async def order_repository(
    async_database_url: str,
) -> AsyncIterator[
    tuple[
        OrderRows,
        PostgresOrderAdoptionRepository,
        PostgresOrderReadRepository,
        PostgresOrderEventRepository,
        PostgresOrderSubmissionRepository,
        async_sessionmaker[AsyncSession],
    ]
]:
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
    yield (
        OrderRows(factory),
        PostgresOrderAdoptionRepository(factory),
        PostgresOrderReadRepository(factory),
        PostgresOrderEventRepository(factory),
        PostgresOrderSubmissionRepository(factory),
        factory,
    )
    await engine.dispose()


async def test_save_exchange_order_event_is_idempotent(
    order_repository: tuple[
        OrderRows,
        PostgresOrderAdoptionRepository,
        PostgresOrderReadRepository,
        PostgresOrderEventRepository,
        PostgresOrderSubmissionRepository,
        async_sessionmaker[AsyncSession],
    ],
) -> None:
    repository, adoption, reads, events, submissions, factory = order_repository
    await _save_intent(submissions)
    await repository.seed_order(_plan())
    event = ExchangeOrderEvent(
        event_id="event-1",
        client_order_id="cml_12345678901234567890123456789012",
        state=ExchangeOrderState.ACKNOWLEDGED,
        occurred_at=NOW + timedelta(seconds=1),
        exchange_order_id="12345",
        details={"status": "NEW"},
    )

    assert await events.append_order_event(event) is True
    assert await events.append_order_event(event) is False

    async with factory() as session:
        count = await session.scalar(select(func.count(ExchangeOrderEventRow.event_id)))
    assert count == 1


async def test_external_order_adoption_uses_normal_cancel_event_journal(
    order_repository,
) -> None:
    repository, adoption, reads, events, submissions, factory = order_repository
    plan = _plan()
    await adoption.adopt_external_order_for_cancellation(
        plan,
        exchange_order_id="external-order",
        observed_at=NOW,
    )

    persisted = await reads.load_order(plan.client_order_id)
    assert persisted is not None
    assert persisted.state is ExchangeOrderState.SUBMITTED
    assert persisted.exchange_order_id == "external-order"

    assert await events.append_order_event(
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
        OrderRows,
        PostgresOrderAdoptionRepository,
        PostgresOrderReadRepository,
        PostgresOrderEventRepository,
        PostgresOrderSubmissionRepository,
        async_sessionmaker[AsyncSession],
    ],
) -> None:
    repository, adoption, reads, events, submissions, factory = order_repository
    await _save_intent(submissions)
    await repository.seed_order(_plan())
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

    assert await events.save_fill(fill) is True
    assert await events.save_fill(duplicate) is False

    async with factory() as session:
        count = await session.scalar(select(func.count()).select_from(ExchangeFillRow))
    assert count == 1


@pytest.mark.parametrize("previously_approved", [False, True])
async def test_prepare_submission_journals_intent_order_and_event_together(
    order_repository: tuple[
        OrderRows,
        PostgresOrderAdoptionRepository,
        PostgresOrderReadRepository,
        PostgresOrderEventRepository,
        PostgresOrderSubmissionRepository,
        async_sessionmaker[AsyncSession],
    ],
    previously_approved: bool,
) -> None:
    repository, adoption, reads, events, submissions, factory = order_repository
    if previously_approved:
        await _save_intent(submissions)
    prepared = await _prepare_submission(
        submissions,
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
    repository, adoption, reads, events, submissions, factory = order_repository
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
        await _prepare_submission(
            submissions,
            intent=intent,
            evaluation=evaluation,
            plan=first_plan,
            prepared_at=NOW + timedelta(seconds=1),
        )
        is not None
    )
    await events.append_order_event(
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
        await _prepare_submission(
            submissions,
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
    repository, adoption, reads, events, submissions, factory = order_repository
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
            _prepare_submission(
                submissions,
                intent=_intent(),
                evaluation=evaluation,
                plan=_plan(),
                prepared_at=NOW + timedelta(seconds=index),
            )
            for index in range(2)
        )
    )
    assert sum(result is not None for result in results) == 1
    await events.append_order_event(
        ExchangeOrderEvent(
            event_id="original-filled",
            client_order_id=_plan().client_order_id,
            state=ExchangeOrderState.FILLED,
            occurred_at=NOW + timedelta(seconds=3),
            exchange_order_id="original-order",
            details={},
        )
    )
    restarted = PostgresOrderSubmissionRepository(factory)
    assert (
        await _prepare_submission(
            restarted,
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
    repository, adoption, reads, events, submissions, factory = order_repository
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
        "max_open_positions": 5,
        "max_daily_loss": Decimal("1000"),
        "max_gross_exposure": Decimal("150"),
        "current_daily_pnl": Decimal("0"),
        "current_gross_exposure": Decimal("0"),
        "open_position_symbols": frozenset(),
        "exposure_notional": Decimal("100"),
    }

    first = await _prepare_submission(
        submissions,
        intent=first_intent,
        evaluation=first_evaluation,
        plan=first_plan,
        prepared_at=NOW + timedelta(seconds=1),
        **claim_kwargs,
    )
    assert first is not None
    with pytest.raises(OrderPreSubmissionError, match="gross exposure"):
        await _prepare_submission(
            submissions,
            intent=second_intent,
            evaluation=second_evaluation,
            plan=second_plan,
            prepared_at=NOW + timedelta(seconds=2),
            **claim_kwargs,
        )

    await events.append_order_event(
        ExchangeOrderEvent(
            event_id="entry-canceled",
            client_order_id=first_plan.client_order_id,
            state=ExchangeOrderState.CANCELED,
            occurred_at=NOW + timedelta(seconds=3),
            exchange_order_id="entry-order",
            details={},
        )
    )
    second = await _prepare_submission(
        submissions,
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


async def test_filled_entry_claim_retained_and_blocks_stale_baseline_order(
    order_repository,
) -> None:
    repository, adoption, reads, events, submissions, factory = order_repository
    first_intent = _intent()
    first_evaluation = _evaluation(first_intent, "eval-fill-block-1")
    first_plan = _plan()

    second_intent = replace(first_intent, candidate_id="candidate-fill-2")
    second_evaluation = _evaluation(second_intent, "eval-fill-block-2")
    second_plan = replace(
        first_plan,
        intent_id="candidate-fill-2",
        client_order_id="cml_cccccccccccccccccccccccccccccccc",
        symbol="ETHUSDT",
    )
    claim_kwargs = {
        "environment": "live",
        "account_label": "primary",
        "strategy_name": "compression_breakout",
        "max_open_positions": 5,
        "max_daily_loss": Decimal("1000"),
        "max_gross_exposure": Decimal("150"),
        "current_daily_pnl": Decimal("0"),
        "current_gross_exposure": Decimal("0"),
        "open_position_symbols": frozenset(),
        "exposure_notional": Decimal("100"),
    }

    first = await _prepare_submission(
        submissions,
        intent=first_intent,
        evaluation=first_evaluation,
        plan=first_plan,
        prepared_at=NOW + timedelta(seconds=1),
        **claim_kwargs,
    )
    assert first is not None

    t_fill = NOW + timedelta(seconds=3)
    await events.append_order_event(
        ExchangeOrderEvent(
            event_id="first-filled",
            client_order_id=first_plan.client_order_id,
            state=ExchangeOrderState.FILLED,
            occurred_at=t_fill,
            exchange_order_id="first-exchange-order",
            details={
                "executed_quantity": str(first_plan.quantity),
                "average_price": "100",
            },
        )
    )

    async with factory() as session:
        claim_row = await session.get(LiveExposureClaimRow, first_plan.intent_id)
        assert claim_row is not None
        assert claim_row.active is True

    with pytest.raises(OrderPreSubmissionError, match="gross exposure"):
        await _prepare_submission(
            submissions,
            intent=second_intent,
            evaluation=second_evaluation,
            plan=second_plan,
            prepared_at=NOW + timedelta(seconds=4),
            baseline_observed_at=t_fill - timedelta(seconds=1),
            **claim_kwargs,
        )


async def test_filled_entry_claim_retired_when_account_baseline_covers_exposure(
    order_repository,
) -> None:
    repository, adoption, reads, events, submissions, factory = order_repository
    first_intent = _intent()
    first_evaluation = _evaluation(first_intent, "eval-cover-1")
    first_plan = _plan()

    second_intent = replace(first_intent, candidate_id="candidate-cover-2")
    second_evaluation = _evaluation(second_intent, "eval-cover-2")
    second_plan = replace(
        first_plan,
        intent_id="candidate-cover-2",
        client_order_id="cml_dddddddddddddddddddddddddddddddd",
        symbol="ETHUSDT",
    )
    claim_kwargs = {
        "environment": "live",
        "account_label": "primary",
        "strategy_name": "compression_breakout",
        "max_open_positions": 5,
        "max_daily_loss": Decimal("1000"),
        "max_gross_exposure": Decimal("150"),
        "current_daily_pnl": Decimal("0"),
        "current_gross_exposure": Decimal("0"),
        "open_position_symbols": frozenset(),
        "exposure_notional": Decimal("100"),
    }

    first = await _prepare_submission(
        submissions,
        intent=first_intent,
        evaluation=first_evaluation,
        plan=first_plan,
        prepared_at=NOW + timedelta(seconds=1),
        **claim_kwargs,
    )
    assert first is not None

    t_fill = NOW + timedelta(seconds=3)
    await events.append_order_event(
        ExchangeOrderEvent(
            event_id="first-covered-filled",
            client_order_id=first_plan.client_order_id,
            state=ExchangeOrderState.FILLED,
            occurred_at=t_fill,
            exchange_order_id="first-covered-order",
            details={
                "executed_quantity": str(first_plan.quantity),
                "average_price": "100",
            },
        )
    )

    updated_kwargs = dict(claim_kwargs)
    updated_kwargs["current_gross_exposure"] = Decimal("100")
    updated_kwargs["open_position_symbols"] = frozenset({first_plan.symbol})

    with pytest.raises(OrderPreSubmissionError, match="gross exposure"):
        await _prepare_submission(
            submissions,
            intent=second_intent,
            evaluation=second_evaluation,
            plan=second_plan,
            prepared_at=NOW + timedelta(seconds=4),
            baseline_observed_at=t_fill + timedelta(seconds=1),
            **updated_kwargs,
        )

    smaller_kwargs = dict(updated_kwargs)
    smaller_kwargs["exposure_notional"] = Decimal("40")
    second = await _prepare_submission(
        submissions,
        intent=second_intent,
        evaluation=second_evaluation,
        plan=second_plan,
        prepared_at=NOW + timedelta(seconds=5),
        baseline_observed_at=t_fill + timedelta(seconds=1),
        **smaller_kwargs,
    )
    assert second is not None

    async with factory() as session:
        first_claim = await session.get(LiveExposureClaimRow, first_plan.intent_id)
        assert first_claim is not None
        assert first_claim.active is False


async def test_partial_fill_canceled_downsizes_claim_and_retains_filled_portion(
    order_repository,
) -> None:
    repository, adoption, reads, events, submissions, factory = order_repository
    first_intent = _intent()
    first_evaluation = _evaluation(first_intent, "eval-partial-1")
    first_plan = replace(_plan(), quantity=Decimal("1.0"))

    second_intent = replace(first_intent, candidate_id="candidate-partial-2")
    second_evaluation = _evaluation(second_intent, "eval-partial-2")
    second_plan = replace(
        first_plan,
        intent_id="candidate-partial-2",
        client_order_id="cml_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        symbol="ETHUSDT",
    )
    claim_kwargs = {
        "environment": "live",
        "account_label": "primary",
        "strategy_name": "compression_breakout",
        "max_open_positions": 5,
        "max_daily_loss": Decimal("1000"),
        "max_gross_exposure": Decimal("150"),
        "current_daily_pnl": Decimal("0"),
        "current_gross_exposure": Decimal("0"),
        "open_position_symbols": frozenset(),
        "exposure_notional": Decimal("100"),
    }

    first = await _prepare_submission(
        submissions,
        intent=first_intent,
        evaluation=first_evaluation,
        plan=first_plan,
        prepared_at=NOW + timedelta(seconds=1),
        **claim_kwargs,
    )
    assert first is not None

    t_partial = NOW + timedelta(seconds=2)
    await events.append_order_event(
        ExchangeOrderEvent(
            event_id="partial-fill-event",
            client_order_id=first_plan.client_order_id,
            state=ExchangeOrderState.PARTIALLY_FILLED,
            occurred_at=t_partial,
            exchange_order_id="partial-order-id",
            details={"executed_quantity": "0.6", "average_price": "100"},
        )
    )

    t_cancel = NOW + timedelta(seconds=3)
    await events.append_order_event(
        ExchangeOrderEvent(
            event_id="partial-canceled-event",
            client_order_id=first_plan.client_order_id,
            state=ExchangeOrderState.CANCELED,
            occurred_at=t_cancel,
            exchange_order_id="partial-order-id",
            details={"executed_quantity": "0.6", "average_price": "100"},
        )
    )

    async with factory() as session:
        claim = await session.get(LiveExposureClaimRow, first_plan.intent_id)
        assert claim is not None
        assert claim.active is True
        assert claim.notional == Decimal("60")

    with pytest.raises(OrderPreSubmissionError, match="gross exposure"):
        await _prepare_submission(
            submissions,
            intent=second_intent,
            evaluation=second_evaluation,
            plan=second_plan,
            prepared_at=NOW + timedelta(seconds=4),
            baseline_observed_at=t_cancel - timedelta(seconds=1),
            **claim_kwargs,
        )

    second_kwargs = dict(claim_kwargs)
    second_kwargs["exposure_notional"] = Decimal("80")
    second = await _prepare_submission(
        submissions,
        intent=second_intent,
        evaluation=second_evaluation,
        plan=second_plan,
        prepared_at=NOW + timedelta(seconds=5),
        baseline_observed_at=t_cancel - timedelta(seconds=1),
        **second_kwargs,
    )
    assert second is not None


async def test_concurrent_workers_with_stale_baseline_serialized_by_advisory_lock(
    order_repository,
) -> None:
    repository, adoption, reads, events, submissions, factory = order_repository
    worker1_intent = _intent()
    worker1_evaluation = _evaluation(worker1_intent, "eval-concurrent-1")
    worker1_plan = _plan()

    worker2_intent = replace(worker1_intent, candidate_id="candidate-concurrent-2")
    worker2_evaluation = _evaluation(worker2_intent, "eval-concurrent-2")
    worker2_plan = replace(
        worker1_plan,
        intent_id="candidate-concurrent-2",
        client_order_id="cml_ffffffffffffffffffffffffffffffff",
        symbol="ETHUSDT",
    )
    claim_kwargs = {
        "environment": "live",
        "account_label": "primary",
        "strategy_name": "compression_breakout",
        "max_open_positions": 5,
        "max_daily_loss": Decimal("1000"),
        "max_gross_exposure": Decimal("150"),
        "current_daily_pnl": Decimal("0"),
        "current_gross_exposure": Decimal("0"),
        "open_position_symbols": frozenset(),
        "exposure_notional": Decimal("100"),
    }

    async def try_submit(sub_repo, intent, eval_obj, plan):
        try:
            return await _prepare_submission(
                sub_repo,
                intent=intent,
                evaluation=eval_obj,
                plan=plan,
                prepared_at=NOW + timedelta(seconds=1),
                **claim_kwargs,
            )
        except OrderPreSubmissionError as err:
            return err

    repo1 = PostgresOrderSubmissionRepository(factory)
    repo2 = PostgresOrderSubmissionRepository(factory)

    results = await asyncio.gather(
        try_submit(repo1, worker1_intent, worker1_evaluation, worker1_plan),
        try_submit(repo2, worker2_intent, worker2_evaluation, worker2_plan),
    )

    succeeded = [r for r in results if not isinstance(r, Exception) and r is not None]
    failed = [r for r in results if isinstance(r, OrderPreSubmissionError)]
    assert len(succeeded) == 1
    assert len(failed) == 1
    assert "gross exposure" in str(failed[0])


async def test_duplicate_and_out_of_order_terminal_events_are_idempotent(
    order_repository,
) -> None:
    repository, adoption, reads, events, submissions, factory = order_repository
    plan = _plan()
    intent = _intent()
    await _prepare_submission(
        submissions,
        intent=intent,
        evaluation=_evaluation(intent, "eval-idempotent"),
        plan=plan,
        prepared_at=NOW,
        environment="live",
        account_label="primary",
        strategy_name="compression_breakout",
        max_gross_exposure=Decimal("150"),
        current_daily_pnl=Decimal("0"),
        current_gross_exposure=Decimal("0"),
        open_position_symbols=frozenset(),
        exposure_notional=Decimal("100"),
    )

    t_filled = NOW + timedelta(seconds=2)
    assert (
        await events.append_order_event(
            ExchangeOrderEvent(
                event_id="idemp-fill-1",
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.FILLED,
                occurred_at=t_filled,
                exchange_order_id="ex-1",
                details={
                    "executed_quantity": str(plan.quantity),
                    "average_price": "100",
                },
            )
        )
        is True
    )

    assert (
        await events.append_order_event(
            ExchangeOrderEvent(
                event_id="idemp-fill-2",
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.FILLED,
                occurred_at=t_filled + timedelta(milliseconds=10),
                exchange_order_id="ex-1",
                details={
                    "executed_quantity": str(plan.quantity),
                    "average_price": "100",
                },
            )
        )
        is True
    )

    assert (
        await events.append_order_event(
            ExchangeOrderEvent(
                event_id="idemp-late-ack",
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.ACKNOWLEDGED,
                occurred_at=t_filled - timedelta(seconds=1),
                exchange_order_id="ex-1",
                details={"status": "NEW"},
            )
        )
        is True
    )

    persisted = await reads.load_order(plan.client_order_id)
    assert persisted is not None
    assert persisted.state is ExchangeOrderState.FILLED


async def test_live_exit_episode_reservation_survives_rolling_workers(
    order_repository,
) -> None:
    repository, adoption, reads, events, submissions, factory = order_repository
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
    }
    results = await asyncio.gather(
        _prepare_submission(
            submissions,
            intent=first_intent,
            evaluation=first_evaluation,
            plan=first_plan,
            prepared_at=NOW + timedelta(seconds=1),
            **fencing_kwargs,
        ),
        _prepare_submission(
            submissions,
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

    await events.append_order_event(
        ExchangeOrderEvent(
            event_id="exit-canceled",
            client_order_id=winner_plan.client_order_id,
            state=ExchangeOrderState.CANCELED,
            occurred_at=NOW + timedelta(seconds=3),
            exchange_order_id="exit-order",
            details={},
        )
    )
    retry = await _prepare_submission(
        submissions,
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
    repository, adoption, reads, events, submissions, factory = order_repository
    prepared = await _prepare_submission(
        submissions,
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
        await _prepare_submission(
            submissions,
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
    repository, adoption, reads, events, submissions, factory = order_repository
    await _save_intent(submissions)
    await repository.seed_order(_plan())
    for event_id, state, seconds in (
        ("filled-first", ExchangeOrderState.FILLED, 3),
        ("late-ack", ExchangeOrderState.ACKNOWLEDGED, 1),
    ):
        await events.append_order_event(
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
    repository, adoption, reads, events, submissions, factory = order_repository
    await _save_intent(submissions)
    await repository.seed_order(_plan())
    await events.append_order_event(
        ExchangeOrderEvent(
            event_id="canceled-first",
            client_order_id=_plan().client_order_id,
            state=ExchangeOrderState.CANCELED,
            occurred_at=NOW + timedelta(seconds=3),
            exchange_order_id="original-order",
            details={},
        )
    )
    await events.append_order_event(
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
    repository, adoption, reads, events, submissions, factory = order_repository
    await _save_intent(submissions)
    await repository.seed_order(_plan())
    for index, exchange_id in enumerate(("original-order", "different-order")):
        await events.append_order_event(
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

    from crypto_momentum_lab.live_rollout.postgres_runtime import (
        _load_exit_batch_bindings,
    )

    repository, adoption, reads, events, submissions, factory = order_repository
    intent = replace(_intent(), reduce_only=True, features={"batch_id": "old-batch"})
    await submissions.save_approved_intent(
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
    await repository.seed_order(plan)
    async with factory() as session:
        orders = (await session.scalars(select(ExchangeOrderRow))).all()
    assert await _load_exit_batch_bindings(factory, orders) == {
        plan.client_order_id: "old-batch"
    }


async def test_load_unresolved_orders_returns_unknown_state(
    order_repository: tuple[
        OrderRows,
        PostgresOrderAdoptionRepository,
        PostgresOrderReadRepository,
        PostgresOrderEventRepository,
        PostgresOrderSubmissionRepository,
        async_sessionmaker[AsyncSession],
    ],
) -> None:
    repository, adoption, reads, events, submissions, _ = order_repository
    await _save_intent(submissions)
    await repository.seed_order(_plan())
    await events.append_order_event(
        ExchangeOrderEvent(
            event_id="event-unknown",
            client_order_id="cml_12345678901234567890123456789012",
            state=ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
            occurred_at=NOW + timedelta(seconds=1),
            exchange_order_id=None,
            details={"cause": "submit_timeout"},
        )
    )

    unresolved = await reads.load_unresolved_orders()

    assert len(unresolved) == 1
    assert unresolved[0].state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION


async def _save_intent(repository: PostgresOrderSubmissionRepository) -> None:
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


async def test_committed_preparation_survives_process_exit_before_dispatch(
    order_repository,
    async_database_url: str,
) -> None:
    # The fixture initializes/clears only the guarded local test database.
    _, _, _, _, submissions, factory = order_repository
    child_code = """
import asyncio
import os
from sqlalchemy.ext.asyncio import async_sessionmaker
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)
from crypto_momentum_lab.persistence.postgres.order_submission_repository import (
    PostgresOrderSubmissionRepository,
)
from crypto_momentum_lab.domain.risk import RiskDecision, RiskEvaluation
from tests.integration.persistence.test_order_repository import _intent, _plan, _prepare_submission, NOW

async def main():
    engine = create_async_database_engine(os.environ["CML_CRASH_TEST_DATABASE_URL"])
    repository = PostgresOrderSubmissionRepository(
        async_sessionmaker(engine, expire_on_commit=False)
    )
    prepared = await _prepare_submission(repository,
        intent=_intent(), plan=_plan(), prepared_at=NOW,
        evaluation=RiskEvaluation(
            evaluation_id="evaluation-crash", candidate_id="candidate-1",
            decision=RiskDecision.APPROVED, reason="approved",
            evaluated_at=NOW, details={},
        ),
    )
    assert prepared is not None
    os._exit(73)

asyncio.run(main())
"""
    environment = dict(os.environ)
    environment["CML_CRASH_TEST_DATABASE_URL"] = async_database_url
    environment["PYTHONPATH"] = os.pathsep.join(sys.path)
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        child_code,
        env=environment,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        _, stderr = await asyncio.wait_for(child.communicate(), timeout=20)
    except TimeoutError:
        child.kill()
        await child.wait()
        raise
    assert child.returncode == 73, stderr.decode()

    async with factory() as session:
        state = await session.scalar(
            select(ExchangeOrderRow.state).where(
                ExchangeOrderRow.client_order_id == _plan().client_order_id,
            )
        )
        event_count = await session.scalar(
            select(func.count()).select_from(ExchangeOrderEventRow)
        )
    assert state == ExchangeOrderState.SUBMITTING.value
    assert event_count == 1
    duplicate = await _prepare_submission(
        submissions,
        intent=_intent(),
        plan=_plan(),
        prepared_at=NOW,
        evaluation=RiskEvaluation(
            evaluation_id="evaluation-restart",
            candidate_id="candidate-1",
            decision=RiskDecision.APPROVED,
            reason="approved",
            evaluated_at=NOW,
            details={},
        ),
    )
    assert duplicate is None


async def test_missing_fill_quote_remains_durable_unresolved_until_priced(
    order_repository,
):
    from unittest.mock import create_autospec

    from crypto_momentum_lab.domain.execution.exchange_contract import (
        OrderExchangeClient,
    )
    from crypto_momentum_lab.domain.execution.order_state import ExchangeOrderSnapshot
    from crypto_momentum_lab.execution_account.orders.state_machine import (
        OrderExecutionStateMachine,
    )

    plans, _, reads, events, submissions, _ = order_repository
    await _save_intent(submissions)
    plan = _plan()
    await plans.seed_order(plan)
    machine = OrderExecutionStateMachine(
        exchange=create_autospec(OrderExchangeClient, instance=True, spec_set=True),
        event_repository=events,
        live_submit_enabled=True,
    )
    snapshot = ExchangeOrderSnapshot(
        plan.client_order_id,
        "actual-exchange-id",
        ExchangeOrderState.FILLED,
        NOW,
        plan.quantity,
        Decimal("0"),
    )
    pending = await machine.apply_observed_snapshot(plan, snapshot)
    assert pending.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
    order = await reads.load_order(plan.client_order_id)
    assert order.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
    assert order.terminal_receipt is None
    assert await reads.load_unresolved_orders(plan.run_id)
    priced = await machine.apply_observed_snapshot(
        plan,
        replace(
            snapshot,
            average_price=Decimal("100"),
            observed_at=NOW + timedelta(seconds=1),
        ),
    )
    assert priced.state is ExchangeOrderState.FILLED
    order = await reads.load_order(plan.client_order_id)
    assert order.terminal_receipt.average_price == Decimal("100")
    assert not await reads.load_unresolved_orders(plan.run_id)


async def test_prepared_exit_restores_final_batch_allocations(order_repository) -> None:
    from crypto_momentum_lab.domain.execution.order_state import ExitAllocation

    _, _, reads, _, submissions, _ = order_repository
    plan = replace(
        _plan(),
        reduce_only=True,
        side="SELL",
        quantity=Decimal("1"),
        batch_id="batch-a",
        allocations=(
            ExitAllocation("batch-a", Decimal("0.4"), Decimal("100")),
            ExitAllocation("batch-b", Decimal("0.6"), Decimal("101")),
        ),
        batch_quantities={"batch-a": Decimal("0.4"), "batch-b": Decimal("0.6")},
        strategy_name="momentum",
        strategy_version="v1",
        reference_price=Decimal("102"),
    )
    await _prepare_submission(
        submissions,
        intent=replace(_intent(), reduce_only=True),
        evaluation=_evaluation(_intent(), "evaluation-1"),
        plan=plan,
        prepared_at=NOW,
    )
    single = await reads.load_order(plan.client_order_id)
    unresolved = await reads.load_unresolved_orders(plan.run_id)
    assert single is not None
    assert len(unresolved) == 1
    for restored in (single.plan, unresolved[0].plan):
        assert restored.batch_id == plan.batch_id
        assert restored.allocations == plan.allocations
        assert restored.batch_quantities == plan.batch_quantities
        assert restored.strategy_name == plan.strategy_name
        assert restored.strategy_version == plan.strategy_version
        assert restored.reference_price == plan.reference_price


async def test_order_observation_commits_fills_and_state_atomically(
    order_repository, monkeypatch
):
    plans, _, reads, events, submissions, factory = order_repository
    await _save_intent(submissions)
    plan = _plan()
    await plans.seed_order(plan)
    fill = ExchangeOrderFill(
        "atomic-fill",
        plan.client_order_id,
        "atomic-trade",
        Decimal("100"),
        plan.quantity,
        Decimal("0"),
        "USDT",
        NOW,
        {},
    )
    observation = ExchangeOrderEvent(
        "atomic-observation",
        plan.client_order_id,
        ExchangeOrderState.FILLED,
        NOW,
        "12345",
        {"executed_quantity": str(plan.quantity), "average_price": "100"},
    )

    async def fail_after_fill(session, event):
        assert (
            await session.scalar(select(func.count()).select_from(ExchangeFillRow)) == 1
        )
        raise RuntimeError("injected event persistence failure")

    with monkeypatch.context() as patch:
        patch.setattr(events, "_append_order_event_in_session", fail_after_fill)
        with pytest.raises(RuntimeError, match="injected event persistence failure"):
            await events.record_order_observation(observation, (fill,))
    async with factory() as session:
        assert (
            await session.scalar(select(func.count()).select_from(ExchangeFillRow)) == 0
        )
        assert (
            await session.scalar(
                select(func.count()).select_from(ExchangeOrderEventRow)
            )
            == 0
        )
    assert await events.record_order_observation(observation, (fill,)) is True
    assert await events.record_order_observation(observation, (fill,)) is False
    persisted = await reads.load_order(plan.client_order_id)
    assert persisted.state is ExchangeOrderState.FILLED
    assert persisted.executed_quantity == plan.quantity
    async with factory() as session:
        assert (
            await session.scalar(select(func.count()).select_from(ExchangeFillRow)) == 1
        )
        assert (
            await session.scalar(
                select(func.count()).select_from(ExchangeOrderEventRow)
            )
            == 1
        )
