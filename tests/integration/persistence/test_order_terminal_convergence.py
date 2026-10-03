"""Integration tests for terminal state convergence and self-healing (S03, S06, S07).

Verifies that:
1. S06: Canceled order across stream epoch bumps converges outbox and durable
   execution_commands to terminal, releasing reservations.
2. Duplicate terminal WS events with non-terminal outbox converge outbox to terminal.
3. S03: Transient projection version mismatch reloads durable head and does not
   brick the ExecutionBook with _persistence_failed.
4. S07: Partial fills followed by cancel settle the filled portion, release unneeded
   reservations, and scale live exposure claims without dropping active exposure.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.command_models import (
    DispatchState,
    ExecutionScope,
)
from crypto_momentum_lab.domain.execution.evidence_models import (
    ExecutionCumulativeOrderReport,
    ExecutionEvidence,
)
from crypto_momentum_lab.domain.execution.execution_book import (
    ExecutionBook,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    FuturesPositionSide,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    PositionReservation,
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy.models import EntryType, StrategySide
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
    OrderExecutionResult,
)
from crypto_momentum_lab.live_rollout.order_reconciliation import (
    LiveOrderReconciliation,
)
from crypto_momentum_lab.persistence.postgres.account_journal_store import (
    PostgresAccountJournalStore,
)
from crypto_momentum_lab.persistence.postgres.command_repository import (
    PostgresCommandRepository,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
    AsyncPostgresExecutionUnitOfWork,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ExchangeOrderRow,
    ExecutionBookHeadRow,
    ExecutionCommandRow,
    ExitEpisodeReservationRow,
    LiveExposureClaimRow,
    OrderIntentExecutionRow,
    PositionReservationRow,
)
from crypto_momentum_lab.persistence.postgres.order_event_repository import (
    PostgresOrderEventRepository,
)
from crypto_momentum_lab.persistence.postgres.order_read_repository import (
    PostgresOrderReadRepository,
)
from crypto_momentum_lab.persistence.postgres.position_reservation_repository import (
    AsyncPostgresPositionReservationRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)

NOW = datetime(2026, 10, 3, 8, 45, tzinfo=UTC)


async def _setup_book_and_repos(factory, account: str):
    commands = PostgresCommandRepository(factory)
    reservations = AsyncPostgresPositionReservationRepository(
        factory, strategy_name="terminal-convergence-test"
    )
    uow = AsyncPostgresExecutionUnitOfWork(
        factory,
        journal_store=PostgresAccountJournalStore(),
        command_repository=commands,
        reservation_repository=reservations,
    )
    book = ExecutionBook(
        command_repository=commands,
        reservation_repository=reservations,
        execution_unit_of_work=uow,
    )
    await book.restore(account_label=account)
    order_events = PostgresOrderEventRepository(factory)
    order_reads = PostgresOrderReadRepository(factory)
    return book, commands, reservations, uow, order_events, order_reads


@pytest.mark.asyncio
async def test_canceled_order_in_new_epoch_converges_outbox_and_durable_command(
    async_database_url: str,
):
    """S06: An unfilled canceled order in a new stream epoch must converge outbox and execution_commands."""
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = "s06-" + uuid4().hex[:10]
    (
        book,
        commands,
        reservations,
        uow,
        order_events,
        order_reads,
    ) = await _setup_book_and_repos(factory, account)

    scope = ExecutionScope("live", account, "BTCUSDT", FuturesPositionSide.LONG)
    pos_key = scope.to_position_key()
    cmd_id = "entry-" + account
    stream_epoch_1 = "epoch-1"
    stream_epoch_2 = "epoch-2"

    # 1. Establish stream epoch 1 and empty position head
    book.register_active_stream(
        environment="live",
        account_label=account,
        stream_id="ws-stream",
        stream_epoch=stream_epoch_1,
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="init-flat-" + account,
            scope=scope,
            stream_id="ws-stream",
            stream_epoch=stream_epoch_1,
            observed_at=NOW,
            snapshot=AccountPositionSnapshot(
                environment="live",
                account_label=account,
                symbol="BTCUSDT",
                position_side="LONG",
                position_amt=Decimal("0"),
                entry_price=Decimal("0"),
                mark_price=Decimal("100"),
                unrealized_pnl=Decimal("0"),
                notional=Decimal("0"),
                leverage=2,
                margin_type="cross",
                observed_at=NOW,
                raw_payload={"include_flat": True},
            ),
        )
    )

    # 2. Prepare entry command with reservation
    command = TradeCommand(
        cmd_id,
        pos_key,
        TradeCommandType.ENTRY,
        StrategySide.LONG,
        EntryType.LIMIT,
        Decimal("1"),
        created_at=NOW,
    )
    res = PositionReservation(
        reservation_id="res-" + cmd_id,
        command_id=cmd_id,
        position_key=pos_key,
        batch_id="batch-" + cmd_id,
        reserved_quantity=Decimal("1"),
        consumed_quantity=Decimal("0"),
        created_at=NOW,
    )
    async with factory() as session, session.begin():
        session.add(
            PositionReservationRow(
                reservation_id=res.reservation_id,
                environment="live",
                account_label=account,
                strategy_name="terminal-convergence-test",
                symbol=pos_key.symbol,
                position_side=pos_key.position_side.value,
                batch_id=res.batch_id,
                command_id=cmd_id,
                client_order_id=cmd_id,
                reserved_quantity=res.reserved_quantity,
                consumed_quantity=Decimal("0"),
                released_quantity=Decimal("0"),
                status="ACTIVE",
                created_at=NOW,
                updated_at=NOW,
            )
        )
    book.coordinator.register_reservation(res)
    book.register_prepared_command(command, scope, [res.reservation_id])
    await book.mark_acknowledged(cmd_id, external_order_id="ex-123")

    # Check that in Postgres execution_commands is acknowledged
    async with factory() as session:
        cmd_row = await session.get(ExecutionCommandRow, cmd_id)
        assert cmd_row is not None
        assert cmd_row.status == "acknowledged"

    # 3. Simulate stream epoch change (e.g. WebSocket reconnection)
    book.register_active_stream(
        environment="live",
        account_label=account,
        stream_id="ws-stream",
        stream_epoch=stream_epoch_2,
    )

    # 4. Order is CANCELED on exchange (0 fills)
    order_ev = ExchangeOrderEvent(
        event_id="order-cancel-" + cmd_id,
        client_order_id=cmd_id,
        state=ExchangeOrderState.CANCELED,
        occurred_at=NOW + timedelta(seconds=10),
        exchange_order_id="ex-123",
        details={
            "executed_quantity": "0",
            "cumulative_quote_quantity": "0",
            "average_price": "0",
        },
    )

    evidence = ExecutionEvidence(
        evidence_id="ev-cancel-" + cmd_id,
        scope=scope,
        observed_at=NOW + timedelta(seconds=10),
        order_event=order_ev,
        stream_id="ws-stream",
        stream_epoch=stream_epoch_2,
        cumulative_order=ExecutionCumulativeOrderReport(
            order_id=cmd_id,
            cumulative_quantity=Decimal("0"),
            cumulative_quote=Decimal("0"),
            observed_at=NOW + timedelta(seconds=10),
        ),
    )

    # 5. ExecutionBook observe must NOT fail on STREAM_RECOVERY_PROOF_REQUIRED
    result = await book.observe(evidence)
    assert not isinstance(result, Exception)
    assert getattr(result, "rejected", False) is False

    # Outbox in memory must be TERMINAL
    outbox = book.get_outbox(cmd_id)
    assert outbox is not None
    assert outbox.state == DispatchState.TERMINAL

    # Reservations must be released
    active_res = book.get_active_reservations(pos_key)
    assert len(active_res) == 0

    # PostgreSQL execution_commands table must be converged to 'terminal'
    async with factory() as session:
        cmd_row = await session.get(ExecutionCommandRow, cmd_id)
        assert cmd_row is not None
        assert cmd_row.status == "terminal"

    await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("recovery_path", ["account_event", "restart_receipt"])
async def test_duplicate_terminal_event_when_outbox_unresolved_converges(
    async_database_url: str,
    recovery_path: str,
):
    """When persisted order is terminal but outbox is unresolved, reconcile_account_event converges it."""
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = "recon-" + uuid4().hex[:10]
    (
        book,
        commands,
        reservations,
        uow,
        order_events,
        order_reads,
    ) = await _setup_book_and_repos(factory, account)

    scope = ExecutionScope("live", account, "ETHUSDT", FuturesPositionSide.LONG)
    pos_key = scope.to_position_key()
    cmd_id = "entry-eth-" + account
    intent_id = "intent-" + cmd_id

    book.register_active_stream(
        environment="live",
        account_label=account,
        stream_id="ws-stream",
        stream_epoch="1",
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="init-flat-" + account,
            scope=scope,
            stream_id="ws-stream",
            stream_epoch="1",
            observed_at=NOW,
            snapshot=AccountPositionSnapshot(
                environment="live",
                account_label=account,
                symbol="ETHUSDT",
                position_side="LONG",
                position_amt=Decimal("0"),
                entry_price=Decimal("0"),
                mark_price=Decimal("2000"),
                unrealized_pnl=Decimal("0"),
                notional=Decimal("0"),
                leverage=2,
                margin_type="cross",
                observed_at=NOW,
                raw_payload={"include_flat": True},
            ),
        )
    )

    command = TradeCommand(
        cmd_id,
        pos_key,
        TradeCommandType.ENTRY,
        StrategySide.LONG,
        EntryType.LIMIT,
        Decimal("2"),
        created_at=NOW,
    )
    res = PositionReservation(
        reservation_id="res-" + cmd_id,
        command_id=cmd_id,
        position_key=pos_key,
        batch_id="batch-" + cmd_id,
        reserved_quantity=Decimal("2"),
        consumed_quantity=Decimal("0"),
        created_at=NOW,
    )
    async with factory() as session, session.begin():
        session.add(
            PositionReservationRow(
                reservation_id=res.reservation_id,
                environment="live",
                account_label=account,
                strategy_name="terminal-convergence-test",
                symbol=pos_key.symbol,
                position_side=pos_key.position_side.value,
                batch_id=res.batch_id,
                command_id=cmd_id,
                client_order_id=cmd_id,
                reserved_quantity=res.reserved_quantity,
                consumed_quantity=Decimal("0"),
                released_quantity=Decimal("0"),
                status="ACTIVE",
                created_at=NOW,
                updated_at=NOW,
            )
        )
    book.coordinator.register_reservation(res)
    book.register_prepared_command(command, scope, [res.reservation_id])
    await book.observe(
        ExecutionEvidence(
            evidence_id="durable-before-crash-" + account,
            scope=scope,
            stream_id="ws-stream",
            stream_epoch="1",
            observed_at=NOW,
            snapshot=AccountPositionSnapshot(
                environment="live",
                account_label=account,
                symbol="ETHUSDT",
                position_side="LONG",
                position_amt=Decimal("0"),
                entry_price=Decimal("0"),
                mark_price=Decimal("2000"),
                unrealized_pnl=Decimal("0"),
                notional=Decimal("0"),
                leverage=2,
                margin_type="cross",
                observed_at=NOW,
                raw_payload={"include_flat": True},
            ),
        )
    )
    await book.mark_acknowledged(cmd_id, external_order_id="ex-eth-1")

    # Persist the order as canceled in exchange_orders table
    async with factory() as session:
        async with session.begin():
            session.add(
                OrderIntentExecutionRow(
                    intent_id=intent_id,
                    candidate_id="cand-" + cmd_id,
                    run_id="run-1",
                    risk_evaluation_id="risk-" + cmd_id,
                    strategy_name="compression_breakout",
                    symbol="ETHUSDT",
                    state="approved",
                    approved_at=NOW,
                    details={"desired_notional": "4000"},
                )
            )
            await session.flush()
            session.add(
                ExchangeOrderRow(
                    client_order_id=cmd_id,
                    intent_id=intent_id,
                    run_id="run-1",
                    symbol="ETHUSDT",
                    side="BUY",
                    order_type="LIMIT",
                    quantity=Decimal("2"),
                    price=Decimal("2000"),
                    reduce_only=False,
                    position_side="LONG",
                    state=ExchangeOrderState.CANCELED.value,
                    exchange_order_id="ex-eth-1",
                    created_at=NOW,
                    updated_at=NOW + timedelta(seconds=5),
                    executed_quantity=Decimal("0"),
                )
            )

    if recovery_path == "restart_receipt":
        # Crash cut: the order event commits, but Book has not accepted it.
        await order_events.append_order_event(
            ExchangeOrderEvent(
                event_id="durable-cancel-" + cmd_id,
                client_order_id=cmd_id,
                state=ExchangeOrderState.CANCELED,
                exchange_order_id="ex-eth-1",
                occurred_at=NOW + timedelta(seconds=10),
                details={"executed_quantity": "0"},
            )
        )
        assert book.get_outbox(cmd_id).state is DispatchState.ACKNOWLEDGED
        # A fresh Book restores durable pending commands and reservations.
        (
            book,
            commands,
            reservations,
            uow,
            order_events,
            order_reads,
        ) = await _setup_book_and_repos(factory, account)

    class DummyBackend:
        async def apply_observed_snapshot(self, plan, snapshot):
            return OrderExecutionResult(
                cmd_id,
                ExchangeOrderState.CANCELED,
                "ex-eth-1",
                executed_quantity=Decimal("0"),
                average_price=Decimal("0"),
                plan=plan,
            )

    coordinator = OrderExecutionCoordinator(
        backend=DummyBackend(),
        account_label=account,
        environment="live",
        execution_book=book,
    )

    reconciliation = LiveOrderReconciliation(
        order_repository=order_reads,
        state_machine=coordinator,
        run_id="run-1",
    )

    # Create account event update
    class MockUpdateEvent:
        client_order_id = cmd_id
        order_update = {
            "s": "ETHUSDT",
            "c": cmd_id,
            "S": "BUY",
            "o": "LIMIT",
            "ps": "LONG",
            "R": False,
            "q": "2",
            "p": "2000",
            "X": "CANCELED",
            "z": "0",
            "ap": "0",
            "i": "ex-eth-1",
            "T": int((NOW + timedelta(seconds=10)).timestamp() * 1000),
        }

    if recovery_path == "restart_receipt":
        from crypto_momentum_lab.live_rollout.command_receipt_recovery import (
            recover_restored_commands,
        )

        async def no_network_query(_plan):
            pytest.fail(
                "durable terminal receipt must recover without a new exchange request"
            )

        for _ in range(2):
            assert not await recover_restored_commands(
                book=book,
                coordinator=coordinator,
                orders=order_reads,
                reconcile_order=no_network_query,
            )
    else:
        # A repeat WS event also repairs the unaccepted Book observation.
        await reconciliation.reconcile_account_event(MockUpdateEvent())

    # Outbox in book must now be TERMINAL
    assert book.get_outbox(cmd_id).state == DispatchState.TERMINAL

    # DB execution_commands must be converged to 'terminal'
    async with factory() as session:
        cmd_row = await session.get(ExecutionCommandRow, cmd_id)
        assert cmd_row is not None
        assert cmd_row.status == "terminal"

    # Second event: Now outbox IS TERMINAL, so it is cleanly handled as duplicate
    await reconciliation.reconcile_account_event(MockUpdateEvent())
    assert book.get_outbox(cmd_id).state == DispatchState.TERMINAL

    await coordinator.aclose()
    await engine.dispose()


@pytest.mark.asyncio
async def test_transient_projection_version_mismatch_reloads_and_does_not_fail_book(
    async_database_url: str,
):
    """S03: When local view projection_version lags durable head, book reloads without setting persistence_failed."""
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = "s03-" + uuid4().hex[:10]
    (
        book,
        commands,
        reservations,
        uow,
        order_events,
        order_reads,
    ) = await _setup_book_and_repos(factory, account)

    scope = ExecutionScope("live", account, "SOLUSDT", FuturesPositionSide.LONG)
    pos_key = scope.to_position_key()
    book.register_active_stream(
        environment="live",
        account_label=account,
        stream_id="ws-stream",
        stream_epoch="1",
    )

    # Establish durable position head with a fill so ExecutionBookHeadRow is persisted
    fill = AccountFillEvent(
        environment="live",
        account_label=account,
        symbol="SOLUSDT",
        trade_id="t-init-" + account,
        order_id="order-sol-1",
        side="BUY",
        price=Decimal("150"),
        quantity=Decimal("1"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=NOW,
        raw_payload={"positionSide": "LONG"},
    )
    snap = AccountPositionSnapshot(
        environment="live",
        account_label=account,
        symbol="SOLUSDT",
        position_side="LONG",
        position_amt=Decimal("1"),
        entry_price=Decimal("150"),
        mark_price=Decimal("150"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("150"),
        leverage=2,
        margin_type="cross",
        observed_at=NOW,
        raw_payload={"positionAmt": "1.0"},
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="init-" + account,
            scope=scope,
            stream_id="ws-stream",
            stream_epoch="1",
            observed_at=NOW,
            fill=fill,
            snapshot=snap,
            sequence=1,
        )
    )

    # Verify book is healthy
    assert not book._persistence_failed

    # Simulate another worker or transaction bumping the durable projection_version on head
    async with factory() as session:
        async with session.begin():
            head_row = await session.get(
                ExecutionBookHeadRow,
                ("live", account, "SOLUSDT", "LONG"),
            )
            assert head_row is not None
            head_row.projection_version = "pv_bumped_123"

    # Now an observation arrives in this process
    ev = ExecutionEvidence(
        evidence_id="ev2-" + account,
        scope=scope,
        stream_id="ws-stream",
        stream_epoch="1",
        observed_at=NOW + timedelta(seconds=1),
        snapshot=AccountPositionSnapshot(
            environment="live",
            account_label=account,
            symbol="SOLUSDT",
            position_side="LONG",
            position_amt=Decimal("1"),
            entry_price=Decimal("150"),
            mark_price=Decimal("150"),
            unrealized_pnl=Decimal("0"),
            notional=Decimal("150"),
            leverage=2,
            margin_type="cross",
            observed_at=NOW + timedelta(seconds=1),
            raw_payload={"positionAmt": "1.0"},
        ),
    )

    await book.observe(ev)
    # The observation must not throw bare RuntimeError or set _persistence_failed = True!
    assert not book._persistence_failed

    await engine.dispose()


@pytest.mark.asyncio
async def test_partial_fill_then_cancel_preserves_executed_exposure_claim(
    async_database_url: str,
):
    """S07: When an order partially fills then cancels, the executed portion scales the claim and retains active=True."""
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = "s07-" + uuid4().hex[:10]
    (
        book,
        commands,
        reservations,
        uow,
        order_events,
        order_reads,
    ) = await _setup_book_and_repos(factory, account)

    cmd_id = "partial-cancel-" + account
    intent_id = "intent-" + cmd_id

    # Insert order intent, exchange order, reservation, and live exposure claim rows in Postgres
    async with factory() as session:
        async with session.begin():
            session.add(
                OrderIntentExecutionRow(
                    intent_id=intent_id,
                    candidate_id="cand-" + cmd_id,
                    run_id="run-1",
                    risk_evaluation_id="risk-" + cmd_id,
                    strategy_name="compression_breakout",
                    symbol="BTCUSDT",
                    state="approved",
                    approved_at=NOW,
                    details={"desired_notional": "120000"},
                )
            )
            await session.flush()
            session.add(
                ExchangeOrderRow(
                    client_order_id=cmd_id,
                    intent_id=intent_id,
                    run_id="run-1",
                    symbol="BTCUSDT",
                    side="BUY",
                    order_type="LIMIT",
                    quantity=Decimal("2"),
                    price=Decimal("60000"),
                    reduce_only=False,
                    position_side="LONG",
                    state=ExchangeOrderState.PARTIALLY_FILLED.value,
                    exchange_order_id="ex-part-1",
                    created_at=NOW,
                    updated_at=NOW,
                    executed_quantity=Decimal("1"),
                )
            )
            session.add(
                ExitEpisodeReservationRow(
                    environment="live",
                    account_label=account,
                    strategy_name="compression_breakout",
                    symbol="BTCUSDT",
                    position_side="LONG",
                    episode_key="ep-1",
                    intent_id=intent_id,
                    client_order_id=cmd_id,
                    state="partially_filled",
                    active=True,
                    created_at=NOW,
                    updated_at=NOW,
                )
            )
            session.add(
                LiveExposureClaimRow(
                    intent_id=intent_id,
                    environment="live",
                    account_label=account,
                    strategy_name="compression_breakout",
                    symbol="BTCUSDT",
                    position_side="LONG",
                    notional=Decimal("120000"),
                    active=True,
                    created_at=NOW,
                    updated_at=NOW,
                )
            )

    # Now append terminal CANCELED event with executed_quantity=1
    cancel_ev = ExchangeOrderEvent(
        event_id="ev-cancel-" + cmd_id,
        client_order_id=cmd_id,
        state=ExchangeOrderState.CANCELED,
        occurred_at=NOW + timedelta(seconds=30),
        exchange_order_id="ex-part-1",
        details={
            "executed_quantity": "1",
            "cumulative_quote_quantity": "60000",
            "average_price": "60000",
        },
    )

    appended = await order_events.append_order_event(cancel_ev)
    assert appended is True

    # Verify rows in Postgres
    async with factory() as session:
        order_row = await session.get(ExchangeOrderRow, cmd_id)
        assert order_row.state == ExchangeOrderState.CANCELED.value
        assert order_row.executed_quantity == Decimal("1")

        # Reservation is no longer active because order is terminal
        res_row = (
            await session.scalars(
                select(ExitEpisodeReservationRow).where(
                    ExitEpisodeReservationRow.intent_id == intent_id
                )
            )
        ).one()
        assert res_row.active is False

        # Live exposure claim must NOT be deactivated because 1 unit is still filled on exchange!
        claim_row = (
            await session.scalars(
                select(LiveExposureClaimRow).where(
                    LiveExposureClaimRow.intent_id == intent_id
                )
            )
        ).one()
        assert claim_row.active is True
        # Notional scaled: (1 / 2) * 120000 = 60000
        assert claim_row.notional == Decimal("60000")

    # Re-apply a second terminal event with a different event ID (e.g. from REST reconciliation)
    cancel_ev_duplicate = ExchangeOrderEvent(
        event_id="ev-cancel-rest-" + cmd_id,
        client_order_id=cmd_id,
        state=ExchangeOrderState.CANCELED,
        occurred_at=NOW + timedelta(seconds=35),
        exchange_order_id="ex-part-1",
        details={
            "executed_quantity": "1",
            "cumulative_quote_quantity": "60000",
            "average_price": "60000",
        },
    )
    appended_dup = await order_events.append_order_event(cancel_ev_duplicate)
    assert appended_dup is True

    async with factory() as session:
        claim_row = (
            await session.scalars(
                select(LiveExposureClaimRow).where(
                    LiveExposureClaimRow.intent_id == intent_id
                )
            )
        ).one()
        assert claim_row.active is True
        # Must remain 60000 stably, NOT halved repeatedly to 30000!
        assert claim_row.notional == Decimal("60000")

    # Now simulate a late terminal event where payload omitted executed_quantity (e.g. empty details)
    cancel_ev_empty_details = ExchangeOrderEvent(
        event_id="ev-cancel-late-nodata-" + cmd_id,
        client_order_id=cmd_id,
        state=ExchangeOrderState.CANCELED,
        occurred_at=NOW + timedelta(seconds=40),
        exchange_order_id="ex-part-1",
        details={},
    )
    appended_late = await order_events.append_order_event(cancel_ev_empty_details)
    assert appended_late is True

    async with factory() as session:
        claim_row = (
            await session.scalars(
                select(LiveExposureClaimRow).where(
                    LiveExposureClaimRow.intent_id == intent_id
                )
            )
        ).one()
        # Must NOT be deactivated to active=False by late event missing quantity!
        assert claim_row.active is True
        assert claim_row.notional == Decimal("60000")

    await engine.dispose()
