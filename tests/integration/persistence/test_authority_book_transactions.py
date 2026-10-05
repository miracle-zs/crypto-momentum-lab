"""Cross-module acceptance of staged Book facts and PostgreSQL rollback."""

from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import delete, event
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
from crypto_momentum_lab.domain.execution.evidence_models import (
    ExecutionCumulativeOrderReport,
    ExecutionEvidence,
)
from crypto_momentum_lab.domain.execution.execution_book import (
    ExecutionBook,
)
from crypto_momentum_lab.domain.execution.observation_models import (
    Applied,
    Duplicate,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy.models import EntryType, StrategySide
from crypto_momentum_lab.persistence.postgres.account_journal_store import (
    PostgresAccountJournalStore,
)
from crypto_momentum_lab.persistence.postgres.command_repository import (
    PostgresCommandRepository,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
    AsyncPostgresExecutionUnitOfWork,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
)
from crypto_momentum_lab.persistence.postgres.position_reservation_repository import (
    AsyncPostgresPositionReservationRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)


def _book(factory, uow_type=AsyncPostgresExecutionUnitOfWork):
    commands = PostgresCommandRepository(factory)
    reservations = AsyncPostgresPositionReservationRepository(
        factory, strategy_name="authority-test"
    )
    uow = uow_type(
        factory,
        journal_store=PostgresAccountJournalStore(),
        command_repository=commands,
        reservation_repository=reservations,
    )
    return ExecutionBook(
        command_repository=commands,
        reservation_repository=reservations,
        execution_unit_of_work=uow,
    )


def _evidence(account, identity, quantity="1"):
    now = datetime.now(UTC)
    scope = ExecutionScope(environment="live", account_label=account, symbol="BTCUSDT")
    fill = AccountFillEvent(
        environment=scope.environment,
        account_label=scope.account_label,
        symbol=scope.symbol,
        trade_id=identity,
        order_id=identity,
        side="BUY",
        price=Decimal("100"),
        quantity=Decimal(quantity),
        realized_pnl=Decimal(0),
        fee=Decimal(0),
        fee_asset="USDT",
        trade_at=now,
        raw_payload={"positionSide": "BOTH", "is_system": True},
    )
    return ExecutionEvidence(
        evidence_id=identity,
        scope=scope,
        observed_at=now,
        fill=fill,
        stream_id="trades",
        stream_epoch="one",
        sequence=int(identity),
    )


async def test_committed_exit_reservation_survives_restart_until_real_trade_proof(
    async_database_url,
):
    from crypto_momentum_lab.domain.execution.trade_command import PositionReservation
    from crypto_momentum_lab.persistence.postgres.models import PositionReservationRow

    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"test-settled-exit-{uuid4().hex}"
    opening = _evidence(account, "1")
    scope = opening.scope
    try:
        book = _book(factory)
        await book.restore(account_label=account)
        await book.observe(opening)
        view = await book.read(scope)
        command_id = f"exit-{uuid4().hex[:30]}"
        reservation = PositionReservation(
            reservation_id=f"res-{uuid4().hex}",
            command_id=command_id,
            position_key=scope.to_position_key(),
            batch_id=view.batches[0].batch_id,
            reserved_quantity=Decimal("1"),
            created_at=opening.observed_at,
        )
        async with factory.begin() as session:
            session.add(
                PositionReservationRow(
                    reservation_id=reservation.reservation_id,
                    environment="live",
                    account_label=account,
                    strategy_name="authority-test",
                    symbol="BTCUSDT",
                    position_side="BOTH",
                    batch_id=reservation.batch_id,
                    command_id=command_id,
                    reserved_quantity=Decimal("1"),
                    consumed_quantity=Decimal("0"),
                    released_quantity=Decimal("0"),
                    status="ACTIVE",
                    created_at=opening.observed_at,
                    updated_at=opening.observed_at,
                )
            )
        book.coordinator.register_reservation(reservation)
        command = TradeCommand(
            command_id,
            scope.to_position_key(),
            TradeCommandType.EXIT,
            StrategySide.LONG,
            EntryType.MARKET,
            Decimal("1"),
            reduce_only=True,
            created_at=opening.observed_at,
        )
        book.register_prepared_command(command, scope, [reservation.reservation_id])
        report = replace(
            opening,
            evidence_id="exit-report",
            fill=None,
            sequence=2,
            order_event=ExchangeOrderEvent(
                "exit-report",
                command_id,
                ExchangeOrderState.FILLED,
                opening.observed_at,
                "real-exit-order",
                {},
            ),
            cumulative_order=ExecutionCumulativeOrderReport(
                command_id, Decimal("1"), Decimal("100"), opening.observed_at
            ),
        )
        assert isinstance(await book.observe(report), Applied)
        assert book.command_requires_recovery(command_id)
        restored = _book(factory)
        await restored.restore(account_label=account)
        assert not restored.get_active_reservations()
        assert restored.coordinator.get_reservation(
            reservation.reservation_id
        ).consumed_quantity == Decimal("1")
        closing = replace(
            opening,
            evidence_id="real-exit-trade",
            sequence=3,
            fill=replace(
                opening.fill,
                trade_id="real-exit-trade",
                order_id="real-exit-order",
                side="SELL",
            ),
        )
        assert isinstance(await restored.observe(closing), Applied)
        assert not restored.command_requires_recovery(command_id)
        assert (await restored.read(scope)).total_quantity == 0
        restarted = _book(factory)
        await restarted.restore(account_label=account)
        assert not restarted.command_requires_recovery(command_id)
        assert (await restarted.read(scope)).total_quantity == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_book_nonzero_restore_preserves_token_and_evidence_identity(
    async_database_url,
):
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"test-book-{uuid4().hex}"
    evidence = _evidence(account, "1")
    try:
        book = _book(factory)
        await book.restore(account_label=account)
        assert isinstance(await book.observe(evidence), Applied)
        historical_cut = datetime.now(UTC)
        assert isinstance(await book.observe(_evidence(account, "2")), Applied)
        original = await book.read(evidence.scope)
        restored = _book(factory)
        await restored.restore(account_label=account)
        view = await restored.read(evidence.scope)
        assert view.total_quantity == Decimal("2")
        assert view.projection_version == original.projection_version
        historical = await restored.read(evidence.scope, event_cut=historical_cut)
        assert historical.total_quantity == Decimal("1")
        assert historical.projection_version != view.projection_version
        assert isinstance(await restored.observe(evidence), Duplicate)
        assert (await restored.read(evidence.scope)).total_quantity == Decimal("2")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_legacy_dispatch_without_head_restores_unknown_atomically(
    async_database_url,
):
    from crypto_momentum_lab.domain.execution.command_models import DispatchState

    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"test-headless-{uuid4().hex}"
    first = _evidence(account, "1")
    scope = first.scope
    try:
        book = _book(factory)
        await book.restore(account_label=account)
        assert isinstance(await book.observe(first), Applied)
        command = TradeCommand(
            "legacy-dispatch",
            scope.to_position_key(),
            TradeCommandType.ENTRY,
            StrategySide.LONG,
            EntryType.MARKET,
            Decimal("1"),
            created_at=first.observed_at,
        )
        book.register_prepared_command(command, scope)
        await book.mark_dispatching(command.command_id)
        # Old deployments persisted journals/commands before adopting Book heads.
        async with factory.begin() as session:
            await session.execute(
                delete(ExecutionBookHeadRow).where(
                    ExecutionBookHeadRow.account_label == account
                )
            )
        restored = _book(factory)
        await restored.restore(account_label=account)
        assert restored.get_outbox(command.command_id).state == DispatchState.UNKNOWN
        assert command.command_id in restored._dispatch_reconciliation_required_commands
        state = await restored._execution_unit_of_work.load_position(
            scope.to_position_key(), as_of=datetime.now(UTC)
        )
        assert state.head.revision == 1
        assert state.cut.facts.fills[0].trade_id == "1"
        restarted = _book(factory)
        await restarted.restore(account_label=account)
        assert restarted.get_outbox(command.command_id).state == DispatchState.UNKNOWN
        assert (await restarted.read(scope)).total_quantity == Decimal("1")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_terminal_command_identity_and_trade_wait_survive_process_restart(
    async_database_url,
):
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"test-terminal-identity-{uuid4().hex}"
    first = _evidence(account, "1")
    scope = first.scope
    try:
        book = _book(factory)
        await book.restore(account_label=account)
        await book.observe(replace(first, fill=None))
        command = TradeCommand(
            "terminal-entry",
            scope.to_position_key(),
            TradeCommandType.ENTRY,
            StrategySide.LONG,
            EntryType.LIMIT,
            Decimal("1"),
            created_at=first.observed_at,
        )
        book.register_prepared_command(command, scope)
        report = replace(
            first,
            evidence_id="terminal-report",
            fill=None,
            sequence=2,
            order_event=ExchangeOrderEvent(
                "terminal-report",
                command.command_id,
                ExchangeOrderState.FILLED,
                first.observed_at,
                "1332041709",
                {},
            ),
            cumulative_order=ExecutionCumulativeOrderReport(
                command.command_id, Decimal("1"), Decimal("100"), first.observed_at
            ),
        )
        assert isinstance(await book.observe(report), Applied)
        restored = _book(factory)
        await restored.restore(account_label=account)
        assert restored.get_outbox(command.command_id).external_order_id == "1332041709"
        assert command.command_id in restored._recovery_required_commands
        trade = replace(
            first,
            evidence_id="true-trade",
            sequence=3,
            fill=replace(first.fill, order_id="1332041709"),
        )
        assert isinstance(await restored.observe(trade), Applied)
        assert not restored._recovery_required_commands
        assert (await restored.read(scope)).total_quantity == Decimal("1")
        assert isinstance(await restored.observe(trade), Duplicate)
        reads = []

        def record_read(conn, cursor, statement, parameters, context, many):
            if statement.startswith("SELECT") and any(
                table in statement
                for table in (
                    "position_fact_journal_events",
                    "position_recovery_checkpoints",
                    "account_fill_events",
                    "account_position_snapshots",
                )
            ):
                reads.append(statement)

        event.listen(engine.sync_engine, "before_cursor_execute", record_read)
        state = await restored._execution_unit_of_work.load_position(
            scope.to_position_key(), as_of=datetime.now(UTC)
        )
        event.remove(engine.sync_engine, "before_cursor_execute", record_read)
        assert reads and all(".symbol =" in query for query in reads)
        assert state.scope.matches(scope.to_position_key())
        assert state.cut.facts.fills[0].order_id == "1332041709"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_book_sql_failure_never_publishes_candidate(async_database_url):
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"test-book-fail-{uuid4().hex}"

    class FailAfterWrites(AsyncPostgresExecutionUnitOfWork):
        @asynccontextmanager
        async def transaction(self, key):
            async with super().transaction(key) as transaction:
                yield transaction
                raise RuntimeError("injected before PostgreSQL commit")

    try:
        first = _evidence(account, "1")
        book = _book(factory)
        await book.restore(account_label=account)
        assert isinstance(await book.observe(first), Applied)
        failed_book = _book(factory, FailAfterWrites)
        await failed_book.restore(account_label=account)
        original = await failed_book.read(first.scope)
        with pytest.raises(RuntimeError, match="injected"):
            await failed_book.observe(_evidence(account, "2"))
        after_failure = await failed_book.read(first.scope)
        assert after_failure.total_quantity == original.total_quantity
        assert after_failure.projection_version == original.projection_version
        restored = _book(factory)
        await restored.restore(account_label=account)
        assert (
            await restored.read(first.scope)
        ).projection_version == original.projection_version
        assert (await restored.read(first.scope)).total_quantity == Decimal("1")
    finally:
        await engine.dispose()


@pytest.mark.parametrize("fail_preparation", [False, True])
@pytest.mark.parametrize("reduce_only", [False, True])
async def test_order_preparation_and_dispatch_share_one_transaction(
    async_database_url,
    fail_preparation,
    reduce_only,
):
    from crypto_momentum_lab.domain.execution.command_models import DispatchState
    from crypto_momentum_lab.domain.execution.execution_book import (
        Accepted,
        ExecutionRequest,
    )
    from crypto_momentum_lab.domain.execution.order_submission import (
        OrderSubmissionPreparation,
    )
    from crypto_momentum_lab.persistence.postgres.models import (
        ExchangeOrderRow,
        ExecutionCommandRow,
    )
    from crypto_momentum_lab.persistence.postgres.order_submission_repository import (
        PostgresOrderSubmissionRepository,
    )
    from tests.integration.persistence.test_order_repository import (
        _evaluation,
        _intent,
        _plan,
    )

    transactions = []

    class CountingUnitOfWork(AsyncPostgresExecutionUnitOfWork):
        @asynccontextmanager
        async def transaction(self, *args, **kwargs):
            transactions.append(1)
            async with super().transaction(*args, **kwargs) as tx:
                yield tx

    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"test-atomic-submit-{uuid4().hex}"
    try:
        book = _book(factory, CountingUnitOfWork)
        await book.restore(account_label=account)
        opening = _evidence(account, "1")
        opening = replace(
            opening,
            observed_at=opening.observed_at - timedelta(seconds=10),
            fill=replace(
                opening.fill, trade_at=opening.fill.trade_at - timedelta(seconds=10)
            ),
        )
        await book.observe(opening)
        view = await book.read(opening.scope)
        identity = f"cml_{uuid4().hex}"
        intent = replace(_intent(), candidate_id=identity, reduce_only=reduce_only)
        plan = replace(
            _plan(),
            intent_id=identity,
            client_order_id=identity,
            projection_version=view.projection_version,
            reduce_only=reduce_only,
            side="SELL" if reduce_only else "BUY",
            batch_id=view.batches[0].batch_id if reduce_only else None,
            created_at=opening.observed_at + timedelta(seconds=1),
        )
        preparation = OrderSubmissionPreparation(
            intent=intent,
            evaluation=_evaluation(intent, identity),
        )
        repository = PostgresOrderSubmissionRepository(factory)
        transactions.clear()

        async def prepare(tx):
            prepared = await repository.prepare_submission_in_session(
                tx.session,
                intent=preparation.intent,
                evaluation=preparation.evaluation,
                plan=plan,
                prepared_at=plan.created_at,
            )
            if fail_preparation:
                raise RuntimeError("injected preparation rollback")
            return prepared

        result = await book.act(
            ExecutionRequest(
                identity,
                opening.scope,
                "authority-test",
                plan.run_id,
                identity,
                view.projection_version,
                TradeCommandType.EXIT if reduce_only else TradeCommandType.ENTRY,
                plan.quantity,
                reduce_only=reduce_only,
                target_batch_ids=(plan.batch_id,) if reduce_only else (),
                created_at=plan.created_at,
                side=StrategySide.LONG,
            ),
            prepare_submission=prepare,
        )
        assert len(transactions) == 1
        async with factory() as session:
            order = await session.get(ExchangeOrderRow, identity)
            command = await session.get(ExecutionCommandRow, identity)
        if fail_preparation:
            assert not isinstance(result, Accepted)
            assert order is None and command is None
            assert book.get_outbox(identity) is None
        else:
            assert isinstance(result, Accepted)
            assert result.prepared_submission.plan == plan
            assert order.state == ExchangeOrderState.SUBMITTING.value
            assert command.status == DispatchState.DISPATCHING.value
            assert book.get_outbox(identity).state is DispatchState.DISPATCHING
            restarted = _book(factory)
            await restarted.restore(account_label=account)
            assert restarted.command_requires_recovery(identity)
            if reduce_only:
                assert (await book.read(opening.scope)).batches[
                    0
                ].exit_order_submitted_at == plan.created_at
                assert (await restarted.read(opening.scope)).batches[
                    0
                ].exit_order_submitted_at == plan.created_at
            # Submission recovery follows the main order even when a secondary
            # command projection is stale. Terminal wire facts still need settlement.
            async with factory() as session:
                async with session.begin():
                    saved_order = await session.get(ExchangeOrderRow, identity)
                    saved_command = await session.get(ExecutionCommandRow, identity)
                    saved_order.state = ExchangeOrderState.ACKNOWLEDGED.value
                    saved_command.status = DispatchState.REJECTED.value
            recovered = await PostgresCommandRepository(
                factory
            ).load_active_execution_commands(account)
            assert (
                next(row for row in recovered if row["command_id"] == identity)[
                    "status"
                ]
                == "acknowledged"
            )
            async with factory() as session:
                async with session.begin():
                    saved_order = await session.get(ExchangeOrderRow, identity)
                    saved_command = await session.get(ExecutionCommandRow, identity)
                    saved_order.state = ExchangeOrderState.CANCELED.value
                    saved_command.status = DispatchState.ACKNOWLEDGED.value
            recovered = await PostgresCommandRepository(
                factory
            ).load_active_execution_commands(account)
            assert (
                next(row for row in recovered if row["command_id"] == identity)[
                    "status"
                ]
                == "unknown"
            )
            # An obsolete rejection must not hide a real terminal fill either.
            async with factory.begin() as session:
                saved_order = await session.get(ExchangeOrderRow, identity)
                saved_command = await session.get(ExecutionCommandRow, identity)
                saved_order.state = ExchangeOrderState.FILLED.value
                saved_order.executed_quantity = saved_order.quantity
                saved_command.status = DispatchState.REJECTED.value
            recovered = await PostgresCommandRepository(
                factory
            ).load_active_execution_commands(account)
            assert (
                next(row for row in recovered if row["command_id"] == identity)[
                    "status"
                ]
                == "unknown"
            )

    finally:
        await engine.dispose()


async def test_exit_ack_returns_with_durable_deadline_before_projection(
    async_database_url,
):
    import asyncio

    from crypto_momentum_lab.domain.execution.order_result import OrderExecutionResult
    from crypto_momentum_lab.domain.execution.order_submission import (
        OrderSubmissionPreparation,
    )
    from crypto_momentum_lab.execution_account.orders.coordinator import (
        OrderExecutionCoordinator,
    )
    from crypto_momentum_lab.persistence.postgres.order_submission_repository import (
        PostgresOrderSubmissionRepository,
    )
    from tests.integration.persistence.test_order_repository import (
        _evaluation,
        _intent,
        _plan,
    )

    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"test-background-exit-{uuid4().hex}"
    release = asyncio.Event()
    coordinator = None
    try:
        book = _book(factory)
        await book.restore(account_label=account)
        opening = _evidence(account, "1")
        opening = replace(
            opening,
            observed_at=opening.observed_at - timedelta(seconds=10),
            fill=replace(
                opening.fill, trade_at=opening.fill.trade_at - timedelta(seconds=10)
            ),
        )
        await book.observe(opening)
        view = await book.read(opening.scope)
        identity = f"cml_{uuid4().hex}"
        intent = replace(_intent(), candidate_id=identity, reduce_only=True)
        plan = replace(
            _plan(),
            intent_id=identity,
            client_order_id=identity,
            reduce_only=True,
            side="SELL",
            batch_id=view.batches[0].batch_id,
            created_at=opening.observed_at + timedelta(seconds=1),
            projection_version=view.projection_version,
        )

        class Backend:
            async def submit(self, submitted, **kwargs):
                return OrderExecutionResult(
                    submitted.client_order_id, ExchangeOrderState.ACKNOWLEDGED, "123"
                )

        coordinator = OrderExecutionCoordinator(
            backend=Backend(),
            environment="live",
            account_label=account,
            execution_book=book,
        )
        coordinator.configure_submission(PostgresOrderSubmissionRepository(factory))
        original = coordinator._observe_order_result_in_execution_book

        async def slow_projection(*args, **kwargs):
            await release.wait()
            await original(*args, **kwargs)

        coordinator._observe_order_result_in_execution_book = slow_projection
        result = await asyncio.wait_for(
            coordinator.prepare_and_execute(
                plan,
                preparation=OrderSubmissionPreparation(
                    intent=intent, evaluation=_evaluation(intent, identity)
                ),
            ),
            timeout=3,
        )
        assert result.state is ExchangeOrderState.ACKNOWLEDGED
        assert not release.is_set()
        assert (await book.read(opening.scope)).batches[
            0
        ].exit_order_submitted_at == plan.created_at
        restarted = _book(factory)
        await restarted.restore(account_label=account)
        assert (await restarted.read(opening.scope)).batches[
            0
        ].exit_order_submitted_at == plan.created_at
        assert restarted.command_requires_recovery(identity)
    finally:
        release.set()
        if coordinator is not None:
            await coordinator.aclose()
        await engine.dispose()


@pytest.mark.parametrize(
    ("capacities", "message"),
    [({}, "no proven capacity"), ({"batch-1": Decimal("0.5")}, "over-reserved")],
)
async def test_transaction_reservation_requires_proven_batch_capacity(
    async_database_url, capacities, message
):
    from crypto_momentum_lab.domain.execution.reservation_registry import (
        ReservationConflictError,
    )
    from crypto_momentum_lab.domain.execution.trade_command import PositionReservation
    from crypto_momentum_lab.persistence.postgres.models import PositionReservationRow

    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"capacity-{uuid4().hex[:16]}"
    reservation = PositionReservation(
        reservation_id=f"res-{uuid4().hex}",
        command_id=f"exit-{uuid4().hex}",
        position_key=ExecutionScope(
            environment="live", account_label=account, symbol="BTCUSDT"
        ).to_position_key(),
        batch_id="batch-1",
        reserved_quantity=Decimal("1"),
        created_at=datetime.now(UTC),
    )
    repository = AsyncPostgresPositionReservationRepository(
        factory, strategy_name="authority-test"
    )
    try:
        with pytest.raises(ReservationConflictError, match=message):
            async with factory.begin() as session:
                await repository.save_reservations_in_session(
                    session,
                    [reservation],
                    batch_quantities=capacities,
                    proven_position_quantity=Decimal("1"),
                )
        async with factory() as session:
            assert (
                await session.get(PositionReservationRow, reservation.reservation_id)
                is None
            )
    finally:
        await engine.dispose()


@pytest.mark.parametrize("in_transaction", [True, False])
async def test_reservations_share_current_batch_capacity(
    async_database_url,
    in_transaction,
):
    from crypto_momentum_lab.domain.execution.trade_command import PositionReservation
    from crypto_momentum_lab.persistence.postgres.models import (
        AccountPositionSnapshotRow,
        PositionReservationRow,
    )

    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"versions-{uuid4().hex[:16]}"
    first = PositionReservation(
        reservation_id=f"res-{uuid4().hex}",
        command_id=f"exit-{uuid4().hex}",
        position_key=ExecutionScope(
            environment="live", account_label=account, symbol="BTCUSDT"
        ).to_position_key(),
        batch_id="batch-1",
        reserved_quantity=Decimal("0.25"),
        created_at=datetime.now(UTC),
    )
    repository = AsyncPostgresPositionReservationRepository(
        factory, strategy_name="authority-test"
    )
    try:
        if not in_transaction:
            async with factory.begin() as session:
                session.add(
                    AccountPositionSnapshotRow(
                        snapshot_id=uuid4(),
                        environment="live",
                        account_label=account,
                        symbol="BTCUSDT",
                        position_side="BOTH",
                        position_amt=Decimal("1"),
                        entry_price=Decimal("100"),
                        mark_price=Decimal("100"),
                        unrealized_pnl=Decimal("0"),
                        notional=Decimal("100"),
                        observed_at=datetime.now(UTC),
                        raw_payload={},
                    )
                )
        for _ in range(2):
            reservation = replace(
                first,
                reservation_id=f"res-{uuid4().hex}",
                command_id=f"exit-{uuid4().hex}",
            )
            if in_transaction:
                async with factory.begin() as session:
                    await repository.save_reservations_in_session(
                        session,
                        [reservation],
                        batch_quantities={"batch-1": Decimal("1")},
                        proven_position_quantity=Decimal("1"),
                    )
            else:
                await repository.save_reservations(
                    [reservation],
                    batch_quantities={"batch-1": Decimal("1")},
                )
        active = await repository.load_active_reservations(first.position_key)
        assert len(active) == 2
        assert sum(item.active_quantity for item in active) == Decimal("0.5")
    finally:
        async with factory.begin() as session:
            await session.execute(
                delete(PositionReservationRow).where(
                    PositionReservationRow.account_label == account
                )
            )
            await session.execute(
                delete(AccountPositionSnapshotRow).where(
                    AccountPositionSnapshotRow.account_label == account
                )
            )
        await engine.dispose()
