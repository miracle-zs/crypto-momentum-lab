"""Order progress and account position facts advance independently."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
from crypto_momentum_lab.domain.execution.evidence_models import (
    ExecutionCumulativeOrderReport,
    ExecutionEvidence,
)
from crypto_momentum_lab.domain.execution.execution_book import (
    ExecutionBook,
    ExecutionRequest,
)
from crypto_momentum_lab.domain.execution.observation_models import Applied
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderSnapshot,
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    PositionReservation,
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

pytestmark = pytest.mark.contract

NOW = datetime(2026, 10, 1, tzinfo=UTC)
SCOPE = ExecutionScope("live", "primary", "BTCUSDT", FuturesPositionSide.LONG)


class ObservationTransaction:
    """Persistence seam only; all observation and settlement use the real Book."""

    def __init__(self, head, checkpoints=None, trade_ids=None):
        self.head = head
        self.checkpoints = checkpoints or {}
        self.trade_ids = set(trade_ids or ())

    async def load_head(self, key):
        return self.head

    async def load_checkpoint_from_head(self, *, scope, checkpoint_id):
        return self.checkpoints.get((scope, checkpoint_id))

    async def record_evidence(self, **kwargs):
        return True

    async def record_trade(self, **kwargs):
        trade_id = kwargs["trade"].trade_id
        if trade_id in self.trade_ids:
            return False
        self.trade_ids.add(trade_id)
        return True

    async def persist_watermark(self, **kwargs):
        pass

    async def persist_head(self, **kwargs):
        self.head = SimpleNamespace(
            revision=kwargs["expected_revision"] + 1,
            stream_id=kwargs["stream_id"],
            stream_epoch=kwargs["stream_epoch"],
            projection_version=kwargs["projection_version"],
            state_payload=kwargs["state_payload"],
        )
        return self.head.revision

    async def upsert_outbox(self, **kwargs):
        pass

    async def update_reservation(self, *args, **kwargs):
        pass


class ObservationUnitOfWork:
    def __init__(self, checkpoints=None):
        self.head = None
        self.fail_commit = False
        self.checkpoints = dict(checkpoints or {})
        self.trade_ids: set[str] = set()

    @asynccontextmanager
    async def transaction(self, key, account_scope=None):
        transaction = ObservationTransaction(
            self.head,
            checkpoints=self.checkpoints,
            trade_ids=self.trade_ids,
        )
        yield transaction
        if self.fail_commit:
            raise RuntimeError("injected commit failure")
        self.head = transaction.head
        self.trade_ids = transaction.trade_ids


class BlockingObservationUnitOfWork(ObservationUnitOfWork):
    """Hold the first durable write while another observation is staged."""

    def __init__(self, *, head, trade_ids):
        super().__init__()
        self.head = head
        self.trade_ids = set(trade_ids)
        self.database_lock = asyncio.Lock()
        self.first_transaction_started = asyncio.Event()
        self.release_first_transaction = asyncio.Event()
        self.block_first_transaction = True

    @asynccontextmanager
    async def transaction(self, key, account_scope=None):
        async with self.database_lock:
            transaction = ObservationTransaction(
                self.head,
                checkpoints=self.checkpoints,
                trade_ids=self.trade_ids,
            )
            if self.block_first_transaction:
                self.block_first_transaction = False
                self.first_transaction_started.set()
                await self.release_first_transaction.wait()
            yield transaction
            if self.fail_commit:
                raise RuntimeError("injected commit failure")
            self.head = transaction.head
            self.trade_ids = transaction.trade_ids


def evidence(identity, **kwargs):
    return ExecutionEvidence(
        identity, SCOPE, NOW, stream_id="hub", stream_epoch="epoch", **kwargs
    )


def fill(identity, quantity, *, entry=False):
    return AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id=identity,
        order_id="entry" if entry else "exit",
        side="BUY" if entry else "SELL",
        quantity=Decimal(quantity),
        price=Decimal("100"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=NOW if entry else NOW + timedelta(seconds=1),
        raw_payload={"positionSide": "LONG", "reduce_only": not entry},
    )


async def reserved_book():
    book = ExecutionBook(execution_unit_of_work=ObservationUnitOfWork())
    book._persistence_failed = False
    assert isinstance(
        await book.observe(evidence("opening", fill=fill("opening", "5", entry=True))),
        Applied,
    )
    command = TradeCommand(
        "exit",
        SCOPE.to_position_key(),
        TradeCommandType.EXIT,
        StrategySide.LONG,
        EntryType.MARKET,
        Decimal("2"),
        reduce_only=True,
        created_at=NOW,
    )
    reservation = PositionReservation(
        reservation_id="reservation",
        command_id="exit",
        position_key=command.position_key,
        batch_id="batch",
        reserved_quantity=Decimal("2"),
        created_at=NOW,
    )
    book.coordinator.register_reservation(reservation)
    book.register_prepared_command(command, SCOPE, [reservation.reservation_id])
    return book


def terminal_report(quantity="2"):
    return evidence(
        "terminal",
        order_event=ExchangeOrderEvent(
            event_id="terminal",
            client_order_id="exit",
            state=ExchangeOrderState.FILLED,
            occurred_at=NOW,
            exchange_order_id="123",
            details={},
        ),
        cumulative_order=ExecutionCumulativeOrderReport(
            "exit", Decimal(quantity), Decimal(quantity) * 100, NOW
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("report_first", [True, False])
async def test_terminal_report_and_real_trades_settle_without_account_recovery(
    report_first,
):
    book = await reserved_book()
    trades = evidence("closing", fill=fill("closing", "2"))
    first, second = (
        (terminal_report(), trades) if report_first else (trades, terminal_report())
    )
    result = await book.observe(first)
    assert isinstance(result, Applied)
    assert not result.recovery_required
    if report_first:
        # Order progress consumes the reservation but does not change holdings.
        assert (await book.read(SCOPE)).total_quantity == Decimal("5")
        assert not book.command_requires_recovery("exit")
    later = await book.observe(second)
    assert isinstance(later, Applied)
    assert not later.recovery_required
    assert result.consumed_quantity + later.consumed_quantity == Decimal("2")
    assert not book.get_active_reservations(SCOPE.to_position_key())
    assert (await book.read(SCOPE)).total_quantity == Decimal("3")
    assert not book.command_requires_recovery("exit")


@pytest.mark.asyncio
@pytest.mark.parametrize("report_first", [True, False])
async def test_exchange_trade_identity_settles_client_command_once(report_first):
    book = await reserved_book()
    trades = evidence(
        "exchange-trade", fill=replace(fill("trade", "2"), order_id="123")
    )
    first, second = (
        (terminal_report(), trades) if report_first else (trades, terminal_report())
    )
    results = [await book.observe(first), await book.observe(second)]
    assert sum(result.consumed_quantity for result in results) == Decimal("2")
    assert not book.get_active_reservations(SCOPE.to_position_key())
    assert not book.command_requires_recovery("exit")
    assert book.get_outbox("exit").external_order_id == "123"
    assert (await book.read(SCOPE)).total_quantity == Decimal("3")
    await book.observe(trades)
    assert (await book.read(SCOPE)).total_quantity == Decimal("3")


@pytest.mark.asyncio
async def test_external_exit_diagnostics_do_not_block_new_entries():
    from crypto_momentum_lab.domain.execution.execution_book import Accepted

    book = ExecutionBook()
    await book.observe(evidence("opening", fill=fill("opening", "2", entry=True)))
    observed = await book.observe(
        evidence("manual-exit", fill=replace(fill("manual", "2"), order_id="external"))
    )

    async def act(scope, identity):
        view = await book.read(scope)
        return await book.act(
            ExecutionRequest(
                identity,
                scope,
                "test",
                "run",
                "decision",
                view.projection_version,
                TradeCommandType.ENTRY,
                Decimal("1"),
                side=StrategySide.LONG,
            )
        )

    assert isinstance(await act(SCOPE, "same-position"), Accepted)
    other = replace(SCOPE, symbol="ETHUSDT")
    await book.observe(replace(evidence("other-ready"), scope=other))
    assert isinstance(await act(other, "other-position"), Accepted)
    assert not observed.recovery_required
    assert (await book.read(SCOPE)).total_quantity == Decimal("0")


@pytest.mark.asyncio
@pytest.mark.parametrize("report_first", [False, True])
async def test_filled_entry_report_does_not_wait_for_position_trade(report_first):
    book = ExecutionBook(execution_unit_of_work=ObservationUnitOfWork())
    book._persistence_failed = False
    await book.observe(evidence("initial"))
    command = TradeCommand(
        "entry",
        SCOPE.to_position_key(),
        TradeCommandType.ENTRY,
        StrategySide.LONG,
        EntryType.LIMIT,
        Decimal("2"),
        created_at=NOW,
    )
    book.register_prepared_command(command, SCOPE)
    report = evidence(
        "entry-terminal",
        order_event=ExchangeOrderEvent(
            "entry-terminal", "entry", ExchangeOrderState.FILLED, NOW, "1332041709", {}
        ),
        cumulative_order=ExecutionCumulativeOrderReport(
            "entry", Decimal("2"), Decimal("200"), NOW
        ),
    )
    trade = evidence(
        "entry-trades",
        fill=replace(fill("buy", "2", entry=True), order_id="1332041709"),
    )
    first, second = (report, trade) if report_first else (trade, report)
    await book.observe(first)
    await book.observe(second)
    assert not book.command_requires_recovery("entry")
    assert book.get_outbox("entry").external_order_id == "1332041709"
    assert (await book.read(SCOPE)).total_quantity == Decimal("2")


@pytest.mark.asyncio
async def test_verified_partial_receipt_resolves_unknown_dispatch_without_release():
    from crypto_momentum_lab.domain.execution.command_models import DispatchState

    book = await reserved_book()
    await book.mark_unknown("exit", "network timeout")
    assert book.command_requires_recovery("exit")
    await book.observe(
        evidence(
            "partial-receipt",
            order_event=ExchangeOrderEvent(
                "partial", "exit", ExchangeOrderState.PARTIALLY_FILLED, NOW, "123", {}
            ),
            cumulative_order=ExecutionCumulativeOrderReport(
                "exit", Decimal("1"), Decimal("100"), NOW
            ),
        )
    )
    assert book.get_outbox("exit").state is DispatchState.ACKNOWLEDGED
    assert sum(
        r.active_quantity for r in book.get_active_reservations(SCOPE.to_position_key())
    ) == Decimal("1")
    assert (await book.read(SCOPE)).total_quantity == Decimal("5")


@pytest.mark.asyncio
async def test_external_exit_fill_updates_current_position_directly():
    book = await reserved_book()
    result = await book.observe(evidence("manual-exit", fill=fill("manual-close", "2")))
    assert isinstance(result, Applied)
    assert not result.recovery_required
    assert (await book.read(SCOPE)).total_quantity == Decimal("3")


@pytest.mark.asyncio
@pytest.mark.parametrize(("quantity", "recovery"), [("1", False), ("3", True)])
async def test_terminal_order_progress_settles_or_reports_reservation_overfill(
    quantity, recovery
):
    book = await reserved_book()
    result = await book.observe(terminal_report(quantity))
    assert isinstance(result, Applied)
    assert result.recovery_required is recovery
    assert book.command_requires_recovery("exit") is recovery
    if quantity == "1":
        assert not book.get_active_reservations(SCOPE.to_position_key())
    else:
        assert "exceeds" in result.diagnostics[0]


@pytest.mark.asyncio
async def test_observed_filled_order_reaches_real_book_before_account_trade():
    from crypto_momentum_lab.domain.execution.order_result import OrderExecutionResult
    from crypto_momentum_lab.execution_account.orders.coordinator import (
        OrderExecutionCoordinator,
    )

    book = await reserved_book()
    snapshot = ExchangeOrderSnapshot(
        client_order_id="exit",
        exchange_order_id="123",
        state=ExchangeOrderState.FILLED,
        observed_at=NOW,
        executed_quantity=Decimal("2"),
        average_price=Decimal("100"),
    )
    plan = OrderExecutionPlan(
        intent_id="intent",
        run_id="run",
        client_order_id="exit",
        symbol="BTCUSDT",
        side="SELL",
        order_type="market",
        quantity=Decimal("2"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.LONG,
        created_at=NOW,
    )

    class Backend:
        async def apply_observed_snapshot(self, observed_plan, observed_snapshot):
            assert observed_plan is plan and observed_snapshot is snapshot
            return OrderExecutionResult(
                client_order_id="exit",
                state=snapshot.state,
                exchange_order_id=snapshot.exchange_order_id,
                executed_quantity=snapshot.executed_quantity,
                average_price=snapshot.average_price,
            )

    coordinator = OrderExecutionCoordinator(
        backend=Backend(),
        account_label="primary",
        environment="live",
        execution_book=book,
    )
    try:
        result = await coordinator.apply_observed_snapshot(plan, snapshot)
        assert result.state is ExchangeOrderState.FILLED
        assert not book.get_active_reservations(SCOPE.to_position_key())
        assert (await book.read(SCOPE)).total_quantity == Decimal("5")
        assert not book.command_requires_recovery("exit")
        trade = await book.observe(evidence("closing", fill=fill("closing", "2")))
        assert isinstance(trade, Applied) and not trade.recovery_required
        assert trade.consumed_quantity == Decimal("0")
        assert (await book.read(SCOPE)).total_quantity == Decimal("3")
        assert not book.command_requires_recovery("exit")
    finally:
        await coordinator.aclose()


@pytest.mark.asyncio
async def test_trade_facts_update_position_without_a_second_settlement_gate():
    book = await reserved_book()
    await book.observe(terminal_report())
    first = await book.observe(evidence("trade-1", fill=fill("trade-1", "1")))
    assert isinstance(first, Applied) and not first.recovery_required
    assert not book.command_requires_recovery("exit")
    assert (await book.read(SCOPE)).total_quantity == Decimal("4")
    await book.observe(evidence("trade-2", fill=fill("trade-2", "1")))
    assert not book.command_requires_recovery("exit")
    assert (await book.read(SCOPE)).total_quantity == Decimal("3")


@pytest.mark.asyncio
async def test_overfill_is_not_healed_by_late_real_trades():
    book = await reserved_book()
    await book.observe(terminal_report("3"))
    await book.observe(evidence("trade", fill=fill("trade", "3")))
    assert book.command_requires_recovery("exit")


@pytest.mark.asyncio
async def test_failed_position_trade_commit_does_not_publish_position_change():
    book = await reserved_book()
    await book.observe(terminal_report())
    book._execution_unit_of_work.fail_commit = True
    with pytest.raises(RuntimeError, match="injected commit failure"):
        await book.observe(evidence("closing", fill=fill("closing", "2")))
    assert not book.command_requires_recovery("exit")
    # The previous published facts survive the failed candidate transaction.
    assert (await book.read(SCOPE)).total_quantity == Decimal("5")


@pytest.mark.asyncio
async def test_missing_reservation_identity_cannot_be_healed_by_matching_trades():
    book = await reserved_book()
    book.coordinator.unregister_reservation("reservation")
    first = await book.observe(terminal_report())
    assert isinstance(first, Applied) and first.recovery_required
    await book.observe(evidence("closing", fill=fill("closing", "2")))
    assert book.command_requires_recovery("exit")


@pytest.mark.asyncio
async def test_entry_report_does_not_wait_for_account_position_facts():
    book = ExecutionBook(execution_unit_of_work=ObservationUnitOfWork())
    book._persistence_failed = False
    command = TradeCommand(
        "exit",
        SCOPE.to_position_key(),
        TradeCommandType.ENTRY,
        StrategySide.LONG,
        EntryType.MARKET,
        Decimal("2"),
        reduce_only=False,
        created_at=NOW,
    )
    book.register_prepared_command(command, SCOPE, [])
    result = await book.observe(terminal_report())
    assert isinstance(result, Applied) and not result.recovery_required
    assert not book.command_requires_recovery("exit")
    assert (await book.read(SCOPE)).total_quantity == Decimal("0")
    trade = replace(
        fill("opening", "2"),
        side="BUY",
        raw_payload={"positionSide": "LONG", "reduce_only": False},
    )
    await book.observe(evidence("opening", fill=trade))
    assert not book.command_requires_recovery("exit")
    assert (await book.read(SCOPE)).total_quantity == Decimal("2")


@pytest.mark.parametrize("report_first", [True, False])
async def test_filled_reduce_only_smaller_than_requested_releases_unused_capacity(
    report_first,
):
    from crypto_momentum_lab.domain.execution.command_models import DispatchState

    book = await reserved_book()
    report = terminal_report("1")
    trades = evidence("closing-reduced", fill=fill("closing-reduced", "1"))
    first, second = (report, trades) if report_first else (trades, report)
    await book.observe(first)
    if report_first:
        assert not book.command_requires_recovery("exit")
        assert book.coordinator.get_reservation("reservation").active_quantity == 0
        assert (await book.read(SCOPE)).total_quantity == Decimal("5")
    await book.observe(second)
    reservation = book.coordinator.get_reservation("reservation")
    assert reservation.consumed_quantity == Decimal("1")
    assert reservation.released_quantity == Decimal("1")
    assert reservation.active_quantity == 0
    assert not book.command_requires_recovery("exit")
    assert (await book.read(SCOPE)).total_quantity == Decimal("4")
    assert book.get_outbox("exit").state == DispatchState.TERMINAL


@pytest.mark.asyncio
async def test_same_position_durable_observations_stage_one_at_a_time():
    original_uow = ObservationUnitOfWork()
    book = ExecutionBook(execution_unit_of_work=original_uow)
    book._persistence_failed = False
    assert isinstance(
        await book.observe(evidence("opening", fill=fill("opening", "5", entry=True))),
        Applied,
    )

    uow = BlockingObservationUnitOfWork(
        head=original_uow.head,
        trade_ids=original_uow.trade_ids,
    )
    book._execution_unit_of_work = uow
    original_stage = book._staged_copy
    staged = 0
    second_stage_started = asyncio.Event()

    def count_staging(key=None):
        nonlocal staged
        staged += 1
        if staged == 2:
            second_stage_started.set()
        return original_stage(key=key)

    book._staged_copy = count_staging
    first = asyncio.create_task(
        book.observe(evidence("trade-a", fill=fill("trade-a", "1", entry=True)))
    )
    await asyncio.wait_for(uow.first_transaction_started.wait(), timeout=1)
    second = asyncio.create_task(
        book.observe(evidence("trade-b", fill=fill("trade-b", "1", entry=True)))
    )
    try:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(second_stage_started.wait(), timeout=0.02)
    finally:
        uow.release_first_transaction.set()

    results = await asyncio.gather(first, second)
    assert all(isinstance(result, Applied) for result in results)
    assert staged == 2
    assert (await book.read(SCOPE)).total_quantity == Decimal("7")
