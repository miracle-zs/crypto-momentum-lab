"""Real Book settlement when an order report precedes its account trades."""

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
    Blocked,
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

NOW = datetime(2026, 10, 1, tzinfo=UTC)
SCOPE = ExecutionScope("live", "primary", "BTCUSDT", FuturesPositionSide.LONG)


class ObservationTransaction:
    """Persistence seam only; all observation and settlement use the real Book."""

    def __init__(self, head):
        self.head = head

    async def load_head(self, key):
        return self.head

    async def record_evidence(self, **kwargs):
        return True

    async def record_trade(self, **kwargs):
        return True

    async def persist_facts(self, **kwargs):
        return SimpleNamespace(has_conflicts=False, revision=kwargs["revision"])

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
    def __init__(self):
        self.head = None
        self.fail_commit = False

    @asynccontextmanager
    async def transaction(self, key):
        transaction = ObservationTransaction(self.head)
        yield transaction
        if self.fail_commit:
            raise RuntimeError("injected commit failure")
        self.head = transaction.head


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
        # A report settles capacity but must not manufacture position trades.
        assert (await book.read(SCOPE)).total_quantity == Decimal("5")
        assert book._recovery_required_commands == {"exit"}
        blocked = await book.act(
            ExecutionRequest(
                request_id="next-exit",
                scope=SCOPE,
                strategy_name="test",
                strategy_version="1",
                run_id="run",
                decision_ref="next",
                expected_view_token="*",
                action=TradeCommandType.EXIT,
                requested_quantity=Decimal("1"),
            )
        )
        assert isinstance(blocked, Blocked)
        assert blocked.reason == "Execution reservation settlement requires recovery"
    later = await book.observe(second)
    assert isinstance(later, Applied)
    assert not later.recovery_required
    assert result.consumed_quantity + later.consumed_quantity == Decimal("2")
    assert not book.get_active_reservations(SCOPE.to_position_key())
    assert (await book.read(SCOPE)).total_quantity == Decimal("3")
    assert not book._recovery_required_commands


@pytest.mark.asyncio
@pytest.mark.parametrize("quantity", ["1", "3"])
async def test_incomplete_or_excess_terminal_settlement_still_requires_recovery(
    quantity,
):
    book = await reserved_book()
    result = await book.observe(terminal_report(quantity))
    assert isinstance(result, Applied)
    assert result.recovery_required
    assert book._recovery_required_commands == {"exit"}
    if quantity == "1":
        assert sum(
            r.active_quantity
            for r in book.get_active_reservations(SCOPE.to_position_key())
        ) == Decimal("1")
    else:
        assert "exceeds" in result.diagnostics[0]


@pytest.mark.asyncio
async def test_observed_filled_order_reaches_real_book_before_account_trade():
    from crypto_momentum_lab.execution_account.orders.coordinator import (
        OrderExecutionCoordinator,
    )
    from crypto_momentum_lab.execution_account.orders.state_machine import (
        OrderExecutionResult,
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
        assert book._recovery_required_commands == {"exit"}
        trade = await book.observe(evidence("closing", fill=fill("closing", "2")))
        assert isinstance(trade, Applied) and not trade.recovery_required
        assert trade.consumed_quantity == Decimal("0")
        assert (await book.read(SCOPE)).total_quantity == Decimal("3")
        assert not book._recovery_required_commands
    finally:
        await coordinator.aclose()


@pytest.mark.asyncio
async def test_partial_trades_keep_gate_until_exact_settlement_is_confirmed():
    book = await reserved_book()
    await book.observe(terminal_report())
    first = await book.observe(evidence("trade-1", fill=fill("trade-1", "1")))
    assert isinstance(first, Applied) and not first.recovery_required
    assert book._recovery_required_commands == {"exit"}
    assert (await book.read(SCOPE)).total_quantity == Decimal("4")
    await book.observe(evidence("trade-2", fill=fill("trade-2", "1")))
    assert not book._recovery_required_commands
    assert (await book.read(SCOPE)).total_quantity == Decimal("3")


@pytest.mark.asyncio
async def test_overfill_is_not_healed_by_late_real_trades():
    book = await reserved_book()
    await book.observe(terminal_report("3"))
    await book.observe(evidence("trade", fill=fill("trade", "3")))
    assert book._recovery_required_commands == {"exit"}


@pytest.mark.asyncio
async def test_failed_trade_commit_does_not_publish_settlement_completion():
    book = await reserved_book()
    await book.observe(terminal_report())
    book._execution_unit_of_work.fail_commit = True
    with pytest.raises(RuntimeError, match="injected commit failure"):
        await book.observe(evidence("closing", fill=fill("closing", "2")))
    assert book._recovery_required_commands == {"exit"}
    assert book._persistence_failed
    # The previous published facts survive the failed candidate transaction.
    assert book._books[
        SCOPE.to_position_key().canonical_id
    ].get_view().total_quantity == Decimal("5")


@pytest.mark.asyncio
async def test_missing_reservation_identity_cannot_be_healed_by_matching_trades():
    book = await reserved_book()
    book.coordinator.unregister_reservation("reservation")
    first = await book.observe(terminal_report())
    assert isinstance(first, Applied) and first.recovery_required
    await book.observe(evidence("closing", fill=fill("closing", "2")))
    assert book._recovery_required_commands == {"exit"}


@pytest.mark.asyncio
async def test_entry_report_waits_for_account_trades_without_exit_reservations():
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
    assert book._recovery_required_commands == {"exit"}
    assert (await book.read(SCOPE)).total_quantity == Decimal("0")
    trade = replace(
        fill("opening", "2"),
        side="BUY",
        raw_payload={"positionSide": "LONG", "reduce_only": False},
    )
    await book.observe(evidence("opening", fill=trade))
    assert not book._recovery_required_commands
    assert (await book.read(SCOPE)).total_quantity == Decimal("2")
