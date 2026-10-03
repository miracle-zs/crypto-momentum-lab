from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from crypto_momentum_lab.domain.execution.command_models import DispatchState
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.order_read_models import (
    PersistedExchangeOrder,
    PersistedOrderReceipt,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
)
from crypto_momentum_lab.live_rollout.command_receipt_recovery import (
    recover_restored_commands,
)
from crypto_momentum_lab.live_rollout.order_reconciliation import (
    LiveOrderReconciliation,
)
from tests.unit.execution.test_terminal_settlement import (
    NOW,
    SCOPE,
    ObservationUnitOfWork,
    evidence,
    fill,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("priced_receipt", [True, False])
@pytest.mark.parametrize("trade_in_current_cut", [True, False])
async def test_historical_account_trades_settle_without_replaying_position_prefix(priced_receipt, trade_in_current_cut):
    """A new epoch can have a flat checkpoint with no old order trades in its cut."""
    from crypto_momentum_lab.persistence.postgres.order_read_repository import (
        PostgresOrderReadRepository,
    )

    book = ExecutionBook(execution_unit_of_work=ObservationUnitOfWork())
    book._persistence_failed = False
    await book.observe(evidence("current-epoch-no-historical-trades"))
    command = TradeCommand(
        "legacy", SCOPE.to_position_key(), TradeCommandType.ENTRY,
        StrategySide.LONG, EntryType.MARKET, Decimal(1), created_at=NOW,
    )
    book.register_prepared_command(command, SCOPE)
    await book.mark_unknown("legacy", "restored dispatch")
    order = SimpleNamespace(
        intent_id="intent", run_id="old-session", client_order_id="legacy",
        exchange_order_id="111", symbol="BTCUSDT", side="BUY",
        order_type="MARKET", quantity=Decimal(1), price=None, reduce_only=False,
        position_side="LONG", state="filled", created_at=NOW, updated_at=NOW,
        time_in_force=None, expires_at=None, executed_quantity=Decimal(1),
    )
    historical_fill = replace(fill("historical-trade", "1", entry=True), order_id="111")
    if trade_in_current_cut:
        await book.observe(evidence("same-trade-in-current-cut", fill=historical_fill))
    session = AsyncMock()
    session.__aenter__.return_value = session
    session.scalar.side_effect = [order, {
        "environment": "live", "account_label": "primary",
        "symbol": "BTCUSDT", "position_side": "LONG",
    }]

    async def rows(query):
        sql = str(query)
        values = []
        if "FROM account_fill_events" in sql:
            values = [historical_fill]
        elif "FROM exchange_order_events" in sql and priced_receipt:
            values = [SimpleNamespace(details={
                "executed_quantity": "1", "average_price": "100",
            })]
        return Mock(all=Mock(return_value=values))

    session.scalars.side_effect = rows
    orders = PostgresOrderReadRepository(Mock(return_value=session))

    class NoExchangeCalls:
        def __getattr__(self, name):
            raise AssertionError(f"unexpected exchange operation: {name}")

    coordinator = OrderExecutionCoordinator(
        backend=NoExchangeCalls(), account_label="primary", environment="live",
        execution_book=book,
    )
    try:
        assert not await recover_restored_commands(
            book=book, coordinator=coordinator, orders=orders,
            reconcile_order=coordinator.reconcile_order,
        )
        assert not book.command_requires_recovery("legacy")
        assert book.get_outbox("legacy").external_order_id == "111"
        # Historical settlement proof must not recreate this closed position.
        assert (await book.read(SCOPE)).total_quantity == int(trade_in_current_cut)
        expected_fills = (historical_fill,) if trade_in_current_cut else ()
        assert book._journals[SCOPE.to_position_key().canonical_id].read_cut().fills == expected_fills
    finally:
        await coordinator.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state", [ExchangeOrderState.FILLED, ExchangeOrderState.CANCELED]
)
async def test_terminal_read_model_does_not_hide_restored_dispatch_gate(state):
    book = ExecutionBook(execution_unit_of_work=ObservationUnitOfWork())
    book._persistence_failed = False
    quantity = Decimal(1) if state == ExchangeOrderState.FILLED else Decimal(0)
    price = Decimal(100) if quantity else Decimal(0)
    await book.observe(
        evidence(
            "legacy-facts",
            fill=replace(fill("real-trade", "1", entry=True), order_id="111")
            if quantity
            else None,
        )
    )
    command = TradeCommand(
        "legacy",
        SCOPE.to_position_key(),
        TradeCommandType.ENTRY,
        StrategySide.LONG,
        EntryType.MARKET,
        Decimal(1),
        created_at=NOW,
    )
    book.register_prepared_command(command, SCOPE)
    await book.mark_dispatching(command.command_id)
    await book.mark_unknown(command.command_id, "recovered dispatch")
    plan = OrderExecutionPlan(
        "intent",
        "old-session",
        "legacy",
        "BTCUSDT",
        "BUY",
        "MARKET",
        Decimal(1),
        None,
        False,
        NOW,
        position_side=SCOPE.position_side,
    )
    receipt = PersistedOrderReceipt(
        "legacy", state, "111" if quantity else None, quantity, price
    )
    persisted = PersistedExchangeOrder(
        plan, state, receipt.exchange_order_id, NOW, quantity, receipt
    )

    class Orders:
        async def load_unresolved_orders(self, run_id):
            return ()  # The durable read model is already terminal.

        async def load_order(self, client_order_id):
            assert client_order_id == "legacy"
            return persisted

    class NoExchangeCalls:
        def __getattr__(self, name):
            raise AssertionError(f"unexpected exchange operation: {name}")

    orders = Orders()
    coordinator = OrderExecutionCoordinator(
        backend=NoExchangeCalls(),
        account_label="primary",
        environment="live",
        execution_book=book,
    )
    runtime = LiveOrderReconciliation(orders, coordinator, "new-session")
    try:
        # The previous unresolved-orders-only scan leaves this command blocked.
        await runtime.reconcile_all(include_confirmed=True)
        assert book.command_requires_recovery("legacy")
        runtime.recover_commands = lambda reconcile_order: recover_restored_commands(
            book=book, coordinator=coordinator, orders=orders,
            reconcile_order=reconcile_order,
        )
        assert not await runtime.reconcile_all(include_confirmed=True)
        assert book.get_outbox("legacy").state == DispatchState.TERMINAL
        assert not book.command_requires_recovery("legacy")
        assert (await book.read(SCOPE)).total_quantity == quantity
        assert not await runtime.reconcile_all()
    finally:
        await coordinator.aclose()


@pytest.mark.asyncio
async def test_receipt_without_real_trades_keeps_settlement_gate():
    book = ExecutionBook(execution_unit_of_work=ObservationUnitOfWork())
    book._persistence_failed = False
    await book.observe(evidence("no-trades"))
    command = TradeCommand(
        "legacy",
        SCOPE.to_position_key(),
        TradeCommandType.ENTRY,
        StrategySide.LONG,
        EntryType.MARKET,
        Decimal(1),
        created_at=NOW,
    )
    book.register_prepared_command(command, SCOPE)
    await book.mark_unknown("legacy", "recovered dispatch")
    plan = OrderExecutionPlan(
        "intent",
        "old-session",
        "legacy",
        "BTCUSDT",
        "BUY",
        "MARKET",
        Decimal(1),
        None,
        False,
        NOW,
        position_side=SCOPE.position_side,
    )
    coordinator = OrderExecutionCoordinator(
        backend=object(),
        account_label="primary",
        environment="live",
        execution_book=book,
    )
    try:
        await coordinator.observe_recovered_receipt(
            plan,
            PersistedOrderReceipt(
                "legacy",
                ExchangeOrderState.FILLED,
                "111",
                Decimal(1),
                Decimal(100),
            ),
        )
        assert book.command_requires_recovery("legacy")
        assert (await book.read(SCOPE)).total_quantity == 0
    finally:
        await coordinator.aclose()


async def test_restored_completed_exit_reservation_can_prove_terminal_settlement():
    from crypto_momentum_lab.domain.execution.trade_command import PositionReservation

    class RestoreUnitOfWork(ObservationUnitOfWork):
        async def load_positions(self, **kwargs):
            return ()

    reservation = PositionReservation(
        reservation_id="closed-reservation", command_id="exit",
        position_key=SCOPE.to_position_key(), batch_id="closed-batch",
        reserved_quantity=Decimal("2"), consumed_quantity=Decimal("2"),
        created_at=NOW,
    )
    commands = AsyncMock()
    commands.load_active_execution_commands.return_value = [{
        "command_id": "exit", "client_order_id": "exit", "command": "exit",
        "status": "terminal", "requested_at": NOW,
        "details": {
            "scope": {"environment": "live", "account_label": "primary",
                      "symbol": "BTCUSDT", "position_side": "LONG"},
            "side": "long", "order_type": "market", "quantity": "2",
            "reduce_only": True, "reservations": [reservation.reservation_id],
            "request_id": "exit", "attempt_count": 1, "external_order_id": "111",
        },
    }]
    reservations = AsyncMock()
    reservations.load_active_reservations.return_value = ()
    reservations.load_reservation.return_value = reservation
    book = ExecutionBook(
        execution_unit_of_work=RestoreUnitOfWork(), command_repository=commands,
        reservation_repository=reservations,
    )
    await book.restore(account_label="primary")
    await book.observe(evidence("restored-stream"))
    watermark_key = book._order_watermark_key(SCOPE.to_position_key(), "exit")
    book._order_cumulative_fills[watermark_key] = Decimal("2")
    book._order_cumulative_quotes[watermark_key] = Decimal("200")
    # A recovered head may still be gated after the durable reservation has settled.
    book._recovery_required_commands.add("exit")
    assert not book.get_active_reservations()
    plan = OrderExecutionPlan(
        "intent", "old-session", "exit", "BTCUSDT", "SELL", "MARKET",
        Decimal("2"), None, True, NOW, position_side=SCOPE.position_side,
    )
    coordinator = OrderExecutionCoordinator(
        backend=object(), account_label="primary", environment="live",
        execution_book=book,
    )
    try:
        await coordinator.observe_recovered_receipt(plan, PersistedOrderReceipt(
            "exit", ExchangeOrderState.FILLED, "111", Decimal("2"), Decimal("100"),
            (replace(fill("real-close", "2"), order_id="111"),),
        ))
        assert not book.command_requires_recovery("exit")
        assert (await book.read(SCOPE)).total_quantity == 0
        reservations.update_reservation.assert_not_awaited()
    finally:
        await coordinator.aclose()
