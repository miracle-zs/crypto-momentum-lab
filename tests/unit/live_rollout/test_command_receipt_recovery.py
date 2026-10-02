from dataclasses import replace
from decimal import Decimal

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
        runtime.recover_commands = lambda: recover_restored_commands(
            book=book, coordinator=coordinator, orders=orders
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
