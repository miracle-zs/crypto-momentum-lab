from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.execution.order_read_models import (
    PersistedExchangeOrder,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.order_result import OrderExecutionResult
from crypto_momentum_lab.live_rollout.pending_entries import LivePendingEntryRegistry

NOW = datetime(2026, 10, 3, tzinfo=UTC)


def _market_plan(
    symbol: str = "BTCUSDT",
    quantity: Decimal = Decimal("2.0"),
    reference_price: Decimal | None = Decimal("50000"),
) -> OrderExecutionPlan:
    return OrderExecutionPlan(
        intent_id=f"intent-{symbol}",
        run_id="run-1",
        client_order_id=f"order-{symbol}",
        symbol=symbol,
        side="BUY",
        order_type="MARKET",
        quantity=quantity,
        price=None,
        reduce_only=False,
        created_at=NOW,
        position_side=FuturesPositionSide.BOTH,
        quantized=True,
        reference_price=reference_price,
    )


def test_market_pending_entries_reserve_symbol_and_notional() -> None:
    registry = LivePendingEntryRegistry(clock=lambda: NOW)
    plan = _market_plan("BTCUSDT", quantity=Decimal("2.0"), reference_price=Decimal("50000"))

    # Remember in-flight market entry
    registry.remember(
        plan,
        OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.SUBMITTING,
            exchange_order_id=None,
            executed_quantity=Decimal("0"),
            plan=plan,
        ),
    )

    notional, symbols = registry.reservation(persisted_orders=())
    # Symbol must be reserved (not omitted)
    assert symbols == frozenset({"BTCUSDT"})
    # Notional must be 2.0 * 50000 = 100,000 (not 0)
    assert notional == Decimal("100000")


def test_persisted_market_pending_orders_reserve_symbol_and_notional() -> None:
    registry = LivePendingEntryRegistry(clock=lambda: NOW)
    plan = _market_plan("ETHUSDT", quantity=Decimal("10.0"), reference_price=Decimal("3000"))

    persisted = (
        PersistedExchangeOrder(
            plan=plan,
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-1",
            updated_at=NOW,
            executed_quantity=Decimal("2.0"),  # 8.0 remaining
        ),
    )

    notional, symbols = registry.reservation(persisted_orders=persisted)
    assert symbols == frozenset({"ETHUSDT"})
    # 8.0 remaining * 3000 = 24,000
    assert notional == Decimal("24000")
