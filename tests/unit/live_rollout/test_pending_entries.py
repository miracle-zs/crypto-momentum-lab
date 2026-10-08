from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

from crypto_momentum_lab.domain.execution.order_read_models import (
    PersistedExchangeOrder,
)
from crypto_momentum_lab.domain.execution.order_result import OrderExecutionResult
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
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
    plan = _market_plan(
        "BTCUSDT", quantity=Decimal("2.0"), reference_price=Decimal("50000")
    )

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


def test_unknown_entry_retains_slot_after_local_expiry() -> None:
    registry = LivePendingEntryRegistry(clock=lambda: NOW + timedelta(hours=1))
    plan = replace(_market_plan(), expires_at=NOW + timedelta(minutes=15))
    registry.remember(
        plan,
        OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
            exchange_order_id=None,
            executed_quantity=Decimal(0),
            plan=plan,
        ),
    )
    from tests.unit.live_rollout.test_daemon import _runtime_context

    registry.sync(_runtime_context())
    assert registry.has_uncertain_entry("BTCUSDT")
    assert len(registry.snapshot()) == 1


def test_persisted_market_pending_orders_reserve_symbol_and_notional() -> None:
    registry = LivePendingEntryRegistry(clock=lambda: NOW)
    plan = _market_plan(
        "ETHUSDT", quantity=Decimal("10.0"), reference_price=Decimal("3000")
    )

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


def test_filled_slot_waits_for_exchange_identity_and_is_never_cancelled() -> None:
    from tests.unit.live_rollout.test_daemon import _runtime_context

    registry = LivePendingEntryRegistry(clock=lambda: NOW)
    plan = _market_plan()
    registry.remember(
        plan,
        OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.FILLED,
            exchange_order_id="ex1",
            executed_quantity=plan.quantity,
            plan=plan,
        ),
    )
    assert registry.snapshot() == ()
    assert len(registry.admission_snapshot()) == 1
    assert registry.reservation(()) == (Decimal("100000"), frozenset({"BTCUSDT"}))
    context = replace(
        _runtime_context(),
        managed_positions=(
            SimpleNamespace(
                symbol="BTCUSDT",
                batches=(
                    SimpleNamespace(
                        entry_client_order_ids=frozenset(),
                        entry_exchange_order_ids=frozenset({"ex1"}),
                    ),
                ),
            ),
        ),
    )
    registry.sync(context)
    assert registry.admission_snapshot() == ()
    assert registry.reservation(()) == (Decimal(0), frozenset())
    assert registry._exchange_ids == {}
