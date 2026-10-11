from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

from crypto_momentum_lab.domain.execution.order_read_models import (
    PersistedExchangeOrder,
)
from crypto_momentum_lab.domain.execution.order_result import OrderExecutionResult
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.live_rollout.pending_entries import (
    LivePendingEntryRegistry,
    TerminalOrderBarrier,
)

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


def test_a2a_terminal_then_late_ack_then_sync_does_not_recreate_pending() -> None:
    registry = LivePendingEntryRegistry(clock=lambda: NOW)
    plan1 = _market_plan(
        symbol="BTCUSDT", quantity=Decimal("10"), reference_price=Decimal("100")
    )
    plan2 = _market_plan(
        symbol="ETHUSDT", quantity=Decimal("2"), reference_price=Decimal("2000")
    )

    # Put plan2 in pending normally
    registry.remember(
        plan2,
        OrderExecutionResult(
            client_order_id=plan2.client_order_id,
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-2",
            executed_quantity=Decimal("0"),
            plan=plan2,
        ),
    )

    # 1. Terminal event arrives first for plan1
    terminal_event = ExchangeOrderEvent(
        event_id="evt-1",
        client_order_id=plan1.client_order_id,
        state=ExchangeOrderState.CANCELED,
        occurred_at=NOW,
        exchange_order_id="ex-1",
        details={"executed_quantity": "0"},
    )
    registry.observe_order_event(plan1, terminal_event)

    # 2. Late REST ACK arrives for plan1
    late_ack = OrderExecutionResult(
        client_order_id=plan1.client_order_id,
        state=ExchangeOrderState.ACKNOWLEDGED,
        exchange_order_id="ex-1",
        executed_quantity=Decimal("0"),
        plan=plan1,
    )
    registry.remember(plan1, late_ack)

    # 3. Sync with empty unresolved orders
    # (plan1 is durable-canceled, not in unresolved)
    ctx = SimpleNamespace(
        unresolved_orders=(),
        managed_positions=(),
    )
    registry.sync(ctx)

    # Assert: plan1 is NOT in pending, notional for plan1 is 0, plan2 preserved!
    assert plan1.client_order_id not in registry._pending
    assert not registry.has_uncertain_entry("BTCUSDT")
    notional, symbols = registry.reservation(())
    assert symbols == frozenset({"ETHUSDT"})
    assert notional == Decimal("4000")


def test_a2b_filled_or_partially_filled_canceled_then_late_ack_respects_batches(
) -> None:
    registry = LivePendingEntryRegistry(clock=lambda: NOW)
    plan = _market_plan(
        symbol="BTCUSDT", quantity=Decimal("10"), reference_price=Decimal("100")
    )

    # Scenario 1: PARTIALLY_FILLED cum=3 then CANCELED cum=3
    partial_event = ExchangeOrderEvent(
        event_id="evt-cancel",
        client_order_id=plan.client_order_id,
        state=ExchangeOrderState.CANCELED,
        occurred_at=NOW,
        exchange_order_id="ex-part",
        details={"executed_quantity": "3"},
    )
    registry.observe_order_event(plan, partial_event)

    # Late ACK arrives
    registry.remember(
        plan,
        OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-part",
            executed_quantity=Decimal("0"),
            plan=plan,
        ),
    )

    # No pending rebuilt: remaining 7 not reserved, only 3 settled is reserved
    assert plan.client_order_id not in registry._pending
    notional, symbols = registry.reservation(())
    assert symbols == frozenset({"BTCUSDT"})
    assert notional == Decimal("300")  # 3 executed * 100

    # Once batch arrives in managed positions, settled reservation is cleared
    context = SimpleNamespace(
        unresolved_orders=(),
        managed_positions=(
            SimpleNamespace(
                symbol="BTCUSDT",
                batches=(
                    SimpleNamespace(
                        entry_client_order_ids=frozenset({plan.client_order_id}),
                        entry_exchange_order_ids=frozenset({"ex-part"}),
                    ),
                ),
            ),
        ),
    )
    registry.sync(context)
    notional_after, symbols_after = registry.reservation(())
    assert symbols_after == frozenset()
    assert notional_after == Decimal("0")


def test_terminal_barrier_bounded_lru_and_durable_monotonicity() -> None:
    durable_db: dict[str, Decimal] = {"order-1": Decimal("0.5")}

    def durable_reader(client_order_id: str) -> tuple[bool, Decimal] | None:
        if client_order_id in durable_db:
            return True, durable_db[client_order_id]
        return None

    # Capacity is strictly 2
    barrier = TerminalOrderBarrier(capacity=2, durable_reader=durable_reader)

    # Record order-1 (0.5), order-2 (1.0)
    barrier.record_terminal("order-1", Decimal("0.5"))
    barrier.record_terminal("order-2", Decimal("1.0"))
    assert len(barrier._terminal_orders) == 2

    # Record order-3 (2.0) -> evicts order-1 from memory LRU
    barrier.record_terminal("order-3", Decimal("2.0"))
    assert len(barrier._terminal_orders) == 2
    assert "order-1" not in barrier._terminal_orders

    # 1. Bounded capacity invariant: memory is strictly bounded, no unbounded set
    assert not hasattr(barrier, "_evicted_terminal_ids")

    # 2. Querying evicted order-1 leverages durable_reader, returning 0.5 (never 0)
    assert barrier.is_terminal("order-1")
    assert barrier.get_executed_quantity("order-1") == Decimal("0.5")

    # 3. Late arriving 0.2 event must not regress executed quantity from 0.5 to 0.2!
    updated = barrier.record_terminal("order-1", Decimal("0.2"))
    assert updated == Decimal("0.5")
    assert barrier.get_executed_quantity("order-1") == Decimal("0.5")


def test_evicted_zero_fill_order_cannot_be_resurrected_by_late_ack() -> None:
    # Capacity is strictly 2
    barrier = TerminalOrderBarrier(capacity=2)
    registry = LivePendingEntryRegistry(clock=lambda: NOW, terminal_barrier=barrier)

    plan_zero = _market_plan(
        symbol="BTCUSDT", quantity=Decimal("1"), reference_price=Decimal("100")
    )
    # Order-zero is CANCELED with 0 executed quantity
    zero_cancel_event = ExchangeOrderEvent(
        event_id="evt-cancel-zero",
        client_order_id=plan_zero.client_order_id,
        state=ExchangeOrderState.CANCELED,
        occurred_at=NOW,
        exchange_order_id="ex-zero",
        details={"executed_quantity": "0"},
    )
    registry.observe_order_event(plan_zero, zero_cancel_event)
    assert barrier.is_terminal(plan_zero.client_order_id)

    # Now add two more terminal orders to evict order_zero from barrier LRU cache
    barrier.record_terminal("order-x", Decimal("1"))
    barrier.record_terminal("order-y", Decimal("2"))
    assert plan_zero.client_order_id not in barrier._terminal_orders

    # Even though evicted from barrier._terminal_orders,
    # registry's fallback remembers it
    assert registry.is_terminal(plan_zero.client_order_id)

    # Late ACK arrives for plan_zero
    late_ack = OrderExecutionResult(
        client_order_id=plan_zero.client_order_id,
        state=ExchangeOrderState.ACKNOWLEDGED,
        exchange_order_id="ex-zero",
        executed_quantity=Decimal("0"),
        plan=plan_zero,
    )
    registry.remember(plan_zero, late_ack)

    # Must NOT resurrect into pending
    assert plan_zero.client_order_id not in registry._pending
    notional, symbols = registry.reservation(())
    assert notional == Decimal("0")
    assert symbols == frozenset()


def test_multi_order_batch_quantity_capped_to_plan_quantity_in_sync() -> None:
    registry = LivePendingEntryRegistry(clock=lambda: NOW)
    plan1 = replace(
        _market_plan(
            symbol="BTCUSDT", quantity=Decimal("1"), reference_price=Decimal("100")
        ),
        client_order_id="order-1",
    )
    plan2 = replace(
        _market_plan(
            symbol="BTCUSDT", quantity=Decimal("2"), reference_price=Decimal("100")
        ),
        client_order_id="order-2",
    )

    registry.remember(
        plan1,
        OrderExecutionResult(
            client_order_id=plan1.client_order_id,
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-1",
            executed_quantity=Decimal("0"),
            plan=plan1,
        ),
    )
    registry.remember(
        plan2,
        OrderExecutionResult(
            client_order_id=plan2.client_order_id,
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-2",
            executed_quantity=Decimal("0"),
            plan=plan2,
        ),
    )

    # A batch arrives containing both entry orders, with batch total quantity = 3
    context = SimpleNamespace(
        unresolved_orders=(),
        managed_positions=(
            SimpleNamespace(
                symbol="BTCUSDT",
                batches=(
                    SimpleNamespace(
                        quantity=Decimal("3"),
                        entry_client_order_ids=frozenset(
                            {plan1.client_order_id, plan2.client_order_id}
                        ),
                        entry_exchange_order_ids=frozenset({"ex-1", "ex-2"}),
                    ),
                ),
            ),
        ),
    )
    registry.sync(context)

    # Order 1 must be capped to plan quantity 1 (NOT 3!)
    # Order 2 must be capped to plan quantity 2 (NOT 3!)
    qty1 = registry._counted_entry_quantities.get(plan1.client_order_id)
    qty2 = registry._counted_entry_quantities.get(plan2.client_order_id)
    assert qty1 == Decimal("1")
    assert qty2 == Decimal("2")


def test_partially_filled_working_order_is_not_marked_terminal_in_sync() -> None:
    registry = LivePendingEntryRegistry(clock=lambda: NOW)
    plan = _market_plan(
        symbol="BTCUSDT", quantity=Decimal("10"), reference_price=Decimal("100")
    )
    # 1. Order is submitted, initially 0 executed
    registry.remember(
        plan,
        OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-part",
            executed_quantity=Decimal("0"),
            plan=plan,
        ),
    )
    assert plan.client_order_id in registry._pending

    # 2. Exchange reports partial fill of 2 via unresolved orders and position batch
    unresolved_item = PersistedExchangeOrder(
        plan=plan,
        state=ExchangeOrderState.PARTIALLY_FILLED,
        exchange_order_id="ex-part",
        updated_at=NOW,
        executed_quantity=Decimal("2"),
    )
    context = SimpleNamespace(
        unresolved_orders=(unresolved_item,),
        managed_positions=(
            SimpleNamespace(
                symbol="BTCUSDT",
                batches=(
                    SimpleNamespace(
                        quantity=Decimal("2"),
                        entry_client_order_ids=frozenset({plan.client_order_id}),
                        entry_exchange_order_ids=frozenset({"ex-part"}),
                    ),
                ),
            ),
        ),
    )
    registry.sync(context)

    # 3. Critical invariant: Working partially filled order MUST NOT be marked terminal!
    assert not registry.is_terminal(plan.client_order_id)
    assert plan.client_order_id in registry._pending
    # Pending record updated to executed_quantity=2, remaining 8 still tracked
    assert registry._pending[plan.client_order_id][1] == Decimal("2")


def test_multiple_orders_sharing_batch_do_not_overallocate() -> None:
    registry = LivePendingEntryRegistry(clock=lambda: NOW)
    plan1 = _market_plan("BTCUSDT", quantity=Decimal("5.0"))
    plan1 = replace(plan1, client_order_id="order-1")
    plan2 = _market_plan("BTCUSDT", quantity=Decimal("5.0"))
    plan2 = replace(plan2, client_order_id="order-2")

    registry.remember(
        plan1,
        OrderExecutionResult(
            client_order_id="order-1",
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-1",
            executed_quantity=Decimal("0"),
            plan=plan1,
        ),
    )
    registry.remember(
        plan2,
        OrderExecutionResult(
            client_order_id="order-2",
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-2",
            executed_quantity=Decimal("0"),
            plan=plan2,
        ),
    )

    # Batch has total quantity 8 (order 1 filled 5, order 2 partially filled 3)
    unresolved_item2 = PersistedExchangeOrder(
        plan=plan2,
        state=ExchangeOrderState.PARTIALLY_FILLED,
        exchange_order_id="ex-2",
        updated_at=NOW,
        executed_quantity=Decimal("3"),
    )
    context = SimpleNamespace(
        unresolved_orders=(unresolved_item2,),
        managed_positions=(
            SimpleNamespace(
                symbol="BTCUSDT",
                batches=(
                    SimpleNamespace(
                        quantity=Decimal("8"),
                        entry_client_order_ids=("order-1", "order-2"),
                        entry_exchange_order_ids=("ex-1", "ex-2"),
                    ),
                ),
            ),
        ),
    )
    registry.sync(context)

    # Order 1 filled 5 (equal to plan 5), so marked terminal
    assert registry.is_terminal("order-1")
    # Order 2 partially filled 3 (plan 5), MUST NOT be marked terminal!
    assert not registry.is_terminal("order-2")
    assert "order-2" in registry._pending
    assert registry._pending["order-2"][1] == Decimal("3")


def test_multiple_unresolved_orders_sync_uses_own_order_state() -> None:
    registry = LivePendingEntryRegistry(clock=lambda: NOW)
    plan1 = replace(
        _market_plan("BTCUSDT", quantity=Decimal("10.0")),
        client_order_id="order-1",
    )
    plan2 = replace(
        _market_plan("ETHUSDT", quantity=Decimal("10.0")),
        client_order_id="order-2",
    )

    registry.remember(
        plan1,
        OrderExecutionResult(
            client_order_id="order-1",
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-1",
            executed_quantity=Decimal("0"),
            plan=plan1,
        ),
    )
    registry.remember(
        plan2,
        OrderExecutionResult(
            client_order_id="order-2",
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-2",
            executed_quantity=Decimal("0"),
            plan=plan2,
        ),
    )

    # Order 1 is partially filled at 4. Order 2 is partially filled at 7.
    # Order 2 is last in unresolved_orders.
    unresolved1 = PersistedExchangeOrder(
        plan=plan1,
        state=ExchangeOrderState.PARTIALLY_FILLED,
        exchange_order_id="ex-1",
        updated_at=NOW,
        executed_quantity=Decimal("4"),
    )
    unresolved2 = PersistedExchangeOrder(
        plan=plan2,
        state=ExchangeOrderState.PARTIALLY_FILLED,
        exchange_order_id="ex-2",
        updated_at=NOW,
        executed_quantity=Decimal("7"),
    )
    context = SimpleNamespace(
        unresolved_orders=(unresolved1, unresolved2),
        managed_positions=(),
    )
    registry.sync(context)

    # Order 1 must track executed 4, NOT order 2's executed 7!
    assert registry._pending["order-1"][1] == Decimal("4")
    assert registry._pending["order-2"][1] == Decimal("7")


def test_terminal_order_eviction_does_not_allow_resurrection() -> None:
    barrier = TerminalOrderBarrier(capacity=5)
    # Record 10 terminal orders to force LRU eviction in barrier
    for i in range(10):
        barrier.record_terminal(f"order-{i}", Decimal("1.0"))

    # Early order-0 was evicted from capacity 5, but its identity is in tombstones!
    assert barrier.is_terminal("order-0")

    # In LivePendingEntryRegistry:
    registry = LivePendingEntryRegistry(
        clock=lambda: NOW, terminal_capacity=5, terminal_barrier=barrier
    )
    # Late ACK for order-0 arriving after eviction must NOT be added to pending!
    plan0 = replace(
        _market_plan("BTCUSDT", quantity=Decimal("1.0")),
        client_order_id="order-0",
    )
    registry.remember(
        plan0,
        OrderExecutionResult(
            client_order_id="order-0",
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-0",
            executed_quantity=Decimal("0"),
            plan=plan0,
        ),
    )
    assert "order-0" not in registry._pending
    assert registry.is_terminal("order-0")


def test_order_plan_quantities_bounded_and_pruned() -> None:
    registry = LivePendingEntryRegistry(clock=lambda: NOW, terminal_capacity=10)
    # Add 50 orders
    for i in range(50):
        plan = replace(
            _market_plan("BTCUSDT", quantity=Decimal("1.0")),
            client_order_id=f"order-{i}",
        )
        registry.remember(
            plan,
            OrderExecutionResult(
                client_order_id=f"order-{i}",
                state=ExchangeOrderState.FILLED,
                exchange_order_id=f"ex-{i}",
                executed_quantity=Decimal("1.0"),
                plan=plan,
            ),
        )

    # Trigger sync to clean up inactive plan quantities
    context = SimpleNamespace(unresolved_orders=(), managed_positions=())
    registry.sync(context)

    # Cache must be bounded to <= 2 * terminal_capacity (20)
    assert len(registry._order_plan_quantities) <= 20
