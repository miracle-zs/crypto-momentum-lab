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
    # entry_client_order_ids is an unordered frozenset in production
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
                        entry_client_order_ids=frozenset(
                            {"order-2", "order-1"}
                        ),
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


def test_multi_order_batch_quantity_attribution_reversed_tuple() -> None:
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

    unresolved_item2 = PersistedExchangeOrder(
        plan=plan2,
        state=ExchangeOrderState.PARTIALLY_FILLED,
        exchange_order_id="ex-2",
        updated_at=NOW,
        executed_quantity=Decimal("3"),
    )
    # Explicitly reverse tuple to guarantee order-2 is processed first
    context = SimpleNamespace(
        unresolved_orders=(unresolved_item2,),
        managed_positions=(
            SimpleNamespace(
                symbol="BTCUSDT",
                batches=(
                    SimpleNamespace(
                        quantity=Decimal("8"),
                        entry_client_order_ids=("order-2", "order-1"),
                        entry_exchange_order_ids=("ex-2", "ex-1"),
                    ),
                ),
            ),
        ),
    )
    registry.sync(context)

    assert registry.is_terminal("order-1")
    assert not registry.is_terminal("order-2")
    assert "order-2" in registry._pending
    assert registry._pending["order-2"][1] == Decimal("3")


def test_multi_order_batch_attribution_terminal_partial_fill_capped() -> None:
    registry = LivePendingEntryRegistry(clock=lambda: NOW)
    plan_a = _market_plan("BTCUSDT", quantity=Decimal("10.0"))
    plan_a = replace(plan_a, client_order_id="order-A")
    plan_b = _market_plan("BTCUSDT", quantity=Decimal("10.0"))
    plan_b = replace(plan_b, client_order_id="order-B")

    # Order A was partially filled 3, then canceled (terminal!)
    registry.remember(
        plan_a,
        OrderExecutionResult(
            client_order_id="order-A",
            state=ExchangeOrderState.CANCELED,
            exchange_order_id="ex-A",
            executed_quantity=Decimal("3.0"),
            plan=plan_a,
        ),
    )
    # Order B was acknowledged
    registry.remember(
        plan_b,
        OrderExecutionResult(
            client_order_id="order-B",
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-B",
            executed_quantity=Decimal("0"),
            plan=plan_b,
        ),
    )

    # Batch total quantity is 13 (order A executed 3, order B executed 10)
    # Order A is CANCELED, so it is NOT in unresolved_orders!
    # Test with Order A sorted first: ("order-A", "order-B")
    context = SimpleNamespace(
        unresolved_orders=(),
        managed_positions=(
            SimpleNamespace(
                symbol="BTCUSDT",
                batches=(
                    SimpleNamespace(
                        quantity=Decimal("13.0"),
                        entry_client_order_ids=("order-A", "order-B"),
                        entry_exchange_order_ids=("ex-A", "ex-B"),
                    ),
                ),
            ),
        ),
    )
    registry.sync(context)

    # Order A must be attributed exactly 3, NEVER 10!
    assert registry._counted_entry_quantities["order-A"] == Decimal("3.0")
    # Order B must be attributed 10
    assert registry._counted_entry_quantities["order-B"] == Decimal("10.0")


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
    durable_store: dict[str, tuple[bool, Decimal]] = {
        f"order-{i}": (True, Decimal("1.0")) for i in range(10)
    }
    barrier = TerminalOrderBarrier(
        capacity=5,
        durable_reader=lambda cid: durable_store.get(cid),
    )
    # Record 10 terminal orders to force LRU eviction in barrier
    for i in range(10):
        barrier.record_terminal(f"order-{i}", Decimal("1.0"))

    # LRU barrier maintains bounded capacity (no memory leak)
    assert len(barrier._terminal_orders) == 5
    # No unbounded tombstone set
    assert not hasattr(barrier, "_terminal_tombstones")

    # Early order-0 was evicted from LRU, but durable storage protects it
    assert barrier.is_terminal("order-0")
    # Reading it into barrier does not exceed capacity
    assert len(barrier._terminal_orders) == 5

    # In LivePendingEntryRegistry:
    registry = LivePendingEntryRegistry(
        clock=lambda: NOW, terminal_capacity=5, terminal_barrier=barrier
    )
    assert not hasattr(registry, "_terminal_tombstones")
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


async def test_terminal_barrier_durable_async_checker_protects_evicted() -> None:
    durable_db: dict[str, tuple[bool, Decimal]] = {
        "pre-restart-order": (True, Decimal("2.5")),
        "evicted-order": (True, Decimal("1.0")),
    }

    async def mock_is_order_terminal(
        cid: str,
    ) -> tuple[bool, Decimal] | None:
        return durable_db.get(cid)

    barrier = TerminalOrderBarrier(
        capacity=3,
        durable_async_checker=mock_is_order_terminal,
    )
    assert not barrier.is_terminal("pre-restart-order")

    res = await barrier.check_durable_terminal("pre-restart-order")
    assert res == (True, Decimal("2.5"))
    assert barrier.is_terminal("pre-restart-order")
    assert barrier.get_executed_quantity("pre-restart-order") == Decimal("2.5")

    barrier.record_terminal("order-1", Decimal("1.0"))
    barrier.record_terminal("order-2", Decimal("1.0"))
    barrier.record_terminal("order-3", Decimal("1.0"))
    assert not barrier.is_terminal("pre-restart-order")
    assert len(barrier._terminal_orders) == 3

    res2 = await barrier.check_durable_terminal("pre-restart-order")
    assert res2 == (True, Decimal("2.5"))
    assert barrier.is_terminal("pre-restart-order")
    assert len(barrier._terminal_orders) == 3


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


def test_multi_order_batch_attribution_terminal_partial_fill_evicted() -> None:
    durable_store: dict[str, tuple[bool, Decimal]] = {
        "order-A": (True, Decimal("3.0")),
    }
    barrier = TerminalOrderBarrier(
        capacity=2,
        durable_reader=lambda cid: durable_store.get(cid),
    )
    registry = LivePendingEntryRegistry(
        clock=lambda: NOW,
        terminal_capacity=2,
        terminal_barrier=barrier,
    )
    plan_a = replace(
        _market_plan("BTCUSDT", quantity=Decimal("10.0")),
        client_order_id="order-A",
    )
    plan_b = replace(
        _market_plan("BTCUSDT", quantity=Decimal("10.0")),
        client_order_id="order-B",
    )

    registry.remember(
        plan_a,
        OrderExecutionResult(
            client_order_id="order-A",
            state=ExchangeOrderState.CANCELED,
            exchange_order_id="ex-A",
            executed_quantity=Decimal("3.0"),
            plan=plan_a,
        ),
    )
    barrier.record_terminal("other-1", Decimal("1.0"))
    barrier.record_terminal("other-2", Decimal("1.0"))
    assert "order-A" not in barrier._terminal_orders

    registry.remember(
        plan_b,
        OrderExecutionResult(
            client_order_id="order-B",
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-B",
            executed_quantity=Decimal("0"),
            plan=plan_b,
        ),
    )

    context = SimpleNamespace(
        unresolved_orders=(),
        managed_positions=(
            SimpleNamespace(
                symbol="BTCUSDT",
                batches=(
                    SimpleNamespace(
                        quantity=Decimal("13.0"),
                        entry_client_order_ids=("order-A", "order-B"),
                        entry_exchange_order_ids=("ex-A", "ex-B"),
                    ),
                ),
            ),
        ),
    )
    registry.sync(context)
    assert registry._counted_entry_quantities["order-A"] == Decimal("3.0")
    assert registry._counted_entry_quantities["order-B"] == Decimal("10.0")


async def test_terminal_barrier_negative_cache_suppresses_checks() -> None:
    checker_calls: list[str] = []

    async def mock_is_order_terminal(
        cid: str,
    ) -> tuple[bool, Decimal] | None:
        checker_calls.append(cid)
        return None

    barrier = TerminalOrderBarrier(
        capacity=5,
        durable_async_checker=mock_is_order_terminal,
        negative_cache_ttl=5.0,
    )

    res1 = await barrier.check_durable_terminal("active-order-1")
    assert res1 is None
    assert checker_calls == ["active-order-1"]

    res2 = await barrier.check_durable_terminal("active-order-1")
    assert res2 is None
    assert checker_calls == ["active-order-1"]

    barrier.record_terminal("active-order-1", Decimal("5.0"))
    assert barrier.is_terminal("active-order-1")
    res3 = await barrier.check_durable_terminal("active-order-1")
    assert res3 == (True, Decimal("5.0"))


async def test_barrier_terminal_discovery_clears_pending_entry() -> None:
    durable_db: dict[str, tuple[bool, Decimal]] = {}

    async def mock_is_order_terminal(
        cid: str,
    ) -> tuple[bool, Decimal] | None:
        return durable_db.get(cid)

    registry = LivePendingEntryRegistry(clock=lambda: NOW)
    barrier = registry.terminal_barrier
    barrier._durable_async_checker = mock_is_order_terminal

    plan = replace(
        _market_plan("BTCUSDT", quantity=Decimal("1.0")),
        client_order_id="order-pending",
    )
    registry.remember(
        plan,
        OrderExecutionResult(
            client_order_id="order-pending",
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-p",
            executed_quantity=Decimal("0"),
            plan=plan,
        ),
    )
    assert "order-pending" in registry._pending

    durable_db["order-pending"] = (True, Decimal("1.0"))
    res = await barrier.check_durable_terminal("order-pending")
    assert res == (True, Decimal("1.0"))

    assert "order-pending" not in registry._pending
    assert registry.is_terminal("order-pending")


def test_multi_order_batch_attribution_unproven_order_never_terminal_and_no_starvation(
) -> None:
    registry = LivePendingEntryRegistry(clock=lambda: NOW)
    plan_active = replace(
        _market_plan("BTCUSDT", quantity=Decimal("5.0")),
        client_order_id="order-active",
    )
    registry.remember(
        plan_active,
        OrderExecutionResult(
            client_order_id="order-active",
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-act",
            executed_quantity=Decimal("0"),
            plan=plan_active,
        ),
    )
    # order-unproven is a cache miss: not in barrier, not in pending, not in persisted
    context = SimpleNamespace(
        unresolved_orders=(),
        managed_positions=(
            SimpleNamespace(
                symbol="BTCUSDT",
                batches=(
                    SimpleNamespace(
                        quantity=Decimal("8.0"),
                        entry_client_order_ids=("order-unproven", "order-active"),
                        entry_exchange_order_ids=(),
                    ),
                ),
            ),
        ),
    )
    registry.sync(context)

    # Active order gets prioritized first and is allocated 5.0
    assert registry._counted_entry_quantities.get("order-active") == Decimal("5.0")
    # Residual 3.0 is allocated to order-unproven for position accounting
    assert registry._counted_entry_quantities.get("order-unproven") == Decimal("3.0")
    # Two unproven orders in one batch cannot be told apart, so NEITHER split is
    # evidence of terminality -- including the one that lands on the plan size.
    assert not registry.is_terminal("order-unproven")
    assert not registry.is_terminal("order-active")
    assert "order-active" in registry._pending
    assert registry._guessed_counted_cids == {"order-active", "order-unproven"}


def test_forced_residual_for_single_unproven_order_is_evidence() -> None:
    """One unproven order left in a batch is forced by the batch total."""
    registry = LivePendingEntryRegistry(clock=lambda: NOW)
    plan1 = replace(
        _market_plan("BTCUSDT", quantity=Decimal("5.0")),
        client_order_id="order-1",
    )
    plan2 = replace(
        _market_plan("BTCUSDT", quantity=Decimal("5.0")),
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
    # order-2 is proven at 3 via the durable read model; batch total 8 leaves
    # exactly 5 for order-1, which is its plan quantity.
    unresolved2 = PersistedExchangeOrder(
        plan=plan2,
        state=ExchangeOrderState.PARTIALLY_FILLED,
        exchange_order_id="ex-2",
        updated_at=NOW,
        executed_quantity=Decimal("3"),
    )
    context = SimpleNamespace(
        unresolved_orders=(unresolved2,),
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

    assert registry._counted_entry_quantities["order-1"] == Decimal("5")
    assert registry._guessed_counted_cids == set()
    assert registry.is_terminal("order-1")


def test_incremental_fill_for_counted_order_reserves_uncounted_exposure() -> None:
    registry = LivePendingEntryRegistry(clock=lambda: NOW)
    plan = replace(
        _market_plan(
            "BTCUSDT",
            quantity=Decimal("10.0"),
            reference_price=Decimal("100"),
        ),
        client_order_id="order-inc",
    )
    registry.remember(
        plan,
        OrderExecutionResult(
            client_order_id="order-inc",
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-inc",
            executed_quantity=Decimal("0"),
            plan=plan,
        ),
    )

    # First batch arrives with quantity 5 (partially counted)
    unresolved = PersistedExchangeOrder(
        plan=plan,
        state=ExchangeOrderState.PARTIALLY_FILLED,
        exchange_order_id="ex-inc",
        updated_at=NOW,
        executed_quantity=Decimal("5.0"),
    )
    context1 = SimpleNamespace(
        unresolved_orders=(unresolved,),
        managed_positions=(
            SimpleNamespace(
                symbol="BTCUSDT",
                batches=(
                    SimpleNamespace(
                        quantity=Decimal("5.0"),
                        entry_client_order_ids=("order-inc",),
                        entry_exchange_order_ids=("ex-inc",),
                    ),
                ),
            ),
        ),
    )
    registry.sync(context1)
    assert registry._counted_entry_quantities["order-inc"] == Decimal("5.0")
    assert "order-inc" in registry._counted_entry_quantities

    # Later event arrives showing order terminated with executed=8.0
    # Uncounted increment is 8.0 - 5.0 = 3.0
    registry.on_terminal_discovered("order-inc", Decimal("8.0"))

    # The ledger keeps the identity and its total executed quantity, so the
    # release rule and dedupe can reconcile it against the batch view.
    assert "order-inc" in registry._settled
    assert registry._settled["order-inc"][1] == Decimal("8.0")

    # admission_snapshot and reservation must protect the uncounted increment
    admission = registry.admission_snapshot()
    assert len(admission) == 1
    assert admission[0][1] == Decimal("3.0")
    notional, symbols = registry.reservation(())
    assert notional == Decimal("300.0")  # 3.0 * 100
    assert symbols == frozenset({"BTCUSDT"})

    # When next position batch arrives with full quantity 8.0, _settled clears
    context2 = SimpleNamespace(
        unresolved_orders=(),
        managed_positions=(
            SimpleNamespace(
                symbol="BTCUSDT",
                batches=(
                    SimpleNamespace(
                        quantity=Decimal("8.0"),
                        entry_client_order_ids=("order-inc",),
                        entry_exchange_order_ids=("ex-inc",),
                    ),
                ),
            ),
        ),
    )
    registry.sync(context2)
    assert "order-inc" not in registry._settled
    assert registry.admission_snapshot() == ()
    notional_after, symbols_after = registry.reservation(())
    assert symbols_after == frozenset()
    assert notional_after == Decimal("0")


def test_terminal_event_never_lowers_observed_executed_quantity() -> None:
    registry = LivePendingEntryRegistry(clock=lambda: NOW)
    plan = replace(
        _market_plan(
            "BTCUSDT",
            quantity=Decimal("10"),
            reference_price=Decimal("100"),
        ),
        client_order_id="order-mono",
    )
    partial = ExchangeOrderEvent(
        event_id="evt-partial",
        client_order_id=plan.client_order_id,
        state=ExchangeOrderState.PARTIALLY_FILLED,
        occurred_at=NOW,
        exchange_order_id="ex-mono",
        details={"executed_quantity": "5"},
    )
    registry.remember(
        plan,
        OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-mono",
            executed_quantity=Decimal("0"),
            plan=plan,
        ),
    )
    registry.observe_order_event(plan, partial)
    assert registry._pending[plan.client_order_id][1] == Decimal("5")

    # A stale terminal event reports an older cumulative quantity of 3.
    stale_cancel = ExchangeOrderEvent(
        event_id="evt-stale",
        client_order_id=plan.client_order_id,
        state=ExchangeOrderState.CANCELED,
        occurred_at=NOW,
        exchange_order_id="ex-mono",
        details={"executed_quantity": "3"},
    )
    registry.observe_order_event(plan, stale_cancel)

    # I1: cumulative executed quantity is monotonic; the 5 stays filled.
    assert registry.get_executed_quantity(plan.client_order_id) == Decimal("5")
    assert registry._settled[plan.client_order_id][1] == Decimal("5")
    assert registry.reservation(()) == (Decimal("500"), frozenset({"BTCUSDT"}))


def test_shared_barrier_terminal_fact_never_lowers_observed_quantity() -> None:
    """Another component recording on the shared barrier keeps monotonicity."""
    registry = LivePendingEntryRegistry(clock=lambda: NOW)
    plan = replace(
        _market_plan(
            "BTCUSDT",
            quantity=Decimal("10"),
            reference_price=Decimal("100"),
        ),
        client_order_id="order-shared",
    )
    registry.remember(
        plan,
        OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.PARTIALLY_FILLED,
            exchange_order_id="ex-shared",
            executed_quantity=Decimal("5"),
            plan=plan,
        ),
    )
    assert registry._pending[plan.client_order_id][1] == Decimal("5")

    # A shared component (GTD lifecycle / order event runtime) records a stale
    # terminal quantity straight on the barrier.
    registry.terminal_barrier.record_terminal(
        plan.client_order_id, Decimal("3")
    )

    assert registry.get_executed_quantity(plan.client_order_id) == Decimal("5")
    assert registry._known_terminal_orders[plan.client_order_id] == Decimal("5")
    assert registry._settled[plan.client_order_id][1] == Decimal("5")
    assert registry.reservation(()) == (Decimal("500"), frozenset({"BTCUSDT"}))

    # A following sync must not regress the settlement from its own caches.
    context = SimpleNamespace(unresolved_orders=(), managed_positions=())
    registry.sync(context)
    assert registry._settled[plan.client_order_id][1] == Decimal("5")
    assert registry.reservation(()) == (Decimal("500"), frozenset({"BTCUSDT"}))


def test_remember_terminal_result_never_lowers_observed_quantity() -> None:
    registry = LivePendingEntryRegistry(clock=lambda: NOW)
    plan = replace(
        _market_plan(
            "BTCUSDT",
            quantity=Decimal("10"),
            reference_price=Decimal("100"),
        ),
        client_order_id="order-mono-ack",
    )
    registry.remember(
        plan,
        OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.PARTIALLY_FILLED,
            exchange_order_id="ex-mono-ack",
            executed_quantity=Decimal("5"),
            plan=plan,
        ),
    )
    registry.remember(
        plan,
        OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.CANCELED,
            exchange_order_id="ex-mono-ack",
            executed_quantity=Decimal("3"),
            plan=plan,
        ),
    )

    assert registry.get_executed_quantity(plan.client_order_id) == Decimal("5")
    assert registry._settled[plan.client_order_id][1] == Decimal("5")
    assert registry.reservation(()) == (Decimal("500"), frozenset({"BTCUSDT"}))


def test_settlement_ledger_keeps_evidence_and_converges_on_the_view() -> None:
    registry = LivePendingEntryRegistry(clock=lambda: NOW, terminal_capacity=3)
    plans = [
        replace(
            _market_plan("BTCUSDT", quantity=Decimal("1.0")),
            client_order_id=f"o-{i}",
        )
        for i in range(5)
    ]
    for plan in plans:
        registry.remember(
            plan,
            OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.FILLED,
                exchange_order_id=f"ex-{plan.client_order_id}",
                executed_quantity=plan.quantity,
                plan=plan,
            ),
        )

    # Over capacity the ledger keeps every identity (evidence is never dropped)
    # and closes entry admission until the view counts those fills.
    assert set(registry._settled) == {f"o-{i}" for i in range(5)}
    assert registry.settlement_pressure is True
    notional, symbols = registry.reservation(())
    assert notional == Decimal("250000")  # 5 x 1.0 x 50000
    assert symbols == frozenset({"BTCUSDT"})
    assert registry.pending_symbols() == frozenset({"BTCUSDT"})
    # The gate is global: an unrelated symbol is refused too, because the only
    # way the per-order ledger can grow is by admitting more entries.
    assert registry.entry_admission_blocked("BTCUSDT")
    assert registry.entry_admission_blocked("ETHUSDT")
    assert registry.has_uncertain_entry("ETHUSDT") is False

    # Once the position view reports the batches, every fill is released and
    # the exposure converges to zero.
    context = SimpleNamespace(
        unresolved_orders=(),
        managed_positions=(
            SimpleNamespace(
                symbol="BTCUSDT",
                batches=(
                    SimpleNamespace(
                        quantity=Decimal("5.0"),
                        entry_client_order_ids=tuple(f"o-{i}" for i in range(5)),
                        entry_exchange_order_ids=(),
                    ),
                ),
            ),
        ),
    )
    registry.sync(context)

    assert registry._settled == {}
    assert registry.settlement_pressure is False
    assert registry.reservation(()) == (Decimal("0"), frozenset())
    assert registry.pending_symbols() == frozenset()
    assert registry.entry_admission_blocked("BTCUSDT") is False
    assert registry.entry_admission_blocked("ETHUSDT") is False


def test_replayed_terminal_after_view_catches_up_does_not_double_count() -> None:
    """A late terminal replay must not reserve an already counted fill twice."""
    registry = LivePendingEntryRegistry(clock=lambda: NOW, terminal_capacity=1)
    plans = {}
    for cid in ("order-a", "order-b"):
        plan = replace(
            _market_plan(
                "BTCUSDT",
                quantity=Decimal("10"),
                reference_price=Decimal("100"),
            ),
            client_order_id=cid,
        )
        plans[cid] = plan
        registry.remember(
            plan,
            OrderExecutionResult(
                client_order_id=cid,
                state=ExchangeOrderState.FILLED,
                exchange_order_id=f"ex-{cid}",
                executed_quantity=plan.quantity,
                plan=plan,
            ),
        )
    assert registry.reservation(()) == (Decimal("2000"), frozenset({"BTCUSDT"}))

    # The view counts both fills: the ledger releases both of them.
    context = SimpleNamespace(
        unresolved_orders=(),
        managed_positions=(
            SimpleNamespace(
                symbol="BTCUSDT",
                batches=(
                    SimpleNamespace(
                        quantity=Decimal("20"),
                        entry_client_order_ids=("order-a", "order-b"),
                        entry_exchange_order_ids=(),
                    ),
                ),
            ),
        ),
    )
    registry.sync(context)
    assert registry._settled == {}
    assert registry.reservation(()) == (Decimal("0"), frozenset())

    # Replaying order-a's terminal event after the view caught up must not
    # reserve the same fill a second time.
    registry.on_terminal_discovered("order-a", Decimal("10"))
    assert registry.reservation(()) == (Decimal("0"), frozenset())
    registry.sync(context)
    assert registry._settled == {}
    assert registry.reservation(()) == (Decimal("0"), frozenset())

    # A replay while the view has not caught up keeps exactly one reservation,
    # even when the bounded plan cache already evicted that order's plan.
    registry2 = LivePendingEntryRegistry(clock=lambda: NOW, terminal_capacity=1)
    for cid in ("order-a", "order-b"):
        registry2.remember(
            plans[cid],
            OrderExecutionResult(
                client_order_id=cid,
                state=ExchangeOrderState.FILLED,
                exchange_order_id=f"ex-{cid}",
                executed_quantity=Decimal("10"),
                plan=plans[cid],
            ),
        )
    only_b = SimpleNamespace(
        unresolved_orders=(),
        managed_positions=(
            SimpleNamespace(
                symbol="BTCUSDT",
                batches=(
                    SimpleNamespace(
                        quantity=Decimal("10"),
                        entry_client_order_ids=("order-b",),
                        entry_exchange_order_ids=(),
                    ),
                ),
            ),
        ),
    )
    registry2.sync(only_b)
    assert set(registry2._settled) == {"order-a"}
    assert registry2.reservation(()) == (Decimal("1000"), frozenset({"BTCUSDT"}))
    registry2.on_terminal_discovered("order-a", Decimal("10"))
    assert registry2._settled["order-a"][1] == Decimal("10")
    assert registry2.reservation(()) == (Decimal("1000"), frozenset({"BTCUSDT"}))


async def test_durable_refresh_recovers_evicted_fact_before_attribution() -> None:
    """A fact evicted from the bounded caches must come back before sync."""
    durable_db: dict[str, tuple[bool, Decimal]] = {
        "order-evicted": (True, Decimal("2.0")),
    }

    async def durable_checker(cid: str):
        return durable_db.get(cid)

    registry = LivePendingEntryRegistry(clock=lambda: NOW)
    barrier = registry.terminal_barrier
    barrier._durable_async_checker = durable_checker

    plan_active = replace(
        _market_plan("BTCUSDT", quantity=Decimal("5.0")),
        client_order_id="order-active",
    )
    plan_evicted = replace(
        _market_plan("BTCUSDT", quantity=Decimal("5.0")),
        client_order_id="order-evicted",
    )
    for plan in (plan_active, plan_evicted):
        registry.remember(
            plan,
            OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.ACKNOWLEDGED,
                exchange_order_id=f"ex-{plan.client_order_id}",
                executed_quantity=Decimal("0"),
                plan=plan,
            ),
        )

    # The durable store knows order-evicted filled 2.0 before canceling, but the
    # local caches lost that fact; both batch orders are therefore unproven.
    context = SimpleNamespace(
        unresolved_orders=(),
        managed_positions=(
            SimpleNamespace(
                symbol="BTCUSDT",
                batches=(
                    SimpleNamespace(
                        quantity=Decimal("8.0"),
                        entry_client_order_ids=("order-active", "order-evicted"),
                        entry_exchange_order_ids=(),
                    ),
                ),
            ),
        ),
    )

    registry.sync(context)
    # Without the durable read the residual split is a plan-quantity guess.
    assert registry._counted_entry_quantities["order-evicted"] == Decimal("3.0")
    assert registry._guessed_counted_cids == {"order-active", "order-evicted"}

    await registry.refresh_durable_facts(context)
    registry.sync(context)

    # The durable fact (2.0) now drives attribution and is not a guess.
    assert registry._counted_entry_quantities["order-evicted"] == Decimal("2.0")
    assert registry._guessed_counted_cids == set()
    assert registry.is_terminal("order-evicted")


async def test_fresh_terminal_check_ignores_cached_non_terminal_verdict() -> None:
    durable_db: dict[str, tuple[bool, Decimal]] = {}
    calls: list[str] = []

    async def durable_checker(cid: str):
        calls.append(cid)
        return durable_db.get(cid)

    barrier = TerminalOrderBarrier(
        durable_async_checker=durable_checker,
        negative_cache_ttl=5.0,
    )
    # An earlier observation caches "not terminal" for this identity.
    assert await barrier.check_durable_terminal("order-late") is None
    assert calls == ["order-late"]

    durable_db["order-late"] = (True, Decimal("1.0"))

    # A cached verdict must not mask a later durable terminal transition ...
    assert await barrier.check_durable_terminal("order-late") is None
    # ... while a caller about to act on the identity reads through the cache.
    assert await barrier.check_durable_terminal("order-late", fresh=True) == (
        True,
        Decimal("1.0"),
    )
    assert calls == ["order-late", "order-late"]
