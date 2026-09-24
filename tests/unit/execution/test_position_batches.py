from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderState,
    FuturesPositionSide,
    ManagedLivePositionBatch,
    PositionHistory,
    PositionObservation,
    PositionOrderFact,
    count_active_symbol_batch_concurrency,
    rebuild_position_batches,
)
from crypto_momentum_lab.domain.strategy import StrategySide

NOW = datetime(2026, 9, 10, 0, 0, tzinfo=UTC)


def _order(
    symbol: str,
    *,
    side: str = "BUY",
    reduce_only: bool = False,
    quantity: Decimal = Decimal("1.0"),
    executed_quantity: Decimal | None = None,
    state: ExchangeOrderState = ExchangeOrderState.FILLED,
    client_order_id: str | None = None,
    exchange_order_id: str | None = None,
    created_at: datetime = NOW,
    price: Decimal | None = Decimal("100"),
    exit_batch_id: str | None = None,
    legacy_exit_attribution: bool = False,
) -> PositionOrderFact:
    exec_qty = quantity if executed_quantity is None else executed_quantity
    return PositionOrderFact(
        symbol=symbol,
        position_side=FuturesPositionSide.BOTH,
        side=side,
        reduce_only=reduce_only,
        order_type="LIMIT",
        quantity=quantity,
        executed_quantity=exec_qty,
        state=state,
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        created_at=created_at,
        updated_at=created_at,
        price=price,
        plan=None,
        exit_batch_id=exit_batch_id,
        legacy_exit_attribution=legacy_exit_attribution,
    )


def test_rebuild_position_batches_single_entry_clean() -> None:
    obs = PositionObservation(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.BOTH,
        position_amt=Decimal("1.0"),
        entry_price=Decimal("100"),
    )
    order = _order(
        "BTCUSDT",
        side="BUY",
        quantity=Decimal("1.0"),
        client_order_id="c1",
        created_at=NOW,
    )
    history = PositionHistory(orders=[order])
    result = rebuild_position_batches(obs, history)

    assert len(result.batches) == 1
    batch = result.batches[0]
    assert batch.batch_id == "BTCUSDT:BOTH:c1"
    assert batch.quantity == Decimal("1.0")
    assert batch.entry_price == Decimal("100")
    assert batch.opened_at == NOW
    assert len(result.diagnostics) == 0


def test_rebuild_position_batches_multiple_entries_consolidate() -> None:
    t1 = NOW
    t2 = NOW + timedelta(minutes=5)
    obs = PositionObservation(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.BOTH,
        position_amt=Decimal("3.0"),
        entry_price=Decimal("110"),
    )
    order1 = _order(
        "BTCUSDT",
        quantity=Decimal("1.0"),
        price=Decimal("100"),
        client_order_id="c1",
        created_at=t1,
    )
    order2 = _order(
        "BTCUSDT",
        quantity=Decimal("2.0"),
        price=Decimal("115"),
        client_order_id="c2",
        created_at=t2,
    )
    history = PositionHistory(orders=[order1, order2])
    result = rebuild_position_batches(obs, history)

    assert len(result.batches) == 1
    batch = result.batches[0]
    assert batch.batch_id == "BTCUSDT:BOTH:c1"
    assert batch.quantity == Decimal("3.0")
    # Weighted entry price: (1 * 100 + 2 * 115) / 3 = 330 / 3 = 110
    assert batch.entry_price == Decimal("110")
    assert batch.opened_at == t2


def test_rebuild_position_batches_overflow_reassigned_and_emits_diagnostic() -> None:
    t1 = NOW
    t2 = NOW + timedelta(minutes=10)
    t3 = NOW + timedelta(minutes=20)
    obs = PositionObservation(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.BOTH,
        position_amt=Decimal("0.5"),
        entry_price=Decimal("100"),
    )
    # Entry 1: 1.0 unit
    e1 = _order("BTCUSDT", quantity=Decimal("1.0"), client_order_id="e1", created_at=t1)
    # First exit: reduce 0.5 from e1 to split batch boundary
    x1 = _order(
        "BTCUSDT",
        side="SELL",
        reduce_only=True,
        quantity=Decimal("0.5"),
        client_order_id="x1",
        created_at=t1 + timedelta(minutes=2),
    )
    # Entry 2: 1.0 unit -> new batch e2
    e2 = _order("BTCUSDT", quantity=Decimal("1.0"), client_order_id="e2", created_at=t2)
    # Over-allocated exit bound to e2: tried to reduce 1.2 units (e2 only has 1.0 unit)
    x2 = _order(
        "BTCUSDT",
        side="SELL",
        reduce_only=True,
        quantity=Decimal("1.2"),
        client_order_id="x2",
        created_at=t3,
        exit_batch_id="BTCUSDT:BOTH:e2",
    )

    history = PositionHistory(orders=[e1, x1, e2, x2])
    result = rebuild_position_batches(obs, history)

    assert any(d.kind == "reassigned" for d in result.diagnostics)


def test_rebuild_position_batches_zero_crossing_cleans_episode() -> None:
    t1 = NOW
    t2 = NOW + timedelta(minutes=5)
    t3 = NOW + timedelta(minutes=10)
    obs = PositionObservation(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.BOTH,
        position_amt=Decimal("1.0"),
        entry_price=Decimal("120"),
    )
    # Entry 1 and full exit:
    e1 = _order("BTCUSDT", quantity=Decimal("1.0"), client_order_id="e1", created_at=t1)
    x1 = _order(
        "BTCUSDT",
        side="SELL",
        reduce_only=True,
        quantity=Decimal("1.0"),
        client_order_id="x1",
        created_at=t2,
    )
    # Entry 2 after flat:
    e2 = _order(
        "BTCUSDT",
        quantity=Decimal("1.0"),
        price=Decimal("120"),
        client_order_id="e2",
        created_at=t3,
    )

    history = PositionHistory(orders=[e1, x1, e2])
    result = rebuild_position_batches(obs, history)

    assert len(result.batches) == 1
    assert result.batches[0].batch_id == "BTCUSDT:BOTH:e2"
    assert result.batches[0].quantity == Decimal("1.0")
    assert result.batches[0].entry_price == Decimal("120")


def test_rebuild_position_batches_legacy_attribution_filters_unbound() -> None:
    t1 = NOW
    t2 = NOW + timedelta(minutes=5)
    obs = PositionObservation(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.BOTH,
        position_amt=Decimal("2.0"),
        entry_price=Decimal("100"),
    )
    # Entry 1 with legacy attribution on exit
    e1 = _order("BTCUSDT", quantity=Decimal("1.0"), client_order_id="e1", created_at=t1)
    x1 = _order(
        "BTCUSDT",
        side="SELL",
        reduce_only=True,
        quantity=Decimal("0.5"),
        client_order_id="x1",
        created_at=t1 + timedelta(minutes=2),
        legacy_exit_attribution=True,
    )
    # Entry 2 cleanly bound
    e2 = _order("BTCUSDT", quantity=Decimal("2.0"), client_order_id="e2", created_at=t2)

    history = PositionHistory(orders=[e1, x1, e2])
    result = rebuild_position_batches(obs, history)

    # e1 has legacy attribution, so it is filtered out.
    # e2 is clean with quantity 2.0, exactly matching target 2.0.
    assert len(result.batches) == 1
    assert result.batches[0].batch_id == "BTCUSDT:BOTH:e2"
    assert result.batches[0].quantity == Decimal("2.0")


def test_rebuild_position_batches_order_predating_lot_not_bound() -> None:
    t1 = NOW
    t2 = NOW + timedelta(minutes=10)
    obs = PositionObservation(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.BOTH,
        position_amt=Decimal("1.0"),
        entry_price=Decimal("100"),
    )
    # An exit order created at t1 with a named batch pointing to a future lot
    # created at t2
    x_stale = _order(
        "BTCUSDT",
        side="SELL",
        reduce_only=True,
        quantity=Decimal("0.5"),
        client_order_id="x_stale",
        created_at=t1,
        exit_batch_id="BTCUSDT:BOTH:e_future",
    )
    e_future = _order(
        "BTCUSDT",
        quantity=Decimal("1.0"),
        client_order_id="e_future",
        created_at=t2,
    )

    history = PositionHistory(orders=[x_stale, e_future])
    result = rebuild_position_batches(obs, history)

    # The stale exit must NOT bind to future lot and steal its quantity
    assert len(result.batches) == 1
    assert result.batches[0].batch_id == "BTCUSDT:BOTH:e_future"
    assert result.batches[0].quantity == Decimal("1.0")


def test_rebuild_position_batches_reconcile_fifo_preserves_newest() -> None:
    t1 = NOW
    t2 = NOW + timedelta(hours=3)
    # Suppose old batch was 254 units, new batch was 172 units.
    # Total recorded entries = 426 units.
    # Actual position observed on exchange is only 172 units
    # (old batch closed manually).
    obs = PositionObservation(
        symbol="B2USDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.BOTH,
        position_amt=Decimal("172.0"),
        entry_price=Decimal("0.54"),
    )
    # Entry 1 (old): 254 units
    e1 = _order(
        "B2USDT",
        quantity=Decimal("254.0"),
        price=Decimal("0.50"),
        client_order_id="e1_old",
        created_at=t1,
    )
    # We simulate exit on e1 with a partial fill or zero reduce_only order
    # To split the boundary so they form 2 distinct batches, e1 has an exit attempt:
    x1 = _order(
        "B2USDT",
        side="SELL",
        reduce_only=True,
        quantity=Decimal("1.0"),
        executed_quantity=Decimal("0"),
        state=ExchangeOrderState.CANCELED,
        client_order_id="x1",
        created_at=t1 + timedelta(hours=1),
    )
    # Entry 2 (new): 172 units
    e2 = _order(
        "B2USDT",
        quantity=Decimal("172.0"),
        price=Decimal("0.54"),
        client_order_id="e2_new",
        created_at=t2,
    )

    history = PositionHistory(orders=[e1, x1, e2])
    result = rebuild_position_batches(obs, history)

    # FIFO must remove the excess (254) from e1_old, leaving ONLY e2_new intact!
    assert len(result.batches) == 1
    assert result.batches[0].batch_id == "B2USDT:BOTH:e2_new"
    assert result.batches[0].quantity == Decimal("172.0")
    assert result.batches[0].opened_at == t2


def test_rebuild_position_batches_tracks_entry_order_count_and_client_ids() -> None:
    t1 = NOW
    t2 = NOW + timedelta(minutes=5)
    obs = PositionObservation(
        symbol="ACUUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.BOTH,
        position_amt=Decimal("20.0"),
        entry_price=Decimal("1.5"),
    )
    e1 = _order(
        "ACUUSDT",
        quantity=Decimal("10.0"),
        price=Decimal("1.4"),
        client_order_id="acu_entry_1",
        created_at=t1,
    )
    e2 = _order(
        "ACUUSDT",
        quantity=Decimal("10.0"),
        price=Decimal("1.6"),
        client_order_id="acu_entry_2",
        created_at=t2,
    )
    history = PositionHistory(orders=[e1, e2])
    result = rebuild_position_batches(obs, history)

    assert len(result.batches) == 1
    batch = result.batches[0]
    assert batch.batch_id == "ACUUSDT:BOTH:acu_entry_1"
    assert batch.quantity == Decimal("20.0")
    assert batch.entry_price == Decimal("1.5")
    assert batch.entry_order_count == 2
    assert batch.entry_client_order_ids == frozenset({"acu_entry_1", "acu_entry_2"})
    assert batch.exit_order_submitted_at is None


def test_rebuild_position_batches_exit_submitted_unfilled_starts_new_batch() -> None:
    t1 = NOW
    t2 = NOW + timedelta(minutes=5)
    t3 = NOW + timedelta(minutes=10)
    obs = PositionObservation(
        symbol="ACUUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.BOTH,
        position_amt=Decimal("25.0"),
        entry_price=Decimal("1.5"),
    )
    # Batch 1: e1 (10 units)
    e1 = _order("ACUUSDT", quantity=Decimal("10.0"), client_order_id="e1", created_at=t1)
    # Exit order submitted for Batch 1, but 0 filled (e.g. limit order placed)
    x1 = _order(
        "ACUUSDT",
        side="SELL",
        reduce_only=True,
        quantity=Decimal("10.0"),
        executed_quantity=Decimal("0"),
        state=ExchangeOrderState.SUBMITTED,
        client_order_id="x1",
        created_at=t2,
        exit_batch_id="ACUUSDT:BOTH:e1",
    )
    # Batch 2: e2 arrives after exit was submitted
    e2 = _order("ACUUSDT", quantity=Decimal("15.0"), client_order_id="e2", created_at=t3)

    history = PositionHistory(orders=[e1, x1, e2])
    result = rebuild_position_batches(obs, history)

    # Must be 2 separate batches
    assert len(result.batches) == 2
    b1 = result.batches[0]
    b2 = result.batches[1]

    # Batch 1 has exit_order_submitted_at set!
    assert b1.batch_id == "ACUUSDT:BOTH:e1"
    assert b1.exit_order_submitted_at is not None
    assert b1.entry_order_count == 1

    # Batch 2 is clean with exit_order_submitted_at = None and entry_order_count = 1
    assert b2.batch_id == "ACUUSDT:BOTH:e2"
    assert b2.exit_order_submitted_at is None
    assert b2.entry_order_count == 1


def test_count_active_symbol_batch_concurrency_scenarios() -> None:
    class MockOrder:
        def __init__(self, symbol: str, reduce_only: bool, client_order_id: str | None = None) -> None:
            self.symbol = symbol
            self.reduce_only = reduce_only
            self.client_order_id = client_order_id

    class MockPosition:
        def __init__(self, symbol: str, batches: tuple[ManagedLivePositionBatch, ...] = ()) -> None:
            self.symbol = symbol
            self.batches = batches

    # Case 1: No positions, no orders -> 0
    assert count_active_symbol_batch_concurrency("ACUUSDT", (), ()) == 0

    # Case 2: 1 pending entry order -> 1
    pending_o1 = MockOrder("ACUUSDT", False, "pending-1")
    assert count_active_symbol_batch_concurrency("ACUUSDT", (), (pending_o1,)) == 1

    # Case 3: 2 pending entry orders -> 2
    pending_o2 = MockOrder("ACUUSDT", False, "pending-2")
    assert count_active_symbol_batch_concurrency("ACUUSDT", (), (pending_o1, pending_o2)) == 2

    # Case 4: Active batch with 1 entry order, 0 pending -> 1
    batch_1 = ManagedLivePositionBatch(
        batch_id="b1",
        quantity=Decimal("10"),
        entry_price=Decimal("1.5"),
        opened_at=NOW,
        entry_order_count=1,
        entry_client_order_ids=frozenset({"e1"}),
    )
    pos = MockPosition("ACUUSDT", (batch_1,))
    assert count_active_symbol_batch_concurrency("ACUUSDT", (pos,), ()) == 1

    # Case 5: Active batch with 2 entry orders (consolidated), 0 pending -> 2
    batch_2 = ManagedLivePositionBatch(
        batch_id="b1",
        quantity=Decimal("20"),
        entry_price=Decimal("1.5"),
        opened_at=NOW,
        entry_order_count=2,
        entry_client_order_ids=frozenset({"e1", "e2"}),
    )
    pos2 = MockPosition("ACUUSDT", (batch_2,))
    assert count_active_symbol_batch_concurrency("ACUUSDT", (pos2,), ()) == 2

    # Case 6: Partial fill deduplication (order is in batch and also in unresolved_orders) -> 1, not 2!
    order_in_flight = MockOrder("ACUUSDT", False, "e1")
    assert count_active_symbol_batch_concurrency("ACUUSDT", (pos,), (order_in_flight,)) == 1

    # Case 7: Batch 1 has exit order submitted -> treated as ENDED, does NOT count!
    batch_ended = ManagedLivePositionBatch(
        batch_id="b1",
        quantity=Decimal("20"),
        entry_price=Decimal("1.5"),
        opened_at=NOW,
        entry_order_count=2,
        exit_order_submitted_at=NOW + timedelta(minutes=10),
    )
    pos_ended = MockPosition("ACUUSDT", (batch_ended,))
    # Active batch concurrency must be 0, allowing new batch to open!
    assert count_active_symbol_batch_concurrency("ACUUSDT", (pos_ended,), ()) == 0

    # Case 8: Batch 1 has exit order submitted, Batch 2 is active with 1 entry -> 1
    batch_new = ManagedLivePositionBatch(
        batch_id="b2",
        quantity=Decimal("10"),
        entry_price=Decimal("1.7"),
        opened_at=NOW + timedelta(minutes=15),
        entry_order_count=1,
    )
    pos_multi_batch = MockPosition("ACUUSDT", (batch_ended, batch_new))
    assert count_active_symbol_batch_concurrency("ACUUSDT", (pos_multi_batch,), ()) == 1

    # Case 9: Different symbol (e.g. BTCUSDT) -> 0
    assert count_active_symbol_batch_concurrency("BTCUSDT", (pos2,), ()) == 0


