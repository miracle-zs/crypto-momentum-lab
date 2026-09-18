from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderState,
    FuturesPositionSide,
    PositionHistory,
    PositionObservation,
    PositionOrderFact,
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
