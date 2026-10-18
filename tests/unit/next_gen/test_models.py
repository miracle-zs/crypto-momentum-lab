from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.next_gen.models import (
    STATUS_RANK,
    Order,
    OrderStatus,
    PositionBatch,
    generate_batch_id,
    generate_entry_order_id,
    generate_exit_order_id,
)


def test_order_status_monotonic_rank():
    assert STATUS_RANK[OrderStatus.PENDING] < STATUS_RANK[OrderStatus.SUBMITTED]
    assert STATUS_RANK[OrderStatus.SUBMITTED] < STATUS_RANK[OrderStatus.OPEN]
    assert STATUS_RANK[OrderStatus.OPEN] == STATUS_RANK[OrderStatus.RECONCILING]
    assert STATUS_RANK[OrderStatus.OPEN] < STATUS_RANK[OrderStatus.PARTIALLY_FILLED]
    assert STATUS_RANK[OrderStatus.PARTIALLY_FILLED] < STATUS_RANK[OrderStatus.FILLED]
    assert STATUS_RANK[OrderStatus.FILLED] == STATUS_RANK[OrderStatus.CANCELED]
    assert STATUS_RANK[OrderStatus.FILLED] == STATUS_RANK[OrderStatus.REJECTED]


def test_order_apply_update_monotonic_progression():
    order = Order(
        environment="live",
        account_label="a1",
        client_order_id="e_BTC_L_261018120000_a1_b1_01",
        batch_id="BTC_L_261018120000_a1_b1",
        symbol="BTCUSDT",
        side="BUY",
        position_side="LONG",
        order_type="LIMIT",
        quantity=Decimal("1.0"),
        price=Decimal("65000"),
        status=OrderStatus.PENDING,
    )
    assert not order.is_terminal

    # 1. PENDING -> SUBMITTED
    delta = order.apply_update(OrderStatus.SUBMITTED)
    assert order.status == OrderStatus.SUBMITTED
    assert delta == Decimal("0")

    # 2. SUBMITTED -> OPEN
    delta = order.apply_update(OrderStatus.OPEN)
    assert order.status == OrderStatus.OPEN
    assert delta == Decimal("0")

    # 3. Out-of-order late SUBMITTED event arrives: rank is 1 < 2, must be ignored
    delta = order.apply_update(OrderStatus.SUBMITTED)
    assert order.status == OrderStatus.OPEN

    # 4. Partial fill: 0.4 @ 65000
    delta = order.apply_update(
        OrderStatus.PARTIALLY_FILLED,
        cum_filled_qty=Decimal("0.4"),
        avg_price=Decimal("65000"),
    )
    assert order.status == OrderStatus.PARTIALLY_FILLED
    assert order.filled_qty == Decimal("0.4")
    assert delta == Decimal("0.4")

    # 5. Full fill: 1.0 @ 65000
    delta = order.apply_update(
        OrderStatus.FILLED,
        cum_filled_qty=Decimal("1.0"),
        avg_price=Decimal("65000"),
    )
    assert order.status == OrderStatus.FILLED
    assert order.filled_qty == Decimal("1.0")
    assert delta == Decimal("0.6")
    assert order.is_terminal

    # 6. Terminal state cannot regress to OPEN or PARTIALLY_FILLED
    order.apply_update(OrderStatus.OPEN)
    assert order.status == OrderStatus.FILLED


def test_position_batch_lot_reservation():
    now = datetime.now(UTC)
    batch = PositionBatch(
        environment="live",
        account_label="a1",
        batch_id="BTC_L_261018120000_a1_b1",
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("2.0"),
        entry_price=Decimal("65000"),
        entry_bar_time=now,
    )
    assert batch.status == "ACTIVE"
    assert batch.exiting_quantity == Decimal("0")

    # Reserve 1.5 for exit
    batch.reserve_for_exit(Decimal("1.5"))
    assert batch.exiting_quantity == Decimal("1.5")
    assert batch.status == "EXITING"

    # Attempting to reserve 0.6 when only 0.5 available must raise ValueError
    with pytest.raises(ValueError, match="可平仓量不足"):
        batch.reserve_for_exit(Decimal("0.6"))

    # Partial cancel: release 0.5 reservation
    batch.release_exit_reservation(Decimal("0.5"))
    assert batch.exiting_quantity == Decimal("1.0")
    assert batch.status == "EXITING"

    # Full cancel of remaining reservation: release 1.0 -> back to ACTIVE
    batch.release_exit_reservation(Decimal("1.0"))
    assert batch.exiting_quantity == Decimal("0")
    assert batch.status == "ACTIVE"


def test_client_order_id_generation_length():
    # Regular symbol
    batch_id_btc = generate_batch_id(
        symbol="BTCUSDT",
        position_side="LONG",
        timestamp_str="261018120035",
        account_label="a1",
        salt="9f",
        counter=1,
    )
    assert len(batch_id_btc) <= 31
    entry_btc = generate_entry_order_id(batch_id_btc, seq=1)
    exit_limit_btc = generate_exit_order_id(batch_id_btc, order_type="LIMIT")
    exit_mkt_btc = generate_exit_order_id(batch_id_btc, order_type="MARKET")

    assert len(entry_btc) <= 36
    assert len(exit_limit_btc) <= 36
    assert len(exit_mkt_btc) <= 36

    # Long symbol: 1000LUNCUSDT (base symbol 1000LUNC)
    batch_id_lunc = generate_batch_id(
        symbol="1000LUNCUSDT",
        position_side="LONG",
        timestamp_str="261018120035",
        account_label="a1",
        salt="9f",
        counter=1,
    )
    entry_lunc = generate_entry_order_id(batch_id_lunc, seq=1)
    exit_limit_lunc = generate_exit_order_id(batch_id_lunc, order_type="LIMIT")

    assert len(entry_lunc) <= 36, (
        f"entry_lunc too long: {entry_lunc} ({len(entry_lunc)})"
    )
    assert len(exit_limit_lunc) <= 36, (
        f"exit_limit_lunc too long: {exit_limit_lunc} ({len(exit_limit_lunc)})"
    )
