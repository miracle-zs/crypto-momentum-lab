from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.next_gen.engine import AccountState
from crypto_momentum_lab.next_gen.models import (
    OrderStatus,
    PositionBatch,
    SystemMode,
    generate_batch_id,
)


@pytest.fixture
def account_state():
    mock_db = AsyncMock()
    mock_rest = AsyncMock()
    state = AccountState(
        environment="live",
        account_label="a1",
        db=mock_db,
        rest_client=mock_rest,
    )
    state.set_system_mode(SystemMode.RUNNING)
    state.ws_connected = True
    return state


def test_create_entry_order_acquires_lock_and_prevents_duplicate(
    account_state: AccountState,
):
    batch_id = generate_batch_id(
        symbol="BTCUSDT",
        position_side="LONG",
        account_label="a1",
        timestamp_str="261018120000",
        salt="9f",
        counter=1,
    )

    order1 = account_state.create_entry_order(
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        price=Decimal("65000"),
        batch_id=batch_id,
        order_type="LIMIT",
    )

    assert order1.client_order_id == f"e_{batch_id}_01"
    assert order1.status == OrderStatus.PENDING
    entry_key = ("a1", "BTCUSDT", "LONG")
    assert account_state.active_entry_orders[entry_key] == order1.client_order_id

    # Attempting second entry for same symbol and position_side while order1 is active
    with pytest.raises(RuntimeError, match="已存在活跃开仓单"):
        account_state.create_entry_order(
            symbol="BTCUSDT",
            position_side="LONG",
            quantity=Decimal("0.5"),
            price=Decimal("64900"),
            batch_id=batch_id,
            order_type="LIMIT",
        )


def test_create_exit_order_reserves_lot_and_acquires_exit_lock(
    account_state: AccountState,
):
    batch_id = generate_batch_id(
        symbol="BTCUSDT",
        position_side="LONG",
        account_label="a1",
        timestamp_str="261018120000",
        salt="9f",
        counter=1,
    )
    batch = PositionBatch(
        environment="live",
        account_label="a1",
        batch_id=batch_id,
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("2.0"),
        entry_price=Decimal("65000"),
        entry_bar_time=datetime.now(UTC),
    )
    account_state.batches[batch_id] = batch

    exit_order = account_state.create_exit_order(
        batch_id=batch_id,
        quantity=Decimal("2.0"),
        price=Decimal("66000"),
        order_type="LIMIT",
    )

    assert exit_order.client_order_id == f"fl_{batch_id}"
    assert exit_order.side == "SELL"
    assert batch.exiting_quantity == Decimal("2.0")
    assert batch.status == "EXITING"
    assert batch.exit_order_submitted_at is not None

    exit_key = ("a1", batch_id)
    assert account_state.active_exit_orders[exit_key] == exit_order.client_order_id

    # Duplicate exit order for same batch is rejected
    with pytest.raises(RuntimeError, match="已存在平仓单"):
        account_state.create_exit_order(
            batch_id=batch_id,
            quantity=Decimal("1.0"),
            price=Decimal("66100"),
            order_type="LIMIT",
        )


def test_exit_and_entry_locks_are_decoupled_allowing_batch_split(
    account_state: AccountState,
):
    # Setup batch 1
    batch1_id = generate_batch_id(
        symbol="BTCUSDT",
        position_side="LONG",
        account_label="a1",
        timestamp_str="261018120000",
        salt="9f",
        counter=1,
    )
    batch1 = PositionBatch(
        environment="live",
        account_label="a1",
        batch_id=batch1_id,
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        entry_price=Decimal("65000"),
        entry_bar_time=datetime.now(UTC),
    )
    account_state.batches[batch1_id] = batch1

    # Exit batch 1: locks active_exit_orders[("a1", batch1_id)]
    exit_order = account_state.create_exit_order(
        batch_id=batch1_id,
        quantity=Decimal("1.0"),
        price=Decimal("66000"),
        order_type="LIMIT",
    )
    assert (
        account_state.active_exit_orders[("a1", batch1_id)]
        == exit_order.client_order_id
    )

    # Entry lock is FREE! A new momentum signal opens batch 2:
    batch2_id = generate_batch_id(
        symbol="BTCUSDT",
        position_side="LONG",
        account_label="a1",
        timestamp_str="261018121500",
        salt="9f",
        counter=2,
    )
    entry_order2 = account_state.create_entry_order(
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        price=Decimal("65500"),
        batch_id=batch2_id,
        order_type="LIMIT",
    )

    # Both locks are active simultaneously without deadlock!
    assert (
        account_state.active_exit_orders[("a1", batch1_id)]
        == exit_order.client_order_id
    )
    assert (
        account_state.active_entry_orders[("a1", "BTCUSDT", "LONG")]
        == entry_order2.client_order_id
    )


def test_mark_order_rejected_cleans_locks_and_rolls_back_lot_reservation(
    account_state: AccountState,
):
    batch_id = generate_batch_id(
        symbol="BTCUSDT",
        position_side="LONG",
        account_label="a1",
        timestamp_str="261018120000",
        salt="9f",
        counter=1,
    )
    batch = PositionBatch(
        environment="live",
        account_label="a1",
        batch_id=batch_id,
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("2.0"),
        entry_price=Decimal("65000"),
        entry_bar_time=datetime.now(UTC),
    )
    account_state.batches[batch_id] = batch

    exit_order = account_state.create_exit_order(
        batch_id=batch_id,
        quantity=Decimal("1.0"),
        price=Decimal("66000"),
        order_type="LIMIT",
    )
    assert batch.exiting_quantity == Decimal("1.0")

    # Reject exit order
    account_state.mark_order_rejected(
        exit_order.client_order_id, reason="INSUFFICIENT_MARGIN"
    )

    assert exit_order.status == OrderStatus.REJECTED
    assert ("a1", batch_id) not in account_state.active_exit_orders
    assert batch.exiting_quantity == Decimal("0")
    assert batch.status == "ACTIVE"
