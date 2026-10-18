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


@pytest.mark.asyncio
async def test_same_batch_addon_entry_after_first_fill(account_state: AccountState):
    batch_id = generate_batch_id(
        symbol="BTCUSDT",
        position_side="LONG",
        account_label="a1",
        timestamp_str="261018120000",
        salt="9f",
        counter=1,
    )

    # 1. First entry: 1.0 @ 65000
    order1 = account_state.create_entry_order(
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        price=Decimal("65000"),
        batch_id=batch_id,
    )
    assert order1.client_order_id == f"e_{batch_id}_01"

    # Fill order 1
    await account_state.on_order_trade_update(
        client_order_id=order1.client_order_id,
        symbol="BTCUSDT",
        position_side="LONG",
        side="BUY",
        order_status=OrderStatus.FILLED,
        cum_filled_qty=Decimal("1.0"),
        avg_price=Decimal("65000"),
        last_filled_qty=Decimal("1.0"),
        last_filled_price=Decimal("65000"),
    )
    assert ("a1", "BTCUSDT", "LONG") not in account_state.active_entry_orders

    # 2. Add-on entry into the same batch (exit has not been submitted)
    order2 = account_state.create_entry_order(
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("0.5"),
        price=Decimal("66000"),
        batch_id=batch_id,
    )
    assert order2.client_order_id == f"e_{batch_id}_02"

    # Fill order 2
    await account_state.on_order_trade_update(
        client_order_id=order2.client_order_id,
        symbol="BTCUSDT",
        position_side="LONG",
        side="BUY",
        order_status=OrderStatus.FILLED,
        cum_filled_qty=Decimal("0.5"),
        avg_price=Decimal("66000"),
        last_filled_qty=Decimal("0.5"),
        last_filled_price=Decimal("66000"),
    )

    batch = account_state.batches[batch_id]
    assert batch.quantity == Decimal("1.5")
    # Weighted avg price:
    # (1.0*65000 + 0.5*66000) / 1.5 = (65000 + 33000) / 1.5 = 65333.333333...
    expected_price = (Decimal("65000") + Decimal("33000")) / Decimal("1.5")
    assert batch.entry_price == expected_price


@pytest.mark.asyncio
async def test_short_position_lifecycle(account_state: AccountState):
    batch_id = generate_batch_id(
        symbol="ETHUSDT",
        position_side="SHORT",
        account_label="a1",
        timestamp_str="261018120000",
        salt="9f",
        counter=1,
    )

    # 1. Entry SHORT: sell 10.0 ETH @ 3000
    entry_order = account_state.create_entry_order(
        symbol="ETHUSDT",
        position_side="SHORT",
        quantity=Decimal("10.0"),
        price=Decimal("3000"),
        batch_id=batch_id,
    )
    assert entry_order.side == "SELL"

    await account_state.on_order_trade_update(
        client_order_id=entry_order.client_order_id,
        symbol="ETHUSDT",
        position_side="SHORT",
        side="SELL",
        order_status=OrderStatus.FILLED,
        cum_filled_qty=Decimal("10.0"),
        avg_price=Decimal("3000"),
        last_filled_qty=Decimal("10.0"),
        last_filled_price=Decimal("3000"),
    )

    batch = account_state.batches[batch_id]
    assert batch.position_side == "SHORT"
    assert batch.quantity == Decimal("10.0")
    assert batch.entry_price == Decimal("3000")

    # 2. Exit SHORT: buy to cover 10.0 ETH @ 2800 (profit: +200 per ETH)
    exit_order = account_state.create_exit_order(
        batch_id=batch_id,
        quantity=Decimal("10.0"),
        price=Decimal("2800"),
        order_type="LIMIT",
    )
    assert exit_order.side == "BUY"

    await account_state.on_order_trade_update(
        client_order_id=exit_order.client_order_id,
        symbol="ETHUSDT",
        position_side="SHORT",
        side="BUY",
        order_status=OrderStatus.FILLED,
        cum_filled_qty=Decimal("10.0"),
        avg_price=Decimal("2800"),
        last_filled_qty=Decimal("10.0"),
        last_filled_price=Decimal("2800"),
    )

    assert batch.status == "CLOSED"
    assert batch.quantity == Decimal("0")
    assert batch.exiting_quantity == Decimal("0")
    # PnL for SHORT: (entry_price - exit_price) * qty = (3000 - 2800) * 10 = 2000 USDT
    assert batch.realized_pnl == Decimal("2000.0")


@pytest.mark.asyncio
async def test_canceled_order_releases_lock(account_state: AccountState):
    batch_id = generate_batch_id(
        symbol="BTCUSDT",
        position_side="LONG",
        account_label="a1",
        timestamp_str="261018120000",
        salt="9f",
        counter=1,
    )
    order = account_state.create_entry_order(
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        price=Decimal("65000"),
        batch_id=batch_id,
    )
    entry_key = ("a1", "BTCUSDT", "LONG")
    assert entry_key in account_state.active_entry_orders

    # Order canceled by exchange / user
    await account_state.on_order_trade_update(
        client_order_id=order.client_order_id,
        symbol="BTCUSDT",
        position_side="LONG",
        side="BUY",
        order_status=OrderStatus.CANCELED,
        cum_filled_qty=Decimal("0"),
        avg_price=Decimal("0"),
        last_filled_qty=Decimal("0"),
        last_filled_price=Decimal("0"),
    )

    assert order.status == OrderStatus.CANCELED
    assert order.is_terminal
    assert entry_key not in account_state.active_entry_orders


def test_notional_cap_check(account_state: AccountState):
    # Setup batch with 1.0 BTC @ 65000 (notional = 65,000)
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
        quantity=Decimal("1.0"),
        entry_price=Decimal("65000"),
        entry_bar_time=datetime.now(UTC),
    )
    account_state.batches[batch_id] = batch

    # Check adding 0.5 BTC @ 65000:
    # Symbol notional = (1.0 + 0.5) * 65000 = 97,500
    # Case 1: symbol_cap = 100,000 -> passes
    assert account_state.check_notional_limits(
        symbol="BTCUSDT",
        new_quantity=Decimal("0.5"),
        current_price=Decimal("65000"),
        symbol_cap=Decimal("100000"),
        account_cap=Decimal("200000"),
    )

    # Case 2: symbol_cap = 90,000 -> fails (97,500 > 90,000)
    assert not account_state.check_notional_limits(
        symbol="BTCUSDT",
        new_quantity=Decimal("0.5"),
        current_price=Decimal("65000"),
        symbol_cap=Decimal("90000"),
        account_cap=Decimal("200000"),
    )

    # Case 3: account_cap = 95,000 -> fails
    assert not account_state.check_notional_limits(
        symbol="BTCUSDT",
        new_quantity=Decimal("0.5"),
        current_price=Decimal("65000"),
        symbol_cap=Decimal("150000"),
        account_cap=Decimal("95000"),
    )


@pytest.mark.asyncio
async def test_late_entry_fill_reopens_closed_batch(account_state: AccountState):
    batch_id = generate_batch_id(
        symbol="BTCUSDT",
        position_side="LONG",
        account_label="a1",
        timestamp_str="261018120000",
        salt="9f",
        counter=1,
    )
    # 1. Start with an existing batch that was completely exited and marked CLOSED
    closed_batch = PositionBatch(
        environment="live",
        account_label="a1",
        batch_id=batch_id,
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("0.0"),
        entry_price=Decimal("65000"),
        entry_bar_time=datetime.now(UTC),
        status="CLOSED",
    )
    account_state.batches[batch_id] = closed_batch

    # Create an entry order for this batch
    entry_order = account_state.create_entry_order(
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("0.5"),
        price=Decimal("65000"),
        batch_id=batch_id,
    )

    # 2. A late fill arrives from Binance WS on this entry order
    await account_state.on_order_trade_update(
        client_order_id=entry_order.client_order_id,
        symbol="BTCUSDT",
        position_side="LONG",
        side="BUY",
        order_status=OrderStatus.FILLED,
        cum_filled_qty=Decimal("0.5"),
        avg_price=Decimal("65000"),
        last_filled_qty=Decimal("0.5"),
        last_filled_price=Decimal("65000"),
    )

    # 3. Assert the closed batch was reopened to ACTIVE with non-zero quantity
    batch = account_state.batches[batch_id]
    assert batch.status == "ACTIVE"
    assert batch.quantity == Decimal("0.5")
    assert batch.entry_price == Decimal("65000")
