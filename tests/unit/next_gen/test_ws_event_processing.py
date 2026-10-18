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
async def test_on_order_trade_update_entry_fill_advances_batch_and_releases_lock(
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
    # Create entry order
    order = account_state.create_entry_order(
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        price=Decimal("65000"),
        batch_id=batch_id,
        order_type="LIMIT",
    )
    order.apply_update(OrderStatus.SUBMITTED)
    entry_key = ("a1", "BTCUSDT", "LONG")
    assert entry_key in account_state.active_entry_orders

    # 1. Partial fill event arrives: 0.5 @ 65000
    await account_state.on_order_trade_update(
        client_order_id=order.client_order_id,
        symbol="BTCUSDT",
        position_side="LONG",
        side="BUY",
        order_status=OrderStatus.PARTIALLY_FILLED,
        cum_filled_qty=Decimal("0.5"),
        avg_price=Decimal("65000"),
        last_filled_qty=Decimal("0.5"),
        last_filled_price=Decimal("65000"),
        exchange_order_id=987654321,
    )

    assert order.status == OrderStatus.PARTIALLY_FILLED
    assert order.filled_qty == Decimal("0.5")
    assert (
        entry_key in account_state.active_entry_orders
    )  # lock still held while non-terminal

    batch = account_state.batches[batch_id]
    assert batch.quantity == Decimal("0.5")
    assert batch.entry_price == Decimal("65000")
    assert batch.status == "ACTIVE"

    # 2. Second partial fill to complete the order: 0.5 @ 65200 (weighted avg)
    await account_state.on_order_trade_update(
        client_order_id=order.client_order_id,
        symbol="BTCUSDT",
        position_side="LONG",
        side="BUY",
        order_status=OrderStatus.FILLED,
        cum_filled_qty=Decimal("1.0"),
        avg_price=Decimal("65100"),
        last_filled_qty=Decimal("0.5"),
        last_filled_price=Decimal("65200"),
        exchange_order_id=987654321,
    )

    assert order.status == OrderStatus.FILLED
    assert order.is_terminal
    # Entry lock is RELEASED upon terminal status
    assert entry_key not in account_state.active_entry_orders
    assert batch.quantity == Decimal("1.0")
    # Weighted avg price: (0.5 * 65000 + 0.5 * 65200) / 1.0 = 65100
    assert batch.entry_price == Decimal("65100")


@pytest.mark.asyncio
async def test_on_order_trade_update_exit_fill_closes_batch_and_computes_pnl(
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
        quantity=Decimal("1.0"),
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
    exit_key = ("a1", batch_id)
    assert exit_key in account_state.active_exit_orders

    # Exit fill event: 1.0 @ 66000
    await account_state.on_order_trade_update(
        client_order_id=exit_order.client_order_id,
        symbol="BTCUSDT",
        position_side="LONG",
        side="SELL",
        order_status=OrderStatus.FILLED,
        cum_filled_qty=Decimal("1.0"),
        avg_price=Decimal("66000"),
        last_filled_qty=Decimal("1.0"),
        last_filled_price=Decimal("66000"),
        exchange_order_id=987654322,
    )

    assert exit_order.status == OrderStatus.FILLED
    assert exit_key not in account_state.active_exit_orders
    assert batch.quantity == Decimal("0")
    assert batch.exiting_quantity == Decimal("0")
    assert batch.status == "CLOSED"
    # Realized PnL: (66000 - 65000) * 1.0 = 1000
    assert batch.realized_pnl == Decimal("1000")


@pytest.mark.asyncio
async def test_on_order_trade_update_unknown_order_triggers_fail_closed(
    account_state: AccountState,
):
    assert account_state.system_mode == SystemMode.RUNNING

    # Unrecognized order (e.g. liquidation or manual external order)
    await account_state.on_order_trade_update(
        client_order_id="autoclose-123456",
        symbol="BTCUSDT",
        position_side="LONG",
        side="SELL",
        order_status=OrderStatus.FILLED,
        cum_filled_qty=Decimal("0.5"),
        avg_price=Decimal("60000"),
        last_filled_qty=Decimal("0.5"),
        last_filled_price=Decimal("60000"),
    )

    # Must immediately freeze system into DEGRADED
    assert account_state.system_mode == SystemMode.DEGRADED
    assert account_state.has_unrecoverable_fault
    assert not account_state.can_submit_entry_order()


@pytest.mark.asyncio
async def test_ws_disconnect_and_reconnect_guard(account_state: AccountState):
    assert account_state.system_mode == SystemMode.RUNNING

    # WS disconnect
    account_state.on_ws_status_change(connected=False)
    assert account_state.system_mode == SystemMode.DEGRADED
    assert not account_state.ws_connected
    assert not account_state.can_submit_entry_order()

    # WS reconnect: does NOT clear DEGRADED if in-doubt orders
    # or unrecoverable fault exists
    account_state.on_ws_status_change(connected=True)
    assert account_state.ws_connected
    # When clean without in-doubt orders, recheck_system_mode restores RUNNING
    assert account_state.system_mode == SystemMode.RUNNING
