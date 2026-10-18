import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.next_gen.engine import AccountState
from crypto_momentum_lab.next_gen.models import (
    OrderStatus,
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
async def test_watchdog_triggers_reconciling_and_freezes_ordering_on_timeout(
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
    order = account_state.create_entry_order(
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        price=Decimal("65000"),
        batch_id=batch_id,
    )
    order.apply_update(OrderStatus.SUBMITTED)

    # Launch watchdog with very short timeout (0.05s) for testing
    watchdog_task = asyncio.create_task(
        account_state.watch_submission_timeout(order.client_order_id, timeout_sec=0.05)
    )

    await watchdog_task

    # Order must be in doubt (RECONCILING)
    assert order.status == OrderStatus.RECONCILING
    # System must be frozen into DEGRADED
    assert account_state.system_mode == SystemMode.DEGRADED
    assert not account_state.can_submit_entry_order()


@pytest.mark.asyncio
async def test_watchdog_noop_when_order_confirmed_before_timeout(
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
    order = account_state.create_entry_order(
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        price=Decimal("65000"),
        batch_id=batch_id,
    )
    order.apply_update(OrderStatus.SUBMITTED)

    # Launch watchdog with 0.1s timeout
    watchdog_task = asyncio.create_task(
        account_state.watch_submission_timeout(order.client_order_id, timeout_sec=0.1)
    )

    # WS event arrives at 0.02s before watchdog fires
    await asyncio.sleep(0.02)
    order.apply_update(OrderStatus.OPEN)

    await watchdog_task

    # Order stays OPEN, system stays RUNNING
    assert order.status == OrderStatus.OPEN
    assert account_state.system_mode == SystemMode.RUNNING
    assert account_state.can_submit_entry_order()


@pytest.mark.asyncio
async def test_system_mode_recovery_when_in_doubt_order_resolves(
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
    order = account_state.create_entry_order(
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        price=Decimal("65000"),
        batch_id=batch_id,
    )
    order.apply_update(OrderStatus.SUBMITTED)

    # Trigger watchdog timeout -> enters RECONCILING & DEGRADED
    await account_state.watch_submission_timeout(
        order.client_order_id, timeout_sec=0.01
    )
    assert order.status == OrderStatus.RECONCILING
    assert account_state.system_mode == SystemMode.DEGRADED
    assert not account_state.can_submit_entry_order()

    # Late WS trade update arrives: confirms order was FILLED
    await account_state.on_order_trade_update(
        client_order_id=order.client_order_id,
        symbol="BTCUSDT",
        position_side="LONG",
        side="BUY",
        order_status=OrderStatus.FILLED,
        cum_filled_qty=Decimal("1.0"),
        avg_price=Decimal("65000"),
        last_filled_qty=Decimal("1.0"),
        last_filled_price=Decimal("65000"),
    )

    # Order is now FILLED (no longer RECONCILING)
    assert order.status == OrderStatus.FILLED
    # System mode has autonomously converged back to RUNNING!
    assert account_state.system_mode == SystemMode.RUNNING
    assert account_state.can_submit_entry_order()
