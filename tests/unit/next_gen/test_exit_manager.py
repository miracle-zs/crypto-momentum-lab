import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.next_gen.engine import AccountState
from crypto_momentum_lab.next_gen.executor import OrderExecutor
from crypto_momentum_lab.next_gen.exit_manager import ExitManager
from crypto_momentum_lab.next_gen.models import (
    PositionBatch,
    SystemMode,
    generate_batch_id,
)
from crypto_momentum_lab.next_gen.persistence import InMemoryTradingRepository


@pytest.fixture
async def exit_setup():
    repo = InMemoryTradingRepository()
    mock_rest = AsyncMock()
    mock_rest.query_order.return_value = {
        "status": "CANCELED",
        "executedQty": "0",
        "avgPrice": "0",
    }
    state = AccountState(
        environment="live",
        account_label="a1",
        db=repo,
        rest_client=mock_rest,
    )
    state.set_system_mode(SystemMode.RUNNING)
    state.ws_connected = True
    executor = OrderExecutor(rest_client=mock_rest, state=state, db=repo)
    exit_mgr = ExitManager(rest_client=mock_rest, state=state, executor=executor)
    try:
        yield state, repo, mock_rest, exit_mgr
    finally:
        await executor.close()


@pytest.mark.asyncio
async def test_exit_manager_grace_timeout_cancels_limit_and_sweeps_market(exit_setup):
    state, repo, mock_rest, exit_mgr = exit_setup
    mock_rest.post_order.return_value = {"orderId": 111, "clientOrderId": "fl_BTC_b1"}
    mock_rest.cancel_order.return_value = {"orderId": 111, "status": "CANCELED"}

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
    state.batches[batch_id] = batch

    # Execute exit with 0.05s grace timeout
    await exit_mgr.execute_batch_exit(
        batch_id=batch_id,
        limit_price=Decimal("66000"),
        grace_period_sec=0.05,
    )

    # 1. Limit order was posted
    assert mock_rest.post_order.call_count == 2  # 1st limit, 2nd market sweep
    first_call = mock_rest.post_order.call_args_list[0].kwargs
    assert first_call["order_type"] == "LIMIT"
    assert first_call["price"] == Decimal("66000")

    # 2. Limit order was canceled after grace timeout
    mock_rest.cancel_order.assert_awaited_once_with(
        symbol="BTCUSDT",
        client_order_id=f"fl_{batch_id}_01",
    )

    # 3. Market sweep order was posted
    second_call = mock_rest.post_order.call_args_list[1].kwargs
    assert second_call["order_type"] == "MARKET"
    assert second_call["client_order_id"] == f"fm_{batch_id}_02"


@pytest.mark.asyncio
async def test_exit_manager_evaluate_holding_expired(exit_setup):
    state, repo, mock_rest, exit_mgr = exit_setup
    mock_rest.post_order.return_value = {"orderId": 111}
    mock_rest.cancel_order.return_value = {"orderId": 111, "status": "CANCELED"}

    batch_id = generate_batch_id("BTCUSDT", "LONG", "a1", counter=1)
    # 1000s ago -> exceeds 900s holding
    old_time = datetime.now(UTC) - timedelta(seconds=1000)
    batch = PositionBatch(
        environment="live",
        account_label="a1",
        batch_id=batch_id,
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        entry_price=Decimal("65000"),
        entry_bar_time=old_time,
        status="ACTIVE",
    )
    state.batches[batch_id] = batch

    scheduled = exit_mgr.evaluate_active_batches()
    assert scheduled == [batch_id]

    # Clean up background tasks
    await exit_mgr.close()


@pytest.mark.asyncio
async def test_exit_manager_evaluate_drain_triggers_immediately(exit_setup):
    state, repo, mock_rest, exit_mgr = exit_setup
    mock_rest.post_order.return_value = {"orderId": 222}

    batch_id = generate_batch_id("BTCUSDT", "LONG", "a1", counter=2)
    # Just created 5s ago
    batch = PositionBatch(
        environment="live",
        account_label="a1",
        batch_id=batch_id,
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        entry_price=Decimal("65000"),
        entry_bar_time=datetime.now(UTC),
        status="ACTIVE",
    )
    state.batches[batch_id] = batch

    # Not draining -> not scheduled
    assert exit_mgr.evaluate_active_batches() == []

    # Admin Drains account
    state.is_draining = True
    scheduled = exit_mgr.evaluate_active_batches()
    assert scheduled == [batch_id]

    await exit_mgr.close()


@pytest.mark.asyncio
async def test_exit_manager_run_loop_lifecycle(exit_setup):
    state, repo, mock_rest, exit_mgr = exit_setup
    stop_event = asyncio.Event()

    async def stop_soon():
        await asyncio.sleep(0.02)
        stop_event.set()

    asyncio.create_task(stop_soon())
    await exit_mgr.run(stop_event=stop_event)
    await exit_mgr.close()


@pytest.mark.asyncio
async def test_exit_manager_cancels_unfilled_entry_orders_before_exit(exit_setup):
    state, repo, mock_rest, exit_mgr = exit_setup
    mock_rest.post_order.return_value = {"orderId": 111}
    mock_rest.cancel_order.return_value = {"orderId": 111, "status": "CANCELED"}

    batch_id = generate_batch_id("BTCUSDT", "LONG", "a1", counter=10)
    batch = PositionBatch(
        environment="live",
        account_label="a1",
        batch_id=batch_id,
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        entry_price=Decimal("65000"),
        entry_bar_time=datetime.now(UTC),
        status="ACTIVE",
    )
    state.batches[batch_id] = batch

    # Add an unfinalized entry order belonging to this batch
    entry_order = state.create_entry_order(
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("0.5"),
        price=Decimal("64000"),
        batch_id=batch_id,
    )
    assert not entry_order.is_terminal

    await exit_mgr.execute_batch_exit(
        batch_id=batch_id,
        limit_price=Decimal("66000"),
        grace_period_sec=0.01,
    )

    # Verify cancel_order was called for entry_order
    mock_rest.cancel_order.assert_any_await(
        symbol="BTCUSDT",
        client_order_id=entry_order.client_order_id,
    )
    await exit_mgr.close()


@pytest.mark.asyncio
async def test_exit_manager_refuses_market_sweep_if_cancel_unconfirmed(exit_setup):
    state, repo, mock_rest, exit_mgr = exit_setup
    # Simulate cancel_order does not immediately confirm
    # and query_order returns non-terminal NEW
    mock_rest.cancel_order.return_value = {"orderId": 333, "status": "NEW"}
    mock_rest.query_order.return_value = {
        "status": "NEW",
        "executedQty": "0",
        "avgPrice": "0",
    }

    batch_id = generate_batch_id("BTCUSDT", "LONG", "a1", counter=11)
    batch = PositionBatch(
        environment="live",
        account_label="a1",
        batch_id=batch_id,
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        entry_price=Decimal("65000"),
        entry_bar_time=datetime.now(UTC),
        status="ACTIVE",
    )
    state.batches[batch_id] = batch

    await exit_mgr.execute_batch_exit(
        batch_id=batch_id,
        limit_price=Decimal("66000"),
        grace_period_sec=0.01,
    )

    # Post order should only have been called ONCE (limit exit), NOT market sweep
    assert mock_rest.post_order.call_count == 1
    # System must fail-closed into DEGRADED to protect from oversell/reversal
    assert state.system_mode == SystemMode.DEGRADED
    await exit_mgr.close()


@pytest.mark.asyncio
async def test_exit_manager_refuses_market_sweep_on_unknown_status(exit_setup):
    state, repo, mock_rest, exit_mgr = exit_setup
    mock_rest.post_order.return_value = {"orderId": 444}
    mock_rest.cancel_order.return_value = {"orderId": 444, "status": "UNKNOWN_ERROR"}
    mock_rest.query_order.return_value = {
        "status": "SOME_WEIRD_STATE",
        "executedQty": "0",
    }

    batch_id = generate_batch_id("BTCUSDT", "LONG", "a1", counter=12)
    batch = PositionBatch(
        environment="live",
        account_label="a1",
        batch_id=batch_id,
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        entry_price=Decimal("65000"),
        entry_bar_time=datetime.now(UTC),
        status="ACTIVE",
    )
    state.batches[batch_id] = batch

    await exit_mgr.execute_batch_exit(
        batch_id=batch_id,
        limit_price=Decimal("66000"),
        grace_period_sec=0.01,
    )

    # Must NOT assume CANCELED! Market sweep must be refused!
    assert mock_rest.post_order.call_count == 1
    assert state.system_mode == SystemMode.DEGRADED
    await exit_mgr.close()


@pytest.mark.asyncio
async def test_exit_manager_refuses_market_sweep_on_invalid_payload(exit_setup):
    state, repo, mock_rest, exit_mgr = exit_setup
    mock_rest.post_order.return_value = {"orderId": 555}
    mock_rest.cancel_order.return_value = {"orderId": 555, "status": "NEW"}
    mock_rest.query_order.return_value = None  # Non-dict payload

    batch_id = generate_batch_id("BTCUSDT", "LONG", "a1", counter=13)
    batch = PositionBatch(
        environment="live",
        account_label="a1",
        batch_id=batch_id,
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        entry_price=Decimal("65000"),
        entry_bar_time=datetime.now(UTC),
        status="ACTIVE",
    )
    state.batches[batch_id] = batch

    await exit_mgr.execute_batch_exit(
        batch_id=batch_id,
        limit_price=Decimal("66000"),
        grace_period_sec=0.01,
    )

    # Must NOT assume CANCELED!
    assert mock_rest.post_order.call_count == 1
    assert state.system_mode == SystemMode.DEGRADED
    await exit_mgr.close()
