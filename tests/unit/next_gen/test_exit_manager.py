import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.domain.market.closed_candle import ClosedCandle15m
from crypto_momentum_lab.next_gen.engine import AccountState
from crypto_momentum_lab.next_gen.executor import OrderExecutor
from crypto_momentum_lab.next_gen.exit_manager import ExitManager
from crypto_momentum_lab.next_gen.models import (
    OrderStatus,
    PositionBatch,
    SystemMode,
    generate_batch_id,
)
from crypto_momentum_lab.next_gen.persistence import InMemoryTradingRepository
from tests.unit.next_gen.factories import risk_config, symbol_rules


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
    state.risk_config = risk_config(state.environment, state.account_label)
    state.set_system_mode(SystemMode.RUNNING)
    state.ws_connected = True
    executor = OrderExecutor(rest_client=mock_rest, state=state, db=repo)
    exit_mgr = ExitManager(
        rest_client=mock_rest,
        state=state,
        executor=executor,
        symbol_rules=symbol_rules(),
        grace_period_sec=7200,
        decision_profit_pct=Decimal("0.001"),
        grace_profit_pct=Decimal("0.0088"),
    )
    try:
        yield state, repo, mock_rest, exit_mgr
    finally:
        await executor.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("expired", [False, True])
async def test_restart_resumes_exit_cancelled_during_shutdown(exit_setup, expired):
    state, repo, rest, manager = exit_setup
    original_deadline_start = datetime.now(UTC) - timedelta(
        seconds=7201 if expired else 100
    )
    batch = PositionBatch(
        "live",
        "a1",
        "restored",
        "BTCUSDT",
        "LONG",
        Decimal("1"),
        Decimal("65000"),
        original_deadline_start,
        exit_order_submitted_at=original_deadline_start,
    )
    state.batches[batch.batch_id] = batch
    order = state.create_exit_order(batch.batch_id, Decimal("1"), Decimal("66000"))
    await state.on_order_trade_update(
        order.client_order_id,
        order.symbol,
        order.position_side,
        order.side,
        OrderStatus.CANCELED,
        Decimal("0"),
        Decimal("0"),
        Decimal("0"),
        Decimal("0"),
    )
    await manager._executor.load_active_state()
    assert state.active_exit_orders == {}
    rest.post_order.return_value = {"orderId": 222}
    if not expired:
        manager._complete_limit_exit = AsyncMock()
    manager.resume_active_exits()
    await asyncio.gather(*manager._active_exit_tasks.values())
    rest.post_order.assert_awaited_once()
    assert rest.post_order.call_args.kwargs["order_type"] == (
        "MARKET" if expired else "LIMIT"
    )
    assert (
        state.batches[batch.batch_id].exit_order_submitted_at == original_deadline_start
    )


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

    async def observe_cancel(**params):
        order = state.orders[params["client_order_id"]]
        await state.on_order_trade_update(
            order.client_order_id,
            order.symbol,
            order.position_side,
            order.side,
            OrderStatus.CANCELED,
            Decimal("0"),
            Decimal("0"),
            Decimal("0"),
            Decimal("0"),
        )

    mock_rest.cancel_order.side_effect = observe_cancel
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
async def test_exit_manager_elapsed_time_alone_does_not_trigger_exit(exit_setup):
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
    assert scheduled == []

    # Clean up background tasks
    mock_rest.query_order.assert_not_awaited()
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
    scheduled = exit_mgr.evaluate_active_batches(
        current_prices={"BTCUSDT": Decimal("65000")}
    )
    assert scheduled == [batch_id]

    mock_rest.query_order.assert_not_awaited()
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
    mock_rest.query_order.assert_not_awaited()
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
    mock_rest.query_order.assert_not_awaited()
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
    mock_rest.query_order.assert_not_awaited()
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
    mock_rest.query_order.assert_not_awaited()
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
    mock_rest.query_order.assert_not_awaited()
    await exit_mgr.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "candle_offset, open_price, close_price, expected",
    [(0, "100", "90", False), (15, "100", "90", True), (15, "90", "100", False)],
)
async def test_exit_requires_eligible_formal_candle(
    exit_setup, candle_offset, open_price, close_price, expected
):
    state, _, _, manager = exit_setup
    entry = datetime(2026, 10, 18, 12, 5, tzinfo=UTC)
    batch = PositionBatch(
        "live",
        "a1",
        "candle_batch",
        "BTCUSDT",
        "LONG",
        Decimal("1"),
        Decimal("100"),
        entry,
    )
    state.batches[batch.batch_id] = batch
    start = entry.replace(minute=0) + timedelta(minutes=candle_offset)
    candle = ClosedCandle15m(
        "BTCUSDT",
        start,
        start + timedelta(minutes=15),
        Decimal(open_price),
        Decimal(close_price),
    )
    result = manager.evaluate_active_batches(
        now=candle.candle_end,
        closed_candle=candle,
        current_prices={"BTCUSDT": candle.close_price},
    )
    assert result == ([batch.batch_id] if expected else [])
    await manager.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("reference_price, immediate", [("99", False), ("101", True)])
async def test_candle_exit_uses_configured_profit_targets(
    exit_setup, reference_price, immediate
):
    state, _, _, manager = exit_setup
    entry = datetime(2026, 10, 18, 12, 5, tzinfo=UTC)
    batch = PositionBatch(
        "live",
        "a1",
        "profit_target",
        "BTCUSDT",
        "LONG",
        Decimal("1"),
        Decimal("100"),
        entry,
    )
    state.batches[batch.batch_id] = batch
    start = entry.replace(minute=15)
    candle = ClosedCandle15m(
        "BTCUSDT",
        start,
        start + timedelta(minutes=15),
        Decimal("102"),
        Decimal(reference_price),
    )
    execute = AsyncMock()
    manager.execute_batch_exit = execute
    manager.evaluate_active_batches(
        now=candle.candle_end,
        closed_candle=candle,
        current_prices={"BTCUSDT": candle.close_price},
    )
    await asyncio.sleep(0)
    execute.assert_awaited_once_with(
        batch_id=batch.batch_id,
        limit_price=Decimal("100.8800"),
        immediate=immediate,
    )
    await manager.close()


@pytest.mark.asyncio
async def test_exit_business_rejection_falls_back_to_market_without_degrade(
    exit_setup,
):
    state, repo, rest, manager = exit_setup
    batch = PositionBatch(
        "live",
        "a1",
        "reject-exit",
        "BTCUSDT",
        "LONG",
        Decimal("1"),
        Decimal("65000"),
        datetime.now(UTC),
    )
    state.batches[batch.batch_id] = batch
    from crypto_momentum_lab.next_gen.client import BinanceOrderRejectedError

    rest.post_order.side_effect = [
        BinanceOrderRejectedError(code=-4131, message="price band"),
        {"orderId": 333},
    ]

    await manager.execute_batch_exit(
        batch.batch_id,
        limit_price=Decimal("65500"),
        immediate=False,
    )

    assert state.has_unrecoverable_fault is False
    assert state.system_mode == SystemMode.RUNNING
    assert rest.post_order.await_count == 2
    assert rest.post_order.await_args_list[0].kwargs["order_type"] == "LIMIT"
    assert rest.post_order.await_args_list[1].kwargs["order_type"] == "MARKET"
    assert state.batches[batch.batch_id].quantity == Decimal("1")
