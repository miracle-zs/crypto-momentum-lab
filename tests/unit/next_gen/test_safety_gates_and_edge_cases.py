from __future__ import annotations

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock

import httpx
import pytest
import respx

from crypto_momentum_lab.next_gen.client import (
    BinanceNetworkError,
    BinanceRestClient,
    BinanceUserDataStream,
)
from crypto_momentum_lab.next_gen.engine import AccountState
from crypto_momentum_lab.next_gen.executor import OrderExecutor
from crypto_momentum_lab.next_gen.exit_manager import ExitManager
from crypto_momentum_lab.next_gen.models import (
    Order,
    OrderStatus,
    PositionBatch,
    SystemMode,
    generate_batch_id,
)
from crypto_momentum_lab.next_gen.persistence import InMemoryTradingRepository
from tests.unit.next_gen.factories import risk_config, symbol_rules


@pytest.mark.asyncio
@respx.mock
async def test_binance_rest_5xx_raises_binance_network_error() -> None:
    respx.post("https://fapi.binance.com/fapi/v1/order").mock(
        return_value=httpx.Response(502, text="Bad Gateway")
    )

    client = BinanceRestClient(api_key="k", api_secret="s")
    with pytest.raises(BinanceNetworkError, match="502"):
        await client.post_order(
            symbol="BTCUSDT",
            side="BUY",
            position_side="LONG",
            order_type="LIMIT",
            quantity=Decimal("1.0"),
            price=Decimal("60000.0"),
            client_order_id="e_test_502",
        )


@pytest.mark.asyncio
async def test_order_executor_network_error_keeps_order_in_doubt_and_locks_held() -> (
    None
):
    repo = InMemoryTradingRepository()
    mock_rest = AsyncMock()
    mock_rest.post_order.side_effect = BinanceNetworkError(504, "Gateway Timeout")

    state = AccountState("live", "a1", db=repo)
    state.risk_config = risk_config(state.environment, state.account_label)
    state.set_system_mode(SystemMode.RUNNING)
    state.set_ws_connected(True)
    order = state.create_entry_order(
        batch_id="b1",
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        price=Decimal("60000.0"),
    )

    executor = OrderExecutor(rest_client=mock_rest, state=state, db=repo)
    try:
        with pytest.raises(BinanceNetworkError):
            await executor.submit_order(order)
    finally:
        await executor.close()

    # 关键防重开验证：订单状态绝对不能是 REJECTED，开仓锁必须继续持有以防策略重复发单！
    assert order.status == OrderStatus.SUBMITTED
    assert ("a1", "BTCUSDT", "LONG") in state.active_entry_orders


@pytest.mark.asyncio
async def test_order_executor_safety_gate_blocks_entry_when_not_running() -> None:
    repo = InMemoryTradingRepository()
    mock_rest = AsyncMock()

    state = AccountState("live", "a1", db=repo)
    state.risk_config = risk_config(state.environment, state.account_label)
    state.set_system_mode(SystemMode.DEGRADED)  # 降级模式
    order = Order(
        environment="live",
        account_label="a1",
        client_order_id="e_b1_00",
        batch_id="b1",
        symbol="BTCUSDT",
        side="BUY",
        position_side="LONG",
        order_type="LIMIT",
        quantity=Decimal("1.0"),
        price=Decimal("60000.0"),
    )

    executor = OrderExecutor(rest_client=mock_rest, state=state, db=repo)
    with pytest.raises(RuntimeError, match="安全门阻断开仓单"):
        await executor.submit_order(order)

    # 验证物理发单接口从未被调用
    mock_rest.post_order.assert_not_called()


@pytest.mark.asyncio
async def test_user_data_stream_parses_expired_in_match() -> None:
    mock_rest = AsyncMock()
    stream = BinanceUserDataStream(rest_client=mock_rest)

    order_updates = []

    async def on_order_update(**kwargs):
        order_updates.append(kwargs)

    raw_event = {
        "e": "ORDER_TRADE_UPDATE",
        "o": {
            "c": "fl_test_1",
            "s": "BTCUSDT",
            "ps": "LONG",
            "S": "SELL",
            "X": "EXPIRED_IN_MATCH",
            "z": "0",
            "ap": "0",
            "l": "0",
            "L": "0",
            "i": 99999,
        },
    }

    await stream.dispatch_raw_event(
        raw_event,
        on_order_trade_update=on_order_update,
        on_account_update=AsyncMock(),
    )

    assert len(order_updates) == 1
    assert order_updates[0]["order_status"] == OrderStatus.CANCELED


def test_account_update_merges_balances_without_wiping_other_assets() -> None:
    state = AccountState("live", "a1")
    state.risk_config = risk_config(state.environment, state.account_label)
    # 初始快照：持有 USDT 与 BTC
    state.update_balances(
        {
            "USDT": Decimal("10000.00"),
            "BTC": Decimal("0.5"),
        }
    )
    assert state.wallet_balance == Decimal("10000.00")

    # 收到仅包含 BNB 变化的增量推流
    state.update_balances(
        {
            "BNB": Decimal("2.5"),
        }
    )

    # 验证 USDT 与 BTC 绝未被清空
    assert state.wallet_balances["USDT"] == Decimal("10000.00")
    assert state.wallet_balances["BTC"] == Decimal("0.5")
    assert state.wallet_balances["BNB"] == Decimal("2.5")
    assert state.wallet_balance == Decimal("10000.00")


def test_generate_batch_id_uniqueness_and_salt_preservation() -> None:
    # 针对超长标的 1000LUNCUSDT，验证字符长度限制与盐值保留
    bid1 = generate_batch_id(
        symbol="1000LUNCUSDT",
        position_side="LONG",
        account_label="a1",
        salt="9f",
        counter=1,
    )
    bid2 = generate_batch_id(
        symbol="1000LUNCUSDT",
        position_side="LONG",
        account_label="a1",
        salt="a2",
        counter=1,
    )

    assert len(bid1) <= 31
    assert len(bid2) <= 31
    assert "9f" in bid1
    assert "a2" in bid2
    assert bid1 != bid2


@pytest.mark.asyncio
async def test_exit_manager_waits_for_ws_terminal_before_market_sweep() -> None:
    repo = InMemoryTradingRepository()
    mock_rest = AsyncMock()
    mock_rest.post_order.return_value = {"orderId": 888}
    mock_rest.cancel_order.return_value = {"orderId": 888, "status": "CANCELED"}

    state = AccountState("live", "a1", db=repo)
    state.risk_config = risk_config(state.environment, state.account_label)
    state.set_system_mode(SystemMode.RUNNING)
    state.set_ws_connected(True)

    from datetime import UTC, datetime

    batch = PositionBatch(
        environment="live",
        account_label="a1",
        batch_id="b_wait_ws",
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        entry_price=Decimal("60000"),
        entry_bar_time=datetime.now(UTC),
        status="ACTIVE",
    )
    state.batches[batch.batch_id] = batch

    executor = OrderExecutor(rest_client=mock_rest, state=state, db=repo)
    exit_mgr = ExitManager(
        rest_client=mock_rest,
        state=state,
        executor=executor,
        symbol_rules=symbol_rules(),
        grace_period_sec=0.01,
        decision_profit_pct=Decimal("0.001"),
        grace_profit_pct=Decimal("0.0088"),
    )

    # 模拟在 cancel 发出后，WS 异步推来终态 CANCELED 并核算剩余
    async def simulate_ws_response():
        await asyncio.sleep(0.03)
        limit_order = state.orders.get("fl_b_wait_ws_01")
        if limit_order:
            await state.on_order_trade_update(
                client_order_id=limit_order.client_order_id,
                symbol="BTCUSDT",
                position_side="LONG",
                side="SELL",
                order_status=OrderStatus.CANCELED,
                cum_filled_qty=Decimal("0.4"),  # 撤单前成交了 0.4
                avg_price=Decimal("60500"),
                last_filled_qty=Decimal("0.4"),
                last_filled_price=Decimal("60500"),
            )

    asyncio.create_task(simulate_ws_response())

    try:
        await exit_mgr.execute_batch_exit(
            batch_id="b_wait_ws",
            limit_price=Decimal("60500"),
            grace_period_sec=0.01,
        )
    finally:
        await exit_mgr.close()
        await executor.close()

    # 验证最终市价扫平单是基于 WS 吸收后的准确净剩余 0.6 发出的
    market_order = state.orders.get("fm_b_wait_ws_02")
    assert market_order is not None
    assert market_order.quantity == Decimal("0.6")
    assert market_order.order_type == "MARKET"
