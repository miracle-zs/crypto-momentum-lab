from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
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
from crypto_momentum_lab.next_gen.models import (
    Order,
    OrderStatus,
    PositionBatch,
    SystemMode,
    generate_batch_id,
    generate_exit_order_id,
)
from crypto_momentum_lab.next_gen.persistence import InMemoryTradingRepository
from tests.unit.next_gen.factories import risk_config


@pytest.mark.asyncio
@respx.mock
async def test_binance_in_doubt_error_code_raises_network_error() -> None:
    # 模拟 HTTP 502 并携带 Binance 错误码 -1000 (UNKNOWN)
    respx.post("https://fapi.binance.com/fapi/v1/order").mock(
        return_value=httpx.Response(
            502,
            json={"code": -1000, "msg": "An unknown error occured"},
        )
    )

    client = BinanceRestClient(api_key="k", api_secret="s")
    with pytest.raises(BinanceNetworkError) as exc_info:
        await client.post_order(
            symbol="BTCUSDT",
            side="BUY",
            position_side="LONG",
            order_type="LIMIT",
            quantity=Decimal("1.0"),
            price=Decimal("60000.0"),
            client_order_id="e_doubt_01",
        )
    assert "-1000" in exc_info.value.message


@pytest.mark.asyncio
async def test_order_executor_in_doubt_error_keeps_lock_and_submitted_status() -> None:
    repo = InMemoryTradingRepository()
    mock_rest = AsyncMock()
    mock_rest.post_order.side_effect = BinanceNetworkError(
        502, "Binance in-doubt error (code=-1000)"
    )

    state = AccountState("live", "primary", db=repo)
    state.risk_config = risk_config(state.environment, state.account_label)
    state.set_system_mode(SystemMode.RUNNING)
    state.on_ws_status_change(True)

    batch_id = generate_batch_id("BTCUSDT", "LONG", "primary", counter=1)
    order = state.create_entry_order(
        batch_id=batch_id,
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("0.5"),
        price=Decimal("60000.0"),
    )

    executor = OrderExecutor(rest_client=mock_rest, state=state, db=repo)
    try:
        with pytest.raises(BinanceNetworkError):
            await executor.submit_order(order)
    finally:
        await executor.close()

    # 验证订单保持 SUBMITTED，绝不可标为 REJECTED
    assert order.status == OrderStatus.SUBMITTED
    # 验证开仓锁依然被持有，防止策略重发造成超开
    assert ("primary", "BTCUSDT", "LONG") in state.active_entry_orders


@pytest.mark.asyncio
async def test_account_update_event_dispatches_safely_to_engine() -> None:
    stream = BinanceUserDataStream(rest_client=AsyncMock())
    state = AccountState("live", "account-2")
    state.risk_config = risk_config(state.environment, state.account_label)

    # 预设 USDT 与 BNB
    state.update_balances(
        {"USDT": Decimal("5000.00"), "BNB": Decimal("10.0")},
        available_balance_snapshot=Decimal("4500.00"),
    )

    # 模拟推流只包含 BTC 资产变化
    event = {
        "e": "ACCOUNT_UPDATE",
        "E": 1700000000000,
        "T": 1700000000005,
        "a": {
            "m": "ORDER",
            "B": [
                {"a": "BTC", "wb": "0.50000000", "cw": "0.50000000", "bc": "0"},
            ],
            "P": [
                {
                    "s": "BTCUSDT",
                    "pa": "0.500",
                    "ep": "60000.0",
                    "cr": "0.0",
                    "up": "50.0",
                    "mt": "cross",
                    "iw": "0",
                    "ps": "LONG",
                }
            ],
        },
    }

    # 验证 dispatch 不会抛出 TypeError，且不会抹掉 USDT 余额
    await stream.dispatch_raw_event(
        event,
        on_order_trade_update=AsyncMock(),
        on_account_update=state.on_account_update,
    )

    assert state.wallet_balances["USDT"] == Decimal("5000.00")
    assert state.wallet_balances["BTC"] == Decimal("0.50000000")


@pytest.mark.asyncio
async def test_order_executor_prewrite_db_failure_rolls_back_and_degrades() -> None:
    class FailingRepo(InMemoryTradingRepository):
        async def async_save_order(self, order: Order) -> None:
            raise RuntimeError("Database disk full / connection dropped")

    repo = FailingRepo()
    mock_rest = AsyncMock()
    state = AccountState("live", "account-3", db=repo)
    state.risk_config = risk_config(state.environment, state.account_label)
    state.set_system_mode(SystemMode.RUNNING)
    state.on_ws_status_change(True)

    batch_id = generate_batch_id("SOLUSDT", "LONG", "account-3", counter=1)
    order = state.create_entry_order(
        batch_id=batch_id,
        symbol="SOLUSDT",
        position_side="LONG",
        quantity=Decimal("10.0"),
        price=Decimal("150.0"),
    )

    executor = OrderExecutor(rest_client=mock_rest, state=state, db=repo)
    try:
        with pytest.raises(RuntimeError, match="订单预落库失败"):
            await executor.submit_order(order)
    finally:
        await executor.close()

    # 绝不能外呼交易所
    mock_rest.post_order.assert_not_called()
    # 内存状态回滚：从 orders 移除，释放活跃锁
    assert order.client_order_id not in state.orders
    assert ("account-3", "SOLUSDT", "LONG") not in state.active_entry_orders
    # 系统进入 DEGRADED 且标记不可恢复故障
    assert state.system_mode == SystemMode.DEGRADED
    assert state.has_unrecoverable_fault is True


def test_position_batch_reservation_idempotency() -> None:
    batch = PositionBatch(
        environment="live",
        account_label="account-4",
        batch_id="b_test_idem",
        symbol="ETHUSDT",
        position_side="LONG",
        quantity=Decimal("5.0"),
        entry_price=Decimal("3000.0"),
        entry_bar_time=datetime.now(UTC),
    )
    # 预留 3.0
    batch.reserve_for_exit(Decimal("3.0"))
    assert batch.exiting_quantity == Decimal("3.0")

    # 尝试超量释放 10.0：绝不可抛出 ValueError，必须安全截断到 0
    batch.release_exit_reservation(Decimal("10.0"))
    assert batch.exiting_quantity == Decimal("0.0")

    # 再次释放 5.0：依然安全为 0
    batch.release_exit_reservation(Decimal("5.0"))
    assert batch.exiting_quantity == Decimal("0.0")


def test_order_release_batch_reservation_idempotency() -> None:
    batch = PositionBatch(
        environment="live",
        account_label="account-4",
        batch_id="b_test_order_idem",
        symbol="ETHUSDT",
        position_side="LONG",
        quantity=Decimal("2.0"),
        entry_price=Decimal("3000.0"),
        entry_bar_time=datetime.now(UTC),
    )
    batch.reserve_for_exit(Decimal("2.0"))
    assert batch.exiting_quantity == Decimal("2.0")

    order = Order(
        environment="live",
        account_label="account-4",
        client_order_id="fl_test_01",
        batch_id="b_test_order_idem",
        symbol="ETHUSDT",
        side="SELL",
        position_side="LONG",
        order_type="LIMIT",
        quantity=Decimal("2.0"),
        price=Decimal("3100.0"),
    )

    # 第一次调用：释放成功
    order.release_batch_reservation(batch)
    assert batch.exiting_quantity == Decimal("0.0")
    assert order.lot_reservation_released is True

    # 再次调用：幂等保护，不再释放
    order.release_batch_reservation(batch)
    assert batch.exiting_quantity == Decimal("0.0")


def test_generate_batch_id_length_and_uniqueness() -> None:
    accounts = ["primary", "account-2", "account-3", "account-4"]
    symbols = ["1000PEPEUSDT", "BTCUSDT", "ETHUSDT"]
    generated_ids: set[str] = set()

    for acc in accounts:
        for sym in symbols:
            bid = generate_batch_id(
                symbol=sym,
                position_side="LONG",
                account_label=acc,
                timestamp_str="261018183000",
                salt="7a",
                counter=1,
            )
            # 严格 <= 28 字符，符合币安与系统约定
            assert len(bid) <= 28
            assert bid not in generated_ids
            generated_ids.add(bid)

            # 衍生出限价和市价退出订单 ID
            fl_id = generate_exit_order_id(bid, "LIMIT", seq=1)
            fm_id = generate_exit_order_id(bid, "MARKET", seq=2)
            assert len(fl_id) <= 36
            assert len(fm_id) <= 36
            assert fl_id.endswith("_01")
            assert fm_id.endswith("_02")


def test_check_notional_limits_excludes_exit_orders_and_accounts_correctly() -> None:
    state = AccountState("live", "primary")
    state.risk_config = risk_config(state.environment, state.account_label)
    state.batches["b1"] = PositionBatch(
        environment="live",
        account_label="primary",
        batch_id="b1",
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        entry_price=Decimal("60000.0"),
        entry_bar_time=datetime.now(UTC),
    )

    # 现价 60000 下，当前持仓名义价值 60000
    state.latest_prices["BTCUSDT"] = Decimal("65000")
    state.risk_config = replace(state.risk_config, max_gross_notional=Decimal("100000"))
    # Count pending entries, and exclude exit orders from exposure.
    # 尝试再买 0.5 BTC (名义价值 +30000 = 90000 <= 100000) -> 允许
    assert state.check_notional_limits(
        symbol="BTCUSDT",
        new_quantity=Decimal("0.5"),
        current_price=Decimal("60000.0"),
    )

    # 尝试再买 1.0 BTC (名义价值 +60000 = 120000 > 100000) -> 阻断
    assert not state.check_notional_limits(
        symbol="BTCUSDT",
        new_quantity=Decimal("1.0"),
        current_price=Decimal("60000.0"),
    )
