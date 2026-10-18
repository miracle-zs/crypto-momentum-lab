from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.next_gen.client import BinanceOrderRejectedError
from crypto_momentum_lab.next_gen.engine import AccountState
from crypto_momentum_lab.next_gen.executor import OrderExecutor
from crypto_momentum_lab.next_gen.models import (
    OrderStatus,
    SystemMode,
    generate_batch_id,
)
from crypto_momentum_lab.next_gen.persistence import InMemoryTradingRepository
from tests.unit.next_gen.factories import risk_config


@pytest.fixture
async def test_setup():
    repo = InMemoryTradingRepository()
    mock_rest = AsyncMock()
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
    try:
        yield state, repo, mock_rest, executor
    finally:
        await executor.close()


@pytest.mark.asyncio
async def test_order_executor_submit_order_success(test_setup):
    state, repo, mock_rest, executor = test_setup
    mock_rest.post_order.return_value = {
        "orderId": 7777777,
        "clientOrderId": "e_BTC_L_01",
    }

    batch_id = generate_batch_id(
        symbol="BTCUSDT",
        position_side="LONG",
        account_label="a1",
        timestamp_str="261018120000",
        salt="9f",
        counter=1,
    )
    order = state.create_entry_order(
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        price=Decimal("65000"),
        batch_id=batch_id,
    )

    res = await executor.submit_order(order)
    assert res["orderId"] == 7777777
    assert order.status == OrderStatus.SUBMITTED

    # Check pre-wire save happened in repo
    saved = repo.get_order("live", "a1", order.client_order_id)
    assert saved is not None
    assert saved.status == OrderStatus.SUBMITTED


@pytest.mark.asyncio
async def test_order_executor_submit_order_rejected_cleans_up(test_setup):
    state, repo, mock_rest, executor = test_setup
    mock_rest.post_order.side_effect = BinanceOrderRejectedError(
        code=-2019, message="Margin is insufficient."
    )

    batch_id = generate_batch_id(
        symbol="BTCUSDT",
        position_side="LONG",
        account_label="a1",
        timestamp_str="261018120000",
        salt="9f",
        counter=1,
    )
    order = state.create_entry_order(
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        price=Decimal("65000"),
        batch_id=batch_id,
    )

    with pytest.raises(BinanceOrderRejectedError):
        await executor.submit_order(order)

    # Order must be marked REJECTED
    assert order.status == OrderStatus.REJECTED
    # Active entry lock must be released
    assert ("a1", "BTCUSDT", "LONG") not in state.active_entry_orders
    # State in repo must be updated to REJECTED
    saved = repo.get_order("live", "a1", order.client_order_id)
    assert saved is not None
    assert saved.status == OrderStatus.REJECTED


@pytest.mark.asyncio
async def test_entry_configures_symbol_settings_once(test_setup):
    state, repo, mock_rest, executor = test_setup
    executor._entry_leverage = 5
    executor._margin_type = "CROSSED"

    await executor._ensure_symbol_settings("BTCUSDT")
    await executor._ensure_symbol_settings("BTCUSDT")

    mock_rest.set_margin_type.assert_awaited_once_with("BTCUSDT", "CROSSED")
    mock_rest.set_leverage.assert_awaited_once_with("BTCUSDT", leverage=5)


@pytest.mark.asyncio
async def test_entry_caches_symbol_setting_failure(test_setup):
    state, repo, mock_rest, executor = test_setup
    mock_rest.set_leverage.side_effect = BinanceOrderRejectedError(
        code=-4028, message="Leverage 5 is not valid"
    )

    with pytest.raises(BinanceOrderRejectedError):
        await executor._ensure_symbol_settings("BTCUSDT")
    with pytest.raises(RuntimeError, match="previously failed"):
        await executor._ensure_symbol_settings("BTCUSDT")

    assert mock_rest.set_leverage.await_count == 1
