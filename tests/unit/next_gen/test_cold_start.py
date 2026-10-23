from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.next_gen.engine import (
    AccountSnapshot,
    AccountState,
    PositionSnapshot,
)
from crypto_momentum_lab.next_gen.executor import OrderExecutor
from crypto_momentum_lab.next_gen.models import OrderStatus, PositionBatch, SystemMode
from crypto_momentum_lab.next_gen.persistence import InMemoryTradingRepository
from tests.unit.next_gen.factories import risk_config


@pytest.fixture
async def startup():
    repo = InMemoryTradingRepository()
    rest = AsyncMock()
    rest.fetch_open_orders.return_value = []
    rest.fetch_account_information.return_value = AccountSnapshot(
        {"USDT": Decimal("1000")},
        Decimal("900"),
        [],
    )
    state = AccountState("live", "a1", repo, rest)
    state.risk_config = risk_config()
    state.on_ws_status_change(True)
    executor = OrderExecutor(rest, state, repo)
    try:
        yield state, repo, rest, executor
    finally:
        await executor.close()


@pytest.mark.asyncio
async def test_flat_startup_initializes_balances_without_orders(startup):
    state, _, rest, executor = startup
    await executor.load_active_state()
    await executor.restore_startup_state()
    assert state.wallet_balance == Decimal("1000")
    assert state.available_balance_snapshot == Decimal("900")
    rest.fetch_startup_order.assert_not_awaited()
    rest.post_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_startup_refuses_unmanaged_position(startup):
    _, _, rest, executor = startup
    rest.fetch_account_information.return_value = AccountSnapshot(
        {},
        Decimal("100"),
        [PositionSnapshot("BTCUSDT", "LONG", Decimal("1"))],
    )
    with pytest.raises(RuntimeError, match="启动基线不一致"):
        await executor.restore_startup_state()
    rest.post_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_startup_recovers_fill_while_process_was_down(startup):
    state, repo, rest, executor = startup
    order = state.create_entry_order(
        "BTCUSDT",
        "LONG",
        Decimal("2"),
        Decimal("100"),
        "restart",
    )
    order.status = OrderStatus.SUBMITTED
    await repo.async_save_order(order)
    await executor.load_active_state()
    rest.fetch_startup_order.return_value = {
        "clientOrderId": order.client_order_id,
        "symbol": "BTCUSDT",
        "side": "BUY",
        "positionSide": "LONG",
        "executedQty": "2",
        "avgPrice": "105",
        "orderId": 7,
        "status": "FILLED",
        "updateTime": 1792300000000,
    }
    rest.fetch_account_information.return_value = AccountSnapshot(
        {},
        Decimal("100"),
        [PositionSnapshot("BTCUSDT", "LONG", Decimal("2"))],
    )
    await executor.restore_startup_state()
    assert state.batches["restart"].quantity == Decimal("2")
    assert state.batches["restart"].entry_price == Decimal("105")
    assert state.orders[order.client_order_id].status == OrderStatus.FILLED
    assert not state.active_entry_orders
    rest.post_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_startup_keeps_known_open_exit_and_its_deadline(startup):
    state, repo, rest, executor = startup
    opened = datetime(2026, 10, 18, 0, 0, tzinfo=UTC)
    batch = PositionBatch(
        "live",
        "a1",
        "old_exit",
        "BTCUSDT",
        "LONG",
        Decimal("1"),
        Decimal("100"),
        opened,
    )
    state.batches[batch.batch_id] = batch
    order = state.create_exit_order(batch.batch_id, Decimal("1"), Decimal("101"))
    order.status = OrderStatus.OPEN
    deadline_anchor = batch.exit_order_submitted_at
    await repo.async_save_order_and_batch(order, batch)
    await executor.load_active_state()
    rest.fetch_startup_order.return_value = {
        "clientOrderId": order.client_order_id,
        "symbol": "BTCUSDT",
        "side": "SELL",
        "positionSide": "LONG",
        "executedQty": "0",
        "avgPrice": "0",
        "orderId": 8,
        "status": "NEW",
        "updateTime": 1792300000000,
    }
    rest.fetch_open_orders.return_value = [{"clientOrderId": order.client_order_id}]
    rest.fetch_account_information.return_value = AccountSnapshot(
        {},
        Decimal("100"),
        [PositionSnapshot("BTCUSDT", "LONG", Decimal("1"))],
    )
    await executor.restore_startup_state()
    recovered = state.batches[batch.batch_id]
    assert recovered.exit_order_submitted_at == deadline_anchor
    assert recovered.exiting_quantity == Decimal("1")
    assert state.active_exit_orders[("a1", batch.batch_id)] == order.client_order_id
    rest.post_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_startup_reader_cannot_run_after_admission(startup):
    state, _, rest, executor = startup
    state.set_system_mode(SystemMode.RUNNING)
    with pytest.raises(RuntimeError, match="only allowed during startup"):
        await executor.restore_startup_state()
    rest.fetch_account_information.assert_not_awaited()
