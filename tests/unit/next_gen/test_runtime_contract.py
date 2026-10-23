"""Regressions from the real protocol and trading lifecycle readiness review."""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.market_data.hub import MarketStateHub, MarketStateHubConfig
from crypto_momentum_lab.next_gen.daemon import NextGenDaemon
from crypto_momentum_lab.next_gen.engine import (
    AccountState,
)
from crypto_momentum_lab.next_gen.executor import OrderExecutor
from crypto_momentum_lab.next_gen.market_feed import MarketDataFeed
from crypto_momentum_lab.next_gen.models import (
    Order,
    OrderStatus,
    PositionBatch,
    SystemMode,
)
from crypto_momentum_lab.next_gen.persistence import InMemoryTradingRepository
from tests.unit.next_gen.factories import risk_config, symbol_rules
from tests.unit.persistence.postgres.test_runtime_state_repository import fixture_state


@pytest.fixture
async def execution():
    repository = InMemoryTradingRepository()
    rest = AsyncMock()
    state = AccountState("live", "review", repository, rest)
    state.risk_config = risk_config(state.environment, state.account_label)
    state.set_system_mode(SystemMode.RUNNING)
    state.on_ws_status_change(True)
    executor = OrderExecutor(rest, state, repository)
    try:
        yield state, repository, rest, executor
    finally:
        await executor.close()


@pytest.mark.asyncio
async def test_feed_subscribes_to_real_hub_and_receives_real_encoded_state():
    hub = MarketStateHub(MarketStateHubConfig(host="127.0.0.1", port=0))
    await hub.start()
    connected, received, stop = asyncio.Event(), asyncio.Event(), asyncio.Event()
    bars = []

    def on_bar(bar):
        bars.append(bar)
        received.set()

    feed = MarketDataFeed(
        ["BTCUSDT"],
        on_bar,
        hub_url=hub.url,
        environment="research",
        consumer_id="review-live-account",
        on_connection_change=lambda online: connected.set() if online else None,
    )
    task = asyncio.create_task(feed.run(stop))
    try:
        await asyncio.wait_for(connected.wait(), 2)
        assert hub.subscriber_count == 1
        state = fixture_state("BTCUSDT", 0)
        await hub.publish((state,))
        await asyncio.wait_for(received.wait(), 2)
        assert bars[0] == state
        assert bars[0].close_price == state.close_price
    finally:
        stop.set()
        await feed.close()
        await asyncio.wait_for(task, 2)
        await hub.stop()


@pytest.mark.asyncio
async def test_cumulative_ws_average_accounts_only_for_missing_fill(execution):
    state, _, _, _ = execution
    batch = PositionBatch(
        "live",
        "review",
        "cost_basis",
        "BTCUSDT",
        "LONG",
        Decimal("2"),
        Decimal("100"),
        datetime.now(UTC),
    )
    state.batches[batch.batch_id] = batch
    order = state.create_exit_order(batch.batch_id, Decimal("2"), Decimal("110"))
    await state.on_order_trade_update(
        order.client_order_id,
        "BTCUSDT",
        "LONG",
        "SELL",
        OrderStatus.PARTIALLY_FILLED,
        Decimal("1"),
        Decimal("110"),
        Decimal("1"),
        Decimal("110"),
    )

    async def receive_fill():
        await state.on_order_trade_update(
            order.client_order_id,
            "BTCUSDT",
            "LONG",
            "SELL",
            OrderStatus.FILLED,
            Decimal("2"),
            Decimal("115"),
            Decimal("1"),
            Decimal("120"),
        )

    await receive_fill()
    assert batch.realized_pnl == Decimal("30")
    assert batch.quantity == 0
    await receive_fill()
    assert batch.realized_pnl == Decimal("30")


@pytest.mark.asyncio
async def test_cancel_without_ws_confirmation_never_queries_rest(execution):
    state, _, rest, executor = execution
    order = state.create_entry_order(
        "BTCUSDT", "LONG", Decimal("2"), Decimal("100"), "ws_only"
    )
    assert not await executor.cancel_and_confirm(order, timeout_sec=0)
    assert order.status == OrderStatus.RECONCILING
    assert state.active_entry_orders
    rest.query_order.assert_not_awaited()
    rest.fetch_account_information.assert_not_awaited()


@pytest.mark.asyncio
async def test_stream_gap_does_not_launch_rest_reconciliation(
    execution, account_config
):
    state, _, rest, _ = execution
    daemon = NextGenDaemon("live", "review", state.db, rest, account_config)
    order = daemon.state.create_entry_order(
        "BTCUSDT", "LONG", Decimal("2"), Decimal("100"), "reconnect"
    )
    daemon._on_ws_status_change(False)
    daemon._on_ws_status_change(True)
    await asyncio.sleep(0)
    assert order.status == OrderStatus.RECONCILING
    assert not daemon.state.can_submit_entry_order()
    rest.query_order.assert_not_awaited()
    rest.fetch_account_information.assert_not_awaited()


@pytest.mark.asyncio
async def test_dynamic_symbol_rules_are_shared_and_exit_leaves_no_dust(
    execution, account_config
):
    _, repository, rest, _ = execution
    daemon = NextGenDaemon(
        "live", "review", repository, rest, account_config, symbols=["SOLUSDT"]
    )
    state = daemon.state
    state.set_system_mode(SystemMode.RUNNING)
    state.on_ws_status_change(True)
    daemon.strategy_engine.symbol_rules.update_from_exchange_info(
        {
            "symbols": [
                {
                    "symbol": "SOLUSDT",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                        {
                            "filterType": "LOT_SIZE",
                            "stepSize": "0.001",
                            "minQty": "0.001",
                        },
                    ],
                }
            ]
        }
    )
    assert daemon.exit_manager.symbol_rules is daemon.strategy_engine.symbol_rules
    batch = PositionBatch(
        "live",
        "review",
        "dynamic_rules",
        "SOLUSDT",
        "LONG",
        Decimal("1.234"),
        Decimal("100"),
        datetime.now(UTC),
    )
    state.batches[batch.batch_id] = batch

    async def accept_and_fill(**params):
        order = state.orders[params["client_order_id"]]
        await state.on_order_trade_update(
            order.client_order_id,
            order.symbol,
            order.position_side,
            order.side,
            OrderStatus.FILLED,
            order.quantity,
            Decimal("100"),
            order.quantity,
            Decimal("100"),
        )
        return {}

    rest.post_order.side_effect = accept_and_fill
    try:
        await daemon.exit_manager.execute_batch_exit(
            batch.batch_id,
            Decimal("100"),
            grace_period_sec=0,
        )
        assert batch.quantity == 0
        assert batch.status == "CLOSED"
    finally:
        await daemon.order_executor.close()


@pytest.mark.asyncio
async def test_drain_releases_cancelled_exit_reservation_before_market_sweep(execution):
    state, repository, rest, executor = execution
    state.is_draining = True
    state.set_system_mode(SystemMode.DRAINING)
    state.ws_connected = True
    batch = PositionBatch(
        "live",
        "review",
        "drain",
        "BTCUSDT",
        "LONG",
        Decimal("1"),
        Decimal("100"),
        datetime.now(UTC),
        status="EXITING",
        exiting_quantity=Decimal("1"),
    )
    old_order = Order(
        "live",
        "review",
        "fl_drain_01",
        "drain",
        "BTCUSDT",
        "SELL",
        "LONG",
        "LIMIT",
        Decimal("1"),
        Decimal("100"),
        status=OrderStatus.OPEN,
    )
    await repository.async_save_batch(batch)
    await repository.async_save_order(old_order)
    await executor.load_active_state()

    async def cancel(**params):
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

    async def fill(**params):
        order = state.orders[params["client_order_id"]]
        await state.on_order_trade_update(
            order.client_order_id,
            order.symbol,
            order.position_side,
            order.side,
            OrderStatus.FILLED,
            order.quantity,
            Decimal("101"),
            order.quantity,
            Decimal("101"),
        )

    rest.cancel_order.side_effect = cancel
    rest.post_order.side_effect = fill
    assert await executor.drain(symbol_rules()) == 1
    assert state.batches["drain"].quantity == 0
    assert state.batches["drain"].exiting_quantity == 0
    assert rest.post_order.await_args.kwargs["client_order_id"] == "fm_drain_02"
