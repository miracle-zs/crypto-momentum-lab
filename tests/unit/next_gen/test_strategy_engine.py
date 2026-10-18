from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.next_gen.engine import AccountState
from crypto_momentum_lab.next_gen.executor import OrderExecutor
from crypto_momentum_lab.next_gen.models import OrderStatus, SystemMode
from crypto_momentum_lab.next_gen.persistence import InMemoryTradingRepository
from crypto_momentum_lab.next_gen.strategy_engine import Bar15s, MomentumStrategyEngine


def generate_bars(
    symbol: str,
    start_price: Decimal,
    end_price: Decimal,
    count: int = 150,
) -> list[Bar15s]:
    base_time = datetime.now(UTC) - timedelta(seconds=15 * count)
    step = (end_price - start_price) / Decimal(count)
    bars = []
    current_p = start_price
    for i in range(count):
        close_t = base_time + timedelta(seconds=15 * (i + 1))
        current_p += step
        bars.append(
            Bar15s(
                symbol=symbol,
                close_time=close_t,
                open_price=current_p,
                high_price=current_p * Decimal("1.001"),
                low_price=current_p * Decimal("0.999"),
                close_price=current_p,
                volume=Decimal("10.0"),
            )
        )
    return bars


@pytest.fixture
def strategy_setup():
    repo = InMemoryTradingRepository()
    mock_rest = AsyncMock()
    mock_rest.post_order.return_value = {"orderId": 12345}

    state = AccountState(
        environment="live",
        account_label="a1",
        db=repo,
        rest_client=mock_rest,
    )
    state.set_system_mode(SystemMode.RUNNING)
    state.ws_connected = True

    executor = OrderExecutor(rest_client=mock_rest, state=state, db=repo)
    engine = MomentumStrategyEngine(
        state=state,
        order_executor=executor,
        symbols=["BTCUSDT", "ETHUSDT"],
        min_warmup_bars=10,
        momentum_window=10,
        min_momentum_threshold=Decimal("0.01"),  # +1%
        batch_notional=Decimal("100.0"),
        symbol_cap=Decimal("1000.0"),
        account_cap=Decimal("5000.0"),
        eval_interval_sec=0.05,
    )
    return state, executor, engine, mock_rest


@pytest.mark.asyncio
async def test_strategy_warmup_and_bar_buffering(strategy_setup):
    _, _, engine, _ = strategy_setup
    bars = generate_bars("BTCUSDT", Decimal("50000"), Decimal("55000"), count=25)
    engine.feed_bars(bars)

    assert len(engine.bar_buffers["BTCUSDT"]) == 20  # clamped to 2 * min_warmup_bars
    assert engine.latest_prices["BTCUSDT"] == bars[-1].close_price
    await engine.warmup_market_data()


def test_strategy_momentum_calculation(strategy_setup):
    _, _, engine, _ = strategy_setup
    # 10 bars from 100 to 110 (+10% gain)
    bars = generate_bars("BTCUSDT", Decimal("100"), Decimal("110"), count=11)
    engine.feed_bars(bars)

    mom = engine.calculate_momentum("BTCUSDT")
    assert mom is not None
    assert mom > Decimal("0.07")


@pytest.mark.asyncio
async def test_strategy_evaluate_generates_and_submits_entry_order(strategy_setup):
    state, executor, engine, mock_rest = strategy_setup
    try:
        # BTC up 10%
        btc_bars = generate_bars(
            "BTCUSDT", Decimal("50000"), Decimal("55000"), count=15
        )
        # ETH up 2%
        eth_bars = generate_bars("ETHUSDT", Decimal("3000"), Decimal("3060"), count=15)
        engine.feed_bars(btc_bars)
        engine.feed_bars(eth_bars)

        submitted = await engine.evaluate_and_generate_orders()
        assert len(submitted) == 1  # top_n=1, BTC chosen
        order_id = submitted[0]
        assert "BTC" in order_id
        assert order_id in state.orders

        order = state.orders[order_id]
        assert order.status == OrderStatus.SUBMITTED
        assert order.symbol == "BTCUSDT"
        assert order.position_side == "LONG"
        mock_rest.post_order.assert_awaited_once()

        # Duplicate check: running again shouldn't re-submit for BTC
        second_run = await engine.evaluate_and_generate_orders()
        assert len(second_run) == 0
    finally:
        await executor.close()


@pytest.mark.asyncio
async def test_strategy_notional_cap_rejects_oversized_intent(strategy_setup):
    state, executor, engine, mock_rest = strategy_setup
    try:
        # Set symbol cap very low so $100 notional exceeds it
        engine.symbol_cap = Decimal("50.0")
        btc_bars = generate_bars(
            "BTCUSDT", Decimal("50000"), Decimal("55000"), count=15
        )
        engine.feed_bars(btc_bars)

        submitted = await engine.evaluate_and_generate_orders()
        assert len(submitted) == 0
        mock_rest.post_order.assert_not_awaited()
    finally:
        await executor.close()


@pytest.mark.asyncio
async def test_strategy_loop_lifecycle_with_stop_event(strategy_setup):
    _, executor, engine, _ = strategy_setup
    try:
        stop_event = asyncio.Event()

        async def stop_soon():
            await asyncio.sleep(0.02)
            stop_event.set()

        asyncio.create_task(stop_soon())
        await engine.run(stop_event=stop_event)
    finally:
        await executor.close()


@pytest.mark.asyncio
async def test_strategy_entry_ttl_cancellation(strategy_setup):
    state, executor, engine, mock_rest = strategy_setup
    try:
        # Create an entry order that is 70 seconds old (exceeding entry_ttl_sec = 60s)
        order = state.create_entry_order(
            symbol="BTCUSDT",
            position_side="LONG",
            quantity=Decimal("0.1"),
            price=Decimal("60000"),
            batch_id="b_btc_01",
        )
        order.created_at = datetime.now(UTC) - timedelta(seconds=70)
        assert not order.is_terminal

        # Run cancel_stale_entry_orders
        await engine.cancel_stale_entry_orders()
        mock_rest.cancel_order.assert_awaited_once_with(
            symbol="BTCUSDT",
            client_order_id=order.client_order_id,
        )
    finally:
        await executor.close()


@pytest.mark.asyncio
async def test_strategy_skip_entry_if_symbol_has_active_batch(strategy_setup):
    state, executor, engine, mock_rest = strategy_setup
    try:
        from crypto_momentum_lab.next_gen.models import PositionBatch

        # Add an active batch for BTCUSDT
        active_batch = PositionBatch(
            environment="live",
            account_label="a1",
            batch_id="b_btc_existing",
            symbol="BTCUSDT",
            position_side="LONG",
            quantity=Decimal("0.5"),
            entry_price=Decimal("60000"),
            entry_bar_time=datetime.now(UTC),
            status="ACTIVE",
        )
        state.batches[active_batch.batch_id] = active_batch

        # Feed strong positive momentum for BTCUSDT
        btc_bars = generate_bars(
            "BTCUSDT", Decimal("50000"), Decimal("55000"), count=15
        )
        engine.feed_bars(btc_bars)

        # BTCUSDT should be skipped because of active batch concurrency guard
        submitted = await engine.evaluate_and_generate_orders()
        assert len(submitted) == 0
        mock_rest.post_order.assert_not_awaited()
    finally:
        await executor.close()


@pytest.mark.asyncio
async def test_strategy_warmup_from_db_without_false_kline_fallback(
    strategy_setup,
):
    _, executor, engine, mock_rest = strategy_setup
    try:
        engine.min_warmup_bars = 4
        mock_db = AsyncMock()
        mock_db.async_get_historical_15s_bars.return_value = {
            "BTCUSDT": [
                {
                    "bucket_end": datetime.now(UTC) - timedelta(seconds=30),
                    "open_price": 50000,
                    "high_price": 50100,
                    "low_price": 49900,
                    "close_price": 50050,
                    "trade_notional": 1000,
                },
                {
                    "bucket_end": datetime.now(UTC) - timedelta(seconds=15),
                    "open_price": 50050,
                    "high_price": 50200,
                    "low_price": 50000,
                    "close_price": 50150,
                    "trade_notional": 1200,
                },
            ],
            "ETHUSDT": [],
        }

        await engine.warmup_market_data(rest_client=mock_rest, db=mock_db)
        mock_db.async_get_historical_15s_bars.assert_awaited_once()
        # Verify fetch_klines was NOT invoked to falsely fabricate 15s bars
        mock_rest.fetch_klines.assert_not_awaited()
        assert len(engine.bar_buffers["BTCUSDT"]) == 2
    finally:
        await executor.close()
