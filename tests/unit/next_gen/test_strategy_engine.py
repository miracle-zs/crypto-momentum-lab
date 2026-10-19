import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.next_gen.engine import AccountState
from crypto_momentum_lab.next_gen.executor import OrderExecutor
from crypto_momentum_lab.next_gen.models import OrderStatus, PositionBatch, SystemMode
from crypto_momentum_lab.next_gen.persistence import InMemoryTradingRepository
from crypto_momentum_lab.next_gen.strategy_engine import MomentumStrategyEngine
from crypto_momentum_lab.next_gen.symbol_rules import SymbolRulesRegistry
from tests.unit.next_gen.factories import risk_config
from tests.unit.strategies.order_flow_impulse.test_order_flow_impulse_runtime import (
    _impulse_states,
)


def exchange_info():
    return {
        "symbols": [
            {
                "symbol": symbol,
                "status": "TRADING",
                "quoteAsset": "USDT",
                "contractType": "PERPETUAL",
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                    {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                    {"filterType": "MIN_NOTIONAL", "notional": "5"},
                ],
            }
            for symbol in ["BTCUSDT", "ETHUSDT"]
        ]
    }


@pytest.fixture
async def strategy_setup(account_config):
    profile = replace(
        account_config.strategy_inputs.profile,
        impulse_window_buckets=3,
        min_return_pct=Decimal("0.01"),
        min_aggressive_imbalance=Decimal("0.5"),
        min_notional_intensity=Decimal("2"),
        min_notional_5m_vs_30m=Decimal("0"),
    )
    config = replace(
        account_config,
        strategy_inputs=replace(
            account_config.strategy_inputs,
            profile=profile,
        ),
    )
    repository = InMemoryTradingRepository()
    repository.entry_symbols = frozenset({"BTCUSDT"})
    rest = AsyncMock()
    rest.fetch_exchange_info.return_value = exchange_info()
    state = AccountState("live", "a1", repository, rest)
    state.risk_config = risk_config(state.environment, state.account_label)
    state.set_system_mode(SystemMode.RUNNING)
    state.set_ws_connected(True)
    executor = OrderExecutor(rest, state, repository)
    strategy = MomentumStrategyEngine(
        state,
        executor,
        config,
        market_environment="research",
        symbols=["BTCUSDT"],
        symbol_rules=SymbolRulesRegistry(),
    )
    end = datetime.now(UTC)
    original = _impulse_states()
    shift = end - original[-1].bucket_end
    states = tuple(
        replace(
            row,
            bucket_start=row.bucket_start + shift,
            bucket_end=row.bucket_end + shift,
        )
        for row in original
    )
    repository.market_states = list(states[:-1])
    await strategy.warmup_market_data(rest, repository)
    strategy.market_connected = True
    try:
        yield state, repository, rest, executor, strategy, states[-1]
    finally:
        await executor.close()


@pytest.mark.asyncio
async def test_original_order_flow_signal_submits_configured_limit(strategy_setup):
    state, repository, rest, _, strategy, last = strategy_setup
    assert strategy.market_data_observation is None
    result = await strategy.on_market_state(last)
    sample = strategy.market_data_observation
    assert sample["bucket_end"] == last.bucket_end.isoformat()
    assert sample["delay_ms"] == max(
        0.0,
        (
            datetime.fromisoformat(sample["received_at"]) - last.bucket_end
        ).total_seconds()
        * 1000,
    )
    assert len(result) == 1
    order = state.orders[result[0]]
    assert order.order_type == "LIMIT"
    assert order.price == Decimal("102")
    assert order.quantity == Decimal("0.980")
    assert strategy.account_config.strategy_inputs.entry_limit_ttl_seconds == 900
    rest.post_order.assert_awaited_once()
    assert repository.entry_signals
    assert repository.entry_signals[0]["symbol"] == "BTCUSDT"
    assert repository.entry_signals[0]["features"]
    assert await strategy.on_market_state(last) == []


@pytest.mark.asyncio
async def test_signal_observation_failure_does_not_change_submitted_order(
    strategy_setup,
):
    state, repository, rest, _, strategy, last = strategy_setup
    repository.async_save_entry_signal = AsyncMock(
        side_effect=RuntimeError("write failed")
    )
    result = await strategy.on_market_state(last)
    assert len(result) == 1
    rest.post_order.assert_awaited_once()
    assert state.orders[result[0]].status is OrderStatus.SUBMITTED


@pytest.mark.asyncio
@pytest.mark.parametrize("exit_started", [False, True])
async def test_entry_batch_merges_until_first_exit(strategy_setup, exit_started):
    state, _, _, _, strategy, last = strategy_setup
    batch = PositionBatch(
        "live",
        "a1",
        "existing",
        "BTCUSDT",
        "LONG",
        Decimal("1"),
        Decimal("100"),
        last.bucket_end - timedelta(minutes=5),
        exit_order_submitted_at=datetime.now(UTC) if exit_started else None,
    )
    state.batches[batch.batch_id] = batch
    result = await strategy.on_market_state(last)
    assert len(result) == 1
    assert (state.orders[result[0]].batch_id == batch.batch_id) is not exit_started


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["stale", "disconnected", "outside_universe"])
async def test_signal_cannot_bypass_admission(strategy_setup, reason):
    _, repository, rest, _, strategy, last = strategy_setup
    if reason == "stale":
        # The strategy can process history, but may not trade its old signal.
        shift = timedelta(days=1)
        strategy.runtime.clear_market_state_buffers()
        strategy._last_state_at.clear()
        history = tuple(
            replace(
                row,
                bucket_start=row.bucket_start - shift,
                bucket_end=row.bucket_end - shift,
            )
            for row in repository.market_states
        )
        for row in history:
            strategy.runtime.warm_market_state(row)
        last = replace(
            last,
            bucket_start=last.bucket_start - shift,
            bucket_end=last.bucket_end - shift,
        )
    elif reason == "disconnected":
        strategy.market_connected = False
    else:
        repository.entry_symbols = frozenset()
    assert await strategy.on_market_state(last) == []
    rest.post_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_entry_ttl_uses_account_configuration(strategy_setup):
    state, _, rest, _, strategy, _ = strategy_setup
    order = state.create_entry_order(
        "BTCUSDT", "LONG", Decimal("1"), Decimal("100"), "ttl"
    )
    order.created_at = datetime.now(UTC) - timedelta(seconds=70)
    await strategy.cancel_stale_entry_orders()
    rest.cancel_order.assert_not_awaited()
    order.created_at = datetime.now(UTC) - timedelta(seconds=901)

    async def cancelled(**kwargs):
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

    rest.cancel_order.side_effect = cancelled
    await strategy.cancel_stale_entry_orders()
    rest.cancel_order.assert_awaited_once()
    rest.query_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_strategy_task_stops(strategy_setup):
    *_, strategy, _ = strategy_setup
    stopped = asyncio.Event()
    stopped.set()
    await strategy.run(stopped)


@pytest.mark.asyncio
async def test_active_halt_is_checked_before_submitting_signal(strategy_setup):
    state, repository, rest, _, strategy, last = strategy_setup
    repository.halted_accounts.add((state.environment, state.account_label))
    assert await strategy.on_market_state(last) == []
    assert state.entry_halted
    rest.post_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_exchange_entry_rejection_is_recorded_and_cools_down(strategy_setup):
    state, repository, rest, executor, strategy, last = strategy_setup
    from crypto_momentum_lab.next_gen.client import BinanceOrderRejectedError

    rest.post_order.side_effect = BinanceOrderRejectedError(
        code=-2010, message="Margin is insufficient."
    )

    assert await strategy.on_market_state(last) == []
    assert rest.post_order.await_count == 1
    assert not state.active_entry_orders
    signal = repository.entry_signals[-1]
    candidate_results = signal["filter_context"]["candidate_filter_results"]
    assert candidate_results
    reason = next(iter(candidate_results.values()))["rejection_reason"]
    assert "exchange_reject:-2010" in reason
    assert ("BTCUSDT", "LONG") in strategy._rejected_entry_until
