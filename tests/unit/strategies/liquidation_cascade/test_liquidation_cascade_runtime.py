from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import (
    RunMode,
    StrategyCheckpoint,
    StrategyRunIdentity,
    StrategySide,
    deterministic_config_hash,
)
from crypto_momentum_lab.strategies.liquidation_cascade import (
    LiquidationCascadeConfig,
    LiquidationCascadeRuntimeConfig,
    LiquidationCascadeRuntimeStrategy,
)


def test_liquidation_cascade_emits_signal_after_liquidation_and_break() -> None:
    strategy = _strategy()

    decision = _last_decision(strategy, _cascade_states())

    assert len(decision.signals) == 1
    assert len(decision.candidates) == 1
    signal = decision.signals[0]
    assert signal.strategy_name == "liquidation_cascade"
    assert signal.side is StrategySide.LONG
    assert signal.reason == "liquidation_cascade"
    assert signal.features["direction"] == "up"
    assert signal.features["liquidation_count"] == 2
    assert signal.features["liquidation_notional"] == "600"
    assert signal.features["cluster_trade_count"] == 20
    assert signal.features["cluster_trade_notional"] == "600"
    assert signal.features["aggressive_buy_notional"] == "500"
    assert signal.features["aggressive_sell_notional"] == "100"


def test_liquidation_cascade_ignores_states_without_liquidation() -> None:
    strategy = _strategy()
    states = tuple(_state(index, Decimal("100") + index) for index in range(6))

    decision = _last_decision(strategy, states)

    assert decision.signals == ()
    assert decision.candidates == ()


def test_liquidation_cascade_restores_checkpoint() -> None:
    strategy = _strategy()
    checkpoint = StrategyCheckpoint(
        last_processed_at_by_symbol={
            "BTCUSDT": datetime(2026, 7, 4, 0, 1, tzinfo=UTC)
        },
        warmup_buckets_by_symbol={"BTCUSDT": 6},
        cooldown_buckets_remaining_by_symbol={"BTCUSDT": 2},
        payload={},
    )

    strategy.restore_checkpoint(checkpoint)

    assert strategy.checkpoint().last_processed_at_by_symbol == (
        checkpoint.last_processed_at_by_symbol
    )


def test_liquidation_cascade_restores_market_buffer_from_checkpoint() -> None:
    original = _strategy()
    _last_decision(original, _cascade_states())
    checkpoint = original.checkpoint()

    restored = _strategy()
    restored.restore_checkpoint(checkpoint)

    assert restored.checkpoint().payload["buffer_sizes"] == {"BTCUSDT": 6}


def _strategy() -> LiquidationCascadeRuntimeStrategy:
    config = LiquidationCascadeRuntimeConfig(
        event_config=LiquidationCascadeConfig(
            liquidation_window_buckets=2,
            breakout_window_buckets=4,
            min_liquidation_count=1,
            min_liquidation_notional=Decimal("500"),
            min_price_move_pct=Decimal("0.01"),
            min_aggressive_imbalance=Decimal("0.50"),
            confirmation_buckets=1,
            cooldown_buckets=2,
            forward_horizon_buckets=(1,),
        ),
        candidate_notional=Decimal("100"),
        candidate_ttl_buckets=2,
    )
    identity = StrategyRunIdentity(
        run_id="run-1",
        strategy_name="liquidation_cascade",
        strategy_version="v0",
        config_hash=deterministic_config_hash(config),
        run_mode=RunMode.PAPER,
        code_commit="unknown",
        created_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        source_paths=("memory",),
    )
    return LiquidationCascadeRuntimeStrategy(config=config, identity=identity)


def _last_decision(
    strategy: LiquidationCascadeRuntimeStrategy,
    states: tuple[MarketState15s, ...],
):
    decision = None
    for state in states:
        decision = strategy.on_market_state(state)
    assert decision is not None
    return decision


def _cascade_states() -> tuple[MarketState15s, ...]:
    return (
        _state(0, Decimal("100.00")),
        _state(1, Decimal("100.00")),
        _state(2, Decimal("100.00")),
        _state(3, Decimal("100.00")),
        _state(
            4,
            Decimal("100.00"),
            buy=Decimal("250"),
            sell=Decimal("50"),
            liquidation_count=1,
            liquidation_notional=Decimal("300"),
        ),
        _state(
            5,
            Decimal("102.00"),
            buy=Decimal("250"),
            sell=Decimal("50"),
            liquidation_count=1,
            liquidation_notional=Decimal("300"),
        ),
    )


def _state(
    bucket_index: int,
    close_price: Decimal,
    *,
    buy: Decimal = Decimal("150"),
    sell: Decimal = Decimal("150"),
    liquidation_count: int = 0,
    liquidation_notional: Decimal = Decimal("0"),
) -> MarketState15s:
    bucket_start = datetime(2026, 7, 4, 0, 0, tzinfo=UTC) + timedelta(
        seconds=15 * bucket_index
    )
    return MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        symbol="BTCUSDT",
        bucket_start=bucket_start,
        bucket_end=bucket_start + timedelta(seconds=15),
        open_price=close_price,
        high_price=close_price,
        low_price=close_price,
        close_price=close_price,
        trade_count=10,
        trade_notional=buy + sell,
        aggressive_buy_notional=buy,
        aggressive_sell_notional=sell,
        last_bid_price=close_price - Decimal("0.01"),
        last_ask_price=close_price + Decimal("0.01"),
        spread=Decimal("0.02"),
        midpoint=close_price,
        liquidation_count=liquidation_count,
        liquidation_notional=liquidation_notional,
        mark_price=close_price,
        closed_kline_count=0,
        source_event_count=10,
        first_received_at=bucket_start,
        last_received_at=bucket_start + timedelta(seconds=15),
    )
