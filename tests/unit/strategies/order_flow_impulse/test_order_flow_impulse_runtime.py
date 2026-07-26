from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import (
    RejectionReason,
    RunMode,
    StrategyCheckpoint,
    StrategyRunIdentity,
    StrategySide,
    deterministic_config_hash,
)
from crypto_momentum_lab.strategies.order_flow_impulse import (
    OrderFlowImpulseConfig,
    OrderFlowImpulseRuntimeConfig,
    OrderFlowImpulseRuntimeStrategy,
)


def test_orderflow_impulse_emits_long_signal_on_buy_imbalance() -> None:
    strategy = _strategy()

    decision = _last_decision(strategy, _impulse_states())

    assert len(decision.signals) == 1
    assert len(decision.candidates) == 1
    signal = decision.signals[0]
    assert signal.strategy_name == "orderflow_impulse"
    assert signal.side is StrategySide.LONG
    assert signal.reason == "orderflow_impulse"
    assert signal.features["direction"] == "up"
    assert decision.candidates[0].desired_notional == Decimal("100")


def test_orderflow_impulse_accepts_missing_midpoint_when_trade_price_exists() -> None:
    strategy = _strategy()

    decision = strategy.on_market_state(_state(0, Decimal("100"), midpoint=None))

    assert decision.signals == ()
    assert decision.rejections[0].reason is RejectionReason.INSUFFICIENT_WARMUP
    assert "midpoint" not in strategy.required_data().required_fields


def test_orderflow_impulse_restores_checkpoint() -> None:
    strategy = _strategy()
    checkpoint = StrategyCheckpoint(
        last_processed_at_by_symbol={"BTCUSDT": datetime(2026, 7, 4, 0, 1, tzinfo=UTC)},
        warmup_buckets_by_symbol={"BTCUSDT": 7},
        cooldown_buckets_remaining_by_symbol={"BTCUSDT": 2},
        payload={},
    )

    strategy.restore_checkpoint(checkpoint)

    assert strategy.checkpoint().cooldown_buckets_remaining_by_symbol == {"BTCUSDT": 2}


def _strategy() -> OrderFlowImpulseRuntimeStrategy:
    config = OrderFlowImpulseRuntimeConfig(
        event_config=OrderFlowImpulseConfig(
            impulse_window_buckets=3,
            baseline_window_buckets=4,
            breakout_window_buckets=4,
            min_return_pct=Decimal("0.01"),
            min_aggressive_imbalance=Decimal("0.50"),
            min_notional_intensity=Decimal("2"),
            confirmation_buckets=1,
            cooldown_buckets=2,
            forward_horizon_buckets=(1,),
        ),
        candidate_notional=Decimal("100"),
        candidate_ttl_buckets=2,
    )
    identity = StrategyRunIdentity(
        run_id="run-1",
        strategy_name="orderflow_impulse",
        strategy_version="v0",
        config_hash=deterministic_config_hash(config),
        run_mode=RunMode.PAPER,
        code_commit="unknown",
        created_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        source_paths=("memory",),
    )
    return OrderFlowImpulseRuntimeStrategy(config=config, identity=identity)


def _last_decision(
    strategy: OrderFlowImpulseRuntimeStrategy,
    states: tuple[MarketState15s, ...],
):
    decision = None
    for state in states:
        decision = strategy.on_market_state(state)
    assert decision is not None
    return decision


def _impulse_states() -> tuple[MarketState15s, ...]:
    return (
        _state(0, Decimal("100.00"), notional=Decimal("100")),
        _state(1, Decimal("100.00"), notional=Decimal("100")),
        _state(2, Decimal("100.00"), notional=Decimal("100")),
        _state(3, Decimal("100.00"), notional=Decimal("100")),
        _state(4, Decimal("100.00"), notional=Decimal("300"), buy=Decimal("250")),
        _state(5, Decimal("101.00"), notional=Decimal("300"), buy=Decimal("250")),
        _state(6, Decimal("102.00"), notional=Decimal("300"), buy=Decimal("250")),
    )


def _state(
    bucket_index: int,
    close_price: Decimal,
    *,
    notional: Decimal = Decimal("100"),
    buy: Decimal | None = None,
    midpoint: Decimal | None | object = "default",
) -> MarketState15s:
    bucket_start = datetime(2026, 7, 4, 0, 0, tzinfo=UTC) + timedelta(
        seconds=15 * bucket_index
    )
    buy_notional = notional / Decimal("2") if buy is None else buy
    sell_notional = notional - buy_notional
    resolved_midpoint = close_price if midpoint == "default" else midpoint
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
        trade_notional=notional,
        aggressive_buy_notional=buy_notional,
        aggressive_sell_notional=sell_notional,
        last_bid_price=close_price - Decimal("0.01"),
        last_ask_price=close_price + Decimal("0.01"),
        spread=Decimal("0.02"),
        midpoint=resolved_midpoint,
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=close_price,
        closed_kline_count=0,
        source_event_count=10,
        first_received_at=bucket_start,
        last_received_at=bucket_start + timedelta(seconds=15),
    )
