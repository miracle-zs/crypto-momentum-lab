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
from crypto_momentum_lab.strategies.compression_breakout import (
    CompressionBreakoutConfig,
    CompressionBreakoutRuntimeConfig,
    CompressionBreakoutRuntimeStrategy,
)


def test_runtime_emits_upward_signal_after_acceptance_window() -> None:
    strategy = _strategy()
    states = (
        _state(0, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(1, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(2, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(3, close=Decimal("101.0")),
        _state(4, close=Decimal("101.2")),
    )

    decisions = tuple(strategy.on_market_state(state) for state in states)
    decision = decisions[-1]

    assert len(decision.signals) == 1
    assert len(decision.candidates) == 1
    signal = decision.signals[0]
    candidate = decision.candidates[0]
    assert signal.symbol == "BTCUSDT"
    assert signal.side is StrategySide.LONG
    assert signal.detected_at == states[-1].bucket_start
    assert signal.source_state_at == states[-1].bucket_start
    assert signal.features["direction"] == "up"
    assert signal.features["range_high"] == "100.1"
    assert signal.features["range_low"] == "99.9"
    assert signal.features["breakout_price"] == "101.2"
    assert signal.features["trade_count"] == 10
    assert signal.features["trade_notional"] == "1000"
    assert signal.features["aggressive_buy_notional"] == "600"
    assert signal.features["aggressive_sell_notional"] == "400"
    assert signal.features["liquidation_count"] == 0
    assert signal.features["liquidation_notional"] == "0"
    assert candidate.signal_id == signal.signal_id
    assert candidate.side is StrategySide.LONG
    assert candidate.desired_notional == Decimal("100")
    assert candidate.created_at == states[-1].bucket_start
    assert candidate.expires_at == states[-1].bucket_start + timedelta(seconds=30)


def test_runtime_emits_downward_signal_after_acceptance_window() -> None:
    strategy = _strategy()
    states = (
        _state(0, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(1, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(2, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(3, close=Decimal("98.8")),
        _state(4, close=Decimal("98.6")),
    )

    decision = _last_decision(strategy, states)

    assert len(decision.signals) == 1
    assert decision.signals[0].side is StrategySide.SHORT
    assert decision.signals[0].features["direction"] == "down"
    assert decision.signals[0].features["breakout_price"] == "98.6"


def test_runtime_records_warmup_and_missing_price_rejections() -> None:
    strategy = _strategy()

    first = strategy.on_market_state(_state(0, close=Decimal("100")))
    missing = strategy.on_market_state(_state(1, close=None))

    assert first.rejections[0].reason is RejectionReason.INSUFFICIENT_WARMUP
    assert missing.rejections[0].reason is RejectionReason.MISSING_REQUIRED_PRICE


def test_runtime_applies_cooldown_after_signal() -> None:
    strategy = _strategy(cooldown_buckets=2)
    first_breakout = (
        _state(0, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(1, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(2, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(3, close=Decimal("101.0")),
        _state(4, close=Decimal("101.2")),
    )
    _last_decision(strategy, first_breakout)

    cooldown_decision = strategy.on_market_state(_state(5, close=Decimal("102.0")))

    assert cooldown_decision.signals == ()
    assert cooldown_decision.candidates == ()
    assert cooldown_decision.rejections[0].reason is RejectionReason.COOLDOWN_ACTIVE


def test_runtime_checkpoint_contains_symbol_state() -> None:
    strategy = _strategy()
    _last_decision(
        strategy,
        (
            _state(0, close=Decimal("100")),
            _state(1, close=Decimal("100")),
            _state(2, close=Decimal("100")),
        ),
    )

    checkpoint = strategy.checkpoint()

    assert (
        checkpoint.last_processed_at_by_symbol["BTCUSDT"]
        == _state(
            2,
            close=Decimal("100"),
        ).bucket_start
    )
    assert checkpoint.warmup_buckets_by_symbol["BTCUSDT"] == 3
    assert checkpoint.payload["buffer_sizes"] == {"BTCUSDT": 3}


def test_runtime_restore_checkpoint_alias_restores_cooldown() -> None:
    strategy = _strategy()
    checkpoint = StrategyCheckpoint(
        last_processed_at_by_symbol={
            "BTCUSDT": datetime(2026, 6, 22, 0, 1, tzinfo=UTC)
        },
        warmup_buckets_by_symbol={"BTCUSDT": 3},
        cooldown_buckets_remaining_by_symbol={"BTCUSDT": 2},
        payload={},
    )

    strategy.restore_checkpoint(checkpoint)
    restored = strategy.checkpoint()

    assert restored.last_processed_at_by_symbol == (
        checkpoint.last_processed_at_by_symbol
    )
    assert restored.cooldown_buckets_remaining_by_symbol == {"BTCUSDT": 2}


def test_runtime_does_not_need_future_rows_for_detection() -> None:
    base_states = (
        _state(0, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(1, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(2, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(3, close=Decimal("101.0")),
        _state(4, close=Decimal("101.2")),
    )
    with_future = base_states + (_state(5, close=Decimal("120")),)

    base_signal = _first_signal(_strategy(), base_states)
    future_signal = _first_signal(_strategy(), with_future)

    assert base_signal.signal_id == future_signal.signal_id
    assert base_signal.features == future_signal.features


def test_runtime_aggregates_closed_five_minute_signal_buckets() -> None:
    strategy = _strategy(
        compression_window_buckets=2,
        acceptance_buckets=1,
        signal_interval_seconds=300,
    )
    states = tuple(
        _state(
            index,
            close=Decimal("100") if index < 40 else Decimal("101"),
            high=Decimal("100.1") if index < 40 else Decimal("101"),
            low=Decimal("99.9") if index < 40 else Decimal("101"),
        )
        for index in range(60)
    )

    decisions = tuple(strategy.on_market_state(state) for state in states)

    assert all(decision.signals == () for decision in decisions[:-1])
    signal = decisions[-1].signals[0]
    candidate = decisions[-1].candidates[0]
    assert signal.detected_at == datetime(2026, 6, 22, 0, 15, tzinfo=UTC)
    assert signal.source_state_at == datetime(2026, 6, 22, 0, 10, tzinfo=UTC)
    assert signal.features["range_start"] == "2026-06-22T00:00:00+00:00"
    assert signal.features["range_end"] == "2026-06-22T00:10:00+00:00"
    assert candidate.created_at == signal.detected_at
    assert candidate.expires_at == signal.detected_at + timedelta(seconds=30)
    assert strategy.required_data().warmup_buckets == 60


def test_runtime_restores_completed_signal_buckets_from_checkpoint() -> None:
    original = _strategy(
        compression_window_buckets=2,
        acceptance_buckets=1,
        signal_interval_seconds=300,
    )
    compression_states = tuple(
        _state(
            index,
            close=Decimal("100"),
            high=Decimal("100.1"),
            low=Decimal("99.9"),
        )
        for index in range(40)
    )
    _last_decision(original, compression_states)
    checkpoint = original.checkpoint()

    restored = _strategy(
        compression_window_buckets=2,
        acceptance_buckets=1,
        signal_interval_seconds=300,
    )
    restored.restore_checkpoint(checkpoint)
    breakout_states = tuple(
        _state(
            index,
            close=Decimal("101"),
            high=Decimal("101"),
            low=Decimal("101"),
        )
        for index in range(40, 60)
    )

    decision = _last_decision(restored, breakout_states)

    assert len(decision.signals) == 1
    assert decision.signals[0].side is StrategySide.LONG
    assert restored.checkpoint().payload["buffer_sizes"] == {"BTCUSDT": 3}


def _strategy(
    *,
    cooldown_buckets: int = 3,
    compression_window_buckets: int = 3,
    acceptance_buckets: int = 2,
    signal_interval_seconds: int = 15,
) -> CompressionBreakoutRuntimeStrategy:
    event_config = CompressionBreakoutConfig(
        compression_window_buckets=compression_window_buckets,
        max_range_width_pct=Decimal("0.01"),
        min_breakout_pct=Decimal("0.001"),
        acceptance_buckets=acceptance_buckets,
        cooldown_buckets=cooldown_buckets,
        forward_horizon_buckets=(1,),
    )
    runtime_config = CompressionBreakoutRuntimeConfig(
        event_config=event_config,
        candidate_notional=Decimal("100"),
        candidate_ttl_buckets=2,
        signal_interval_seconds=signal_interval_seconds,
    )
    identity = StrategyRunIdentity(
        run_id="run-1",
        strategy_name="compression_breakout",
        strategy_version="v0",
        config_hash=deterministic_config_hash(runtime_config),
        run_mode=RunMode.REPLAY,
        code_commit="unknown",
        created_at=datetime(2026, 6, 22, 0, 0, tzinfo=UTC),
        source_paths=("memory",),
    )
    return CompressionBreakoutRuntimeStrategy(
        config=runtime_config,
        identity=identity,
    )


def _last_decision(
    strategy: CompressionBreakoutRuntimeStrategy,
    states: tuple[MarketState15s, ...],
):
    decision = None
    for state in states:
        decision = strategy.on_market_state(state)
    assert decision is not None
    return decision


def _first_signal(
    strategy: CompressionBreakoutRuntimeStrategy,
    states: tuple[MarketState15s, ...],
):
    for state in states:
        decision = strategy.on_market_state(state)
        if decision.signals:
            return decision.signals[0]
    raise AssertionError("expected a signal")


def _state(
    bucket_index: int,
    *,
    close: Decimal | None,
    high: Decimal | None = None,
    low: Decimal | None = None,
) -> MarketState15s:
    bucket_start = datetime(2026, 6, 22, 0, 0, tzinfo=UTC) + timedelta(
        seconds=15 * bucket_index
    )
    bucket_end = bucket_start + timedelta(seconds=15)
    bid = close - Decimal("0.01") if close is not None else None
    ask = close + Decimal("0.01") if close is not None else None
    return MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        symbol="BTCUSDT",
        bucket_start=bucket_start,
        bucket_end=bucket_end,
        open_price=close,
        high_price=high if high is not None else close,
        low_price=low if low is not None else close,
        close_price=close,
        trade_count=10,
        trade_notional=Decimal("1000"),
        aggressive_buy_notional=Decimal("600"),
        aggressive_sell_notional=Decimal("400"),
        last_bid_price=bid,
        last_ask_price=ask,
        spread=Decimal("0.02") if close is not None else None,
        midpoint=close,
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=close,
        closed_kline_count=0,
        source_event_count=10,
        first_received_at=bucket_start,
        last_received_at=bucket_end,
    )
