from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.strategies.compression_breakout import (
    BreakoutDirection,
    CompressionBreakoutConfig,
    find_compression_breakouts,
    summarize_compression_breakouts,
)


def test_finds_upward_breakout_after_compressed_range() -> None:
    states = (
        _state(0, close_price=Decimal("100.00")),
        _state(1, close_price=Decimal("100.10")),
        _state(2, close_price=Decimal("99.95")),
        _state(3, close_price=Decimal("100.20")),
        _state(
            4,
            close_price=Decimal("100.50"),
            aggressive_buy_notional=Decimal("500"),
            aggressive_sell_notional=Decimal("100"),
        ),
        _state(5, close_price=Decimal("101.00")),
        _state(6, close_price=Decimal("100.25")),
    )

    events = find_compression_breakouts(states, _config())

    assert len(events) == 1
    event = events[0]
    assert event.symbol == "BTCUSDT"
    assert event.direction is BreakoutDirection.UP
    assert event.detected_at == states[4].bucket_start
    assert event.range_high == Decimal("100.20")
    assert event.range_low == Decimal("99.95")
    assert event.breakout_price == Decimal("100.50")
    assert event.aggressive_imbalance == Decimal("0.6666666666666666666666666667")
    assert event.forward_returns[1] == Decimal(
        "0.004975124378109452736318407960"
    )
    assert event.forward_returns[2] == Decimal(
        "-0.002487562189054726368159203980"
    )
    assert event.max_favorable_return == event.forward_returns[1]
    assert event.max_adverse_return == event.forward_returns[2]


def test_finds_downward_breakout_after_compressed_range() -> None:
    states = (
        _state(0, close_price=Decimal("100.00")),
        _state(1, close_price=Decimal("100.05")),
        _state(2, close_price=Decimal("99.95")),
        _state(3, close_price=Decimal("100.10")),
        _state(
            4,
            close_price=Decimal("99.70"),
            aggressive_buy_notional=Decimal("100"),
            aggressive_sell_notional=Decimal("500"),
        ),
        _state(5, close_price=Decimal("99.10")),
    )

    events = find_compression_breakouts(states, _config())

    assert len(events) == 1
    event = events[0]
    assert event.direction is BreakoutDirection.DOWN
    assert event.range_low == Decimal("99.95")
    assert event.breakout_price == Decimal("99.70")
    assert event.forward_returns[1] == Decimal(
        "0.006018054162487462387161484453"
    )


def test_rejects_breakout_when_prior_range_is_not_compressed() -> None:
    states = (
        _state(0, close_price=Decimal("100.00")),
        _state(1, close_price=Decimal("103.00")),
        _state(2, close_price=Decimal("98.00")),
        _state(3, close_price=Decimal("101.00")),
        _state(4, close_price=Decimal("104.00")),
    )

    assert find_compression_breakouts(states, _config()) == ()


def test_skips_windows_with_missing_prices() -> None:
    states = (
        _state(0, close_price=Decimal("100.00")),
        _state(1, close_price=None),
        _state(2, close_price=Decimal("99.95")),
        _state(3, close_price=Decimal("100.10")),
        _state(4, close_price=Decimal("100.50")),
    )

    assert find_compression_breakouts(states, _config()) == ()


def test_requires_acceptance_and_applies_cooldown() -> None:
    config = CompressionBreakoutConfig(
        compression_window_buckets=4,
        max_range_width_pct=Decimal("0.01"),
        min_breakout_pct=Decimal("0.001"),
        acceptance_buckets=2,
        cooldown_buckets=3,
        forward_horizon_buckets=(1,),
    )
    states = (
        _state(0, close_price=Decimal("100.00")),
        _state(1, close_price=Decimal("100.05")),
        _state(2, close_price=Decimal("99.95")),
        _state(3, close_price=Decimal("100.10")),
        _state(4, close_price=Decimal("100.30")),
        _state(5, close_price=Decimal("100.40")),
        _state(6, close_price=Decimal("100.55")),
        _state(7, close_price=Decimal("100.60")),
    )

    events = find_compression_breakouts(states, config)

    assert len(events) == 1
    assert events[0].detected_at == states[5].bucket_start


def test_summarizes_events_by_direction() -> None:
    states = (
        _state(0, close_price=Decimal("100.00")),
        _state(1, close_price=Decimal("100.10")),
        _state(2, close_price=Decimal("99.95")),
        _state(3, close_price=Decimal("100.20")),
        _state(4, close_price=Decimal("100.50")),
        _state(5, close_price=Decimal("101.00")),
        _state(6, close_price=Decimal("100.25")),
    )
    events = find_compression_breakouts(states, _config())

    summary = summarize_compression_breakouts(events, horizons=(1, 2))

    assert summary.total_count == 1
    assert summary.by_direction[BreakoutDirection.UP].count == 1
    assert summary.by_direction[BreakoutDirection.UP].mean_forward_returns[1] == (
        Decimal("0.004975124378109452736318407960")
    )
    assert summary.by_direction[BreakoutDirection.DOWN].count == 0


def _config() -> CompressionBreakoutConfig:
    return CompressionBreakoutConfig(
        compression_window_buckets=4,
        max_range_width_pct=Decimal("0.01"),
        min_breakout_pct=Decimal("0.001"),
        acceptance_buckets=1,
        cooldown_buckets=2,
        forward_horizon_buckets=(1, 2),
    )


def _state(
    bucket_index: int,
    *,
    close_price: Decimal | None,
    aggressive_buy_notional: Decimal = Decimal("200"),
    aggressive_sell_notional: Decimal = Decimal("100"),
) -> MarketState15s:
    bucket_start = datetime(2026, 6, 19, 0, 0, tzinfo=UTC) + timedelta(
        seconds=15 * bucket_index
    )
    bucket_end = bucket_start + timedelta(seconds=15)
    return MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        symbol="BTCUSDT",
        bucket_start=bucket_start,
        bucket_end=bucket_end,
        open_price=close_price,
        high_price=close_price,
        low_price=close_price,
        close_price=close_price,
        trade_count=3 if close_price is not None else 0,
        trade_notional=Decimal("300") if close_price is not None else Decimal("0"),
        aggressive_buy_notional=aggressive_buy_notional,
        aggressive_sell_notional=aggressive_sell_notional,
        last_bid_price=None if close_price is None else close_price - Decimal("0.01"),
        last_ask_price=None if close_price is None else close_price + Decimal("0.01"),
        spread=None if close_price is None else Decimal("0.02"),
        midpoint=close_price,
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=close_price,
        closed_kline_count=0,
        source_event_count=3,
        first_received_at=bucket_start,
        last_received_at=bucket_end,
    )
