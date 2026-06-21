from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.strategies.liquidation_cascade import (
    LiquidationCascadeConfig,
    LiquidationCascadeDirection,
    find_liquidation_cascades,
    summarize_liquidation_cascades,
)


def test_finds_upward_continuation_after_liquidation_cluster() -> None:
    states = (
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
        _state(6, Decimal("103.00")),
        _state(7, Decimal("101.50")),
    )

    events = find_liquidation_cascades(states, _config())

    assert len(events) == 1
    event = events[0]
    assert event.symbol == "BTCUSDT"
    assert event.direction is LiquidationCascadeDirection.UP
    assert event.detected_at == states[5].bucket_start
    assert event.cluster_start_price == Decimal("100.00")
    assert event.cluster_end_price == Decimal("102.00")
    assert event.cluster_move_pct == Decimal("0.02")
    assert event.breakout_level == Decimal("100.00")
    assert event.breakout_distance_pct == Decimal("0.02")
    assert event.liquidation_count == 2
    assert event.liquidation_notional == Decimal("600")
    assert event.aggressive_imbalance == Decimal(
        "0.6666666666666666666666666667"
    )
    assert event.forward_returns[1] == Decimal(
        "0.009803921568627450980392156863"
    )
    assert event.forward_returns[2] == Decimal(
        "-0.004901960784313725490196078431"
    )
    assert event.max_favorable_return == event.forward_returns[1]
    assert event.max_adverse_return == event.forward_returns[2]


def test_finds_downward_continuation_after_liquidation_cluster() -> None:
    states = (
        _state(0, Decimal("100.00")),
        _state(1, Decimal("100.00")),
        _state(2, Decimal("100.00")),
        _state(3, Decimal("100.00")),
        _state(
            4,
            Decimal("100.00"),
            buy=Decimal("50"),
            sell=Decimal("250"),
            liquidation_count=1,
            liquidation_notional=Decimal("300"),
        ),
        _state(
            5,
            Decimal("98.00"),
            buy=Decimal("50"),
            sell=Decimal("250"),
            liquidation_count=1,
            liquidation_notional=Decimal("300"),
        ),
        _state(6, Decimal("97.00")),
    )

    events = find_liquidation_cascades(states, _config())

    assert len(events) == 1
    event = events[0]
    assert event.direction is LiquidationCascadeDirection.DOWN
    assert event.cluster_move_pct == Decimal("0.02")
    assert event.breakout_level == Decimal("100.00")
    assert event.forward_returns[1] == Decimal(
        "0.01020408163265306122448979592"
    )


def test_rejects_price_move_without_liquidation_activity() -> None:
    states = (
        _state(0, Decimal("100.00")),
        _state(1, Decimal("100.00")),
        _state(2, Decimal("100.00")),
        _state(3, Decimal("100.00")),
        _state(4, Decimal("100.00"), buy=Decimal("250"), sell=Decimal("50")),
        _state(5, Decimal("102.00"), buy=Decimal("250"), sell=Decimal("50")),
    )

    assert find_liquidation_cascades(states, _config()) == ()


def test_rejects_liquidation_cluster_when_aggressive_flow_is_not_aligned() -> None:
    states = (
        _state(0, Decimal("100.00")),
        _state(1, Decimal("100.00")),
        _state(2, Decimal("100.00")),
        _state(3, Decimal("100.00")),
        _state(
            4,
            Decimal("100.00"),
            buy=Decimal("150"),
            sell=Decimal("150"),
            liquidation_count=1,
            liquidation_notional=Decimal("300"),
        ),
        _state(
            5,
            Decimal("102.00"),
            buy=Decimal("150"),
            sell=Decimal("150"),
            liquidation_count=1,
            liquidation_notional=Decimal("300"),
        ),
    )

    assert find_liquidation_cascades(states, _config()) == ()


def test_skips_missing_price_and_insufficient_history() -> None:
    missing_price = (
        _state(0, Decimal("100.00")),
        _state(1, Decimal("100.00")),
        _state(2, Decimal("100.00")),
        _state(3, Decimal("100.00")),
        _state(
            4,
            None,
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
    insufficient_history = missing_price[3:]

    assert find_liquidation_cascades(missing_price, _config()) == ()
    assert find_liquidation_cascades(insufficient_history, _config()) == ()


def test_requires_confirmation_and_applies_cooldown() -> None:
    config = LiquidationCascadeConfig(
        liquidation_window_buckets=2,
        breakout_window_buckets=4,
        min_liquidation_count=1,
        min_liquidation_notional=Decimal("500"),
        min_price_move_pct=Decimal("0.01"),
        min_aggressive_imbalance=Decimal("0.50"),
        confirmation_buckets=2,
        cooldown_buckets=4,
        forward_horizon_buckets=(1,),
    )
    states = (
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
        _state(6, Decimal("102.50"), buy=Decimal("250"), sell=Decimal("50")),
        _state(7, Decimal("103.00"), buy=Decimal("250"), sell=Decimal("50")),
        _state(8, Decimal("103.50"), buy=Decimal("250"), sell=Decimal("50")),
    )

    events = find_liquidation_cascades(states, config)

    assert len(events) == 1
    assert events[0].detected_at == states[6].bucket_start


def test_summarizes_events_by_direction() -> None:
    states = (
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
        _state(6, Decimal("103.00")),
    )
    events = find_liquidation_cascades(states, _config())

    summary = summarize_liquidation_cascades(events, horizons=(1, 2))

    assert summary.total_count == 1
    assert summary.by_direction[LiquidationCascadeDirection.UP].count == 1
    assert summary.by_direction[
        LiquidationCascadeDirection.UP
    ].mean_forward_returns[1] == Decimal(
        "0.009803921568627450980392156863"
    )
    assert summary.by_direction[LiquidationCascadeDirection.DOWN].count == 0


def _config() -> LiquidationCascadeConfig:
    return LiquidationCascadeConfig(
        liquidation_window_buckets=2,
        breakout_window_buckets=4,
        min_liquidation_count=1,
        min_liquidation_notional=Decimal("500"),
        min_price_move_pct=Decimal("0.01"),
        min_aggressive_imbalance=Decimal("0.50"),
        confirmation_buckets=1,
        cooldown_buckets=2,
        forward_horizon_buckets=(1, 2),
    )


def _state(
    bucket_index: int,
    close_price: Decimal | None,
    *,
    buy: Decimal = Decimal("150"),
    sell: Decimal = Decimal("150"),
    liquidation_count: int = 0,
    liquidation_notional: Decimal = Decimal("0"),
) -> MarketState15s:
    bucket_start = datetime(2026, 6, 20, 0, 0, tzinfo=UTC) + timedelta(
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
        trade_count=10 if close_price is not None else 0,
        trade_notional=buy + sell,
        aggressive_buy_notional=buy,
        aggressive_sell_notional=sell,
        last_bid_price=None if close_price is None else close_price - Decimal("0.01"),
        last_ask_price=None if close_price is None else close_price + Decimal("0.01"),
        spread=None if close_price is None else Decimal("0.02"),
        midpoint=close_price,
        liquidation_count=liquidation_count,
        liquidation_notional=liquidation_notional,
        mark_price=close_price,
        closed_kline_count=0,
        source_event_count=10,
        first_received_at=bucket_start,
        last_received_at=bucket_end,
    )
