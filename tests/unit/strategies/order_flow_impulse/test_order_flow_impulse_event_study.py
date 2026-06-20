from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.strategies.order_flow_impulse import (
    OrderFlowDirection,
    OrderFlowImpulseConfig,
    find_order_flow_impulses,
    summarize_order_flow_impulses,
)


def test_finds_upward_impulse_with_aligned_aggression_and_intensity() -> None:
    states = (
        _state(0, Decimal("100.00"), notional=Decimal("100")),
        _state(1, Decimal("100.00"), notional=Decimal("100")),
        _state(2, Decimal("100.00"), notional=Decimal("100")),
        _state(3, Decimal("100.00"), notional=Decimal("100")),
        _state(4, Decimal("100.00"), notional=Decimal("300"), buy=Decimal("250")),
        _state(5, Decimal("101.00"), notional=Decimal("300"), buy=Decimal("250")),
        _state(6, Decimal("102.00"), notional=Decimal("300"), buy=Decimal("250")),
        _state(7, Decimal("103.00"), notional=Decimal("200"), buy=Decimal("150")),
        _state(8, Decimal("101.50"), notional=Decimal("200"), buy=Decimal("80")),
    )

    events = find_order_flow_impulses(states, _config())

    assert len(events) == 1
    event = events[0]
    assert event.symbol == "BTCUSDT"
    assert event.direction is OrderFlowDirection.UP
    assert event.detected_at == states[6].bucket_start
    assert event.impulse_start_price == Decimal("100.00")
    assert event.impulse_end_price == Decimal("102.00")
    assert event.impulse_return_pct == Decimal("0.02")
    assert event.breakout_level == Decimal("101.00")
    assert event.breakout_distance_pct == Decimal(
        "0.009900990099009900990099009901"
    )
    assert event.aggressive_imbalance == Decimal(
        "0.6666666666666666666666666667"
    )
    assert event.notional_intensity == Decimal("3")
    assert event.forward_returns[1] == Decimal(
        "0.009803921568627450980392156863"
    )
    assert event.forward_returns[2] == Decimal(
        "-0.004901960784313725490196078431"
    )
    assert event.max_favorable_return == event.forward_returns[1]
    assert event.max_adverse_return == event.forward_returns[2]


def test_finds_downward_impulse_with_aligned_aggression_and_intensity() -> None:
    states = (
        _state(0, Decimal("100.00"), notional=Decimal("100")),
        _state(1, Decimal("100.00"), notional=Decimal("100")),
        _state(2, Decimal("100.00"), notional=Decimal("100")),
        _state(3, Decimal("100.00"), notional=Decimal("100")),
        _state(4, Decimal("100.00"), notional=Decimal("300"), sell=Decimal("250")),
        _state(5, Decimal("99.00"), notional=Decimal("300"), sell=Decimal("250")),
        _state(6, Decimal("98.00"), notional=Decimal("300"), sell=Decimal("250")),
        _state(7, Decimal("97.00"), notional=Decimal("200"), sell=Decimal("150")),
    )

    events = find_order_flow_impulses(states, _config())

    assert len(events) == 1
    event = events[0]
    assert event.direction is OrderFlowDirection.DOWN
    assert event.impulse_return_pct == Decimal("0.02")
    assert event.breakout_level == Decimal("99.00")
    assert event.forward_returns[1] == Decimal(
        "0.01020408163265306122448979592"
    )


def test_rejects_price_impulse_when_aggressive_imbalance_is_weak() -> None:
    states = (
        _state(0, Decimal("100.00"), notional=Decimal("100")),
        _state(1, Decimal("100.00"), notional=Decimal("100")),
        _state(2, Decimal("100.00"), notional=Decimal("100")),
        _state(3, Decimal("100.00"), notional=Decimal("100")),
        _state(4, Decimal("100.00"), notional=Decimal("300"), buy=Decimal("160")),
        _state(5, Decimal("101.00"), notional=Decimal("300"), buy=Decimal("160")),
        _state(6, Decimal("102.00"), notional=Decimal("300"), buy=Decimal("160")),
    )

    assert find_order_flow_impulses(states, _config()) == ()


def test_rejects_imbalance_without_notional_expansion() -> None:
    states = (
        _state(0, Decimal("100.00"), notional=Decimal("200")),
        _state(1, Decimal("100.00"), notional=Decimal("200")),
        _state(2, Decimal("100.00"), notional=Decimal("200")),
        _state(3, Decimal("100.00"), notional=Decimal("200")),
        _state(4, Decimal("100.00"), notional=Decimal("100"), buy=Decimal("90")),
        _state(5, Decimal("101.00"), notional=Decimal("100"), buy=Decimal("90")),
        _state(6, Decimal("102.00"), notional=Decimal("100"), buy=Decimal("90")),
    )

    assert find_order_flow_impulses(states, _config()) == ()


def test_skips_missing_price_and_insufficient_history() -> None:
    missing_price = (
        _state(0, Decimal("100.00"), notional=Decimal("100")),
        _state(1, Decimal("100.00"), notional=Decimal("100")),
        _state(2, Decimal("100.00"), notional=Decimal("100")),
        _state(3, Decimal("100.00"), notional=Decimal("100")),
        _state(4, None, notional=Decimal("300"), buy=Decimal("250")),
        _state(5, Decimal("101.00"), notional=Decimal("300"), buy=Decimal("250")),
        _state(6, Decimal("102.00"), notional=Decimal("300"), buy=Decimal("250")),
    )
    insufficient_history = missing_price[3:]

    assert find_order_flow_impulses(missing_price, _config()) == ()
    assert find_order_flow_impulses(insufficient_history, _config()) == ()


def test_requires_confirmation_and_applies_cooldown() -> None:
    config = OrderFlowImpulseConfig(
        impulse_window_buckets=3,
        baseline_window_buckets=4,
        breakout_window_buckets=4,
        min_return_pct=Decimal("0.01"),
        min_aggressive_imbalance=Decimal("0.50"),
        min_notional_intensity=Decimal("2"),
        confirmation_buckets=2,
        cooldown_buckets=4,
        forward_horizon_buckets=(1,),
    )
    states = (
        _state(0, Decimal("100.00"), notional=Decimal("100")),
        _state(1, Decimal("100.00"), notional=Decimal("100")),
        _state(2, Decimal("100.00"), notional=Decimal("100")),
        _state(3, Decimal("100.00"), notional=Decimal("100")),
        _state(4, Decimal("100.00"), notional=Decimal("300"), buy=Decimal("250")),
        _state(5, Decimal("101.00"), notional=Decimal("300"), buy=Decimal("250")),
        _state(6, Decimal("102.00"), notional=Decimal("300"), buy=Decimal("250")),
        _state(7, Decimal("102.50"), notional=Decimal("300"), buy=Decimal("250")),
        _state(8, Decimal("103.00"), notional=Decimal("300"), buy=Decimal("250")),
        _state(9, Decimal("103.50"), notional=Decimal("300"), buy=Decimal("250")),
    )

    events = find_order_flow_impulses(states, config)

    assert len(events) == 1
    assert events[0].detected_at == states[7].bucket_start


def test_summarizes_events_by_direction() -> None:
    states = (
        _state(0, Decimal("100.00"), notional=Decimal("100")),
        _state(1, Decimal("100.00"), notional=Decimal("100")),
        _state(2, Decimal("100.00"), notional=Decimal("100")),
        _state(3, Decimal("100.00"), notional=Decimal("100")),
        _state(4, Decimal("100.00"), notional=Decimal("300"), buy=Decimal("250")),
        _state(5, Decimal("101.00"), notional=Decimal("300"), buy=Decimal("250")),
        _state(6, Decimal("102.00"), notional=Decimal("300"), buy=Decimal("250")),
        _state(7, Decimal("103.00"), notional=Decimal("200"), buy=Decimal("150")),
    )
    events = find_order_flow_impulses(states, _config())

    summary = summarize_order_flow_impulses(events, horizons=(1, 2))

    assert summary.total_count == 1
    assert summary.by_direction[OrderFlowDirection.UP].count == 1
    assert summary.by_direction[OrderFlowDirection.UP].mean_forward_returns[1] == (
        Decimal("0.009803921568627450980392156863")
    )
    assert summary.by_direction[OrderFlowDirection.DOWN].count == 0


def _config() -> OrderFlowImpulseConfig:
    return OrderFlowImpulseConfig(
        impulse_window_buckets=3,
        baseline_window_buckets=4,
        breakout_window_buckets=4,
        min_return_pct=Decimal("0.01"),
        min_aggressive_imbalance=Decimal("0.50"),
        min_notional_intensity=Decimal("2"),
        confirmation_buckets=1,
        cooldown_buckets=2,
        forward_horizon_buckets=(1, 2),
    )


def _state(
    bucket_index: int,
    close_price: Decimal | None,
    *,
    notional: Decimal,
    buy: Decimal | None = None,
    sell: Decimal | None = None,
) -> MarketState15s:
    bucket_start = datetime(2026, 6, 20, 0, 0, tzinfo=UTC) + timedelta(
        seconds=15 * bucket_index
    )
    bucket_end = bucket_start + timedelta(seconds=15)
    if buy is not None:
        buy_notional = buy
        sell_notional = sell if sell is not None else notional - buy_notional
    elif sell is not None:
        sell_notional = sell
        buy_notional = notional - sell_notional
    else:
        buy_notional = notional / Decimal("2")
        sell_notional = notional - buy_notional
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
        trade_notional=notional,
        aggressive_buy_notional=buy_notional,
        aggressive_sell_notional=sell_notional,
        last_bid_price=None if close_price is None else close_price - Decimal("0.01"),
        last_ask_price=None if close_price is None else close_price + Decimal("0.01"),
        spread=None if close_price is None else Decimal("0.02"),
        midpoint=close_price,
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=close_price,
        closed_kline_count=0,
        source_event_count=10,
        first_received_at=bucket_start,
        last_received_at=bucket_end,
    )
