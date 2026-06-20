from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from crypto_momentum_lab.domain.market.models import MarketState15s


class OrderFlowDirection(StrEnum):
    UP = "up"
    DOWN = "down"


@dataclass(frozen=True, slots=True)
class OrderFlowImpulseConfig:
    impulse_window_buckets: int
    baseline_window_buckets: int
    breakout_window_buckets: int
    min_return_pct: Decimal
    min_aggressive_imbalance: Decimal
    min_notional_intensity: Decimal
    confirmation_buckets: int
    cooldown_buckets: int
    forward_horizon_buckets: tuple[int, ...]

    def __post_init__(self) -> None:
        if self.impulse_window_buckets <= 1:
            raise ValueError("impulse_window_buckets must be greater than 1")
        if self.baseline_window_buckets <= 0:
            raise ValueError("baseline_window_buckets must be positive")
        if self.breakout_window_buckets <= 0:
            raise ValueError("breakout_window_buckets must be positive")
        if self.min_return_pct <= 0:
            raise ValueError("min_return_pct must be positive")
        if self.min_aggressive_imbalance < 0:
            raise ValueError("min_aggressive_imbalance must be non-negative")
        if self.min_notional_intensity <= 0:
            raise ValueError("min_notional_intensity must be positive")
        if self.confirmation_buckets <= 0:
            raise ValueError("confirmation_buckets must be positive")
        if self.cooldown_buckets < 0:
            raise ValueError("cooldown_buckets must be non-negative")
        if not self.forward_horizon_buckets:
            raise ValueError("forward_horizon_buckets must not be empty")
        horizons = tuple(sorted(set(self.forward_horizon_buckets)))
        if any(horizon <= 0 for horizon in horizons):
            raise ValueError("forward_horizon_buckets must be positive")
        object.__setattr__(self, "forward_horizon_buckets", horizons)


@dataclass(frozen=True, slots=True)
class OrderFlowImpulseEvent:
    symbol: str
    direction: OrderFlowDirection
    detected_at: datetime
    impulse_start: datetime
    impulse_end: datetime
    impulse_start_price: Decimal
    impulse_end_price: Decimal
    impulse_return_pct: Decimal
    breakout_level: Decimal
    breakout_distance_pct: Decimal
    impulse_trade_count: int
    impulse_trade_notional: Decimal
    aggressive_buy_notional: Decimal
    aggressive_sell_notional: Decimal
    aggressive_imbalance: Decimal
    baseline_notional: Decimal
    notional_intensity: Decimal
    spread: Decimal | None
    midpoint: Decimal | None
    liquidation_count: int
    liquidation_notional: Decimal
    forward_returns: dict[int, Decimal | None]
    max_favorable_return: Decimal | None
    max_adverse_return: Decimal | None


@dataclass(frozen=True, slots=True)
class OrderFlowImpulseDirectionSummary:
    count: int
    mean_forward_returns: dict[int, Decimal | None]


@dataclass(frozen=True, slots=True)
class OrderFlowImpulseSummary:
    total_count: int
    by_direction: dict[OrderFlowDirection, OrderFlowImpulseDirectionSummary]


def find_order_flow_impulses(
    states: Iterable[MarketState15s],
    config: OrderFlowImpulseConfig,
) -> tuple[OrderFlowImpulseEvent, ...]:
    events: list[OrderFlowImpulseEvent] = []
    for symbol_states in _states_by_symbol(states).values():
        events.extend(_find_symbol_events(symbol_states, config))
    return tuple(sorted(events, key=lambda event: (event.symbol, event.detected_at)))


def summarize_order_flow_impulses(
    events: Iterable[OrderFlowImpulseEvent],
    *,
    horizons: Iterable[int],
) -> OrderFlowImpulseSummary:
    event_tuple = tuple(events)
    horizon_tuple = tuple(sorted(set(horizons)))
    by_direction = {
        direction: _summarize_direction(
            tuple(event for event in event_tuple if event.direction is direction),
            horizon_tuple,
        )
        for direction in OrderFlowDirection
    }
    return OrderFlowImpulseSummary(
        total_count=len(event_tuple),
        by_direction=by_direction,
    )


def _find_symbol_events(
    states: tuple[MarketState15s, ...],
    config: OrderFlowImpulseConfig,
) -> tuple[OrderFlowImpulseEvent, ...]:
    events: list[OrderFlowImpulseEvent] = []
    index = _first_candidate_index(config)
    while index < len(states):
        candidate = _candidate_at(states, index, config)
        if candidate is None:
            index += 1
            continue
        direction, breakout_level = candidate
        detection_index = _confirmed_detection_index(
            states,
            index,
            direction=direction,
            breakout_level=breakout_level,
            config=config,
        )
        if detection_index is None:
            index += 1
            continue
        event = _build_event(
            states,
            index,
            detection_index,
            direction=direction,
            breakout_level=breakout_level,
            config=config,
        )
        if event is not None:
            events.append(event)
            index = detection_index + config.cooldown_buckets + 1
        else:
            index += 1
    return tuple(events)


def _states_by_symbol(
    states: Iterable[MarketState15s],
) -> dict[str, tuple[MarketState15s, ...]]:
    grouped: dict[str, list[MarketState15s]] = defaultdict(list)
    for state in states:
        grouped[state.symbol].append(state)
    return {
        symbol: tuple(sorted(items, key=lambda state: state.bucket_start))
        for symbol, items in grouped.items()
    }


def _first_candidate_index(config: OrderFlowImpulseConfig) -> int:
    return max(
        config.baseline_window_buckets + config.impulse_window_buckets - 1,
        config.breakout_window_buckets,
    )


def _candidate_at(
    states: tuple[MarketState15s, ...],
    index: int,
    config: OrderFlowImpulseConfig,
) -> tuple[OrderFlowDirection, Decimal] | None:
    impulse = _impulse_window(states, index, config)
    baseline = _baseline_window(states, index, config)
    breakout = _breakout_window(states, index, config)
    if impulse is None or baseline is None or breakout is None:
        return None
    metrics = _impulse_metrics(impulse, baseline)
    if metrics is None:
        return None
    (
        impulse_return,
        aggressive_imbalance,
        _baseline_notional,
        notional_intensity,
    ) = metrics
    if notional_intensity < config.min_notional_intensity:
        return None
    start_price = _state_price(impulse[0])
    end_price = _state_price(impulse[-1])
    if start_price is None or end_price is None:
        return None
    breakout_high, breakout_low = _breakout_bounds(breakout)
    if breakout_high is None or breakout_low is None:
        return None
    if (
        impulse_return >= config.min_return_pct
        and aggressive_imbalance >= config.min_aggressive_imbalance
        and end_price > breakout_high
    ):
        return OrderFlowDirection.UP, breakout_high
    if (
        -impulse_return >= config.min_return_pct
        and aggressive_imbalance <= -config.min_aggressive_imbalance
        and end_price < breakout_low
    ):
        return OrderFlowDirection.DOWN, breakout_low
    return None


def _confirmed_detection_index(
    states: tuple[MarketState15s, ...],
    start_index: int,
    *,
    direction: OrderFlowDirection,
    breakout_level: Decimal,
    config: OrderFlowImpulseConfig,
) -> int | None:
    end_index = start_index + config.confirmation_buckets - 1
    if end_index >= len(states):
        return None
    for index in range(start_index, end_index + 1):
        state = states[index]
        price = _state_price(state)
        if price is None:
            return None
        imbalance = _aggressive_imbalance(
            state.aggressive_buy_notional,
            state.aggressive_sell_notional,
        )
        if direction is OrderFlowDirection.UP:
            if price <= breakout_level or imbalance < config.min_aggressive_imbalance:
                return None
        elif price >= breakout_level or imbalance > -config.min_aggressive_imbalance:
            return None
    return end_index


def _build_event(
    states: tuple[MarketState15s, ...],
    trigger_index: int,
    detection_index: int,
    *,
    direction: OrderFlowDirection,
    breakout_level: Decimal,
    config: OrderFlowImpulseConfig,
) -> OrderFlowImpulseEvent | None:
    impulse = _impulse_window(states, trigger_index, config)
    baseline = _baseline_window(states, trigger_index, config)
    if impulse is None or baseline is None:
        return None
    metrics = _impulse_metrics(impulse, baseline)
    if metrics is None:
        return None
    (
        impulse_return,
        aggressive_imbalance,
        baseline_notional,
        notional_intensity,
    ) = metrics
    impulse_start_price = _state_price(impulse[0])
    impulse_end_price = _state_price(impulse[-1])
    event_price = _state_price(states[detection_index])
    if (
        impulse_start_price is None
        or impulse_end_price is None
        or event_price is None
    ):
        return None
    directional_return = (
        impulse_return if direction is OrderFlowDirection.UP else -impulse_return
    )
    impulse_trade_notional = sum(
        (state.trade_notional for state in impulse),
        Decimal("0"),
    )
    aggressive_buy_notional = sum(
        (state.aggressive_buy_notional for state in impulse),
        Decimal("0"),
    )
    aggressive_sell_notional = sum(
        (state.aggressive_sell_notional for state in impulse),
        Decimal("0"),
    )
    forward_returns = {
        horizon: _forward_return(
            states,
            detection_index,
            horizon,
            direction=direction,
            event_price=event_price,
        )
        for horizon in config.forward_horizon_buckets
    }
    available_returns = tuple(
        value for value in forward_returns.values() if value is not None
    )
    detection_state = states[detection_index]
    return OrderFlowImpulseEvent(
        symbol=detection_state.symbol,
        direction=direction,
        detected_at=detection_state.bucket_start,
        impulse_start=impulse[0].bucket_start,
        impulse_end=impulse[-1].bucket_end,
        impulse_start_price=impulse_start_price,
        impulse_end_price=impulse_end_price,
        impulse_return_pct=directional_return,
        breakout_level=breakout_level,
        breakout_distance_pct=_breakout_distance_pct(
            event_price,
            direction=direction,
            breakout_level=breakout_level,
        ),
        impulse_trade_count=sum(state.trade_count for state in impulse),
        impulse_trade_notional=impulse_trade_notional,
        aggressive_buy_notional=aggressive_buy_notional,
        aggressive_sell_notional=aggressive_sell_notional,
        aggressive_imbalance=aggressive_imbalance,
        baseline_notional=baseline_notional,
        notional_intensity=notional_intensity,
        spread=detection_state.spread,
        midpoint=detection_state.midpoint,
        liquidation_count=sum(state.liquidation_count for state in impulse),
        liquidation_notional=sum(
            (state.liquidation_notional for state in impulse),
            Decimal("0"),
        ),
        forward_returns=forward_returns,
        max_favorable_return=max(available_returns) if available_returns else None,
        max_adverse_return=min(available_returns) if available_returns else None,
    )


def _impulse_window(
    states: tuple[MarketState15s, ...],
    index: int,
    config: OrderFlowImpulseConfig,
) -> tuple[MarketState15s, ...] | None:
    start = index - config.impulse_window_buckets + 1
    if start < 0:
        return None
    window = states[start : index + 1]
    if len(window) != config.impulse_window_buckets:
        return None
    if any(_state_price(state) is None for state in window):
        return None
    return window


def _baseline_window(
    states: tuple[MarketState15s, ...],
    index: int,
    config: OrderFlowImpulseConfig,
) -> tuple[MarketState15s, ...] | None:
    end = index - config.impulse_window_buckets + 1
    start = end - config.baseline_window_buckets
    if start < 0:
        return None
    window = states[start:end]
    if len(window) != config.baseline_window_buckets:
        return None
    return window


def _breakout_window(
    states: tuple[MarketState15s, ...],
    index: int,
    config: OrderFlowImpulseConfig,
) -> tuple[MarketState15s, ...] | None:
    start = index - config.breakout_window_buckets
    if start < 0:
        return None
    window = states[start:index]
    if len(window) != config.breakout_window_buckets:
        return None
    if any(_state_price(state) is None for state in window):
        return None
    return window


def _impulse_metrics(
    impulse: tuple[MarketState15s, ...],
    baseline: tuple[MarketState15s, ...],
) -> tuple[Decimal, Decimal, Decimal, Decimal] | None:
    start_price = _state_price(impulse[0])
    end_price = _state_price(impulse[-1])
    if start_price is None or end_price is None or start_price <= 0:
        return None
    baseline_total = sum(
        (state.trade_notional for state in baseline),
        Decimal("0"),
    )
    baseline_notional = (
        baseline_total / Decimal(len(baseline)) * Decimal(len(impulse))
    )
    if baseline_notional <= 0:
        return None
    impulse_notional = sum(
        (state.trade_notional for state in impulse),
        Decimal("0"),
    )
    aggressive_buy_notional = sum(
        (state.aggressive_buy_notional for state in impulse),
        Decimal("0"),
    )
    aggressive_sell_notional = sum(
        (state.aggressive_sell_notional for state in impulse),
        Decimal("0"),
    )
    return (
        (end_price - start_price) / start_price,
        _aggressive_imbalance(aggressive_buy_notional, aggressive_sell_notional),
        baseline_notional,
        impulse_notional / baseline_notional,
    )


def _breakout_bounds(
    states: tuple[MarketState15s, ...],
) -> tuple[Decimal | None, Decimal | None]:
    highs = tuple(_state_high(state) for state in states)
    lows = tuple(_state_low(state) for state in states)
    if any(value is None for value in highs) or any(value is None for value in lows):
        return None, None
    high_values = tuple(value for value in highs if value is not None)
    low_values = tuple(value for value in lows if value is not None)
    return max(high_values), min(low_values)


def _forward_return(
    states: tuple[MarketState15s, ...],
    detection_index: int,
    horizon: int,
    *,
    direction: OrderFlowDirection,
    event_price: Decimal,
) -> Decimal | None:
    future_index = detection_index + horizon
    if future_index >= len(states) or event_price <= 0:
        return None
    future_price = _state_price(states[future_index])
    if future_price is None:
        return None
    if direction is OrderFlowDirection.UP:
        return (future_price - event_price) / event_price
    return (event_price - future_price) / event_price


def _breakout_distance_pct(
    price: Decimal,
    *,
    direction: OrderFlowDirection,
    breakout_level: Decimal,
) -> Decimal:
    if direction is OrderFlowDirection.UP:
        return (price - breakout_level) / breakout_level
    return (breakout_level - price) / breakout_level


def _aggressive_imbalance(
    aggressive_buy_notional: Decimal,
    aggressive_sell_notional: Decimal,
) -> Decimal:
    total = aggressive_buy_notional + aggressive_sell_notional
    if total == 0:
        return Decimal("0")
    return (aggressive_buy_notional - aggressive_sell_notional) / total


def _state_price(state: MarketState15s) -> Decimal | None:
    return state.close_price or state.midpoint or state.mark_price


def _state_high(state: MarketState15s) -> Decimal | None:
    return state.high_price or _state_price(state)


def _state_low(state: MarketState15s) -> Decimal | None:
    return state.low_price or _state_price(state)


def _summarize_direction(
    events: tuple[OrderFlowImpulseEvent, ...],
    horizons: tuple[int, ...],
) -> OrderFlowImpulseDirectionSummary:
    return OrderFlowImpulseDirectionSummary(
        count=len(events),
        mean_forward_returns={
            horizon: _mean(
                tuple(
                    event.forward_returns[horizon]
                    for event in events
                    if event.forward_returns.get(horizon) is not None
                )
            )
            for horizon in horizons
        },
    )


def _mean(values: tuple[Decimal | None, ...]) -> Decimal | None:
    decimals = tuple(value for value in values if value is not None)
    if not decimals:
        return None
    return sum(decimals, Decimal("0")) / Decimal(len(decimals))
