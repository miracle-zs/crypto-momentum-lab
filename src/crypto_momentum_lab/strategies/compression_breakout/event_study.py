from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from crypto_momentum_lab.domain.market.models import MarketState15s


class BreakoutDirection(StrEnum):
    UP = "up"
    DOWN = "down"


@dataclass(frozen=True, slots=True)
class CompressionBreakoutConfig:
    compression_window_buckets: int
    max_range_width_pct: Decimal
    min_breakout_pct: Decimal
    acceptance_buckets: int
    cooldown_buckets: int
    forward_horizon_buckets: tuple[int, ...]

    def __post_init__(self) -> None:
        if self.compression_window_buckets <= 0:
            raise ValueError("compression_window_buckets must be positive")
        if self.max_range_width_pct <= 0:
            raise ValueError("max_range_width_pct must be positive")
        if self.min_breakout_pct < 0:
            raise ValueError("min_breakout_pct must be non-negative")
        if self.acceptance_buckets <= 0:
            raise ValueError("acceptance_buckets must be positive")
        if self.cooldown_buckets < 0:
            raise ValueError("cooldown_buckets must be non-negative")
        if not self.forward_horizon_buckets:
            raise ValueError("forward_horizon_buckets must not be empty")
        horizons = tuple(sorted(set(self.forward_horizon_buckets)))
        if any(horizon <= 0 for horizon in horizons):
            raise ValueError("forward_horizon_buckets must be positive")
        object.__setattr__(self, "forward_horizon_buckets", horizons)


@dataclass(frozen=True, slots=True)
class CompressionBreakoutEvent:
    symbol: str
    direction: BreakoutDirection
    detected_at: datetime
    range_start: datetime
    range_end: datetime
    range_high: Decimal
    range_low: Decimal
    range_midpoint: Decimal
    range_width_pct: Decimal
    breakout_price: Decimal
    breakout_distance_pct: Decimal
    trade_count: int
    trade_notional: Decimal
    aggressive_buy_notional: Decimal
    aggressive_sell_notional: Decimal
    aggressive_imbalance: Decimal
    spread: Decimal | None
    midpoint: Decimal | None
    liquidation_count: int
    liquidation_notional: Decimal
    forward_returns: dict[int, Decimal | None]
    max_favorable_return: Decimal | None
    max_adverse_return: Decimal | None


@dataclass(frozen=True, slots=True)
class CompressionBreakoutDirectionSummary:
    count: int
    mean_forward_returns: dict[int, Decimal | None]


@dataclass(frozen=True, slots=True)
class CompressionBreakoutSummary:
    total_count: int
    by_direction: dict[BreakoutDirection, CompressionBreakoutDirectionSummary]


def find_compression_breakouts(
    states: Iterable[MarketState15s],
    config: CompressionBreakoutConfig,
) -> tuple[CompressionBreakoutEvent, ...]:
    events: list[CompressionBreakoutEvent] = []
    for symbol_states in _states_by_symbol(states).values():
        events.extend(_find_symbol_events(symbol_states, config))
    return tuple(sorted(events, key=lambda event: (event.symbol, event.detected_at)))


def summarize_compression_breakouts(
    events: Iterable[CompressionBreakoutEvent],
    *,
    horizons: Iterable[int],
) -> CompressionBreakoutSummary:
    event_tuple = tuple(events)
    horizon_tuple = tuple(sorted(set(horizons)))
    by_direction = {
        direction: _summarize_direction(
            tuple(event for event in event_tuple if event.direction is direction),
            horizon_tuple,
        )
        for direction in BreakoutDirection
    }
    return CompressionBreakoutSummary(
        total_count=len(event_tuple),
        by_direction=by_direction,
    )


def _find_symbol_events(
    states: tuple[MarketState15s, ...],
    config: CompressionBreakoutConfig,
) -> tuple[CompressionBreakoutEvent, ...]:
    events: list[CompressionBreakoutEvent] = []
    index = config.compression_window_buckets
    while index < len(states):
        lookback = states[index - config.compression_window_buckets : index]
        compression = _compression_range(lookback, config)
        if compression is None:
            index += 1
            continue
        range_high, range_low, range_midpoint, range_width_pct = compression
        candidate_price = _state_price(states[index])
        if candidate_price is None:
            index += 1
            continue
        direction = _breakout_direction(
            candidate_price,
            range_high=range_high,
            range_low=range_low,
            config=config,
        )
        if direction is None:
            index += 1
            continue
        detection_index = _accepted_detection_index(
            states,
            index,
            direction=direction,
            range_high=range_high,
            range_low=range_low,
            acceptance_buckets=config.acceptance_buckets,
        )
        if detection_index is None:
            index += 1
            continue
        event = _build_event(
            states,
            detection_index,
            lookback=lookback,
            direction=direction,
            range_high=range_high,
            range_low=range_low,
            range_midpoint=range_midpoint,
            range_width_pct=range_width_pct,
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


def _compression_range(
    lookback: tuple[MarketState15s, ...],
    config: CompressionBreakoutConfig,
) -> tuple[Decimal, Decimal, Decimal, Decimal] | None:
    highs = tuple(_state_high(state) for state in lookback)
    lows = tuple(_state_low(state) for state in lookback)
    if any(value is None for value in highs) or any(value is None for value in lows):
        return None
    high_values = tuple(value for value in highs if value is not None)
    low_values = tuple(value for value in lows if value is not None)
    range_high = max(high_values)
    range_low = min(low_values)
    if range_low <= 0:
        return None
    range_midpoint = (range_high + range_low) / Decimal("2")
    if range_midpoint <= 0:
        return None
    range_width_pct = (range_high - range_low) / range_midpoint
    if range_width_pct > config.max_range_width_pct:
        return None
    return range_high, range_low, range_midpoint, range_width_pct


def _breakout_direction(
    price: Decimal,
    *,
    range_high: Decimal,
    range_low: Decimal,
    config: CompressionBreakoutConfig,
) -> BreakoutDirection | None:
    if price > range_high * (Decimal("1") + config.min_breakout_pct):
        return BreakoutDirection.UP
    if price < range_low * (Decimal("1") - config.min_breakout_pct):
        return BreakoutDirection.DOWN
    return None


def _accepted_detection_index(
    states: tuple[MarketState15s, ...],
    start_index: int,
    *,
    direction: BreakoutDirection,
    range_high: Decimal,
    range_low: Decimal,
    acceptance_buckets: int,
) -> int | None:
    end_index = start_index + acceptance_buckets - 1
    if end_index >= len(states):
        return None
    for index in range(start_index, end_index + 1):
        price = _state_price(states[index])
        if price is None:
            return None
        if direction is BreakoutDirection.UP and price <= range_high:
            return None
        if direction is BreakoutDirection.DOWN and price >= range_low:
            return None
    return end_index


def _build_event(
    states: tuple[MarketState15s, ...],
    detection_index: int,
    *,
    lookback: tuple[MarketState15s, ...],
    direction: BreakoutDirection,
    range_high: Decimal,
    range_low: Decimal,
    range_midpoint: Decimal,
    range_width_pct: Decimal,
    config: CompressionBreakoutConfig,
) -> CompressionBreakoutEvent | None:
    state = states[detection_index]
    breakout_price = _state_price(state)
    if breakout_price is None:
        return None
    forward_returns = {
        horizon: _forward_return(
            states,
            detection_index,
            horizon,
            direction=direction,
            event_price=breakout_price,
        )
        for horizon in config.forward_horizon_buckets
    }
    available_returns = tuple(
        value for value in forward_returns.values() if value is not None
    )
    return CompressionBreakoutEvent(
        symbol=state.symbol,
        direction=direction,
        detected_at=state.bucket_start,
        range_start=lookback[0].bucket_start,
        range_end=lookback[-1].bucket_end,
        range_high=range_high,
        range_low=range_low,
        range_midpoint=range_midpoint,
        range_width_pct=range_width_pct,
        breakout_price=breakout_price,
        breakout_distance_pct=_breakout_distance_pct(
            breakout_price,
            direction=direction,
            range_high=range_high,
            range_low=range_low,
        ),
        trade_count=state.trade_count,
        trade_notional=state.trade_notional,
        aggressive_buy_notional=state.aggressive_buy_notional,
        aggressive_sell_notional=state.aggressive_sell_notional,
        aggressive_imbalance=_aggressive_imbalance(state),
        spread=state.spread,
        midpoint=state.midpoint,
        liquidation_count=state.liquidation_count,
        liquidation_notional=state.liquidation_notional,
        forward_returns=forward_returns,
        max_favorable_return=max(available_returns) if available_returns else None,
        max_adverse_return=min(available_returns) if available_returns else None,
    )


def _forward_return(
    states: tuple[MarketState15s, ...],
    detection_index: int,
    horizon: int,
    *,
    direction: BreakoutDirection,
    event_price: Decimal,
) -> Decimal | None:
    future_index = detection_index + horizon
    if future_index >= len(states) or event_price <= 0:
        return None
    future_price = _state_price(states[future_index])
    if future_price is None:
        return None
    if direction is BreakoutDirection.UP:
        return (future_price - event_price) / event_price
    return (event_price - future_price) / event_price


def _breakout_distance_pct(
    price: Decimal,
    *,
    direction: BreakoutDirection,
    range_high: Decimal,
    range_low: Decimal,
) -> Decimal:
    if direction is BreakoutDirection.UP:
        return (price - range_high) / range_high
    return (range_low - price) / range_low


def _aggressive_imbalance(state: MarketState15s) -> Decimal:
    total = state.aggressive_buy_notional + state.aggressive_sell_notional
    if total == 0:
        return Decimal("0")
    return (state.aggressive_buy_notional - state.aggressive_sell_notional) / total


def _state_price(state: MarketState15s) -> Decimal | None:
    if state.close_price is not None:
        return state.close_price
    if state.midpoint is not None:
        return state.midpoint
    return state.mark_price


def _state_high(state: MarketState15s) -> Decimal | None:
    return state.high_price if state.high_price is not None else _state_price(state)


def _state_low(state: MarketState15s) -> Decimal | None:
    return state.low_price if state.low_price is not None else _state_price(state)


def _summarize_direction(
    events: tuple[CompressionBreakoutEvent, ...],
    horizons: tuple[int, ...],
) -> CompressionBreakoutDirectionSummary:
    return CompressionBreakoutDirectionSummary(
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
