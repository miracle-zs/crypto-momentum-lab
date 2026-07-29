from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from crypto_momentum_lab.domain.market.models import MarketState15s


class LiquidationCascadeDirection(StrEnum):
    UP = "up"
    DOWN = "down"


@dataclass(frozen=True, slots=True)
class LiquidationCascadeConfig:
    liquidation_window_buckets: int
    breakout_window_buckets: int
    min_liquidation_count: int
    min_liquidation_notional: Decimal
    min_price_move_pct: Decimal
    min_aggressive_imbalance: Decimal
    confirmation_buckets: int
    cooldown_buckets: int
    forward_horizon_buckets: tuple[int, ...]

    def __post_init__(self) -> None:
        if self.liquidation_window_buckets <= 0:
            raise ValueError("liquidation_window_buckets must be positive")
        if self.breakout_window_buckets <= 0:
            raise ValueError("breakout_window_buckets must be positive")
        if self.min_liquidation_count <= 0:
            raise ValueError("min_liquidation_count must be positive")
        if self.min_liquidation_notional <= 0:
            raise ValueError("min_liquidation_notional must be positive")
        if self.min_price_move_pct <= 0:
            raise ValueError("min_price_move_pct must be positive")
        if self.min_aggressive_imbalance < 0:
            raise ValueError("min_aggressive_imbalance must be non-negative")
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
class LiquidationCascadeEvent:
    symbol: str
    direction: LiquidationCascadeDirection
    detected_at: datetime
    cluster_start: datetime
    cluster_end: datetime
    cluster_start_price: Decimal
    cluster_end_price: Decimal
    cluster_move_pct: Decimal
    breakout_level: Decimal
    breakout_distance_pct: Decimal
    liquidation_count: int
    liquidation_notional: Decimal
    cluster_trade_count: int
    cluster_trade_notional: Decimal
    aggressive_buy_notional: Decimal
    aggressive_sell_notional: Decimal
    aggressive_imbalance: Decimal
    spread: Decimal | None
    midpoint: Decimal | None
    mark_price: Decimal | None
    forward_returns: dict[int, Decimal | None]
    max_favorable_return: Decimal | None
    max_adverse_return: Decimal | None


@dataclass(frozen=True, slots=True)
class LiquidationCascadeDirectionSummary:
    count: int
    mean_forward_returns: dict[int, Decimal | None]


@dataclass(frozen=True, slots=True)
class LiquidationCascadeSummary:
    total_count: int
    by_direction: dict[LiquidationCascadeDirection, LiquidationCascadeDirectionSummary]


def find_liquidation_cascades(
    states: Iterable[MarketState15s],
    config: LiquidationCascadeConfig,
) -> tuple[LiquidationCascadeEvent, ...]:
    events: list[LiquidationCascadeEvent] = []
    for symbol_states in _states_by_symbol(states).values():
        events.extend(_find_symbol_events(symbol_states, config))
    return tuple(sorted(events, key=lambda event: (event.symbol, event.detected_at)))


def summarize_liquidation_cascades(
    events: Iterable[LiquidationCascadeEvent],
    *,
    horizons: Iterable[int],
) -> LiquidationCascadeSummary:
    event_tuple = tuple(events)
    horizon_tuple = tuple(sorted(set(horizons)))
    by_direction = {
        direction: _summarize_direction(
            tuple(event for event in event_tuple if event.direction is direction),
            horizon_tuple,
        )
        for direction in LiquidationCascadeDirection
    }
    return LiquidationCascadeSummary(
        total_count=len(event_tuple),
        by_direction=by_direction,
    )


def _find_symbol_events(
    states: tuple[MarketState15s, ...],
    config: LiquidationCascadeConfig,
) -> tuple[LiquidationCascadeEvent, ...]:
    events: list[LiquidationCascadeEvent] = []
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


def _first_candidate_index(config: LiquidationCascadeConfig) -> int:
    return max(
        config.breakout_window_buckets,
        config.liquidation_window_buckets - 1,
    )


def _candidate_at(
    states: tuple[MarketState15s, ...],
    index: int,
    config: LiquidationCascadeConfig,
) -> tuple[LiquidationCascadeDirection, Decimal] | None:
    cluster = _cluster_window(states, index, config)
    breakout = _breakout_window(states, index, config)
    if cluster is None or breakout is None:
        return None
    metrics = _cluster_metrics(cluster)
    if metrics is None:
        return None
    (
        raw_move,
        liquidation_count,
        liquidation_notional,
        aggressive_imbalance,
    ) = metrics
    if liquidation_count < config.min_liquidation_count:
        return None
    if liquidation_notional < config.min_liquidation_notional:
        return None
    cluster_end_price = _state_price(cluster[-1])
    if cluster_end_price is None:
        return None
    breakout_high, breakout_low = _breakout_bounds(breakout)
    if breakout_high is None or breakout_low is None:
        return None
    if (
        raw_move >= config.min_price_move_pct
        and aggressive_imbalance >= config.min_aggressive_imbalance
        and cluster_end_price > breakout_high
    ):
        return LiquidationCascadeDirection.UP, breakout_high
    if (
        -raw_move >= config.min_price_move_pct
        and aggressive_imbalance <= -config.min_aggressive_imbalance
        and cluster_end_price < breakout_low
    ):
        return LiquidationCascadeDirection.DOWN, breakout_low
    return None


def _confirmed_detection_index(
    states: tuple[MarketState15s, ...],
    start_index: int,
    *,
    direction: LiquidationCascadeDirection,
    breakout_level: Decimal,
    config: LiquidationCascadeConfig,
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
        if direction is LiquidationCascadeDirection.UP:
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
    direction: LiquidationCascadeDirection,
    breakout_level: Decimal,
    config: LiquidationCascadeConfig,
) -> LiquidationCascadeEvent | None:
    cluster = _cluster_window(states, trigger_index, config)
    if cluster is None:
        return None
    metrics = _cluster_metrics(cluster)
    if metrics is None:
        return None
    raw_move, liquidation_count, liquidation_notional, aggressive_imbalance = metrics
    cluster_start_price = _state_price(cluster[0])
    cluster_end_price = _state_price(cluster[-1])
    event_price = _state_price(states[detection_index])
    if (
        cluster_start_price is None
        or cluster_end_price is None
        or event_price is None
    ):
        return None
    directional_move = (
        raw_move if direction is LiquidationCascadeDirection.UP else -raw_move
    )
    aggressive_buy_notional = sum(
        (state.aggressive_buy_notional for state in cluster),
        Decimal("0"),
    )
    aggressive_sell_notional = sum(
        (state.aggressive_sell_notional for state in cluster),
        Decimal("0"),
    )
    cluster_trade_notional = sum(
        (state.trade_notional for state in cluster),
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
    return LiquidationCascadeEvent(
        symbol=detection_state.symbol,
        direction=direction,
        detected_at=detection_state.bucket_start,
        cluster_start=cluster[0].bucket_start,
        cluster_end=cluster[-1].bucket_end,
        cluster_start_price=cluster_start_price,
        cluster_end_price=cluster_end_price,
        cluster_move_pct=directional_move,
        breakout_level=breakout_level,
        breakout_distance_pct=_breakout_distance_pct(
            event_price,
            direction=direction,
            breakout_level=breakout_level,
        ),
        liquidation_count=liquidation_count,
        liquidation_notional=liquidation_notional,
        cluster_trade_count=sum(state.trade_count for state in cluster),
        cluster_trade_notional=cluster_trade_notional,
        aggressive_buy_notional=aggressive_buy_notional,
        aggressive_sell_notional=aggressive_sell_notional,
        aggressive_imbalance=aggressive_imbalance,
        spread=detection_state.spread,
        midpoint=detection_state.midpoint,
        mark_price=detection_state.mark_price,
        forward_returns=forward_returns,
        max_favorable_return=max(available_returns) if available_returns else None,
        max_adverse_return=min(available_returns) if available_returns else None,
    )


def _cluster_window(
    states: tuple[MarketState15s, ...],
    index: int,
    config: LiquidationCascadeConfig,
) -> tuple[MarketState15s, ...] | None:
    start = index - config.liquidation_window_buckets + 1
    if start < 0:
        return None
    window = states[start : index + 1]
    if len(window) != config.liquidation_window_buckets:
        return None
    if any(_state_price(state) is None for state in window):
        return None
    return window


def _breakout_window(
    states: tuple[MarketState15s, ...],
    index: int,
    config: LiquidationCascadeConfig,
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


def _cluster_metrics(
    cluster: tuple[MarketState15s, ...],
) -> tuple[Decimal, int, Decimal, Decimal] | None:
    start_price = _state_price(cluster[0])
    end_price = _state_price(cluster[-1])
    if start_price is None or end_price is None or start_price <= 0:
        return None
    liquidation_notional = sum(
        (state.liquidation_notional for state in cluster),
        Decimal("0"),
    )
    aggressive_buy_notional = sum(
        (state.aggressive_buy_notional for state in cluster),
        Decimal("0"),
    )
    aggressive_sell_notional = sum(
        (state.aggressive_sell_notional for state in cluster),
        Decimal("0"),
    )
    return (
        (end_price - start_price) / start_price,
        sum(state.liquidation_count for state in cluster),
        liquidation_notional,
        _aggressive_imbalance(aggressive_buy_notional, aggressive_sell_notional),
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
    direction: LiquidationCascadeDirection,
    event_price: Decimal,
) -> Decimal | None:
    future_index = detection_index + horizon
    if future_index >= len(states) or event_price <= 0:
        return None
    future_price = _state_price(states[future_index])
    if future_price is None:
        return None
    if direction is LiquidationCascadeDirection.UP:
        return (future_price - event_price) / event_price
    return (event_price - future_price) / event_price


def _breakout_distance_pct(
    price: Decimal,
    *,
    direction: LiquidationCascadeDirection,
    breakout_level: Decimal,
) -> Decimal:
    if direction is LiquidationCascadeDirection.UP:
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
    events: tuple[LiquidationCascadeEvent, ...],
    horizons: tuple[int, ...],
) -> LiquidationCascadeDirectionSummary:
    return LiquidationCascadeDirectionSummary(
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
