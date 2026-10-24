from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.market.models import JsonValue, MarketState15s
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategyCheckpoint,
    StrategyDataRequirement,
    StrategyDecision,
    StrategyMetadata,
    StrategyRunIdentity,
    StrategySide,
    StrategySignal,
    deterministic_candidate_id,
    deterministic_signal_id,
)
from crypto_momentum_lab.strategies.order_flow_impulse.event_study import (
    VOLUME_RATIO_TOTAL_BUCKETS,
    OrderFlowDirection,
    OrderFlowImpulseConfig,
    OrderFlowImpulseEvent,
    find_order_flow_impulses,
)
from crypto_momentum_lab.strategies.runtime_state import (
    StrategyRuntimeState,
    evaluate_buffered_state,
)


@dataclass(frozen=True, slots=True)
class OrderFlowImpulseRuntimeConfig:
    event_config: OrderFlowImpulseConfig
    candidate_notional: Decimal | None
    candidate_ttl_buckets: int

    def __post_init__(self) -> None:
        if self.candidate_notional is not None and self.candidate_notional <= 0:
            raise ValueError("candidate_notional must be positive")
        if self.candidate_ttl_buckets <= 0:
            raise ValueError("candidate_ttl_buckets must be positive")


class OrderFlowImpulseRuntimeStrategy:
    def __init__(
        self,
        *,
        config: OrderFlowImpulseRuntimeConfig,
        identity: StrategyRunIdentity,
    ) -> None:
        self._config = config
        self._identity = identity
        self._runtime = StrategyRuntimeState()

    def metadata(self) -> StrategyMetadata:
        return StrategyMetadata(name="orderflow_impulse", version="v0")

    def required_data(self) -> StrategyDataRequirement:
        event_config = self._config.event_config
        return StrategyDataRequirement(
            warmup_buckets=_warmup_buckets(event_config),
            max_gap_seconds=30,
        )

    def warm_market_state(self, state: MarketState15s) -> None:
        """Rebuild the derivable rolling buffer without evaluating signals.

        Startup feeds historical market states through this method so warming
        does not advance cooldowns or signal-id sequences.
        """
        self._runtime.warm_market_state(
            state,
            max_buffer_length=self.required_data().warmup_buckets + 16,
        )

    def clear_market_state_buffers(self) -> None:
        """Drop rolling market data without resetting signal or cooldown state."""
        self._runtime.clear_market_state_buffers()

    def reset_symbol(self, symbol: str) -> None:
        """Drop buffered state after the live source skips a data gap."""
        self._runtime.reset_symbol(symbol)

    @property
    def buffered_state_count(self) -> int:
        """Return the total number of retained rolling states."""

        return sum(len(buffer) for buffer in self._runtime.buffers.values())

    def on_market_state(self, state: MarketState15s) -> StrategyDecision:
        requirement = self.required_data()
        return evaluate_buffered_state(
            self._runtime,
            state,
            warmup_buckets=requirement.warmup_buckets,
            max_buffer_length=requirement.warmup_buckets + 16,
            cooldown_buckets=self._config.event_config.cooldown_buckets,
            find_event=self._find_event,
            build_signal_and_candidate=self._build_signal_and_candidate,
        )

    def checkpoint(self) -> StrategyCheckpoint:
        return self._runtime.checkpoint()

    def _build_signal_and_candidate(
        self,
        event: OrderFlowImpulseEvent,
        detected_at: datetime,
    ) -> tuple[StrategySignal, OrderIntentCandidate]:
        self._runtime.signal_sequence += 1
        side = _strategy_side(event.direction)
        signal_id = deterministic_signal_id(
            identity=self._identity,
            symbol=event.symbol,
            side=side,
            detected_at=detected_at,
            sequence=self._runtime.signal_sequence,
        )
        features = _features(event)
        signal = StrategySignal(
            signal_id=signal_id,
            run_id=self._identity.run_id,
            strategy_name=self._identity.strategy_name,
            strategy_version=self._identity.strategy_version,
            config_hash=self._identity.config_hash,
            symbol=event.symbol,
            side=side,
            detected_at=detected_at,
            source_state_at=event.detected_at,
            reason="orderflow_impulse",
            features=features,
            reference_prices={
                "breakout_level": str(event.breakout_level),
                "spread": _optional_decimal(event.spread),
                "midpoint": _optional_decimal(event.midpoint),
            },
        )
        candidate = OrderIntentCandidate(
            candidate_id=deterministic_candidate_id(
                signal_id=signal.signal_id,
                sequence=1,
            ),
            signal_id=signal.signal_id,
            run_id=self._identity.run_id,
            strategy_name=self._identity.strategy_name,
            strategy_version=self._identity.strategy_version,
            config_hash=self._identity.config_hash,
            symbol=event.symbol,
            side=side,
            entry_type=EntryType.MARKET,
            limit_price=None,
            desired_notional=self._config.candidate_notional,
            reduce_only=False,
            expires_at=detected_at
            + timedelta(seconds=15 * self._config.candidate_ttl_buckets),
            created_at=detected_at,
            reason="orderflow_impulse",
            features=features,
        )
        return signal, candidate

    def _find_event(
        self,
        states: tuple[MarketState15s, ...],
        state: MarketState15s,
    ) -> OrderFlowImpulseEvent | None:
        return _latest_event_for_state(states, self._config.event_config, state)


def _latest_event_for_state(
    states: tuple[MarketState15s, ...],
    config: OrderFlowImpulseConfig,
    state: MarketState15s,
) -> OrderFlowImpulseEvent | None:
    events = find_order_flow_impulses(states, config)
    for event in reversed(events):
        if event.symbol == state.symbol and event.detected_at == state.bucket_start:
            return event
    return None


def _warmup_buckets(config: OrderFlowImpulseConfig) -> int:
    first_candidate = max(
        config.baseline_window_buckets + config.impulse_window_buckets - 1,
        config.breakout_window_buckets,
    )
    strategy_warmup = first_candidate + config.confirmation_buckets
    if config.min_notional_5m_vs_30m > 0:
        return max(strategy_warmup, VOLUME_RATIO_TOTAL_BUCKETS)
    return strategy_warmup


def _strategy_side(direction: OrderFlowDirection) -> StrategySide:
    if direction is OrderFlowDirection.UP:
        return StrategySide.LONG
    return StrategySide.SHORT


def _features(event: OrderFlowImpulseEvent) -> dict[str, JsonValue]:
    return {
        "direction": event.direction.value,
        "impulse_start": event.impulse_start.isoformat(),
        "impulse_end": event.impulse_end.isoformat(),
        "impulse_start_price": str(event.impulse_start_price),
        "impulse_end_price": str(event.impulse_end_price),
        "impulse_return_pct": str(event.impulse_return_pct),
        "breakout_level": str(event.breakout_level),
        "breakout_distance_pct": str(event.breakout_distance_pct),
        "impulse_trade_count": event.impulse_trade_count,
        "impulse_trade_notional": str(event.impulse_trade_notional),
        "aggressive_buy_notional": str(event.aggressive_buy_notional),
        "aggressive_sell_notional": str(event.aggressive_sell_notional),
        "aggressive_imbalance": str(event.aggressive_imbalance),
        "baseline_notional": str(event.baseline_notional),
        "notional_intensity": str(event.notional_intensity),
        "notional_5m_vs_30m": _optional_decimal(event.notional_5m_vs_30m),
        "liquidation_count": event.liquidation_count,
        "liquidation_notional": str(event.liquidation_notional),
    }


def _optional_decimal(value: Decimal | None) -> str | None:
    return None if value is None else str(value)
