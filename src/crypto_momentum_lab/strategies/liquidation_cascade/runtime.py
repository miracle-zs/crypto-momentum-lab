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
from crypto_momentum_lab.strategies.liquidation_cascade.event_study import (
    LiquidationCascadeConfig,
    LiquidationCascadeDirection,
    LiquidationCascadeEvent,
    find_liquidation_cascades,
)
from crypto_momentum_lab.strategies.runtime_state import (
    StrategyRuntimeState,
    evaluate_buffered_state,
)


@dataclass(frozen=True, slots=True)
class LiquidationCascadeRuntimeConfig:
    event_config: LiquidationCascadeConfig
    candidate_notional: Decimal | None
    candidate_ttl_buckets: int

    def __post_init__(self) -> None:
        if self.candidate_notional is not None and self.candidate_notional <= 0:
            raise ValueError("candidate_notional must be positive")
        if self.candidate_ttl_buckets <= 0:
            raise ValueError("candidate_ttl_buckets must be positive")


class LiquidationCascadeRuntimeStrategy:
    def __init__(
        self,
        *,
        config: LiquidationCascadeRuntimeConfig,
        identity: StrategyRunIdentity,
    ) -> None:
        self._config = config
        self._identity = identity
        self._runtime = StrategyRuntimeState(
            buffer_payload_key="market_state_buffers"
        )

    def metadata(self) -> StrategyMetadata:
        return StrategyMetadata(name="liquidation_cascade", version="v0")

    def required_data(self) -> StrategyDataRequirement:
        event_config = self._config.event_config
        return StrategyDataRequirement(
            base_state_interval_seconds=15,
            warmup_buckets=_warmup_buckets(event_config),
            required_fields=(
                "close_price",
                "liquidation_count",
                "liquidation_notional",
                "aggressive_buy_notional",
                "aggressive_sell_notional",
            ),
            max_gap_seconds=30,
            allow_entries_before_warmup=False,
        )

    def restore(self, checkpoint: StrategyCheckpoint) -> None:
        self._runtime.restore(
            checkpoint,
            max_buffer_length=self.required_data().warmup_buckets + 16,
        )

    def restore_checkpoint(self, checkpoint: StrategyCheckpoint) -> None:
        self.restore(checkpoint)

    def warm_market_state(self, state: MarketState15s) -> None:
        """Rebuild the derivable rolling buffer without evaluating signals."""
        self._runtime.warm_market_state(
            state,
            max_buffer_length=self.required_data().warmup_buckets + 16,
        )

    def clear_market_state_buffers(self) -> None:
        """Drop checkpointed rolling data before a durable live rewarm."""

        self._runtime.clear_market_state_buffers()

    def reset_symbol(self, symbol: str) -> None:
        """Drop buffered state after the live source skips a data gap."""
        self._runtime.reset_symbol(symbol)

    def cooldown_buckets(self) -> int:
        return self._config.event_config.cooldown_buckets

    def on_market_state_without_cooldown(
        self,
        state: MarketState15s,
    ) -> StrategyDecision:
        """Evaluate one state without committing shared paired-run cooldown."""

        saved_cooldown = self._runtime.cooldown_remaining
        self._runtime.cooldown_remaining = {}
        try:
            return self.on_market_state(state)
        finally:
            self._runtime.cooldown_remaining = saved_cooldown

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

    def checkpoint(
        self,
        *,
        include_market_state_buffers: bool = True,
    ) -> StrategyCheckpoint:
        return self._runtime.checkpoint(
            include_market_state_buffers=include_market_state_buffers
        )

    def _build_signal_and_candidate(
        self,
        event: LiquidationCascadeEvent,
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
            reason="liquidation_cascade",
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
            reason="liquidation_cascade",
            features=features,
        )
        return signal, candidate

    def _find_event(
        self,
        states: tuple[MarketState15s, ...],
        state: MarketState15s,
    ) -> LiquidationCascadeEvent | None:
        return _latest_event_for_state(states, self._config.event_config, state)


def _latest_event_for_state(
    states: tuple[MarketState15s, ...],
    config: LiquidationCascadeConfig,
    state: MarketState15s,
) -> LiquidationCascadeEvent | None:
    events = find_liquidation_cascades(states, config)
    for event in reversed(events):
        if event.symbol == state.symbol and event.detected_at == state.bucket_start:
            return event
    return None


def _warmup_buckets(config: LiquidationCascadeConfig) -> int:
    first_candidate = max(
        config.breakout_window_buckets,
        config.liquidation_window_buckets - 1,
    )
    return first_candidate + config.confirmation_buckets


def _strategy_side(direction: LiquidationCascadeDirection) -> StrategySide:
    if direction is LiquidationCascadeDirection.UP:
        return StrategySide.LONG
    return StrategySide.SHORT


def _features(event: LiquidationCascadeEvent) -> dict[str, JsonValue]:
    return {
        "direction": event.direction.value,
        "cluster_start": event.cluster_start.isoformat(),
        "cluster_end": event.cluster_end.isoformat(),
        "cluster_start_price": str(event.cluster_start_price),
        "cluster_end_price": str(event.cluster_end_price),
        "cluster_move_pct": str(event.cluster_move_pct),
        "breakout_level": str(event.breakout_level),
        "breakout_distance_pct": str(event.breakout_distance_pct),
        "liquidation_count": event.liquidation_count,
        "liquidation_notional": str(event.liquidation_notional),
        "cluster_trade_count": event.cluster_trade_count,
        "cluster_trade_notional": str(event.cluster_trade_notional),
        "aggressive_buy_notional": str(event.aggressive_buy_notional),
        "aggressive_sell_notional": str(event.aggressive_sell_notional),
        "aggressive_imbalance": str(event.aggressive_imbalance),
        "mark_price": _optional_decimal(event.mark_price),
    }


def _optional_decimal(value: Decimal | None) -> str | None:
    return None if value is None else str(value)
