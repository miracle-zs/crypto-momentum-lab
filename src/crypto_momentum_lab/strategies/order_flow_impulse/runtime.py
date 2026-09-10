from collections.abc import Collection
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
        self._runtime = StrategyRuntimeState(
            buffer_payload_key="market_state_buffers"
        )

    def metadata(self) -> StrategyMetadata:
        return StrategyMetadata(name="orderflow_impulse", version="v0")

    def required_data(self) -> StrategyDataRequirement:
        event_config = self._config.event_config
        return StrategyDataRequirement(
            base_state_interval_seconds=15,
            warmup_buckets=_warmup_buckets(event_config),
            required_fields=(
                "close_price",
                "trade_notional",
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
        """Rebuild the derivable rolling buffer without evaluating signals.

        Durable checkpoints keep only control state.  The canonical runtime
        market-state table replays this method during restart so warming does
        not advance cooldowns or signal-id sequences.
        """
        self._runtime.warm_market_state(
            state,
            max_buffer_length=self.required_data().warmup_buckets + 16,
        )

    def reset_symbol(self, symbol: str) -> None:
        """Drop buffered state after the live source skips a data gap."""
        self._runtime.reset_symbol(symbol)

    @property
    def buffered_symbol_count(self) -> int:
        """Return the number of symbols with a derived rolling buffer."""

        return len(self._runtime.buffers)

    @property
    def buffered_state_count(self) -> int:
        """Return the total number of retained rolling states."""

        return sum(len(buffer) for buffer in self._runtime.buffers.values())

    def cache_protected_symbols(self) -> frozenset[str]:
        """Return symbols whose strategy cooldown must survive cache pruning."""

        return frozenset(
            symbol
            for symbol, remaining in self._runtime.cooldown_remaining.items()
            if remaining > 0
        )

    def prune_inactive_symbols(
        self,
        *,
        now: datetime,
        protected_symbols: Collection[str] = (),
        inactive_after: timedelta = timedelta(minutes=15),
    ) -> tuple[str, ...]:
        """Drop derived state for symbols that have been idle long enough.

        The strategy keeps account-independent rolling state locally, but the
        live daemon owns the protection set.  An evicted symbol starts from an
        empty buffer when it returns and therefore fails closed until it has
        warmed again.  This is deliberately an inactivity policy rather than
        a global byte cap: active symbols retain the existing buffer length.
        """

        _require_aware_datetime(now, "now")
        if inactive_after <= timedelta(0):
            raise ValueError("inactive_after must be positive")
        protected = {
            symbol.strip().upper()
            for symbol in protected_symbols
            if symbol.strip()
        }
        protected.update(self.cache_protected_symbols())
        cutoff = now - inactive_after
        candidates = (
            set(self._runtime.buffers)
            | set(self._runtime.warmup)
            | set(self._runtime.cooldown_remaining)
            | set(self._runtime.last_processed)
        )
        evicted: list[str] = []
        for symbol in sorted(candidates):
            if symbol in protected:
                continue
            last_processed = self._runtime.last_processed.get(symbol)
            if last_processed is not None and last_processed >= cutoff:
                continue
            self.reset_symbol(symbol)
            evicted.append(symbol)
        return tuple(evicted)

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


def _require_aware_datetime(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
