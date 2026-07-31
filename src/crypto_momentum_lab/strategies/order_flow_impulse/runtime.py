from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.market.models import JsonValue, MarketState15s
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    RejectionReason,
    StrategyCheckpoint,
    StrategyDataRequirement,
    StrategyDecision,
    StrategyMetadata,
    StrategyRejection,
    StrategyRunIdentity,
    StrategySide,
    StrategySignal,
    deterministic_candidate_id,
    deterministic_signal_id,
)
from crypto_momentum_lab.strategies.order_flow_impulse.event_study import (
    OrderFlowDirection,
    OrderFlowImpulseConfig,
    OrderFlowImpulseEvent,
    find_order_flow_impulses,
)
from crypto_momentum_lab.strategies.runtime_checkpoint import (
    market_state_payload,
    restore_market_state_buffers,
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
        self._buffers: dict[str, deque[MarketState15s]] = {}
        self._warmup: dict[str, int] = {}
        self._cooldown_remaining: dict[str, int] = {}
        self._last_processed: dict[str, datetime] = {}
        self._signal_sequence = 0

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
        self._warmup = dict(checkpoint.warmup_buckets_by_symbol)
        self._cooldown_remaining = dict(checkpoint.cooldown_buckets_remaining_by_symbol)
        self._last_processed = dict(checkpoint.last_processed_at_by_symbol)
        restored_buffers = checkpoint.payload.get("market_state_buffers")
        if isinstance(restored_buffers, dict):
            self._buffers = restore_market_state_buffers(
                restored_buffers,
                maxlen=self.required_data().warmup_buckets + 16,
            )
            for symbol, buffer in self._buffers.items():
                self._warmup[symbol] = len(buffer)
        self._signal_sequence = _checkpoint_sequence(checkpoint.payload)

    def restore_checkpoint(self, checkpoint: StrategyCheckpoint) -> None:
        self.restore(checkpoint)

    def reset_symbol(self, symbol: str) -> None:
        """Drop buffered state after the live source skips a data gap."""
        self._buffers.pop(symbol, None)
        self._warmup.pop(symbol, None)
        self._cooldown_remaining.pop(symbol, None)
        self._last_processed.pop(symbol, None)

    def on_market_state(self, state: MarketState15s) -> StrategyDecision:
        self._last_processed[state.symbol] = state.bucket_start
        if state.close_price is None:
            return self._decision(
                rejections=(
                    StrategyRejection(
                        reason=RejectionReason.MISSING_REQUIRED_PRICE,
                        symbol=state.symbol,
                        bucket_start=state.bucket_start,
                        details={"field": "close_price"},
                    ),
                )
            )

        requirement = self.required_data()
        buffer = self._buffers.setdefault(
            state.symbol,
            deque(maxlen=requirement.warmup_buckets + 16),
        )
        buffer.append(state)
        self._warmup[state.symbol] = len(buffer)
        if len(buffer) < requirement.warmup_buckets:
            return self._decision(
                rejections=(
                    StrategyRejection(
                        reason=RejectionReason.INSUFFICIENT_WARMUP,
                        symbol=state.symbol,
                        bucket_start=state.bucket_start,
                        details={
                            "have": len(buffer),
                            "need": requirement.warmup_buckets,
                        },
                    ),
                )
            )

        cooldown = self._cooldown_remaining.get(state.symbol, 0)
        if cooldown > 0:
            self._cooldown_remaining[state.symbol] = cooldown - 1
            return self._decision(
                rejections=(
                    StrategyRejection(
                        reason=RejectionReason.COOLDOWN_ACTIVE,
                        symbol=state.symbol,
                        bucket_start=state.bucket_start,
                        details={"remaining": cooldown},
                    ),
                )
            )

        event = _latest_event_for_state(
            tuple(buffer),
            self._config.event_config,
            state,
        )
        if event is None:
            return self._decision(
                rejections=(
                    StrategyRejection(
                        reason=RejectionReason.NO_SIGNAL,
                        symbol=state.symbol,
                        bucket_start=state.bucket_start,
                        details={"state": "evaluated"},
                    ),
                )
            )

        signal, candidate = self._build_signal_and_candidate(event)
        self._cooldown_remaining[state.symbol] = (
            self._config.event_config.cooldown_buckets
        )
        return self._decision(signals=(signal,), candidates=(candidate,))

    def checkpoint(self) -> StrategyCheckpoint:
        return StrategyCheckpoint(
            last_processed_at_by_symbol=dict(self._last_processed),
            warmup_buckets_by_symbol=dict(self._warmup),
            cooldown_buckets_remaining_by_symbol=dict(self._cooldown_remaining),
            payload={
                "buffer_sizes": {
                    symbol: len(buffer) for symbol, buffer in self._buffers.items()
                },
                "market_state_buffers": {
                    symbol: [market_state_payload(state) for state in buffer]
                    for symbol, buffer in self._buffers.items()
                },
                "signal_sequence": self._signal_sequence,
            },
        )

    def _decision(
        self,
        *,
        signals: tuple[StrategySignal, ...] = (),
        candidates: tuple[OrderIntentCandidate, ...] = (),
        rejections: tuple[StrategyRejection, ...] = (),
    ) -> StrategyDecision:
        return StrategyDecision(
            signals=signals,
            candidates=candidates,
            rejections=rejections,
        )

    def _build_signal_and_candidate(
        self,
        event: OrderFlowImpulseEvent,
    ) -> tuple[StrategySignal, OrderIntentCandidate]:
        self._signal_sequence += 1
        side = _strategy_side(event.direction)
        signal_id = deterministic_signal_id(
            identity=self._identity,
            symbol=event.symbol,
            side=side,
            detected_at=event.detected_at,
            sequence=self._signal_sequence,
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
            detected_at=event.detected_at,
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
            expires_at=event.detected_at
            + timedelta(seconds=15 * self._config.candidate_ttl_buckets),
            created_at=event.detected_at,
            reason="orderflow_impulse",
            features=features,
        )
        return signal, candidate


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
    return first_candidate + config.confirmation_buckets


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
        "liquidation_count": event.liquidation_count,
        "liquidation_notional": str(event.liquidation_notional),
    }


def _optional_decimal(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _checkpoint_sequence(payload: dict[str, JsonValue]) -> int:
    value = payload.get("signal_sequence", 0)
    try:
        sequence = int(str(value))
    except (TypeError, ValueError):
        return 0
    return max(sequence, 0)
