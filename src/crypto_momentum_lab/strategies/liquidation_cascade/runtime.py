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
from crypto_momentum_lab.strategies.liquidation_cascade.event_study import (
    LiquidationCascadeConfig,
    LiquidationCascadeDirection,
    LiquidationCascadeEvent,
    find_liquidation_cascades,
)
from crypto_momentum_lab.strategies.runtime_checkpoint import (
    market_state_payload,
    restore_market_state_buffers,
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
        self._buffers: dict[str, deque[MarketState15s]] = {}
        self._warmup: dict[str, int] = {}
        self._cooldown_remaining: dict[str, int] = {}
        self._last_processed: dict[str, datetime] = {}
        self._signal_sequence = 0

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
        self._warmup = dict(checkpoint.warmup_buckets_by_symbol)
        self._cooldown_remaining = dict(
            checkpoint.cooldown_buckets_remaining_by_symbol
        )
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
            checkpoint=self.checkpoint(),
        )

    def _build_signal_and_candidate(
        self,
        event: LiquidationCascadeEvent,
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
            expires_at=event.detected_at
            + timedelta(seconds=15 * self._config.candidate_ttl_buckets),
            created_at=event.detected_at,
            reason="liquidation_cascade",
            features=features,
        )
        return signal, candidate


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
        "cluster_move_pct": str(event.cluster_move_pct),
        "breakout_level": str(event.breakout_level),
        "breakout_distance_pct": str(event.breakout_distance_pct),
        "liquidation_count": event.liquidation_count,
        "liquidation_notional": str(event.liquidation_notional),
        "aggressive_imbalance": str(event.aggressive_imbalance),
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
