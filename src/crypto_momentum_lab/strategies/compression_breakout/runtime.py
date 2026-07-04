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
from crypto_momentum_lab.strategies.compression_breakout.event_study import (
    BreakoutDirection,
    CompressionBreakoutConfig,
)


@dataclass(frozen=True, slots=True)
class CompressionBreakoutRuntimeConfig:
    event_config: CompressionBreakoutConfig
    candidate_notional: Decimal | None
    candidate_ttl_buckets: int

    def __post_init__(self) -> None:
        if self.candidate_notional is not None and self.candidate_notional <= 0:
            raise ValueError("candidate_notional must be positive")
        if self.candidate_ttl_buckets <= 0:
            raise ValueError("candidate_ttl_buckets must be positive")


@dataclass(frozen=True, slots=True)
class _CompressionEvaluation:
    direction: BreakoutDirection
    range_start: datetime
    range_end: datetime
    range_high: Decimal
    range_low: Decimal
    range_midpoint: Decimal
    range_width_pct: Decimal
    breakout_price: Decimal
    breakout_distance_pct: Decimal
    spread: Decimal | None
    midpoint: Decimal | None


class CompressionBreakoutRuntimeStrategy:
    def __init__(
        self,
        *,
        config: CompressionBreakoutRuntimeConfig,
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
        return StrategyMetadata(name="compression_breakout", version="v0")

    def required_data(self) -> StrategyDataRequirement:
        event_config = self._config.event_config
        return StrategyDataRequirement(
            base_state_interval_seconds=15,
            warmup_buckets=(
                event_config.compression_window_buckets
                + event_config.acceptance_buckets
            ),
            required_fields=("close_price", "high_price", "low_price"),
            max_gap_seconds=30,
            allow_entries_before_warmup=False,
        )

    def restore(self, checkpoint: StrategyCheckpoint) -> None:
        self._warmup = dict(checkpoint.warmup_buckets_by_symbol)
        self._cooldown_remaining = dict(
            checkpoint.cooldown_buckets_remaining_by_symbol
        )
        self._last_processed = dict(checkpoint.last_processed_at_by_symbol)

    def restore_checkpoint(self, checkpoint: StrategyCheckpoint) -> None:
        self.restore(checkpoint)

    def on_market_state(self, state: MarketState15s) -> StrategyDecision:
        self._last_processed[state.symbol] = state.bucket_start
        if _state_price(state) is None:
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
            deque(maxlen=requirement.warmup_buckets),
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

        evaluation = _evaluate_buffer(tuple(buffer), self._config.event_config)
        if evaluation is None:
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

        signal, candidate = self._build_signal_and_candidate(state, evaluation)
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
                }
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
        state: MarketState15s,
        evaluation: _CompressionEvaluation,
    ) -> tuple[StrategySignal, OrderIntentCandidate]:
        self._signal_sequence += 1
        side = _strategy_side(evaluation.direction)
        signal_id = deterministic_signal_id(
            identity=self._identity,
            symbol=state.symbol,
            side=side,
            detected_at=state.bucket_start,
            sequence=self._signal_sequence,
        )
        features = _features(evaluation)
        signal = StrategySignal(
            signal_id=signal_id,
            run_id=self._identity.run_id,
            strategy_name=self._identity.strategy_name,
            strategy_version=self._identity.strategy_version,
            config_hash=self._identity.config_hash,
            symbol=state.symbol,
            side=side,
            detected_at=state.bucket_start,
            source_state_at=state.bucket_start,
            reason="compression_breakout",
            features=features,
            reference_prices={
                "breakout_price": str(evaluation.breakout_price),
                "spread": _optional_decimal(evaluation.spread),
                "midpoint": _optional_decimal(evaluation.midpoint),
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
            symbol=state.symbol,
            side=side,
            entry_type=EntryType.MARKET,
            limit_price=None,
            desired_notional=self._config.candidate_notional,
            reduce_only=False,
            expires_at=state.bucket_start
            + timedelta(seconds=15 * self._config.candidate_ttl_buckets),
            created_at=state.bucket_start,
            reason="compression_breakout",
            features=features,
        )
        return signal, candidate


def _evaluate_buffer(
    buffer: tuple[MarketState15s, ...],
    config: CompressionBreakoutConfig,
) -> _CompressionEvaluation | None:
    lookback = buffer[: config.compression_window_buckets]
    acceptance = buffer[config.compression_window_buckets :]
    compression = _compression_range(lookback, config)
    if compression is None:
        return None
    range_high, range_low, range_midpoint, range_width_pct = compression
    candidate_price = _state_price(acceptance[0])
    if candidate_price is None:
        return None
    direction = _breakout_direction(
        candidate_price,
        range_high=range_high,
        range_low=range_low,
        config=config,
    )
    if direction is None:
        return None
    for state in acceptance:
        price = _state_price(state)
        if price is None:
            return None
        if direction is BreakoutDirection.UP and price <= range_high:
            return None
        if direction is BreakoutDirection.DOWN and price >= range_low:
            return None
    detection_state = acceptance[-1]
    breakout_price = _state_price(detection_state)
    if breakout_price is None:
        return None
    return _CompressionEvaluation(
        direction=direction,
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
        spread=detection_state.spread,
        midpoint=detection_state.midpoint,
    )


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


def _features(evaluation: _CompressionEvaluation) -> dict[str, JsonValue]:
    return {
        "direction": evaluation.direction.value,
        "range_start": evaluation.range_start.isoformat(),
        "range_end": evaluation.range_end.isoformat(),
        "range_high": str(evaluation.range_high),
        "range_low": str(evaluation.range_low),
        "range_midpoint": str(evaluation.range_midpoint),
        "range_width_pct": str(evaluation.range_width_pct),
        "breakout_price": str(evaluation.breakout_price),
        "breakout_distance_pct": str(evaluation.breakout_distance_pct),
        "spread": _optional_decimal(evaluation.spread),
        "midpoint": _optional_decimal(evaluation.midpoint),
    }


def _strategy_side(direction: BreakoutDirection) -> StrategySide:
    if direction is BreakoutDirection.UP:
        return StrategySide.LONG
    return StrategySide.SHORT


def _state_price(state: MarketState15s) -> Decimal | None:
    return state.close_price or state.midpoint or state.mark_price


def _state_high(state: MarketState15s) -> Decimal | None:
    return state.high_price or _state_price(state)


def _state_low(state: MarketState15s) -> Decimal | None:
    return state.low_price or _state_price(state)


def _optional_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value)
