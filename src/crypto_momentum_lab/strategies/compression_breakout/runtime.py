from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
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
from crypto_momentum_lab.strategies.runtime_checkpoint import (
    market_state_payload,
)


@dataclass(frozen=True, slots=True)
class CompressionBreakoutRuntimeConfig:
    event_config: CompressionBreakoutConfig
    candidate_notional: Decimal | None
    candidate_ttl_buckets: int
    signal_interval_seconds: int = 300

    def __post_init__(self) -> None:
        if self.candidate_notional is not None and self.candidate_notional <= 0:
            raise ValueError("candidate_notional must be positive")
        if self.candidate_ttl_buckets <= 0:
            raise ValueError("candidate_ttl_buckets must be positive")
        if self.signal_interval_seconds <= 0:
            raise ValueError("signal_interval_seconds must be positive")
        if self.signal_interval_seconds % 15 != 0:
            raise ValueError("signal_interval_seconds must be divisible by 15")


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
        self._pending_signal_states: dict[str, list[MarketState15s]] = {}
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
                (
                    event_config.compression_window_buckets
                    + event_config.acceptance_buckets
                )
                * self._config.signal_interval_seconds
                // 15
            ),
            required_fields=("close_price", "high_price", "low_price"),
            max_gap_seconds=30,
            allow_entries_before_warmup=False,
        )

    def restore(self, checkpoint: StrategyCheckpoint) -> None:
        restored_buffers = checkpoint.payload.get("signal_buffers")
        if isinstance(restored_buffers, dict):
            self._buffers = _restore_signal_buffers(
                restored_buffers,
                maxlen=(
                    self._config.event_config.compression_window_buckets
                    + self._config.event_config.acceptance_buckets
                ),
            )
        self._warmup = {symbol: len(buffer) for symbol, buffer in self._buffers.items()}
        self._cooldown_remaining = dict(checkpoint.cooldown_buckets_remaining_by_symbol)
        self._last_processed = dict(checkpoint.last_processed_at_by_symbol)
        self._signal_sequence = _checkpoint_sequence(checkpoint.payload)

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

        signal_state = self._ingest_signal_state(state)
        if signal_state is None:
            warmup_complete = len(self._buffers.get(state.symbol, ())) >= (
                self._config.event_config.compression_window_buckets
                + self._config.event_config.acceptance_buckets
            )
            return self._decision(
                rejections=(
                    StrategyRejection(
                        reason=(
                            RejectionReason.NO_SIGNAL
                            if warmup_complete
                            else RejectionReason.INSUFFICIENT_WARMUP
                        ),
                        symbol=state.symbol,
                        bucket_start=state.bucket_start,
                        details={
                            "have": self._raw_warmup_bucket_count(state.symbol),
                            "need": self.required_data().warmup_buckets,
                            "state": "building_signal_bucket",
                        },
                    ),
                )
            )

        requirement = self.required_data()
        buffer = self._buffers.setdefault(
            signal_state.symbol,
            deque(
                maxlen=(
                    self._config.event_config.compression_window_buckets
                    + self._config.event_config.acceptance_buckets
                )
            ),
        )
        buffer.append(signal_state)
        self._warmup[signal_state.symbol] = len(buffer)

        required_signal_buckets = (
            self._config.event_config.compression_window_buckets
            + self._config.event_config.acceptance_buckets
        )
        if len(buffer) < required_signal_buckets:
            return self._decision(
                rejections=(
                    StrategyRejection(
                        reason=RejectionReason.INSUFFICIENT_WARMUP,
                        symbol=signal_state.symbol,
                        bucket_start=signal_state.bucket_start,
                        details={
                            "have": self._raw_warmup_bucket_count(signal_state.symbol),
                            "need": requirement.warmup_buckets,
                        },
                    ),
                )
            )

        cooldown = self._cooldown_remaining.get(signal_state.symbol, 0)
        if cooldown > 0:
            self._cooldown_remaining[signal_state.symbol] = cooldown - 1
            return self._decision(
                rejections=(
                    StrategyRejection(
                        reason=RejectionReason.COOLDOWN_ACTIVE,
                        symbol=signal_state.symbol,
                        bucket_start=signal_state.bucket_start,
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
                        symbol=signal_state.symbol,
                        bucket_start=signal_state.bucket_start,
                        details={"state": "evaluated"},
                    ),
                )
            )

        signal, candidate = self._build_signal_and_candidate(
            signal_state,
            evaluation,
        )
        self._cooldown_remaining[signal_state.symbol] = (
            self._config.event_config.cooldown_buckets
        )
        return self._decision(signals=(signal,), candidates=(candidate,))

    def _ingest_signal_state(
        self,
        state: MarketState15s,
    ) -> MarketState15s | None:
        interval_seconds = self._config.signal_interval_seconds
        if _state_duration_seconds(state) == interval_seconds:
            return state

        signal_start = _signal_bucket_start(
            state.bucket_start,
            interval_seconds=interval_seconds,
        )
        pending = self._pending_signal_states.get(state.symbol)
        if (
            pending
            and _signal_bucket_start(
                pending[0].bucket_start,
                interval_seconds=interval_seconds,
            )
            != signal_start
        ):
            pending = None
        if pending is None:
            pending = []
            self._pending_signal_states[state.symbol] = pending
        pending.append(state)

        signal_end = signal_start + timedelta(seconds=interval_seconds)
        if state.bucket_end < signal_end:
            return None

        self._pending_signal_states.pop(state.symbol, None)
        if pending[0].bucket_start != signal_start or state.bucket_end != signal_end:
            return None
        return _aggregate_signal_state(
            tuple(pending),
            bucket_start=signal_start,
            bucket_end=signal_end,
        )

    def _raw_warmup_bucket_count(self, symbol: str) -> int:
        completed = len(self._buffers.get(symbol, ()))
        pending = len(self._pending_signal_states.get(symbol, ()))
        return completed * self._config.signal_interval_seconds // 15 + pending

    def checkpoint(self) -> StrategyCheckpoint:
        return StrategyCheckpoint(
            last_processed_at_by_symbol=dict(self._last_processed),
            warmup_buckets_by_symbol=dict(self._warmup),
            cooldown_buckets_remaining_by_symbol=dict(self._cooldown_remaining),
            payload={
                "buffer_sizes": {
                    symbol: len(buffer) for symbol, buffer in self._buffers.items()
                },
                "signal_interval_seconds": self._config.signal_interval_seconds,
                "signal_sequence": self._signal_sequence,
                "signal_buffers": {
                    symbol: [market_state_payload(state) for state in buffer]
                    for symbol, buffer in self._buffers.items()
                },
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
        detected_at = (
            state.bucket_end
            if self._config.signal_interval_seconds > 15
            else state.bucket_start
        )
        side = _strategy_side(evaluation.direction)
        signal_id = deterministic_signal_id(
            identity=self._identity,
            symbol=state.symbol,
            side=side,
            detected_at=detected_at,
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
            detected_at=detected_at,
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
            expires_at=detected_at
            + timedelta(seconds=15 * self._config.candidate_ttl_buckets),
            created_at=detected_at,
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
    if state.close_price is not None:
        return state.close_price
    if state.midpoint is not None:
        return state.midpoint
    return state.mark_price


def _state_high(state: MarketState15s) -> Decimal | None:
    return state.high_price if state.high_price is not None else _state_price(state)


def _state_low(state: MarketState15s) -> Decimal | None:
    return state.low_price if state.low_price is not None else _state_price(state)


def _optional_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value)


def aggregate_compression_signal_states(
    states: Iterable[MarketState15s],
    *,
    signal_interval_seconds: int,
) -> tuple[MarketState15s, ...]:
    if signal_interval_seconds <= 0 or signal_interval_seconds % 15 != 0:
        raise ValueError("signal_interval_seconds must be positive and divisible by 15")
    grouped: dict[tuple[str, datetime], list[MarketState15s]] = {}
    completed: list[MarketState15s] = []
    for state in sorted(states, key=lambda item: (item.bucket_start, item.symbol)):
        if _state_duration_seconds(state) == signal_interval_seconds:
            completed.append(state)
            continue
        bucket_start = _signal_bucket_start(
            state.bucket_start,
            interval_seconds=signal_interval_seconds,
        )
        grouped.setdefault((state.symbol, bucket_start), []).append(state)
    for (_, bucket_start), bucket_states in grouped.items():
        bucket_end = bucket_start + timedelta(seconds=signal_interval_seconds)
        if (
            bucket_states[0].bucket_start != bucket_start
            or bucket_states[-1].bucket_end != bucket_end
        ):
            continue
        completed.append(
            _aggregate_signal_state(
                tuple(bucket_states),
                bucket_start=bucket_start,
                bucket_end=bucket_end,
            )
        )
    return tuple(sorted(completed, key=lambda item: (item.symbol, item.bucket_start)))


def _state_duration_seconds(state: MarketState15s) -> int:
    return int((state.bucket_end - state.bucket_start).total_seconds())


def _signal_bucket_start(
    value: datetime,
    *,
    interval_seconds: int,
) -> datetime:
    epoch_seconds = int(value.timestamp())
    aligned_epoch = epoch_seconds - epoch_seconds % interval_seconds
    return datetime.fromtimestamp(aligned_epoch, tz=UTC)


def _aggregate_signal_state(
    states: tuple[MarketState15s, ...],
    *,
    bucket_start: datetime,
    bucket_end: datetime,
) -> MarketState15s:
    first = states[0]
    prices = tuple(
        price for state in states if (price := _state_price(state)) is not None
    )
    highs = tuple(
        price for state in states if (price := _state_high(state)) is not None
    )
    lows = tuple(
        price for state in states if (price := _state_low(state)) is not None
    )
    return MarketState15s(
        schema_version=first.schema_version,
        exchange=first.exchange,
        environment=first.environment,
        symbol=first.symbol,
        bucket_start=bucket_start,
        bucket_end=bucket_end,
        open_price=prices[0] if prices else None,
        high_price=max(highs) if highs else None,
        low_price=min(lows) if lows else None,
        close_price=prices[-1] if prices else None,
        trade_count=sum(state.trade_count for state in states),
        trade_notional=sum(
            (state.trade_notional for state in states),
            Decimal("0"),
        ),
        aggressive_buy_notional=sum(
            (state.aggressive_buy_notional for state in states),
            Decimal("0"),
        ),
        aggressive_sell_notional=sum(
            (state.aggressive_sell_notional for state in states),
            Decimal("0"),
        ),
        last_bid_price=_last_value(states, "last_bid_price"),
        last_ask_price=_last_value(states, "last_ask_price"),
        spread=_last_value(states, "spread"),
        midpoint=_last_value(states, "midpoint"),
        liquidation_count=sum(state.liquidation_count for state in states),
        liquidation_notional=sum(
            (state.liquidation_notional for state in states),
            Decimal("0"),
        ),
        mark_price=_last_value(states, "mark_price"),
        closed_kline_count=sum(state.closed_kline_count for state in states),
        source_event_count=sum(state.source_event_count for state in states),
        first_received_at=next(
            (
                state.first_received_at
                for state in states
                if state.first_received_at is not None
            ),
            None,
        ),
        last_received_at=next(
            (
                state.last_received_at
                for state in reversed(states)
                if state.last_received_at is not None
            ),
            None,
        ),
    )


def _last_value(
    states: tuple[MarketState15s, ...],
    field_name: str,
) -> Decimal | None:
    for state in reversed(states):
        value = getattr(state, field_name)
        if isinstance(value, Decimal):
            return value
    return None


def _signal_state_payload(state: MarketState15s) -> dict[str, JsonValue]:
    return {
        "schema_version": state.schema_version,
        "exchange": state.exchange,
        "environment": state.environment,
        "symbol": state.symbol,
        "bucket_start": state.bucket_start.isoformat(),
        "bucket_end": state.bucket_end.isoformat(),
        "open_price": _optional_decimal(state.open_price),
        "high_price": _optional_decimal(state.high_price),
        "low_price": _optional_decimal(state.low_price),
        "close_price": _optional_decimal(state.close_price),
        "spread": _optional_decimal(state.spread),
        "midpoint": _optional_decimal(state.midpoint),
        "mark_price": _optional_decimal(state.mark_price),
    }


def _restore_signal_buffers(
    payload: dict[str, JsonValue],
    *,
    maxlen: int,
) -> dict[str, deque[MarketState15s]]:
    restored: dict[str, deque[MarketState15s]] = {}
    for symbol, raw_states in payload.items():
        if not isinstance(raw_states, list):
            continue
        states: deque[MarketState15s] = deque(maxlen=maxlen)
        for raw_state in raw_states:
            if isinstance(raw_state, dict):
                states.append(_signal_state_from_payload(raw_state))
        if states:
            restored[symbol] = states
    return restored


def _signal_state_from_payload(payload: dict[str, JsonValue]) -> MarketState15s:
    return MarketState15s(
        schema_version=int(str(payload["schema_version"])),
        exchange=str(payload["exchange"]),
        environment=str(payload["environment"]),
        symbol=str(payload["symbol"]),
        bucket_start=datetime.fromisoformat(str(payload["bucket_start"])),
        bucket_end=datetime.fromisoformat(str(payload["bucket_end"])),
        open_price=_payload_decimal(payload.get("open_price")),
        high_price=_payload_decimal(payload.get("high_price")),
        low_price=_payload_decimal(payload.get("low_price")),
        close_price=_payload_decimal(payload.get("close_price")),
        trade_count=0,
        trade_notional=Decimal("0"),
        aggressive_buy_notional=Decimal("0"),
        aggressive_sell_notional=Decimal("0"),
        last_bid_price=None,
        last_ask_price=None,
        spread=_payload_decimal(payload.get("spread")),
        midpoint=_payload_decimal(payload.get("midpoint")),
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=_payload_decimal(payload.get("mark_price")),
        closed_kline_count=0,
        source_event_count=0,
        first_received_at=None,
        last_received_at=None,
    )


def _payload_decimal(value: JsonValue) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def _checkpoint_sequence(payload: dict[str, JsonValue]) -> int:
    value = payload.get("signal_sequence", 0)
    try:
        sequence = int(str(value))
    except (TypeError, ValueError):
        return 0
    return max(sequence, 0)
