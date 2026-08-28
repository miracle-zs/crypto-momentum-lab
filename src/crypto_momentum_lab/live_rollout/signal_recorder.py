"""Non-blocking persistence of live strategy signals and filter context."""

import asyncio
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable, Collection, Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

import structlog

from crypto_momentum_lab.domain.market.models import JsonValue, MarketState15s
from crypto_momentum_lab.domain.strategy import (
    OrderIntentCandidate,
    StrategyDecision,
    StrategySignal,
)
from crypto_momentum_lab.live_rollout.volume import (
    QuoteVolume24hProvider,
    QuoteVolume24hSnapshot,
)

log = structlog.get_logger()

_SIGNAL_SCHEMA_VERSION = 1
_MAX_SIGNAL_BATCH = 128
_PERSIST_BATCH_TIMEOUT_SECONDS = 0.25


class LiveSignalRecorderPort(Protocol):
    def record_decision(
        self,
        *,
        decision: StrategyDecision,
        state: MarketState15s,
        recorded_at: datetime,
        account_context: Mapping[str, object],
        filter_context: Mapping[str, object],
    ) -> None: ...

    def record_candidate(
        self,
        *,
        candidate: OrderIntentCandidate,
        state: MarketState15s,
        recorded_at: datetime,
        account_context: Mapping[str, object],
        filter_context: Mapping[str, object],
    ) -> None: ...


LiveSignalBatchSink = Callable[
    [tuple[Mapping[str, object], ...]],
    Awaitable[None],
]


@dataclass(frozen=True, slots=True)
class _QuoteVolumeFields:
    quote_volume_24h: Decimal | None
    quote_volume_24h_quote_asset: str | None
    quote_volume_24h_source: str | None
    quote_volume_24h_source_at: datetime | None
    quote_volume_24h_fetched_at: datetime | None
    quote_volume_24h_age_ms: int | None


@dataclass(frozen=True, slots=True)
class LiveSignalObservation:
    observation_id: str
    signal_id: str
    candidate_id: str | None
    run_id: str
    account_label: str
    strategy_name: str
    strategy_version: str
    config_hash: str
    code_commit: str
    signal_kind: str
    symbol: str
    side: str
    detected_at: datetime
    source_state_at: datetime
    recorded_at: datetime
    reason: str
    quote_volume_24h: Decimal | None
    quote_volume_24h_quote_asset: str | None
    quote_volume_24h_source: str | None
    quote_volume_24h_source_at: datetime | None
    quote_volume_24h_fetched_at: datetime | None
    quote_volume_24h_age_ms: int | None
    features: dict[str, JsonValue]
    reference_prices: dict[str, JsonValue]
    market_context: dict[str, JsonValue]
    filter_context: dict[str, JsonValue]
    candidate_context: dict[str, JsonValue]
    account_context: dict[str, JsonValue]

    def row(self) -> dict[str, object]:
        return {
            "observation_id": self.observation_id,
            "signal_id": self.signal_id,
            "candidate_id": self.candidate_id,
            "run_id": self.run_id,
            "account_label": self.account_label,
            "strategy_name": self.strategy_name,
            "strategy_version": self.strategy_version,
            "config_hash": self.config_hash,
            "code_commit": self.code_commit,
            "signal_kind": self.signal_kind,
            "symbol": self.symbol,
            "side": self.side,
            "detected_at": self.detected_at,
            "source_state_at": self.source_state_at,
            "recorded_at": self.recorded_at,
            "reason": self.reason,
            "schema_version": _SIGNAL_SCHEMA_VERSION,
            "quote_volume_24h": self.quote_volume_24h,
            "quote_volume_24h_quote_asset": (
                self.quote_volume_24h_quote_asset
            ),
            "quote_volume_24h_source": self.quote_volume_24h_source,
            "quote_volume_24h_source_at": self.quote_volume_24h_source_at,
            "quote_volume_24h_fetched_at": self.quote_volume_24h_fetched_at,
            "quote_volume_24h_age_ms": self.quote_volume_24h_age_ms,
            "features": self.features,
            "reference_prices": self.reference_prices,
            "market_context": self.market_context,
            "filter_context": self.filter_context,
            "candidate_context": self.candidate_context,
            "account_context": self.account_context,
        }


class LiveStrategySignalRecorder:
    """Record signals through a bounded queue isolated from order execution."""

    def __init__(
        self,
        *,
        run_id: str,
        account_label: str,
        strategy_name: str,
        strategy_version: str,
        config_hash: str,
        code_commit: str,
        quote_volume_provider: QuoteVolume24hProvider | None = None,
        persist: LiveSignalBatchSink | None = None,
        queue_size: int = 4096,
        max_recent_records: int = 4096,
    ) -> None:
        for value, field_name in (
            (run_id, "run_id"),
            (account_label, "account_label"),
            (strategy_name, "strategy_name"),
            (strategy_version, "strategy_version"),
            (config_hash, "config_hash"),
            (code_commit, "code_commit"),
        ):
            if not value.strip():
                raise ValueError(f"{field_name} must not be empty")
        if queue_size <= 0:
            raise ValueError("queue_size must be positive")
        if max_recent_records <= 0:
            raise ValueError("max_recent_records must be positive")
        self._run_id = run_id
        self._account_label = account_label
        self._strategy_name = strategy_name
        self._strategy_version = strategy_version
        self._config_hash = config_hash
        self._code_commit = code_commit
        self._quote_volume_provider = quote_volume_provider
        self._persist = persist
        self._queue_size = queue_size
        self._queue: asyncio.Queue[LiveSignalObservation | None] | None = None
        self._writer_task: asyncio.Task[None] | None = None
        self._recent_records: deque[LiveSignalObservation] = deque(
            maxlen=max_recent_records
        )
        self._recorded_count = 0
        self._dropped_count = 0
        self._persist_failure_count = 0
        self._build_failure_count = 0
        self._volume_lookup_failure_count = 0

    @property
    def recorded_count(self) -> int:
        return self._recorded_count

    @property
    def dropped_count(self) -> int:
        return self._dropped_count

    @property
    def persist_failure_count(self) -> int:
        return self._persist_failure_count

    @property
    def build_failure_count(self) -> int:
        return self._build_failure_count

    @property
    def volume_lookup_failure_count(self) -> int:
        return self._volume_lookup_failure_count

    @property
    def recent_records(self) -> tuple[LiveSignalObservation, ...]:
        return tuple(self._recent_records)

    async def start(self) -> None:
        if self._persist is None or self._writer_task is not None:
            return
        self._queue = asyncio.Queue(maxsize=self._queue_size)
        self._writer_task = asyncio.create_task(
            self._write_records(),
            name="live-strategy-signal-writer",
        )

    async def stop(self) -> None:
        writer_task = self._writer_task
        queue = self._queue
        if writer_task is None or queue is None:
            return
        await queue.put(None)
        await writer_task
        self._writer_task = None
        self._queue = None
        log.info(
            "live_strategy_signal_recorder_stopped",
            run_id=self._run_id,
            recorded_count=self._recorded_count,
            dropped_count=self._dropped_count,
            persist_failure_count=self._persist_failure_count,
            build_failure_count=self._build_failure_count,
            volume_lookup_failure_count=self._volume_lookup_failure_count,
        )

    def record_decision(
        self,
        *,
        decision: StrategyDecision,
        state: MarketState15s,
        recorded_at: datetime,
        account_context: Mapping[str, object],
        filter_context: Mapping[str, object],
    ) -> None:
        candidates_by_signal: dict[str, list[OrderIntentCandidate]] = defaultdict(
            list
        )
        for candidate in decision.candidates:
            candidates_by_signal[candidate.signal_id].append(candidate)
        for signal in decision.signals:
            self._record_observation(
                signal=signal,
                candidate_id=None,
                signal_kind="strategy_signal",
                state=state,
                recorded_at=recorded_at,
                account_context=account_context,
                filter_context=filter_context,
                candidate_context=_candidate_context(
                    candidates_by_signal.get(signal.signal_id, ())
                ),
            )

    def record_candidate(
        self,
        *,
        candidate: OrderIntentCandidate,
        state: MarketState15s,
        recorded_at: datetime,
        account_context: Mapping[str, object],
        filter_context: Mapping[str, object],
    ) -> None:
        self._record_observation(
            signal_id=candidate.signal_id,
            candidate_id=candidate.candidate_id,
            run_id=candidate.run_id,
            strategy_name=candidate.strategy_name,
            strategy_version=candidate.strategy_version,
            config_hash=candidate.config_hash,
            symbol=candidate.symbol,
            side=_enum_value(candidate.side),
            detected_at=candidate.created_at,
            source_state_at=state.bucket_start,
            reason=candidate.reason,
            features=candidate.features,
            reference_prices={
                "limit_price": _json_value(candidate.limit_price),
                "desired_notional": _json_value(candidate.desired_notional),
            },
            signal_kind=(
                "reduce_only_candidate"
                if candidate.reduce_only
                else "candidate"
            ),
            state=state,
            recorded_at=recorded_at,
            account_context=account_context,
            filter_context=filter_context,
            candidate_context={"candidate": _candidate_payload(candidate)},
        )

    def _record_observation(
        self,
        *,
        state: MarketState15s,
        recorded_at: datetime,
        account_context: Mapping[str, object],
        filter_context: Mapping[str, object],
        candidate_context: Mapping[str, object],
        signal_kind: str,
        signal: StrategySignal | None = None,
        signal_id: str | None = None,
        candidate_id: str | None = None,
        run_id: str | None = None,
        strategy_name: str | None = None,
        strategy_version: str | None = None,
        config_hash: str | None = None,
        symbol: str | None = None,
        side: str | None = None,
        detected_at: datetime | None = None,
        source_state_at: datetime | None = None,
        reason: str | None = None,
        features: Mapping[str, object] | None = None,
        reference_prices: Mapping[str, object] | None = None,
    ) -> None:
        try:
            if signal is not None:
                signal_id = signal.signal_id
                run_id = signal.run_id
                strategy_name = signal.strategy_name
                strategy_version = signal.strategy_version
                config_hash = signal.config_hash
                symbol = signal.symbol
                side = _enum_value(signal.side)
                detected_at = signal.detected_at
                source_state_at = signal.source_state_at
                reason = signal.reason
                features = signal.features
                reference_prices = signal.reference_prices
            values = (
                signal_id,
                run_id,
                strategy_name,
                strategy_version,
                config_hash,
                symbol,
                side,
                detected_at,
                source_state_at,
                reason,
            )
            if any(value is None for value in values):
                raise ValueError("live signal observation is missing identity")
            assert signal_id is not None
            assert run_id is not None
            assert strategy_name is not None
            assert strategy_version is not None
            assert config_hash is not None
            assert symbol is not None
            assert side is not None
            assert detected_at is not None
            assert source_state_at is not None
            assert reason is not None
            volume_fields = self._volume_fields(
                symbol=symbol,
                detected_at=detected_at,
            )
            observation = LiveSignalObservation(
                observation_id=_observation_id(
                    signal_kind=signal_kind,
                    run_id=run_id,
                    signal_id=signal_id,
                    candidate_id=candidate_id,
                ),
                signal_id=signal_id,
                candidate_id=candidate_id,
                run_id=run_id,
                account_label=self._account_label,
                strategy_name=strategy_name,
                strategy_version=strategy_version,
                config_hash=config_hash,
                code_commit=self._code_commit,
                signal_kind=signal_kind,
                symbol=symbol,
                side=side,
                detected_at=detected_at,
                source_state_at=source_state_at,
                recorded_at=recorded_at,
                reason=reason,
                quote_volume_24h=volume_fields.quote_volume_24h,
                quote_volume_24h_quote_asset=(
                    volume_fields.quote_volume_24h_quote_asset
                ),
                quote_volume_24h_source=volume_fields.quote_volume_24h_source,
                quote_volume_24h_source_at=(
                    volume_fields.quote_volume_24h_source_at
                ),
                quote_volume_24h_fetched_at=(
                    volume_fields.quote_volume_24h_fetched_at
                ),
                quote_volume_24h_age_ms=volume_fields.quote_volume_24h_age_ms,
                features=_json_mapping(features or {}),
                reference_prices=_json_mapping(reference_prices or {}),
                market_context=_market_context(state),
                filter_context=_json_mapping(filter_context),
                candidate_context=_json_mapping(candidate_context),
                account_context=_json_mapping(account_context),
            )
        except Exception as error:
            self._build_failure_count += 1
            log.warning(
                "live_strategy_signal_record_build_failed",
                run_id=self._run_id,
                error_type=type(error).__name__,
            )
            return
        self._recorded_count += 1
        self._recent_records.append(observation)
        self._enqueue(observation)

    def _volume_fields(
        self,
        *,
        symbol: str,
        detected_at: datetime,
    ) -> _QuoteVolumeFields:
        provider = self._quote_volume_provider
        if provider is None:
            return _empty_volume_fields()
        try:
            snapshot = provider.snapshot(symbol, as_of=detected_at)
        except Exception as error:
            self._volume_lookup_failure_count += 1
            log.warning(
                "live_strategy_signal_quote_volume_lookup_failed",
                run_id=self._run_id,
                symbol=symbol,
                error_type=type(error).__name__,
            )
            return _empty_volume_fields()
        if snapshot is None:
            return _empty_volume_fields()
        return _volume_fields(snapshot, detected_at=detected_at)

    def _enqueue(self, observation: LiveSignalObservation) -> None:
        if self._queue is None:
            return
        try:
            self._queue.put_nowait(observation)
        except asyncio.QueueFull:
            self._dropped_count += 1

    async def _write_records(self) -> None:
        if self._queue is None or self._persist is None:
            return
        while True:
            first = await self._queue.get()
            if first is None:
                return
            batch = [first]
            stop_after_batch = False
            for _ in range(_MAX_SIGNAL_BATCH - 1):
                try:
                    next_record = self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if next_record is None:
                    stop_after_batch = True
                    break
                batch.append(next_record)
            try:
                await asyncio.wait_for(
                    self._persist(
                        tuple(record.row() for record in batch)
                    ),
                    timeout=_PERSIST_BATCH_TIMEOUT_SECONDS,
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._persist_failure_count += len(batch)
                log.warning(
                    "live_strategy_signal_persist_failed",
                    run_id=self._run_id,
                    event_count=len(batch),
                    error_type=type(error).__name__,
                )
            if stop_after_batch:
                return


def _candidate_context(
    candidates: Collection[OrderIntentCandidate],
) -> dict[str, object]:
    return {
        "candidate_count": len(candidates),
        "candidates": [_candidate_payload(candidate) for candidate in candidates],
    }


def _candidate_payload(candidate: OrderIntentCandidate) -> dict[str, object]:
    return {
        "candidate_id": candidate.candidate_id,
        "signal_id": candidate.signal_id,
        "side": _enum_value(candidate.side),
        "entry_type": _enum_value(candidate.entry_type),
        "limit_price": candidate.limit_price,
        "desired_notional": candidate.desired_notional,
        "reduce_only": candidate.reduce_only,
        "expires_at": candidate.expires_at,
        "created_at": candidate.created_at,
        "reason": candidate.reason,
        "features": candidate.features,
    }


def _market_context(state: MarketState15s) -> dict[str, JsonValue]:
    values: dict[str, object] = {
        "exchange": state.exchange,
        "environment": state.environment,
        "symbol": state.symbol,
        "bucket_start": state.bucket_start,
        "bucket_end": state.bucket_end,
        "open_price": state.open_price,
        "high_price": state.high_price,
        "low_price": state.low_price,
        "close_price": state.close_price,
        "trade_count": state.trade_count,
        "trade_notional": state.trade_notional,
        "aggressive_buy_notional": state.aggressive_buy_notional,
        "aggressive_sell_notional": state.aggressive_sell_notional,
        "last_bid_price": state.last_bid_price,
        "last_ask_price": state.last_ask_price,
        "spread": state.spread,
        "midpoint": state.midpoint,
        "liquidation_count": state.liquidation_count,
        "liquidation_notional": state.liquidation_notional,
        "mark_price": state.mark_price,
        "closed_kline_count": state.closed_kline_count,
        "source_event_count": state.source_event_count,
        "first_received_at": state.first_received_at,
        "last_received_at": state.last_received_at,
        "data_complete": getattr(state, "data_complete", None),
        "missing_agg_trade_count": getattr(
            state,
            "missing_agg_trade_count",
            None,
        ),
    }
    return _json_mapping(values)


def _volume_fields(
    snapshot: QuoteVolume24hSnapshot,
    *,
    detected_at: datetime,
) -> _QuoteVolumeFields:
    age_ms = max(
        0,
        int((detected_at - snapshot.fetched_at).total_seconds() * 1000),
    )
    return _QuoteVolumeFields(
        quote_volume_24h=snapshot.quote_volume,
        quote_volume_24h_quote_asset=snapshot.quote_asset,
        quote_volume_24h_source=snapshot.source,
        quote_volume_24h_source_at=snapshot.source_at,
        quote_volume_24h_fetched_at=snapshot.fetched_at,
        quote_volume_24h_age_ms=age_ms,
    )


def _empty_volume_fields() -> _QuoteVolumeFields:
    return _QuoteVolumeFields(
        quote_volume_24h=None,
        quote_volume_24h_quote_asset=None,
        quote_volume_24h_source=None,
        quote_volume_24h_source_at=None,
        quote_volume_24h_fetched_at=None,
        quote_volume_24h_age_ms=None,
    )


def _observation_id(
    *,
    signal_kind: str,
    run_id: str,
    signal_id: str,
    candidate_id: str | None,
) -> str:
    value = "|".join(
        (
            signal_kind,
            run_id,
            signal_id,
            candidate_id or "",
        )
    )
    return f"live_sig_{uuid5(NAMESPACE_URL, value)}"


def _enum_value(value: object) -> str:
    if isinstance(value, Enum):
        return str(value.value)
    return str(value)


def _json_mapping(value: Mapping[str, object]) -> dict[str, JsonValue]:
    normalized = _json_value(value)
    if not isinstance(normalized, dict):
        raise TypeError("value must normalize to a JSON object")
    return normalized


def _json_value(value: object) -> JsonValue:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return _json_value(value.value)
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple | set | frozenset):
        return [_json_value(item) for item in value]
    return str(value)


__all__ = [
    "LiveSignalObservation",
    "LiveSignalRecorderPort",
    "LiveSignalBatchSink",
    "LiveStrategySignalRecorder",
]
