import asyncio
import heapq
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Protocol

import structlog

from crypto_momentum_lab.domain.market.models import (
    AggTradeGap,
    CaptureStream,
    MarketState15s,
    NormalizedAggTrade,
    NormalizedBookTicker,
    NormalizedMarketEvent,
    RawEnvelope,
    RealtimeMarketQuote,
)
from crypto_momentum_lab.market_data.aggregation import (
    MarketState15sAccumulator,
    MarketState15sSnapshot,
    bucket_start_15s,
)
from crypto_momentum_lab.market_data.normalization import (
    BinanceNormalizationError,
    normalize_binance_envelope,
)
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    RuntimeStateSequenceRange,
)

type _BucketKey = tuple[str, str, datetime]
type _BucketDeadline = tuple[datetime, str, str, _BucketKey]
type _DurableBatch = tuple[
    tuple[MarketState15s, ...],
    datetime,
    RuntimeStateSequenceRange,
]
type _DurableCommand = _DurableBatch | AggTradeGap

_BUCKET_SECONDS = 15
_LATENESS_THRESHOLDS_SECONDS = (0.5, 1.0, 2.0, 3.0)
_LATENESS_BUCKET_UPPER_BOUNDS_MS = (
    0.0,
    50.0,
    100.0,
    250.0,
    500.0,
    1_000.0,
    2_000.0,
    3_000.0,
    5_000.0,
    10_000.0,
)
_LATENESS_BUCKET_LABELS = (
    "<=0ms",
    "0-50ms",
    "50-100ms",
    "100-250ms",
    "250-500ms",
    "500-1000ms",
    "1000-2000ms",
    "2000-3000ms",
    "3000-5000ms",
    "5000-10000ms",
    ">10000ms",
)


class ClosedStateRepository(Protocol):
    async def save_closed_states(
        self,
        states: tuple[MarketState15s, ...],
        *,
        source_watermark_at: datetime,
        sequence_range: RuntimeStateSequenceRange,
    ) -> None: ...

    async def mark_incomplete(self, gap: AggTradeGap) -> None: ...


type RealtimeStateSink = Callable[[tuple[MarketState15s, ...]], Awaitable[None]]
type RealtimeQuoteSink = Callable[[RealtimeMarketQuote], Awaitable[None]]


@dataclass(frozen=True, slots=True, init=False)
class ClosedMarketStatePublisherConfig:
    realtime_closure_delay_seconds: float
    durable_closure_delay_seconds: float
    persistence_queue_size: int = 128
    persistence_retry_seconds: float = 1.0

    def __init__(
        self,
        *,
        realtime_closure_delay_seconds: float = 1.0,
        durable_closure_delay_seconds: float = 3.0,
        persistence_queue_size: int = 128,
        persistence_retry_seconds: float = 1.0,
        closure_delay_seconds: float | None = None,
    ) -> None:
        # Existing research fixtures still pass one delay.  Treat it as both
        # clocks so those callers retain their original semantics.
        if closure_delay_seconds is not None:
            realtime_closure_delay_seconds = closure_delay_seconds
            durable_closure_delay_seconds = closure_delay_seconds
        object.__setattr__(
            self,
            "realtime_closure_delay_seconds",
            realtime_closure_delay_seconds,
        )
        object.__setattr__(
            self,
            "durable_closure_delay_seconds",
            durable_closure_delay_seconds,
        )
        object.__setattr__(self, "persistence_queue_size", persistence_queue_size)
        object.__setattr__(
            self,
            "persistence_retry_seconds",
            persistence_retry_seconds,
        )
        self.__post_init__()

    def __post_init__(self) -> None:
        if self.realtime_closure_delay_seconds <= 0:
            raise ValueError(
                "realtime_closure_delay_seconds must be positive"
            )
        if self.durable_closure_delay_seconds <= 0:
            raise ValueError(
                "durable_closure_delay_seconds must be positive"
            )
        if (
            self.durable_closure_delay_seconds
            < self.realtime_closure_delay_seconds
        ):
            raise ValueError(
                "durable_closure_delay_seconds must be >= "
                "realtime_closure_delay_seconds"
            )
        if self.persistence_queue_size <= 0:
            raise ValueError("persistence_queue_size must be positive")
        if self.persistence_retry_seconds <= 0:
            raise ValueError("persistence_retry_seconds must be positive")

    @property
    def closure_delay_seconds(self) -> float:
        """Compatibility alias for the realtime clock."""
        return self.realtime_closure_delay_seconds


@dataclass(frozen=True, slots=True)
class ClosedMarketStatePublisherMetrics:
    received_envelope_count: int
    normalized_event_count: int
    closed_state_count: int
    rejected_envelope_count: int
    late_event_count: int
    latest_watermark_at: datetime | None
    realtime_batch_count: int
    realtime_sink_failure_count: int
    durable_queue_size: int
    durable_sink_failure_count: int
    aggregation_processing_count: int
    aggregation_processing_average_ms: float
    aggregation_processing_max_ms: float
    active_bucket_count: int
    active_bucket_high_watermark: int
    incomplete_active_bucket_count: int
    incomplete_gap_count: int
    missing_agg_trade_count: int
    late_recovered_event_count: int


@dataclass(slots=True)
class _EventLatenessCounters:
    raw_event_count: int = 0
    timestamped_event_count: int = 0
    normalized_event_count: int = 0
    missing_exchange_event_at_count: int = 0
    negative_lateness_count: int = 0
    lateness_bucket_counts: list[int] = field(
        default_factory=lambda: [0] * len(_LATENESS_BUCKET_LABELS)
    )
    received_over_threshold_counts: list[int] = field(
        default_factory=lambda: [0] * len(_LATENESS_THRESHOLDS_SECONDS)
    )
    simulated_close_drop_counts: list[int] = field(
        default_factory=lambda: [0] * len(_LATENESS_THRESHOLDS_SECONDS)
    )

    def observe_raw(self, envelope: RawEnvelope) -> None:
        self.raw_event_count += 1
        exchange_event_at = envelope.exchange_event_at
        if exchange_event_at is None:
            self.missing_exchange_event_at_count += 1
            return

        self.timestamped_event_count += 1
        lateness_seconds = (
            envelope.received_at - exchange_event_at
        ).total_seconds()
        if lateness_seconds < 0:
            self.negative_lateness_count += 1
        lateness_ms = lateness_seconds * 1000
        bucket_index = len(_LATENESS_BUCKET_UPPER_BOUNDS_MS)
        for index, upper_bound_ms in enumerate(
            _LATENESS_BUCKET_UPPER_BOUNDS_MS
        ):
            if lateness_ms <= upper_bound_ms:
                bucket_index = index
                break
        self.lateness_bucket_counts[bucket_index] += 1
        for index, threshold_seconds in enumerate(_LATENESS_THRESHOLDS_SECONDS):
            if lateness_seconds > threshold_seconds:
                self.received_over_threshold_counts[index] += 1

    def observe_normalized(self, *, simulated_close_drops: tuple[bool, ...]) -> None:
        if len(simulated_close_drops) != len(_LATENESS_THRESHOLDS_SECONDS):
            raise ValueError("simulated_close_drops has an unexpected length")
        self.normalized_event_count += 1
        for index, would_drop in enumerate(simulated_close_drops):
            if would_drop:
                self.simulated_close_drop_counts[index] += 1

    def snapshot(self) -> dict[str, object]:
        threshold_keys = tuple(
            f"{threshold_seconds:g}"
            for threshold_seconds in _LATENESS_THRESHOLDS_SECONDS
        )
        return {
            "raw_event_count": self.raw_event_count,
            "timestamped_event_count": self.timestamped_event_count,
            "normalized_event_count": self.normalized_event_count,
            "missing_exchange_event_at_count": (
                self.missing_exchange_event_at_count
            ),
            "negative_lateness_count": self.negative_lateness_count,
            "lateness_histogram_ms": dict(
                zip(
                    _LATENESS_BUCKET_LABELS,
                    self.lateness_bucket_counts,
                    strict=True,
                )
            ),
            "received_over_threshold_count": dict(
                zip(
                    threshold_keys,
                    self.received_over_threshold_counts,
                    strict=True,
                )
            ),
            "simulated_close_drop_count": dict(
                zip(
                    threshold_keys,
                    self.simulated_close_drop_counts,
                    strict=True,
                )
            ),
        }


class ClosedMarketStatePublisher:
    def __init__(
        self,
        *,
        repository: ClosedStateRepository,
        config: ClosedMarketStatePublisherConfig | None = None,
        realtime_state_sink: RealtimeStateSink | None = None,
        realtime_quote_sink: RealtimeQuoteSink | None = None,
    ) -> None:
        self._repository = repository
        self._realtime_state_sink = realtime_state_sink
        self._realtime_quote_sink = realtime_quote_sink
        self._config = (
            ClosedMarketStatePublisherConfig()
            if config is None
            else config
        )
        self._accumulators_by_bucket: dict[
            _BucketKey, MarketState15sAccumulator
        ] = {}
        self._latest_book_ticker_by_bucket: dict[
            _BucketKey,
            NormalizedBookTicker,
        ] = {}
        self._realtime_deadlines: list[_BucketDeadline] = []
        self._durable_deadlines: list[_BucketDeadline] = []
        self._incomplete_buckets: dict[_BucketKey, int] = {}
        self._realtime_latest_quotes: dict[
            tuple[str, str], tuple[Decimal, Decimal]
        ] = {}
        self._durable_latest_quotes: dict[
            tuple[str, str], tuple[Decimal, Decimal]
        ] = {}
        self._max_seen_event_at: datetime | None = None
        self._latest_watermark_at: datetime | None = None
        self._latest_durable_watermark_at: datetime | None = None
        self._received_envelope_count = 0
        self._normalized_event_count = 0
        self._closed_state_count = 0
        self._rejected_envelope_count = 0
        self._late_event_count = 0
        self._realtime_batch_count = 0
        self._realtime_sink_failure_count = 0
        self._realtime_quote_failure_count = 0
        self._durable_sink_failure_count = 0
        self._aggregation_processing_count = 0
        self._aggregation_processing_seconds = 0.0
        self._aggregation_processing_max_seconds = 0.0
        self._active_bucket_high_watermark = 0
        self._incomplete_gap_count = 0
        self._missing_agg_trade_count = 0
        self._late_recovered_event_count = 0
        self._lateness_by_stream: dict[
            CaptureStream,
            _EventLatenessCounters,
        ] = {}
        self._durable_queue: asyncio.Queue[_DurableCommand | None] | None = None
        self._durable_task: asyncio.Task[None] | None = None
        self._log = structlog.get_logger()

    async def start(self) -> None:
        if self._durable_task is not None:
            return
        queue: asyncio.Queue[_DurableCommand | None] = asyncio.Queue(
            maxsize=self._config.persistence_queue_size
        )
        self._durable_queue = queue
        self._durable_task = asyncio.create_task(self._persist_loop(queue))

    async def stop(self) -> None:
        task = self._durable_task
        queue = self._durable_queue
        self._durable_task = None
        self._durable_queue = None
        if task is None or queue is None:
            return
        try:
            await asyncio.wait_for(queue.join(), timeout=30)
        except TimeoutError:
            self._log.error(
                "runtime_state_persistence_drain_timed_out",
                queue_size=queue.qsize(),
            )
            task.cancel()
        else:
            await queue.put(None)
        await asyncio.gather(task, return_exceptions=True)

    @property
    def metrics(self) -> ClosedMarketStatePublisherMetrics:
        average_seconds = (
            0.0
            if self._aggregation_processing_count == 0
            else self._aggregation_processing_seconds
            / self._aggregation_processing_count
        )
        return ClosedMarketStatePublisherMetrics(
            received_envelope_count=self._received_envelope_count,
            normalized_event_count=self._normalized_event_count,
            closed_state_count=self._closed_state_count,
            rejected_envelope_count=self._rejected_envelope_count,
            late_event_count=self._late_event_count,
            latest_watermark_at=self._latest_watermark_at,
            realtime_batch_count=self._realtime_batch_count,
            realtime_sink_failure_count=self._realtime_sink_failure_count,
            durable_queue_size=(
                0 if self._durable_queue is None else self._durable_queue.qsize()
            ),
            durable_sink_failure_count=self._durable_sink_failure_count,
            aggregation_processing_count=self._aggregation_processing_count,
            aggregation_processing_average_ms=average_seconds * 1000,
            aggregation_processing_max_ms=(
                self._aggregation_processing_max_seconds * 1000
            ),
            active_bucket_count=len(self._accumulators_by_bucket),
            active_bucket_high_watermark=self._active_bucket_high_watermark,
            incomplete_active_bucket_count=len(self._incomplete_buckets),
            incomplete_gap_count=self._incomplete_gap_count,
            missing_agg_trade_count=self._missing_agg_trade_count,
            late_recovered_event_count=self._late_recovered_event_count,
        )

    def lateness_metrics_snapshot(self) -> dict[str, object]:
        """Return bounded transport and close-threshold counters by stream.

        ``received_over_threshold_count`` measures transport lateness directly
        from ``received_at - exchange_event_at``.  ``simulated_close_drop_count``
        replays the current watermark rule with each candidate delay, so the
        two counters do not conflate a late packet with a packet that would
        actually arrive after a state bucket had already been closed.
        """
        metrics = self.metrics
        return {
            "configured_closure_delay_seconds": (
                self._config.realtime_closure_delay_seconds
            ),
            "configured_realtime_closure_delay_seconds": (
                self._config.realtime_closure_delay_seconds
            ),
            "configured_durable_closure_delay_seconds": (
                self._config.durable_closure_delay_seconds
            ),
            "realtime_quote_failure_count": self._realtime_quote_failure_count,
            "thresholds_seconds": _LATENESS_THRESHOLDS_SECONDS,
            "streams": {
                stream.value: counters.snapshot()
                for stream, counters in sorted(
                    self._lateness_by_stream.items(),
                    key=lambda item: item[0].value,
                )
            },
            "aggregation": {
                "processing_count": metrics.aggregation_processing_count,
                "processing_average_ms": round(
                    metrics.aggregation_processing_average_ms,
                    6,
                ),
                "processing_max_ms": round(
                    metrics.aggregation_processing_max_ms,
                    6,
                ),
                "active_bucket_count": metrics.active_bucket_count,
                "active_bucket_high_watermark": (
                    metrics.active_bucket_high_watermark
                ),
            },
            "completeness": {
                "incomplete_active_bucket_count": (
                    metrics.incomplete_active_bucket_count
                ),
                "incomplete_gap_count": metrics.incomplete_gap_count,
                "missing_agg_trade_count": metrics.missing_agg_trade_count,
                "late_recovered_event_count": (
                    metrics.late_recovered_event_count
                ),
            },
        }

    async def observe_realtime_quote(self, envelope: RawEnvelope) -> None:
        """Publish a book-ticker quote before capture coalescing/archive I/O."""
        if (
            self._realtime_quote_sink is None
            or envelope.stream is not CaptureStream.BOOK_TICKER
        ):
            return
        try:
            event = normalize_binance_envelope(envelope)
            if not isinstance(event, NormalizedBookTicker):
                return
            await self._realtime_quote_sink(
                RealtimeMarketQuote(
                    exchange=event.exchange,
                    environment=event.environment,
                    symbol=event.symbol,
                    event_at=event.event_at,
                    received_at=event.received_at,
                    bid_price=event.bid_price,
                    ask_price=event.ask_price,
                )
            )
        except Exception:
            # A quote consumer outage must not stop the primary capture
            # connection. The state and durable paths remain independent.
            self._realtime_quote_failure_count += 1

    async def mark_incomplete(self, gap: AggTradeGap) -> None:
        self._incomplete_gap_count += 1
        self._missing_agg_trade_count += gap.missing_count
        previous_bucket = bucket_start_15s(gap.previous_event_at)
        current_bucket = bucket_start_15s(gap.current_event_at)
        bucket = min(previous_bucket, current_bucket)
        final_bucket = max(previous_bucket, current_bucket)
        while bucket <= final_bucket:
            key = (gap.environment, gap.symbol, bucket)
            if (
                key in self._accumulators_by_bucket
                or self._latest_durable_watermark_at is None
                or bucket + timedelta(seconds=_BUCKET_SECONDS)
                > self._latest_durable_watermark_at
            ):
                self._incomplete_buckets.setdefault(key, 0)
            bucket += timedelta(seconds=_BUCKET_SECONDS)
        current_key = (
            gap.environment,
            gap.symbol,
            current_bucket,
        )
        if current_key in self._incomplete_buckets:
            self._incomplete_buckets[current_key] += gap.missing_count
        if self._durable_queue is None:
            await self._persist_gap(gap)
        else:
            # State inserts and invalidations share one FIFO actor. A late gap
            # therefore cannot race ahead of the row it needs to invalidate.
            await self._durable_queue.put(gap)

    async def observe(self, envelope: RawEnvelope) -> None:
        started_at = time.perf_counter()
        try:
            await self._observe(envelope)
        finally:
            elapsed = time.perf_counter() - started_at
            self._aggregation_processing_count += 1
            self._aggregation_processing_seconds += elapsed
            self._aggregation_processing_max_seconds = max(
                self._aggregation_processing_max_seconds,
                elapsed,
            )

    async def _observe(self, envelope: RawEnvelope) -> None:
        self._received_envelope_count += 1
        counters = self._lateness_by_stream.setdefault(
            envelope.stream,
            _EventLatenessCounters(),
        )
        counters.observe_raw(envelope)
        try:
            event = normalize_binance_envelope(envelope)
        except BinanceNormalizationError:
            self._rejected_envelope_count += 1
            return

        self._normalized_event_count += 1
        if (
            self._max_seen_event_at is None
            or event.event_at > self._max_seen_event_at
        ):
            self._max_seen_event_at = event.event_at
        realtime_watermark = self._max_seen_event_at - timedelta(
            seconds=self._config.realtime_closure_delay_seconds
        )
        durable_watermark = self._max_seen_event_at - timedelta(
            seconds=self._config.durable_closure_delay_seconds
        )
        self._latest_watermark_at = realtime_watermark
        self._latest_durable_watermark_at = durable_watermark

        key = _bucket_key(event)
        bucket_end = key[2] + timedelta(seconds=_BUCKET_SECONDS)
        simulated_close_drops = tuple(
            bucket_end
            <= self._max_seen_event_at - timedelta(seconds=threshold_seconds)
            for threshold_seconds in _LATENESS_THRESHOLDS_SECONDS
        )
        counters.observe_normalized(
            simulated_close_drops=simulated_close_drops
        )
        # The durable watermark is the point after which the event can no
        # longer be incorporated into the audit row. Events between the two
        # clocks are deliberately accepted after realtime publication and are
        # folded into the durable state below.
        if bucket_end <= durable_watermark:
            self._late_event_count += 1
            if envelope.recovered and isinstance(event, NormalizedAggTrade):
                self._late_recovered_event_count += 1
                try:
                    aggregate_trade_id = int(event.trade_id)
                except ValueError:
                    aggregate_trade_id = 0
                if aggregate_trade_id > 0:
                    await self.mark_incomplete(
                        AggTradeGap(
                            environment=event.environment,
                            symbol=event.symbol,
                            previous_id=aggregate_trade_id - 1,
                            current_id=aggregate_trade_id,
                            previous_event_at=event.event_at,
                            current_event_at=event.event_at,
                            missing_count=1,
                            reason="late_recovery_after_durable_close",
                        )
                    )
            return

        accumulator = self._accumulators_by_bucket.get(key)
        if accumulator is None:
            accumulator = MarketState15sAccumulator.for_bucket(event)
            self._accumulators_by_bucket[key] = accumulator
            self._active_bucket_high_watermark = max(
                self._active_bucket_high_watermark,
                len(self._accumulators_by_bucket),
            )
            deadline = _bucket_deadline(key)
            heapq.heappush(self._realtime_deadlines, deadline)
            heapq.heappush(self._durable_deadlines, deadline)

        if isinstance(event, NormalizedBookTicker):
            # A closed 15-second state only needs the latest executable quote
            # for each symbol. Keeping every quote here turns a harmless
            # high-frequency stream into an unbounded in-memory event list and
            # makes the downstream database/archive path compete with the
            # WebSocket reader.
            self._latest_book_ticker_by_bucket[key] = event
        else:
            accumulator.observe(event)
        await self._close_ready_buckets(
            realtime_watermark=realtime_watermark,
            durable_watermark=durable_watermark,
        )

    async def _close_ready_buckets(
        self,
        *,
        realtime_watermark: datetime,
        durable_watermark: datetime,
    ) -> None:
        realtime_ready_keys = _pop_ready_keys(
            self._realtime_deadlines,
            watermark=realtime_watermark,
            active_keys=self._accumulators_by_bucket,
        )
        if realtime_ready_keys:
            realtime_snapshots = self._build_snapshots(
                realtime_ready_keys,
                latest_quotes=self._realtime_latest_quotes,
            )
            realtime_states = tuple(
                snapshot.state for snapshot in realtime_snapshots
            )
            for state in realtime_states:
                if (
                    state.last_bid_price is not None
                    and state.last_ask_price is not None
                ):
                    self._realtime_latest_quotes[(state.environment, state.symbol)] = (
                        state.last_bid_price,
                        state.last_ask_price,
                    )
            if self._realtime_state_sink is not None and realtime_states:
                try:
                    await self._realtime_state_sink(realtime_states)
                    self._realtime_batch_count += 1
                except Exception:
                    self._realtime_sink_failure_count += 1

        durable_ready_keys = _pop_ready_keys(
            self._durable_deadlines,
            watermark=durable_watermark,
            active_keys=self._accumulators_by_bucket,
        )
        self._prune_orphaned_incomplete_buckets(durable_watermark)
        if not durable_ready_keys:
            return

        durable_snapshots = self._build_snapshots(
            durable_ready_keys,
            latest_quotes=self._durable_latest_quotes,
        )
        states_tuple = tuple(
            snapshot.state for snapshot in durable_snapshots
        )
        for key in durable_ready_keys:
            self._accumulators_by_bucket.pop(key, None)
            self._latest_book_ticker_by_bucket.pop(key, None)
            self._incomplete_buckets.pop(key, None)
        for state in states_tuple:
            if (
                state.last_bid_price is not None
                and state.last_ask_price is not None
            ):
                self._durable_latest_quotes[(state.environment, state.symbol)] = (
                    state.last_bid_price,
                    state.last_ask_price,
                )
        if not durable_snapshots:
            return

        sequence_range = RuntimeStateSequenceRange(
            minimum=min(
                snapshot.input_sequence_min
                for snapshot in durable_snapshots
            ),
            maximum=max(
                snapshot.input_sequence_max
                for snapshot in durable_snapshots
            ),
        )
        durable_batch = (states_tuple, durable_watermark, sequence_range)
        if self._durable_queue is None:
            await self._persist_batch(durable_batch)
        else:
            await self._durable_queue.put(durable_batch)
        self._closed_state_count += len(states_tuple)

    def _build_snapshots(
        self,
        keys: tuple[_BucketKey, ...],
        *,
        latest_quotes: dict[tuple[str, str], tuple[Decimal, Decimal]],
    ) -> tuple[MarketState15sSnapshot, ...]:
        snapshots: list[MarketState15sSnapshot] = []
        for key in keys:
            accumulator = self._accumulators_by_bucket.get(key)
            if accumulator is None:
                continue
            latest_book_ticker = self._latest_book_ticker_by_bucket.get(key)
            snapshot = accumulator.snapshot(
                initial_quote=latest_quotes.get((key[0], key[1])),
                latest_quote=latest_book_ticker,
                data_complete=key not in self._incomplete_buckets,
                missing_agg_trade_count=self._incomplete_buckets.get(key, 0),
            )
            snapshots.append(snapshot)
            state = snapshot.state
            if (
                state.last_bid_price is not None
                and state.last_ask_price is not None
            ):
                latest_quotes[(state.environment, state.symbol)] = (
                    state.last_bid_price,
                    state.last_ask_price,
                )
        return tuple(
            sorted(
                snapshots,
                key=lambda snapshot: (
                    snapshot.state.bucket_start,
                    snapshot.state.symbol,
                ),
            )
        )

    def _prune_orphaned_incomplete_buckets(
        self,
        durable_watermark: datetime,
    ) -> None:
        for key in tuple(self._incomplete_buckets):
            if (
                key not in self._accumulators_by_bucket
                and key[2] + timedelta(seconds=_BUCKET_SECONDS)
                <= durable_watermark
            ):
                self._incomplete_buckets.pop(key, None)

    async def _persist_loop(
        self,
        queue: asyncio.Queue[_DurableCommand | None],
    ) -> None:
        while True:
            command = await queue.get()
            if command is None:
                queue.task_done()
                return
            try:
                if isinstance(command, AggTradeGap):
                    await self._persist_gap(command)
                else:
                    await self._persist_batch(command)
            finally:
                queue.task_done()

    async def _persist_batch(self, batch: _DurableBatch) -> None:
        states, watermark, sequence_range = batch
        while True:
            try:
                await self._repository.save_closed_states(
                    states,
                    source_watermark_at=watermark,
                    sequence_range=sequence_range,
                )
                return
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._durable_sink_failure_count += 1
                self._log.exception(
                    "runtime_state_persistence_failed",
                    error=str(error),
                    retry_seconds=self._config.persistence_retry_seconds,
                )
                await asyncio.sleep(self._config.persistence_retry_seconds)

    async def _persist_gap(self, gap: AggTradeGap) -> None:
        while True:
            try:
                await self._repository.mark_incomplete(gap)
                return
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._durable_sink_failure_count += 1
                self._log.exception(
                    "runtime_state_gap_persistence_failed",
                    symbol=gap.symbol,
                    previous_id=gap.previous_id,
                    current_id=gap.current_id,
                    error=str(error),
                    retry_seconds=self._config.persistence_retry_seconds,
                )
                await asyncio.sleep(self._config.persistence_retry_seconds)


def _bucket_deadline(key: _BucketKey) -> _BucketDeadline:
    return (
        key[2] + timedelta(seconds=_BUCKET_SECONDS),
        key[1],
        key[0],
        key,
    )


def _pop_ready_keys(
    deadlines: list[_BucketDeadline],
    *,
    watermark: datetime,
    active_keys: dict[_BucketKey, MarketState15sAccumulator],
) -> tuple[_BucketKey, ...]:
    ready: list[_BucketKey] = []
    while deadlines and deadlines[0][0] <= watermark:
        *_, key = heapq.heappop(deadlines)
        if key in active_keys:
            ready.append(key)
    return tuple(ready)


def _bucket_key(event: NormalizedMarketEvent) -> _BucketKey:
    return (
        event.environment,
        event.symbol,
        bucket_start_15s(event.event_at),
    )
