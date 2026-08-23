import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Protocol

import structlog

from crypto_momentum_lab.domain.market.models import (
    CaptureStream,
    MarketState15s,
    NormalizedBookTicker,
    NormalizedMarketEvent,
    RawEnvelope,
)
from crypto_momentum_lab.market_data.aggregation import (
    aggregate_market_states_15s,
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
type _DurableBatch = tuple[
    tuple[MarketState15s, ...],
    datetime,
    RuntimeStateSequenceRange,
]

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


type RealtimeStateSink = Callable[[tuple[MarketState15s, ...]], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class ClosedMarketStatePublisherConfig:
    closure_delay_seconds: float = 3.0
    persistence_queue_size: int = 128
    persistence_retry_seconds: float = 1.0

    def __post_init__(self) -> None:
        if self.closure_delay_seconds <= 0:
            raise ValueError("closure_delay_seconds must be positive")
        if self.persistence_queue_size <= 0:
            raise ValueError("persistence_queue_size must be positive")
        if self.persistence_retry_seconds <= 0:
            raise ValueError("persistence_retry_seconds must be positive")


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
    ) -> None:
        self._repository = repository
        self._realtime_state_sink = realtime_state_sink
        self._config = (
            ClosedMarketStatePublisherConfig()
            if config is None
            else config
        )
        self._events_by_bucket: dict[
            _BucketKey,
            list[NormalizedMarketEvent],
        ] = {}
        self._latest_book_ticker_by_bucket: dict[
            _BucketKey,
            NormalizedBookTicker,
        ] = {}
        self._latest_quotes: dict[tuple[str, str], tuple[Decimal, Decimal]] = {}
        self._max_seen_event_at: datetime | None = None
        self._latest_watermark_at: datetime | None = None
        self._received_envelope_count = 0
        self._normalized_event_count = 0
        self._closed_state_count = 0
        self._rejected_envelope_count = 0
        self._late_event_count = 0
        self._realtime_batch_count = 0
        self._realtime_sink_failure_count = 0
        self._durable_sink_failure_count = 0
        self._lateness_by_stream: dict[
            CaptureStream,
            _EventLatenessCounters,
        ] = {}
        self._durable_queue: asyncio.Queue[_DurableBatch | None] | None = None
        self._durable_task: asyncio.Task[None] | None = None
        self._log = structlog.get_logger()

    async def start(self) -> None:
        if self._durable_task is not None:
            return
        self._durable_queue = asyncio.Queue(
            maxsize=self._config.persistence_queue_size
        )
        self._durable_task = asyncio.create_task(self._persist_loop())

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
        )

    def lateness_metrics_snapshot(self) -> dict[str, object]:
        """Return bounded transport and close-threshold counters by stream.

        ``received_over_threshold_count`` measures transport lateness directly
        from ``received_at - exchange_event_at``.  ``simulated_close_drop_count``
        replays the current watermark rule with each candidate delay, so the
        two counters do not conflate a late packet with a packet that would
        actually arrive after a state bucket had already been closed.
        """
        return {
            "configured_closure_delay_seconds": (
                self._config.closure_delay_seconds
            ),
            "thresholds_seconds": _LATENESS_THRESHOLDS_SECONDS,
            "streams": {
                stream.value: counters.snapshot()
                for stream, counters in sorted(
                    self._lateness_by_stream.items(),
                    key=lambda item: item[0].value,
                )
            },
        }

    async def observe(self, envelope: RawEnvelope) -> None:
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
        watermark = self._max_seen_event_at - timedelta(
            seconds=self._config.closure_delay_seconds
        )
        self._latest_watermark_at = watermark

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
        if bucket_end <= watermark:
            self._late_event_count += 1
            return

        if isinstance(event, NormalizedBookTicker):
            # A closed 15-second state only needs the latest executable quote
            # for each symbol. Keeping every quote here turns a harmless
            # high-frequency stream into an unbounded in-memory event list and
            # makes the downstream database/archive path compete with the
            # WebSocket reader.
            self._latest_book_ticker_by_bucket[key] = event
        else:
            self._events_by_bucket.setdefault(key, []).append(event)
        await self._close_ready_buckets(watermark)

    async def _close_ready_buckets(self, watermark: datetime) -> None:
        pending_keys = set(self._events_by_bucket)
        pending_keys.update(self._latest_book_ticker_by_bucket)
        ready_keys = tuple(
            key
            for key in sorted(
                pending_keys,
                key=lambda item: (item[2], item[1], item[0]),
            )
            if key[2] + timedelta(seconds=_BUCKET_SECONDS) <= watermark
        )
        if not ready_keys:
            return

        states: list[MarketState15s] = []
        closed_events: list[NormalizedMarketEvent] = []
        for key in ready_keys:
            events = tuple(self._events_by_bucket.pop(key, ()))
            latest_book_ticker = self._latest_book_ticker_by_bucket.pop(
                key,
                None,
            )
            if latest_book_ticker is not None:
                events = (*events, latest_book_ticker)
            closed_events.extend(events)
            environment = events[0].environment
            initial_quotes = {
                symbol: quote
                for (quote_environment, symbol), quote in self._latest_quotes.items()
                if quote_environment == environment
            }
            batch_states = aggregate_market_states_15s(
                events,
                initial_quotes=initial_quotes,
            )
            states.extend(batch_states)
            for state in batch_states:
                if (
                    state.last_bid_price is not None
                    and state.last_ask_price is not None
                ):
                    self._latest_quotes[(state.environment, state.symbol)] = (
                        state.last_bid_price,
                        state.last_ask_price,
                    )
        states_tuple = tuple(
            sorted(states, key=lambda state: (state.bucket_start, state.symbol))
        )
        sequence_range = RuntimeStateSequenceRange(
            minimum=min(event.source_local_sequence for event in closed_events),
            maximum=max(event.source_local_sequence for event in closed_events),
        )

        # The realtime sink is intentionally before the durable adapter. A
        # slow database write must not put the live decision path behind the
        # historical runtime-state backlog. The database remains the
        # audit/recovery path if a consumer disconnects.
        if self._realtime_state_sink is not None:
            try:
                await self._realtime_state_sink(states_tuple)
                self._realtime_batch_count += 1
            except Exception:
                self._realtime_sink_failure_count += 1

        durable_batch = (states_tuple, watermark, sequence_range)
        if self._durable_queue is None:
            await self._persist_batch(durable_batch)
        else:
            await self._durable_queue.put(durable_batch)
        self._closed_state_count += len(states_tuple)

    async def _persist_loop(self) -> None:
        queue = self._durable_queue
        if queue is None:
            raise RuntimeError("durable persistence queue is not initialized")
        while True:
            batch = await queue.get()
            if batch is None:
                queue.task_done()
                return
            try:
                await self._persist_batch(batch)
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


def _bucket_key(event: NormalizedMarketEvent) -> _BucketKey:
    return (
        event.environment,
        event.symbol,
        bucket_start_15s(event.event_at),
    )
