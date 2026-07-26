from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from crypto_momentum_lab.domain.market.models import (
    MarketState15s,
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

_BUCKET_SECONDS = 15


class ClosedStateRepository(Protocol):
    async def save_closed_states(
        self,
        states: tuple[MarketState15s, ...],
        *,
        source_watermark_at: datetime,
        sequence_range: RuntimeStateSequenceRange,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class ClosedMarketStatePublisherConfig:
    closure_delay_seconds: float = 3.0

    def __post_init__(self) -> None:
        if self.closure_delay_seconds <= 0:
            raise ValueError("closure_delay_seconds must be positive")


@dataclass(frozen=True, slots=True)
class ClosedMarketStatePublisherMetrics:
    received_envelope_count: int
    normalized_event_count: int
    closed_state_count: int
    rejected_envelope_count: int
    late_event_count: int
    latest_watermark_at: datetime | None


class ClosedMarketStatePublisher:
    def __init__(
        self,
        *,
        repository: ClosedStateRepository,
        config: ClosedMarketStatePublisherConfig | None = None,
    ) -> None:
        self._repository = repository
        self._config = (
            ClosedMarketStatePublisherConfig()
            if config is None
            else config
        )
        self._events_by_bucket: dict[
            _BucketKey,
            list[NormalizedMarketEvent],
        ] = {}
        self._closed_buckets: set[_BucketKey] = set()
        self._max_seen_event_at: datetime | None = None
        self._latest_watermark_at: datetime | None = None
        self._received_envelope_count = 0
        self._normalized_event_count = 0
        self._closed_state_count = 0
        self._rejected_envelope_count = 0
        self._late_event_count = 0

    @property
    def metrics(self) -> ClosedMarketStatePublisherMetrics:
        return ClosedMarketStatePublisherMetrics(
            received_envelope_count=self._received_envelope_count,
            normalized_event_count=self._normalized_event_count,
            closed_state_count=self._closed_state_count,
            rejected_envelope_count=self._rejected_envelope_count,
            late_event_count=self._late_event_count,
            latest_watermark_at=self._latest_watermark_at,
        )

    async def observe(self, envelope: RawEnvelope) -> None:
        self._received_envelope_count += 1
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
        if key in self._closed_buckets:
            self._late_event_count += 1
            return

        self._events_by_bucket.setdefault(key, []).append(event)
        await self._close_ready_buckets(watermark)

    async def _close_ready_buckets(self, watermark: datetime) -> None:
        ready_keys = tuple(
            key
            for key in sorted(
                self._events_by_bucket,
                key=lambda item: (item[2], item[1], item[0]),
            )
            if key[2] + timedelta(seconds=_BUCKET_SECONDS) <= watermark
        )
        if not ready_keys:
            return

        states: list[MarketState15s] = []
        closed_events: list[NormalizedMarketEvent] = []
        for key in ready_keys:
            events = tuple(self._events_by_bucket[key])
            closed_events.extend(events)
            states.extend(aggregate_market_states_15s(events))
        states_tuple = tuple(
            sorted(states, key=lambda state: (state.bucket_start, state.symbol))
        )
        sequence_range = RuntimeStateSequenceRange(
            minimum=min(event.source_local_sequence for event in closed_events),
            maximum=max(event.source_local_sequence for event in closed_events),
        )

        await self._repository.save_closed_states(
            states_tuple,
            source_watermark_at=watermark,
            sequence_range=sequence_range,
        )
        for key in ready_keys:
            self._closed_buckets.add(key)
            del self._events_by_bucket[key]
        self._closed_state_count += len(states_tuple)


def _bucket_key(event: NormalizedMarketEvent) -> _BucketKey:
    return (
        event.environment,
        event.symbol,
        bucket_start_15s(event.event_at),
    )
