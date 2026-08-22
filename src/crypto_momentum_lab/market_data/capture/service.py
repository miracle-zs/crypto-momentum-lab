import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol

from crypto_momentum_lab.domain.market.models import (
    CaptureStream,
    MarketDataState,
    RawEnvelope,
)
from crypto_momentum_lab.market_data.capture.queue import (
    BoundedEnvelopeQueue,
    CaptureQueueFull,
)


class DiskStatus(StrEnum):
    HEALTHY = "healthy"
    WARNING = "warning"
    HALT = "halt"


class DiskSpaceGuard:
    def __init__(
        self,
        *,
        warning_free_bytes: int,
        halt_free_bytes: int,
        recovery_free_bytes: int,
    ) -> None:
        self._warning_free_bytes = warning_free_bytes
        self._halt_free_bytes = halt_free_bytes
        self._recovery_free_bytes = recovery_free_bytes
        self._halted = False

    def evaluate(self, free_bytes: int) -> DiskStatus:
        if free_bytes <= self._halt_free_bytes:
            self._halted = True
            return DiskStatus.HALT
        if self._halted:
            if free_bytes < self._recovery_free_bytes:
                return DiskStatus.HALT
            self._halted = False
            return DiskStatus.HEALTHY
        if free_bytes < self._warning_free_bytes:
            return DiskStatus.WARNING
        return DiskStatus.HEALTHY


@dataclass(frozen=True, slots=True)
class CaptureMetricsSnapshot:
    state: MarketDataState
    monitoring_generation: int
    monitoring_symbols: int
    desired_subscriptions: int
    active_subscriptions: int
    active_connections: int
    reconnect_count: int
    received_messages: int
    received_bytes: int
    queue_events: int
    queue_bytes: int
    archived_rows: int
    archived_bytes: int
    open_writers: int
    pending_manifests: int
    oldest_pending_manifest_seconds: float | None
    disk_free_bytes: int
    queue_coalesced_replacements: int = 0
    queue_dropped_events: int = 0
    queue_pending_coalesced_events: int = 0


class CaptureStateRepository(Protocol):
    async def save_process_state(
        self,
        *,
        state: MarketDataState,
        occurred_at: datetime,
        reason: str | None,
    ) -> None: ...


class CaptureConnectionPool(Protocol):
    async def apply_symbols(
        self,
        symbols: frozenset[str],
        *,
        streams: tuple[CaptureStream, ...],
        generation: int,
    ) -> None: ...

    async def stop(self) -> None: ...


class CaptureRunner(Protocol):
    async def run(self) -> None: ...

    async def stop(self) -> None: ...


class MarketDataCaptureService:
    def __init__(
        self,
        *,
        queue: BoundedEnvelopeQueue,
        repository: CaptureStateRepository,
        connection_pool: CaptureConnectionPool,
        disk_guard: DiskSpaceGuard,
        coordinator: CaptureRunner | None = None,
    ) -> None:
        self._queue = queue
        self._repository = repository
        self._connection_pool = connection_pool
        self._disk_guard = disk_guard
        self._coordinator = coordinator
        self._state = MarketDataState.STARTING
        self._monitoring_generation = 0
        self._monitoring_symbols = 0
        self._desired_subscriptions = 0
        self._active_subscriptions = 0
        self._active_connections = 0
        self._reconnect_count = 0
        self._received_messages = 0
        self._received_bytes = 0
        self._archived_rows = 0
        self._archived_bytes = 0
        self._open_writers = 0
        self._pending_manifests = 0
        self._oldest_pending_manifest_seconds: float | None = None
        self._disk_free_bytes = 0

    @property
    def state(self) -> MarketDataState:
        return self._state

    async def start(
        self,
        *,
        symbols: frozenset[str],
        streams: tuple[CaptureStream, ...],
        generation: int,
    ) -> None:
        await self.apply_symbols(
            symbols,
            streams=streams,
            generation=generation,
        )
        await self._transition(MarketDataState.READY, reason=None)

    async def run(self) -> None:
        if self._coordinator is None:
            await asyncio.Event().wait()
        else:
            await self._coordinator.run()

    async def stop(self) -> None:
        await self._connection_pool.stop()
        if self._coordinator is not None:
            await self._coordinator.stop()
        await self._transition(MarketDataState.STOPPED, reason=None)

    async def submit(self, envelope: RawEnvelope) -> None:
        try:
            await self._queue.put_nowait(envelope)
        except CaptureQueueFull:
            await self._transition(
                MarketDataState.HALTED,
                reason="capture queue overflow",
            )
            raise

    async def apply_symbols(
        self,
        symbols: frozenset[str],
        *,
        streams: tuple[CaptureStream, ...],
        generation: int,
    ) -> None:
        await self._connection_pool.apply_symbols(
            symbols,
            streams=streams,
            generation=generation,
        )
        self._monitoring_generation = generation
        self._monitoring_symbols = len(symbols)
        self._desired_subscriptions = len(symbols) * len(streams)

    def metrics_snapshot(self) -> CaptureMetricsSnapshot:
        return CaptureMetricsSnapshot(
            state=self._state,
            monitoring_generation=self._monitoring_generation,
            monitoring_symbols=self._monitoring_symbols,
            desired_subscriptions=self._desired_subscriptions,
            active_subscriptions=self._active_subscriptions,
            active_connections=self._active_connections,
            reconnect_count=self._reconnect_count,
            received_messages=self._received_messages,
            received_bytes=self._received_bytes,
            queue_events=self._queue.size,
            queue_bytes=self._queue.current_bytes,
            queue_coalesced_replacements=self._queue.coalesced_replacements,
            queue_dropped_events=self._queue.dropped_events,
            queue_pending_coalesced_events=(
                self._queue.pending_coalesced_events
            ),
            archived_rows=self._archived_rows,
            archived_bytes=self._archived_bytes,
            open_writers=self._open_writers,
            pending_manifests=self._pending_manifests,
            oldest_pending_manifest_seconds=(
                self._oldest_pending_manifest_seconds
            ),
            disk_free_bytes=self._disk_free_bytes,
        )

    async def _transition(
        self,
        state: MarketDataState,
        *,
        reason: str | None,
    ) -> None:
        self._state = state
        await self._repository.save_process_state(
            state=state,
            occurred_at=datetime.now(UTC),
            reason=reason,
        )
