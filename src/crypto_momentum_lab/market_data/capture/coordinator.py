import asyncio
import inspect
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol

from crypto_momentum_lab.domain.market.models import (
    ConnectionLifecycleEvent,
    DurableArchiveAcknowledgement,
    MarketDataState,
    QualityEvent,
    RawEnvelope,
)
from crypto_momentum_lab.domain.market.ports import RawArchive
from crypto_momentum_lab.market_data.capture.queue import BoundedEnvelopeQueue


class QualityTracker(Protocol):
    def observe(self, envelope: RawEnvelope) -> tuple[QualityEvent, ...]: ...

    def observe_lifecycle(
        self,
        event: ConnectionLifecycleEvent,
    ) -> tuple[QualityEvent, ...]: ...


class QualityRepository(Protocol):
    async def save_quality_event(self, event: QualityEvent) -> None: ...

    async def save_process_state(
        self,
        *,
        state: MarketDataState,
        occurred_at: datetime,
        reason: str | None,
    ) -> None: ...


type AcknowledgementSink = Callable[[DurableArchiveAcknowledgement], object]
type ArchivedEnvelopeSink = Callable[[RawEnvelope], object]

_MAX_ARCHIVE_BATCH_SIZE = 10000


class CaptureCoordinator:
    def __init__(
        self,
        *,
        queue: BoundedEnvelopeQueue,
        archive: RawArchive,
        quality: QualityTracker,
        repository: QualityRepository,
        acknowledgement_sink: AcknowledgementSink | None = None,
        archived_envelope_sink: ArchivedEnvelopeSink | None = None,
    ) -> None:
        self._queue = queue
        self._archive = archive
        self._quality = quality
        self._repository = repository
        self._acknowledgement_sink = acknowledgement_sink
        self._archived_envelope_sink = archived_envelope_sink
        self._stopping = False

    async def submit(self, envelope: RawEnvelope) -> None:
        await self._queue.put_nowait(envelope)

    async def observe_lifecycle(
        self,
        event: ConnectionLifecycleEvent,
    ) -> None:
        for quality_event in self._quality.observe_lifecycle(event):
            await self._repository.save_quality_event(quality_event)

    async def run(self) -> None:
        while not self._stopping or self._queue.size:
            try:
                envelope = await asyncio.wait_for(self._queue.get(), timeout=0.1)
            except TimeoutError:
                continue
            batch = [envelope]
            while len(batch) < _MAX_ARCHIVE_BATCH_SIZE:
                next_envelope = self._queue.get_nowait()
                if next_envelope is None:
                    break
                batch.append(next_envelope)
            await self._process_batch(tuple(batch))

    async def _process_batch(self, batch: tuple[RawEnvelope, ...]) -> None:
        try:
            tasks = tuple(
                asyncio.create_task(self._process_envelope(envelope))
                for envelope in batch
            )
            results = await asyncio.gather(*tasks, return_exceptions=True)

            failures = tuple(
                result for result in results if isinstance(result, Exception)
            )
            if failures:
                reason = f"archive failure: {failures[0]}"
                await self._halt(reason)
                raise failures[0]

            await self._publish_archived_batch(batch)
        finally:
            for envelope in batch:
                self._queue.task_done(envelope)

    async def _process_envelope(self, envelope: RawEnvelope) -> None:
        quality_events = self._quality.observe(envelope)
        acknowledgement = await self._archive.append(envelope)
        for event in quality_events:
            await self._repository.save_quality_event(event)
        if self._acknowledgement_sink is not None:
            result = self._acknowledgement_sink(acknowledgement)
            if inspect.isawaitable(result):
                await result

    async def _publish_archived_batch(
        self,
        batch: tuple[RawEnvelope, ...],
    ) -> None:
        if self._archived_envelope_sink is None:
            return
        for envelope in batch:
            result = self._archived_envelope_sink(envelope)
            if inspect.isawaitable(result):
                await result

    async def stop(self) -> None:
        self._stopping = True
        await self._queue.join()
        await self._archive.close()

    async def _halt(self, reason: str) -> None:
        await self._repository.save_process_state(
            state=MarketDataState.HALTED,
            occurred_at=datetime.now(UTC),
            reason=reason,
        )
