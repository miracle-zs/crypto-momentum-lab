import asyncio
import inspect
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol

from crypto_momentum_lab.domain.market.models import (
    DurableArchiveAcknowledgement,
    MarketDataState,
    QualityEvent,
    RawEnvelope,
)
from crypto_momentum_lab.domain.market.ports import RawArchive
from crypto_momentum_lab.market_data.capture.queue import BoundedEnvelopeQueue


class QualityTracker(Protocol):
    def observe(self, envelope: RawEnvelope) -> tuple[QualityEvent, ...]: ...


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


class CaptureCoordinator:
    def __init__(
        self,
        *,
        queue: BoundedEnvelopeQueue,
        archive: RawArchive,
        quality: QualityTracker,
        repository: QualityRepository,
        acknowledgement_sink: AcknowledgementSink | None = None,
    ) -> None:
        self._queue = queue
        self._archive = archive
        self._quality = quality
        self._repository = repository
        self._acknowledgement_sink = acknowledgement_sink
        self._stopping = False

    async def submit(self, envelope: RawEnvelope) -> None:
        await self._queue.put_nowait(envelope)

    async def run(self) -> None:
        while not self._stopping or self._queue.size:
            try:
                envelope = await asyncio.wait_for(self._queue.get(), timeout=0.1)
            except TimeoutError:
                continue
            try:
                quality_events = self._quality.observe(envelope)
                acknowledgement = await self._archive.append(envelope)
                for event in quality_events:
                    await self._repository.save_quality_event(event)
                if self._acknowledgement_sink is not None:
                    result = self._acknowledgement_sink(acknowledgement)
                    if inspect.isawaitable(result):
                        await result
            except Exception as error:
                await self._halt(f"archive failure: {error}")
                raise
            finally:
                self._queue.task_done(envelope)

    async def stop(self) -> None:
        self._stopping = True
        await self._queue.join()

    async def _halt(self, reason: str) -> None:
        await self._repository.save_process_state(
            state=MarketDataState.HALTED,
            occurred_at=datetime.now(UTC),
            reason=reason,
        )
