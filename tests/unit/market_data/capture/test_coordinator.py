import asyncio
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    CaptureStream,
    ConnectionLifecycleEvent,
    DurableArchiveAcknowledgement,
    QualityCategory,
    QualityEvent,
    RawEnvelope,
)
from crypto_momentum_lab.market_data.capture.coordinator import (
    CaptureCoordinator,
)
from crypto_momentum_lab.market_data.capture.queue import BoundedEnvelopeQueue


class ControlledArchive:
    def __init__(self) -> None:
        self._append_started = asyncio.Event()
        self._append_released = asyncio.Event()

    async def append(
        self,
        envelope: RawEnvelope,
    ) -> DurableArchiveAcknowledgement:
        self._append_started.set()
        await self._append_released.wait()
        return DurableArchiveAcknowledgement(
            connection_session_id=envelope.connection_session_id,
            local_sequence=envelope.local_sequence,
            relative_path=Path("raw.jsonl.zst"),
            committed_at=envelope.received_at,
        )

    async def close(self) -> None:
        return None

    async def wait_until_append_started(self) -> None:
        await asyncio.wait_for(self._append_started.wait(), timeout=1)

    def release_append(self) -> None:
        self._append_released.set()


class FakeQualityTracker:
    def observe(self, envelope: RawEnvelope) -> tuple[QualityEvent, ...]:
        return ()

    def observe_lifecycle(
        self,
        event: ConnectionLifecycleEvent,
    ) -> tuple[QualityEvent, ...]:
        return (
            QualityEvent(
                event_id=UUID(int=10),
                category=QualityCategory.CONNECTION_OPENED,
                occurred_at=event.occurred_at,
                route=event.route,
                stream=event.stream,
                symbol=event.symbols[0],
                connection_session_id=event.session_id,
                local_sequence=None,
                details={},
            ),
        )


class FakeCaptureRepository:
    def __init__(self) -> None:
        self.quality_events: list[QualityEvent] = []

    async def save_quality_event(self, event: QualityEvent) -> None:
        self.quality_events.append(event)

    async def save_process_state(self, **kwargs: Any) -> None:
        return None


async def test_ack_is_emitted_only_after_archive_returns(
    raw_envelope: RawEnvelope,
) -> None:
    archive = ControlledArchive()
    acknowledgements: list[DurableArchiveAcknowledgement] = []
    coordinator = CaptureCoordinator(
        queue=BoundedEnvelopeQueue(max_events=10, max_bytes=100000),
        archive=archive,
        quality=FakeQualityTracker(),
        repository=FakeCaptureRepository(),
        acknowledgement_sink=acknowledgements.append,
    )

    task = asyncio.create_task(coordinator.run())
    await coordinator.submit(raw_envelope)
    await archive.wait_until_append_started()
    assert acknowledgements == []

    archive.release_append()
    await coordinator.stop()
    await task
    assert acknowledgements[0].local_sequence == 1


async def test_lifecycle_events_are_persisted() -> None:
    repository = FakeCaptureRepository()
    coordinator = CaptureCoordinator(
        queue=BoundedEnvelopeQueue(max_events=10, max_bytes=100000),
        archive=ControlledArchive(),
        quality=FakeQualityTracker(),
        repository=repository,
        acknowledgement_sink=None,
    )

    await coordinator.observe_lifecycle(
        ConnectionLifecycleEvent(
            session_id=UUID(int=1),
            route=CaptureRoute.MARKET,
            stream=CaptureStream.AGG_TRADE,
            symbols=("BTCUSDT",),
            occurred_at=datetime(2026, 6, 15, 2, 0, tzinfo=UTC),
            opened=True,
            reason=None,
        )
    )

    assert repository.quality_events[0].category is (
        QualityCategory.CONNECTION_OPENED
    )
