import asyncio
from pathlib import Path
from typing import Any

from crypto_momentum_lab.domain.market.models import (
    DurableArchiveAcknowledgement,
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


class FakeCaptureRepository:
    async def save_quality_event(self, event: QualityEvent) -> None:
        return None

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
