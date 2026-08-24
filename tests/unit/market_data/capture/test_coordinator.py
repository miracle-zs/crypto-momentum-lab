import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from crypto_momentum_lab.domain.market.models import (
    AggTradeGap,
    CaptureRoute,
    CaptureStream,
    ConnectionLifecycleEvent,
    DurableArchiveAcknowledgement,
    QualityCategory,
    QualityEvent,
    RawEnvelope,
)
from crypto_momentum_lab.market_data.agg_trade_recovery import (
    AggTradeRecoveryBatch,
)
from crypto_momentum_lab.market_data.capture.coordinator import (
    CaptureCoordinator,
)
from crypto_momentum_lab.market_data.capture.queue import BoundedEnvelopeQueue


class ControlledArchive:
    def __init__(self) -> None:
        self._append_started = asyncio.Event()
        self._append_count_changed = asyncio.Event()
        self._append_released = asyncio.Event()
        self.started_count = 0
        self.appended: list[RawEnvelope] = []

    async def append(
        self,
        envelope: RawEnvelope,
    ) -> DurableArchiveAcknowledgement:
        self.started_count += 1
        self._append_started.set()
        self._append_count_changed.set()
        await self._append_released.wait()
        self.appended.append(envelope)
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

    async def wait_until_append_count(self, count: int) -> None:
        while self.started_count < count:
            self._append_count_changed.clear()
            if self.started_count >= count:
                return
            await self._append_count_changed.wait()

    def release_append(self) -> None:
        self._append_released.set()


class FakeQualityTracker:
    def __init__(self) -> None:
        self.observed: list[RawEnvelope] = []

    def observe(self, envelope: RawEnvelope) -> tuple[QualityEvent, ...]:
        self.observed.append(envelope)
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


class FakeEnvelopeRecovery:
    def __init__(
        self,
        result: AggTradeRecoveryBatch,
    ) -> None:
        self.result = result
        self.monitored_symbol_sets: list[frozenset[str]] = []

    def set_monitored_symbols(self, symbols: frozenset[str]) -> None:
        self.monitored_symbol_sets.append(symbols)

    async def expand(
        self,
        batch: tuple[RawEnvelope, ...],
    ) -> AggTradeRecoveryBatch:
        return self.result


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


async def test_coordinator_batches_archive_appends(
    raw_envelope: RawEnvelope,
) -> None:
    archive = ControlledArchive()
    coordinator = CaptureCoordinator(
        queue=BoundedEnvelopeQueue(max_events=10, max_bytes=100000),
        archive=archive,
        quality=FakeQualityTracker(),
        repository=FakeCaptureRepository(),
        acknowledgement_sink=None,
    )

    task = asyncio.create_task(coordinator.run())
    await coordinator.submit(raw_envelope)
    await coordinator.submit(replace(raw_envelope, local_sequence=2))

    await asyncio.wait_for(archive.wait_until_append_count(2), timeout=1)
    archive.release_append()
    await coordinator.stop()
    await task


async def test_coordinator_limits_concurrent_archive_batch_size(
    raw_envelope: RawEnvelope,
) -> None:
    archive = ControlledArchive()
    coordinator = CaptureCoordinator(
        queue=BoundedEnvelopeQueue(max_events=10, max_bytes=100000),
        archive=archive,
        quality=FakeQualityTracker(),
        repository=FakeCaptureRepository(),
        acknowledgement_sink=None,
        max_archive_batch_size=2,
    )

    await coordinator.submit(raw_envelope)
    await coordinator.submit(replace(raw_envelope, local_sequence=2))
    await coordinator.submit(replace(raw_envelope, local_sequence=3))
    task = asyncio.create_task(coordinator.run())

    await asyncio.wait_for(archive.wait_until_append_count(2), timeout=1)
    await asyncio.sleep(0)
    assert archive.started_count == 2

    archive.release_append()
    await coordinator.stop()
    await task
    assert archive.started_count == 3


async def test_archived_envelope_sink_runs_after_successful_batch_in_queue_order(
    raw_envelope: RawEnvelope,
) -> None:
    archive = ControlledArchive()
    published: list[RawEnvelope] = []
    coordinator = CaptureCoordinator(
        queue=BoundedEnvelopeQueue(max_events=10, max_bytes=100000),
        archive=archive,
        quality=FakeQualityTracker(),
        repository=FakeCaptureRepository(),
        acknowledgement_sink=None,
        archived_envelope_sink=published.append,
    )

    task = asyncio.create_task(coordinator.run())
    second = replace(raw_envelope, local_sequence=2)
    await coordinator.submit(raw_envelope)
    await coordinator.submit(second)
    await asyncio.wait_for(archive.wait_until_append_count(2), timeout=1)

    assert published == []

    archive.release_append()
    await coordinator.stop()
    await task

    assert tuple(item.local_sequence for item in archive.appended) == (1, 2)
    assert tuple(item.local_sequence for item in published) == (1, 2)


async def test_realtime_envelope_sink_runs_before_archive(
    raw_envelope: RawEnvelope,
) -> None:
    archive = ControlledArchive()
    published: list[RawEnvelope] = []
    coordinator = CaptureCoordinator(
        queue=BoundedEnvelopeQueue(max_events=10, max_bytes=100000),
        archive=archive,
        quality=FakeQualityTracker(),
        repository=FakeCaptureRepository(),
        realtime_envelope_sink=published.append,
    )

    task = asyncio.create_task(coordinator.run())
    await coordinator.submit(raw_envelope)
    await archive.wait_until_append_started()

    assert tuple(item.local_sequence for item in published) == (1,)
    assert archive.appended == []

    archive.release_append()
    await coordinator.stop()
    await task


async def test_coordinator_only_archives_selected_streams(
    raw_envelope: RawEnvelope,
) -> None:
    archive = ControlledArchive()
    archive.release_append()
    published: list[RawEnvelope] = []
    coordinator = CaptureCoordinator(
        queue=BoundedEnvelopeQueue(max_events=10, max_bytes=100000),
        archive=archive,
        quality=FakeQualityTracker(),
        repository=FakeCaptureRepository(),
        realtime_envelope_sink=published.append,
        archive_streams=frozenset({CaptureStream.FORCE_ORDER}),
    )
    liquidation = replace(
        raw_envelope,
        stream=CaptureStream.FORCE_ORDER,
        local_sequence=2,
    )

    task = asyncio.create_task(coordinator.run())
    await coordinator.submit(raw_envelope)
    await coordinator.submit(liquidation)
    await coordinator.stop()
    await task

    assert tuple(item.local_sequence for item in published) == (1, 2)
    assert tuple(item.local_sequence for item in archive.appended) == (2,)


async def test_coordinator_does_not_side_process_unarchived_book_ticker(
    raw_envelope: RawEnvelope,
) -> None:
    archive = ControlledArchive()
    quality = FakeQualityTracker()
    coordinator = CaptureCoordinator(
        queue=BoundedEnvelopeQueue(max_events=10, max_bytes=100000),
        archive=archive,
        quality=quality,
        repository=FakeCaptureRepository(),
        archive_streams=frozenset({CaptureStream.FORCE_ORDER}),
    )
    book_ticker = replace(
        raw_envelope,
        route=CaptureRoute.PUBLIC,
        stream=CaptureStream.BOOK_TICKER,
        raw_payload={
            "e": "bookTicker",
            "s": "BTCUSDT",
            "u": 1,
        },
    )

    task = asyncio.create_task(coordinator.run())
    await coordinator.submit(book_ticker)
    await coordinator.stop()
    await task

    assert quality.observed == []
    assert archive.appended == []


async def test_coordinator_filters_global_book_ticker_to_monitored_symbols(
    raw_envelope: RawEnvelope,
) -> None:
    coordinator = CaptureCoordinator(
        queue=BoundedEnvelopeQueue(max_events=10, max_bytes=100000),
        archive=ControlledArchive(),
        quality=FakeQualityTracker(),
        repository=FakeCaptureRepository(),
        archive_streams=frozenset({CaptureStream.FORCE_ORDER}),
    )
    coordinator.set_monitored_symbols(frozenset({"BTCUSDT"}))

    await coordinator.submit(
        replace(
            raw_envelope,
            route=CaptureRoute.PUBLIC,
            stream=CaptureStream.BOOK_TICKER,
            symbol="ETHUSDT",
        )
    )

    assert coordinator.filtered_book_ticker_events == 1


async def test_coordinator_resets_recovery_and_filters_retired_streams(
    raw_envelope: RawEnvelope,
) -> None:
    recovery = FakeEnvelopeRecovery(AggTradeRecoveryBatch((), ()))
    queue = BoundedEnvelopeQueue(max_events=10, max_bytes=100000)
    coordinator = CaptureCoordinator(
        queue=queue,
        archive=ControlledArchive(),
        quality=FakeQualityTracker(),
        repository=FakeCaptureRepository(),
        envelope_recovery=recovery,
    )

    coordinator.set_monitored_symbols(frozenset({"BTCUSDT"}))
    await coordinator.submit(replace(raw_envelope, symbol="ETHUSDT"))

    assert recovery.monitored_symbol_sets == [frozenset({"BTCUSDT"})]
    assert queue.size == 0


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

    assert repository.quality_events[0].category is (QualityCategory.CONNECTION_OPENED)


async def test_coordinator_publishes_recovered_events_and_unrecovered_gaps(
    raw_envelope: RawEnvelope,
) -> None:
    recovered = replace(
        raw_envelope,
        local_sequence=2,
        exchange_sequence="2",
        recovered=True,
    )
    gap = AggTradeGap(
        environment=raw_envelope.environment,
        symbol=raw_envelope.symbol or "BTCUSDT",
        previous_id=2,
        current_id=4,
        previous_event_at=raw_envelope.received_at,
        current_event_at=raw_envelope.received_at,
        missing_count=1,
        reason="history_incomplete",
    )
    published: list[RawEnvelope] = []
    gaps: list[AggTradeGap] = []
    coordinator = CaptureCoordinator(
        queue=BoundedEnvelopeQueue(max_events=10, max_bytes=100000),
        archive=ControlledArchive(),
        quality=FakeQualityTracker(),
        repository=FakeCaptureRepository(),
        realtime_envelope_sink=published.append,
        envelope_recovery=FakeEnvelopeRecovery(
            AggTradeRecoveryBatch(
                envelopes=(recovered, raw_envelope),
                unrecovered_gaps=(gap,),
            )
        ),
        gap_sink=gaps.append,
        archive_streams=frozenset({CaptureStream.FORCE_ORDER}),
    )

    task = asyncio.create_task(coordinator.run())
    await coordinator.submit(raw_envelope)
    await coordinator.stop()
    await task

    assert published == [recovered, raw_envelope]
    assert gaps == [gap]
