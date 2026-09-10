from typing import Any

import pytest

from crypto_momentum_lab.domain.market.models import (
    CaptureStream,
    MarketDataState,
    RawEnvelope,
)
from crypto_momentum_lab.market_data.capture.queue import (
    BoundedEnvelopeQueue,
    CaptureQueueFull,
)
from crypto_momentum_lab.market_data.capture.service import (
    DiskSpaceGuard,
    DiskStatus,
    MarketDataCaptureService,
)


class FakeConnectionPool:
    def __init__(self) -> None:
        self.calls = []
        self.stopped = False

    async def apply_symbols(self, *args: Any, **kwargs: Any) -> None:
        self.calls.append((args, kwargs))
        return None

    async def stop(self) -> None:
        self.stopped = True


class FakeRepository:
    def __init__(self) -> None:
        self.states = []

    async def save_process_state(self, *args: Any, **kwargs: Any) -> None:
        self.states.append(kwargs["state"])
        return None


class FakeCoordinator:
    def __init__(self) -> None:
        self.envelopes: list[RawEnvelope] = []
        self.filtered_book_ticker_events = 0

    async def submit(self, envelope: RawEnvelope) -> None:
        self.envelopes.append(envelope)


def build_service(
    *,
    queue_max_events: int,
    queue_max_bytes: int = 100000,
    disk_free_bytes_provider=None,
) -> MarketDataCaptureService:
    return MarketDataCaptureService(
        queue=BoundedEnvelopeQueue(
            max_events=queue_max_events,
            max_bytes=queue_max_bytes,
        ),
        repository=FakeRepository(),
        connection_pool=FakeConnectionPool(),
        disk_guard=DiskSpaceGuard(
            warning_free_bytes=300,
            halt_free_bytes=200,
            recovery_free_bytes=250,
        ),
        disk_free_bytes_provider=disk_free_bytes_provider,
    )


async def test_unrecoverable_queue_input_halts_service(
    raw_envelope: RawEnvelope,
) -> None:
    service = build_service(queue_max_events=1, queue_max_bytes=1)

    with pytest.raises(CaptureQueueFull, match="envelope exceeds"):
        await service.submit(raw_envelope)

    assert service.state is MarketDataState.HALTED


async def test_submit_delegates_to_coordinator_when_configured(
    raw_envelope: RawEnvelope,
) -> None:
    coordinator = FakeCoordinator()
    service = MarketDataCaptureService(
        queue=BoundedEnvelopeQueue(max_events=10, max_bytes=100000),
        repository=FakeRepository(),
        connection_pool=FakeConnectionPool(),
        disk_guard=DiskSpaceGuard(
            warning_free_bytes=300,
            halt_free_bytes=200,
            recovery_free_bytes=250,
        ),
        coordinator=coordinator,
    )

    await service.submit(raw_envelope)

    assert coordinator.envelopes == [raw_envelope]
    assert service.metrics_snapshot().queue_events == 0


def test_metrics_snapshot_reports_queue_and_state() -> None:
    service = build_service(queue_max_events=10)

    snapshot = service.metrics_snapshot()

    assert snapshot.state is MarketDataState.STARTING
    assert snapshot.queue_events == 0
    assert snapshot.queue_bytes == 0
    assert snapshot.desired_subscriptions == 0
    assert snapshot.active_connections == 0


def test_disk_halt_requires_recovery_threshold() -> None:
    guard = DiskSpaceGuard(
        warning_free_bytes=300,
        halt_free_bytes=200,
        recovery_free_bytes=250,
    )

    assert guard.evaluate(190) is DiskStatus.HALT
    assert guard.evaluate(220) is DiskStatus.HALT
    assert guard.evaluate(260) is DiskStatus.HEALTHY


async def test_low_disk_space_halts_before_accepting_an_envelope(
    raw_envelope: RawEnvelope,
) -> None:
    service = build_service(
        queue_max_events=10,
        disk_free_bytes_provider=lambda: 190,
    )

    with pytest.raises(CaptureQueueFull, match="disk free space"):
        await service.submit(raw_envelope)

    assert service.state is MarketDataState.HALTED
    assert service.metrics_snapshot().disk_free_bytes == 190


async def test_start_applies_initial_symbols_and_stop_persists_state() -> None:
    repository = FakeRepository()
    connection_pool = FakeConnectionPool()
    service = MarketDataCaptureService(
        queue=BoundedEnvelopeQueue(max_events=10, max_bytes=100000),
        repository=repository,
        connection_pool=connection_pool,
        disk_guard=DiskSpaceGuard(
            warning_free_bytes=300,
            halt_free_bytes=200,
            recovery_free_bytes=250,
        ),
    )

    await service.start(
        symbols=frozenset({"BTCUSDT"}),
        streams=(CaptureStream.AGG_TRADE,),
        generation=1,
    )
    await service.stop()

    assert service.state is MarketDataState.STOPPED
    assert repository.states == [MarketDataState.READY, MarketDataState.STOPPED]
    assert connection_pool.stopped is True
