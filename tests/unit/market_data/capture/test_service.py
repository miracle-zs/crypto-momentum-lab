from typing import Any

import pytest

from crypto_momentum_lab.domain.market.models import MarketDataState, RawEnvelope
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
    async def apply_symbols(self, *args: Any, **kwargs: Any) -> None:
        return None


class FakeRepository:
    async def save_process_state(self, *args: Any, **kwargs: Any) -> None:
        return None


def build_service(*, queue_max_events: int) -> MarketDataCaptureService:
    return MarketDataCaptureService(
        queue=BoundedEnvelopeQueue(max_events=queue_max_events, max_bytes=100000),
        repository=FakeRepository(),
        connection_pool=FakeConnectionPool(),
        disk_guard=DiskSpaceGuard(
            warning_free_bytes=300,
            halt_free_bytes=200,
            recovery_free_bytes=250,
        ),
    )


async def test_queue_overflow_halts_service(raw_envelope: RawEnvelope) -> None:
    service = build_service(queue_max_events=1)
    await service.submit(raw_envelope)

    with pytest.raises(CaptureQueueFull):
        await service.submit(raw_envelope)

    assert service.state is MarketDataState.HALTED


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
