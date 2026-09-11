import asyncio

import pytest

from crypto_momentum_lab.live_rollout.health_monitor import LiveHealthMonitor


class FakeHealth:
    def __init__(self, *, fail_heartbeat: bool = False) -> None:
        self.heartbeats = 0
        self.degraded_calls = 0
        self.fail_heartbeat = fail_heartbeat

    def heartbeat(self) -> None:
        if self.fail_heartbeat and self.heartbeats == 0:
            self.heartbeats += 1
            raise OSError("health directory unavailable")
        self.heartbeats += 1

    def degraded(self) -> None:
        self.degraded_calls += 1


async def _run_once(monitor: LiveHealthMonitor) -> None:
    with pytest.raises(asyncio.CancelledError):
        await monitor.run()


async def test_health_monitor_publishes_heartbeat_for_healthy_tasks() -> None:
    health = FakeHealth()
    sleeps = 0

    async def sleep(_: float) -> None:
        nonlocal sleeps
        sleeps += 1
        if sleeps > 1:
            raise asyncio.CancelledError

    monitor = LiveHealthMonitor(
        health=health,  # type: ignore[arg-type]
        interval_seconds=15,
        is_degraded=lambda: False,
        sleep=sleep,
    )

    await _run_once(monitor)

    assert health.heartbeats == 1
    assert health.degraded_calls == 0


async def test_health_monitor_marks_degraded_when_critical_task_stops() -> None:
    health = FakeHealth()
    sleeps = 0

    async def sleep(_: float) -> None:
        nonlocal sleeps
        sleeps += 1
        if sleeps > 1:
            raise asyncio.CancelledError

    monitor = LiveHealthMonitor(
        health=health,  # type: ignore[arg-type]
        interval_seconds=15,
        is_degraded=lambda: True,
        sleep=sleep,
    )

    await _run_once(monitor)

    assert health.heartbeats == 0
    assert health.degraded_calls == 1


async def test_health_monitor_keeps_running_after_marker_failure() -> None:
    health = FakeHealth(fail_heartbeat=True)
    sleeps = 0

    async def sleep(_: float) -> None:
        nonlocal sleeps
        sleeps += 1
        if sleeps > 2:
            raise asyncio.CancelledError

    monitor = LiveHealthMonitor(
        health=health,  # type: ignore[arg-type]
        interval_seconds=15,
        is_degraded=lambda: False,
        sleep=sleep,
    )

    await _run_once(monitor)

    assert health.heartbeats == 2
