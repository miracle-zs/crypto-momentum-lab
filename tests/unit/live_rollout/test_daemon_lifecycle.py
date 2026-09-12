import asyncio
from collections.abc import AsyncIterator
from typing import cast

import pytest

from crypto_momentum_lab.live_rollout.daemon_lifecycle import (
    LiveDaemonLifecycle,
)
from crypto_momentum_lab.live_rollout.exit_lane import ExitLaneOutcome
from crypto_momentum_lab.live_rollout.market_loop import LiveDaemonResult


class FakeCheckpointCoordinator:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def start(self) -> None:
        self.events.append("checkpoint_start")

    async def stop(self) -> None:
        self.events.append("checkpoint_stop")


class FakeExitLane:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def start(self) -> None:
        self.events.append("exit_start")

    async def stop(self) -> ExitLaneOutcome:
        self.events.append("exit_stop")
        return ExitLaneOutcome(
            approved_intent_count=2,
            submitted_order_count=3,
        )


class HangingExitLane(FakeExitLane):
    async def stop(self) -> ExitLaneOutcome:
        self.events.append("exit_stop_started")
        await asyncio.Event().wait()
        raise AssertionError("hanging exit lane should be timed out")


class FakeScheduledController:
    approved_intent_count = 4
    submitted_order_count = 5

    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.cancelled = asyncio.Event()

    async def run(self) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.events.append("scheduled_cancelled")
            self.cancelled.set()
            raise


async def _empty_states() -> AsyncIterator:
    if False:
        yield None


async def test_lifecycle_owns_lane_start_stop_and_result_aggregation() -> None:
    events: list[str] = []
    scheduled = FakeScheduledController(events)
    active: list[bool] = []

    async def run_market_loop(states) -> LiveDaemonResult:
        del states
        await asyncio.sleep(0)
        events.append("market_loop")
        return LiveDaemonResult(7, 11, 13, None, None)

    lifecycle = LiveDaemonLifecycle(
        run_id="run-1",
        checkpoint_coordinator=cast(object, FakeCheckpointCoordinator(events)),
        exit_lane=cast(object, FakeExitLane(events)),
        exit_manager=cast(object, object()),
        scheduled_controller=cast(object, scheduled),
        scheduled_risk_window_enabled=True,
        run_market_loop=run_market_loop,
        set_run_active=active.append,
    )

    result = await lifecycle.run(_empty_states())

    assert result == LiveDaemonResult(7, 17, 21, None, None)
    assert active == [True, False]
    assert events == [
        "checkpoint_start",
        "exit_start",
        "market_loop",
        "scheduled_cancelled",
        "exit_stop",
        "checkpoint_stop",
    ]
    assert scheduled.cancelled.is_set()


async def test_lifecycle_stops_lanes_when_market_loop_fails() -> None:
    events: list[str] = []
    active: list[bool] = []

    async def run_market_loop(states) -> LiveDaemonResult:
        del states
        raise RuntimeError("market loop failed")

    lifecycle = LiveDaemonLifecycle(
        run_id="run-1",
        checkpoint_coordinator=cast(object, FakeCheckpointCoordinator(events)),
        exit_lane=cast(object, FakeExitLane(events)),
        exit_manager=cast(object, object()),
        scheduled_controller=cast(object, FakeScheduledController(events)),
        scheduled_risk_window_enabled=False,
        run_market_loop=run_market_loop,
        set_run_active=active.append,
    )

    with pytest.raises(RuntimeError, match="market loop failed"):
        await lifecycle.run(_empty_states())

    assert active == [True, False]
    assert events == [
        "checkpoint_start",
        "exit_start",
        "exit_stop",
        "checkpoint_stop",
    ]


async def test_lifecycle_bounds_exit_lane_shutdown() -> None:
    events: list[str] = []

    async def run_market_loop(states) -> LiveDaemonResult:
        del states
        return LiveDaemonResult(1, 0, 0, None, None)

    lifecycle = LiveDaemonLifecycle(
        run_id="run-1",
        checkpoint_coordinator=cast(object, FakeCheckpointCoordinator(events)),
        exit_lane=cast(object, HangingExitLane(events)),
        exit_manager=cast(object, object()),
        scheduled_controller=cast(object, FakeScheduledController(events)),
        scheduled_risk_window_enabled=False,
        run_market_loop=run_market_loop,
        set_run_active=lambda _active: None,
        shutdown_timeout_seconds=0.01,
    )

    result = await lifecycle.run(_empty_states())

    assert result.halt_reason == "exit_lane_shutdown_timed_out"
    assert events == [
        "checkpoint_start",
        "exit_start",
        "exit_stop_started",
        "checkpoint_stop",
    ]
