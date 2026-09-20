import asyncio
from time import perf_counter
from typing import cast
from unittest.mock import MagicMock

import pytest

from crypto_momentum_lab.live_rollout.daemon import LiveDaemonResult
from crypto_momentum_lab.live_rollout.resource_lifecycle import (
    LiveResourceLifecycle,
)
from crypto_momentum_lab.live_rollout.runtime_session import (
    ResourceOwnershipRegistry,
    RuntimeSession,
    SessionLifecycleState,
    ShutdownResult,
)
from crypto_momentum_lab.live_rollout.runtime_supervisor import (
    LiveRuntimeSupervisor,
)


class FakeSupervisor:
    def __init__(self, result: LiveDaemonResult | None = None) -> None:
        self.result = result or LiveDaemonResult(
            processed_state_count=5,
            approved_intent_count=2,
            submitted_order_count=2,
            halt_reason=None,
            final_state_at=None,
        )
        self.run_called = False
        self.stop_called = False
        self.run_future: asyncio.Future[LiveDaemonResult] | None = None

    async def run(self) -> LiveDaemonResult:
        self.run_called = True
        if self.run_future is not None:
            return await self.run_future
        return self.result

    async def stop(self) -> None:
        self.stop_called = True


class FakeResourceLifecycle:
    def __init__(self) -> None:
        self.close_called = False

    async def close(self) -> None:
        self.close_called = True


async def test_resource_ownership_registry_reverse_teardown() -> None:
    registry = ResourceOwnershipRegistry(run_id="run-1")
    events: list[str] = []

    def sync_cleanup() -> None:
        events.append("engine_disposed")

    async def async_cleanup_1() -> None:
        await asyncio.sleep(0.01)
        events.append("feed_closed")

    async def async_cleanup_2() -> None:
        events.append("socket_closed")

    registry.register("engine", sync_cleanup)
    registry.register("feed", async_cleanup_1)
    registry.register("socket", async_cleanup_2)

    await registry.teardown_all()

    # LIFO order
    assert events == ["socket_closed", "feed_closed", "engine_disposed"]


async def test_resource_ownership_registry_handles_cleanup_exceptions() -> None:
    registry = ResourceOwnershipRegistry(run_id="run-1")
    events: list[str] = []

    async def failing_cleanup() -> None:
        events.append("failing_attempted")
        raise RuntimeError("boom")

    def succeeding_cleanup() -> None:
        events.append("succeeding_done")

    registry.register("first", succeeding_cleanup)
    registry.register("second_faulty", failing_cleanup)

    # Should not raise; faulty cleanup caught and remaining teardowns proceed
    await registry.teardown_all()

    assert events == ["failing_attempted", "succeeding_done"]


async def test_resource_ownership_registry_deadline_exceeded() -> None:
    registry = ResourceOwnershipRegistry(run_id="run-1")
    events: list[str] = []

    async def slow_cleanup() -> None:
        events.append("slow_started")
        await asyncio.sleep(0.5)
        events.append("slow_finished")

    def next_cleanup() -> None:
        events.append("next_done")

    registry.register("next", next_cleanup)
    registry.register("slow", slow_cleanup)

    deadline = perf_counter() + 0.05
    await registry.teardown_all(deadline=deadline)

    assert "slow_started" in events
    # Because slow exceeded deadline, next is skipped
    assert "next_done" not in events


async def test_runtime_session_successful_run_and_4_phase_shutdown() -> None:
    supervisor = FakeSupervisor()
    lifecycle = FakeResourceLifecycle()
    events: list[str] = []

    async def save_final(timeout_seconds: float | None) -> bool:
        events.append("save_final")
        return True

    async def transition_state(reason: str | None) -> None:
        events.append(f"transition:{reason}")

    health = MagicMock()

    session = RuntimeSession(
        run_id="session-test-1",
        supervisor=cast(LiveRuntimeSupervisor, supervisor),
        lifecycle=cast(LiveResourceLifecycle, lifecycle),
        save_final_checkpoint=save_final,
        transition_terminal_state=transition_state,
        health=health,
        shutdown_budget_seconds=10.0,
    )

    assert session.state == SessionLifecycleState.READY

    session.set_recovering()
    assert session.state == SessionLifecycleState.RECOVERING
    session.set_ready()
    assert session.state == SessionLifecycleState.READY

    result = await session.run()

    assert result.processed_state_count == 5
    assert supervisor.run_called is True
    assert supervisor.stop_called is True
    assert lifecycle.close_called is True
    assert events == ["save_final", "transition:None"]
    health.stopped.assert_called_once()
    assert session.state == SessionLifecycleState.STOPPED


async def test_runtime_session_stop_requested_with_halt_reason() -> None:
    loop = asyncio.get_running_loop()
    supervisor = FakeSupervisor()
    supervisor.run_future = loop.create_future()
    lifecycle = FakeResourceLifecycle()
    events: list[str] = []

    async def save_final(timeout_seconds: float | None) -> bool:
        events.append("save_final")
        return True

    async def transition_state(reason: str | None) -> None:
        events.append(f"transition:{reason}")

    session = RuntimeSession(
        run_id="session-stop-req",
        supervisor=cast(LiveRuntimeSupervisor, supervisor),
        lifecycle=cast(LiveResourceLifecycle, lifecycle),
        save_final_checkpoint=save_final,
        transition_terminal_state=transition_state,
        shutdown_budget_seconds=5.0,
    )

    session.request_stop("manual_operator_halt")

    async def finish_supervisor() -> None:
        await asyncio.sleep(0.01)
        supervisor.run_future.set_result(
            LiveDaemonResult(
                processed_state_count=10,
                approved_intent_count=1,
                submitted_order_count=1,
                halt_reason="manual_operator_halt",
                final_state_at=None,
            )
        )

    asyncio.create_task(finish_supervisor())

    result = await session.run()

    assert result.halt_reason == "manual_operator_halt"
    assert session.state == SessionLifecycleState.STOPPED
    assert events == ["save_final", "transition:manual_operator_halt"]


async def test_runtime_session_close_idempotency() -> None:
    supervisor = FakeSupervisor()
    lifecycle = FakeResourceLifecycle()
    close_count = 0

    async def save_final(timeout_seconds: float | None) -> bool:
        nonlocal close_count
        close_count += 1
        return True

    session = RuntimeSession(
        run_id="session-idempotent",
        supervisor=cast(LiveRuntimeSupervisor, supervisor),
        lifecycle=cast(LiveResourceLifecycle, lifecycle),
        save_final_checkpoint=save_final,
    )

    # Call close multiple times concurrently
    await asyncio.gather(session.close(), session.close(), session.close())

    assert close_count == 1
    assert session.state == SessionLifecycleState.STOPPED


async def test_runtime_session_handles_exceptions_in_shutdown_phases() -> None:
    supervisor = FakeSupervisor()

    async def failing_stop() -> None:
        raise RuntimeError("supervisor stop failed")

    supervisor.stop = failing_stop  # type: ignore

    lifecycle = FakeResourceLifecycle()

    async def failing_close() -> None:
        raise RuntimeError("lifecycle close failed")

    lifecycle.close = failing_close  # type: ignore

    async def failing_save_final(timeout_seconds: float | None) -> bool:
        raise RuntimeError("checkpoint failed")

    async def failing_transition(reason: str | None) -> None:
        raise RuntimeError("transition failed")

    session = RuntimeSession(
        run_id="session-failing-phases",
        supervisor=cast(LiveRuntimeSupervisor, supervisor),
        lifecycle=cast(LiveResourceLifecycle, lifecycle),
        save_final_checkpoint=failing_save_final,
        transition_terminal_state=failing_transition,
        shutdown_budget_seconds=5.0,
    )

    # Calling close should not raise even if every phase raises
    await session.close()

    assert session.state == SessionLifecycleState.STOPPED


async def test_runtime_session_cancelled_run_triggers_close() -> None:
    loop = asyncio.get_running_loop()
    supervisor = FakeSupervisor()
    supervisor.run_future = loop.create_future()
    lifecycle = FakeResourceLifecycle()

    session = RuntimeSession(
        run_id="session-cancel",
        supervisor=cast(LiveRuntimeSupervisor, supervisor),
        lifecycle=cast(LiveResourceLifecycle, lifecycle),
        shutdown_budget_seconds=5.0,
    )

    task = asyncio.create_task(session.run())
    await asyncio.sleep(0.01)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert supervisor.stop_called is True
    assert lifecycle.close_called is True
    assert session.state == SessionLifecycleState.STOPPED


async def test_runtime_session_shared_shutdown_budget_bounds_total_time() -> None:
    supervisor = FakeSupervisor()

    async def slow_stop() -> None:
        await asyncio.sleep(0.1)

    supervisor.stop = slow_stop  # type: ignore

    lifecycle = FakeResourceLifecycle()

    async def slow_close() -> None:
        await asyncio.sleep(0.1)

    lifecycle.close = slow_close  # type: ignore

    session = RuntimeSession(
        run_id="session-budget",
        supervisor=cast(LiveRuntimeSupervisor, supervisor),
        lifecycle=cast(LiveResourceLifecycle, lifecycle),
        shutdown_budget_seconds=1.0,
    )

    start = perf_counter()
    await session.close()
    elapsed = perf_counter() - start

    assert session.state == SessionLifecycleState.STOPPED
    assert elapsed < 1.0  # Finished within budget


async def test_runtime_session_persisting_timeout_bounds_callback() -> None:
    """Regression test for P1-D: hanging transition must not defeat budget."""
    supervisor = FakeSupervisor()
    lifecycle = FakeResourceLifecycle()
    hanging_event = asyncio.Event()

    async def hanging_transition(reason: str | None) -> None:
        await hanging_event.wait()

    session = RuntimeSession(
        run_id="session-hanging-persist",
        supervisor=cast(LiveRuntimeSupervisor, supervisor),
        lifecycle=cast(LiveResourceLifecycle, lifecycle),
        transition_terminal_state=hanging_transition,
        shutdown_budget_seconds=0.1,
    )

    start = perf_counter()
    await session.close(deadline=start + 0.1)
    elapsed = perf_counter() - start

    assert elapsed < 0.5  # Did not hang forever!
    assert lifecycle.close_called is True  # Phase 3 CLOSING was still reached!
    assert session.state == SessionLifecycleState.STOPPED


async def test_runtime_session_cooperative_stop_stops_supervisor_cleanly() -> None:
    """Verify external request_stop cooperatively stops supervisor and runs."""
    supervisor = FakeSupervisor()
    lifecycle = FakeResourceLifecycle()

    # Supervisor runs until stopped
    supervisor_stopped = asyncio.Event()

    async def slow_run() -> LiveDaemonResult:
        await supervisor_stopped.wait()
        return LiveDaemonResult(
            processed_state_count=5,
            approved_intent_count=0,
            submitted_order_count=0,
            halt_reason="operator_requested",
            final_state_at=None,
        )

    async def cooperative_stop() -> None:
        supervisor.stop_called = True
        supervisor_stopped.set()

    supervisor.run = slow_run  # type: ignore
    supervisor.stop = cooperative_stop  # type: ignore

    session = RuntimeSession(
        run_id="session-cooperative-stop",
        supervisor=cast(LiveRuntimeSupervisor, supervisor),
        lifecycle=cast(LiveResourceLifecycle, lifecycle),
    )

    run_task = asyncio.create_task(session.run())
    await asyncio.sleep(0.01)

    # Request cooperative stop
    session.request_stop("operator_requested")

    result = await run_task
    assert result.halt_reason == "operator_requested"
    assert supervisor.stop_called is True
    assert session.state == SessionLifecycleState.STOPPED


async def test_runtime_session_checkpoint_false_prevents_completed_terminal_state() -> None:
    """Verify that when save_final_checkpoint returns False, terminal state is not COMPLETED."""
    supervisor = FakeSupervisor()  # halt_reason is None
    lifecycle = FakeResourceLifecycle()
    terminal_reasons: list[str | None] = []

    async def fake_save_final(_timeout: float | None) -> bool:
        return False

    async def fake_terminal(reason: str | None) -> None:
        terminal_reasons.append(reason)

    session = RuntimeSession(
        run_id="session-checkpoint-fail",
        supervisor=cast(LiveRuntimeSupervisor, supervisor),
        lifecycle=cast(LiveResourceLifecycle, lifecycle),
        save_final_checkpoint=fake_save_final,
        transition_terminal_state=fake_terminal,
        shutdown_budget_seconds=5.0,
    )

    await session.close()

    assert session.state == SessionLifecycleState.STOPPED
    assert terminal_reasons == ["final_checkpoint_failed"]
    assert session.shutdown_result is not None
    assert session.shutdown_result.checkpoint_durable is False
    assert "checkpoint_save_returned_false" in session.shutdown_result.failures
    assert session.shutdown_result.halt_reason == "final_checkpoint_failed"


async def test_runtime_session_checkpoint_exception_prevents_completed_state() -> None:
    """Verify that when save_final_checkpoint raises, terminal state is not COMPLETED."""
    supervisor = FakeSupervisor()
    lifecycle = FakeResourceLifecycle()
    terminal_reasons: list[str | None] = []

    async def fake_save_final(_timeout: float | None) -> bool:
        raise RuntimeError("checkpoint db connection dropped")

    async def fake_terminal(reason: str | None) -> None:
        terminal_reasons.append(reason)

    session = RuntimeSession(
        run_id="session-checkpoint-exc",
        supervisor=cast(LiveRuntimeSupervisor, supervisor),
        lifecycle=cast(LiveResourceLifecycle, lifecycle),
        save_final_checkpoint=fake_save_final,
        transition_terminal_state=fake_terminal,
        shutdown_budget_seconds=5.0,
    )

    await session.close()

    assert session.state == SessionLifecycleState.STOPPED
    assert terminal_reasons == ["final_checkpoint_failed"]
    assert session.shutdown_result is not None
    assert session.shutdown_result.checkpoint_durable is False
    assert any("checkpoint_error" in f for f in session.shutdown_result.failures)
    assert session.shutdown_result.halt_reason == "final_checkpoint_failed"


async def test_runtime_session_records_clean_shutdown_result() -> None:
    """Verify clean shutdown creates structured ShutdownResult with no failures."""
    supervisor = FakeSupervisor()
    lifecycle = FakeResourceLifecycle()
    terminal_reasons: list[str | None] = []

    async def fake_save_final(_timeout: float | None) -> bool:
        return True

    async def fake_terminal(reason: str | None) -> None:
        terminal_reasons.append(reason)

    session = RuntimeSession(
        run_id="session-clean-shutdown",
        supervisor=cast(LiveRuntimeSupervisor, supervisor),
        lifecycle=cast(LiveResourceLifecycle, lifecycle),
        save_final_checkpoint=fake_save_final,
        transition_terminal_state=fake_terminal,
        shutdown_budget_seconds=5.0,
    )

    await session.close()

    assert session.state == SessionLifecycleState.STOPPED
    assert terminal_reasons == [None]
    assert session.shutdown_result is not None
    assert session.shutdown_result.drained is True
    assert session.shutdown_result.checkpoint_durable is True
    assert session.shutdown_result.terminal_recorded is True
    assert session.shutdown_result.resources_closed is True
    assert session.shutdown_result.failures == ()
    assert session.shutdown_result.halt_reason is None
    assert session.shutdown_result.duration_seconds >= 0.0
