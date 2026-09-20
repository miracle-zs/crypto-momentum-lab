"""Fault-injection tests for lifecycle ownership, shutdown boundaries, and progress contracts."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

from crypto_momentum_lab.live_rollout.daemon import LiveDaemonResult
from crypto_momentum_lab.live_rollout.resource_lifecycle import (
    LiveResourceLifecycle,
)
from crypto_momentum_lab.live_rollout.runtime_session import (
    ResourceOwnershipRegistry,
    RuntimeSession,
    SessionLifecycleState,
)
from crypto_momentum_lab.live_rollout.runtime_supervisor import (
    LiveRuntimeSupervisor,
)


class MockSupervisor:
    def __init__(self, raise_on_stop: BaseException | None = None) -> None:
        self.stop_called = False
        self.raise_on_stop = raise_on_stop

    async def run(self) -> LiveDaemonResult:
        return LiveDaemonResult(
            processed_state_count=1,
            approved_intent_count=0,
            submitted_order_count=0,
            halt_reason=None,
            final_state_at=None,
        )

    async def stop(self) -> None:
        self.stop_called = True
        if self.raise_on_stop is not None:
            raise self.raise_on_stop


class MockLifecycle:
    def __init__(self, raise_on_close: BaseException | None = None) -> None:
        self.close_called = False
        self.raise_on_close = raise_on_close

    async def close(self) -> None:
        self.close_called = True
        if self.raise_on_close is not None:
            raise self.raise_on_close


async def test_resource_ownership_registry_disarm() -> None:
    """Disarming the construction registry clears registered cleanups, preventing double-closing."""
    registry = ResourceOwnershipRegistry(run_id="run-disarm")
    cleaned_up: list[str] = []

    registry.register("db_pool", lambda: cleaned_up.append("db"))
    registry.register("http_client", lambda: cleaned_up.append("http"))

    # When ownership is transferred to session, disarm is called
    registry.disarm()

    # Teardown should now be a no-op
    await registry.teardown_all()
    assert cleaned_up == []


async def test_shutdown_fault_injection_drain_exception() -> None:
    """When drain phase raises an exception, terminal transition receives the drain error (R9)."""
    supervisor = MockSupervisor(raise_on_stop=RuntimeError("supervisor drain crashed"))
    lifecycle = MockLifecycle()
    terminal_called_with: str | None = "not_called"

    async def record_terminal(reason: str | None) -> None:
        nonlocal terminal_called_with
        terminal_called_with = reason

    session = RuntimeSession(
        run_id="session-drain-exc",
        supervisor=cast(LiveRuntimeSupervisor, supervisor),
        lifecycle=cast(LiveResourceLifecycle, lifecycle),
        save_final_checkpoint=lambda _t: asyncio.sleep(0.001, result=True),
        transition_terminal_state=record_terminal,
        shutdown_budget_seconds=3.0,
    )

    await session.close()

    assert session.state == SessionLifecycleState.STOPPED
    assert lifecycle.close_called is True
    assert session.shutdown_result is not None
    assert terminal_called_with == "drain_error:RuntimeError"
    assert session.shutdown_result.halt_reason == "drain_error:RuntimeError"
    assert any("drain_error" in f for f in session.shutdown_result.failures)


async def test_shutdown_fault_injection_drain_cancelled() -> None:
    """When drain phase is cancelled, checkpoint is marked not durable and resources are closed."""
    supervisor = MockSupervisor(raise_on_stop=asyncio.CancelledError())
    lifecycle = MockLifecycle()

    session = RuntimeSession(
        run_id="session-drain-cancel",
        supervisor=cast(LiveRuntimeSupervisor, supervisor),
        lifecycle=cast(LiveResourceLifecycle, lifecycle),
        save_final_checkpoint=lambda _t: asyncio.sleep(0.001, result=True),
        shutdown_budget_seconds=3.0,
    )

    await session.close()

    assert session.state == SessionLifecycleState.STOPPED
    assert lifecycle.close_called is True
    assert session.shutdown_result is not None
    assert "drain_cancelled" in session.shutdown_result.failures
    assert session.shutdown_result.halt_reason == "drain_cancelled"



async def test_shutdown_fault_injection_persist_timeout() -> None:
    """When checkpoint persists beyond timeout, checkpoint_durable is False and halt_reason is set."""
    supervisor = MockSupervisor()
    lifecycle = MockLifecycle()

    async def hanging_checkpoint(_t: float | None) -> bool:
        await asyncio.sleep(10.0)
        return True

    session = RuntimeSession(
        run_id="session-persist-timeout",
        supervisor=cast(LiveRuntimeSupervisor, supervisor),
        lifecycle=cast(LiveResourceLifecycle, lifecycle),
        save_final_checkpoint=hanging_checkpoint,
        shutdown_budget_seconds=0.05,
    )

    await session.close()

    assert session.state == SessionLifecycleState.STOPPED
    assert lifecycle.close_called is True
    assert session.shutdown_result is not None
    assert session.shutdown_result.checkpoint_durable is False
    assert "persist_timeout" in session.shutdown_result.failures
    assert session.shutdown_result.halt_reason == "final_checkpoint_failed"


async def test_shutdown_fault_injection_close_exception() -> None:
    """When lifecycle.close raises an exception, session still cleanly achieves STOPPED state."""
    supervisor = MockSupervisor()
    lifecycle = MockLifecycle(raise_on_close=ConnectionResetError("peer dropped socket"))

    session = RuntimeSession(
        run_id="session-close-exc",
        supervisor=cast(LiveRuntimeSupervisor, supervisor),
        lifecycle=cast(LiveResourceLifecycle, lifecycle),
        save_final_checkpoint=lambda _t: asyncio.sleep(0.001, result=True),
        shutdown_budget_seconds=3.0,
    )

    await session.close()

    assert session.state == SessionLifecycleState.STOPPED
    assert session.shutdown_result is not None
    assert session.shutdown_result.checkpoint_durable is True
    assert any("close_error" in f for f in session.shutdown_result.failures)


async def test_shutdown_fault_injection_close_cancelled() -> None:
    """When lifecycle.close is cancelled, session still records close_cancelled and reaches STOPPED."""
    supervisor = MockSupervisor()
    lifecycle = MockLifecycle(raise_on_close=asyncio.CancelledError())

    session = RuntimeSession(
        run_id="session-close-cancel",
        supervisor=cast(LiveRuntimeSupervisor, supervisor),
        lifecycle=cast(LiveResourceLifecycle, lifecycle),
        save_final_checkpoint=lambda _t: asyncio.sleep(0.001, result=True),
        shutdown_budget_seconds=3.0,
    )

    await session.close()

    assert session.state == SessionLifecycleState.STOPPED
    assert session.shutdown_result is not None
    assert "close_cancelled" in session.shutdown_result.failures


async def test_shutdown_real_lifecycle_close_failure_marks_resources_not_closed() -> None:
    """When a real LiveResourceLifecycle has a failing resource, resources_closed is False (R8)."""
    class FailingClient:
        async def aclose(self) -> None:
            raise RuntimeError("simulated client aclose failed")

    real_lifecycle = LiveResourceLifecycle(
        client=cast(Any, FailingClient()),
    )
    supervisor = MockSupervisor()

    session = RuntimeSession(
        run_id="session-real-lifecycle-fail",
        supervisor=cast(LiveRuntimeSupervisor, supervisor),
        lifecycle=real_lifecycle,
        shutdown_budget_seconds=3.0,
    )

    await session.close()

    assert session.shutdown_result is not None
    assert session.shutdown_result.resources_closed is False
    assert any("trade_client:RuntimeError" in f for f in session.shutdown_result.failures)
    assert session.shutdown_result.halt_reason is not None

