"""Unified runtime session lifecycle and resource ownership.

As specified in the system evolution blueprint (Astra §8), the RuntimeSession
is the single lifecycle owner exposed to orchestration and CLI runners:
- Explicit 7-stage state machine: CONSTRUCTING -> RECOVERING -> READY ->
  DRAINING -> PERSISTING -> CLOSING -> STOPPED;
- Single ownership registry ensuring reverse-dependency cleanup on construction
  failure without orphan tasks or engines;
- 4-phase bounded shutdown protocol with a shared global budget (default 60s);
- Idempotent cancellation and stop handling.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from time import perf_counter

import structlog

from crypto_momentum_lab.health import LocalHealthWriter
from crypto_momentum_lab.live_rollout.daemon import LiveDaemonResult
from crypto_momentum_lab.live_rollout.resource_lifecycle import LiveResourceLifecycle
from crypto_momentum_lab.live_rollout.runtime_supervisor import LiveRuntimeSupervisor

log = structlog.get_logger()

_DEFAULT_SESSION_SHUTDOWN_BUDGET_SECONDS = 60.0
_DRAIN_PHASE_MAX_SECONDS = 15.0
_PERSIST_PHASE_MAX_SECONDS = 15.0
_CLOSE_PHASE_MAX_SECONDS = 25.0


class SessionLifecycleState(StrEnum):
    """Explicit point-in-time state of a live runtime session."""

    CONSTRUCTING = "constructing"
    RECOVERING = "recovering"
    READY = "ready"
    DRAINING = "draining"
    PERSISTING = "persisting"
    CLOSING = "closing"
    STOPPED = "stopped"


@dataclass(frozen=True, slots=True)
class RegisteredResource:
    """A resource registered with the session ownership registry."""

    name: str
    cleanup: Callable[[], Awaitable[None] | None]


class ResourceOwnershipRegistry:
    """Maintains reverse-dependency ownership of live resources during construction.

    If an exception occurs partway through assembly, the registry dismantles
    all previously registered resources in reverse order, preventing engine,
    thread, or socket leaks.
    """

    def __init__(self, run_id: str) -> None:
        self._run_id = run_id
        self._resources: list[RegisteredResource] = []

    def register(
        self,
        name: str,
        cleanup: Callable[[], Awaitable[None] | None],
    ) -> None:
        """Register a resource cleanup handler."""
        self._resources.append(RegisteredResource(name=name, cleanup=cleanup))

    async def teardown_all(self, *, deadline: float | None = None) -> None:
        """Tear down all registered resources in reverse order."""
        for res in reversed(self._resources):
            started_at = perf_counter()
            try:
                if deadline is not None and perf_counter() >= deadline:
                    log.warning(
                        "resource_teardown_deadline_exceeded",
                        run_id=self._run_id,
                        resource=res.name,
                    )
                    break
                result = res.cleanup()
                if asyncio.iscoroutine(result):
                    timeout = None
                    if deadline is not None:
                        timeout = max(0.1, deadline - perf_counter())
                    if timeout is not None:
                        async with asyncio.timeout(timeout):
                            await result
                    else:
                        await result
                log.info(
                    "resource_teardown_completed",
                    run_id=self._run_id,
                    resource=res.name,
                    duration_seconds=round(perf_counter() - started_at, 3),
                )
            except Exception:
                log.exception(
                    "resource_teardown_failed",
                    run_id=self._run_id,
                    resource=res.name,
                )
        self._resources.clear()


class RuntimeSession:
    """Coordinates the full execution lifecycle of one live daemon session.

    The session owns:
    1. The supervisor monitoring critical async tasks;
    2. The resource lifecycle closing transports and engines;
    3. The ordered 4-phase graceful shutdown with a shared overall deadline;
    4. State progression with audit logging and idempotent termination.
    """

    def __init__(
        self,
        *,
        run_id: str,
        supervisor: LiveRuntimeSupervisor,
        lifecycle: LiveResourceLifecycle,
        save_final_checkpoint: (
            Callable[[float | None], Awaitable[bool]] | None
        ) = None,
        transition_terminal_state: (
            Callable[[str | None], Awaitable[None]] | None
        ) = None,
        health: LocalHealthWriter | None = None,
        shutdown_budget_seconds: float = _DEFAULT_SESSION_SHUTDOWN_BUDGET_SECONDS,
    ) -> None:
        if not run_id.strip():
            raise ValueError("run_id must not be empty")
        if shutdown_budget_seconds <= 0:
            raise ValueError("shutdown_budget_seconds must be positive")
        self._run_id = run_id
        self._supervisor = supervisor
        self._lifecycle = lifecycle
        self._save_final_checkpoint = save_final_checkpoint
        self._transition_terminal_state = transition_terminal_state
        self._health = health
        self._shutdown_budget_seconds = shutdown_budget_seconds
        self._state = SessionLifecycleState.READY
        self._state_lock = asyncio.Lock()
        self._stop_requested = asyncio.Event()
        self._stop_reason: str | None = None
        self._closed = False
        self._last_result: LiveDaemonResult | None = None

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def state(self) -> SessionLifecycleState:
        return self._state

    def set_recovering(self) -> None:
        """Mark session in recovery phase prior to entering READY."""
        self._state = SessionLifecycleState.RECOVERING

    def set_ready(self) -> None:
        """Mark session ready for active signal generation."""
        self._state = SessionLifecycleState.READY

    def request_stop(self, reason: str = "operator_requested") -> None:
        """Request a cooperative shutdown from an external caller."""
        if not self._stop_requested.is_set():
            self._stop_reason = reason
            self._stop_requested.set()
            log.info("session_stop_requested", run_id=self._run_id, reason=reason)

    async def run(self) -> LiveDaemonResult:
        """Run the live runtime supervisor until completion, halt, or stop request."""
        self._state = SessionLifecycleState.READY
        try:
            result = await self._supervisor.run()
            self._last_result = result
            return result
        except asyncio.CancelledError:
            log.info("session_run_cancelled", run_id=self._run_id)
            raise
        except Exception as exc:
            log.exception(
                "session_run_failed",
                run_id=self._run_id,
                error_type=type(exc).__name__,
            )
            raise
        finally:
            await self.close()

    async def close(self, deadline: float | None = None) -> None:
        """Execute the bounded 4-phase shutdown protocol idempotently."""
        async with self._state_lock:
            if self._closed:
                return
            self._closed = True

        budget = (
            deadline - perf_counter()
            if deadline is not None
            else self._shutdown_budget_seconds
        )
        total_deadline = perf_counter() + max(1.0, budget)
        started_at = perf_counter()

        halt_reason = (
            self._stop_reason
            if self._stop_reason is not None
            else (
                self._last_result.halt_reason if self._last_result is not None else None
            )
        )

        # Phase 1: DRAINING
        self._state = SessionLifecycleState.DRAINING
        log.info(
            "session_shutdown_phase_started",
            run_id=self._run_id,
            phase=self._state.value,
        )
        try:
            drain_timeout = min(
                _DRAIN_PHASE_MAX_SECONDS,
                max(0.5, total_deadline - perf_counter()),
            )
            async with asyncio.timeout(drain_timeout):
                # Supervisor blocks entry submissions, stops sources,
                # and drains in-flight submissions
                await self._supervisor.stop()
        except TimeoutError:
            log.warning(
                "session_shutdown_phase_timed_out",
                run_id=self._run_id,
                phase=SessionLifecycleState.DRAINING.value,
            )
        except Exception:
            log.exception(
                "session_shutdown_phase_failed",
                run_id=self._run_id,
                phase=SessionLifecycleState.DRAINING.value,
            )

        # Phase 2: PERSISTING
        self._state = SessionLifecycleState.PERSISTING
        log.info(
            "session_shutdown_phase_started",
            run_id=self._run_id,
            phase=self._state.value,
        )
        try:
            persist_timeout = min(
                _PERSIST_PHASE_MAX_SECONDS,
                max(0.5, total_deadline - perf_counter()),
            )
            if self._save_final_checkpoint is not None:
                await self._save_final_checkpoint(persist_timeout)
            if self._transition_terminal_state is not None:
                await self._transition_terminal_state(halt_reason)
        except Exception:
            log.exception(
                "session_shutdown_phase_failed",
                run_id=self._run_id,
                phase=SessionLifecycleState.PERSISTING.value,
            )

        # Phase 3: CLOSING
        self._state = SessionLifecycleState.CLOSING
        log.info(
            "session_shutdown_phase_started",
            run_id=self._run_id,
            phase=self._state.value,
        )
        try:
            close_timeout = min(
                _CLOSE_PHASE_MAX_SECONDS,
                max(0.5, total_deadline - perf_counter()),
            )
            async with asyncio.timeout(close_timeout):
                await self._lifecycle.close()
        except TimeoutError:
            log.warning(
                "session_shutdown_phase_timed_out",
                run_id=self._run_id,
                phase=SessionLifecycleState.CLOSING.value,
            )
        except Exception:
            log.exception(
                "session_shutdown_phase_failed",
                run_id=self._run_id,
                phase=SessionLifecycleState.CLOSING.value,
            )

        # Phase 4: STOPPED
        self._state = SessionLifecycleState.STOPPED
        if self._health is not None:
            try:
                self._health.stopped()
            except Exception:
                log.exception("session_health_stopped_marker_failed")

        log.info(
            "session_shutdown_completed",
            run_id=self._run_id,
            duration_seconds=round(perf_counter() - started_at, 3),
        )


__all__ = [
    "RegisteredResource",
    "ResourceOwnershipRegistry",
    "RuntimeSession",
    "SessionLifecycleState",
]
