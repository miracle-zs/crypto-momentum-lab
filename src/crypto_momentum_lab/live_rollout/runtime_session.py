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
class ShutdownResult:
    """Structured audit result of the ordered 4-phase shutdown protocol."""

    run_id: str
    drained: bool
    checkpoint_durable: bool
    terminal_recorded: bool
    resources_closed: bool
    failures: tuple[str, ...]
    halt_reason: str | None
    duration_seconds: float


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

    def disarm(self) -> None:
        """Disarm the construction registry once ownership transfers to session lifecycle."""
        self._resources.clear()

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
        ownership_registry: ResourceOwnershipRegistry | None = None,
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
        self._ownership_registry = ownership_registry
        self._save_final_checkpoint = save_final_checkpoint
        self._transition_terminal_state = transition_terminal_state
        self._health = health
        self._shutdown_budget_seconds = shutdown_budget_seconds
        self._state = SessionLifecycleState.READY
        self._state_lock = asyncio.Lock()
        self._stop_requested = asyncio.Event()
        self._stop_reason: str | None = None
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None
        self._last_result: LiveDaemonResult | None = None
        self._shutdown_result: ShutdownResult | None = None

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def state(self) -> SessionLifecycleState:
        return self._state

    @property
    def shutdown_result(self) -> ShutdownResult | None:
        return self._shutdown_result

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
        supervisor_task = asyncio.create_task(
            self._supervisor.run(),
            name=f"live-supervisor:{self._run_id}",
        )
        stop_waiter = asyncio.create_task(
            self._stop_requested.wait(),
            name=f"live-session-stop-waiter:{self._run_id}",
        )
        try:
            done, pending = await asyncio.wait(
                {supervisor_task, stop_waiter},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stop_waiter in done:
                log.info(
                    "session_cooperative_stop_triggered",
                    run_id=self._run_id,
                    reason=self._stop_reason,
                )
                await self._supervisor.stop()
                result = await supervisor_task
            else:
                result = supervisor_task.result()
            self._last_result = result
            return result
        except asyncio.CancelledError:
            log.info("session_run_cancelled", run_id=self._run_id)
            if not supervisor_task.done():
                supervisor_task.cancel()
            raise
        except Exception as exc:
            log.exception(
                "session_run_failed",
                run_id=self._run_id,
                error_type=type(exc).__name__,
            )
            raise
        finally:
            if not stop_waiter.done():
                stop_waiter.cancel()
            await self.close()

    async def close(self, deadline: float | None = None) -> None:
        """Execute the bounded 4-phase shutdown protocol idempotently."""
        async with self._state_lock:
            if self._closed:
                return
            if self._close_task is None:
                self._close_task = asyncio.create_task(
                    self._execute_close(deadline),
                    name=f"live-session-close:{self._run_id}",
                )
            task = self._close_task

        try:
            await asyncio.shield(task)
        except Exception:
            raise

    async def _execute_close(self, deadline: float | None) -> None:
        budget = (
            deadline - perf_counter()
            if deadline is not None
            else self._shutdown_budget_seconds
        )
        total_deadline = perf_counter() + max(0.01, budget)
        started_at = perf_counter()

        halt_reason = (
            self._stop_reason
            if self._stop_reason is not None
            else (
                self._last_result.halt_reason if self._last_result is not None else None
            )
        )

        drained = False
        checkpoint_durable = self._save_final_checkpoint is None
        terminal_recorded = False
        resources_closed = False
        failures: list[str] = []

        try:
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
                    max(0.01, total_deadline - perf_counter()),
                )
                async with asyncio.timeout(drain_timeout):
                    await self._supervisor.stop()
                drained = True
            except TimeoutError:
                failures.append("drain_timeout")
                halt_reason = "drain_timeout"
                log.warning(
                    "session_shutdown_phase_timed_out",
                    run_id=self._run_id,
                    phase=SessionLifecycleState.DRAINING.value,
                )
            except asyncio.CancelledError:
                failures.append("drain_cancelled")
                halt_reason = "drain_cancelled"
                log.warning(
                    "session_shutdown_phase_cancelled",
                    run_id=self._run_id,
                    phase=SessionLifecycleState.DRAINING.value,
                )
            except Exception as exc:
                err_msg = f"drain_error:{type(exc).__name__}"
                failures.append(err_msg)
                halt_reason = err_msg
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
                    max(0.01, total_deadline - perf_counter()),
                )
                async with asyncio.timeout(persist_timeout):
                    if self._save_final_checkpoint is not None:
                        try:
                            saved = await self._save_final_checkpoint(persist_timeout)
                            if saved:
                                checkpoint_durable = True
                            else:
                                checkpoint_durable = False
                                failures.append("checkpoint_save_returned_false")
                        except asyncio.CancelledError:
                            checkpoint_durable = False
                            failures.append("checkpoint_cancelled")
                            raise
                        except Exception as exc:
                            checkpoint_durable = False
                            failures.append(f"checkpoint_error:{type(exc).__name__}")
                            log.exception(
                                "session_checkpoint_save_failed",
                                run_id=self._run_id,
                            )

                    if not checkpoint_durable and halt_reason is None:
                        halt_reason = "final_checkpoint_failed"

                    if self._transition_terminal_state is not None:
                        await self._transition_terminal_state(halt_reason)
                        terminal_recorded = True
            except TimeoutError:
                failures.append("persist_timeout")
                checkpoint_durable = False
                if halt_reason is None:
                    halt_reason = "final_checkpoint_failed"
                log.warning(
                    "session_shutdown_phase_timed_out",
                    run_id=self._run_id,
                    phase=SessionLifecycleState.PERSISTING.value,
                )
            except asyncio.CancelledError:
                failures.append("persist_cancelled")
                checkpoint_durable = False
                if halt_reason is None:
                    halt_reason = "persist_cancelled"
                log.warning(
                    "session_shutdown_phase_cancelled",
                    run_id=self._run_id,
                    phase=SessionLifecycleState.PERSISTING.value,
                )
            except Exception as exc:
                err_msg = f"persist_error:{type(exc).__name__}"
                if f"checkpoint_error:{type(exc).__name__}" not in failures:
                    failures.append(err_msg)
                if halt_reason is None:
                    halt_reason = err_msg
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
                    max(0.01, total_deadline - perf_counter()),
                )
                async with asyncio.timeout(close_timeout):
                    close_res = await self._lifecycle.close()
                    lifecycle_failures: tuple[str, ...] = ()
                    if isinstance(close_res, (list, tuple)):
                        lifecycle_failures = tuple(close_res)
                    elif hasattr(self._lifecycle, "close_failures"):
                        lifecycle_failures = tuple(self._lifecycle.close_failures)

                    if lifecycle_failures:
                        failures.extend(lifecycle_failures)
                        resources_closed = False
                        if halt_reason is None:
                            halt_reason = f"close_error:{lifecycle_failures[0]}"
                    else:
                        resources_closed = True

                    if self._ownership_registry is not None:
                        await self._ownership_registry.teardown_all(
                            deadline=total_deadline
                        )
            except TimeoutError:
                failures.append("close_timeout")
                resources_closed = False
                log.warning(
                    "session_shutdown_phase_timed_out",
                    run_id=self._run_id,
                    phase=SessionLifecycleState.CLOSING.value,
                )
            except asyncio.CancelledError:
                failures.append("close_cancelled")
                resources_closed = False
                log.warning(
                    "session_shutdown_phase_cancelled",
                    run_id=self._run_id,
                    phase=SessionLifecycleState.CLOSING.value,
                )
            except Exception as exc:
                failures.append(f"close_error:{type(exc).__name__}")
                resources_closed = False
                log.exception(
                    "session_shutdown_phase_failed",
                    run_id=self._run_id,
                    phase=SessionLifecycleState.CLOSING.value,
                )
        finally:
            # Phase 4: STOPPED
            self._state = SessionLifecycleState.STOPPED
            if self._health is not None:
                try:
                    self._health.stopped()
                except Exception:
                    log.exception("session_health_stopped_marker_failed")

            async with self._state_lock:
                self._closed = True

            if not checkpoint_durable and halt_reason is None:
                halt_reason = "final_checkpoint_failed"
            elif halt_reason is None and failures:
                halt_reason = failures[0]

            duration = round(perf_counter() - started_at, 3)
            self._shutdown_result = ShutdownResult(

                run_id=self._run_id,
                drained=drained,
                checkpoint_durable=checkpoint_durable,
                terminal_recorded=terminal_recorded,
                resources_closed=resources_closed,
                failures=tuple(failures),
                halt_reason=halt_reason,
                duration_seconds=duration,
            )

            log.info(
                "session_shutdown_completed",
                run_id=self._run_id,
                checkpoint_durable=checkpoint_durable,
                halt_reason=halt_reason,
                failures=failures,
                duration_seconds=duration,
            )


__all__ = [
    "RegisteredResource",
    "ResourceOwnershipRegistry",
    "RuntimeSession",
    "SessionLifecycleState",
    "ShutdownResult",
]
