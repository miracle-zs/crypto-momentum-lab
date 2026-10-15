"""Supervise and shut down the process-level live runtime tasks."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from time import perf_counter
from typing import Any

import structlog

from crypto_momentum_lab.live_rollout.market_runtime_contracts import LiveDaemonResult

log = structlog.get_logger()

_DEFAULT_SHUTDOWN_TIMEOUT_SECONDS = 15.0
_SHUTDOWN_PHASE_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True, slots=True)
class LiveRuntimeTasks:
    """Tasks owned by one live worker after all channels are assembled."""

    market: asyncio.Task[LiveDaemonResult]
    account: asyncio.Task[None]
    reconcile: asyncio.Task[None]
    startup_market: asyncio.Task[None] | None = None
    quote: asyncio.Task[None] | None = None
    closed_candle: asyncio.Task[None] | None = None
    grace_timeout: asyncio.Task[None] | None = None
    risk_control: asyncio.Task[None] | None = None
    entry_filter_cache: asyncio.Task[None] | None = None
    entry_symbol_cache: asyncio.Task[None] | None = None
    local_health: asyncio.Task[None] | None = None
    account_producer: asyncio.Task[None] | None = None
    shutdown: asyncio.Task[bool] | None = None

    def critical_task_roles(self) -> tuple[tuple[asyncio.Task[Any], str], ...]:
        """Return every task whose unexpected completion stops the worker."""

        roles = [
            (self.market, "market runtime"),
            (self.account, "account event channel"),
            (self.reconcile, "live order reconcile task"),
        ]
        optional_roles = (
            (self.startup_market, "startup market buffer"),
            (self.account_producer, "account stream producer"),
            (self.risk_control, "risk-control channel"),
            (self.quote, "market quote channel"),
            (self.closed_candle, "closed candle exit channel"),
            (self.grace_timeout, "grace timeout exit channel"),
            (self.entry_filter_cache, "live entry filter cache task"),
            (self.entry_symbol_cache, "live entry symbol cache task"),
            (self.local_health, "local health monitor"),
        )
        roles.extend(
            (task, label) for task, label in optional_roles if task is not None
        )
        return tuple(roles)

    def monitored(self) -> set[asyncio.Task[Any]]:
        """Return tasks whose unexpected completion stops the worker."""

        tasks = {task for task, _label in self.critical_task_roles()}
        if self.shutdown is not None:
            tasks.add(self.shutdown)
        return tasks

    def all_tasks(self) -> tuple[asyncio.Task[Any], ...]:
        """Return every task that must be joined during shutdown."""

        return tuple(
            task
            for task in (
                self.market,
                self.startup_market,
                self.account,
                self.account_producer,
                self.risk_control,
                self.quote,
                self.closed_candle,
                self.grace_timeout,
                self.reconcile,
                self.local_health,
                self.entry_filter_cache,
                self.entry_symbol_cache,
                self.shutdown,
            )
            if task is not None
        )


class LiveRuntimeSupervisor:
    """Own fail-fast monitoring and ordered shutdown for live tasks.

    The supervisor deliberately knows about task roles, but not about their
    business implementations.  This keeps the safety ordering at one seam:
    stop accepting new entries, stop event producers, cancel workers, close
    control-plane state, stop cache loops, and finally join every task.
    """

    def __init__(
        self,
        *,
        tasks: LiveRuntimeTasks,
        block_entry_submissions: Callable[[], None],
        stop_sources: Callable[[], None],
        close_risk_control: Callable[[], Awaitable[None]],
        stop_entry_caches: Callable[[], Awaitable[None]],
        wait_for_entry_submissions_idle: Callable[[], Awaitable[None]] | None = None,
        run_id: str = "unknown",
        shutdown_timeout_seconds: float = _DEFAULT_SHUTDOWN_TIMEOUT_SECONDS,
    ) -> None:
        if not run_id.strip():
            raise ValueError("run_id must not be empty")
        if shutdown_timeout_seconds <= 0:
            raise ValueError("shutdown_timeout_seconds must be positive")
        self._run_id = run_id
        self._tasks = tasks
        self._block_entry_submissions = block_entry_submissions
        self._stop_sources = stop_sources
        self._close_risk_control = close_risk_control
        self._stop_entry_caches = stop_entry_caches
        self._wait_for_entry_submissions_idle = wait_for_entry_submissions_idle
        self._shutdown_timeout_seconds = shutdown_timeout_seconds
        self._stopped = False

    async def run(self) -> LiveDaemonResult:
        """Wait for the first critical task to stop and return market result."""

        done, _ = await asyncio.wait(
            self._tasks.monitored(),
            return_when=asyncio.FIRST_COMPLETED,
        )
        if self._tasks.shutdown is not None and self._tasks.shutdown in done:
            log.info("live_shutdown_requested", run_id=self._run_id)
            return LiveDaemonResult(0, 0, 0, "shutdown_requested", None)
        await self._raise_if_unexpected_completion()
        try:
            return await self._tasks.market
        except Exception as error:
            log.exception(
                "live_runtime_market_task_failed",
                run_id=self._run_id,
                task_name=self._tasks.market.get_name(),
                error_type=type(error).__name__,
            )
            raise

    async def stop(self) -> None:
        """Stop all owned tasks in the safety-critical order exactly once."""

        if self._stopped:
            return
        self._stopped = True
        deadline = perf_counter() + self._shutdown_timeout_seconds

        try:
            self._block_entry_submissions()
        except Exception:
            log.exception("live_shutdown_block_entries_failed")
        try:
            self._stop_sources()
        except Exception:
            log.exception("live_shutdown_stop_sources_failed")

        self._cancel(self._tasks.market)
        self._cancel(self._tasks.startup_market)
        self._cancel(self._tasks.account)
        self._cancel(self._tasks.account_producer)
        self._cancel(self._tasks.risk_control)
        self._cancel(self._tasks.quote)
        self._cancel(self._tasks.closed_candle)
        self._cancel(self._tasks.grace_timeout)
        self._cancel(self._tasks.reconcile)
        self._cancel(self._tasks.local_health)
        self._cancel(self._tasks.entry_filter_cache)
        self._cancel(self._tasks.entry_symbol_cache)
        self._cancel(self._tasks.shutdown)

        if self._wait_for_entry_submissions_idle is not None:
            await self._run_phase(
                "entry_submission_drain",
                self._wait_for_entry_submissions_idle,
                deadline,
            )
        await self._run_phase(
            "risk_control_close",
            self._close_risk_control,
            deadline,
        )
        await self._run_phase(
            "entry_cache_stop",
            self._stop_entry_caches,
            deadline,
        )

        remaining = deadline - perf_counter()
        if remaining <= 0:
            log.warning(
                "live_runtime_shutdown_timed_out",
                phase="task_join",
                timeout_seconds=self._shutdown_timeout_seconds,
            )
            return
        try:
            async with asyncio.timeout(remaining):
                await asyncio.gather(
                    *self._tasks.all_tasks(),
                    return_exceptions=True,
                )
        except TimeoutError:
            log.warning(
                "live_runtime_shutdown_timed_out",
                phase="task_join",
                timeout_seconds=self._shutdown_timeout_seconds,
            )
        else:
            log.info(
                "live_runtime_shutdown_completed",
                duration_seconds=round(
                    self._shutdown_timeout_seconds
                    - max(0.0, deadline - perf_counter()),
                    3,
                ),
            )

    async def _run_phase(
        self,
        label: str,
        operation: Callable[[], Awaitable[None]],
        deadline: float,
    ) -> None:
        remaining = deadline - perf_counter()
        if remaining <= 0:
            log.warning(
                "live_shutdown_phase_timed_out",
                phase=label,
                timeout_seconds=0.0,
            )
            return
        timeout_seconds = min(remaining, _SHUTDOWN_PHASE_TIMEOUT_SECONDS)
        started_at = perf_counter()
        try:
            async with asyncio.timeout(timeout_seconds):
                await operation()
        except TimeoutError:
            log.warning(
                "live_shutdown_phase_timed_out",
                phase=label,
                timeout_seconds=round(timeout_seconds, 3),
            )
        except Exception:
            log.exception("live_shutdown_phase_failed", phase=label)
        else:
            log.info(
                "live_shutdown_phase_completed",
                phase=label,
                duration_seconds=round(perf_counter() - started_at, 3),
            )

    async def _raise_if_unexpected_completion(self) -> None:
        for task, label in self._tasks.critical_task_roles():
            if not task.done():
                continue
            try:
                await task
            except asyncio.CancelledError:
                log.error(
                    "live_runtime_critical_task_cancelled",
                    run_id=self._run_id,
                    task_name=task.get_name(),
                    task_role=label,
                )
                raise
            except Exception as error:
                log.exception(
                    "live_runtime_critical_task_failed",
                    run_id=self._run_id,
                    task_name=task.get_name(),
                    task_role=label,
                    error_type=type(error).__name__,
                )
                raise
            log.error(
                "live_runtime_critical_task_stopped",
                run_id=self._run_id,
                task_name=task.get_name(),
                task_role=label,
            )
            raise RuntimeError(f"{label} stopped unexpectedly")

    @staticmethod
    def _cancel(task: asyncio.Task[Any] | None) -> None:
        if task is not None and not task.done():
            task.cancel()


__all__ = ["LiveRuntimeSupervisor", "LiveRuntimeTasks"]
