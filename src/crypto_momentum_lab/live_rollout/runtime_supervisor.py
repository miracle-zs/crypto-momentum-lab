"""Supervise and shut down the process-level live runtime tasks."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from crypto_momentum_lab.live_rollout.market_loop import LiveDaemonResult


@dataclass(frozen=True, slots=True)
class LiveRuntimeTasks:
    """Tasks owned by one live worker after all channels are assembled."""

    market: asyncio.Task[LiveDaemonResult]
    account: asyncio.Task[None]
    lease: asyncio.Task[None]
    reconcile: asyncio.Task[None]
    startup_market: asyncio.Task[None] | None = None
    quote: asyncio.Task[None] | None = None
    closed_candle: asyncio.Task[None] | None = None
    grace_timeout: asyncio.Task[None] | None = None
    risk_control: asyncio.Task[None] | None = None
    entry_filter_cache: asyncio.Task[None] | None = None
    entry_symbol_cache: asyncio.Task[None] | None = None
    local_health: asyncio.Task[None] | None = None

    def monitored(self) -> set[asyncio.Task[Any]]:
        """Return tasks whose unexpected completion stops the worker."""

        return {
            self.market,
            self.account,
            self.lease,
            self.reconcile,
            *(() if self.quote is None else (self.quote,)),
            *(
                ()
                if self.closed_candle is None
                else (self.closed_candle,)
            ),
            *(
                ()
                if self.grace_timeout is None
                else (self.grace_timeout,)
            ),
            *(
                ()
                if self.risk_control is None
                else (self.risk_control,)
            ),
            *(
                ()
                if self.entry_filter_cache is None
                else (self.entry_filter_cache,)
            ),
            *(
                ()
                if self.entry_symbol_cache is None
                else (self.entry_symbol_cache,)
            ),
        }

    def all_tasks(self) -> tuple[asyncio.Task[Any], ...]:
        """Return every task that must be joined during shutdown."""

        return tuple(
            task
            for task in (
                self.market,
                self.startup_market,
                self.account,
                self.risk_control,
                self.quote,
                self.closed_candle,
                self.grace_timeout,
                self.lease,
                self.reconcile,
                self.local_health,
                self.entry_filter_cache,
                self.entry_symbol_cache,
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
    ) -> None:
        self._tasks = tasks
        self._block_entry_submissions = block_entry_submissions
        self._stop_sources = stop_sources
        self._close_risk_control = close_risk_control
        self._stop_entry_caches = stop_entry_caches
        self._stopped = False

    async def run(self) -> LiveDaemonResult:
        """Wait for the first critical task to stop and return market result."""

        await asyncio.wait(
            self._tasks.monitored(),
            return_when=asyncio.FIRST_COMPLETED,
        )
        await self._raise_if_unexpected_completion()
        return await self._tasks.market

    async def stop(self) -> None:
        """Stop all owned tasks in the safety-critical order exactly once."""

        if self._stopped:
            return
        self._stopped = True

        self._block_entry_submissions()
        self._stop_sources()

        self._cancel(self._tasks.market)
        self._cancel(self._tasks.startup_market)
        self._cancel(self._tasks.account)
        self._cancel(self._tasks.risk_control)
        await self._close_risk_control()
        self._cancel(self._tasks.quote)
        self._cancel(self._tasks.closed_candle)
        self._cancel(self._tasks.grace_timeout)
        self._cancel(self._tasks.lease)
        self._cancel(self._tasks.reconcile)
        self._cancel(self._tasks.local_health)
        self._cancel(self._tasks.entry_filter_cache)
        self._cancel(self._tasks.entry_symbol_cache)
        await self._stop_entry_caches()
        await asyncio.gather(*self._tasks.all_tasks(), return_exceptions=True)

    async def _raise_if_unexpected_completion(self) -> None:
        checks = (
            (self._tasks.account, "account event channel"),
            (self._tasks.risk_control, "risk-control channel"),
            (self._tasks.quote, "market quote channel"),
            (self._tasks.closed_candle, "closed candle exit channel"),
            (self._tasks.grace_timeout, "grace timeout exit channel"),
            (self._tasks.lease, "live lease heartbeat"),
            (self._tasks.reconcile, "live order reconcile task"),
            (self._tasks.entry_filter_cache, "live entry filter cache task"),
            (self._tasks.entry_symbol_cache, "live entry symbol cache task"),
        )
        for task, label in checks:
            if task is not None and task.done():
                await task
                raise RuntimeError(f"{label} stopped unexpectedly")

    @staticmethod
    def _cancel(task: asyncio.Task[Any] | None) -> None:
        if task is not None and not task.done():
            task.cancel()


__all__ = ["LiveRuntimeSupervisor", "LiveRuntimeTasks"]
