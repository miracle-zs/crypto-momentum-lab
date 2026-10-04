"""Ordered market-state context prefetching for the live decision loop.

The prefetcher overlaps the next context read with the current decision while
preserving strict market-state order.  A caller supplies the current context
generation so a context invalidated by an account event or completed order is
never mistaken for an authorization for a later decision.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterable, Callable
from dataclasses import dataclass
from datetime import datetime

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.live_rollout.context import (
    LiveContextProvider,
    LiveDaemonRuntimeContext,
    PrefetchedContext,
)


@dataclass(slots=True)
class _PendingContext:
    state: MarketState15s
    generation: int
    received_at: datetime
    task: asyncio.Task[LiveDaemonRuntimeContext] | None


class LiveContextPrefetcher:
    """Overlap context I/O without reordering or leaking provider tasks."""

    def __init__(
        self,
        *,
        context_provider: LiveContextProvider,
        context_generation: Callable[[], int],
        clock: Callable[[], datetime],
    ) -> None:
        self._context_provider = context_provider
        self._context_generation = context_generation
        self._clock = clock

    async def stream(
        self,
        states: AsyncIterable[MarketState15s],
    ) -> AsyncGenerator[PrefetchedContext, None]:
        """Yield states in source order with their prefetched context.

        Provider failures are carried with the corresponding state so the
        market loop can keep strategy indicators warm while remaining
        fail-closed for authorization.  Cancellation closes both the source
        producer and every outstanding provider task.
        """

        queue: asyncio.Queue[_PendingContext | None] = asyncio.Queue(maxsize=2)
        pending_tasks: set[asyncio.Task[LiveDaemonRuntimeContext]] = set()
        producer_error: BaseException | None = None

        async def producer() -> None:
            nonlocal producer_error
            cancelled = False
            try:
                async for state in states:
                    generation = self._context_generation()
                    received_at = self._clock()
                    # Backfill only warms strategy indicators. Loading today's
                    # account at an old market cut can trigger false unmanaged
                    # repairs and serialize replay behind database work.
                    context_task = None
                    if not state.is_backfill:
                        context_task = asyncio.ensure_future(
                            self._context_provider(state)
                        )
                        pending_tasks.add(context_task)
                    await queue.put(
                        _PendingContext(
                            state=state,
                            generation=generation,
                            received_at=received_at,
                            task=context_task,
                        )
                    )
            except asyncio.CancelledError:
                cancelled = True
                raise
            except BaseException as error:
                producer_error = error
            finally:
                if not cancelled:
                    await queue.put(None)

        producer_task = asyncio.create_task(
            producer(),
            name="live-market-state-prefetch",
        )
        try:
            while True:
                pending = await queue.get()
                if pending is None:
                    await producer_task
                    if producer_error is not None:
                        raise producer_error
                    return
                if pending.task is None:
                    yield PrefetchedContext(
                        state=pending.state,
                        generation=pending.generation,
                        received_at=pending.received_at,
                        context=None,
                        error=None,
                    )
                    continue
                try:
                    context = await pending.task
                except Exception as error:
                    yield PrefetchedContext(
                        state=pending.state,
                        generation=pending.generation,
                        received_at=pending.received_at,
                        context=None,
                        error=error,
                    )
                else:
                    yield PrefetchedContext(
                        state=pending.state,
                        generation=pending.generation,
                        received_at=pending.received_at,
                        context=context,
                        error=None,
                    )
                finally:
                    pending_tasks.discard(pending.task)
        finally:
            if not producer_task.done():
                producer_task.cancel()
            for task in pending_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(
                producer_task,
                *pending_tasks,
                return_exceptions=True,
            )
