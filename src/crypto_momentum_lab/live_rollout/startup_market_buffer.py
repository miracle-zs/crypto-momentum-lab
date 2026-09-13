"""Buffer live market states while a strategy rebuilds its rolling history."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from datetime import datetime

from crypto_momentum_lab.domain.market.models import MarketState15s


class StartupMarketStateBuffer:
    """Keep the Hub stream lossless across the asynchronous warmup boundary.

    The producer starts before database recovery.  Consumers may start later;
    the queue therefore has a deliberately high, bounded capacity.  Backpressure
    is applied instead of silently dropping states when the queue is full.
    """

    def __init__(self, *, max_states: int) -> None:
        if max_states <= 0:
            raise ValueError("max_states must be positive")
        self._queue: asyncio.Queue[MarketState15s] = asyncio.Queue(
            maxsize=max_states
        )
        self._connection_available = False
        self._connection_reason = "market_state_hub_connecting"
        self._closed = False
        self._closed_event = asyncio.Event()
        self._error: BaseException | None = None

    @property
    def connection_available(self) -> bool:
        return self._connection_available

    @property
    def connection_reason(self) -> str:
        return self._connection_reason

    @property
    def buffered_state_count(self) -> int:
        return self._queue.qsize()

    def observe_connection_change(
        self,
        available: bool,
        reason: str | None,
    ) -> None:
        self._connection_available = available
        self._connection_reason = (
            "market_state_hub_ready"
            if available
            else (reason or "market_state_hub_unavailable")
        )

    async def append(self, state: MarketState15s) -> None:
        if self._closed:
            return
        # Backpressure is deliberate: dropping a state would invalidate the
        # per-symbol rolling window.  The Hub client will fail closed if this
        # consumer remains behind long enough for its own queue to overflow.
        if not self._queue.full():
            self._queue.put_nowait(state)
            return
        put_task = asyncio.create_task(self._queue.put(state))
        closed_task = asyncio.create_task(self._closed_event.wait())
        try:
            done, pending = await asyncio.wait(
                (put_task, closed_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
        except asyncio.CancelledError:
            put_task.cancel()
            closed_task.cancel()
            await asyncio.gather(
                put_task,
                closed_task,
                return_exceptions=True,
            )
            raise
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        if put_task in done:
            put_task.result()
            return
        if self._error is not None:
            raise RuntimeError(
                "live market-state startup buffer stopped"
            ) from self._error

    def close(self, error: BaseException | None = None) -> None:
        if self._closed:
            return
        self._closed = True
        self._error = error
        self._closed_event.set()

    async def _next_item(self) -> MarketState15s | None:
        if not self._queue.empty():
            return await self._queue.get()
        if self._closed:
            return None
        get_task = asyncio.create_task(self._queue.get())
        closed_task = asyncio.create_task(self._closed_event.wait())
        try:
            done, pending = await asyncio.wait(
                (get_task, closed_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
        except asyncio.CancelledError:
            get_task.cancel()
            closed_task.cancel()
            await asyncio.gather(
                get_task,
                closed_task,
                return_exceptions=True,
            )
            raise
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        if get_task in done:
            return get_task.result()
        if not self._queue.empty():
            return await self._queue.get()
        return None

    async def stream(
        self,
        *,
        skip_through: Mapping[str, datetime] | None = None,
    ) -> AsyncIterator[MarketState15s]:
        """Yield buffered/future states, suppressing history duplicates."""

        watermarks = dict(skip_through or {})
        while True:
            if self._error is not None:
                raise RuntimeError(
                    "live market-state startup buffer stopped"
                ) from self._error
            state = await self._next_item()
            if state is None:
                if self._error is not None:
                    raise RuntimeError(
                        "live market-state startup buffer stopped"
                    ) from self._error
                return
            previous = watermarks.get(state.symbol)
            if previous is not None and state.bucket_start <= previous:
                continue
            watermarks[state.symbol] = state.bucket_start
            yield state


__all__ = ["StartupMarketStateBuffer"]
