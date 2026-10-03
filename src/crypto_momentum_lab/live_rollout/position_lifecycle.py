"""Per-position asynchronous actors for the live order lifecycle."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey

T = TypeVar("T")


def live_symbol_position_key(account_label: str, symbol: str) -> PositionKey:
    """One lifecycle owner per live account and symbol, across all batches."""
    return PositionKey(
        environment="live",
        account_label=account_label,
        symbol=symbol,
        position_side=FuturesPositionSide.BOTH,
    )


@dataclass(slots=True)
class _Message[T]:
    operation: Callable[[], Awaitable[T]]
    result: asyncio.Future[T]


class _PositionActor:
    def __init__(self, key: PositionKey) -> None:
        self.key = key
        self.queue: asyncio.Queue[_Message[Any]] = asyncio.Queue()
        self.worker: asyncio.Task[None] | None = None

    async def run(self, operation: Callable[[], Awaitable[T]]) -> T:
        loop = asyncio.get_running_loop()
        result: asyncio.Future[T] = loop.create_future()
        self.queue.put_nowait(_Message(operation, result))
        if self.worker is None:
            self.worker = asyncio.create_task(
                self._consume(),
                name=(
                    f"position-lifecycle:{self.key.account_label}:"
                    f"{self.key.symbol}:{self.key.position_side.value}"
                ),
            )
        return await result

    async def drain(self) -> None:
        await self.queue.join()
        worker = self.worker
        if worker is not None:
            await worker

    async def _consume(self) -> None:
        while not self.queue.empty():
            message = self.queue.get_nowait()
            try:
                value = await message.operation()
            except asyncio.CancelledError:
                if not message.result.done():
                    message.result.cancel()
                raise
            except Exception as error:
                if not message.result.done():
                    message.result.set_exception(error)
            else:
                if not message.result.done():
                    message.result.set_result(value)
            finally:
                self.queue.task_done()
        self.worker = None


class PositionLifecycleActors:
    """Serialize complete order work for each live account and symbol."""

    def __init__(self) -> None:
        self._actors: dict[PositionKey, _PositionActor] = {}
        self._closed = False

    @property
    def actor_count(self) -> int:
        return len(self._actors)

    async def run(
        self,
        key: PositionKey,
        operation: Callable[[], Awaitable[T]],
    ) -> T:
        if self._closed:
            raise RuntimeError("position lifecycle actors are closed")
        actor = self._actors.get(key)
        if actor is None:
            actor = _PositionActor(key)
            self._actors[key] = actor
        return await actor.run(operation)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._drain()
        self._actors.clear()

    async def drain(self) -> None:
        """Finish queued work and release idle actor state while remaining reusable."""
        if self._closed:
            raise RuntimeError("position lifecycle actors are closed")
        await self._drain()
        self._actors.clear()

    async def _drain(self) -> None:
        await asyncio.gather(*(actor.drain() for actor in self._actors.values()))
