"""Per-position asynchronous actors for the live order lifecycle."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

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


class PositionLifecycleLocks:
    """Mutual exclusion for complete lifecycle work; no queue or worker task."""

    def __init__(self) -> None:
        self._locks: dict[PositionKey, asyncio.Lock] = {}
        self._closed = False
        self._draining = False
        self._active = 0
        self._idle = asyncio.Event()
        self._idle.set()

    async def run(
        self, key: PositionKey, operation: Callable[[], Awaitable[T]]
    ) -> T:
        if self._closed or self._draining:
            raise RuntimeError("position lifecycle is closed or draining")
        lock = self._locks.setdefault(key, asyncio.Lock())
        self._active += 1
        self._idle.clear()
        try:
            async with lock:
                return await operation()
        finally:
            self._active -= 1
            if self._active == 0:
                self._idle.set()

    async def close(self) -> None:
        self._closed = True
        await self._idle.wait()
        self._locks.clear()

    async def drain(self) -> None:
        if self._closed:
            raise RuntimeError("position lifecycle is closed")
        self._draining = True
        try:
            await self._idle.wait()
            self._locks.clear()
        finally:
            self._draining = False
