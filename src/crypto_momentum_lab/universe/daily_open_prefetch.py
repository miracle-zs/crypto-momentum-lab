"""Asynchronously fill daily opens without blocking universe refreshes."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime, time, timedelta
from typing import Protocol

import structlog

from crypto_momentum_lab.domain.universe.ports import (
    UniverseMarketData,
    UniverseRepository,
)

log = structlog.get_logger()


class DailyOpenPrefetchSink(Protocol):
    async def request(
        self,
        symbols: Iterable[str],
        utc_day: date,
    ) -> None: ...


class DailyOpenPrefetcher:
    """Own the slow, retryable daily-open fill workflow.

    Universe refreshes only enqueue the symbols they need.  The worker reads
    and writes in bounded batches, so a day rollover cannot hold the realtime
    price and membership refresh behind hundreds of independent REST calls.
    """

    def __init__(
        self,
        market_data: UniverseMarketData,
        repository: UniverseRepository,
        *,
        batch_size: int = 50,
        retry_delay_seconds: float = 5.0,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must not be negative")
        self._market_data = market_data
        self._repository = repository
        self._batch_size = batch_size
        self._retry_delay_seconds = retry_delay_seconds
        self._clock = clock or (lambda: datetime.now(tz=UTC))
        self._pending: dict[date, set[str]] = {}
        self._active_symbols: frozenset[str] = frozenset()
        self._wake = asyncio.Event()
        self._work_lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._closed = False

    async def start(self) -> None:
        """Start the background worker if it is not already running."""
        if self._task is not None and not self._task.done():
            return
        self._closed = False
        self._task = asyncio.create_task(
            self._run(),
            name="daily-open-prefetcher",
        )

    async def stop(self) -> None:
        """Stop the worker without waiting for a slow REST batch to finish."""
        self._closed = True
        self._wake.set()
        task = self._task
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self._task = None

    async def request(
        self,
        symbols: Iterable[str],
        utc_day: date,
    ) -> None:
        """Queue daily opens for a refresh without performing network I/O."""
        normalized = _normalize_symbols(symbols)
        if not normalized:
            return
        self._active_symbols = normalized
        self._pending.setdefault(utc_day, set()).update(normalized)
        self._wake.set()

    async def bootstrap_current_day(self, observed_at: datetime) -> None:
        """Fill the current active universe once before the first snapshot."""
        observed_at = _normalize_datetime(observed_at)
        contracts = await self._market_data.fetch_active_usdt_perpetuals()
        symbols = frozenset(item.symbol for item in contracts)
        self._active_symbols = symbols
        async with self._work_lock:
            await self._prefetch_symbols(
                symbols,
                observed_at.date(),
                captured_at=observed_at,
            )
        log.info(
            "daily_open_prefetch_bootstrapped",
            utc_day=observed_at.date().isoformat(),
            symbol_count=len(symbols),
        )

    async def prefetch_now(
        self,
        symbols: Iterable[str],
        utc_day: date,
        *,
        captured_at: datetime | None = None,
    ) -> None:
        """Synchronously fill a supplied set, primarily for startup/tests."""
        normalized = _normalize_symbols(symbols)
        self._active_symbols = normalized
        async with self._work_lock:
            await self._prefetch_symbols(
                normalized,
                utc_day,
                captured_at=_normalize_datetime(captured_at or self._clock()),
            )

    async def _run(self) -> None:
        try:
            while not self._closed:
                try:
                    await asyncio.wait_for(
                        self._wake.wait(),
                        timeout=self._seconds_until_next_utc_day(),
                    )
                except TimeoutError:
                    self._enqueue_active_symbols(self._now().date())
                else:
                    self._wake.clear()
                await self._drain_pending()
        except asyncio.CancelledError:
            raise

    async def _drain_pending(self) -> None:
        async with self._work_lock:
            while self._pending and not self._closed:
                utc_day = min(self._pending)
                symbols = self._pending[utc_day]
                batch = frozenset(sorted(symbols, key=str)[: self._batch_size])
                symbols.difference_update(batch)
                if not symbols:
                    del self._pending[utc_day]
                try:
                    await self._prefetch_symbols(
                        batch,
                        utc_day,
                        captured_at=self._now(),
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self._pending.setdefault(utc_day, set()).update(batch)
                    log.exception(
                        "daily_open_prefetch_failed",
                        utc_day=utc_day.isoformat(),
                        symbol_count=len(batch),
                        retry_delay_seconds=self._retry_delay_seconds,
                    )
                    await asyncio.sleep(self._retry_delay_seconds)

    async def _prefetch_symbols(
        self,
        symbols: frozenset[str],
        utc_day: date,
        *,
        captured_at: datetime,
    ) -> None:
        remaining = set(symbols)
        while remaining:
            stored = await self._repository.load_daily_opens(
                utc_day,
                frozenset(remaining),
            )
            remaining.difference_update(stored)
            if not remaining:
                return
            batch = frozenset(sorted(remaining, key=str)[: self._batch_size])
            fetched = await self._market_data.fetch_daily_opens(
                batch,
                utc_day,
            )
            if fetched:
                await self._repository.save_daily_opens(
                    fetched,
                    captured_at=captured_at,
                )
            remaining.difference_update(item.symbol for item in fetched)
            if not fetched:
                return

    def _enqueue_active_symbols(self, utc_day: date) -> None:
        if not self._active_symbols:
            return
        self._pending.setdefault(utc_day, set()).update(self._active_symbols)

    def _seconds_until_next_utc_day(self) -> float:
        now = self._now()
        next_day = datetime.combine(
            now.date() + timedelta(days=1),
            time.min,
            tzinfo=UTC,
        )
        return max(0.1, (next_day - now).total_seconds())

    def _now(self) -> datetime:
        return _normalize_datetime(self._clock())


def _normalize_symbols(symbols: Iterable[str]) -> frozenset[str]:
    return frozenset(symbol.strip().upper() for symbol in symbols if symbol.strip())


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)
