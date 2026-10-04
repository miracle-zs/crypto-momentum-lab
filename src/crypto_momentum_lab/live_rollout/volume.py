"""Causally safe 24-hour quote-volume snapshots for live signal telemetry.

The live decision loop must not perform a REST request for an auxiliary
feature.  This module consumes shared market-data Hub snapshots
on a background task and exposes only snapshots that had already arrived by
the signal timestamp.  A refresh outage therefore removes a diagnostic field,
not an execution capability.
"""

import asyncio
from collections import deque
from collections.abc import AsyncIterable
from datetime import UTC, datetime
from typing import Protocol

import structlog

from crypto_momentum_lab.market_data.quote_volume import QuoteVolume24hSnapshot

log = structlog.get_logger()

_DEFAULT_HISTORY_SIZE = 128


class QuoteVolume24hProvider(Protocol):
    def snapshot(
        self,
        symbol: str,
        *,
        as_of: datetime,
    ) -> "QuoteVolume24hSnapshot | None": ...

    def metrics_snapshot(
        self,
        *,
        now: datetime | None = None,
    ) -> dict[str, object]: ...


class QuoteVolumeSnapshotSource(AsyncIterable[QuoteVolume24hSnapshot], Protocol):
    """A shared snapshot stream whose owner can stop delivery synchronously."""

    def stop(self) -> None: ...


class WebSocketQuoteVolumeProvider:
    """Consume the market-data hub's shared volume snapshots."""

    def __init__(
        self,
        source: QuoteVolumeSnapshotSource,
        *,
        history_size: int = _DEFAULT_HISTORY_SIZE,
    ) -> None:
        self._source = source
        if history_size <= 0:
            raise ValueError("history_size must be positive")
        self._history_size = history_size
        self._snapshots: dict[str, deque[QuoteVolume24hSnapshot]] = {}
        self.last_refresh_at: datetime | None = None
        self._lookup_hit_count: int = 0
        self._lookup_miss_count: int = 0
        self._task: asyncio.Task[None] | None = None
        self._failure_count = 0

    @property
    def refresh_failure_count(self) -> int:
        return self._failure_count

    @property
    def cached_symbol_count(self) -> int:
        return len(self._snapshots)

    @property
    def total_snapshot_count(self) -> int:
        return sum(len(history) for history in self._snapshots.values())

    @property
    def lookup_hit_count(self) -> int:
        return self._lookup_hit_count

    @property
    def lookup_miss_count(self) -> int:
        return self._lookup_miss_count

    def oldest_snapshot_age_seconds(
        self,
        *,
        now: datetime | None = None,
    ) -> float | None:
        oldest_at: datetime | None = None
        for history in self._snapshots.values():
            if history:
                first = history[0].fetched_at
                if oldest_at is None or first < oldest_at:
                    oldest_at = first
        if oldest_at is None:
            return None
        current = now or datetime.now(tz=UTC)
        return max(0.0, (current - oldest_at).total_seconds())

    def metrics_snapshot(
        self,
        *,
        now: datetime | None = None,
    ) -> dict[str, object]:
        return {
            "cached_symbol_count": self.cached_symbol_count,
            "total_snapshot_count": self.total_snapshot_count,
            "oldest_snapshot_age_seconds": self.oldest_snapshot_age_seconds(now=now),
            "lookup_hit_count": self.lookup_hit_count,
            "lookup_miss_count": self.lookup_miss_count,
        }

    async def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.create_task(
            self._run(),
            name="live-24h-quote-volume-hub",
        )

    async def stop(self) -> None:
        self._source.stop()
        task = self._task
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self._task = None

    def snapshot(
        self,
        symbol: str,
        *,
        as_of: datetime,
    ) -> QuoteVolume24hSnapshot | None:
        if not symbol.strip():
            raise ValueError("symbol must not be empty")
        _require_aware(as_of, "as_of")
        history = self._snapshots.get(symbol.upper())
        if not history:
            self._lookup_miss_count += 1
            return None
        for snapshot in reversed(history):
            if snapshot.fetched_at <= as_of:
                self._lookup_hit_count += 1
                return snapshot
        self._lookup_miss_count += 1
        return None

    def _observe(self, snapshot: QuoteVolume24hSnapshot) -> bool:
        # The live strategy trades USDT-margined perpetuals.  The global
        # endpoint also returns COIN-M/USDC-style symbols, for which a value
        # labelled as USDT would be misleading.
        if not snapshot.symbol.upper().endswith("USDT"):
            return False
        normalized = QuoteVolume24hSnapshot(
            symbol=snapshot.symbol.upper(),
            quote_volume=snapshot.quote_volume,
            source_at=snapshot.source_at,
            fetched_at=snapshot.fetched_at,
            quote_asset=snapshot.quote_asset,
            source=snapshot.source,
            exchange=snapshot.exchange,
            environment=snapshot.environment,
        )
        history = self._snapshots.setdefault(
            normalized.symbol,
            deque(maxlen=self._history_size),
        )
        history.append(normalized)
        self.last_refresh_at = normalized.fetched_at
        return True

    async def _run(self) -> None:
        while True:
            try:
                async for snapshot in self._source:
                    self._observe(snapshot)
            except Exception as error:
                self._failure_count += 1
                log.warning(
                    "live_24h_quote_volume_hub_failed",
                    error_type=type(error).__name__,
                    failure_count=self._failure_count,
                )
                await asyncio.sleep(1.0)


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


__all__ = [
    "QuoteVolume24hProvider",
    "QuoteVolume24hSnapshot",
    "WebSocketQuoteVolumeProvider",
]
