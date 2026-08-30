"""Background cache for live entry-pool and closed-15m EMA data.

The live strategy should make an entry decision from already available
snapshots.  Network I/O belongs in this module's warmup loop, where failures
can fail closed without holding up account-event exits or the market-state
consumer.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime

import structlog

from crypto_momentum_lab.domain.universe.models import UniverseSnapshot
from crypto_momentum_lab.strategy_runner.candle_source import (
    ClosedCandleEmaProvider,
    ClosedCandleEmaSnapshot,
)

log = structlog.get_logger()

SymbolLoader = Callable[[datetime], Awaitable[frozenset[str]]]
UniverseLoader = Callable[
    [datetime],
    Awaitable["LiveEntryUniverseData | None"],
]
Clock = Callable[[], datetime]
ReadyCallback = Callable[[bool], None]


@dataclass(frozen=True, slots=True)
class LiveEntryUniverseData:
    """Cached universe data used by the live entry filter and recorder."""

    symbols: frozenset[str]
    snapshot: UniverseSnapshot | None


def universe_context_for(
    data: LiveEntryUniverseData | None,
    *,
    symbol: str,
    entry_pool_name: str,
    entry_pool_top_count: int,
) -> dict[str, object] | None:
    """Build a JSON-friendly ranking context from a cached snapshot."""

    if data is None or data.snapshot is None:
        return None
    normalized_symbol = symbol.strip().upper()
    snapshot = data.snapshot
    candidate = next(
        (
            item
            for item in snapshot.ranking.candidates
            if item.symbol == normalized_symbol
        ),
        None,
    )
    gainer_ranks = {
        item.symbol: item.rank for item in snapshot.ranking.gainers
    }
    loser_ranks = {
        item.symbol: item.rank for item in snapshot.ranking.losers
    }
    gainer_rank = gainer_ranks.get(normalized_symbol)
    loser_rank = loser_ranks.get(normalized_symbol)
    if gainer_rank is not None and loser_rank is not None:
        ranking_side = "both"
    elif gainer_rank is not None:
        ranking_side = "gainer"
    elif loser_rank is not None:
        ranking_side = "loser"
    else:
        ranking_side = None
    daily_return = next(
        (
            item.utc_day_return
            for item in (*snapshot.ranking.gainers, *snapshot.ranking.losers)
            if item.symbol == normalized_symbol
        ),
        None,
    )
    if (
        daily_return is None
        and candidate is not None
        and candidate.open_price is not None
        and candidate.current_price is not None
        and candidate.open_price > 0
    ):
        daily_return = (
            candidate.current_price - candidate.open_price
        ) / candidate.open_price
    return {
        "snapshot_id": str(snapshot.snapshot_id),
        "snapshot_observed_at": snapshot.observed_at,
        "snapshot_config_hash": snapshot.config_hash,
        "snapshot_activated": snapshot.activated,
        "utc_day": snapshot.utc_day,
        "symbol": normalized_symbol,
        "daily_open_price": (
            None if candidate is None else candidate.open_price
        ),
        "daily_current_price": (
            None if candidate is None else candidate.current_price
        ),
        "daily_price_time": (
            None if candidate is None else candidate.price_time
        ),
        "utc_day_return": daily_return,
        "utc_day_return_pct": (
            None if daily_return is None else daily_return * 100
        ),
        "gainer_rank": gainer_rank,
        "loser_rank": loser_rank,
        "ranking_side": ranking_side,
        "ranking_population_size": len(snapshot.ranking.candidates),
        "is_target": normalized_symbol in snapshot.ranking.target_symbols,
        "exclusion_reason": snapshot.ranking.exclusions.get(normalized_symbol),
        "entry_pool_name": entry_pool_name,
        "entry_pool_top_count": entry_pool_top_count,
        "in_entry_pool": normalized_symbol in data.symbols,
    }


@dataclass(frozen=True, slots=True)
class EntryFilterCacheConfig:
    refresh_interval_seconds: float = 15.0
    prefetch_concurrency: int = 4

    def __post_init__(self) -> None:
        if self.refresh_interval_seconds <= 0:
            raise ValueError("refresh_interval_seconds must be positive")
        if self.prefetch_concurrency <= 0:
            raise ValueError("prefetch_concurrency must be positive")


@dataclass(frozen=True, slots=True)
class EntryFilterCacheMetrics:
    refresh_count: int
    refresh_failure_count: int
    prefetched_snapshot_count: int
    prefetch_failure_count: int
    tracked_symbol_count: int
    ready: bool
    last_refresh_at: datetime | None
    last_refresh_duration_seconds: float | None


class LiveEntryFilterCache:
    """Deep module exposing only memory reads to the live strategy.

    Interface invariants:

    * ``snapshot_for`` never performs I/O and returns ``None`` when the exact
      closed-15m boundary has not been warmed.
    * ``symbols_for`` never performs I/O and returns the latest pool snapshot
      at or before the requested 15-second bucket.
    * the refresh loop owns all calls to the synchronous candle provider and
      limits their concurrency so REST backfill cannot consume the strategy's
      event-loop time.
    * a failed refresh never replaces a previously known pool with an empty
      pool, but an unwarmed EMA boundary still fails closed per symbol.
    """

    def __init__(
        self,
        *,
        ema_provider: ClosedCandleEmaProvider,
        symbol_loader: SymbolLoader | None = None,
        universe_loader: UniverseLoader | None = None,
        config: EntryFilterCacheConfig | None = None,
        clock: Clock | None = None,
        on_ready: ReadyCallback | None = None,
    ) -> None:
        if symbol_loader is None and universe_loader is None:
            raise ValueError("symbol_loader or universe_loader is required")
        if symbol_loader is not None and universe_loader is not None:
            raise ValueError("symbol_loader and universe_loader are exclusive")
        self._ema_provider = ema_provider
        self._symbol_loader = symbol_loader
        self._universe_loader = universe_loader
        self._config = config or EntryFilterCacheConfig()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._on_ready = on_ready
        self._provider_lock = threading.Lock()
        self._stop_event = asyncio.Event()
        self._run_task: asyncio.Task[None] | None = None
        self._ready = False
        self._symbols_by_bucket: dict[datetime, frozenset[str]] = {}
        self._universe_data_by_bucket: dict[
            datetime,
            LiveEntryUniverseData,
        ] = {}
        self._snapshots: dict[
            tuple[str, datetime],
            ClosedCandleEmaSnapshot,
        ] = {}
        self._refresh_count = 0
        self._refresh_failure_count = 0
        self._prefetched_snapshot_count = 0
        self._prefetch_failure_count = 0
        self._last_refresh_at: datetime | None = None
        self._last_refresh_duration_seconds: float | None = None

    @property
    def ready(self) -> bool:
        return self._ready

    @property
    def metrics(self) -> EntryFilterCacheMetrics:
        return EntryFilterCacheMetrics(
            refresh_count=self._refresh_count,
            refresh_failure_count=self._refresh_failure_count,
            prefetched_snapshot_count=self._prefetched_snapshot_count,
            prefetch_failure_count=self._prefetch_failure_count,
            tracked_symbol_count=len(
                {
                    symbol
                    for symbol, _bucket in self._snapshots
                }
            ),
            ready=self._ready,
            last_refresh_at=self._last_refresh_at,
            last_refresh_duration_seconds=self._last_refresh_duration_seconds,
        )

    def set_ready_callback(self, callback: ReadyCallback | None) -> None:
        self._on_ready = callback

    def symbols_for(self, observed_at: datetime) -> frozenset[str]:
        bucket = _bucket_start_15s(observed_at)
        candidates = [
            key for key in self._symbols_by_bucket if key <= bucket
        ]
        if not candidates:
            return frozenset()
        return self._symbols_by_bucket[max(candidates)]

    def universe_data_for(
        self,
        observed_at: datetime,
    ) -> LiveEntryUniverseData | None:
        bucket = _bucket_start_15s(observed_at)
        candidates = [
            key for key in self._universe_data_by_bucket if key <= bucket
        ]
        if not candidates:
            return None
        return self._universe_data_by_bucket[max(candidates)]

    def snapshot_for(
        self,
        *,
        symbol: str,
        observed_at: datetime,
    ) -> ClosedCandleEmaSnapshot | None:
        return self._snapshots.get(
            (symbol.strip().upper(), _candle_start_15m(observed_at))
        )

    async def run(self) -> None:
        """Refresh the pool and its EMA snapshots until stopped."""

        if self._run_task is not None:
            raise RuntimeError("entry filter cache is already running")
        self._run_task = asyncio.current_task()
        try:
            while not self._stop_event.is_set():
                await self._refresh(self._clock())
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._config.refresh_interval_seconds,
                    )
                except TimeoutError:
                    continue
        finally:
            self._run_task = None

    async def stop(self) -> None:
        """Stop after an in-flight background refresh finishes safely."""

        self._stop_event.set()
        task = self._run_task
        current = asyncio.current_task()
        if task is not None and task is not current:
            await task

    async def _refresh(self, observed_at: datetime) -> None:
        started = time.monotonic()
        self._refresh_count += 1
        try:
            universe_data: LiveEntryUniverseData | None = None
            if self._universe_loader is not None:
                universe_data = await self._universe_loader(observed_at)
                if universe_data is None:
                    universe_data = LiveEntryUniverseData(
                        symbols=frozenset(),
                        snapshot=None,
                    )
                symbols = universe_data.symbols
            else:
                symbol_loader = self._symbol_loader
                assert symbol_loader is not None
                symbols = await symbol_loader(observed_at)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._refresh_failure_count += 1
            self._log_refresh_failure("symbol_pool", error)
            return

        bucket = _bucket_start_15s(observed_at)
        loaded, failed = await self._prefetch(symbols, observed_at)
        self._symbols_by_bucket[bucket] = symbols
        if universe_data is not None:
            self._universe_data_by_bucket[bucket] = universe_data
        self._prune_pool_history(bucket)
        self._ready = bool(symbols)
        self._last_refresh_at = observed_at
        self._last_refresh_duration_seconds = time.monotonic() - started
        self._prefetched_snapshot_count += loaded
        self._prefetch_failure_count += failed
        log.info(
            "live_entry_filter_cache_refreshed",
            symbols=len(symbols),
            snapshots_loaded=loaded,
            snapshot_failures=failed,
            ready=self._ready,
            duration_ms=round(self._last_refresh_duration_seconds * 1000, 1),
        )
        if self._on_ready is not None:
            self._on_ready(self._ready)

    async def _prefetch(
        self,
        symbols: frozenset[str],
        observed_at: datetime,
    ) -> tuple[int, int]:
        boundary = _candle_start_15m(observed_at)
        missing = tuple(
            sorted(
                symbol.strip().upper()
                for symbol in symbols
                if (symbol.strip().upper(), boundary) not in self._snapshots
            )
        )
        if not missing:
            return 0, 0
        semaphore = asyncio.Semaphore(self._config.prefetch_concurrency)

        async def load(symbol: str) -> bool:
            async with semaphore:
                try:
                    snapshot = await asyncio.to_thread(
                        self._load_snapshot,
                        symbol,
                        observed_at,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    log.warning(
                        "live_entry_filter_snapshot_failed",
                        symbol=symbol,
                        boundary=boundary.isoformat(),
                        error_type=type(error).__name__,
                    )
                    return False
                self._snapshots[(symbol, boundary)] = snapshot
                self._prune_snapshot_history(boundary)
                return True

        results = await asyncio.gather(*(load(symbol) for symbol in missing))
        loaded = sum(results)
        return loaded, len(results) - loaded

    def _load_snapshot(
        self,
        symbol: str,
        observed_at: datetime,
    ) -> ClosedCandleEmaSnapshot:
        # ClosedCandleEmaProvider owns a mutable range cache and a synchronous
        # HTTP client. Keep those internals out of concurrent worker threads;
        # the semaphore above still bounds task creation and the whole refresh
        # remains off the live event loop.
        with self._provider_lock:
            return self._ema_provider.load(
                symbol=symbol,
                observed_at=observed_at,
            )

    def _prune_pool_history(self, latest_bucket: datetime) -> None:
        if len(self._symbols_by_bucket) <= 32:
            return
        retained = sorted(self._symbols_by_bucket)[-32:]
        retained_set = set(retained)
        self._symbols_by_bucket = {
            key: value
            for key, value in self._symbols_by_bucket.items()
            if key in retained_set
        }
        self._universe_data_by_bucket = {
            key: value
            for key, value in self._universe_data_by_bucket.items()
            if key in retained_set
        }

    def _prune_snapshot_history(self, latest_boundary: datetime) -> None:
        cutoff = latest_boundary.timestamp() - 15 * 60 * 32
        self._snapshots = {
            key: value
            for key, value in self._snapshots.items()
            if key[1].timestamp() >= cutoff
        }

    @staticmethod
    def _log_refresh_failure(stage: str, error: Exception) -> None:
        log.warning(
            "live_entry_filter_cache_refresh_failed",
            stage=stage,
            error_type=type(error).__name__,
        )


class LiveEntrySymbolCache:
    """Refresh the live entry symbol pool without blocking state evaluation."""

    def __init__(
        self,
        *,
        symbol_loader: SymbolLoader | None = None,
        universe_loader: UniverseLoader | None = None,
        config: EntryFilterCacheConfig | None = None,
        clock: Clock | None = None,
        on_ready: ReadyCallback | None = None,
    ) -> None:
        if symbol_loader is None and universe_loader is None:
            raise ValueError("symbol_loader or universe_loader is required")
        if symbol_loader is not None and universe_loader is not None:
            raise ValueError("symbol_loader and universe_loader are exclusive")
        self._symbol_loader = symbol_loader
        self._universe_loader = universe_loader
        self._config = config or EntryFilterCacheConfig()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._on_ready = on_ready
        self._stop_event = asyncio.Event()
        self._run_task: asyncio.Task[None] | None = None
        self._ready = False
        self._symbols_by_bucket: dict[datetime, frozenset[str]] = {}
        self._universe_data_by_bucket: dict[
            datetime,
            LiveEntryUniverseData,
        ] = {}
        self._refresh_count = 0
        self._refresh_failure_count = 0

    @property
    def ready(self) -> bool:
        return self._ready

    def set_ready_callback(self, callback: ReadyCallback | None) -> None:
        self._on_ready = callback

    def symbols_for(self, observed_at: datetime) -> frozenset[str]:
        bucket = _bucket_start_15s(observed_at)
        candidates = [key for key in self._symbols_by_bucket if key <= bucket]
        if not candidates:
            return frozenset()
        return self._symbols_by_bucket[max(candidates)]

    def universe_data_for(
        self,
        observed_at: datetime,
    ) -> LiveEntryUniverseData | None:
        bucket = _bucket_start_15s(observed_at)
        candidates = [
            key for key in self._universe_data_by_bucket if key <= bucket
        ]
        if not candidates:
            return None
        return self._universe_data_by_bucket[max(candidates)]

    async def run(self) -> None:
        if self._run_task is not None:
            raise RuntimeError("entry symbol cache is already running")
        self._run_task = asyncio.current_task()
        try:
            while not self._stop_event.is_set():
                await self._refresh(self._clock())
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._config.refresh_interval_seconds,
                    )
                except TimeoutError:
                    continue
        finally:
            self._run_task = None

    async def stop(self) -> None:
        self._stop_event.set()
        task = self._run_task
        current = asyncio.current_task()
        if task is not None and task is not current:
            await task

    async def _refresh(self, observed_at: datetime) -> None:
        self._refresh_count += 1
        try:
            universe_data: LiveEntryUniverseData | None = None
            if self._universe_loader is not None:
                universe_data = await self._universe_loader(observed_at)
                if universe_data is None:
                    universe_data = LiveEntryUniverseData(
                        symbols=frozenset(),
                        snapshot=None,
                    )
                symbols = universe_data.symbols
            else:
                symbol_loader = self._symbol_loader
                assert symbol_loader is not None
                symbols = await symbol_loader(observed_at)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._refresh_failure_count += 1
            log.warning(
                "live_entry_symbol_cache_refresh_failed",
                error_type=type(error).__name__,
            )
            return
        bucket = _bucket_start_15s(observed_at)
        self._symbols_by_bucket[bucket] = symbols
        if universe_data is not None:
            self._universe_data_by_bucket[bucket] = universe_data
        if len(self._symbols_by_bucket) > 32:
            retained = sorted(self._symbols_by_bucket)[-32:]
            retained_set = set(retained)
            self._symbols_by_bucket = {
                key: value
                for key, value in self._symbols_by_bucket.items()
                if key in retained_set
            }
            self._universe_data_by_bucket = {
                key: value
                for key, value in self._universe_data_by_bucket.items()
                if key in retained_set
            }
        ready = bool(symbols)
        changed = ready != self._ready
        self._ready = ready
        log.info(
            "live_entry_symbol_cache_refreshed",
            symbols=len(symbols),
            ready=ready,
            refresh_count=self._refresh_count,
        )
        if changed and self._on_ready is not None:
            self._on_ready(ready)


def _bucket_start_15s(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    utc_value = value.astimezone(UTC)
    return utc_value.replace(
        second=(utc_value.second // 15) * 15,
        microsecond=0,
    )


def _candle_start_15m(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    utc_value = value.astimezone(UTC)
    return utc_value.replace(
        minute=utc_value.minute - utc_value.minute % 15,
        second=0,
        microsecond=0,
    )
