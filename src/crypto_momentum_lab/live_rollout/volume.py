"""Causally safe 24-hour quote-volume snapshots for live signal telemetry.

The live decision loop must not perform a REST request for an auxiliary
feature.  This module refreshes Binance's all-symbol 24-hour ticker endpoint
on a background task and exposes only snapshots that had already arrived by
the signal timestamp.  A refresh outage therefore removes a diagnostic field,
not an execution capability.
"""

import asyncio
from collections import deque
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol

import structlog

from crypto_momentum_lab.market_data.binance.rest import (
    Binance24hTicker,
)
from crypto_momentum_lab.market_data.quote_hub import (
    WebSocketMarketQuoteVolumeSource,
)
from crypto_momentum_lab.market_data.quote_volume import QuoteVolume24hSnapshot

log = structlog.get_logger()

_DEFAULT_REFRESH_INTERVAL_SECONDS = 60.0
_DEFAULT_HISTORY_SIZE = 2_880


class Binance24hTickerClient(Protocol):
    async def fetch_24h_tickers(self) -> dict[str, Binance24hTicker]: ...


class QuoteVolume24hProvider(Protocol):
    def snapshot(
        self,
        symbol: str,
        *,
        as_of: datetime,
    ) -> "QuoteVolume24hSnapshot | None": ...


class _QuoteVolumeHistory:
    def __init__(self, history_size: int) -> None:
        if history_size <= 0:
            raise ValueError("history_size must be positive")
        self._history_size = history_size
        self._snapshots: dict[str, deque[QuoteVolume24hSnapshot]] = {}
        self.last_refresh_at: datetime | None = None

    def observe(self, snapshot: QuoteVolume24hSnapshot) -> bool:
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
            return None
        for snapshot in reversed(history):
            if snapshot.fetched_at <= as_of:
                return snapshot
        return None


class Binance24hQuoteVolumeCache:
    """Keep a bounded, per-symbol history of Binance 24h quote volume."""

    def __init__(
        self,
        client: Binance24hTickerClient,
        *,
        refresh_interval_seconds: float = _DEFAULT_REFRESH_INTERVAL_SECONDS,
        history_size: int = _DEFAULT_HISTORY_SIZE,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if refresh_interval_seconds <= 0:
            raise ValueError("refresh_interval_seconds must be positive")
        if history_size <= 0:
            raise ValueError("history_size must be positive")
        self._client = client
        self._refresh_interval_seconds = refresh_interval_seconds
        self._clock = clock or (lambda: datetime.now(tz=UTC))
        self._history = _QuoteVolumeHistory(history_size)
        self._refresh_task: asyncio.Task[None] | None = None
        self._refresh_failure_count = 0

    @property
    def refresh_failure_count(self) -> int:
        return self._refresh_failure_count

    @property
    def last_refresh_at(self) -> datetime | None:
        return self._history.last_refresh_at

    async def start(self) -> None:
        if self._refresh_task is not None:
            return
        self._refresh_task = asyncio.create_task(
            self._run_refresh_loop(),
            name="live-24h-quote-volume-cache",
        )

    async def stop(self) -> None:
        refresh_task = self._refresh_task
        if refresh_task is None:
            return
        refresh_task.cancel()
        await asyncio.gather(refresh_task, return_exceptions=True)
        self._refresh_task = None

    async def refresh_once(self) -> int:
        """Fetch one complete ticker snapshot and return its symbol count."""

        fetched_at = self._clock()
        _require_aware(fetched_at, "fetched_at")
        tickers = await self._client.fetch_24h_tickers()
        refreshed_count = 0
        for ticker in tickers.values():
            snapshot = QuoteVolume24hSnapshot(
                symbol=ticker.symbol.upper(),
                quote_volume=ticker.quote_volume,
                source_at=ticker.close_time,
                fetched_at=fetched_at,
            )
            refreshed_count += int(self._history.observe(snapshot))
        return refreshed_count

    def snapshot(
        self,
        symbol: str,
        *,
        as_of: datetime,
    ) -> QuoteVolume24hSnapshot | None:
        """Return the newest cache value available at ``as_of``.

        Looking at ``fetched_at`` rather than the ticker's exchange close time
        prevents a later REST response from leaking into an earlier signal.
        """

        return self._history.snapshot(symbol, as_of=as_of)

    async def _run_refresh_loop(self) -> None:
        while True:
            try:
                refreshed_count = await self.refresh_once()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._refresh_failure_count += 1
                log.warning(
                    "live_24h_quote_volume_refresh_failed",
                    error_type=type(error).__name__,
                    failure_count=self._refresh_failure_count,
                )
            else:
                log.debug(
                    "live_24h_quote_volume_refreshed",
                    symbol_count=refreshed_count,
                    fetched_at=self.last_refresh_at,
                )
            await asyncio.sleep(self._refresh_interval_seconds)


class WebSocketQuoteVolumeProvider:
    """Consume the market-data hub's shared volume snapshots."""

    def __init__(
        self,
        source: WebSocketMarketQuoteVolumeSource,
        *,
        history_size: int = _DEFAULT_HISTORY_SIZE,
    ) -> None:
        self._source = source
        self._history = _QuoteVolumeHistory(history_size)
        self._task: asyncio.Task[None] | None = None
        self._failure_count = 0

    @property
    def refresh_failure_count(self) -> int:
        return self._failure_count

    @property
    def last_refresh_at(self) -> datetime | None:
        return self._history.last_refresh_at

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
        return self._history.snapshot(symbol, as_of=as_of)

    async def _run(self) -> None:
        while True:
            try:
                async for snapshot in self._source:
                    self._history.observe(snapshot)
            except asyncio.CancelledError:
                raise
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
    "Binance24hQuoteVolumeCache",
    "QuoteVolume24hProvider",
    "QuoteVolume24hSnapshot",
    "WebSocketQuoteVolumeProvider",
]
