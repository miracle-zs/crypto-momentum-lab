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
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Protocol

import structlog

from crypto_momentum_lab.market_data.binance.rest import (
    Binance24hTicker,
)

log = structlog.get_logger()

_DEFAULT_REFRESH_INTERVAL_SECONDS = 60.0
_DEFAULT_HISTORY_SIZE = 2_880
_BINANCE_24H_TICKER_SOURCE = "binance_fapi_ticker_24hr"


class Binance24hTickerClient(Protocol):
    async def fetch_24h_tickers(self) -> dict[str, Binance24hTicker]: ...


class QuoteVolume24hProvider(Protocol):
    def snapshot(
        self,
        symbol: str,
        *,
        as_of: datetime,
    ) -> "QuoteVolume24hSnapshot | None": ...


@dataclass(frozen=True, slots=True)
class QuoteVolume24hSnapshot:
    symbol: str
    quote_volume: Decimal
    source_at: datetime
    fetched_at: datetime
    quote_asset: str = "USDT"
    source: str = _BINANCE_24H_TICKER_SOURCE

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValueError("symbol must not be empty")
        if self.quote_volume < 0:
            raise ValueError("quote_volume must be non-negative")
        if not self.quote_asset.strip():
            raise ValueError("quote_asset must not be empty")
        if not self.source.strip():
            raise ValueError("source must not be empty")
        for field_name in ("source_at", "fetched_at"):
            value = getattr(self, field_name)
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{field_name} must be timezone-aware")


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
        self._history_size = history_size
        self._clock = clock or (lambda: datetime.now(tz=UTC))
        self._snapshots: dict[str, deque[QuoteVolume24hSnapshot]] = {}
        self._refresh_task: asyncio.Task[None] | None = None
        self._refresh_failure_count = 0
        self._last_refresh_at: datetime | None = None

    @property
    def refresh_failure_count(self) -> int:
        return self._refresh_failure_count

    @property
    def last_refresh_at(self) -> datetime | None:
        return self._last_refresh_at

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
            # The live strategy trades USDT-margined perpetuals.  The global
            # endpoint also returns COIN-M/USDC-style symbols, for which a
            # value labelled as USDT would be misleading.
            if not ticker.symbol.upper().endswith("USDT"):
                continue
            snapshot = QuoteVolume24hSnapshot(
                symbol=ticker.symbol.upper(),
                quote_volume=ticker.quote_volume,
                source_at=ticker.close_time,
                fetched_at=fetched_at,
            )
            history = self._snapshots.setdefault(
                snapshot.symbol,
                deque(maxlen=self._history_size),
            )
            history.append(snapshot)
            refreshed_count += 1
        self._last_refresh_at = fetched_at
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
                    fetched_at=self._last_refresh_at,
                )
            await asyncio.sleep(self._refresh_interval_seconds)


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


__all__ = [
    "Binance24hQuoteVolumeCache",
    "QuoteVolume24hProvider",
    "QuoteVolume24hSnapshot",
]
