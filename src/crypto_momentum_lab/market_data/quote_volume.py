"""Shared 24-hour quote-volume snapshots for market-data fan-out."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Protocol

import structlog

from crypto_momentum_lab.market_data.binance.rest import Binance24hTicker

log = structlog.get_logger()

_DEFAULT_REFRESH_INTERVAL_SECONDS = 60.0
_BINANCE_24H_TICKER_SOURCE = "binance_fapi_ticker_24hr"


@dataclass(frozen=True, slots=True)
class QuoteVolume24hSnapshot:
    """Causally safe, transport-independent 24-hour volume snapshot."""

    symbol: str
    quote_volume: Decimal
    source_at: datetime
    fetched_at: datetime
    quote_asset: str = "USDT"
    source: str = _BINANCE_24H_TICKER_SOURCE
    exchange: str = "binance-usdm"
    environment: str = "research"

    def __post_init__(self) -> None:
        for value, field_name in (
            (self.symbol, "symbol"),
            (self.quote_asset, "quote_asset"),
            (self.source, "source"),
            (self.exchange, "exchange"),
            (self.environment, "environment"),
        ):
            if not value.strip():
                raise ValueError(f"{field_name} must not be empty")
        if self.quote_volume < 0:
            raise ValueError("quote_volume must be non-negative")
        for value, field_name in (
            (self.source_at, "source_at"),
            (self.fetched_at, "fetched_at"),
        ):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{field_name} must be timezone-aware")


class Binance24hTickerClient(Protocol):
    async def fetch_24h_tickers(self) -> dict[str, Binance24hTicker]: ...


QuoteVolumeSink = Callable[[tuple[QuoteVolume24hSnapshot, ...]], Awaitable[None]]


class Binance24hQuoteVolumePublisher:
    """Fetch one public ticker snapshot and publish it to local consumers."""

    def __init__(
        self,
        client: Binance24hTickerClient,
        *,
        publish: QuoteVolumeSink,
        environment: str = "research",
        refresh_interval_seconds: float = _DEFAULT_REFRESH_INTERVAL_SECONDS,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not environment.strip():
            raise ValueError("environment must not be empty")
        if refresh_interval_seconds <= 0:
            raise ValueError("refresh_interval_seconds must be positive")
        self._client = client
        self._publish = publish
        self._environment = environment
        self._refresh_interval_seconds = refresh_interval_seconds
        self._clock = clock or (lambda: datetime.now(tz=UTC))
        self._task: asyncio.Task[None] | None = None
        self._refresh_failure_count = 0
        self._last_refresh_at: datetime | None = None

    @property
    def refresh_failure_count(self) -> int:
        return self._refresh_failure_count

    @property
    def last_refresh_at(self) -> datetime | None:
        return self._last_refresh_at

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(
                self._run(),
                name="market-data-24h-quote-volume",
            )

    async def stop(self) -> None:
        task = self._task
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self._task = None

    async def refresh_once(self) -> int:
        fetched_at = self._clock()
        _require_aware(fetched_at, "fetched_at")
        tickers = await self._client.fetch_24h_tickers()
        snapshots = tuple(
            QuoteVolume24hSnapshot(
                symbol=ticker.symbol.upper(),
                quote_volume=ticker.quote_volume,
                source_at=ticker.close_time,
                fetched_at=fetched_at,
                exchange="binance-usdm",
                environment=self._environment,
            )
            for ticker in tickers.values()
            if ticker.symbol.upper().endswith("USDT")
        )
        await self._publish(snapshots)
        self._last_refresh_at = fetched_at
        return len(snapshots)

    async def _run(self) -> None:
        while True:
            try:
                refreshed_count = await self.refresh_once()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._refresh_failure_count += 1
                log.warning(
                    "market_data_24h_quote_volume_refresh_failed",
                    error_type=type(error).__name__,
                    failure_count=self._refresh_failure_count,
                )
            else:
                log.debug(
                    "market_data_24h_quote_volume_refreshed",
                    symbol_count=refreshed_count,
                    fetched_at=self._last_refresh_at,
                )
            await asyncio.sleep(self._refresh_interval_seconds)


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


__all__ = [
    "Binance24hQuoteVolumePublisher",
    "QuoteVolume24hSnapshot",
]
