from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Protocol, Self

import httpx

from crypto_momentum_lab.strategy_runner.portfolio import ClosedCandle15m

_CANDLE_INTERVAL = timedelta(minutes=15)
_MAX_KLINES_PER_REQUEST = 1500


class ClosedCandleSourceError(RuntimeError):
    """The exchange did not provide a complete immutable candle range."""


class ClosedCandle15mSource(Protocol):
    def load_closed_candles(
        self,
        *,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> tuple[ClosedCandle15m, ...]: ...


class BinanceRestClosedCandle15mSource:
    """Load immutable official 15-minute candles with a bounded local cache."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float = 10.0,
        cache_retention: timedelta = timedelta(days=2),
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if cache_retention <= timedelta(0):
            raise ValueError("cache_retention must be positive")
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            timeout=timeout_seconds,
            trust_env=False,
            transport=transport,
        )
        self._cache_retention = cache_retention
        self._clock = clock
        self._candles: dict[tuple[str, datetime], ClosedCandle15m] = {}
        self._coverage: dict[str, tuple[datetime, datetime]] = {}
        self._retry_delays = (0.25, 0.5, 1.0)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def load_closed_candles(
        self,
        *,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> tuple[ClosedCandle15m, ...]:
        normalized_symbol = symbol.strip().upper()
        if not normalized_symbol:
            raise ValueError("symbol must not be empty")
        aligned_start = _candle_start_15m(start)
        aligned_end = _candle_start_15m(end)
        if aligned_end <= aligned_start:
            return ()
        fetch_end = max(aligned_end, _candle_start_15m(self._clock()))

        coverage = self._coverage.get(normalized_symbol)
        if coverage is None:
            self._fetch_range(normalized_symbol, aligned_start, fetch_end)
            coverage = (aligned_start, fetch_end)
        else:
            covered_start, covered_end = coverage
            if aligned_start < covered_start:
                self._fetch_range(
                    normalized_symbol,
                    aligned_start,
                    covered_start,
                )
                covered_start = aligned_start
            if fetch_end > covered_end:
                self._fetch_range(
                    normalized_symbol,
                    covered_end,
                    fetch_end,
                )
                covered_end = fetch_end
            coverage = (covered_start, covered_end)
        self._coverage[normalized_symbol] = coverage

        candles = tuple(
            candle
            for (cached_symbol, candle_start), candle in sorted(
                self._candles.items(),
                key=lambda item: item[0],
            )
            if cached_symbol == normalized_symbol
            and aligned_start <= candle_start < aligned_end
        )
        expected_starts = {
            aligned_start + index * _CANDLE_INTERVAL
            for index in range(
                int((aligned_end - aligned_start) / _CANDLE_INTERVAL)
            )
        }
        actual_starts = {candle.candle_start for candle in candles}
        missing_starts = expected_starts - actual_starts
        if missing_starts:
            missing = ", ".join(
                item.isoformat() for item in sorted(missing_starts)[:5]
            )
            suffix = "..." if len(missing_starts) > 5 else ""
            raise ClosedCandleSourceError(
                f"incomplete Binance 15m candle range for {normalized_symbol}: "
                f"missing {len(missing_starts)} candle(s) starting at "
                f"{missing}{suffix}"
            )
        self._prune(normalized_symbol, requested_start=aligned_start, end=aligned_end)
        return candles

    def _fetch_range(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> None:
        cursor = start
        while cursor < end:
            remaining = int((end - cursor) / _CANDLE_INTERVAL)
            limit = min(_MAX_KLINES_PER_REQUEST, max(1, remaining))
            response = self._get(
                "/fapi/v1/klines",
                params={
                    "symbol": symbol,
                    "interval": "15m",
                    "startTime": int(cursor.timestamp() * 1000),
                    "endTime": int(end.timestamp() * 1000) - 1,
                    "limit": limit,
                },
            )
            rows = response.json()
            if not isinstance(rows, list):
                raise ValueError("Binance kline response must be a list")
            parsed = tuple(
                candle
                for row in rows
                if (candle := _parse_kline(symbol, row)) is not None
                and start <= candle.candle_start < end
                and candle.candle_end <= end
            )
            for candle in parsed:
                self._candles[(symbol, candle.candle_start)] = candle
            if not parsed:
                raise ClosedCandleSourceError(
                    f"Binance returned no closed 15m candles for {symbol} "
                    f"from {cursor.isoformat()} to {end.isoformat()}"
                )
            first_start = min(item.candle_start for item in parsed)
            if first_start > cursor:
                raise ClosedCandleSourceError(
                    f"Binance 15m candle range starts late for {symbol}: "
                    f"expected {cursor.isoformat()}, got {first_start.isoformat()}"
                )
            next_cursor = max(item.candle_start for item in parsed) + _CANDLE_INTERVAL
            if next_cursor <= cursor:
                raise ValueError("Binance kline pagination did not advance")
            cursor = next_cursor
            if len(rows) < limit:
                if cursor < end:
                    raise ClosedCandleSourceError(
                        f"Binance returned a short 15m candle page for {symbol}; "
                        f"missing candles after "
                        f"{cursor.isoformat()}; "
                        f"range ends at {cursor.isoformat()}, expected "
                        f"{end.isoformat()}"
                    )
                return

    def _get(
        self,
        path: str,
        *,
        params: dict[str, str | int],
    ) -> httpx.Response:
        for attempt in range(len(self._retry_delays) + 1):
            try:
                response = self._client.get(path, params=params)
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as error:
                retryable = (
                    error.response.status_code == 429
                    or error.response.status_code >= 500
                )
                if not retryable or attempt == len(self._retry_delays):
                    raise
            except (
                httpx.ConnectError,
                httpx.ReadError,
                httpx.RemoteProtocolError,
                httpx.TimeoutException,
            ):
                if attempt == len(self._retry_delays):
                    raise
            time.sleep(self._retry_delays[attempt])
        raise AssertionError("retry loop exhausted")

    def _prune(
        self,
        symbol: str,
        *,
        requested_start: datetime,
        end: datetime,
    ) -> None:
        retention_before = (
            _candle_start_15m(self._clock()) - self._cache_retention
        )
        prune_before = min(end, max(requested_start, retention_before))
        stale_keys = tuple(
            key
            for key in self._candles
            if key[0] == symbol and key[1] < prune_before
        )
        for key in stale_keys:
            self._candles.pop(key, None)
        covered_start, covered_end = self._coverage[symbol]
        self._coverage[symbol] = (max(covered_start, prune_before), covered_end)


def _parse_kline(
    symbol: str,
    row: object,
) -> ClosedCandle15m | None:
    if not isinstance(row, list) or len(row) < 7:
        raise ValueError("Binance kline row is malformed")
    open_ms = row[0]
    close_ms = row[6]
    if not isinstance(open_ms, int) or not isinstance(close_ms, int):
        raise ValueError("Binance kline timestamps must be integers")
    candle_start = datetime.fromtimestamp(open_ms / 1000, tz=UTC)
    candle_end = datetime.fromtimestamp((close_ms + 1) / 1000, tz=UTC)
    return ClosedCandle15m(
        symbol=symbol,
        candle_start=candle_start,
        candle_end=candle_end,
        open_price=Decimal(str(row[1])),
        close_price=Decimal(str(row[4])),
    )


def _candle_start_15m(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("candle boundary must be timezone-aware")
    utc_value = value.astimezone(UTC)
    return utc_value.replace(
        minute=utc_value.minute - utc_value.minute % 15,
        second=0,
        microsecond=0,
    )
