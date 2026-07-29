import asyncio
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Self

import httpx

from crypto_momentum_lab.domain.universe.models import (
    ContractMetadata,
    DailyOpen,
    PricePoint,
)


def _utc_from_ms(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=UTC)


class BinanceUsdMRestClient:
    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float = 10.0,
        daily_open_concurrency: int = 10,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout_seconds,
            trust_env=False,
        )
        self._daily_open_concurrency = daily_open_concurrency
        self._retry_delays = (0.25, 0.5, 1.0)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _get(
        self,
        path: str,
        *,
        params: dict[str, str | int | float | bool | None] | None = None,
    ) -> httpx.Response:
        for attempt in range(len(self._retry_delays) + 1):
            try:
                response = await self._client.get(path, params=params)
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as error:
                retryable = (
                    error.response.status_code == 429
                    or error.response.status_code >= 500
                )
                if not retryable or attempt == len(self._retry_delays):
                    raise
                await asyncio.sleep(self._retry_delays[attempt])
            except (httpx.ConnectError, httpx.ReadTimeout):
                if attempt == len(self._retry_delays):
                    raise
                await asyncio.sleep(self._retry_delays[attempt])
        raise AssertionError("retry loop exhausted")

    async def fetch_active_usdt_perpetuals(
        self,
    ) -> tuple[ContractMetadata, ...]:
        response = await self._get("/fapi/v1/exchangeInfo")
        contracts = []
        for item in response.json()["symbols"]:
            if not (
                item["contractType"] == "PERPETUAL"
                and item["status"] == "TRADING"
                and item["quoteAsset"] == "USDT"
                and item["marginAsset"] == "USDT"
            ):
                continue
            contracts.append(
                ContractMetadata(
                    symbol=item["symbol"],
                    contract_type=item["contractType"],
                    status=item["status"],
                    quote_asset=item["quoteAsset"],
                    margin_asset=item["marginAsset"],
                    onboard_at=_utc_from_ms(item["onboardDate"]),
                    raw=item,
                )
            )
        return tuple(sorted(contracts, key=lambda item: item.symbol))

    async def fetch_latest_prices(self) -> dict[str, PricePoint]:
        response = await self._get("/fapi/v2/ticker/price")
        return {
            item["symbol"]: PricePoint(
                symbol=item["symbol"],
                price=Decimal(item["price"]),
                observed_at=_utc_from_ms(item["time"]),
            )
            for item in response.json()
        }

    async def fetch_daily_open(
        self,
        symbol: str,
        utc_day: date,
    ) -> DailyOpen | None:
        start = datetime.combine(utc_day, time.min, tzinfo=UTC)
        end = start + timedelta(days=1) - timedelta(milliseconds=1)
        response = await self._get(
            "/fapi/v1/klines",
            params={
                "symbol": symbol,
                "interval": "1d",
                "startTime": int(start.timestamp() * 1000),
                "endTime": int(end.timestamp() * 1000),
                "limit": 1,
            },
        )
        rows = response.json()
        if not rows:
            return None
        row = rows[0]
        return DailyOpen(
            symbol=symbol,
            utc_day=utc_day,
            open_price=Decimal(row[1]),
            open_time=_utc_from_ms(row[0]),
        )

    async def fetch_daily_opens(
        self,
        symbols: frozenset[str],
        utc_day: date,
    ) -> tuple[DailyOpen, ...]:
        semaphore = asyncio.Semaphore(self._daily_open_concurrency)

        async def fetch(symbol: str) -> DailyOpen | None:
            async with semaphore:
                return await self.fetch_daily_open(symbol, utc_day)

        results = await asyncio.gather(
            *(fetch(symbol) for symbol in sorted(symbols))
        )
        return tuple(item for item in results if item is not None)
