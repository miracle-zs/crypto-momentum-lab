import asyncio
from datetime import UTC, date
from decimal import Decimal

import httpx
import pytest
import respx

from crypto_momentum_lab.market_data.binance.rest import BinanceUsdMRestClient


@respx.mock
async def test_lists_only_active_usdt_perpetuals() -> None:
    respx.get("https://fapi.binance.com/fapi/v1/exchangeInfo").mock(
        return_value=httpx.Response(
            200,
            json={
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "contractType": "PERPETUAL",
                        "status": "TRADING",
                        "quoteAsset": "USDT",
                        "marginAsset": "USDT",
                        "onboardDate": 1598252400000,
                        "filters": [],
                    },
                    {
                        "symbol": "BTCUSDC",
                        "contractType": "PERPETUAL",
                        "status": "TRADING",
                        "quoteAsset": "USDC",
                        "marginAsset": "USDC",
                        "onboardDate": 1598252400000,
                        "filters": [],
                    },
                ]
            },
        )
    )
    async with BinanceUsdMRestClient("https://fapi.binance.com") as client:
        contracts = await client.fetch_active_usdt_perpetuals()

    assert [contract.symbol for contract in contracts] == ["BTCUSDT"]


@respx.mock
async def test_fetches_all_latest_prices_from_v2() -> None:
    respx.get("https://fapi.binance.com/fapi/v2/ticker/price").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "symbol": "BTCUSDT",
                    "price": "60000.5",
                    "time": 1781415660000,
                }
            ],
        )
    )
    async with BinanceUsdMRestClient("https://fapi.binance.com") as client:
        prices = await client.fetch_latest_prices()

    assert prices["BTCUSDT"].price == Decimal("60000.5")
    assert prices["BTCUSDT"].observed_at.tzinfo is UTC


@respx.mock
async def test_fetches_24h_quote_volume_from_ticker_endpoint() -> None:
    respx.get("https://fapi.binance.com/fapi/v1/ticker/24hr").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "symbol": "BTCUSDT",
                    "quoteVolume": "123456789.25",
                    "openTime": 1781400000000,
                    "closeTime": 1781486399999,
                }
            ],
        )
    )
    async with BinanceUsdMRestClient("https://fapi.binance.com") as client:
        tickers = await client.fetch_24h_tickers()

    ticker = tickers["BTCUSDT"]
    assert ticker.quote_volume == Decimal("123456789.25")
    assert ticker.open_time.tzinfo is UTC
    assert ticker.close_time.tzinfo is UTC


@respx.mock
async def test_fetches_aggregate_trade_history_from_id() -> None:
    route = respx.get("https://fapi.binance.com/fapi/v1/aggTrades").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "a": 11,
                    "p": "60000.5",
                    "q": "0.25",
                    "f": 101,
                    "l": 102,
                    "T": 1781415660000,
                    "m": False,
                }
            ],
        )
    )

    async with BinanceUsdMRestClient("https://fapi.binance.com") as client:
        trades = await client.fetch_agg_trades(
            "btcusdt",
            from_id=11,
            limit=2,
        )

    assert trades[0].aggregate_trade_id == 11
    assert trades[0].price == Decimal("60000.5")
    assert trades[0].event_at.tzinfo is UTC
    assert route.calls[0].request.url.params["symbol"] == "BTCUSDT"
    assert route.calls[0].request.url.params["fromId"] == "11"
    assert route.calls[0].request.url.params["limit"] == "2"


@respx.mock
async def test_fetches_current_utc_day_open() -> None:
    route = respx.get("https://fapi.binance.com/fapi/v1/klines").mock(
        return_value=httpx.Response(
            200,
            json=[
                [
                    1781395200000,
                    "59000.0",
                    "61000",
                    "58000",
                    "60000",
                    "1",
                    1781481599999,
                    "1",
                    1,
                    "1",
                    "1",
                    "0",
                ]
            ],
        )
    )
    async with BinanceUsdMRestClient("https://fapi.binance.com") as client:
        daily_open = await client.fetch_daily_open(
            "BTCUSDT",
            date(2026, 6, 14),
        )

    assert daily_open is not None
    assert daily_open.open_price == Decimal("59000.0")
    assert route.calls[0].request.url.params["interval"] == "1d"
    assert route.calls[0].request.url.params["limit"] == "1"


@respx.mock
async def test_retries_server_error_then_succeeds() -> None:
    route = respx.get("https://fapi.binance.com/fapi/v2/ticker/price").mock(
        side_effect=[
            httpx.Response(500),
            httpx.Response(200, json=[]),
        ]
    )
    async with BinanceUsdMRestClient("https://fapi.binance.com") as client:
        client._retry_delays = (0.0, 0.0, 0.0)
        assert await client.fetch_latest_prices() == {}

    assert route.call_count == 2


@respx.mock
async def test_does_not_retry_bad_request() -> None:
    route = respx.get("https://fapi.binance.com/fapi/v2/ticker/price").mock(
        return_value=httpx.Response(400)
    )
    async with BinanceUsdMRestClient("https://fapi.binance.com") as client:
        with pytest.raises(httpx.HTTPStatusError):
            await client.fetch_latest_prices()

    assert route.call_count == 1


@respx.mock
async def test_limits_daily_open_concurrency() -> None:
    active = 0
    maximum = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0)
        active -= 1
        return httpx.Response(
            200,
            json=[
                [
                    1781395200000,
                    "100",
                    "100",
                    "100",
                    "100",
                    "1",
                    1781481599999,
                    "1",
                    1,
                    "1",
                    "1",
                    "0",
                ]
            ],
        )

    respx.get("https://fapi.binance.com/fapi/v1/klines").mock(
        side_effect=handler
    )
    async with BinanceUsdMRestClient(
        "https://fapi.binance.com",
        daily_open_concurrency=2,
    ) as client:
        opens = await client.fetch_daily_opens(
            frozenset({"AUSDT", "BUSDT", "CUSDT", "DUSDT"}),
            date(2026, 6, 14),
        )

    assert len(opens) == 4
    assert maximum == 2
