from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from crypto_momentum_lab.strategy_runner.candle_source import (
    BinanceRestClosedCandle15mSource,
    ClosedCandleSourceError,
)
from crypto_momentum_lab.strategy_runner.portfolio import ClosedCandle15m


def test_binance_candle_source_loads_closed_15m_rows_and_caches_range() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json=[
                _kline(
                    start="2026-08-01T12:30:00+00:00",
                    open_price="0.0300400",
                    close_price="0.0300600",
                ),
                _kline(
                    start="2026-08-01T12:45:00+00:00",
                    open_price="0.0300400",
                    close_price="0.0298600",
                ),
            ],
        )

    source = BinanceRestClosedCandle15mSource(
        base_url="https://fapi.binance.test",
        transport=httpx.MockTransport(handler),
        clock=lambda: datetime(2026, 8, 1, 13, 0, tzinfo=UTC),
    )
    start = datetime(2026, 8, 1, 12, 30, tzinfo=UTC)
    end = datetime(2026, 8, 1, 13, 0, tzinfo=UTC)
    try:
        first = source.load_closed_candles(
            symbol="TAKEUSDT",
            start=start,
            end=end,
        )
        second = source.load_closed_candles(
            symbol="TAKEUSDT",
            start=start,
            end=end,
        )
    finally:
        source.close()

    assert first == second == (
        ClosedCandle15m(
            symbol="TAKEUSDT",
            candle_start=start,
            candle_end=datetime(2026, 8, 1, 12, 45, tzinfo=UTC),
            open_price=Decimal("0.0300400"),
            close_price=Decimal("0.0300600"),
        ),
        ClosedCandle15m(
            symbol="TAKEUSDT",
            candle_start=datetime(2026, 8, 1, 12, 45, tzinfo=UTC),
            candle_end=end,
            open_price=Decimal("0.0300400"),
            close_price=Decimal("0.0298600"),
        ),
    )
    assert len(requests) == 1
    assert requests[0].url.params["symbol"] == "TAKEUSDT"
    assert requests[0].url.params["interval"] == "15m"


def test_binance_candle_source_fails_closed_when_a_candle_is_missing() -> None:
    source = BinanceRestClosedCandle15mSource(
        base_url="https://fapi.binance.test",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json=[
                    _kline(
                        start="2026-08-01T12:30:00+00:00",
                        open_price="100",
                        close_price="101",
                    )
                ],
            )
        ),
        clock=lambda: datetime(2026, 8, 1, 13, 0, tzinfo=UTC),
    )
    try:
        with pytest.raises(ClosedCandleSourceError, match="missing"):
            source.load_closed_candles(
                symbol="BTCUSDT",
                start=datetime(2026, 8, 1, 12, 30, tzinfo=UTC),
                end=datetime(2026, 8, 1, 13, 0, tzinfo=UTC),
            )
    finally:
        source.close()


def test_binance_candle_source_retries_read_timeout() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ReadTimeout("temporary timeout", request=request)
        return httpx.Response(
            200,
            json=[
                _kline(
                    start="2026-08-01T12:30:00+00:00",
                    open_price="100",
                    close_price="101",
                ),
                _kline(
                    start="2026-08-01T12:45:00+00:00",
                    open_price="101",
                    close_price="100",
                ),
            ],
        )

    source = BinanceRestClosedCandle15mSource(
        base_url="https://fapi.binance.test",
        transport=httpx.MockTransport(handler),
        clock=lambda: datetime(2026, 8, 1, 13, 0, tzinfo=UTC),
    )
    try:
        candles = source.load_closed_candles(
            symbol="BTCUSDT",
            start=datetime(2026, 8, 1, 12, 30, tzinfo=UTC),
            end=datetime(2026, 8, 1, 13, 0, tzinfo=UTC),
        )
    finally:
        source.close()

    assert len(candles) == 2
    assert attempts == 2


def _kline(
    *,
    start: str,
    open_price: str,
    close_price: str,
) -> list[object]:
    opened_at = datetime.fromisoformat(start)
    open_ms = int(opened_at.timestamp() * 1000)
    close_ms = open_ms + 15 * 60 * 1000 - 1
    return [
        open_ms,
        open_price,
        open_price,
        close_price,
        close_price,
        "0",
        close_ms,
        "0",
        0,
        "0",
        "0",
        "0",
    ]
