from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest

from crypto_momentum_lab.strategy_runner.candle_source import (
    BinanceRestClosedCandle15mSource,
    ClosedCandleEmaProvider,
    ClosedCandleSourceError,
)
from crypto_momentum_lab.strategy_runner.portfolio import ClosedCandle15m


class FakeClosedCandleSource:
    def __init__(self, candles: tuple[ClosedCandle15m, ...]) -> None:
        self.candles = candles
        self.calls = 0

    def load_closed_candles(
        self,
        *,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> tuple[ClosedCandle15m, ...]:
        del symbol, start, end
        self.calls += 1
        return self.candles


def test_closed_candle_ema_provider_uses_closed_prices_and_caches_boundary() -> None:
    candle_start = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    candles = tuple(
        ClosedCandle15m(
            symbol="BTCUSDT",
            candle_start=candle_start + index * timedelta(minutes=15),
            candle_end=candle_start + (index + 1) * timedelta(minutes=15),
            open_price=Decimal(index + 1),
            close_price=Decimal(index + 1),
        )
        for index in range(10)
    )
    source = FakeClosedCandleSource(candles)
    provider = ClosedCandleEmaProvider(source)

    first = provider.load(
        symbol="btcusdt",
        observed_at=datetime(2026, 8, 1, 14, 31, tzinfo=UTC),
    )
    second = provider.load(
        symbol="BTCUSDT",
        observed_at=datetime(2026, 8, 1, 14, 44, tzinfo=UTC),
    )

    assert first == second
    assert first.ema5 == Decimal("8")
    assert first.ema10 == Decimal("5.5")
    assert first.symbol == "BTCUSDT"
    assert first.observed_at == datetime(2026, 8, 1, 14, 30, tzinfo=UTC)
    assert first.snapshot_id == "ema-BTCUSDT-20260801T143000Z-200"
    assert first.config_hash is not None
    assert source.calls == 1


def test_closed_candle_ema_provider_prunes_old_boundaries_and_idle_symbols() -> None:
    candle_start = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    candles = tuple(
        ClosedCandle15m(
            symbol="BTCUSDT",
            candle_start=candle_start + index * timedelta(minutes=15),
            candle_end=candle_start + (index + 1) * timedelta(minutes=15),
            open_price=Decimal(index + 1),
            close_price=Decimal(index + 1),
        )
        for index in range(10)
    )
    access_at = datetime(2026, 8, 1, 15, 0, tzinfo=UTC)
    provider = ClosedCandleEmaProvider(
        FakeClosedCandleSource(candles),
        clock=lambda: access_at,
    )
    for boundary in (12, 12, 12):
        provider.load(
            symbol="BTCUSDT",
            observed_at=datetime(2026, 8, 1, boundary, 0, tzinfo=UTC),
        )

    assert provider.cache_entry_count == 1

    provider.load(
        symbol="BTCUSDT",
        observed_at=datetime(2026, 8, 1, 12, 15, tzinfo=UTC),
    )
    provider.load(
        symbol="BTCUSDT",
        observed_at=datetime(2026, 8, 1, 12, 30, tzinfo=UTC),
    )
    assert provider.cache_entry_count == 3
    assert (
        provider.prune(
            now=access_at,
            protected_symbols={"BTCUSDT"},
            max_boundaries_per_symbol=2,
        )
        == 1
    )
    assert provider.cache_entry_count == 2

    idle_provider = ClosedCandleEmaProvider(
        FakeClosedCandleSource(candles),
        clock=lambda: access_at,
    )
    idle_provider.load(
        symbol="ETHUSDT",
        observed_at=datetime(2026, 8, 1, 12, 0, tzinfo=UTC),
    )
    assert (
        idle_provider.prune(
            now=access_at + timedelta(hours=2),
            protected_symbols=(),
            inactive_after=timedelta(hours=1),
        )
        == 1
    )
    assert idle_provider.cache_entry_count == 0


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


def test_binance_candle_source_normalizes_http_failures() -> None:
    source = BinanceRestClosedCandle15mSource(
        base_url="https://fapi.binance.test",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(400, json={"code": -1121})
        ),
        clock=lambda: datetime(2026, 8, 1, 13, 0, tzinfo=UTC),
    )
    try:
        with pytest.raises(ClosedCandleSourceError, match="HTTP 400"):
            source.load_closed_candles(
                symbol="BTCUSDT",
                start=datetime(2026, 8, 1, 12, 30, tzinfo=UTC),
                end=datetime(2026, 8, 1, 13, 0, tzinfo=UTC),
            )
    finally:
        source.close()


def test_binance_candle_source_normalizes_exhausted_transport_failures() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ReadTimeout("temporary timeout", request=request)

    source = BinanceRestClosedCandle15mSource(
        base_url="https://fapi.binance.test",
        transport=httpx.MockTransport(handler),
        clock=lambda: datetime(2026, 8, 1, 13, 0, tzinfo=UTC),
    )
    source._retry_delays = (0.0,)
    try:
        with pytest.raises(ClosedCandleSourceError, match="after retries"):
            source.load_closed_candles(
                symbol="BTCUSDT",
                start=datetime(2026, 8, 1, 12, 30, tzinfo=UTC),
                end=datetime(2026, 8, 1, 13, 0, tzinfo=UTC),
            )
    finally:
        source.close()

    assert attempts == 2


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


def test_symbol_cache_isolated_across_pruning_and_historical_refill() -> None:
    now = datetime(2026, 8, 1, 13, 0, tzinfo=UTC)
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.params["symbol"])
        start_ms = int(request.url.params["startTime"])
        end_ms = int(request.url.params["endTime"]) + 1
        # Reverse rows to verify reads still return chronological candles.
        return httpx.Response(
            200,
            json=[
                _kline(
                    start=datetime.fromtimestamp(ms / 1000, UTC).isoformat(),
                    open_price="100",
                    close_price="101",
                )
                for ms in reversed(range(start_ms, end_ms, 900_000))
            ],
        )

    with BinanceRestClosedCandle15mSource(
        "https://fapi.binance.test",
        transport=httpx.MockTransport(handler),
        clock=lambda: now,
        cache_retention=timedelta(minutes=30),
    ) as source:
        start = now - timedelta(minutes=30)
        btc = source.load_closed_candles(symbol="BTCUSDT", start=start, end=now)
        eth = source.load_closed_candles(symbol="ETHUSDT", start=start, end=now)
        assert [c.candle_start for c in btc] == [start, start + timedelta(minutes=15)]
        now += timedelta(minutes=30)
        source.load_closed_candles(symbol="BTCUSDT", start=start, end=now)
        # Re-request pruned BTC history; ETH retains its independent history.
        assert (
            source.load_closed_candles(
                symbol="BTCUSDT",
                start=start,
                end=now - timedelta(minutes=30),
            )
            == btc
        )
        assert (
            source.load_closed_candles(
                symbol="ETHUSDT",
                start=start,
                end=now - timedelta(minutes=30),
            )
            == eth
        )
        assert calls == ["BTCUSDT", "ETHUSDT", "BTCUSDT", "BTCUSDT", "ETHUSDT"]


def test_concurrent_same_symbol_requests_share_one_fetch() -> None:
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    fetching = Event()
    release = Event()
    calls = 0
    start = datetime(2026, 8, 1, 12, 45, tzinfo=UTC)
    end = start + timedelta(minutes=15)

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        fetching.set()
        assert release.wait(5)
        return httpx.Response(
            200,
            json=[
                _kline(start=start.isoformat(), open_price="100", close_price="101"),
            ],
        )

    with (
        BinanceRestClosedCandle15mSource(
            "https://fapi.binance.test",
            transport=httpx.MockTransport(handler),
            clock=lambda: end,
        ) as source,
        ThreadPoolExecutor(max_workers=2) as executor,
    ):
        first = executor.submit(
            source.load_closed_candles,
            symbol="BTCUSDT",
            start=start,
            end=end,
        )
        assert fetching.wait(5)
        second = executor.submit(
            source.load_closed_candles,
            symbol="BTCUSDT",
            start=start,
            end=end,
        )
        release.set()
        assert first.result(timeout=5) == second.result(timeout=5)
    assert calls == 1


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
