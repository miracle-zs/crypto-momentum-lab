import asyncio
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.universe.models import DailyOpen
from crypto_momentum_lab.universe.daily_open_prefetch import DailyOpenPrefetcher


class FakeMarketData:
    def __init__(self, opens: tuple[DailyOpen, ...]) -> None:
        self.opens = opens
        self.requests: list[frozenset[str]] = []

    async def fetch_daily_opens(
        self,
        symbols: frozenset[str],
        utc_day: date,
    ) -> tuple[DailyOpen, ...]:
        self.requests.append(symbols)
        return tuple(
            item
            for item in self.opens
            if item.utc_day == utc_day and item.symbol in symbols
        )

    async def fetch_active_usdt_perpetuals(self):
        return ()


class FakeRepository:
    def __init__(self) -> None:
        self.opens: dict[tuple[date, str], Decimal] = {}

    async def load_daily_opens(
        self,
        utc_day: date,
        symbols: frozenset[str],
    ) -> dict[str, Decimal]:
        return {
            symbol: self.opens[(utc_day, symbol)]
            for symbol in symbols
            if (utc_day, symbol) in self.opens
        }

    async def save_daily_opens(
        self,
        opens: tuple[DailyOpen, ...],
        *,
        captured_at: datetime,
    ) -> None:
        self.opens.update(
            {(item.utc_day, item.symbol): item.open_price for item in opens}
        )


@pytest.mark.asyncio
async def test_prefetch_now_batches_and_skips_stored_opens() -> None:
    utc_day = date(2026, 9, 12)
    observed_at = datetime(2026, 9, 12, 0, 1, tzinfo=UTC)
    market_data = FakeMarketData(
        tuple(
            DailyOpen(symbol, utc_day, Decimal("100"), observed_at)
            for symbol in ("AAAUSDT", "BBBUSDT", "CCCUSDT")
        )
    )
    repository = FakeRepository()
    repository.opens[(utc_day, "AAAUSDT")] = Decimal("99")
    prefetcher = DailyOpenPrefetcher(
        market_data,
        repository,
        batch_size=2,
    )

    await prefetcher.prefetch_now(
        ("AAAUSDT", "BBBUSDT", "CCCUSDT"),
        utc_day,
        captured_at=observed_at,
    )

    assert repository.opens[(utc_day, "AAAUSDT")] == Decimal("99")
    assert set(repository.opens) == {
        (utc_day, "AAAUSDT"),
        (utc_day, "BBBUSDT"),
        (utc_day, "CCCUSDT"),
    }
    assert market_data.requests == [
        frozenset({"BBBUSDT", "CCCUSDT"}),
    ]


@pytest.mark.asyncio
async def test_request_is_processed_by_background_worker() -> None:
    utc_day = date(2026, 9, 12)
    observed_at = datetime(2026, 9, 12, 0, 1, tzinfo=UTC)
    market_data = FakeMarketData(
        (DailyOpen("AAAUSDT", utc_day, Decimal("100"), observed_at),)
    )
    repository = FakeRepository()
    prefetcher = DailyOpenPrefetcher(
        market_data,
        repository,
        retry_delay_seconds=0,
    )

    await prefetcher.start()
    try:
        await prefetcher.request(("AAAUSDT",), utc_day)
        for _ in range(20):
            if (utc_day, "AAAUSDT") in repository.opens:
                break
            await asyncio.sleep(0)
        assert repository.opens[(utc_day, "AAAUSDT")] == Decimal("100")
    finally:
        await prefetcher.stop()
