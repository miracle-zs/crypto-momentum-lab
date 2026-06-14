from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.config.models import UniverseConfig
from crypto_momentum_lab.domain.universe.models import (
    ContractMetadata,
    DailyOpen,
    MembershipStatus,
    PricePoint,
)
from crypto_momentum_lab.persistence.postgres.repository import (
    PostgresUniverseRepository,
)
from crypto_momentum_lab.universe.refresh import UniverseRefreshService


class FixtureMarketData:
    def __init__(self, observed_at: datetime) -> None:
        self.observed_at = observed_at
        self.symbols = tuple(f"S{index:02d}USDT" for index in range(45))
        self.contracts = tuple(
            ContractMetadata(
                symbol,
                "PERPETUAL",
                "TRADING",
                "USDT",
                "USDT",
                observed_at,
                {},
            )
            for symbol in self.symbols
        )
        self.price_values = {
            symbol: Decimal(80 + index)
            for index, symbol in enumerate(self.symbols)
        }

    async def fetch_active_usdt_perpetuals(
        self,
    ) -> tuple[ContractMetadata, ...]:
        return self.contracts

    async def fetch_latest_prices(self) -> dict[str, PricePoint]:
        return {
            symbol: PricePoint(
                symbol,
                price,
                self.observed_at,
            )
            for symbol, price in self.price_values.items()
        }

    async def fetch_daily_opens(
        self,
        symbols: frozenset[str],
        utc_day: date,
    ) -> tuple[DailyOpen, ...]:
        open_time = datetime.combine(
            utc_day,
            datetime.min.time(),
            tzinfo=UTC,
        )
        return tuple(
            DailyOpen(
                symbol,
                utc_day,
                Decimal("100"),
                open_time,
            )
            for symbol in sorted(symbols)
        )


def build_fixture_service(
    repository: PostgresUniverseRepository,
    market_data: FixtureMarketData,
) -> UniverseRefreshService:
    return UniverseRefreshService(
        market_data=market_data,
        repository=repository,
        config=UniverseConfig(
            top_count=20,
            retention_rank=30,
            retention_hours=2,
            activation_minute=1,
        ),
        config_hash="a" * 64,
    )


@pytest.mark.e2e
async def test_refresh_is_deterministic_and_persists_point_in_time(
    repository: PostgresUniverseRepository,
) -> None:
    at = datetime(2026, 6, 14, 11, 1, tzinfo=UTC)
    market_data = FixtureMarketData(at)
    service = build_fixture_service(repository, market_data)

    first = await service.refresh(observed_at=at)
    second = await service.refresh(observed_at=at)
    loaded = await repository.load_snapshot(at)

    assert first == second
    assert loaded == first
    assert len(first.ranking.target_symbols) == 40
    assert len(first.memberships) == 40


@pytest.mark.e2e
async def test_rank_21_former_target_is_retained(
    repository: PostgresUniverseRepository,
) -> None:
    first_at = datetime(2026, 6, 14, 11, 1, tzinfo=UTC)
    market_data = FixtureMarketData(first_at)
    service = build_fixture_service(repository, market_data)
    first = await service.refresh(observed_at=first_at)
    assert "S25USDT" in first.ranking.target_symbols

    second_at = datetime(2026, 6, 14, 12, 1, tzinfo=UTC)
    market_data.observed_at = second_at
    market_data.price_values["S24USDT"] = Decimal("105.5")
    market_data.price_values["S25USDT"] = Decimal("104.5")
    second = await service.refresh(observed_at=second_at)

    membership = next(
        item for item in second.memberships if item.symbol == "S25USDT"
    )
    assert membership.status is MembershipStatus.RETAINED
    assert len(second.ranking.target_symbols) == 40
    assert len(second.memberships) == 41
