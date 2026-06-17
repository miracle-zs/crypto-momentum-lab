import os
from collections.abc import AsyncIterator
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    CaptureStream,
    RawEnvelope,
)
from crypto_momentum_lab.domain.universe.models import (
    ContractMetadata,
    DailyOpen,
    PricePoint,
    TrackedMembership,
    UniverseSnapshot,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ContractMetadataRow,
    DailyOpenRow,
    MarketDataProcessStateRow,
    MarketDataQualityEventRow,
    MonitoringMembershipRow,
    RawArchiveManifestRow,
    UniverseEntryRow,
    UniverseSnapshotRow,
)
from crypto_momentum_lab.persistence.postgres.capture_repository import (
    PostgresCaptureRepository,
)
from crypto_momentum_lab.persistence.postgres.repository import (
    PostgresUniverseRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)


class FakeUniverseMarketData:
    def __init__(self) -> None:
        self.contracts: tuple[ContractMetadata, ...] = ()
        self.prices: dict[str, PricePoint] = {}
        self.opens: tuple[DailyOpen, ...] = ()
        self.requested_open_symbols = frozenset[str]()

    async def fetch_active_usdt_perpetuals(
        self,
    ) -> tuple[ContractMetadata, ...]:
        return self.contracts

    async def fetch_latest_prices(self) -> dict[str, PricePoint]:
        return self.prices

    async def fetch_daily_opens(
        self,
        symbols: frozenset[str],
        utc_day: date,
    ) -> tuple[DailyOpen, ...]:
        self.requested_open_symbols = symbols
        return tuple(item for item in self.opens if item.symbol in symbols)

    def seed_single_symbol(self, observed_at: datetime) -> None:
        symbol = "BTCUSDT"
        self.contracts = (
            ContractMetadata(
                symbol,
                "PERPETUAL",
                "TRADING",
                "USDT",
                "USDT",
                observed_at,
                {},
            ),
        )
        self.prices = {
            symbol: PricePoint(symbol, Decimal("110"), observed_at)
        }
        self.opens = (
            DailyOpen(
                symbol,
                observed_at.date(),
                Decimal("100"),
                datetime.combine(
                    observed_at.date(),
                    datetime.min.time(),
                    tzinfo=UTC,
                ),
            ),
        )


class FakeUniverseRepository:
    def __init__(self) -> None:
        self.daily_opens: dict[str, Decimal] = {}
        self.active_memberships: dict[str, TrackedMembership] = {}
        self.saved_snapshot: UniverseSnapshot | None = None

    async def save_contract_metadata(
        self,
        contracts: tuple[ContractMetadata, ...],
        *,
        effective_at: datetime,
    ) -> None:
        return None

    async def load_daily_opens(
        self,
        utc_day: date,
        symbols: frozenset[str],
    ) -> dict[str, Decimal]:
        return {
            symbol: price
            for symbol, price in self.daily_opens.items()
            if symbol in symbols
        }

    async def save_daily_opens(
        self,
        opens: tuple[DailyOpen, ...],
        *,
        captured_at: datetime,
    ) -> None:
        self.daily_opens.update(
            {item.symbol: item.open_price for item in opens}
        )

    async def load_active_memberships(
        self,
    ) -> dict[str, TrackedMembership]:
        return self.active_memberships

    async def save_snapshot(self, snapshot: UniverseSnapshot) -> None:
        self.saved_snapshot = snapshot
        if snapshot.activated:
            self.active_memberships = {
                item.symbol: item for item in snapshot.memberships
            }

    async def load_snapshot(
        self,
        observed_at: datetime,
    ) -> UniverseSnapshot | None:
        if (
            self.saved_snapshot is not None
            and self.saved_snapshot.observed_at == observed_at
        ):
            return self.saved_snapshot
        return None


@pytest.fixture(scope="session")
def async_database_url() -> str:
    return os.environ.get(
        "CML_TEST_ASYNC_DATABASE_URL",
        "postgresql+asyncpg://cml:cml@localhost:54329/cml",
    )


@pytest.fixture
async def repository(
    async_database_url: str,
) -> AsyncIterator[PostgresUniverseRepository]:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        async with session.begin():
            for model in (
                MarketDataQualityEventRow,
                MarketDataProcessStateRow,
                RawArchiveManifestRow,
                MonitoringMembershipRow,
                UniverseEntryRow,
                UniverseSnapshotRow,
                DailyOpenRow,
                ContractMetadataRow,
            ):
                await session.execute(delete(model))
    yield PostgresUniverseRepository(factory)
    await engine.dispose()


@pytest.fixture
async def capture_repository(
    async_database_url: str,
) -> AsyncIterator[PostgresCaptureRepository]:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        async with session.begin():
            for model in (
                MarketDataQualityEventRow,
                MarketDataProcessStateRow,
                RawArchiveManifestRow,
            ):
                await session.execute(delete(model))
    yield PostgresCaptureRepository(factory)
    await engine.dispose()


@pytest.fixture
def fake_market_data() -> FakeUniverseMarketData:
    return FakeUniverseMarketData()


@pytest.fixture
def fake_repository() -> FakeUniverseRepository:
    return FakeUniverseRepository()


@pytest.fixture
def raw_envelope() -> RawEnvelope:
    return RawEnvelope(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        route=CaptureRoute.MARKET,
        stream=CaptureStream.AGG_TRADE,
        symbol="BTCUSDT",
        exchange_event_at=datetime(2026, 6, 15, 2, 0, tzinfo=UTC),
        received_at=datetime(2026, 6, 15, 2, 0, 1, tzinfo=UTC),
        received_monotonic_ns=1,
        connection_session_id=UUID(int=1),
        local_sequence=1,
        exchange_sequence="42",
        subscription_generation=1,
        raw_payload={"e": "aggTrade", "s": "BTCUSDT"},
    )
