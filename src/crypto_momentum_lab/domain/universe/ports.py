from datetime import date, datetime
from decimal import Decimal
from typing import Protocol

from crypto_momentum_lab.domain.universe.models import (
    ContractMetadata,
    DailyOpen,
    PricePoint,
    TrackedMembership,
    UniverseSnapshot,
)


class UniverseRepository(Protocol):
    async def save_contract_metadata(
        self,
        contracts: tuple[ContractMetadata, ...],
        *,
        effective_at: datetime,
    ) -> None: ...

    async def load_daily_opens(
        self,
        utc_day: date,
        symbols: frozenset[str],
    ) -> dict[str, Decimal]: ...

    async def save_daily_opens(
        self,
        opens: tuple[DailyOpen, ...],
        *,
        captured_at: datetime,
    ) -> None: ...

    async def load_active_memberships(
        self,
    ) -> dict[str, TrackedMembership]: ...

    async def save_snapshot(self, snapshot: UniverseSnapshot) -> None: ...

    async def load_snapshot(
        self,
        observed_at: datetime,
    ) -> UniverseSnapshot | None: ...


class UniverseMarketData(Protocol):
    async def fetch_active_usdt_perpetuals(
        self,
    ) -> tuple[ContractMetadata, ...]: ...

    async def fetch_latest_prices(self) -> dict[str, PricePoint]: ...

    async def fetch_daily_opens(
        self,
        symbols: frozenset[str],
        utc_day: date,
    ) -> tuple[DailyOpen, ...]: ...


class MonitoringObligationProvider(Protocol):
    async def forced_symbols(self) -> frozenset[str]: ...


class NoMonitoringObligations:
    async def forced_symbols(self) -> frozenset[str]:
        return frozenset()


class UniverseSnapshotObserver(Protocol):
    async def snapshot_updated(
        self,
        snapshot: UniverseSnapshot,
    ) -> None: ...


class NoUniverseSnapshotObserver:
    async def snapshot_updated(
        self,
        snapshot: UniverseSnapshot,
    ) -> None:
        return None
