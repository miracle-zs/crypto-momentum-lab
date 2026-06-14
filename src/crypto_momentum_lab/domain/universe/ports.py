from datetime import date, datetime
from decimal import Decimal
from typing import Protocol

from crypto_momentum_lab.domain.universe.models import (
    ContractMetadata,
    DailyOpen,
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
