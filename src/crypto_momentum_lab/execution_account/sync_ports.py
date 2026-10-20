"""Read-only client contract used by the in-process account runtime."""

from datetime import datetime
from typing import Protocol

from crypto_momentum_lab.domain.account.models import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    AccountFillEvent,
    AccountFillPageScan,
    AccountOpenOrderSnapshot,
    AccountPositionSnapshot,
)


class ReadOnlyAccountClient(Protocol):
    async def fetch_account_config(self) -> AccountConfigSnapshot:
        pass

    async def fetch_balances(self) -> tuple[AccountBalanceSnapshot, ...]:
        pass

    async def fetch_positions(
        self, *, include_flat: bool = False
    ) -> tuple[AccountPositionSnapshot, ...]:
        pass

    async def fetch_open_orders(self) -> tuple[AccountOpenOrderSnapshot, ...]:
        pass

    async def fetch_fills_with_provenance(
        self,
        symbol: str,
        *,
        start_time_ms: int,
        checked_through: datetime,
        max_pages_per_window: int = 10,
    ) -> tuple[tuple[AccountFillEvent, ...], AccountFillPageScan]:
        pass
