"""Client and persistence contracts consumed by account synchronization."""

from collections.abc import Mapping
from datetime import datetime
from typing import Protocol

from crypto_momentum_lab.domain.account.models import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    AccountFillEvent,
    AccountFillPageScan,
    AccountFillReconciliationCursor,
    AccountOpenOrderSnapshot,
    AccountPositionSnapshot,
    AccountReconciliationRun,
    ExecutionAccountProcessState,
)


class AccountFillProvenanceFetcher(Protocol):
    async def __call__(
        self,
        symbol: str,
        *,
        start_time_ms: int,
        checked_through: datetime,
        max_pages_per_window: int = 10,
    ) -> tuple[tuple[AccountFillEvent, ...], AccountFillPageScan]: ...


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

    @property
    def incomplete_fill_symbols(self) -> frozenset[str]:
        pass

    async def fetch_recent_fills(
        self,
        symbols: tuple[str, ...] = (),
        *,
        from_id_by_symbol: Mapping[str, int] | None = None,
        start_time_by_symbol: Mapping[str, int] | None = None,
    ) -> tuple[AccountFillEvent, ...]:
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


class AccountSyncRepository(Protocol):
    async def save_process_state(self, state: ExecutionAccountProcessState) -> None:
        pass

    async def save_balance_snapshot(self, snapshot: AccountBalanceSnapshot) -> None:
        pass

    async def save_position_snapshot(self, snapshot: AccountPositionSnapshot) -> None:
        pass

    async def save_balance_position_snapshot(
        self,
        *,
        balances: tuple[AccountBalanceSnapshot, ...],
        positions: tuple[AccountPositionSnapshot, ...],
    ) -> None:
        pass

    async def upsert_open_order(self, order: AccountOpenOrderSnapshot) -> None:
        pass

    async def save_fill_event(self, fill: AccountFillEvent) -> None:
        pass

    async def save_config_snapshot(self, snapshot: AccountConfigSnapshot) -> None:
        pass

    async def save_reconciliation_run(self, run: AccountReconciliationRun) -> None:
        pass

    async def save_reconciliation_snapshot(
        self,
        *,
        config: AccountConfigSnapshot,
        balances: tuple[AccountBalanceSnapshot, ...],
        positions: tuple[AccountPositionSnapshot, ...],
        open_orders: tuple[AccountOpenOrderSnapshot, ...],
        fills: tuple[AccountFillEvent, ...],
        run: AccountReconciliationRun,
        cursors: tuple[AccountFillReconciliationCursor, ...] = (),
    ) -> None:
        pass

    async def save_reconciliation_fills_and_cursors(
        self,
        *,
        fills: tuple[AccountFillEvent, ...],
        cursors: tuple[AccountFillReconciliationCursor, ...] = (),
    ) -> None:
        pass

    async def save_fill_reconciliation_cursors(
        self,
        cursors: tuple[AccountFillReconciliationCursor, ...],
    ) -> None:
        pass
