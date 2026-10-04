"""Client and persistence contracts consumed by account synchronization."""

from collections.abc import Mapping
from datetime import datetime
from typing import Protocol

from crypto_momentum_lab.domain.account.event_journal import AccountEventReceipt
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
    async def append_user_data_event(
        self,
        *,
        environment: str,
        account_label: str,
        receiver_session_id: str,
        stream_token: int | None,
        event: AccountEventReceipt,
    ) -> int:
        pass

    async def user_data_journal_cursor(
        self, *, environment: str, account_label: str
    ) -> int:
        pass

    async def save_process_state(self, state: ExecutionAccountProcessState) -> None:
        pass

    async def save_position_snapshot(self, snapshot: AccountPositionSnapshot) -> None:
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
