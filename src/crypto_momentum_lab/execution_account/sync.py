from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from crypto_momentum_lab.domain.account import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    AccountFillEvent,
    AccountOpenOrderSnapshot,
    AccountPositionSnapshot,
    AccountReconciliationRun,
    ExecutionAccountProcessState,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.domain.market.models import JsonValue


class ReadOnlyAccountClient(Protocol):
    async def fetch_account_config(self) -> AccountConfigSnapshot:
        pass

    async def fetch_balances(self) -> tuple[AccountBalanceSnapshot, ...]:
        pass

    async def fetch_positions(self) -> tuple[AccountPositionSnapshot, ...]:
        pass

    async def fetch_open_orders(self) -> tuple[AccountOpenOrderSnapshot, ...]:
        pass

    async def fetch_recent_fills(self) -> tuple[AccountFillEvent, ...]:
        pass


class AccountSyncRepository(Protocol):
    async def save_process_state(self, state: ExecutionAccountProcessState) -> None:
        pass

    async def save_balance_snapshot(self, snapshot: AccountBalanceSnapshot) -> None:
        pass

    async def save_position_snapshot(self, snapshot: AccountPositionSnapshot) -> None:
        pass

    async def upsert_open_order(self, order: AccountOpenOrderSnapshot) -> None:
        pass

    async def save_fill_event(self, fill: AccountFillEvent) -> None:
        pass

    async def save_config_snapshot(self, snapshot: AccountConfigSnapshot) -> None:
        pass

    async def save_reconciliation_run(self, run: AccountReconciliationRun) -> None:
        pass


@dataclass(frozen=True, slots=True)
class ExecutionAccountSyncConfig:
    environment: str
    account_label: str
    expected_multi_assets_mode: bool
    observed_at: datetime

    def __post_init__(self) -> None:
        if not self.environment.strip():
            raise ValueError("environment must not be empty")
        if not self.account_label.strip():
            raise ValueError("account_label must not be empty")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class ExecutionAccountSyncResult:
    status: ExecutionAccountStatus
    reconciliation_id: str
    mismatch_count: int


class ExecutionAccountSyncService:
    def __init__(
        self,
        *,
        client: ReadOnlyAccountClient,
        repository: AccountSyncRepository,
        config: ExecutionAccountSyncConfig,
    ) -> None:
        self._client = client
        self._repository = repository
        self._config = config

    async def sync_once(self) -> ExecutionAccountSyncResult:
        await self._save_state(ExecutionAccountStatus.STARTING)
        await self._save_state(ExecutionAccountStatus.SYNCING)
        account_config = await self._client.fetch_account_config()
        await self._repository.save_config_snapshot(account_config)
        reconciliation_id = _reconciliation_id(self._config)
        if account_config.multi_assets_mode != self._config.expected_multi_assets_mode:
            reason = "multi_assets_mode_mismatch"
            run = _reconciliation_run(
                self._config,
                reconciliation_id=reconciliation_id,
                status="halted",
                mismatch_count=1,
                details={"reason": reason},
            )
            await self._repository.save_reconciliation_run(run)
            await self._save_state(ExecutionAccountStatus.HALTED_READONLY, reason)
            return ExecutionAccountSyncResult(
                status=ExecutionAccountStatus.HALTED_READONLY,
                reconciliation_id=reconciliation_id,
                mismatch_count=1,
            )

        balances = await self._client.fetch_balances()
        positions = await self._client.fetch_positions()
        open_orders = await self._client.fetch_open_orders()
        fills = await self._client.fetch_recent_fills()
        for balance in balances:
            await self._repository.save_balance_snapshot(balance)
        for position in positions:
            await self._repository.save_position_snapshot(position)
        for order in open_orders:
            await self._repository.upsert_open_order(order)
        for fill in fills:
            await self._repository.save_fill_event(fill)
        await self._repository.save_reconciliation_run(
            _reconciliation_run(
                self._config,
                reconciliation_id=reconciliation_id,
                status="ready",
                mismatch_count=0,
                details={},
                balance_count=len(balances),
                position_count=len(positions),
                open_order_count=len(open_orders),
                fill_count=len(fills),
            )
        )
        await self._save_state(ExecutionAccountStatus.READY_READONLY)
        return ExecutionAccountSyncResult(
            status=ExecutionAccountStatus.READY_READONLY,
            reconciliation_id=reconciliation_id,
            mismatch_count=0,
        )

    async def _save_state(
        self,
        state: ExecutionAccountStatus,
        reason: str | None = None,
    ) -> None:
        await self._repository.save_process_state(
            ExecutionAccountProcessState(
                environment=self._config.environment,
                account_label=self._config.account_label,
                state=state,
                occurred_at=self._config.observed_at,
                reason=reason,
            )
        )


def _reconciliation_id(config: ExecutionAccountSyncConfig) -> str:
    return str(
        uuid5(
            NAMESPACE_URL,
            "account-reconciliation:"
            f"{config.environment}:{config.account_label}:"
            f"{config.observed_at.isoformat()}",
        )
    )


def _reconciliation_run(
    config: ExecutionAccountSyncConfig,
    *,
    reconciliation_id: str,
    status: str,
    mismatch_count: int,
    details: dict[str, JsonValue],
    balance_count: int = 0,
    position_count: int = 0,
    open_order_count: int = 0,
    fill_count: int = 0,
) -> AccountReconciliationRun:
    return AccountReconciliationRun(
        reconciliation_id=reconciliation_id,
        environment=config.environment,
        account_label=config.account_label,
        status=status,
        observed_at=config.observed_at,
        balance_count=balance_count,
        position_count=position_count,
        open_order_count=open_order_count,
        fill_count=fill_count,
        mismatch_count=mismatch_count,
        details=details,
    )
