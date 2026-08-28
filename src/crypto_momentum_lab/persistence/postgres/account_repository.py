from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.account import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    AccountFillEvent,
    AccountOpenOrderSnapshot,
    AccountPositionSnapshot,
    AccountReconciliationRun,
    ExecutionAccountProcessState,
)
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.persistence.postgres.models import (
    AccountBalanceSnapshotRow,
    AccountConfigSnapshotRow,
    AccountFillEventRow,
    AccountOpenOrderRow,
    AccountPositionSnapshotRow,
    AccountReconciliationRunRow,
    ExecutionAccountProcessStateRow,
)


def balance_snapshot_row(snapshot: AccountBalanceSnapshot) -> dict[str, object]:
    return {
        "snapshot_id": _row_id(
            "account-balance",
            snapshot.environment,
            snapshot.account_label,
            snapshot.asset,
            snapshot.observed_at.isoformat(),
        ),
        "environment": snapshot.environment,
        "account_label": snapshot.account_label,
        "asset": snapshot.asset,
        "wallet_balance": snapshot.wallet_balance,
        "available_balance": snapshot.available_balance,
        "unrealized_pnl": snapshot.unrealized_pnl,
        "observed_at": snapshot.observed_at,
        "raw_payload": _jsonable(snapshot.raw_payload),
    }


def process_state_row(state: ExecutionAccountProcessState) -> dict[str, object]:
    return {
        "state_id": _row_id(
            "execution-account-state",
            state.environment,
            state.account_label,
            state.state.value,
            state.occurred_at.isoformat(),
            state.reason or "",
        ),
        "environment": state.environment,
        "account_label": state.account_label,
        "state": state.state.value,
        "occurred_at": state.occurred_at,
        "reason": state.reason,
    }


class PostgresAccountRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory

    async def save_balance_snapshot(self, snapshot: AccountBalanceSnapshot) -> None:
        await self._insert(AccountBalanceSnapshotRow, balance_snapshot_row(snapshot))

    async def save_position_snapshot(self, snapshot: AccountPositionSnapshot) -> None:
        await self._insert(
            AccountPositionSnapshotRow,
            position_snapshot_row(snapshot),
        )

    async def save_balance_position_snapshot(
        self,
        *,
        balances: tuple[AccountBalanceSnapshot, ...],
        positions: tuple[AccountPositionSnapshot, ...],
    ) -> None:
        """Persist a lightweight account-state observation atomically."""
        async with self._session_factory() as session:
            async with session.begin():
                await self._insert_in_session(
                    session,
                    AccountBalanceSnapshotRow,
                    [balance_snapshot_row(item) for item in balances],
                )
                await self._insert_in_session(
                    session,
                    AccountPositionSnapshotRow,
                    [position_snapshot_row(item) for item in positions],
                )

    async def upsert_open_order(self, order: AccountOpenOrderSnapshot) -> None:
        await self._insert(AccountOpenOrderRow, open_order_snapshot_row(order))

    async def save_fill_event(self, fill: AccountFillEvent) -> None:
        await self._insert(AccountFillEventRow, fill_event_row(fill))

    async def save_config_snapshot(self, snapshot: AccountConfigSnapshot) -> None:
        await self._insert(AccountConfigSnapshotRow, config_snapshot_row(snapshot))

    async def save_reconciliation_run(self, run: AccountReconciliationRun) -> None:
        await self._insert(
            AccountReconciliationRunRow,
            reconciliation_run_row(run),
        )

    async def save_reconciliation_snapshot(
        self,
        *,
        config: AccountConfigSnapshot,
        balances: tuple[AccountBalanceSnapshot, ...],
        positions: tuple[AccountPositionSnapshot, ...],
        open_orders: tuple[AccountOpenOrderSnapshot, ...],
        fills: tuple[AccountFillEvent, ...],
        run: AccountReconciliationRun,
    ) -> None:
        """Persist one account observation atomically across all tables."""
        async with self._session_factory() as session:
            async with session.begin():
                await self._insert_in_session(
                    session,
                    AccountConfigSnapshotRow,
                    config_snapshot_row(config),
                )
                await self._insert_in_session(
                    session,
                    AccountBalanceSnapshotRow,
                    [balance_snapshot_row(item) for item in balances],
                )
                await self._insert_in_session(
                    session,
                    AccountPositionSnapshotRow,
                    [position_snapshot_row(item) for item in positions],
                )
                await session.execute(
                    delete(AccountOpenOrderRow).where(
                        AccountOpenOrderRow.environment == config.environment,
                        AccountOpenOrderRow.account_label == config.account_label,
                    )
                )
                await self._insert_in_session(
                    session,
                    AccountOpenOrderRow,
                    [open_order_snapshot_row(item) for item in open_orders],
                )
                await self._insert_in_session(
                    session,
                    AccountFillEventRow,
                    [fill_event_row(item) for item in fills],
                )
                await self._insert_in_session(
                    session,
                    AccountReconciliationRunRow,
                    reconciliation_run_row(run),
                )

    async def save_process_state(self, state: ExecutionAccountProcessState) -> None:
        await self._insert(ExecutionAccountProcessStateRow, process_state_row(state))

    async def load_active_position_symbols(
        self,
        *,
        environment: str,
        account_label: str,
    ) -> frozenset[str]:
        if not environment.strip():
            raise ValueError("environment must not be empty")
        if not account_label.strip():
            raise ValueError("account_label must not be empty")
        async with self._session_factory() as session:
            reconciliation = await session.scalar(
                select(AccountReconciliationRunRow)
                .where(
                    AccountReconciliationRunRow.environment == environment,
                    AccountReconciliationRunRow.account_label == account_label,
                    AccountReconciliationRunRow.status == "ready",
                )
                .order_by(AccountReconciliationRunRow.observed_at.desc())
                .limit(1)
            )
            if reconciliation is None or reconciliation.position_count == 0:
                return frozenset()
            latest_observed_at = await session.scalar(
                select(func.max(AccountPositionSnapshotRow.observed_at)).where(
                    AccountPositionSnapshotRow.environment == environment,
                    AccountPositionSnapshotRow.account_label == account_label,
                )
            )
            if latest_observed_at is None:
                raise RuntimeError(
                    "ready reconciliation is missing active position snapshots"
                )
            symbols = await session.scalars(
                select(AccountPositionSnapshotRow.symbol).where(
                    AccountPositionSnapshotRow.environment == environment,
                    AccountPositionSnapshotRow.account_label == account_label,
                    AccountPositionSnapshotRow.observed_at == latest_observed_at,
                    AccountPositionSnapshotRow.position_amt != 0,
                )
            )
            return frozenset(symbols.all())

    async def _insert(self, model: Any, values: dict[str, object]) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                await self._insert_in_session(session, model, values)

    @staticmethod
    async def _insert_in_session(
        session: AsyncSession,
        model: Any,
        values: dict[str, object] | list[dict[str, object]],
    ) -> None:
        if not values:
            return
        await session.execute(
            insert(model).values(values).on_conflict_do_nothing()
        )


def _snapshot_base(
    snapshot: AccountPositionSnapshot | AccountConfigSnapshot,
    namespace: str,
    key: str,
) -> dict[str, object]:
    values = asdict(snapshot)
    return {
        "snapshot_id": _row_id(
            namespace,
            str(values["environment"]),
            str(values["account_label"]),
            key,
            values["observed_at"].isoformat(),
        ),
        "environment": values["environment"],
        "account_label": values["account_label"],
        "observed_at": values["observed_at"],
    }


def position_snapshot_row(snapshot: AccountPositionSnapshot) -> dict[str, object]:
    return {
        **_snapshot_base(
            snapshot,
            "account-position",
            f"{snapshot.symbol}:{snapshot.position_side}",
        ),
        "symbol": snapshot.symbol,
        "position_side": snapshot.position_side,
        "position_amt": snapshot.position_amt,
        "entry_price": snapshot.entry_price,
        "mark_price": snapshot.mark_price,
        "unrealized_pnl": snapshot.unrealized_pnl,
        "notional": snapshot.notional,
        "leverage": snapshot.leverage,
        "margin_type": snapshot.margin_type,
        "raw_payload": _jsonable(snapshot.raw_payload),
    }


def open_order_snapshot_row(order: AccountOpenOrderSnapshot) -> dict[str, object]:
    return {
        "environment": order.environment,
        "account_label": order.account_label,
        "symbol": order.symbol,
        "order_id": order.order_id,
        "client_order_id": order.client_order_id,
        "side": order.side,
        "order_type": order.order_type,
        "status": order.status,
        "price": order.price,
        "original_quantity": order.original_quantity,
        "executed_quantity": order.executed_quantity,
        "reduce_only": order.reduce_only,
        "observed_at": order.observed_at,
        "raw_payload": _jsonable(order.raw_payload),
    }


def fill_event_row(fill: AccountFillEvent) -> dict[str, object]:
    return {
        "environment": fill.environment,
        "account_label": fill.account_label,
        "symbol": fill.symbol,
        "trade_id": fill.trade_id,
        "order_id": fill.order_id,
        "side": fill.side,
        "price": fill.price,
        "quantity": fill.quantity,
        "realized_pnl": fill.realized_pnl,
        "fee": fill.fee,
        "fee_asset": fill.fee_asset,
        "trade_at": fill.trade_at,
        "raw_payload": _jsonable(fill.raw_payload),
    }


def config_snapshot_row(snapshot: AccountConfigSnapshot) -> dict[str, object]:
    return {
        **_snapshot_base(snapshot, "account-config", "config"),
        "multi_assets_mode": snapshot.multi_assets_mode,
        "hedge_mode": snapshot.hedge_mode,
        "can_trade": snapshot.can_trade,
        "fee_tier": snapshot.fee_tier,
        "raw_payload": _jsonable(snapshot.raw_payload),
    }


def reconciliation_run_row(run: AccountReconciliationRun) -> dict[str, object]:
    return {
        "reconciliation_id": run.reconciliation_id,
        "environment": run.environment,
        "account_label": run.account_label,
        "status": run.status,
        "observed_at": run.observed_at,
        "balance_count": run.balance_count,
        "position_count": run.position_count,
        "open_order_count": run.open_order_count,
        "fill_count": run.fill_count,
        "mismatch_count": run.mismatch_count,
        "details": _jsonable(run.details),
    }


def _row_id(namespace: str, *parts: str) -> UUID:
    return uuid5(NAMESPACE_URL, ":".join((namespace, *parts)))


def _jsonable(value: object) -> JsonValue:
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, datetime):
        return (
            value.astimezone(UTC).isoformat()
            if value.tzinfo is not None and value.utcoffset() is not None
            else value.isoformat()
        )
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    return str(value)
