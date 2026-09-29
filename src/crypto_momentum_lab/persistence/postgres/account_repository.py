from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import structlog
from sqlalchemy import case, delete, func, select, text, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.account import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    AccountFillEvent,
    AccountFillReconciliationCursor,
    AccountOpenOrderSnapshot,
    AccountPositionSnapshot,
    AccountPositionStateSnapshot,
    AccountReconciliationHead,
    AccountReconciliationRun,
    ExecutionAccountProcessState,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountBalanceSnapshotRow,
    AccountConfigSnapshotRow,
    AccountFillEventRow,
    AccountFillReconciliationCursorRow,
    AccountOpenOrderRow,
    AccountPositionSnapshotRow,
    AccountReconciliationHeadRow,
    AccountReconciliationRunRow,
    ExecutionAccountProcessStateRow,
)
from crypto_momentum_lab.persistence.postgres.serialization import jsonable

log = structlog.get_logger()


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
        "raw_payload": jsonable(snapshot.raw_payload),
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
        async with self._session_factory() as session:
            async with session.begin():
                await self._insert_in_session(
                    session,
                    AccountReconciliationRunRow,
                    reconciliation_run_row(run),
                )
                await self._upsert_reconciliation_head_in_session(session, run)

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
        """Persist one account observation atomically across all tables."""
        async with self._session_factory() as session:
            async with session.begin():
                # Open-order snapshots are a replace-all projection.  A
                # transaction advisory lock serializes writers across
                # processes, including the empty-snapshot case where there is
                # no row whose observed_at could act as a fence.
                await session.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
                    {
                        "lock_key": (
                            f"account-open-orders:{config.environment}:"
                            f"{config.account_label}"
                        )
                    },
                )
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
                latest_reconciliation_observed_at = await session.scalar(
                    select(func.max(AccountReconciliationRunRow.observed_at)).where(
                        AccountReconciliationRunRow.environment == config.environment,
                        AccountReconciliationRunRow.account_label
                        == config.account_label,
                        AccountReconciliationRunRow.status == "ready",
                    )
                )
                snapshot_observed_at = max(config.observed_at, run.observed_at)
                if (
                    latest_reconciliation_observed_at is None
                    or snapshot_observed_at >= latest_reconciliation_observed_at
                ):
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
                if cursors:
                    await self._save_fill_reconciliation_cursors_in_session(
                        session, cursors
                    )
                await self._insert_in_session(
                    session,
                    AccountReconciliationRunRow,
                    reconciliation_run_row(run),
                )
                await self._upsert_reconciliation_head_in_session(session, run)

    async def save_reconciliation_fills_and_cursors(
        self,
        *,
        fills: tuple[AccountFillEvent, ...],
        cursors: tuple[AccountFillReconciliationCursor, ...] = (),
    ) -> None:
        """Persist fills and cursor updates atomically without snapshot state."""
        if not fills and not cursors:
            return
        async with self._session_factory() as session:
            async with session.begin():
                if fills:
                    await self._insert_in_session(
                        session,
                        AccountFillEventRow,
                        [fill_event_row(item) for item in fills],
                    )
                if cursors:
                    await self._save_fill_reconciliation_cursors_in_session(
                        session, cursors
                    )

    async def save_process_state(self, state: ExecutionAccountProcessState) -> None:
        await self._insert(ExecutionAccountProcessStateRow, process_state_row(state))

    async def load_historical_fill_symbols(
        self,
        *,
        environment: str,
        account_label: str,
    ) -> frozenset[str]:
        """Return every symbol already seen in the durable fill ledger.

        A restarted account synchronizer cannot rely on the current position
        snapshot to discover symbols: a symbol can have been opened and fully
        closed while the process was down.  Seeding REST fill reconciliation
        from this immutable history lets the next sync repair that gap without
        changing the live position view.
        """
        if not environment.strip():
            raise ValueError("environment must not be empty")
        if not account_label.strip():
            raise ValueError("account_label must not be empty")
        async with self._session_factory() as session:
            symbols = await session.scalars(
                select(AccountFillEventRow.symbol)
                .where(
                    AccountFillEventRow.environment == environment,
                    AccountFillEventRow.account_label == account_label,
                )
                .distinct()
            )
            return frozenset(symbols.all())

    async def load_fill_reconciliation_cursors(
        self,
        *,
        environment: str,
        account_label: str,
    ) -> dict[str, AccountFillReconciliationCursor]:
        if not environment.strip():
            raise ValueError("environment must not be empty")
        if not account_label.strip():
            raise ValueError("account_label must not be empty")
        async with self._session_factory() as session:
            rows = await session.scalars(
                select(AccountFillReconciliationCursorRow).where(
                    AccountFillReconciliationCursorRow.environment == environment,
                    AccountFillReconciliationCursorRow.account_label == account_label,
                )
            )
            return {
                row.symbol: AccountFillReconciliationCursor(
                    environment=row.environment,
                    account_label=row.account_label,
                    symbol=row.symbol,
                    from_id=row.from_id,
                    start_time_ms=row.start_time_ms,
                    last_checked_at=row.last_checked_at,
                )
                for row in rows.all()
            }

    @staticmethod
    async def _save_fill_reconciliation_cursors_in_session(
        session: AsyncSession,
        cursors: Sequence[AccountFillReconciliationCursor],
    ) -> None:
        if not cursors:
            return
        values = [fill_reconciliation_cursor_row(cursor) for cursor in cursors]
        statement = insert(AccountFillReconciliationCursorRow).values(values)
        statement = statement.on_conflict_do_update(
            index_elements=[
                "environment",
                "account_label",
                "symbol",
            ],
            set_={
                # Reconciliation persistence is intentionally
                # asynchronous.  A slower result must never move a
                # cursor backwards after a newer result committed.
                # The cursor mode is part of the versioned value: an
                # id cursor clears the time cursor and vice versa, so
                # the one-position check constraint remains valid.
                "from_id": case(
                    (
                        statement.excluded.from_id.is_not(None),
                        func.greatest(
                            func.coalesce(
                                AccountFillReconciliationCursorRow.from_id,
                                0,
                            ),
                            statement.excluded.from_id,
                        ),
                    ),
                    else_=None,
                ),
                "start_time_ms": case(
                    (
                        statement.excluded.start_time_ms.is_not(None),
                        func.greatest(
                            func.coalesce(
                                AccountFillReconciliationCursorRow.start_time_ms,
                                0,
                            ),
                            statement.excluded.start_time_ms,
                        ),
                    ),
                    else_=None,
                ),
                "last_checked_at": statement.excluded.last_checked_at,
            },
            where=(
                AccountFillReconciliationCursorRow.last_checked_at
                <= statement.excluded.last_checked_at
            ),
        )
        await session.execute(statement)

    async def save_fill_reconciliation_cursors(
        self,
        cursors: tuple[AccountFillReconciliationCursor, ...],
    ) -> None:
        if not cursors:
            return
        async with self._session_factory() as session:
            async with session.begin():
                await self._save_fill_reconciliation_cursors_in_session(
                    session, cursors
                )

    async def load_active_position_state(
        self,
        *,
        environment: str,
        account_label: str,
    ) -> AccountPositionStateSnapshot | None:
        if not environment.strip():
            raise ValueError("environment must not be empty")
        if not account_label.strip():
            raise ValueError("account_label must not be empty")
        async with self._session_factory() as session:
            latest_run = await session.scalar(
                select(AccountReconciliationRunRow)
                .where(
                    AccountReconciliationRunRow.environment == environment,
                    AccountReconciliationRunRow.account_label == account_label,
                    AccountReconciliationRunRow.status.in_(("ready", "catching_up")),
                )
                .order_by(
                    AccountReconciliationRunRow.observed_at.desc(),
                    AccountReconciliationRunRow.reconciliation_id.desc(),
                )
                .limit(1)
            )
            if latest_run is None:
                return None

            versioned = _position_state_from_run(latest_run)
            if versioned is not None:
                return versioned
            if latest_run.position_count == 0:
                # A zero-count run is authoritative even in legacy data. Do not
                # resurrect older non-zero detail rows after a flat reconciliation.
                return AccountPositionStateSnapshot(
                    environment=environment,
                    account_label=account_label,
                    reconciliation_id=latest_run.reconciliation_id,
                    observed_at=latest_run.observed_at,
                    position_count=0,
                    position_keys=(),
                    complete=True,
                )

            # Legacy position history is sparse per key. Reconstruct the best
            # known set at the run's cut, never from one account-wide timestamp
            # and never from observations after that cut. The result remains
            # incomplete because old rows do not prove an account-wide snapshot.
            latest_by_key = (
                select(
                    AccountPositionSnapshotRow.symbol,
                    AccountPositionSnapshotRow.position_side,
                    AccountPositionSnapshotRow.position_amt,
                )
                .where(
                    AccountPositionSnapshotRow.environment == environment,
                    AccountPositionSnapshotRow.account_label == account_label,
                    AccountPositionSnapshotRow.observed_at <= latest_run.observed_at,
                )
                .distinct(
                    AccountPositionSnapshotRow.symbol,
                    AccountPositionSnapshotRow.position_side,
                )
                .order_by(
                    AccountPositionSnapshotRow.symbol,
                    AccountPositionSnapshotRow.position_side,
                    AccountPositionSnapshotRow.observed_at.desc(),
                    AccountPositionSnapshotRow.snapshot_id.desc(),
                )
                .subquery()
            )
            rows = (
                await session.execute(
                    select(
                        latest_by_key.c.symbol,
                        latest_by_key.c.position_side,
                        latest_by_key.c.position_amt,
                    )
                )
            ).all()
            known_keys = tuple(
                (row.symbol, row.position_side)
                for row in rows
                if row.position_amt != 0
            )
            if len(known_keys) != latest_run.position_count:
                log.warning(
                    "legacy_position_state_incomplete",
                    environment=environment,
                    account_label=account_label,
                    reconciliation_id=latest_run.reconciliation_id,
                    expected_position_count=latest_run.position_count,
                    known_position_count=len(known_keys),
                )
            return AccountPositionStateSnapshot(
                environment=environment,
                account_label=account_label,
                reconciliation_id=latest_run.reconciliation_id,
                observed_at=latest_run.observed_at,
                position_count=latest_run.position_count,
                position_keys=known_keys,
                complete=False,
            )

    async def load_active_position_symbols(
        self,
        *,
        environment: str,
        account_label: str,
    ) -> frozenset[str]:
        state = await self.load_active_position_state(
            environment=environment,
            account_label=account_label,
        )
        return frozenset() if state is None else state.symbols

    async def load_active_position_account_labels(
        self,
        *,
        environment: str,
        account_labels: Iterable[str] | None = None,
    ) -> frozenset[str]:
        """Return live account labels whose latest ready run has positions.

        Queries the single-row-per-account `account_reconciliation_heads` projection
        table. If the projection has not been populated yet for the given environment,
        it defensively falls back to the historical reconciliation runs query.
        """
        if not environment.strip():
            raise ValueError("environment must not be empty")
        expected_labels = set(account_labels) if account_labels is not None else None
        if expected_labels == set():
            return frozenset()
        async with self._session_factory() as session:
            head_stmt = select(AccountReconciliationHeadRow).where(
                AccountReconciliationHeadRow.environment == environment,
                AccountReconciliationHeadRow.status == "ready",
            )
            if expected_labels is not None:
                head_stmt = head_stmt.where(
                    AccountReconciliationHeadRow.account_label.in_(expected_labels)
                )
            heads = (await session.scalars(head_stmt)).all()
            active_by_label = {
                row.account_label: (
                    row.position_count > 0
                    or (
                        (state := _position_state_from_run(row)) is not None
                        and not state.complete
                    )
                )
                for row in heads
            }

            # Reconciliation runs also carry complete position state while
            # fill coverage is catching up. Read the newest usable run per
            # account so those observations supersede older ready heads.
            latest_runs = (
                select(AccountReconciliationRunRow)
                .distinct(AccountReconciliationRunRow.account_label)
                .where(
                    AccountReconciliationRunRow.environment == environment,
                    AccountReconciliationRunRow.status.in_(
                        ("ready", "catching_up")
                    ),
                )
            )
            if expected_labels is not None:
                latest_runs = latest_runs.where(
                    AccountReconciliationRunRow.account_label.in_(expected_labels)
                )
            latest_runs = latest_runs.order_by(
                AccountReconciliationRunRow.account_label,
                AccountReconciliationRunRow.observed_at.desc(),
                AccountReconciliationRunRow.reconciliation_id.desc(),
            )
            current_runs = (await session.scalars(latest_runs)).all()
            for row in current_runs:
                state = _position_state_from_run(row)
                active_by_label[row.account_label] = (
                    row.position_count > 0
                    or (state is not None and not state.complete)
                )

            return frozenset(
                label for label, active in active_by_label.items() if active
            )

    async def load_reconciliation_heads(
        self,
        *,
        environment: str,
    ) -> dict[str, AccountReconciliationHead]:
        """Return a mapping of account_label to its latest ready reconciliation head."""
        if not environment.strip():
            raise ValueError("environment must not be empty")
        async with self._session_factory() as session:
            rows = (
                await session.scalars(
                    select(AccountReconciliationHeadRow).where(
                        AccountReconciliationHeadRow.environment == environment,
                    )
                )
            ).all()
            return {
                row.account_label: AccountReconciliationHead(
                    environment=row.environment,
                    account_label=row.account_label,
                    reconciliation_id=row.reconciliation_id,
                    status=row.status,
                    observed_at=row.observed_at,
                    balance_count=row.balance_count,
                    position_count=row.position_count,
                    open_order_count=row.open_order_count,
                    fill_count=row.fill_count,
                    mismatch_count=row.mismatch_count,
                    details=dict(row.details or {}),  # type: ignore[arg-type]
                    projection_schema_version=row.projection_schema_version,
                    projected_at=row.projected_at,
                )
                for row in rows
            }

    @staticmethod
    async def _upsert_reconciliation_head_in_session(
        session: AsyncSession,
        run: AccountReconciliationRun,
        *,
        projected_at: datetime | None = None,
    ) -> None:
        if run.status != "ready":
            return
        now = projected_at or datetime.now(UTC)
        values = {
            "environment": run.environment,
            "account_label": run.account_label,
            "reconciliation_id": run.reconciliation_id,
            "status": run.status,
            "observed_at": run.observed_at,
            "balance_count": run.balance_count,
            "position_count": run.position_count,
            "open_order_count": run.open_order_count,
            "fill_count": run.fill_count,
            "mismatch_count": run.mismatch_count,
            "details": jsonable(run.details),
            "projection_schema_version": 1,
            "projected_at": now,
        }
        stmt = insert(AccountReconciliationHeadRow).values(values)
        update_dict = {
            "reconciliation_id": stmt.excluded.reconciliation_id,
            "status": stmt.excluded.status,
            "observed_at": stmt.excluded.observed_at,
            "balance_count": stmt.excluded.balance_count,
            "position_count": stmt.excluded.position_count,
            "open_order_count": stmt.excluded.open_order_count,
            "fill_count": stmt.excluded.fill_count,
            "mismatch_count": stmt.excluded.mismatch_count,
            "details": stmt.excluded.details,
            "projection_schema_version": stmt.excluded.projection_schema_version,
            "projected_at": stmt.excluded.projected_at,
        }
        where_cond = tuple_(
            stmt.excluded.observed_at,
            stmt.excluded.reconciliation_id,
        ) >= tuple_(
            AccountReconciliationHeadRow.observed_at,
            AccountReconciliationHeadRow.reconciliation_id,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["environment", "account_label"],
            set_=update_dict,
            where=where_cond,
        )
        await session.execute(stmt)

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
        await session.execute(insert(model).values(values).on_conflict_do_nothing())


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
        "raw_payload": jsonable(snapshot.raw_payload),
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
        "raw_payload": jsonable(order.raw_payload),
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
        "raw_payload": jsonable(fill.raw_payload),
    }


def fill_reconciliation_cursor_row(
    cursor: AccountFillReconciliationCursor,
) -> dict[str, object]:
    return {
        "environment": cursor.environment,
        "account_label": cursor.account_label,
        "symbol": cursor.symbol,
        "from_id": cursor.from_id,
        "start_time_ms": cursor.start_time_ms,
        "last_checked_at": cursor.last_checked_at,
    }


def config_snapshot_row(snapshot: AccountConfigSnapshot) -> dict[str, object]:
    return {
        **_snapshot_base(snapshot, "account-config", "config"),
        "multi_assets_mode": snapshot.multi_assets_mode,
        "hedge_mode": snapshot.hedge_mode,
        "fee_tier": snapshot.fee_tier,
        "raw_payload": jsonable(snapshot.raw_payload),
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
        "details": jsonable(run.details),
    }


def _row_id(namespace: str, *parts: str) -> UUID:
    return uuid5(NAMESPACE_URL, ":".join((namespace, *parts)))


def _position_state_from_run(
    run: AccountReconciliationRunRow | AccountReconciliationHeadRow,
) -> AccountPositionStateSnapshot | None:
    details = run.details
    if not isinstance(details, Mapping):
        return None
    schema_version = details.get("position_state_schema_version")
    if schema_version is None:
        return None
    raw_keys = details.get("position_keys")
    position_keys: list[tuple[str, str]] = []
    malformed = type(schema_version) is not int or schema_version != 1
    if not isinstance(raw_keys, list):
        malformed = True
    else:
        for item in raw_keys:
            if not isinstance(item, Mapping):
                malformed = True
                continue
            symbol = item.get("symbol")
            side = item.get("position_side")
            if not isinstance(symbol, str) or not isinstance(side, str):
                malformed = True
                continue
            normalized_key = (symbol.strip().upper(), side.strip().upper())
            if not all(normalized_key):
                malformed = True
                continue
            position_keys.append(normalized_key)
    if len(set(position_keys)) != len(position_keys):
        malformed = True
    position_keys = sorted(set(position_keys))
    complete = (
        not malformed
        and schema_version == 1
        and len(position_keys) == run.position_count
    )
    if not complete:
        log.warning(
            "account_position_state_details_incomplete",
            environment=run.environment,
            account_label=run.account_label,
            reconciliation_id=run.reconciliation_id,
            schema_version=schema_version,
            expected_position_count=run.position_count,
            known_position_count=len(position_keys),
        )
    return AccountPositionStateSnapshot(
        environment=run.environment,
        account_label=run.account_label,
        reconciliation_id=run.reconciliation_id,
        observed_at=run.observed_at,
        position_count=run.position_count,
        position_keys=tuple(position_keys),
        complete=complete,
    )
