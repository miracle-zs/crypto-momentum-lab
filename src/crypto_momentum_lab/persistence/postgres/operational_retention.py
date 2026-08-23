"""Bounded retention for high-churn operational tables.

These tables are serving the live control plane, not acting as an archive.  A
bounded, small-batch delete keeps the database's working set finite and avoids
one large transaction holding locks or generating a second memory spike.
"""

from datetime import datetime
from typing import Any, cast

from sqlalchemy import text
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.sql.elements import TextClause

_ACCOUNT_SNAPSHOT_KEYS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "account_balance_snapshots",
        ("environment", "account_label", "asset"),
    ),
    (
        "account_position_snapshots",
        ("environment", "account_label", "symbol", "position_side"),
    ),
    (
        "account_config_snapshots",
        ("environment", "account_label"),
    ),
    (
        "account_reconciliation_runs",
        ("environment", "account_label"),
    ),
)


class PostgresOperationalRetentionRepository:
    """Prune data that can be rebuilt from the market-data archive."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory

    async def prune_contract_metadata(
        self,
        *,
        before: datetime,
        batch_size: int = 1_000,
    ) -> int:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        # Keep the newest snapshot for every symbol even when it is older than
        # the retention horizon.  The live runner needs one usable rule row;
        # deleting the only row makes an unchanged in-memory metadata cache
        # look valid while the database is empty.
        statement = text(
            "WITH doomed AS ("
            "SELECT metadata.ctid "
            "FROM contract_metadata AS metadata "
            "WHERE metadata.effective_at < :before "
            "AND EXISTS ("
            "SELECT 1 FROM contract_metadata AS newer "
            "WHERE newer.symbol = metadata.symbol "
            "AND newer.effective_at > metadata.effective_at"
            ") "
            "ORDER BY metadata.effective_at "
            "LIMIT :batch_size"
            ") "
            "DELETE FROM contract_metadata AS metadata "
            "USING doomed "
            "WHERE metadata.ctid = doomed.ctid"
        )
        return await self._execute_delete(
            statement,
            before=before,
            batch_size=batch_size,
        )

    async def prune_runtime_market_states(
        self,
        *,
        before: datetime,
        batch_size: int = 1_000,
    ) -> int:
        return await self._delete_batch(
            "runtime_market_states_15s",
            "bucket_start",
            before=before,
            batch_size=batch_size,
        )

    async def prune_account_snapshots(
        self,
        *,
        environment: str,
        account_label: str,
        before: datetime,
        batch_size: int = 1_000,
        max_rows_per_table: int = 10_000,
    ) -> dict[str, int]:
        """Delete old account snapshots without deleting each latest view.

        Account snapshot tables are operational history, not the durable fill
        ledger.  The latest row for every balance asset, position side,
        account configuration, and reconciliation stream is retained even if
        it is older than the configured horizon.  Fills are intentionally not
        included here because they are the audit trail for execution.
        """
        if not environment.strip():
            raise ValueError("environment must not be empty")
        if not account_label.strip():
            raise ValueError("account_label must not be empty")
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if max_rows_per_table <= 0:
            raise ValueError("max_rows_per_table must be positive")

        deleted: dict[str, int] = {}
        for table_name, key_columns in _ACCOUNT_SNAPSHOT_KEYS:
            latest_by_key = await self._account_snapshot_latest_by_key(
                table_name,
                key_columns,
                environment=environment,
                account_label=account_label,
            )
            table_deleted = 0
            for key, latest_at in latest_by_key:
                if table_deleted >= max_rows_per_table:
                    break
                # If a key stopped updating, preserve its last row while
                # still removing the older history.  Active keys use the
                # retention horizon directly.
                delete_before = before if latest_at > before else latest_at
                while table_deleted < max_rows_per_table:
                    current_batch_size = min(
                        batch_size,
                        max_rows_per_table - table_deleted,
                    )
                    count = await self._execute_account_snapshot_delete(
                        table_name,
                        key,
                        environment=environment,
                        account_label=account_label,
                        before=delete_before,
                        batch_size=current_batch_size,
                    )
                    table_deleted += count
                    if count < current_batch_size:
                        break
            deleted[table_name] = table_deleted
        return deleted

    async def _account_snapshot_latest_by_key(
        self,
        table_name: str,
        key_columns: tuple[str, ...],
        *,
        environment: str,
        account_label: str,
    ) -> tuple[tuple[tuple[tuple[str, object], ...], datetime], ...]:
        grouped_columns = tuple(
            column
            for column in key_columns
            if column not in {"environment", "account_label"}
        )
        if not grouped_columns:
            statement = text(
                f"SELECT max(observed_at) FROM {table_name} "
                "WHERE environment = :environment "
                "AND account_label = :account_label"
            )
            async with self._session_factory() as session:
                latest = await session.scalar(
                    statement,
                    {
                        "environment": environment,
                        "account_label": account_label,
                    },
                )
            if not isinstance(latest, datetime):
                return ()
            return (((), latest),)

        columns = ", ".join(grouped_columns)
        statement = text(
            f"SELECT {columns}, max(observed_at) AS latest_observed_at "
            f"FROM {table_name} "
            "WHERE environment = :environment "
            "AND account_label = :account_label "
            f"GROUP BY {columns}"
        )
        async with self._session_factory() as session:
            rows = (
                await session.execute(
                    statement,
                    {
                        "environment": environment,
                        "account_label": account_label,
                    },
                )
            ).all()
        latest_by_key: list[tuple[tuple[tuple[str, object], ...], datetime]] = []
        for row in rows:
            latest_at = row._mapping["latest_observed_at"]
            if not isinstance(latest_at, datetime):
                continue
            key = tuple(
                (column, row._mapping[column]) for column in grouped_columns
            )
            latest_by_key.append((key, latest_at))
        return tuple(latest_by_key)

    async def _delete_batch(
        self,
        table_name: str,
        timestamp_column: str,
        *,
        before: datetime,
        batch_size: int,
    ) -> int:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        # PostgreSQL's ctid lets us bound each delete without requiring a
        # materialized list of millions of primary keys.  The retention index
        # makes the ordered candidate selection cheap.
        statement = text(
            f"WITH doomed AS ("
            f"SELECT ctid FROM {table_name} "
            f"WHERE {timestamp_column} < :before "
            f"ORDER BY {timestamp_column} "
            f"LIMIT :batch_size"
            f") "
            f"DELETE FROM {table_name} "
            f"WHERE ctid IN (SELECT ctid FROM doomed)"
        )
        return await self._execute_delete(
            statement,
            before=before,
            batch_size=batch_size,
        )

    async def _execute_delete(
        self,
        statement: TextClause,
        *,
        before: datetime,
        batch_size: int,
    ) -> int:
        async with self._session_factory() as session:
            async with session.begin():
                result = cast(
                    CursorResult[Any],
                    await session.execute(
                        statement,
                        {"before": before, "batch_size": batch_size},
                    ),
                )
                return max(result.rowcount or 0, 0)

    async def _execute_account_snapshot_delete(
        self,
        table_name: str,
        key: tuple[tuple[str, object], ...],
        *,
        environment: str,
        account_label: str,
        before: datetime,
        batch_size: int,
    ) -> int:
        key_conditions = " AND ".join(
            f"candidate.{column} = :key_{index}"
            for index, (column, _value) in enumerate(key)
        )
        scoped_key_conditions = (
            "AND " + key_conditions if key_conditions else ""
        )
        parameters: dict[str, object] = {
            "environment": environment,
            "account_label": account_label,
            "before": before,
            "batch_size": batch_size,
        }
        parameters.update(
            {f"key_{index}": value for index, (_column, value) in enumerate(key)}
        )
        statement = text(
            "WITH doomed AS ("
            f"SELECT candidate.ctid FROM {table_name} AS candidate "
            "WHERE candidate.environment = :environment "
            "AND candidate.account_label = :account_label "
            f"{scoped_key_conditions} "
            "AND candidate.observed_at < :before "
            "ORDER BY candidate.observed_at "
            "LIMIT :batch_size"
            ") "
            f"DELETE FROM {table_name} AS candidate "
            "USING doomed WHERE candidate.ctid = doomed.ctid"
        )
        async with self._session_factory() as session:
            async with session.begin():
                result = cast(
                    CursorResult[Any],
                    await session.execute(
                        statement,
                        parameters,
                    ),
                )
                return max(result.rowcount or 0, 0)
