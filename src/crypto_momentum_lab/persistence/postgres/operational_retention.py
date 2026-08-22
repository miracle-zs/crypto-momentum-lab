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
