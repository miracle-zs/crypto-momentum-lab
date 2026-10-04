"""Online partition management for the strategy runtime-event table.

``strategy_runtime_events`` is the largest PostgreSQL table on the live host
and is append-only operational history.  Once partitioned by day, retention
drops whole partitions instead of deleting hundreds of thousands of rows and
leaving dead tuples behind for VACUUM.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from re import fullmatch
from typing import Any, Final, cast

import structlog
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.operational.retention_models import (
    resolve_dataset_scope,
)

EVENT_TABLE: Final = "strategy_runtime_events"
EVENT_PARTITION_PREFIX: Final = "strategy_runtime_events_p_"
EVENT_PARTITION_INTERVAL: Final = timedelta(days=1)
# Two days of empty partitions covers a weekend host reboot without leaving
# "now" uncovered, while keeping the planner object count small.
EVENT_PARTITION_LOOKAHEAD: Final = timedelta(days=2)


log = structlog.get_logger()


def floor_event_partition_start(value: datetime) -> datetime:
    """Return the UTC day boundary containing ``value``."""

    normalized = _as_utc(value)
    return normalized.replace(hour=0, minute=0, second=0, microsecond=0)


def event_partition_name(start: datetime) -> str:
    return f"{EVENT_PARTITION_PREFIX}{floor_event_partition_start(start):%Y%m%d}"


async def event_table_is_partitioned(
    session_factory: async_sessionmaker[AsyncSession],
) -> bool:
    async with session_factory() as session:
        value = await session.scalar(
            text(
                "SELECT c.relkind = 'p' "
                "FROM pg_class AS c "
                "WHERE c.oid = to_regclass(:table_name)"
            ),
            {"table_name": EVENT_TABLE},
        )
    return bool(value)


async def ensure_event_partitions(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    through: datetime,
    from_at: datetime | None = None,
) -> int:
    """Create missing daily partitions up to ``through``.

    No-op while the relation is still the legacy unpartitioned table.
    """

    end = _ceil_event_partition_end(through)
    start = floor_event_partition_start(
        datetime.now(UTC) - EVENT_PARTITION_INTERVAL if from_at is None else from_at
    )
    if end <= start:
        return 0

    async with session_factory() as session:
        async with session.begin():
            if not await _table_is_partitioned(session, EVENT_TABLE):
                return 0
            return await _ensure_partitions_in_session(
                session,
                parent_table=EVENT_TABLE,
                start=start,
                end=end,
            )


async def drop_expired_event_partitions(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    before: datetime,
    authority: Any | None = None,
    plan: Any | None = None,
) -> int:
    """Drop complete partitions whose upper bound is outside retention.

    Acquires advisory lock retention_strategy_runtime_events and verifies
    RetentionAuthority epoch fence before dropping each partition.
    """

    cutoff = _as_utc(before)
    if authority is not None:
        if plan is None:
            plan = await authority.plan_prune_async(
                dataset_name=EVENT_TABLE,
                requested_cutoff=cutoff,
            )
        if plan.effective_cutoff < cutoff:
            cutoff = plan.effective_cutoff

    async with session_factory() as session:
        if not await _table_is_partitioned(session, EVENT_TABLE):
            return 0
        rows = (
            await session.execute(
                text(
                    "SELECT child.relname "
                    "FROM pg_inherits AS inheritance "
                    "JOIN pg_class AS parent "
                    "ON parent.oid = inheritance.inhparent "
                    "JOIN pg_class AS child "
                    "ON child.oid = inheritance.inhrelid "
                    "WHERE parent.oid = to_regclass(:table_name) "
                    "AND child.relispartition "
                    "ORDER BY child.relname"
                ),
                {"table_name": EVENT_TABLE},
            )
        ).all()

    expired: list[str] = []
    for row in rows:
        name = cast(str, row[0])
        start = _partition_start_from_name(name)
        if start is not None and start + EVENT_PARTITION_INTERVAL <= cutoff:
            expired.append(name)

    dropped = 0
    scope = resolve_dataset_scope(EVENT_TABLE)
    for name in expired:
        if authority is not None and plan is not None:
            await authority.verify_fence_async(plan)
        async with session_factory() as drop_session:
            try:
                async with drop_session.begin():
                    # Acquire advisory locks in deterministic order to coordinate
                    # with prune, retention, and archive operations
                    for lock_key in scope.advisory_lock_keys:
                        await drop_session.execute(
                            text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
                            {"lock_key": lock_key},
                        )
                    await drop_session.execute(text("SET LOCAL lock_timeout = '2s'"))
                    await drop_session.execute(
                        text(f"DROP TABLE {_quote_identifier(name)}")
                    )
                dropped += 1
            except DBAPIError as error:
                log.warning(
                    "strategy_runtime_event_partition_drop_skipped",
                    partition=name,
                    error=str(error),
                )
    if dropped:
        log.info(
            "strategy_runtime_event_partitions_dropped",
            dropped=dropped,
            cutoff=cutoff.isoformat(),
        )
    return dropped


async def _ensure_partitions_in_session(
    session: AsyncSession,
    *,
    parent_table: str,
    start: datetime,
    end: datetime,
) -> int:
    existing = {
        cast(str, row[0])
        for row in (
            await session.execute(
                text(
                    "SELECT child.relname "
                    "FROM pg_inherits AS inheritance "
                    "JOIN pg_class AS parent "
                    "ON parent.oid = inheritance.inhparent "
                    "JOIN pg_class AS child "
                    "ON child.oid = inheritance.inhrelid "
                    "WHERE parent.oid = to_regclass(:table_name)"
                ),
                {"table_name": parent_table},
            )
        ).all()
    }
    cursor = floor_event_partition_start(start)
    created = 0
    while cursor < end:
        next_boundary = cursor + EVENT_PARTITION_INTERVAL
        name = event_partition_name(cursor)
        if name not in existing:
            await session.execute(
                text(
                    f"CREATE TABLE {_quote_identifier(name)} "
                    f"PARTITION OF {_quote_identifier(parent_table)} "
                    f"FOR VALUES FROM ({_sql_timestamp(cursor)}) "
                    f"TO ({_sql_timestamp(next_boundary)})"
                )
            )
            created += 1
        cursor = next_boundary
    return created


async def _table_is_partitioned(
    session: AsyncSession,
    table_name: str,
) -> bool:
    value = await session.scalar(
        text(
            "SELECT c.relkind = 'p' "
            "FROM pg_class AS c "
            "WHERE c.oid = to_regclass(:table_name)"
        ),
        {"table_name": table_name},
    )
    return bool(value)


def _ceil_event_partition_end(value: datetime) -> datetime:
    start = floor_event_partition_start(value)
    return start + EVENT_PARTITION_INTERVAL


def _partition_start_from_name(name: str) -> datetime | None:
    match = fullmatch(rf"{EVENT_PARTITION_PREFIX}(\d{{8}})", name)
    if match is None:
        return None
    return datetime.strptime(match.group(1), "%Y%m%d").replace(tzinfo=UTC)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _sql_timestamp(value: datetime) -> str:
    return "'" + _as_utc(value).isoformat(sep=" ") + "'::timestamptz"
