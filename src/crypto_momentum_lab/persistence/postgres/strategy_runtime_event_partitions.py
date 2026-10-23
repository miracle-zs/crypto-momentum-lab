"""Online partition management for the strategy runtime-event table.

``strategy_runtime_events`` is the largest PostgreSQL table on the live host
and is append-only operational history.  Once partitioned by day, retention
drops whole partitions instead of deleting hundreds of thousands of rows and
leaving dead tuples behind for VACUUM.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Final, cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

EVENT_TABLE: Final = "strategy_runtime_events"
EVENT_PARTITION_PREFIX: Final = "strategy_runtime_events_p_"
EVENT_PARTITION_INTERVAL: Final = timedelta(days=1)
# Two days of empty partitions covers a weekend host reboot without leaving
# "now" uncovered, while keeping the planner object count small.
EVENT_PARTITION_LOOKAHEAD: Final = timedelta(days=2)


def floor_event_partition_start(value: datetime) -> datetime:
    """Return the UTC day boundary containing ``value``."""

    normalized = _as_utc(value)
    return normalized.replace(hour=0, minute=0, second=0, microsecond=0)


def event_partition_name(start: datetime) -> str:
    return f"{EVENT_PARTITION_PREFIX}{floor_event_partition_start(start):%Y%m%d}"


async def ensure_event_partitions(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    through: datetime,
    from_at: datetime | None = None,
) -> int:
    """Create missing daily partitions up to ``through``."""

    end = _ceil_event_partition_end(through)
    start = floor_event_partition_start(
        datetime.now(UTC) - EVENT_PARTITION_INTERVAL if from_at is None else from_at
    )
    if end <= start:
        return 0

    async with session_factory() as session:
        async with session.begin():
            return await _ensure_partitions_in_session(
                session,
                parent_table=EVENT_TABLE,
                start=start,
                end=end,
            )


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


def _ceil_event_partition_end(value: datetime) -> datetime:
    start = floor_event_partition_start(value)
    return start + EVENT_PARTITION_INTERVAL


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _sql_timestamp(value: datetime) -> str:
    return "'" + _as_utc(value).isoformat(sep=" ") + "'::timestamptz"
