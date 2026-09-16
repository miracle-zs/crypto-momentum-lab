"""Online partition management for the strategy runtime-event table.

``strategy_runtime_events`` is the largest PostgreSQL table on the live host
and is append-only operational history.  Once partitioned by day, retention
drops whole partitions instead of deleting hundreds of thousands of rows and
leaving dead tuples behind for VACUUM.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from re import fullmatch
from typing import Any, Final, cast

import structlog
from sqlalchemy import text
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

EVENT_TABLE: Final = "strategy_runtime_events"
EVENT_SHADOW_TABLE: Final = "strategy_runtime_events_partitioned"
EVENT_PARTITION_PREFIX: Final = "strategy_runtime_events_p_"
EVENT_PARTITION_INTERVAL: Final = timedelta(days=1)
# Two days of empty partitions covers a weekend host reboot without leaving
# "now" uncovered, while keeping the planner object count small.
EVENT_PARTITION_LOOKAHEAD: Final = timedelta(days=2)

_EVENT_PRIMARY_KEY: Final = "pk_strategy_runtime_events_partitioned"
_EVENT_INDEXES: Final[tuple[tuple[str, str], ...]] = (
    (
        "ix_strategy_runtime_events_partitioned_run_time",
        '("run_id", "occurred_at")',
    ),
    (
        "ix_strategy_runtime_events_partitioned_type_time",
        '("event_type", "occurred_at")',
    ),
    (
        "ix_strategy_runtime_events_partitioned_time",
        '("occurred_at")',
    ),
)

log = structlog.get_logger()


@dataclass(frozen=True, slots=True)
class EventPartitionPrepareReport:
    source_rows: int
    shadow_rows: int
    partitions_created: int
    first_partition_start: datetime
    last_partition_end: datetime


@dataclass(frozen=True, slots=True)
class EventPartitionCutoverReport:
    rows_copied_during_cutover: int
    source_rows: int
    shadow_rows: int
    legacy_table: str


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
        datetime.now(UTC) - EVENT_PARTITION_INTERVAL
        if from_at is None
        else from_at
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
) -> int:
    """Drop complete partitions whose upper bound is outside retention."""

    cutoff = _as_utc(before)
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
    for name in expired:
        async with session_factory() as drop_session:
            try:
                async with drop_session.begin():
                    await drop_session.execute(
                        text("SET LOCAL lock_timeout = '2s'")
                    )
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


async def prepare_event_partition(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    now: datetime | None = None,
    lookahead: timedelta = EVENT_PARTITION_LOOKAHEAD,
) -> EventPartitionPrepareReport:
    """Build and populate the partitioned shadow table.

    The source table remains writable.  Cutover performs a final copy after
    taking an access-exclusive lock.
    """

    if lookahead <= timedelta(0):
        raise ValueError("lookahead must be positive")
    occurred_at = datetime.now(UTC) if now is None else _as_utc(now)

    async with session_factory() as session:
        async with session.begin():
            if await _table_is_partitioned(session, EVENT_TABLE):
                raise RuntimeError(f"{EVENT_TABLE} is already partitioned")
            if await _table_exists(session, EVENT_SHADOW_TABLE):
                raise RuntimeError(
                    f"shadow table already exists: {EVENT_SHADOW_TABLE}"
                )

            source = _quote_identifier(EVENT_TABLE)
            shadow = _quote_identifier(EVENT_SHADOW_TABLE)
            summary = (
                await session.execute(
                    text(
                        f"SELECT count(*)::bigint AS row_count, "
                        f"min(\"occurred_at\") AS first_at, "
                        f"max(\"occurred_at\") AS last_at "
                        f"FROM {source}"
                    )
                )
            ).one()._mapping
            source_rows = int(summary["row_count"])
            first_at = summary["first_at"]
            last_at = summary["last_at"]
            if source_rows <= 0 or not isinstance(first_at, datetime):
                raise RuntimeError("strategy runtime-event source table is empty")
            if not isinstance(last_at, datetime):
                raise RuntimeError("strategy runtime-event source has no max time")

            await session.execute(
                text(
                    f"CREATE TABLE {shadow} "
                    f"(LIKE {source} INCLUDING DEFAULTS INCLUDING CONSTRAINTS) "
                    f"PARTITION BY RANGE (\"occurred_at\")"
                )
            )
            first_partition_start = floor_event_partition_start(first_at)
            last_partition_end = _ceil_event_partition_end(
                max(last_at, occurred_at + lookahead)
            )
            partitions_created = await _ensure_partitions_in_session(
                session,
                parent_table=EVENT_SHADOW_TABLE,
                start=first_partition_start,
                end=last_partition_end,
            )
            await session.execute(text(f"INSERT INTO {shadow} SELECT * FROM {source}"))
            await _create_shadow_indexes(session)
            shadow_rows = int(
                await session.scalar(text(f"SELECT count(*)::bigint FROM {shadow}"))
                or 0
            )
            if shadow_rows != source_rows:
                raise RuntimeError(
                    "shadow row count differs after prepare: "
                    f"source={source_rows} shadow={shadow_rows}"
                )

    return EventPartitionPrepareReport(
        source_rows=source_rows,
        shadow_rows=shadow_rows,
        partitions_created=partitions_created,
        first_partition_start=first_partition_start,
        last_partition_end=last_partition_end,
    )


async def cutover_event_partition(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    legacy_table: str | None = None,
) -> EventPartitionCutoverReport:
    """Copy the final delta and atomically rename the shadow into place."""

    resolved_legacy_table = legacy_table or (
        f"{EVENT_TABLE}_legacy_{datetime.now(UTC):%Y%m%d%H%M%S}"
    )
    _validate_legacy_table_name(resolved_legacy_table)

    async with session_factory() as session:
        async with session.begin():
            if await _table_is_partitioned(session, EVENT_TABLE):
                raise RuntimeError(f"{EVENT_TABLE} is already partitioned")
            if not await _table_is_partitioned(session, EVENT_SHADOW_TABLE):
                raise RuntimeError(
                    "partitioned shadow table is missing: "
                    f"{EVENT_SHADOW_TABLE}"
                )
            if await _table_exists(session, resolved_legacy_table):
                raise RuntimeError(
                    f"legacy table already exists: {resolved_legacy_table}"
                )

            await session.execute(text("SET LOCAL statement_timeout = '0'"))
            await session.execute(text("SET LOCAL lock_timeout = '15s'"))
            await session.execute(
                text(
                    f"LOCK TABLE {_quote_identifier(EVENT_TABLE)} "
                    "IN ACCESS EXCLUSIVE MODE"
                )
            )
            source = _quote_identifier(EVENT_TABLE)
            shadow = _quote_identifier(EVENT_SHADOW_TABLE)
            inserted = cast(
                CursorResult[Any],
                await session.execute(
                    text(
                        f"INSERT INTO {shadow} SELECT * FROM {source} "
                        "ON CONFLICT DO NOTHING"
                    )
                ),
            )
            copied = max(inserted.rowcount or 0, 0)
            source_rows = int(
                await session.scalar(text(f"SELECT count(*)::bigint FROM {source}"))
                or 0
            )
            shadow_rows = int(
                await session.scalar(text(f"SELECT count(*)::bigint FROM {shadow}"))
                or 0
            )
            if shadow_rows < source_rows:
                raise RuntimeError(
                    "event cutover validation failed: "
                    f"source={source_rows} shadow={shadow_rows}"
                )

            await session.execute(
                text(
                    f"ALTER TABLE {source} "
                    f"RENAME TO {_quote_identifier(resolved_legacy_table)}"
                )
            )
            await session.execute(
                text(
                    f"ALTER TABLE {shadow} "
                    f"RENAME TO {_quote_identifier(EVENT_TABLE)}"
                )
            )

    async with session_factory() as analyze_session:
        await analyze_session.execute(
            text(f"ANALYZE {_quote_identifier(EVENT_TABLE)}")
        )
        await analyze_session.commit()

    return EventPartitionCutoverReport(
        rows_copied_during_cutover=copied,
        source_rows=source_rows,
        shadow_rows=shadow_rows,
        legacy_table=resolved_legacy_table,
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


async def _create_shadow_indexes(session: AsyncSession) -> None:
    shadow = _quote_identifier(EVENT_SHADOW_TABLE)
    await session.execute(
        text(
            f"ALTER TABLE {shadow} ADD CONSTRAINT "
            f"{_quote_identifier(_EVENT_PRIMARY_KEY)} PRIMARY KEY "
            f"(\"event_id\", \"occurred_at\")"
        )
    )
    for index_name, columns in _EVENT_INDEXES:
        await session.execute(
            text(
                f"CREATE INDEX {_quote_identifier(index_name)} "
                f"ON {shadow} {columns}"
            )
        )


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


async def _table_exists(session: AsyncSession, table_name: str) -> bool:
    value = await session.scalar(
        text("SELECT to_regclass(:table_name) IS NOT NULL"),
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


def _validate_legacy_table_name(name: str) -> None:
    if fullmatch(r"strategy_runtime_events_legacy_[0-9]{14}", name) is None:
        raise ValueError("legacy_table must use the generated event name")


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _sql_timestamp(value: datetime) -> str:
    return "'" + _as_utc(value).isoformat(sep=" ") + "'::timestamptz"
