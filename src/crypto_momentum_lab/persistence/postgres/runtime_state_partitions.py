"""Online partition management for the hot runtime market-state table.

The runtime-state table is operational data that can be rebuilt from the
capture archive.  Keep the expensive table rewrite behind an explicit,
two-phase operator command.  Once the cutover is complete, retention can
remove whole six-hour partitions instead of issuing millions of row deletes.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from re import fullmatch
from typing import Any, Final, cast

import structlog
from sqlalchemy import text
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

RUNTIME_STATE_TABLE: Final = "runtime_market_states_15s"
RUNTIME_STATE_SHADOW_TABLE: Final = "runtime_market_states_15s_partitioned"
RUNTIME_STATE_PARTITION_PREFIX: Final = "runtime_market_states_15s_p_"
RUNTIME_STATE_PARTITION_INTERVAL: Final = timedelta(hours=6)
RUNTIME_STATE_PARTITION_LOOKAHEAD: Final = timedelta(days=7)

_RUNTIME_STATE_PRIMARY_KEY: Final = (
    "pk_runtime_market_states_15s_partitioned"
)
_RUNTIME_STATE_INDEXES: Final[tuple[tuple[str, str], ...]] = (
    (
        "ix_runtime_market_states_15s_partitioned_polling",
        '"environment", "bucket_start", "symbol"',
    ),
    (
        "ix_runtime_market_states_15s_partitioned_created",
        '"environment", "created_at"',
    ),
    (
        "ix_runtime_market_states_15s_partitioned_latest_bucket",
        '"bucket_start" INCLUDE ("bucket_end")',
    ),
)

log = structlog.get_logger()


@dataclass(frozen=True, slots=True)
class RuntimeStatePartitionPrepareReport:
    source_rows: int
    shadow_rows: int
    partitions_created: int
    first_partition_start: datetime
    last_partition_end: datetime


@dataclass(frozen=True, slots=True)
class RuntimeStatePartitionCutoverReport:
    rows_copied_during_cutover: int
    source_rows: int
    shadow_rows: int
    legacy_table: str


def floor_runtime_state_partition_start(value: datetime) -> datetime:
    """Return the UTC six-hour boundary containing ``value``."""

    normalized = _as_utc(value)
    return normalized.replace(
        hour=normalized.hour - normalized.hour % 6,
        minute=0,
        second=0,
        microsecond=0,
    )


def runtime_state_partition_name(start: datetime) -> str:
    return (
        f"{RUNTIME_STATE_PARTITION_PREFIX}"
        f"{floor_runtime_state_partition_start(start):%Y%m%d_%H%M}"
    )


async def runtime_state_table_is_partitioned(
    session_factory: async_sessionmaker[AsyncSession],
) -> bool:
    async with session_factory() as session:
        value = await session.scalar(
            text(
                "SELECT c.relkind = 'p' "
                "FROM pg_class AS c "
                "WHERE c.oid = to_regclass(:table_name)"
            ),
            {"table_name": RUNTIME_STATE_TABLE},
        )
    return bool(value)


async def ensure_runtime_state_partitions(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    through: datetime,
    from_at: datetime | None = None,
) -> int:
    """Create missing six-hour partitions up to ``through``.

    This is a no-op while the relation is still the legacy unpartitioned
    table.  Identifiers are generated only from fixed prefixes and UTC
    timestamps, so the DDL does not accept arbitrary SQL identifiers.
    """

    end = _ceil_runtime_state_partition_end(through)
    start = floor_runtime_state_partition_start(
        datetime.now(UTC) - RUNTIME_STATE_PARTITION_INTERVAL
        if from_at is None
        else from_at
    )
    if end <= start:
        return 0

    async with session_factory() as session:
        async with session.begin():
            if not await _table_is_partitioned(session, RUNTIME_STATE_TABLE):
                return 0
            return await _ensure_partitions_in_session(
                session,
                parent_table=RUNTIME_STATE_TABLE,
                start=start,
                end=end,
            )


async def drop_expired_runtime_state_partitions(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    before: datetime,
) -> int:
    """Drop complete partitions whose upper bound is outside retention."""

    cutoff = _as_utc(before)
    async with session_factory() as session:
        if not await _table_is_partitioned(session, RUNTIME_STATE_TABLE):
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
                {"table_name": RUNTIME_STATE_TABLE},
            )
        ).all()

    expired: list[str] = []
    for row in rows:
        name = cast(str, row[0])
        start = _partition_start_from_name(name)
        if start is not None and start + RUNTIME_STATE_PARTITION_INTERVAL <= cutoff:
            expired.append(name)

    dropped = 0
    for name in expired:
        async with session_factory() as drop_session:
            try:
                async with drop_session.begin():
                    # A stuck reader must not turn retention into a database
                    # outage.  The next interval retries the partition.
                    await drop_session.execute(
                        text("SET LOCAL lock_timeout = '2s'")
                    )
                    await drop_session.execute(
                        text(f"DROP TABLE {_quote_identifier(name)}")
                    )
                dropped += 1
            except DBAPIError as error:
                log.warning(
                    "runtime_state_partition_drop_skipped",
                    partition=name,
                    error=str(error),
                )
    if dropped:
        log.info(
            "runtime_state_partitions_dropped",
            dropped=dropped,
            cutoff=cutoff.isoformat(),
        )
    return dropped


async def prepare_runtime_state_partition(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    now: datetime | None = None,
    lookahead: timedelta = RUNTIME_STATE_PARTITION_LOOKAHEAD,
) -> RuntimeStatePartitionPrepareReport:
    """Build and populate the partitioned shadow table.

    The source table remains writable during this phase.  The cutover phase
    performs a final copy after taking an access-exclusive lock.
    """

    if lookahead <= timedelta(0):
        raise ValueError("lookahead must be positive")
    observed_at = datetime.now(UTC) if now is None else _as_utc(now)

    async with session_factory() as session:
        async with session.begin():
            if await _table_is_partitioned(session, RUNTIME_STATE_TABLE):
                raise RuntimeError(
                    f"{RUNTIME_STATE_TABLE} is already partitioned"
                )
            if await _table_exists(session, RUNTIME_STATE_SHADOW_TABLE):
                raise RuntimeError(
                    f"shadow table already exists: {RUNTIME_STATE_SHADOW_TABLE}"
                )

            source = _quote_identifier(RUNTIME_STATE_TABLE)
            shadow = _quote_identifier(RUNTIME_STATE_SHADOW_TABLE)
            summary = (
                await session.execute(
                    text(
                        f"SELECT count(*)::bigint AS row_count, "
                        f"min(\"bucket_start\") AS first_bucket, "
                        f"max(\"bucket_start\") AS last_bucket "
                        f"FROM {source}"
                    )
                )
            ).one()._mapping
            source_rows = int(summary["row_count"])
            first_bucket = summary["first_bucket"]
            last_bucket = summary["last_bucket"]
            if source_rows <= 0 or not isinstance(first_bucket, datetime):
                raise RuntimeError("runtime market-state source table is empty")
            if not isinstance(last_bucket, datetime):
                raise RuntimeError("runtime market-state source has no max bucket")

            await session.execute(
                text(
                    f"CREATE TABLE {shadow} "
                    f"(LIKE {source} INCLUDING DEFAULTS INCLUDING CONSTRAINTS) "
                    f"PARTITION BY RANGE (\"bucket_start\")"
                )
            )
            first_partition_start = floor_runtime_state_partition_start(
                first_bucket
            )
            last_partition_end = _ceil_runtime_state_partition_end(
                max(last_bucket, observed_at + lookahead)
            )
            partitions_created = await _ensure_partitions_in_session(
                session,
                parent_table=RUNTIME_STATE_SHADOW_TABLE,
                start=first_partition_start,
                end=last_partition_end,
            )
            await session.execute(
                text(
                    f"INSERT INTO {shadow} SELECT * FROM {source}"
                )
            )
            await _create_shadow_indexes(session)
            shadow_rows = int(
                await session.scalar(
                    text(f"SELECT count(*)::bigint FROM {shadow}")
                )
                or 0
            )
            if shadow_rows != source_rows:
                raise RuntimeError(
                    "shadow row count differs after prepare: "
                    f"source={source_rows} shadow={shadow_rows}"
                )

    return RuntimeStatePartitionPrepareReport(
        source_rows=source_rows,
        shadow_rows=shadow_rows,
        partitions_created=partitions_created,
        first_partition_start=first_partition_start,
        last_partition_end=last_partition_end,
    )


async def cutover_runtime_state_partition(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    legacy_table: str | None = None,
) -> RuntimeStatePartitionCutoverReport:
    """Copy the final delta and atomically rename the shadow into place."""

    resolved_legacy_table = legacy_table or (
        f"{RUNTIME_STATE_TABLE}_legacy_"
        f"{datetime.now(UTC):%Y%m%d%H%M%S}"
    )
    _validate_legacy_table_name(resolved_legacy_table)

    async with session_factory() as session:
        async with session.begin():
            if await _table_is_partitioned(session, RUNTIME_STATE_TABLE):
                raise RuntimeError(
                    f"{RUNTIME_STATE_TABLE} is already partitioned"
                )
            if not await _table_is_partitioned(
                session,
                RUNTIME_STATE_SHADOW_TABLE,
            ):
                raise RuntimeError(
                    "partitioned shadow table is missing: "
                    f"{RUNTIME_STATE_SHADOW_TABLE}"
                )
            if await _table_exists(session, resolved_legacy_table):
                raise RuntimeError(
                    f"legacy table already exists: {resolved_legacy_table}"
                )

            await session.execute(text("SET LOCAL statement_timeout = '0'"))
            await session.execute(text("SET LOCAL lock_timeout = '15s'"))
            # The operator must stop market-data before invoking this phase.
            # Taking the lock before the final copy makes that precondition
            # enforceable even if a second writer was missed.
            await session.execute(
                text(
                    f"LOCK TABLE {_quote_identifier(RUNTIME_STATE_TABLE)} "
                    "IN ACCESS EXCLUSIVE MODE"
                )
            )
            source = _quote_identifier(RUNTIME_STATE_TABLE)
            shadow = _quote_identifier(RUNTIME_STATE_SHADOW_TABLE)
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
            missing_rows = int(
                await session.scalar(
                    text(
                        f"SELECT count(*)::bigint FROM {source} AS source_row "
                        f"WHERE NOT EXISTS ("
                        f"SELECT 1 FROM {shadow} AS shadow_row "
                        "WHERE shadow_row.\"environment\" = "
                        "source_row.\"environment\" "
                        "AND shadow_row.\"symbol\" = source_row.\"symbol\" "
                        "AND shadow_row.\"bucket_start\" = "
                        "source_row.\"bucket_start\""
                        f")"
                    )
                )
                or 0
            )
            if source_rows != shadow_rows or missing_rows:
                raise RuntimeError(
                    "runtime-state cutover validation failed: "
                    f"source={source_rows} shadow={shadow_rows} "
                    f"missing={missing_rows}"
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
                    f"RENAME TO {_quote_identifier(RUNTIME_STATE_TABLE)}"
                )
            )

    async with session_factory() as analyze_session:
        await analyze_session.execute(
            text(f"ANALYZE {_quote_identifier(RUNTIME_STATE_TABLE)}")
        )
        await analyze_session.commit()

    return RuntimeStatePartitionCutoverReport(
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
    cursor = floor_runtime_state_partition_start(start)
    created = 0
    while cursor < end:
        next_boundary = cursor + RUNTIME_STATE_PARTITION_INTERVAL
        name = runtime_state_partition_name(cursor)
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
    shadow = _quote_identifier(RUNTIME_STATE_SHADOW_TABLE)
    await session.execute(
        text(
            f"ALTER TABLE {shadow} ADD CONSTRAINT "
            f"{_quote_identifier(_RUNTIME_STATE_PRIMARY_KEY)} PRIMARY KEY "
            f"(\"environment\", \"symbol\", \"bucket_start\")"
        )
    )
    for index_name, columns in _RUNTIME_STATE_INDEXES:
        await session.execute(
            text(
                f"CREATE INDEX {_quote_identifier(index_name)} "
                f"ON {shadow} ({columns})"
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


def _ceil_runtime_state_partition_end(value: datetime) -> datetime:
    start = floor_runtime_state_partition_start(value)
    return start + RUNTIME_STATE_PARTITION_INTERVAL


def _partition_start_from_name(name: str) -> datetime | None:
    match = fullmatch(
        rf"{RUNTIME_STATE_PARTITION_PREFIX}(\d{{8}}_\d{{4}})",
        name,
    )
    if match is None:
        return None
    return datetime.strptime(match.group(1), "%Y%m%d_%H%M").replace(tzinfo=UTC)


def _validate_legacy_table_name(name: str) -> None:
    if fullmatch(r"runtime_market_states_15s_legacy_[0-9]{14}", name) is None:
        raise ValueError("legacy_table must use the generated runtime-state name")


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _sql_timestamp(value: datetime) -> str:
    return "'" + _as_utc(value).isoformat(sep=" ") + "'::timestamptz"
