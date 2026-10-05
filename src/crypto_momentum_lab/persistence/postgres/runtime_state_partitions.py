"""Partition maintenance for runtime market states."""

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

RUNTIME_STATE_TABLE: Final = "runtime_market_states_15s"
RUNTIME_STATE_PARTITION_PREFIX: Final = "runtime_market_states_15s_p_"
RUNTIME_STATE_PARTITION_INTERVAL: Final = timedelta(hours=6)
RUNTIME_STATE_PARTITION_LOOKAHEAD: Final = timedelta(days=2)

log = structlog.get_logger()


def floor_runtime_state_partition_start(value: datetime) -> datetime:
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


async def ensure_runtime_state_partitions(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    through: datetime,
    from_at: datetime | None = None,
) -> int:
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
            return await _ensure_partitions_in_session(session, start=start, end=end)


async def drop_expired_runtime_state_partitions(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    before: datetime,
    authority: Any | None = None,
    plan: Any | None = None,
) -> int:
    cutoff = _as_utc(before)
    if authority is not None:
        if plan is None:
            plan = await authority.plan_prune_async(
                dataset_name=RUNTIME_STATE_TABLE,
                requested_cutoff=cutoff,
            )
        cutoff = min(cutoff, plan.effective_cutoff)

    async with session_factory() as session:
        rows = (
            await session.execute(
                text(
                    "SELECT child.relname "
                    "FROM pg_inherits AS inheritance "
                    "JOIN pg_class AS parent ON parent.oid = inheritance.inhparent "
                    "JOIN pg_class AS child ON child.oid = inheritance.inhrelid "
                    "WHERE parent.oid = to_regclass(:table_name) "
                    "AND child.relispartition ORDER BY child.relname"
                ),
                {"table_name": RUNTIME_STATE_TABLE},
            )
        ).all()
    expired = [
        cast(str, row[0])
        for row in rows
        if (start := _partition_start_from_name(cast(str, row[0]))) is not None
        and start + RUNTIME_STATE_PARTITION_INTERVAL <= cutoff
    ]
    scope = resolve_dataset_scope(RUNTIME_STATE_TABLE)
    dropped = 0
    for name in expired:
        if authority is not None and plan is not None:
            await authority.verify_fence_async(plan)
        try:
            async with session_factory() as session:
                async with session.begin():
                    for key in scope.advisory_lock_keys:
                        await session.execute(
                            text("SELECT pg_advisory_xact_lock(hashtext(:key))"),
                            {"key": key},
                        )
                    await session.execute(text("SET LOCAL lock_timeout = '2s'"))
                    await session.execute(text(f"DROP TABLE {_quote_identifier(name)}"))
            dropped += 1
        except DBAPIError as error:
            log.warning(
                "runtime_state_partition_drop_skipped",
                partition=name,
                error=str(error),
            )
    return dropped


async def _ensure_partitions_in_session(
    session: AsyncSession,
    *,
    start: datetime,
    end: datetime,
) -> int:
    existing = {
        cast(str, row[0])
        for row in (
            await session.execute(
                text(
                    "SELECT child.relname FROM pg_inherits AS inheritance "
                    "JOIN pg_class AS parent ON parent.oid = inheritance.inhparent "
                    "JOIN pg_class AS child ON child.oid = inheritance.inhrelid "
                    "WHERE parent.oid = to_regclass(:table_name)"
                ),
                {"table_name": RUNTIME_STATE_TABLE},
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
                    f"PARTITION OF {_quote_identifier(RUNTIME_STATE_TABLE)} "
                    f"FOR VALUES FROM ({_sql_timestamp(cursor)}) "
                    f"TO ({_sql_timestamp(next_boundary)})"
                )
            )
            created += 1
        cursor = next_boundary
    return created


def _ceil_runtime_state_partition_end(value: datetime) -> datetime:
    return floor_runtime_state_partition_start(value) + RUNTIME_STATE_PARTITION_INTERVAL


def _partition_start_from_name(name: str) -> datetime | None:
    match = fullmatch(r"runtime_market_states_15s_p_(\d{8})_(\d{4})", name)
    if match is None:
        return None
    return datetime.strptime("".join(match.groups()), "%Y%m%d%H%M").replace(tzinfo=UTC)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _sql_timestamp(value: datetime) -> str:
    return "'" + _as_utc(value).isoformat() + "'"
