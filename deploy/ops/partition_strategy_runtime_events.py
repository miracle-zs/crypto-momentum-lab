#!/usr/bin/env python3
"""One-shot prepare/cutover for strategy_runtime_events daily partitions.

Runs on the server via ``docker exec psql`` so the host does not need the
application package installed.  Mirrors the two-phase pattern used for
runtime_market_states_15s:

  prepare  -- build the partitioned shadow and copy history (writers stay up)
  cutover  -- lock, copy the delta, rename (writers must be paused)

Usage:
    python3 partition_strategy_runtime_events.py --phase prepare
    python3 partition_strategy_runtime_events.py --phase cutover --confirm-writers-paused
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import UTC, datetime, timedelta

_TABLE = "strategy_runtime_events"
_SHADOW = "strategy_runtime_events_partitioned"
_PREFIX = "strategy_runtime_events_p_"
_DEFAULT_CONTAINER = "crypto-momentum-lab-postgres-1"


def _psql(sql: str, *, container: str, database: str, user: str) -> str:
    result = subprocess.run(  # noqa: S603
        [
            "docker", "exec", container,
            "psql", "-U", user, "-d", database, "-At", "-X", "-q", "-c", sql,
        ],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise SystemExit(
            f"psql failed: {result.stderr.decode(errors='replace').strip()}\n"
            f"sql: {sql[:200]}"
        )
    return result.stdout.decode().strip()


def _scalar(sql: str, **kw: str) -> str:
    return _psql(sql, **kw).splitlines()[0].strip()


def _is_partitioned(table: str, **kw: str) -> bool:
    return _scalar(
        "SELECT c.relkind = 'p' FROM pg_class c "
        f"WHERE c.oid = to_regclass('{table}')",
        **kw,
    ) == "t"


def _table_exists(table: str, **kw: str) -> bool:
    return _scalar(
        f"SELECT to_regclass('{table}') IS NOT NULL",
        **kw,
    ) == "t"


def _ensure_partitions(parent: str, start: datetime, end: datetime, **kw: str) -> int:
    existing_raw = _psql(
        "SELECT c.relname FROM pg_inherits i "
        "JOIN pg_class p ON p.oid = i.inhparent "
        "JOIN pg_class c ON c.oid = i.inhrelid "
        f"WHERE p.oid = to_regclass('{parent}')",
        **kw,
    )
    existing = {line.strip() for line in existing_raw.splitlines() if line.strip()}
    cursor = start.replace(hour=0, minute=0, second=0, microsecond=0)
    created = 0
    while cursor < end:
        name = f"{_PREFIX}{cursor:%Y%m%d}"
        nxt = cursor + timedelta(days=1)
        if name not in existing:
            _psql(
                f'CREATE TABLE "{name}" PARTITION OF "{parent}" '
                f"FOR VALUES FROM ('{cursor.isoformat(sep=' ')}'::timestamptz) "
                f"TO ('{nxt.isoformat(sep=' ')}'::timestamptz)",
                **kw,
            )
            created += 1
        cursor = nxt
    return created


def phase_prepare(*, lookahead_days: int, **kw: str) -> None:
    if _is_partitioned(_TABLE, **kw):
        raise SystemExit(f"{_TABLE} is already partitioned")
    if _table_exists(_SHADOW, **kw):
        raise SystemExit(f"shadow already exists: {_SHADOW}")

    summary = _psql(
        f'SELECT count(*)::text || \'|\' || min("occurred_at")::text || \'|\' || '
        f'max("occurred_at")::text FROM {_TABLE}',
        **kw,
    )
    count_s, first_s, last_s = summary.split("|", 2)
    source_rows = int(count_s)
    if source_rows <= 0:
        raise SystemExit("source table is empty")
    first_at = datetime.fromisoformat(first_s)
    last_at = datetime.fromisoformat(last_s)
    now = datetime.now(UTC)
    start = first_at.astimezone(UTC).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end = max(last_at.astimezone(UTC), now) + timedelta(days=lookahead_days)
    end = end.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

    print(f"prepare: source_rows={source_rows} range=[{start.date()} .. {end.date()})")
    _psql(
        f'CREATE TABLE "{_SHADOW}" (LIKE "{_TABLE}" '
        "INCLUDING DEFAULTS INCLUDING CONSTRAINTS) "
        'PARTITION BY RANGE ("occurred_at")',
        **kw,
    )
    created = _ensure_partitions(_SHADOW, start, end, **kw)
    print(f"  created {created} partitions")
    print("  copying history (this can take several minutes)...")
    _psql(f'INSERT INTO "{_SHADOW}" SELECT * FROM "{_TABLE}"', **kw)
    _psql(
        f'ALTER TABLE "{_SHADOW}" ADD CONSTRAINT '
        f'"pk_strategy_runtime_events_partitioned" '
        f'PRIMARY KEY ("event_id", "occurred_at")',
        **kw,
    )
    _psql(
        'CREATE INDEX "ix_strategy_runtime_events_partitioned_run_time" '
        f'ON "{_SHADOW}" ("run_id", "occurred_at")',
        **kw,
    )
    _psql(
        'CREATE INDEX "ix_strategy_runtime_events_partitioned_type_time" '
        f'ON "{_SHADOW}" ("event_type", "occurred_at")',
        **kw,
    )
    _psql(
        'CREATE INDEX "ix_strategy_runtime_events_partitioned_time" '
        f'ON "{_SHADOW}" ("occurred_at")',
        **kw,
    )
    shadow_rows = int(_scalar(f'SELECT count(*) FROM "{_SHADOW}"', **kw))
    if shadow_rows != source_rows:
        raise SystemExit(
            f"shadow row mismatch: source={source_rows} shadow={shadow_rows}"
        )
    print(f"prepare ok: shadow_rows={shadow_rows} partitions={created}")


def phase_cutover(**kw: str) -> None:
    if _is_partitioned(_TABLE, **kw):
        raise SystemExit(f"{_TABLE} is already partitioned")
    if not _is_partitioned(_SHADOW, **kw):
        raise SystemExit(f"shadow is missing or not partitioned: {_SHADOW}")

    legacy = f"{_TABLE}_legacy_{datetime.now(UTC):%Y%m%d%H%M%S}"
    if _table_exists(legacy, **kw):
        raise SystemExit(f"legacy name already used: {legacy}")

    print("cutover: locking source and copying final delta...")
    # One session so the ACCESS EXCLUSIVE lock is held across copy+rename.
    script = f"""
BEGIN;
SET LOCAL statement_timeout = '0';
SET LOCAL lock_timeout = '15s';
LOCK TABLE "{_TABLE}" IN ACCESS EXCLUSIVE MODE;
INSERT INTO "{_SHADOW}" SELECT * FROM "{_TABLE}" ON CONFLICT DO NOTHING;
ALTER TABLE "{_TABLE}" RENAME TO "{legacy}";
ALTER TABLE "{_SHADOW}" RENAME TO "{_TABLE}";
COMMIT;
ANALYZE "{_TABLE}";
"""
    result = subprocess.run(  # noqa: S603
        [
            "docker", "exec", "-i", kw["container"],
            "psql", "-U", kw["user"], "-d", kw["database"],
            "-v", "ON_ERROR_STOP=1", "-X", "-q",
        ],
        input=script.encode(),
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise SystemExit(
            f"cutover failed: {result.stderr.decode(errors='replace').strip()}"
        )
    rows = _scalar(f'SELECT count(*) FROM "{_TABLE}"', **kw)
    kind = _scalar(
        "SELECT c.relkind FROM pg_class c WHERE c.oid = to_regclass('" + _TABLE + "')",
        **kw,
    )
    print(f"cutover ok: rows={rows} relkind={kind} legacy={legacy}")
    print("verify writers, then DROP the legacy table when safe.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("prepare", "cutover"), required=True)
    parser.add_argument("--lookahead-days", type=int, default=2)
    parser.add_argument(
        "--confirm-writers-paused",
        action="store_true",
        help="Required for cutover; live-strategy must already be stopped.",
    )
    parser.add_argument("--container", default=_DEFAULT_CONTAINER)
    parser.add_argument("--database", default="cml")
    parser.add_argument("--user", default="cml")
    args = parser.parse_args(argv)

    if args.lookahead_days <= 0:
        raise SystemExit("--lookahead-days must be positive")
    if args.phase == "cutover" and not args.confirm_writers_paused:
        raise SystemExit("--confirm-writers-paused is required for cutover")

    kw = {
        "container": args.container,
        "database": args.database,
        "user": args.user,
    }
    if args.phase == "prepare":
        phase_prepare(lookahead_days=args.lookahead_days, **kw)
    else:
        phase_cutover(**kw)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
