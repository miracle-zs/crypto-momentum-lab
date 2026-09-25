#!/usr/bin/env python3
"""Archive then trim rows older than a retention window.

The tables below live only in PostgreSQL -- unlike the market-data streams they
have no parquet copy behind them -- so deleting a row before archiving it loses
it for good.  This runs ``archive_table.py`` over exactly the range about to be
dropped, checks the row count it recorded, and only then deletes, in small
batches so no long transaction holds a lock the trading path needs.

It is meant to run *on the server* (it shells out to ``docker exec`` locally),
so the SQL never has to survive an ssh + shell quoting round trip.

Usage:
    python3 archive_and_trim.py --retention-days 7 [--dry-run] [--table NAME]
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_ARCHIVER = _HERE / "archive_table.py"
_ARCHIVE_ROOT = Path("/var/lib/crypto-momentum-lab/table-archive")
_DEFAULT_CONTAINER = "crypto-momentum-lab-postgres-1"

# table, time column -- the window is half-open [oldest, cutoff).
# Order matters only for readability; each table is independent.  Deleting
# universe_snapshots cascades to monitoring_memberships and leftover
# universe_entries, so those child tables must be archived first when they
# need their own copy (universe_entries already is).
TABLES: tuple[tuple[str, str], ...] = (
    ("strategy_runtime_events", "occurred_at"),
    ("universe_entries", "price_time"),
    ("account_balance_snapshots", "observed_at"),
    ("account_position_snapshots", "observed_at"),
    ("exchange_order_events", "occurred_at"),
    ("paper_equity_snapshots", "observed_at"),
    # The dashboard only reads the newest row per account, and nothing else
    # queries this table's history -- yet it had no retention, so it had grown
    # to 17 days / 144 MB (89 MB of that is TOAST holding six jsonb columns,
    # so archiving it stays on the JSONL path).
    ("live_strategy_signals", "recorded_at"),
    # State-machine transitions for execution-account daemons.  Only the
    # latest row per account is load-bearing; history is audit and had no
    # retention (~12k rows/day, 24 MB and growing).
    ("execution_account_process_states", "occurred_at"),
    # Market-data quality diagnostics (jsonb -> JSONL archive).  Volume
    # spikes with connection churn; without a window this table grows
    # without bound.
    ("market_data_quality_events", "occurred_at"),
    # Parent of monitoring_memberships + universe_entries (both ON DELETE
    # CASCADE).  Entries are archived above; deleting an old snapshot then
    # drops its membership rows in one statement.  11k snapshots / 1M
    # membership rows had accumulated from 2026-07-25 with no TTL.
    ("universe_snapshots", "observed_at"),
)


def _psql(sql: str, *, container: str, database: str, user: str) -> str:
    result = subprocess.run(  # noqa: S603 - argv built here, never a shell
        [
            "docker",
            "exec",
            container,
            "psql",
            "-U",
            user,
            "-d",
            database,
            "-At",
            "-X",
            "-q",
            "-c",
            sql,
        ],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise SystemExit(
            f"psql failed: {result.stderr.decode(errors='replace').strip()}"
        )
    return result.stdout.decode().strip()


def _scalar(sql: str, **kw: str) -> str:
    lines = _psql(sql, **kw).splitlines()
    return lines[0].strip() if lines else ""


def _table_is_partitioned(table: str, **kw: str) -> bool:
    value = _scalar(
        f"SELECT c.relkind = 'p' FROM pg_class c WHERE c.oid = to_regclass('{table}')",
        **kw,
    )
    return value == "t"


def _resolve_consumer_watermark(table: str, **db: str) -> datetime | None:
    """Resolve earliest recovery watermark required across active consumers."""
    has_dep_table = _scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
        "WHERE table_name = 'consumer_dependencies')",
        **db,
    )
    if has_dep_table == "t":
        earliest_dep = _scalar(
            "SELECT min(recovery_watermark)::text FROM consumer_dependencies "
            f"WHERE dataset_name = '{table}'",
            **db,
        )
        if earliest_dep:
            try:
                return datetime.fromisoformat(earliest_dep)
            except Exception:
                pass

    if table in ("account_position_snapshots", "account_balance_snapshots"):
        earliest_pos = _scalar(
            "SELECT min(observed_at)::text FROM account_position_snapshots "
            "WHERE abs(position_amt) > 0",
            **db,
        )
        if earliest_pos:
            try:
                return datetime.fromisoformat(earliest_pos)
            except Exception:
                pass

    return None


def _drop_expired_partitions(table: str, cutoff: date, **kw: str) -> int:
    """Drop day partitions whose upper bound is on or before ``cutoff``.

    Only the strategy-runtime-event table uses this path today.  Identifiers
    come from the fixed prefix plus a YYYYMMDD token parsed out of the catalog.
    """

    rows = _psql(
        "SELECT child.relname FROM pg_inherits i "
        "JOIN pg_class parent ON parent.oid = i.inhparent "
        "JOIN pg_class child ON child.oid = i.inhrelid "
        "WHERE parent.oid = to_regclass('" + table + "') "
        "AND child.relispartition ORDER BY child.relname",
        **kw,
    )
    dropped = 0
    for name in rows.splitlines():
        name = name.strip()
        if not name.startswith("strategy_runtime_events_p_"):
            continue
        day_token = name.removeprefix("strategy_runtime_events_p_")
        if len(day_token) != 8 or not day_token.isdigit():
            continue
        partition_day = (
            datetime.strptime(day_token, "%Y%m%d").replace(tzinfo=UTC).date()
        )
        # A partition covers [day, day+1); it is fully outside the window
        # once day+1 <= cutoff, i.e. day < cutoff.
        if partition_day < cutoff:
            _psql(f'DROP TABLE IF EXISTS "{name}"', **kw)
            print(f"  dropped partition {name}")
            dropped += 1
    return dropped


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--retention-days", type=int, default=7)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--table", default=None, help="limit to one table")
    parser.add_argument("--container", default=_DEFAULT_CONTAINER)
    parser.add_argument("--database", default="cml")
    parser.add_argument("--user", default="cml")
    parser.add_argument("--batch-rows", type=int, default=5000)
    args = parser.parse_args(argv)

    if args.retention_days <= 0:
        raise SystemExit("--retention-days must be positive")

    db = {"container": args.container, "database": args.database, "user": args.user}
    cutoff = (datetime.now(tz=UTC) - timedelta(days=args.retention_days)).date()
    print(
        f"retention: {args.retention_days}d   cutoff: {cutoff}   "
        f"dry-run: {args.dry_run}\n"
    )

    for table, column in TABLES:
        if args.table is not None and table != args.table:
            continue

        oldest_raw = _scalar(
            f"SELECT coalesce(min(\"{column}\")::date::text, '') FROM {table}", **db
        )
        if not oldest_raw:
            print(f"{table}: empty, skipping")
            continue
        oldest = datetime.fromisoformat(oldest_raw).date()
        min_watermark = _resolve_consumer_watermark(table, **db)
        effective_cutoff = cutoff
        if min_watermark is not None:
            min_watermark_date = min_watermark.date()
            if min_watermark_date < cutoff:
                print(
                    f"  [CONSTRAINED] {table} cutoff {cutoff} pulled back to "
                    f"{min_watermark_date} by active consumer/position dependency"
                )
                effective_cutoff = min_watermark_date

        if oldest >= effective_cutoff:
            print(
                f"{table}: oldest {oldest} is inside protected window "
                f"[{effective_cutoff}, ...), skipping"
            )
            continue

        pending = int(
            _scalar(
                f'SELECT count(*) FROM {table} WHERE "{column}" < '
                f"'{effective_cutoff}+00'",
                **db,
            )
        )
        print(f"{table}: archiving [{oldest}, {effective_cutoff}) -> {pending} rows")
        if args.dry_run:
            continue

        archived = subprocess.run(  # noqa: S603
            [
                sys.executable,
                str(_ARCHIVER),
                table,
                column,
                str(oldest),
                str(effective_cutoff),
                "--container",
                args.container,
                "--database",
                args.database,
                "--user",
                args.user,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if archived.returncode != 0:
            print(f"  archive failed: {archived.stderr.strip()}", file=sys.stderr)
            return 1
        print(f"  {archived.stdout.strip()}")

        # Verify against the manifest before removing anything.
        manifest = (
            _ARCHIVE_ROOT
            / table
            / f"{table}_{oldest:%Y%m%d}_{effective_cutoff:%Y%m%d}.manifest.json"
        )
        if not manifest.exists():
            print(f"  no manifest at {manifest} -- refusing to delete", file=sys.stderr)
            return 1
        recorded = int(
            subprocess.run(  # noqa: S603
                [
                    sys.executable,
                    "-c",
                    f"import json;print(json.load(open({str(manifest)!r}))['rows'])",
                ],
                capture_output=True,
                text=True,
                check=False,
            ).stdout.strip()
            or "0"
        )
        if recorded != pending:
            print(
                f"  archived {recorded} rows but {pending} are in range "
                "-- refusing to delete",
                file=sys.stderr,
            )
            return 1
        print(f"  manifest verified: {recorded} rows")

        if table == "strategy_runtime_events" and _table_is_partitioned(table, **db):
            dropped = _drop_expired_partitions(table, effective_cutoff, **db)
            print(f"  dropped {dropped} expired partitions")
            continue

        deleted = 0
        while True:
            removed = int(
                _scalar(
                    "WITH d AS (DELETE FROM "
                    f"{table} WHERE ctid IN (SELECT ctid FROM {table} "
                    f'WHERE "{column}" < \'{effective_cutoff}+00\' '
                    f"LIMIT {args.batch_rows}) "
                    "RETURNING 1) SELECT count(*) FROM d",
                    **db,
                )
            )
            if removed == 0:
                break
            deleted += removed
            if deleted % (args.batch_rows * 20) == 0:
                print(f"  deleted {deleted} / {pending}")
        print(f"  deleted {deleted} rows")

    if args.dry_run:
        print("\ndry run: nothing archived or deleted")
        return 0

    print("\nvacuuming...")
    for table, _column in TABLES:
        if args.table is not None and table != args.table:
            continue
        _psql(f"VACUUM (ANALYZE) {table}", **db)
        print(f"  vacuumed {table}")
    print("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
