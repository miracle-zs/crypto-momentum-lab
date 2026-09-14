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
from datetime import UTC, datetime, timedelta
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_ARCHIVER = _HERE / "archive_table.py"
_ARCHIVE_ROOT = Path("/var/lib/crypto-momentum-lab/table-archive")
_DEFAULT_CONTAINER = "crypto-momentum-lab-postgres-1"

# table, time column -- the window is half-open [oldest, cutoff).
TABLES: tuple[tuple[str, str], ...] = (
    ("strategy_runtime_events", "occurred_at"),
    ("universe_entries", "price_time"),
    ("account_balance_snapshots", "observed_at"),
    ("account_position_snapshots", "observed_at"),
    ("exchange_order_events", "occurred_at"),
    ("paper_equity_snapshots", "observed_at"),
)


def _psql(sql: str, *, container: str, database: str, user: str) -> str:
    result = subprocess.run(  # noqa: S603 - argv built here, never a shell
        [
            "docker", "exec", container,
            "psql", "-U", user, "-d", database, "-At", "-X", "-q", "-c", sql,
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
    return _psql(sql, **kw).splitlines()[0].strip()


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
            f'SELECT coalesce(min("{column}")::date::text, \'\') FROM {table}', **db
        )
        if not oldest_raw:
            print(f"{table}: empty, skipping")
            continue
        oldest = datetime.fromisoformat(oldest_raw).date()
        if oldest >= cutoff:
            print(f"{table}: oldest {oldest} is already inside the window, skipping")
            continue

        pending = int(
            _scalar(
                f"SELECT count(*) FROM {table} WHERE \"{column}\" < '{cutoff}+00'",
                **db,
            )
        )
        print(f"{table}: archiving [{oldest}, {cutoff}) -> {pending} rows")
        if args.dry_run:
            continue

        archived = subprocess.run(  # noqa: S603
            [
                sys.executable, str(_ARCHIVER),
                table, column, str(oldest), str(cutoff),
                "--container", args.container,
                "--database", args.database,
                "--user", args.user,
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
            _ARCHIVE_ROOT / table
            / f"{table}_{oldest:%Y%m%d}_{cutoff:%Y%m%d}.manifest.json"
        )
        if not manifest.exists():
            print(f"  no manifest at {manifest} -- refusing to delete", file=sys.stderr)
            return 1
        recorded = int(
            subprocess.run(  # noqa: S603
                [sys.executable, "-c",
                 f"import json;print(json.load(open({str(manifest)!r}))['rows'])"],
                capture_output=True, text=True, check=False,
            ).stdout.strip() or "0"
        )
        if recorded != pending:
            print(
                f"  archived {recorded} rows but {pending} are in range "
                "-- refusing to delete",
                file=sys.stderr,
            )
            return 1
        print(f"  manifest verified: {recorded} rows")

        deleted = 0
        while True:
            removed = int(
                _scalar(
                    "WITH d AS (DELETE FROM "
                    f"{table} WHERE ctid IN (SELECT ctid FROM {table} "
                    f"WHERE \"{column}\" < '{cutoff}+00' LIMIT {args.batch_rows}) "
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
