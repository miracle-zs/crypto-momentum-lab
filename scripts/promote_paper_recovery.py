#!/usr/bin/env python3
"""Atomically promote recovered paper-account artifacts into production."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass

import psycopg
from psycopg import Connection, sql
from sqlalchemy.engine import make_url

RUN_IDS = (
    "paper-account-01-compression-v1",
    "paper-account-02-orderflow-v1",
    "paper-account-03-liquidation-v1",
    "paper-account-04-compression-candle15m-v1",
    "paper-account-05-orderflow-candle15m-v1",
    "paper-account-06-liquidation-candle15m-v1",
)


@dataclass(frozen=True, slots=True)
class TableCopy:
    name: str
    has_run_id: bool = True


TABLES = (
    TableCopy("strategy_runs"),
    TableCopy("strategy_signals"),
    TableCopy("order_intent_candidates"),
    TableCopy("paper_fills"),
    TableCopy("paper_positions"),
    TableCopy("paper_equity_snapshots"),
    TableCopy("strategy_checkpoints"),
    TableCopy("strategy_runtime_events"),
    TableCopy("strategy_runtime_checkpoints"),
    TableCopy("runtime_market_states_15s", has_run_id=False),
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", default=os.environ.get("CML_DATABASE_URL"))
    parser.add_argument("--source-database", required=True)
    parser.add_argument("--target-database", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    if not args.database_url:
        raise SystemExit("--database-url or CML_DATABASE_URL is required")
    source_dsn = _database_dsn(args.database_url, args.source_database)
    target_dsn = _database_dsn(args.database_url, args.target_database)

    with (
        psycopg.connect(source_dsn) as source,
        psycopg.connect(target_dsn) as target,
    ):
        _guard_database(source, args.source_database)
        _guard_database(target, args.target_database)
        staged = {
            table.name: _stage_table(source, target, table)
            for table in TABLES
        }
        report = {
            "mode": "apply" if args.apply else "dry-run",
            "source_database": args.source_database,
            "target_database": args.target_database,
            "staged_rows": staged,
        }
        if not args.apply:
            target.rollback()
            print(json.dumps(report, indent=2, sort_keys=True))
            return

        _replace_accounts(target)
        target.commit()
        report["target_rows"] = _target_counts(target)
        print(json.dumps(report, indent=2, sort_keys=True))


def _stage_table(
    source: Connection[object],
    target: Connection[object],
    table: TableCopy,
) -> int:
    stage_name = f"recovery_stage_{table.name}"
    with target.cursor() as target_cursor:
        target_cursor.execute(
            sql.SQL("CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS)").format(
                sql.Identifier(stage_name),
                sql.Identifier(table.name),
            )
        )
    source_query = _source_copy_query(table)
    target_query = sql.SQL("COPY {} FROM STDIN (FORMAT BINARY)").format(
        sql.Identifier(stage_name)
    )
    with (
        source.cursor().copy(source_query) as source_copy,
        target.cursor().copy(target_query) as target_copy,
    ):
        for block in source_copy:
            target_copy.write(block)
    with target.cursor() as cursor:
        cursor.execute(
            sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(stage_name))
        )
        row = cursor.fetchone()
    if row is None:
        raise RuntimeError(f"could not count staged rows for {table.name}")
    return int(row[0])


def _source_copy_query(table: TableCopy) -> sql.Composed:
    if table.has_run_id:
        return sql.SQL(
            "COPY (SELECT * FROM {} WHERE run_id IN ({})) "
            "TO STDOUT (FORMAT BINARY)"
        ).format(
            sql.Identifier(table.name),
            sql.SQL(", ").join(sql.Literal(run_id) for run_id in RUN_IDS),
        )
    return sql.SQL(
        "COPY (SELECT * FROM runtime_market_states_15s "
        "WHERE closure_reason = 'raw_archive_backfill' "
        "AND bucket_start >= '2026-08-01 01:40:00+00' "
        "AND bucket_start < '2026-08-01 01:48:15+00') "
        "TO STDOUT (FORMAT BINARY)"
    )


def _replace_accounts(target: Connection[object]) -> None:
    run_ids = sql.SQL(", ").join(sql.Literal(run_id) for run_id in RUN_IDS)
    with target.cursor() as cursor:
        for table_name in (
            "strategy_runtime_events",
            "strategy_runtime_checkpoints",
            "strategy_checkpoints",
        ):
            cursor.execute(
                sql.SQL("DELETE FROM {} WHERE run_id IN ({})").format(
                    sql.Identifier(table_name),
                    run_ids,
                )
            )
        cursor.execute(
            sql.SQL("DELETE FROM strategy_runs WHERE run_id IN ({})").format(
                run_ids
            )
        )
        for table in TABLES[:-1]:
            cursor.execute(
                sql.SQL("INSERT INTO {} SELECT * FROM {}").format(
                    sql.Identifier(table.name),
                    sql.Identifier(f"recovery_stage_{table.name}"),
                )
            )
        cursor.execute(
            """
            INSERT INTO runtime_market_states_15s
            SELECT * FROM recovery_stage_runtime_market_states_15s
            ON CONFLICT (environment, symbol, bucket_start) DO NOTHING
            """
        )


def _target_counts(target: Connection[object]) -> dict[str, int]:
    counts: dict[str, int] = {}
    run_ids = sql.SQL(", ").join(sql.Literal(run_id) for run_id in RUN_IDS)
    with target.cursor() as cursor:
        for table in TABLES[:-1]:
            cursor.execute(
                sql.SQL("SELECT count(*) FROM {} WHERE run_id IN ({})").format(
                    sql.Identifier(table.name),
                    run_ids,
                )
            )
            row = cursor.fetchone()
            counts[table.name] = 0 if row is None else int(row[0])
    return counts


def _guard_database(connection: Connection[object], expected: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT current_database()")
        row = cursor.fetchone()
    actual = None if row is None else row[0]
    if actual != expected:
        raise SystemExit(
            f"database guard failed: expected {expected!r}, connected to {actual!r}"
        )


def _database_dsn(raw_url: str, database_name: str) -> str:
    return (
        make_url(raw_url)
        .set(drivername="postgresql", database=database_name)
        .render_as_string(hide_password=False)
    )


if __name__ == "__main__":
    main()
