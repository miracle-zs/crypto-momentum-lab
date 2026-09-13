#!/usr/bin/env python3
"""Normalize and de-duplicate exported runtime market-state CSV files."""

from __future__ import annotations

import argparse
import csv
import gzip
import sqlite3
from pathlib import Path
from typing import TextIO


FIELDS = [
    "symbol",
    "bucket_start",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "trade_count",
    "trade_notional",
    "aggressive_buy_notional",
    "aggressive_sell_notional",
    "data_complete",
    "missing_agg_trade_count",
]


def open_csv(path: Path) -> TextIO:
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", newline="")
    return path.open("r", encoding="utf-8", newline="")


def value(row: dict[str, str], name: str, default: str = "") -> str:
    raw = row.get(name)
    return default if raw is None or raw == "" else raw


def canonical(row: dict[str, str]) -> tuple[str, ...] | None:
    symbol = value(row, "symbol")
    bucket_start = value(row, "bucket_start")
    high = value(row, "high_price")
    low = value(row, "low_price")
    close = value(row, "close_price")
    if not symbol or not bucket_start or not high or not low or not close:
        return None
    open_price = value(row, "open_price", close)
    return (
        symbol,
        bucket_start,
        open_price,
        high,
        low,
        close,
        value(row, "trade_count", "0"),
        value(row, "trade_notional", "0"),
        value(row, "aggressive_buy_notional", "0"),
        value(row, "aggressive_sell_notional", "0"),
        value(row, "data_complete", "t"),
        value(row, "missing_agg_trade_count", "0"),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("inputs", type=Path, nargs="+")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path = args.output.with_suffix(args.output.suffix + ".sqlite")
    if db_path.exists():
        db_path.unlink()
    connection = sqlite3.connect(db_path)
    connection.execute("PRAGMA journal_mode=OFF")
    connection.execute("PRAGMA synchronous=OFF")
    connection.execute(
        "CREATE TABLE states ("
        "symbol TEXT NOT NULL, bucket_start TEXT NOT NULL, "
        "open_price TEXT, high_price TEXT, low_price TEXT, close_price TEXT, "
        "trade_count TEXT, trade_notional TEXT, "
        "aggressive_buy_notional TEXT, aggressive_sell_notional TEXT, "
        "data_complete TEXT, missing_agg_trade_count TEXT, "
        "PRIMARY KEY (symbol, bucket_start))"
    )

    inserted = 0
    skipped = 0
    for path in args.inputs:
        batch: list[tuple[str, ...]] = []
        with open_csv(path) as handle:
            for row in csv.DictReader(handle):
                item = canonical(row)
                if item is None:
                    skipped += 1
                    continue
                batch.append(item)
                if len(batch) >= 10_000:
                    connection.executemany(
                        "INSERT OR REPLACE INTO states VALUES (" + ",".join("?" * len(FIELDS)) + ")",
                        batch,
                    )
                    inserted += len(batch)
                    batch.clear()
            if batch:
                connection.executemany(
                    "INSERT OR REPLACE INTO states VALUES (" + ",".join("?" * len(FIELDS)) + ")",
                    batch,
                )
                inserted += len(batch)
        connection.commit()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(args.output, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(FIELDS)
        cursor = connection.execute(
            "SELECT " + ",".join(FIELDS) + " FROM states ORDER BY symbol, bucket_start"
        )
        writer.writerows(cursor)
    unique_rows = connection.execute("SELECT count(*) FROM states").fetchone()[0]
    symbols = connection.execute("SELECT count(DISTINCT symbol) FROM states").fetchone()[0]
    connection.close()
    db_path.unlink()
    print(
        {
            "input_rows_accepted": inserted,
            "invalid_rows_skipped": skipped,
            "unique_rows": unique_rows,
            "symbols": symbols,
            "output": str(args.output),
        }
    )


if __name__ == "__main__":
    main()
