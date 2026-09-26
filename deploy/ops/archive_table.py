#!/usr/bin/env python3
"""Export a PostgreSQL table's rows to compressed CSV, with a manifest.

Several tables live *only* in PostgreSQL -- ``strategy_runtime_events``,
``universe_entries``, the ``account_*_snapshots`` family -- so the retention
that trims them is a hard delete.  That is different from the market-data
streams, which have a zstd-parquet archive behind them and can be trimmed
freely.

This produces that missing copy first: a gzip/zstd CSV of a date range plus a
sibling manifest recording the row count, byte size and sha256.  A later
deletion step can consult the manifest to prove the archive exists before it
removes anything.

Usage:
    archive_table.py <table> <time-column> <from-date> <to-date> [options]

    ./archive_table.py universe_entries price_time 2026-07-25 2026-08-01

The range is half-open: ``[from-date, to-date)`` in UTC.

Run it on the server (it shells out to ``docker exec`` on the local host) or
point ``--host`` at one over ssh.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shlex
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

_DEFAULT_OUT = "/var/lib/crypto-momentum-lab/table-archive"
_DEFAULT_CONTAINER = "crypto-momentum-lab-postgres-1"
_DEFAULT_DATABASE = "cml"
_DEFAULT_USER = "cml"


def _run(argv: list[str]) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(  # noqa: S603 - argv is built here, never a shell string
        argv,
        capture_output=True,
        check=False,
    )


def _remote_prefix(host: str | None) -> list[str]:
    if host is None:
        return []
    # BatchMode: fail instead of prompting, so a bad key is an error not a hang.
    return ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", host]


def _psql(prefix: list[str], container: str, user: str, database: str) -> list[str]:
    return [
        *prefix,
        "docker",
        "exec",
        container,
        "psql",
        "-U",
        user,
        "-d",
        database,
        "-X",
        "-q",
    ]


def has_json_columns(
    prefix: list[str],
    *,
    container: str,
    user: str,
    database: str,
    table: str,
) -> bool:
    """Report whether the table holds json/jsonb, which CSV would mangle.

    A jsonb column lands in CSV as one escaped string, so a table carrying
    nested payloads is exported as JSONL instead -- one ``row_to_json`` object
    per line, the same shape ``raw_files`` already produces.
    """
    sql = (
        "SELECT count(*) FROM information_schema.columns "
        "WHERE table_schema = 'public' "
        f"AND table_name = '{table}' AND data_type IN ('json', 'jsonb')"
    )
    result = _run([*_psql(prefix, container, user, database), "-At", "-c", sql])
    if result.returncode != 0:
        raise SystemExit(
            f"column probe failed: {result.stderr.decode(errors='replace').strip()}"
        )
    return int(result.stdout.decode().strip() or "0") > 0


def count_rows(
    prefix: list[str],
    *,
    container: str,
    user: str,
    database: str,
    table: str,
    column: str,
    start: str,
    end: str,
) -> int:
    """Return the number of rows the range covers, so an empty range is caught."""
    sql = (
        f'SELECT count(*) FROM "{table}" '
        f"WHERE \"{column}\" >= '{start}+00' AND \"{column}\" < '{end}+00'"
    )
    result = _run(
        [*_psql(prefix, container, user, database), "-At", "-c", sql]
    )
    if result.returncode != 0:
        raise SystemExit(
            f"count failed: {result.stderr.decode(errors='replace').strip()}"
        )
    return int(result.stdout.decode().strip() or "0")


def compute_range_fingerprint(
    prefix: list[str],
    *,
    container: str,
    user: str,
    database: str,
    table: str,
    column: str,
    start: str,
    end: str,
) -> str:
    """Deterministic content fingerprint of rows in [start, end) range."""
    sql = (
        "SELECT count(*)::text || '|' || "
        "coalesce(md5(string_agg(md5(t::text), '' ORDER BY md5(t::text))), '') "
        f'FROM (SELECT * FROM "{table}" '
        f"WHERE \"{column}\" >= '{start}+00' AND \"{column}\" < '{end}+00') t"
    )
    result = _run([*_psql(prefix, container, user, database), "-At", "-c", sql])
    if result.returncode != 0:
        raise SystemExit(
            f"fingerprint failed: {result.stderr.decode(errors='replace').strip()}"
        )
    return result.stdout.decode().strip()


def export_range(
    prefix: list[str],
    *,
    container: str,
    user: str,
    database: str,
    table: str,
    column: str,
    start: str,
    end: str,
    destination: Path,
    as_jsonl: bool,
) -> None:
    """Stream ``COPY ... TO STDOUT`` through zstd into ``destination``.

    The two processes are joined by a pipe rather than captured into memory:
    this table is hundreds of megabytes, and the point of archiving on a host
    with 3.6 GiB is to *not* hold it all at once.  Nothing uncompressed ever
    touches the disk either.

    ``as_jsonl`` wraps each row in ``row_to_json`` so nested jsonb survives as
    structure instead of a quoted string.
    """
    if as_jsonl:
        copy = (
            "COPY (SELECT row_to_json(t) FROM ("
            f'SELECT * FROM "{table}" '
            f"WHERE \"{column}\" >= '{start}+00' AND \"{column}\" < '{end}+00' "
            f'ORDER BY "{column}") t) TO STDOUT'
        )
    else:
        copy = (
            f'COPY (SELECT * FROM "{table}" '
            f"WHERE \"{column}\" >= '{start}+00' AND \"{column}\" < '{end}+00' "
            f"ORDER BY \"{column}\") TO STDOUT WITH (FORMAT csv, HEADER)"
        )
    destination.parent.mkdir(parents=True, exist_ok=True)

    with destination.open("wb") as sink:
        psql = subprocess.Popen(  # noqa: S603
            [*_psql(prefix, container, user, database), "-c", copy],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert psql.stdout is not None
        compress = subprocess.Popen(  # noqa: S603
            ["zstd", "-3", "-q", "-f", "-o", str(destination), "-"],
            stdin=psql.stdout,
            stdout=sink,
            stderr=subprocess.PIPE,
        )
        psql.stdout.close()  # let psql see EPIPE if zstd dies first
        _, compress_err = compress.communicate()
        psql_err = psql.stderr.read() if psql.stderr else b""
        psql.wait()

    if psql.returncode != 0:
        raise SystemExit(
            f"copy failed: {psql_err.decode(errors='replace').strip()}"
        )
    if compress.returncode != 0:
        raise SystemExit(
            f"zstd failed: {compress_err.decode(errors='replace').strip()}"
        )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("table")
    parser.add_argument("time_column")
    parser.add_argument("from_date", help="inclusive UTC date, YYYY-MM-DD")
    parser.add_argument("to_date", help="exclusive UTC date, YYYY-MM-DD")
    parser.add_argument("--out", default=_DEFAULT_OUT)
    parser.add_argument("--container", default=_DEFAULT_CONTAINER)
    parser.add_argument("--database", default=_DEFAULT_DATABASE)
    parser.add_argument("--user", default=_DEFAULT_USER)
    parser.add_argument("--host", default=None, help="ssh host; omit for local")
    args = parser.parse_args(argv)

    prefix = _remote_prefix(args.host)
    rows = count_rows(
        prefix,
        container=args.container,
        user=args.user,
        database=args.database,
        table=args.table,
        column=args.time_column,
        start=args.from_date,
        end=args.to_date,
    )
    if rows == 0:
        print(f"{args.table}: nothing in [{args.from_date}, {args.to_date})")
        return 0

    as_jsonl = has_json_columns(
        prefix,
        container=args.container,
        user=args.user,
        database=args.database,
        table=args.table,
    )
    suffix = "jsonl.zst" if as_jsonl else "csv.zst"

    start_tag = args.from_date.replace("-", "")
    end_tag = args.to_date.replace("-", "")
    stem = f"{args.table}_{start_tag}_{end_tag}"
    directory = Path(args.out) / args.table
    destination = directory / f"{stem}.{suffix}"
    manifest_path = directory / f"{stem}.manifest.json"

    export_range(
        prefix,
        container=args.container,
        user=args.user,
        database=args.database,
        table=args.table,
        column=args.time_column,
        start=args.from_date,
        end=args.to_date,
        destination=destination,
        as_jsonl=as_jsonl,
    )

    fingerprint = compute_range_fingerprint(
        prefix,
        container=args.container,
        user=args.user,
        database=args.database,
        table=args.table,
        column=args.time_column,
        start=args.from_date,
        end=args.to_date,
    )

    manifest = {
        "table": args.table,
        "time_column": args.time_column,
        "from": args.from_date,
        "to": args.to_date,
        "rows": rows,
        "content_fingerprint": fingerprint,
        "format": "jsonl" if as_jsonl else "csv",
        "compressed_bytes": destination.stat().st_size,
        "sha256": sha256_file(destination),
        "file": destination.name,
        "archived_at": datetime.now(tz=UTC).isoformat(),
        "command": " ".join(shlex.quote(part) for part in (argv or sys.argv[1:])),
    }
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    print(
        f"{args.table} [{args.from_date}, {args.to_date}): "
        f"{rows} rows -> {destination} "
        f"({manifest['compressed_bytes'] / 1024 / 1024:.1f} MiB, "
        f"{manifest['format']})"
    )
    print(f"manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
