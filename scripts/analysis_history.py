"""Synchronize retained trading history and expose unified, read-only DuckDB views.

Run with the local research environment; dependencies are listed in
local_optimization/requirements.txt.
Never writes trading tables or changes retention. Raw exports remain the audit source.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import shlex
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

TABLE_KEYS = {
    "account_fill_events": ["environment", "account_label", "symbol", "trade_id"],
    "account_balance_snapshots": ["snapshot_id"],
    "account_position_snapshots": ["snapshot_id"],
    "account_config_snapshots": ["snapshot_id"],
    "live_strategy_signals": ["observation_id"],
    "exchange_orders": ["client_order_id"],
    "order_intents": ["intent_id"],
    "exchange_order_events": ["event_id"],
    "strategy_runs": ["run_id"],
    "strategy_runtime_events": ["event_id", "occurred_at"],
    "live_orders": ["environment", "account_label", "client_order_id"],
    "live_position_batches": ["environment", "account_label", "batch_id"],
    "universe_snapshots": ["snapshot_id"],
    "universe_entries": ["snapshot_id", "symbol"],
    "execution_book_heads": ["environment", "account_label", "symbol", "position_side"],
    "live_session_transitions": ["transition_id"],
}
ARCHIVE_ROOT = "/var/lib/crypto-momentum-lab/table-archive"


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def ssh(args: argparse.Namespace, command: str, **kwargs):
    argv = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15"]
    if args.control_path:
        argv += ["-o", "ControlPath=" + args.control_path]
    return subprocess.run([*argv, args.host, command], check=True, **kwargs)


def download(args: argparse.Namespace, remote: str, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    pending = target.with_suffix(target.suffix + ".partial")
    with pending.open("wb") as output:
        ssh(args, "cat -- " + shlex.quote(remote), stdout=output)
    pending.replace(target)


def sync(args: argparse.Namespace, root: Path) -> None:
    cursors = {}
    journal_until = None
    for metadata_path in (root / "logs").rglob("*.metadata.json"):
        metadata = json.loads(metadata_path.read_text())
        if metadata.get("format") == "journal" and "until" in metadata:
            journal_until = max(journal_until or "", metadata["until"])
        if "file" in metadata:
            container_id = Path(metadata["file"]).name.split(".")[0]
            cursors[container_id] = max(
                cursors.get(container_id, ""), metadata["until"]
            )
    # Capture Docker's retained rotated logs before any further container removal.
    code = r"""import gzip,json,pathlib,subprocess
from datetime import UTC,datetime
stamp=datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
until=datetime.now(UTC).isoformat()
root=pathlib.Path("/var/lib/crypto-momentum-lab/history-logs")/stamp
root.mkdir(parents=True)
containers=json.loads(subprocess.check_output(["docker","inspect"]+subprocess.check_output(["docker","ps","-aq","--filter","label=com.docker.compose.project=crypto-momentum-lab"],text=True).split(),text=True))
result=[]
for c in containers:
    service=c["Config"]["Labels"].get("com.docker.compose.service","")
    if service in ("migrate","volume-init","bootstrap-universe"): continue
    since=cursors.get(c["Id"])
    if since and not c["State"]["Running"]:
        finished=datetime.fromisoformat(c["State"]["FinishedAt"].replace("Z","+00:00"))
        if finished <= datetime.fromisoformat(since): continue
    target=root/(c["Id"]+".log.gz")
    with gzip.open(target,"wb",compresslevel=1) as out:
        argv=["docker","logs","--timestamps","--until",until]
        if since: argv += ["--since",since]
        process=subprocess.Popen(argv+[c["Id"]],stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        while chunk:=process.stdout.read(65536): out.write(chunk)
        if process.wait(): raise RuntimeError("log export failed: "+c["Name"])
    result.append({"file":str(target),"container":c["Name"],"service":service,"image":c["Config"]["Image"],"until":until})
    target.with_suffix(".metadata.json").write_text(json.dumps(result[-1]))
print(json.dumps(result))
"""
    code = "cursors=" + repr(cursors) + "\n" + code
    logs = json.loads(
        ssh(args, "python3 -", input=code, text=True, capture_output=True).stdout
    )
    for item in logs:
        target = (
            root / "logs" / Path(item["file"]).parent.name / Path(item["file"]).name
        )
        download(args, item["file"], target)
        target.with_suffix(".metadata.json").write_text(json.dumps(item))
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    host_log = root / "logs" / stamp / "host.log.gz"
    host_log.parent.mkdir(parents=True, exist_ok=True)
    until = datetime.now(UTC)
    bounds = " --until @" + str(until.timestamp())
    if journal_until:
        bounds += " --since @" + str(datetime.fromisoformat(journal_until).timestamp())
    command = (
        "journalctl --no-pager -o json -u cml-ops-monitor.service "
        "-u cml-live-container-events.service -u cml-archive-trim.service"
        + bounds
        + " | gzip -1c"
    )
    with host_log.open("wb") as output:
        ssh(args, "bash -o pipefail -c " + shlex.quote(command), stdout=output)
    host_log.with_suffix(".metadata.json").write_text(
        json.dumps(
            {
                "format": "journal",
                "until": until.isoformat(),
                "container": "host",
                "service": "systemd",
                "image": None,
            }
        )
    )
    print(f"preserved {len(logs)} container log streams and host journals", flush=True)
    # Export JSON without converting numeric source values to binary floats.
    for table in TABLE_KEYS:
        target = root / "hot" / (table + ".jsonl.gz")
        target.parent.mkdir(parents=True, exist_ok=True)
        pending = target.with_suffix(".partial")
        sql = f"SELECT row_to_json(t) FROM {table} t"
        command = (
            "docker exec -e "
            + shlex.quote(
                "PGOPTIONS=-c default_transaction_read_only=on "
                "-c statement_timeout=120000 -c work_mem=4MB"
            )
            + " crypto-momentum-lab-postgres-1 psql -X -qAt -U cml -d cml -c "
            + shlex.quote(sql)
            + " | gzip -1c"
        )
        with pending.open("wb") as output:
            ssh(args, "bash -o pipefail -c " + shlex.quote(command), stdout=output)
        pending.replace(target)
        print("exported " + table, flush=True)
    # Discover archives after hot reads: retention during export cannot hide rows.
    listing = (
        "import json,pathlib; r=pathlib.Path(" + repr(ARCHIVE_ROOT) + "); "
        "print(json.dumps([{'path':str(p),'manifest':json.loads(p.read_text())} "
        "for p in r.glob('*/*.manifest.json') if p.parent.name in "
        + repr(list(TABLE_KEYS))
        + "]))"
    )
    archives = json.loads(
        ssh(
            args, "python3 -c " + shlex.quote(listing), capture_output=True, text=True
        ).stdout
    )
    for item in archives:
        manifest = item["manifest"]
        if manifest["format"] not in ("jsonl", "csv"):
            raise ValueError("unsupported archive format: " + item["path"])
        remote = str(Path(item["path"]).parent / manifest["file"])
        target = root / "archives" / manifest["table"] / manifest["file"]
        if not target.exists() or digest(target) != manifest["sha256"]:
            download(args, remote, target)
        if digest(target) != manifest["sha256"]:
            raise ValueError("archive checksum mismatch: " + str(target))
        target.with_suffix(target.suffix + ".manifest.json").write_text(
            json.dumps(manifest)
        )
    print(f"synchronized {len(archives)} verified archives", flush=True)


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def normalize_logs(root: Path) -> None:
    schema = pa.schema(
        [
            (k, pa.string())
            for k in (
                "record_id",
                "received_at",
                "container",
                "service",
                "image",
                "account_label",
                "message",
                "source_file",
            )
        ]
    )
    for source in sorted((root / "logs").rglob("*.log.gz")):
        target = source.with_suffix(".parquet")
        if target.exists():
            continue
        meta = json.loads(source.with_suffix(".metadata.json").read_text())
        service = meta["service"]
        account = None
        for label in ("primary", "account-2", "account-3", "account-4"):
            if service.endswith("-" + label):
                account = label
        if service in ("live-strategy", "execution-account-live"):
            account = "primary"
        pending = target.with_suffix(".partial")
        with pq.ParquetWriter(pending, schema, compression="zstd") as writer:
            batch = []
            with gzip.open(source, "rt", errors="strict") as stream:
                for line in stream:
                    if meta.get("format") == "journal":
                        entry = json.loads(line)
                        stamp = datetime.fromtimestamp(
                            int(entry["__REALTIME_TIMESTAMP"]) / 1000000, tz=UTC
                        ).isoformat()
                        message = entry["MESSAGE"]
                        service = entry.get("_SYSTEMD_UNIT", "systemd")
                        separator = " "
                    else:
                        stamp, separator, message = line.rstrip("\n").partition(" ")
                    # docker logs --timestamps always prefixes RFC3339Nano time.
                    datetime.fromisoformat(stamp.replace("Z", "+00:00"))
                    if not separator:
                        raise ValueError("missing log timestamp: " + str(source))
                    identity = meta["container"] + stamp + message
                    batch.append(
                        dict(
                            zip(
                                schema.names,
                                (
                                    hashlib.sha256(identity.encode()).hexdigest(),
                                    stamp,
                                    meta["container"],
                                    service,
                                    meta["image"],
                                    account,
                                    message,
                                    str(source),
                                ),
                                strict=True,
                            )
                        )
                    )
                    if len(batch) >= 1024:
                        writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                        batch.clear()
            if batch:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
        pending.replace(target)


def build(root: Path) -> None:
    normalize_logs(root)
    target = root / "history.duckdb"
    db = duckdb.connect(str(target))
    db.execute("SET memory_limit='2GB'")
    db.execute("SET threads=1")
    db.execute("SET preserve_insertion_order=false")
    db.execute("SET TimeZone='UTC'")
    db.execute("BEGIN")
    try:
        db.execute(
            "CREATE OR REPLACE TABLE history_sources ("
            "source_table VARCHAR, source_file VARCHAR, origin VARCHAR, "
            "expected_rows BIGINT, sha256 VARCHAR)"
        )
        for table, keys in TABLE_KEYS.items():
            files = (
                sorted((root / "archives" / table).iterdir())
                if (root / "archives" / table).exists()
                else []
            )
            files = [
                p
                for p in files
                if p.name.endswith((".jsonl.gz", ".jsonl.zst", ".csv.gz", ".csv.zst"))
            ]
            hot = root / "hot" / (table + ".jsonl.gz")
            if hot.exists():
                files.append(hot)
            for path in files:
                manifest_path = path.with_suffix(path.suffix + ".manifest.json")
                manifest = (
                    json.loads(manifest_path.read_text())
                    if manifest_path.exists()
                    else {}
                )
                db.execute(
                    "INSERT INTO history_sources VALUES (?,?,?,?,?)",
                    [
                        table,
                        str(path),
                        "hot" if path == hot else "archive",
                        manifest.get("rows"),
                        manifest.get("sha256"),
                    ],
                )
            # Empty exports are valid; a typed empty relation keeps all views usable.
            sources = []
            for suffix, function in (
                ("jsonl", "read_json_objects"),
                ("csv", "read_csv"),
            ):
                group = [p for p in files if f".{suffix}." in p.name]
                if not group:
                    continue
                paths = "[" + ",".join(literal(str(p)) for p in group) + "]"
                options = (
                    "format='newline_delimited',maximum_object_size=67108864"
                    if suffix == "jsonl"
                    else "header=true,all_varchar=true,union_by_name=true"
                )
                raw = "json" if suffix == "jsonl" else "to_json(t)"
                sources.append(
                    f"SELECT {raw} raw,filename source_file,"
                    f"CASE WHEN filename={literal(str(hot))} THEN 1 ELSE 0 END "
                    f"source_priority FROM {function}"
                    f"({paths},filename=true,{options}) t"
                )
            source = (
                " UNION ALL ".join(sources)
                if sources
                else (
                    "SELECT NULL::JSON raw, NULL::VARCHAR source_file, "
                    "0 source_priority WHERE false"
                )
            )
            key = ",".join(field(k) for k in keys)
            # PostgreSQL and archives share the same primary keys; hot rows win overlap.
            db.execute(
                f"CREATE OR REPLACE VIEW raw_{table} AS SELECT raw,source_file "
                f"FROM ({source}) QUALIFY row_number() OVER "
                f"(PARTITION BY {key} ORDER BY source_priority DESC,source_file DESC)=1"
            )
            if table == "strategy_runtime_events":
                for alias, event_type in (
                    ("nextgen_signals", "nextgen_entry_signal"),
                    ("nextgen_balances", "nextgen_runtime_status"),
                ):
                    db.execute(
                        f"CREATE OR REPLACE VIEW raw_{alias} AS "
                        f"SELECT raw,source_file FROM ({source}) WHERE "
                        "json_extract_string(raw,'$.event_type')="
                        f"{literal(event_type)} "
                        f"QUALIFY row_number() OVER (PARTITION BY {key} "
                        "ORDER BY source_priority DESC,source_file DESC)=1"
                    )
        for name, sql in VIEWS.items():
            db.execute(f"CREATE OR REPLACE VIEW {name} AS {sql}")
        logs = sorted((root / "logs").rglob("*.log.parquet"))
        if logs:
            paths = "[" + ",".join(literal(str(p)) for p in logs) + "]"
            db.execute(
                "CREATE OR REPLACE VIEW logs AS SELECT * EXCLUDE(received_at), "
                "CAST(received_at AS TIMESTAMPTZ) received_at, "
                "received_at received_at_raw FROM "
                f"read_parquet({paths}) QUALIFY row_number() OVER "
                "(PARTITION BY record_id ORDER BY source_file DESC)=1"
            )
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise
    finally:
        db.close()
    print("history database: " + str(target), flush=True)


MATERIALIZED_TABLES = {
    **{
        name: name
        for name in (
            "fills",
            "orders",
            "signals",
            "balances",
            "runtime_events",
            "position_batches",
            "logs",
            "history_sources",
        )
    },
    "account_configs": "raw_account_config_snapshots",
    "position_snapshots": "raw_account_position_snapshots",
    "strategy_runs": "raw_strategy_runs",
    "order_intents": "raw_order_intents",
    "order_events": "raw_exchange_order_events",
    "universe_snapshots": "raw_universe_snapshots",
    "universe_entries": "raw_universe_entries",
    "session_transitions": "raw_live_session_transitions",
    "legacy_position_state": "raw_execution_book_heads",
}


def materialize(root: Path) -> None:
    """Publish one self-contained research database; retain raw source backups."""
    source = root / "history.duckdb"
    target = root / "research_history.duckdb"
    pending = root / "research_history.building.duckdb"
    if pending.exists():
        pending.unlink()
    with duckdb.connect(str(source), read_only=True) as db:
        db.execute("SET memory_limit='2GB'")
        db.execute("SET threads=1")
        db.execute("SET preserve_insertion_order=false")
        db.execute("SET TimeZone='UTC'")
        db.execute("SET max_temp_directory_size='4GB'")
        db.execute(f"ATTACH {literal(str(pending))} AS merged (READ_WRITE)")
        counts = []
        for name, relation in MATERIALIZED_TABLES.items():
            count = db.execute(
                f"CREATE TABLE merged.{name} AS SELECT * FROM main.{relation}"
            ).fetchone()[0]
            counts.append((name, count))
            print(f"materialized {name}: {count} rows", flush=True)
        db.execute(
            "CREATE TABLE merged.row_counts (table_name VARCHAR,row_count BIGINT)"
        )
        db.executemany("INSERT INTO merged.row_counts VALUES (?,?)", counts)
        db.execute(
            "CREATE TABLE merged.build_metadata AS SELECT "
            "current_timestamp AS built_at, ?::VARCHAR AS normalizer_sha256",
            [digest(Path(__file__))],
        )
        db.execute("DETACH merged")
    with duckdb.connect(str(pending), read_only=True) as db:
        for name, expected in counts:
            actual = db.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
            if actual != expected:
                raise RuntimeError(f"materialized row count mismatch: {name}")
        views = db.execute(
            "SELECT count(*) FROM duckdb_views() WHERE NOT internal"
        ).fetchone()[0]
        if views:
            raise RuntimeError("research database still contains views")
    pending.replace(target)
    print(
        f"standalone history database: {target} ({target.stat().st_size} bytes)",
        flush=True,
    )


# Fields absent from a source remain NULL. Keep raw JSON and exact decimal strings.
def field(name: str) -> str:
    expression = f"json_extract_string(raw, '$.{name}')"
    leaf = name.split(".")[-1]
    if leaf in {
        "price",
        "quantity",
        "fee",
        "realized_pnl",
        "filled_quantity",
        "average_price",
        "executed_quantity",
        "wallet_balance",
        "available_balance",
        "unrealized_pnl",
        "USDT",
        "quote_volume_24h",
    }:
        return f"CAST({expression} AS DECIMAL(38,18))"
    if leaf in {
        "trade_at",
        "created_at",
        "updated_at",
        "detected_at",
        "recorded_at",
        "occurred_at",
        "observed_at",
    }:
        return f"CAST({expression} AS TIMESTAMPTZ)"
    return expression


VIEWS = {
    "fills": "SELECT "
    + ",".join(
        field(k) + " AS " + k
        for k in (
            "environment",
            "account_label",
            "symbol",
            "trade_id",
            "order_id",
            "side",
            "price",
            "quantity",
            "fee",
            "fee_asset",
            "realized_pnl",
            "trade_at",
        )
    )
    + ",raw,source_file FROM raw_account_fill_events",
    "orders": "SELECT 'nextgen' engine,"
    + ",".join(
        field(k) + " AS " + k
        for k in (
            "environment",
            "account_label",
            "symbol",
            "client_order_id",
            "exchange_order_id",
            "side",
            "position_side",
            "order_type",
            "quantity",
            "price",
            "status",
            "filled_quantity",
            "average_price",
            "created_at",
            "updated_at",
        )
    )
    + ",raw,source_file FROM raw_live_orders UNION ALL SELECT 'legacy',"
    + "NULL,NULL,"
    + ",".join(
        field(k)
        for k in (
            "symbol",
            "client_order_id",
            "exchange_order_id",
            "side",
            "position_side",
            "order_type",
            "quantity",
            "price",
        )
    )
    + ",upper("
    + field("state")
    + "),"
    + field("executed_quantity")
    + ",NULL,"
    + field("created_at")
    + ","
    + field("updated_at")
    + ",raw,source_file FROM raw_exchange_orders",
    "runtime_events": "SELECT "
    + ",".join(
        field(k) + " AS " + k
        for k in ("event_id", "run_id", "event_type", "occurred_at", "symbol")
    )
    + ",raw,source_file FROM raw_strategy_runtime_events",
    "balances": "SELECT 'exchange_snapshot' valuation_basis,"
    + ",".join(
        field(k) + " AS " + k
        for k in (
            "environment",
            "account_label",
            "asset",
            "wallet_balance",
            "available_balance",
            "unrealized_pnl",
            "observed_at",
        )
    )
    + ",raw,source_file FROM raw_account_balance_snapshots",
    "position_batches": "SELECT * FROM raw_live_position_batches",
}

signal_columns = (
    "signal_id",
    "symbol",
    "side",
    "detected_at",
    "recorded_at",
    "source_state_at",
    "strategy_name",
    "strategy_version",
    "config_hash",
    "code_commit",
    "reason",
    "quote_volume_24h",
)
signal_json_columns = ("features", "reference_prices", "filter_context")
VIEWS["signals"] = (
    "SELECT 'legacy' engine,"
    + field("account_label")
    + " account_label,"
    + ",".join(field(k) + " AS " + k for k in signal_columns)
    + ","
    + ",".join("json_extract(raw,'$." + k + "') AS " + k for k in signal_json_columns)
    + ",raw,source_file FROM raw_live_strategy_signals UNION ALL "
    "SELECT 'nextgen',split_part("
    + field("run_id")
    + ",':',3),"
    + ",".join(field("details." + k) for k in signal_columns)
    + ","
    + ",".join("json_extract(raw,'$.details." + k + "')" for k in signal_json_columns)
    + ",raw,source_file FROM raw_strategy_runtime_events WHERE "
    + field("event_type")
    + "='nextgen_entry_signal'"
)

VIEWS["runtime_events"] = VIEWS["runtime_events"].replace(
    ",raw,source_file", ",json_extract(raw,'$.details') details,raw,source_file"
)
VIEWS["runtime_events"] = VIEWS["runtime_events"].replace(
    "SELECT ",
    "SELECT CASE WHEN " + field("run_id") + " LIKE 'nextgen:live:%' THEN "
    "split_part("
    + field("run_id")
    + ",':',3) ELSE "
    + field("details.account_label")
    + " END account_label,",
    1,
)

VIEWS["fills"] = VIEWS["fills"].replace(
    ",raw,source_file",
    ",coalesce("
    "json_extract_string(raw,'$.raw_payload.positionSide'),"
    "json_extract_string(raw,'$.raw_payload.ps'),"
    "json_extract_string(raw,'$.raw_payload.row.positionSide'),"
    "json_extract_string(raw,'$.raw_payload.event.o.ps')) position_side,"
    "CAST(json_extract_string(raw,'$.raw_payload.is_cumulative') AS BOOLEAN) "
    "is_cumulative,CAST(json_extract_string(raw,'$.raw_payload.synthetic_from_order') "
    "AS BOOLEAN) synthetic_from_order,raw,source_file",
)

# Only a run with one observed account/environment receives that ownership.
run_accounts = (
    "SELECT json_extract_string(e.raw,'$.run_id') run_id,"
    "min(f.environment) environment,min(f.account_label) account_label "
    "FROM raw_exchange_orders e JOIN fills f ON "
    "f.symbol=json_extract_string(e.raw,'$.symbol') AND "
    "f.order_id=json_extract_string(e.raw,'$.exchange_order_id') "
    "GROUP BY 1 HAVING count(DISTINCT f.environment||':'||f.account_label)=1"
)
VIEWS = {"fills": VIEWS.pop("fills"), "legacy_run_accounts": run_accounts, **VIEWS}
VIEWS["orders"] = (
    VIEWS["orders"].replace(
        "SELECT 'legacy',NULL,NULL,", "SELECT 'legacy',f.environment,f.account_label,"
    )
    + " LEFT JOIN legacy_run_accounts f ON f.run_id="
    + field("run_id")
)

VIEWS["runtime_events"] = (
    VIEWS["runtime_events"].replace(
        " ELSE " + field("details.account_label") + " END account_label",
        " ELSE coalesce("
        + field("details.account_label")
        + ",owner.account_label) END account_label",
    )
    + " LEFT JOIN legacy_run_accounts owner ON owner.run_id="
    + field("run_id")
)

# A local mark projection is distinct from a historical exchange valuation.
VIEWS["balances"] += (
    " UNION ALL SELECT 'local_mark_projection',"
    + "split_part("
    + field("run_id")
    + ",':',2),"
    + "split_part("
    + field("run_id")
    + ",':',3),'USDT',"
    + field("details.balances.USDT")
    + ",NULL,"
    + field("details.unrealized_pnl")
    + ","
    + field("occurred_at")
    + ",raw,source_file FROM raw_strategy_runtime_events WHERE "
    + field("event_type")
    + "='nextgen_runtime_status'"
)

VIEWS["signals"] = VIEWS["signals"].replace(
    "FROM raw_strategy_runtime_events", "FROM raw_nextgen_signals"
)
VIEWS["balances"] = VIEWS["balances"].replace(
    "FROM raw_strategy_runtime_events", "FROM raw_nextgen_balances"
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("data/analysis_history"))
    parser.add_argument("--host", default="root@43.167.191.253")
    parser.add_argument("--control-path")
    parser.add_argument("--build-only", action="store_true")
    parser.add_argument("--materialize-only", action="store_true")
    parser.add_argument("--query", help="Read-only SQL against the local history")
    args = parser.parse_args()
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    if args.query:
        with duckdb.connect(
            str(root / "research_history.duckdb"), read_only=True
        ) as db:
            db.execute("SET memory_limit='2GB'")
            db.execute("SET TimeZone='UTC'")
            db.execute("SET threads=1")
            db.execute("SET preserve_insertion_order=false")
            result = db.execute(args.query)
            writer = csv.writer(sys.stdout)
            writer.writerow([column[0] for column in result.description])
            while rows := result.fetchmany(1000):
                writer.writerows(rows)
        return
    if not args.materialize_only:
        if not args.build_only:
            sync(args, root)
        build(root)
    materialize(root)


if __name__ == "__main__":
    main()
