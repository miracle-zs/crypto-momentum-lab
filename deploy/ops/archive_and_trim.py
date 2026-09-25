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
import hashlib
import json
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_REPO_ROOT = _HERE.parent.parent
if str(_REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT / "src"))

from crypto_momentum_lab.domain.operational.retention_authority import (  # noqa: E402
    RetentionAuthority,
)
from crypto_momentum_lab.domain.operational.retention_models import (  # noqa: E402
    ConsumerDependency,
    PrunePlan,
    PrunePlanStatus,
    PruneReceipt,
    PruneReceiptStatus,
    RecoverySpec,
)

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
class PsqlRetentionRepository:
    """RetentionRepository adapter communicating with Postgres via docker exec psql."""

    def __init__(self, db: dict[str, str]) -> None:
        self.db = db

    def save_dependency(self, dependency: ConsumerDependency) -> None:
        has_table = _scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'consumer_dependencies')",
            **self.db,
        )
        if has_table != "t":
            raise RuntimeError(
                "Table 'consumer_dependencies' does not exist; failing closed to prevent data loss"
            )
        spec = dependency.recovery_spec
        watermark_str = spec.earliest_needed_watermark.isoformat()
        deadline_str = (
            f"'{spec.recovery_deadline.isoformat()}'"
            if spec.recovery_deadline
            else "NULL"
        )
        checkpoint_str = (
            f"'{spec.earliest_checkpoint_id}'"
            if spec.earliest_checkpoint_id
            else "NULL"
        )
        cold_bool = "TRUE" if spec.cold_recovery_supported else "FALSE"
        reason_escaped = spec.reason.replace("'", "''")
        updated_str = dependency.updated_at.isoformat()
        sql = (
            "INSERT INTO consumer_dependencies (consumer_id, dataset_name, generation, "
            "recovery_watermark, earliest_checkpoint_id, recovery_deadline, "
            "cold_recovery_supported, dependency_version, reason, updated_at) "
            f"VALUES ('{dependency.consumer_id}', '{dependency.dataset_name}', {dependency.generation}, "
            f"'{watermark_str}', {checkpoint_str}, {deadline_str}, {cold_bool}, "
            f"'{dependency.dependency_version}', '{reason_escaped}', '{updated_str}') "
            "ON CONFLICT (consumer_id, dataset_name) DO UPDATE SET "
            "generation = EXCLUDED.generation, "
            "recovery_watermark = EXCLUDED.recovery_watermark, "
            "earliest_checkpoint_id = EXCLUDED.earliest_checkpoint_id, "
            "recovery_deadline = EXCLUDED.recovery_deadline, "
            "cold_recovery_supported = EXCLUDED.cold_recovery_supported, "
            "dependency_version = EXCLUDED.dependency_version, "
            "reason = EXCLUDED.reason, "
            "updated_at = EXCLUDED.updated_at"
        )
        _psql(sql, **self.db)

    def delete_dependency(self, consumer_id: str, dataset_name: str) -> None:
        has_table = _scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'consumer_dependencies')",
            **self.db,
        )
        if has_table != "t":
            raise RuntimeError(
                "Table 'consumer_dependencies' does not exist; failing closed to prevent data loss"
            )
        sql = (
            f"DELETE FROM consumer_dependencies WHERE consumer_id = '{consumer_id}' "
            f"AND dataset_name = '{dataset_name}'"
        )
        _psql(sql, **self.db)

    def get_dependencies(self, dataset_name: str) -> tuple[ConsumerDependency, ...]:
        has_table = _scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'consumer_dependencies')",
            **self.db,
        )
        if has_table != "t":
            raise RuntimeError(
                "Table 'consumer_dependencies' does not exist in database. "
                "Refusing to prune operational data (failing closed)."
            )
        output = _psql(
            "SELECT consumer_id, dataset_name, generation, recovery_watermark::text, "
            "coalesce(earliest_checkpoint_id, ''), coalesce(recovery_deadline::text, ''), "
            "cold_recovery_supported::text, dependency_version, reason, updated_at::text "
            f"FROM consumer_dependencies WHERE dataset_name = '{dataset_name}'",
            **self.db,
        )
        deps: list[ConsumerDependency] = []
        for line in output.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split("|")
            if len(parts) < 10:
                raise RuntimeError(
                    f"Malformed consumer dependency record line: {line!r}. "
                    "Refusing to prune operational data (failing closed)."
                )
            (
                cid,
                dname,
                gen,
                wmark,
                chk,
                dline,
                cold,
                dver,
                reason,
                up_at,
            ) = parts[:10]
            try:
                wm_dt = datetime.fromisoformat(wmark).astimezone(UTC)
                dl_dt = datetime.fromisoformat(dline).astimezone(UTC) if dline else None
                up_dt = datetime.fromisoformat(up_at).astimezone(UTC)
            except Exception as parse_err:
                raise RuntimeError(
                    "Failed to parse consumer dependency timestamps from "
                    f"line: {line!r}. Refusing to prune operational data "
                    "(failing closed)."
                ) from parse_err
            deps.append(
                ConsumerDependency(
                    consumer_id=cid,
                    dataset_name=dname,
                    generation=int(gen) if gen.isdigit() else 1,
                    recovery_spec=RecoverySpec(
                        source_dataset=dname,
                        earliest_needed_watermark=wm_dt,
                        earliest_checkpoint_id=chk or None,
                        recovery_deadline=dl_dt,
                        cold_recovery_supported=(cold == "t"),
                        reason=reason,
                    ),
                    dependency_version=dver,
                    updated_at=up_dt,
                )
            )
        return tuple(deps)

    def save_plan(self, plan: PrunePlan) -> None:
        has_table = _scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'prune_plans')",
            **self.db,
        )
        if has_table != "t":
            raise RuntimeError(
                "Table 'prune_plans' does not exist; failing closed to prevent unrecorded data loss"
            )
        binding_str = (
            f"'{plan.binding_consumer_id}'" if plan.binding_consumer_id else "NULL"
        )
        manifest_str = f"'{plan.manifest_hash}'" if plan.manifest_hash else "NULL"
        is_constrained_bool = "TRUE" if plan.is_constrained else "FALSE"
        sql = (
            "INSERT INTO prune_plans (plan_id, dataset_name, requested_cutoff, effective_cutoff, "
            "is_constrained, binding_consumer_id, manifest_hash, expected_dependency_version, "
            "status, rows_archived, rows_deleted, created_at) "
            f"VALUES ('{plan.plan_id}', '{plan.dataset_name}', '{plan.requested_cutoff.isoformat()}', "
            f"'{plan.effective_cutoff.isoformat()}', {is_constrained_bool}, {binding_str}, "
            f"{manifest_str}, '{plan.expected_dependency_version}', '{plan.status.value}', "
            f"0, 0, '{plan.created_at.isoformat()}') "
            "ON CONFLICT (plan_id) DO UPDATE SET status = EXCLUDED.status, manifest_hash = EXCLUDED.manifest_hash"
        )
        _psql(sql, **self.db)

    def update_plan(self, plan: PrunePlan) -> None:
        has_table = _scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'prune_plans')",
            **self.db,
        )
        if has_table != "t":
            raise RuntimeError(
                "Table 'prune_plans' does not exist; failing closed"
            )
        manifest_str = f"'{plan.manifest_hash}'" if plan.manifest_hash else "NULL"
        sql = (
            f"UPDATE prune_plans SET status = '{plan.status.value}', "
            f"manifest_hash = {manifest_str} "
            f"WHERE plan_id = '{plan.plan_id}'"
        )
        _psql(sql, **self.db)

    def save_receipt(self, receipt: PruneReceipt) -> None:
        has_table = _scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'prune_plans')",
            **self.db,
        )
        if has_table != "t":
            raise RuntimeError(
                "Table 'prune_plans' does not exist; failing closed"
            )
        status_val = (
            PrunePlanStatus.COMPLETED.value
            if receipt.status == PruneReceiptStatus.SUCCESS
            else PrunePlanStatus.ABORTED.value
        )
        sql = (
            f"UPDATE prune_plans SET status = '{status_val}', "
            f"rows_archived = {receipt.rows_archived}, "
            f"rows_deleted = {receipt.rows_deleted}, "
            f"executed_at = '{receipt.executed_at.isoformat()}' "
            f"WHERE plan_id = '{receipt.plan_id}'"
        )
        _psql(sql, **self.db)


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
    cutoff_dt = datetime(cutoff.year, cutoff.month, cutoff.day, tzinfo=UTC)
    repo = PsqlRetentionRepository(db)
    authority = RetentionAuthority(repository=repo)
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

        # Protect active positions if table holds balance/position snapshots
        if table in ("account_position_snapshots", "account_balance_snapshots"):
            earliest_pos_raw = _scalar(
                "SELECT min(observed_at)::text FROM account_position_snapshots "
                "WHERE abs(position_amt) > 0",
                **db,
            )
            if earliest_pos_raw:
                try:
                    earliest_pos_dt = datetime.fromisoformat(earliest_pos_raw).astimezone(UTC)
                    authority.register_dependency(
                        consumer_id="live_active_positions",
                        generation=1,
                        recovery_spec=RecoverySpec(
                            source_dataset=table,
                            earliest_needed_watermark=earliest_pos_dt,
                            reason="Active position protection",
                        ),
                    )
                except Exception:
                    pass

        min_watermark = _resolve_consumer_watermark(table, **db)
        if min_watermark is not None:
            authority.register_dependency(
                consumer_id=f"consumer_{table}",
                generation=1,
                recovery_spec=RecoverySpec(
                    source_dataset=table,
                    earliest_needed_watermark=min_watermark,
                    reason="Registered consumer recovery watermark",
                ),
            )

        plan = authority.plan_prune(
            dataset_name=table,
            requested_cutoff=cutoff_dt,
        )

        effective_cutoff = plan.effective_cutoff.date()
        if plan.is_constrained:
            print(
                f"  [CONSTRAINED] {table} cutoff {cutoff} pulled back to "
                f"{effective_cutoff} by active consumer/position dependency ({plan.binding_consumer_id})"
            )

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
        manifest_data = json.loads(manifest.read_text(encoding="utf-8"))
        recorded = int(manifest_data.get("rows", 0))
        from_str = str(manifest_data.get("from", str(oldest)))
        to_str = str(manifest_data.get("to", str(effective_cutoff)))
        if recorded != pending:
            print(
                f"  archived {recorded} rows but {pending} are in range "
                "-- refusing to delete",
                file=sys.stderr,
            )
            return 1
        print(f"  manifest verified: {recorded} rows")

        manifest_hash = hashlib.sha256(manifest.read_bytes()).hexdigest()
        try:
            plan = authority.bind_manifest(
                plan,
                manifest_hash=manifest_hash,
            )
        except Exception as bind_err:
            print(f"  failed to bind manifest to plan: {bind_err}", file=sys.stderr)
            return 1

        def executor(
            p: PrunePlan,
            tbl: str = table,
            col: str = column,
            rec: int = recorded,
            pend: int = pending,
            from_dt: str = from_str,
            to_dt: str = to_str,
        ) -> tuple[int, int]:
            if tbl == "strategy_runtime_events" and _table_is_partitioned(tbl, **db):
                authority.verify_fence(p)
                dropped = _drop_expired_partitions(tbl, p.effective_cutoff.date(), **db)
                print(f"  dropped {dropped} expired partitions")
                return (rec, 0)

            deleted = 0
            while True:
                authority.verify_fence(p)
                removed = int(
                    _scalar(
                        "WITH d AS (DELETE FROM "
                        f"{tbl} WHERE ctid IN (SELECT ctid FROM {tbl} "
                        f'WHERE "{col}" >= \'{from_dt}+00\' '
                        f'AND "{col}" < \'{to_dt}+00\' '
                        f"LIMIT {args.batch_rows}) "
                        "RETURNING 1) SELECT count(*) FROM d",
                        **db,
                    )
                )
                if removed == 0:
                    break
                deleted += removed
                if deleted > rec:
                    raise RuntimeError(
                        f"Deleted {deleted} rows exceeding archived manifest rows "
                        f"({rec})! Aborting prune to prevent unarchived data loss."
                    )
                if deleted % (args.batch_rows * 20) == 0:
                    print(f"  deleted {deleted} / {pend}")
            print(f"  deleted {deleted} rows")
            return (rec, deleted)

        receipt = authority.execute_prune(
            plan=plan,
            expected_dependency_version=plan.expected_dependency_version,
            executor_fn=executor,
        )
        if receipt.status != PruneReceiptStatus.SUCCESS:
            print(f"  prune execution rejected: {receipt.details}", file=sys.stderr)
            return 1

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
