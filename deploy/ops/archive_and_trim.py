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
import threading
from collections.abc import Callable
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


class PsqlSession:
    """One long-lived psql session.

    Session-level advisory locks taken via :meth:`run` stay held until the
    session ends, which is what lets fence checks and deletes share a lock.
    Each ``_psql`` call is a fresh process and cannot do that: the lock is
    released the moment that process exits.
    """

    def __init__(self, *, container: str, database: str, user: str) -> None:
        self._container = container
        self._database = database
        self._user = user
        self._proc: subprocess.Popen[str] | None = None
        self._stderr_lines: list[str] = []
        self._seq = 0

    def __enter__(self) -> PsqlSession:
        self._proc = subprocess.Popen(  # noqa: S603 - argv built here
            [
                "docker",
                "exec",
                "-i",
                self._container,
                "psql",
                "-U",
                self._user,
                "-d",
                self._database,
                "-At",
                "-X",
                "-q",
                "-v",
                "ON_ERROR_STOP=1",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        threading.Thread(target=self._drain_stderr, daemon=True).start()
        return self

    def _drain_stderr(self) -> None:
        proc = self._proc
        if proc is None or proc.stderr is None:
            return
        for line in proc.stderr:
            self._stderr_lines.append(line)

    def run(self, sql: str) -> str:
        proc = self._proc
        if proc is None or proc.stdin is None or proc.stdout is None:
            raise RuntimeError("PsqlSession is not started")
        if proc.poll() is not None:
            raise RuntimeError(
                "psql session already exited: " + "".join(self._stderr_lines)
            )
        self._seq += 1
        marker = f"__cml_done_{self._seq}__"
        proc.stdin.write(sql.rstrip() + "\n")
        proc.stdin.write(f"SELECT '{marker}';\n")
        proc.stdin.flush()
        out: list[str] = []
        for line in proc.stdout:
            if line.rstrip("\n") == marker:
                return "".join(out)
            out.append(line)
        raise RuntimeError(
            "psql session ended before marker: " + "".join(self._stderr_lines)
        )

    def __exit__(self, *exc: object) -> None:
        proc = self._proc
        if proc is None:
            return
        try:
            if proc.stdin is not None:
                proc.stdin.close()
        except Exception:
            pass
        try:
            proc.wait(timeout=30)
        except Exception:
            proc.kill()


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
        lock_key = f"retention_{dependency.dataset_name}"
        sql = (
            "BEGIN;\n"
            f"SELECT pg_advisory_xact_lock(hashtext('{lock_key}'));\n"
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
            "updated_at = EXCLUDED.updated_at;\n"
            "COMMIT;"
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
        lock_key = f"retention_{dataset_name}"
        sql = (
            "BEGIN;\n"
            f"SELECT pg_advisory_xact_lock(hashtext('{lock_key}'));\n"
            f"DELETE FROM consumer_dependencies WHERE consumer_id = '{consumer_id}' "
            f"AND dataset_name = '{dataset_name}';\n"
            "COMMIT;"
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


def build_freeze_targets_sql(
    table: str,
    column: str,
    from_dt: str,
    to_dt: str,
) -> str:
    """SQL that freezes the exact rows a prune will delete.

    The set is materialized once under the session lock; later batches
    consume only this set instead of re-querying a mutable time range.
    """
    return (
        "CREATE TEMP TABLE prune_targets AS\n"
        f"SELECT ctid AS id FROM {table}\n"
        f"WHERE \"{column}\" >= '{from_dt}+00'\n"
        f"AND \"{column}\" < '{to_dt}+00';"
    )


def build_freeze_fingerprint_sql() -> str:
    """Content fingerprint of the frozen delete set (ctid identity)."""
    return (
        "SELECT count(*)::text || '|' || "
        "coalesce(md5(string_agg(id::text, ',' ORDER BY id)), '') "
        "FROM prune_targets;"
    )


def build_batch_delete_sql(table: str, batch_limit: int) -> str:
    """SQL that deletes one batch of already-frozen target ctids."""
    return (
        "WITH picked AS (\n"
        f"  SELECT id FROM prune_targets LIMIT {batch_limit}\n"
        "), del AS (\n"
        f"  DELETE FROM {table} WHERE ctid IN (\n"
        "    SELECT id FROM picked\n"
        "  ) RETURNING 1\n"
        "), cleaned AS (\n"
        "  DELETE FROM prune_targets WHERE id IN (\n"
        "    SELECT id FROM picked\n"
        "  )\n"
        ")\n"
        "SELECT count(*) FROM del;"
    )


def run_locked_prune(
    *,
    session: PsqlSession,
    authority: RetentionAuthority,
    plan: PrunePlan,
    table: str,
    column: str,
    recorded: int,
    pending: int,
    from_dt: str,
    to_dt: str,
    batch_rows: int,
    db: dict[str, str],
    is_partitioned_table: bool = False,
) -> tuple[int, int]:
    """Holds one session lock across fence checks and every delete batch.

    Deletion targets are frozen into ``prune_targets`` first; a count
    mismatch with the archive manifest aborts before any row is removed.
    """
    lock_key = f"retention_{plan.dataset_name}"
    session.run(f"SELECT pg_advisory_lock(hashtext('{lock_key}'));")
    try:
        authority.verify_fence(plan)
        if is_partitioned_table:
            dropped = _drop_expired_partitions(
                table,
                plan.effective_cutoff.date(),
                run=session.run,
            )
            # Dropping partitions must be accounted for; returning 0 hides
            # the fact that data was removed.
            print(f"  dropped {dropped} expired partitions")
            # Dropping expired partitions removes all recorded archived rows.
            rows_deleted = recorded if dropped > 0 else 0
            return (recorded, rows_deleted)

        session.run(
            build_freeze_targets_sql(table, column, from_dt, to_dt)
        )
        frozen = int(
            session.run("SELECT count(*) FROM prune_targets;").strip() or "0"
        )
        if frozen != recorded:
            raise RuntimeError(
                f"Frozen prune targets ({frozen}) do not match archived "
                f"manifest rows ({recorded}). Aborting so rows outside "
                "the archive are never deleted."
            )
        fingerprint = session.run(build_freeze_fingerprint_sql()).strip()
        print(f"  frozen fingerprint {fingerprint}")

        deleted = 0
        while deleted < recorded:
            batch_limit = min(batch_rows, recorded - deleted)
            if batch_limit <= 0:
                break
            authority.verify_fence(plan)
            removed = int(
                session.run(build_batch_delete_sql(table, batch_limit))
                .strip()
                or "0"
            )
            if removed == 0:
                break
            deleted += removed
            if batch_rows and deleted % (batch_rows * 20) == 0:
                print(f"  deleted {deleted} / {pending}")

        if deleted != recorded:
            raise RuntimeError(
                f"Deleted {deleted} rows does not match archived "
                f"manifest rows ({recorded})! Aborting prune to prevent "
                "unarchived data loss."
            )
        leftover = int(
            session.run(
                "SELECT count(*) FROM prune_targets;"
            ).strip()
            or "0"
        )
        if leftover != 0:
            raise RuntimeError(
                f"{leftover} frozen targets remain undeleted; aborting."
            )
        print(f"  deleted {deleted} rows (fingerprint {fingerprint})")
        return (recorded, deleted)
    finally:
        session.run(f"SELECT pg_advisory_unlock(hashtext('{lock_key}'));")


def _drop_expired_partitions(
    table: str,
    cutoff: date,
    *,
    run: Callable[[str], str] | None = None,
    **kw: str,
) -> int:
    """Drop day partitions whose upper bound is on or before ``cutoff``.

    Only the strategy-runtime-event table uses this path today.  Identifiers
    come from the fixed prefix plus a YYYYMMDD token parsed out of the catalog.
    Pass ``run`` to execute every statement on an already-locked session so
    the drop set stays under the same advisory lock as the prune.
    """

    def _exec(sql: str) -> str:
        if run is not None:
            return run(sql)
        return _psql(sql, **kw)

    rows = _exec(
        "SELECT child.relname FROM pg_inherits i "
        "JOIN pg_class parent ON parent.oid = i.inhparent "
        "JOIN pg_class child ON child.oid = i.inhrelid "
        "WHERE parent.oid = to_regclass('" + table + "') "
        "AND child.relispartition ORDER BY child.relname"
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
            _exec(f'DROP TABLE IF EXISTS "{name}"')
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

        # Verify archive artifact existence and sha256 digest
        archive_name = manifest_data.get("file")
        if not archive_name:
            print(
                f"  manifest {manifest} missing 'file' field -- refusing to delete",
                file=sys.stderr,
            )
            return 1
        archive_file = manifest.parent / str(archive_name)
        if not archive_file.exists():
            print(
                f"  archive file {archive_file} does not exist -- refusing to delete",
                file=sys.stderr,
            )
            return 1

        hasher = hashlib.sha256()
        with archive_file.open("rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        actual_archive_hash = hasher.hexdigest().lower()
        expected_archive_hash = str(manifest_data.get("sha256", "")).strip().lower()

        if actual_archive_hash != expected_archive_hash:
            print(
                f"  archive file {archive_file} sha256 mismatch: "
                f"expected {expected_archive_hash}, got {actual_archive_hash} "
                "-- refusing to delete",
                file=sys.stderr,
            )
            return 1
        print(f"  archive artifact verified: {archive_file.name} (sha256={actual_archive_hash[:16]}...)")

        try:
            plan = authority.bind_manifest(
                plan,
                manifest_hash=actual_archive_hash,
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
            is_part = (
                tbl == "strategy_runtime_events"
                and _table_is_partitioned(tbl, **db)
            )
            with PsqlSession(**db) as session:
                return run_locked_prune(
                    session=session,
                    authority=authority,
                    plan=p,
                    table=tbl,
                    column=col,
                    recorded=rec,
                    pending=pend,
                    from_dt=from_dt,
                    to_dt=to_dt,
                    batch_rows=args.batch_rows,
                    db=db,
                    is_partitioned_table=is_part,
                )

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
