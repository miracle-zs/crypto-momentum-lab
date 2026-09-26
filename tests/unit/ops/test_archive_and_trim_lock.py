"""Prove the prune lock covers fence+delete and targets stay frozen.

The old executor took a session advisory lock in a throwaway ``psql -c``, so
the lock died before any delete ran. Deletes also re-queried a mutable time
range per batch. These tests pin the corrected protocol.
"""

from __future__ import annotations

import importlib.util
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT = _ROOT / "deploy" / "ops" / "archive_and_trim.py"


def _load_module() -> Any:
    spec = importlib.util.spec_from_file_location("archive_and_trim", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeSession:
    """Records SQL in order and replays scripted scalar results."""

    def __init__(self, results: dict[str, str] | None = None) -> None:
        self.sql: list[str] = []
        self._results = results or {}

    def run(self, sql: str) -> str:
        self.sql.append(sql)
        for key, value in self._results.items():
            if key in sql:
                return value
        return ""


class FakeAuthority:
    def __init__(self) -> None:
        self.fence_calls = 0

    def verify_fence(self, plan: Any) -> None:
        self.fence_calls += 1


def _plan(mod: Any) -> Any:
    return mod.PrunePlan(
        plan_id="plan_t1",
        dataset_name="demo_table",
        requested_cutoff=datetime(2026, 9, 1, tzinfo=UTC),
        effective_cutoff=datetime(2026, 9, 1, tzinfo=UTC),
        is_constrained=False,
        binding_consumer_id=None,
        manifest_hash="abc",
        expected_dependency_version="dep_v0_empty",
    )


def test_freeze_sql_captures_ctid_set_not_recomputed_range() -> None:
    mod = _load_module()
    sql = mod.build_freeze_targets_sql(
        "demo_table", "occurred_at", "2026-07-25", "2026-08-01"
    )
    assert "CREATE TEMP TABLE prune_targets" in sql
    assert "SELECT ctid AS id FROM demo_table" in sql
    assert "occurred_at" in sql


def test_batch_delete_consumes_frozen_set_only() -> None:
    mod = _load_module()
    sql = mod.build_batch_delete_sql("demo_table", 100)
    assert "FROM prune_targets LIMIT 100" in sql
    assert "DELETE FROM demo_table WHERE ctid IN" in sql
    # Must not re-select by time window inside the batch.
    assert "occurred_at" not in sql
    assert "prune_targets" in sql


def test_lock_held_across_fence_and_delete() -> None:
    mod = _load_module()
    session = FakeSession(
        {"count(*) FROM prune_targets": "3", "count(*) FROM del": "3"}
    )
    authority = FakeAuthority()
    result = mod.run_locked_prune(
        session=session,
        authority=authority,
        plan=_plan(mod),
        table="demo_table",
        column="occurred_at",
        recorded=3,
        pending=3,
        from_dt="2026-07-25",
        to_dt="2026-08-01",
        batch_rows=10,
        db={},
    )
    assert result == (3, 3)
    assert authority.fence_calls >= 1
    first = session.sql[0]
    last = session.sql[-1]
    assert "pg_advisory_lock" in first
    assert "pg_advisory_unlock" in last
    # Fence and delete run between lock and unlock.
    joined = "\n---\n".join(session.sql)
    lock_at = joined.find("pg_advisory_lock")
    unlock_at = joined.find("pg_advisory_unlock")
    freeze_at = joined.find("CREATE TEMP TABLE prune_targets")
    delete_at = joined.find("DELETE FROM demo_table")
    assert lock_at < freeze_at < delete_at < unlock_at


def test_frozen_count_mismatch_aborts_without_delete() -> None:
    mod = _load_module()
    session = FakeSession({"count(*) FROM prune_targets": "5"})
    authority = FakeAuthority()
    with pytest.raises(RuntimeError, match="Frozen prune targets"):
        mod.run_locked_prune(
            session=session,
            authority=authority,
            plan=_plan(mod),
            table="demo_table",
            column="occurred_at",
            recorded=3,
            pending=3,
            from_dt="2026-07-25",
            to_dt="2026-08-01",
            batch_rows=10,
            db={},
        )
    assert authority.fence_calls >= 1
    assert any("pg_advisory_unlock" in s for s in session.sql)
    assert not any("DELETE FROM demo_table" in s for s in session.sql)


def test_deleted_count_mismatch_raises() -> None:
    mod = _load_module()

    class ShortSession(FakeSession):
        def run(self, sql: str) -> str:
            self.sql.append(sql)
            if "count(*) FROM prune_targets" in sql:
                return "3"
            if "count(*) FROM del" in sql:
                # First batch deletes 2, later batches nothing.
                prior = sum(1 for s in self.sql if "count(*) FROM del" in s)
                return "2" if prior == 1 else "0"
            return ""

    with pytest.raises(RuntimeError, match="does not match archived"):
        mod.run_locked_prune(
            session=ShortSession(),
            authority=FakeAuthority(),
            plan=_plan(mod),
            table="demo_table",
            column="occurred_at",
            recorded=3,
            pending=3,
            from_dt="2026-07-25",
            to_dt="2026-08-01",
            batch_rows=10,
            db={},
        )


def test_save_dependency_takes_advisory_xact_lock() -> None:
    mod = _load_module()
    captured: list[str] = []

    def fake_psql(sql: str, **kw: Any) -> str:
        captured.append(sql)
        return "t"

    mod._psql = fake_psql
    mod._scalar = lambda sql, **kw: "t"
    repo = mod.PsqlRetentionRepository(
        {"container": "c", "database": "d", "user": "u"}
    )
    spec = mod.RecoverySpec(
        source_dataset="demo_table",
        earliest_needed_watermark=datetime(2026, 1, 1, tzinfo=UTC),
    )
    dep = mod.ConsumerDependency(
        consumer_id="c1",
        dataset_name="demo_table",
        generation=1,
        recovery_spec=spec,
        dependency_version="dep_v0_empty",
    )
    repo.save_dependency(dep)
    assert captured, "expected save_dependency to issue SQL"
    sql = captured[0]
    assert "pg_advisory_xact_lock" in sql
    assert "retention_demo_table" in sql
    assert sql.strip().startswith("BEGIN;")
    assert sql.strip().endswith("COMMIT;")
