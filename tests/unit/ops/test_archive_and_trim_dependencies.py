from __future__ import annotations

import importlib.util
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from crypto_momentum_lab.domain.operational.retention_authority import (
    InMemoryRetentionRepository,
    RetentionAuthority,
)
from crypto_momentum_lab.domain.operational.retention_models import RecoverySpec

_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT = _ROOT / "deploy" / "ops" / "archive_and_trim.py"


def _load_module() -> Any:
    spec = importlib.util.spec_from_file_location("archive_and_trim", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeAuthority:
    def __init__(self) -> None:
        self.registered: list[dict[str, Any]] = []
        self.retired: list[dict[str, Any]] = []

    def register_dependency(self, **kwargs: Any) -> None:
        self.registered.append(kwargs)

    def unregister_dependency(self, **kwargs: Any) -> None:
        self.retired.append(kwargs)


def _register(
    authority: RetentionAuthority,
    *,
    consumer_id: str,
    dataset_name: str,
    watermark: datetime,
) -> None:
    authority.register_dependency(
        consumer_id=consumer_id,
        generation=1,
        recovery_spec=RecoverySpec(
            source_dataset=dataset_name,
            earliest_needed_watermark=watermark,
            reason="test dependency",
        ),
    )


def test_position_watermark_uses_current_nextgen_batches() -> None:
    sql = _load_module().build_active_position_watermark_sql()

    assert "SELECT min(entry_bar_time)::text FROM live_position_batches" in sql
    assert "WHERE environment = 'live'" in sql
    assert "status IN ('ACTIVE', 'EXITING')" in sql
    assert "quantity > 0" in sql
    assert "account_position_snapshots" not in sql


def test_active_position_refreshes_one_dependency_and_retires_old_mirrors() -> None:
    mod = _load_module()
    authority = RetentionAuthority(InMemoryRetentionRepository())
    old_watermark = datetime(2026, 10, 10, tzinfo=UTC)
    watermark = datetime(2026, 10, 20, 12, 30, tzinfo=UTC)
    _register(
        authority,
        consumer_id="live_active_positions",
        dataset_name="account_balance_snapshots",
        watermark=old_watermark,
    )
    _register(
        authority,
        consumer_id="consumer_account_balance_snapshots",
        dataset_name="account_balance_snapshots",
        watermark=old_watermark,
    )
    _register(
        authority,
        consumer_id="consumer_account_position_snapshots",
        dataset_name="account_position_snapshots",
        watermark=old_watermark,
    )

    mod.sync_active_position_dependencies(authority, watermark, dry_run=False)

    for table in ("account_balance_snapshots", "account_position_snapshots"):
        plan = authority.plan_prune(
            dataset_name=table,
            requested_cutoff=datetime(2026, 10, 22, tzinfo=UTC),
        )
        assert plan.is_constrained
        assert plan.effective_cutoff == watermark


def test_flat_account_retires_position_dependencies() -> None:
    authority = RetentionAuthority(InMemoryRetentionRepository())
    old_watermark = datetime(2026, 10, 10, tzinfo=UTC)
    for table in ("account_balance_snapshots", "account_position_snapshots"):
        _register(
            authority,
            consumer_id="live_active_positions",
            dataset_name=table,
            watermark=old_watermark,
        )
        _register(
            authority,
            consumer_id=f"consumer_{table}",
            dataset_name=table,
            watermark=old_watermark,
        )

    _load_module().sync_active_position_dependencies(
        authority,
        None,
        dry_run=False,
    )

    for table in ("account_balance_snapshots", "account_position_snapshots"):
        plan = authority.plan_prune(
            dataset_name=table,
            requested_cutoff=datetime(2026, 10, 22, tzinfo=UTC),
        )
        assert not plan.is_constrained


def test_dry_run_does_not_change_position_dependencies() -> None:
    authority = FakeAuthority()

    _load_module().sync_active_position_dependencies(
        authority,
        datetime(2026, 10, 20, 12, 30, tzinfo=UTC),
        dry_run=True,
    )

    assert not authority.registered
    assert not authority.retired
