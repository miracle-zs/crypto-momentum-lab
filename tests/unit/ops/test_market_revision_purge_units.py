"""Keep the online market-revision purge bounded and non-destructive by default."""

from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]


def test_purge_service_uses_a_grace_window_and_a_bounded_batch() -> None:
    service = (_ROOT / "deploy/ops/cml-market-revision-purge.service").read_text()

    assert "--purge-only" in service
    assert "--minimum-revision-age-hours 72" in service
    assert "--max-purge-count 10000" in service
    assert "--batch-size 500" in service
    assert "flock -n -E 0" in service


def test_purge_timer_is_daily_and_persistent() -> None:
    timer = (_ROOT / "deploy/ops/cml-market-revision-purge.timer").read_text()

    assert "OnCalendar=*-*-* 03:35:00 Asia/Shanghai" in timer
    assert "Persistent=true" in timer


def test_schema_migration_declares_the_online_purge_indexes() -> None:
    migration = (
        _ROOT / "alembic/versions/20261007_0050_market_revision_purge_indexes.py"
    ).read_text()

    assert "autocommit_block" in migration
    assert "ix_decision_traces_uncompacted_normal_hold" in migration
    assert "ix_market_revision_refs_noncanonical_published" in migration
