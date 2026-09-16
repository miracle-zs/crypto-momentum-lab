"""Guard the retention table list used by the daily archive-and-trim job.

These tables have no parquet copy behind them, so a missing entry means
unbounded growth on the live PostgreSQL.  The smoke test only checks shape
and membership -- running the job itself needs a live container.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT = _ROOT / "deploy" / "ops" / "archive_and_trim.py"


def _load_tables() -> tuple[tuple[str, str], ...]:
    spec = importlib.util.spec_from_file_location("archive_and_trim", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.TABLES


def test_archive_and_trim_covers_unbounded_growth_tables() -> None:
    tables = _load_tables()
    names = {name for name, _column in tables}

    # Phase-0 additions: process-state history, quality diagnostics, and the
    # universe parent whose CASCADE drops monitoring_memberships.
    assert "execution_account_process_states" in names
    assert "market_data_quality_events" in names
    assert "universe_snapshots" in names

    # Must keep archiving the high-churn operational set.
    for required in (
        "strategy_runtime_events",
        "universe_entries",
        "account_balance_snapshots",
        "account_position_snapshots",
        "live_strategy_signals",
    ):
        assert required in names


def test_archive_and_trim_entries_are_well_formed() -> None:
    tables = _load_tables()
    assert tables, "TABLES must not be empty"

    seen: set[str] = set()
    for table, column in tables:
        assert table and table.isidentifier(), f"bad table name: {table!r}"
        assert column and column.isidentifier(), f"bad column: {column!r}"
        assert table not in seen, f"duplicate table: {table}"
        seen.add(table)

    # universe_entries must be archived before universe_snapshots is deleted,
    # because snapshot deletion cascades into leftover entries.
    names = [name for name, _ in tables]
    assert names.index("universe_entries") < names.index("universe_snapshots")
