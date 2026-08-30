from crypto_momentum_lab.persistence.postgres import models  # noqa: F401
from crypto_momentum_lab.persistence.postgres.base import Base


def test_strategy_run_tables_are_registered_in_metadata() -> None:
    assert {
        "strategy_runs",
        "strategy_signals",
        "order_intent_candidates",
        "paper_fills",
        "strategy_checkpoints",
    } <= set(Base.metadata.tables)


def test_live_signal_table_has_account_time_read_index() -> None:
    live_signals = Base.metadata.tables["live_strategy_signals"]

    assert any(
        index.name == "ix_live_strategy_signals_account_time"
        and [column.name for column in index.columns]
        == ["account_label", "detected_at", "recorded_at"]
        for index in live_signals.indexes
    )


def test_account_balance_table_has_descending_latest_index() -> None:
    balances = Base.metadata.tables["account_balance_snapshots"]

    assert any(
        index.name == "ix_account_balance_latest_desc"
        and [column.name for column in index.columns]
        == ["environment", "account_label", "asset", "observed_at"]
        and str(index.expressions[-1]).endswith("observed_at DESC")
        for index in balances.indexes
    )
    assert all(
        index.name != "ix_account_balance_latest" for index in balances.indexes
    )
    assert any(
        index.name == "ix_account_balance_asset_hour_observed"
        for index in balances.indexes
    )


def test_strategy_run_relationships_are_declared() -> None:
    strategy_signals = Base.metadata.tables["strategy_signals"]
    candidates = Base.metadata.tables["order_intent_candidates"]
    paper_fills = Base.metadata.tables["paper_fills"]
    checkpoints = Base.metadata.tables["strategy_checkpoints"]

    assert _foreign_key_targets(strategy_signals, "run_id") == {
        "strategy_runs.run_id"
    }
    assert _foreign_key_targets(candidates, "run_id") == {
        "strategy_runs.run_id"
    }
    assert _foreign_key_targets(candidates, "signal_id") == {
        "strategy_signals.signal_id"
    }
    assert _foreign_key_targets(paper_fills, "run_id") == {
        "strategy_runs.run_id"
    }
    assert _foreign_key_targets(paper_fills, "candidate_id") == {
        "order_intent_candidates.candidate_id"
    }
    assert _foreign_key_targets(paper_fills, "signal_id") == {
        "strategy_signals.signal_id"
    }
    assert _foreign_key_targets(checkpoints, "run_id") == {
        "strategy_runs.run_id"
    }


def _foreign_key_targets(table: object, column_name: str) -> set[str]:
    column = table.c[column_name]
    return {
        f"{key.column.table.name}.{key.column.name}"
        for key in column.foreign_keys
    }
