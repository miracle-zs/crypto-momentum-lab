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
    return {f"{key.column.table.name}.{key.column.name}" for key in column.foreign_keys}
