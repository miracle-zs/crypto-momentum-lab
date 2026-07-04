from datetime import UTC, datetime

from crypto_momentum_lab.domain.strategy import StrategyCheckpoint
from crypto_momentum_lab.persistence.postgres.paper_daemon_repository import (
    checkpoint_from_row_values,
    runtime_event_row,
)
from crypto_momentum_lab.strategy_runner.daemon import StrategyRuntimeEvent


def test_runtime_event_row_preserves_details() -> None:
    event = StrategyRuntimeEvent(
        event_id="event-1",
        run_id="run-1",
        event_type="checkpoint_saved",
        occurred_at=datetime(2026, 7, 4, 0, 1, tzinfo=UTC),
        symbol="BTCUSDT",
        bucket_start=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        details={"state_count": 10},
    )

    row = runtime_event_row(event)

    assert row == {
        "event_id": "event-1",
        "run_id": "run-1",
        "event_type": "checkpoint_saved",
        "occurred_at": datetime(2026, 7, 4, 0, 1, tzinfo=UTC),
        "symbol": "BTCUSDT",
        "bucket_start": datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        "details": {"state_count": 10},
    }


def test_checkpoint_from_row_values_restores_checkpoint() -> None:
    checkpoint = checkpoint_from_row_values(
        last_processed_at_by_symbol={
            "BTCUSDT": "2026-07-04T00:00:15+00:00"
        },
        warmup_buckets_by_symbol={"BTCUSDT": 3},
        cooldown_buckets_remaining_by_symbol={"BTCUSDT": 0},
        payload={"latest_signal": "sig-1"},
    )

    assert checkpoint == StrategyCheckpoint(
        last_processed_at_by_symbol={
            "BTCUSDT": datetime(2026, 7, 4, 0, 0, 15, tzinfo=UTC)
        },
        warmup_buckets_by_symbol={"BTCUSDT": 3},
        cooldown_buckets_remaining_by_symbol={"BTCUSDT": 0},
        payload={"latest_signal": "sig-1"},
    )
