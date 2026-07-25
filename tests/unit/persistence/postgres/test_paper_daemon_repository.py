from datetime import UTC, datetime

from crypto_momentum_lab.domain.strategy import StrategyCheckpoint
from crypto_momentum_lab.persistence.postgres.models import (
    OrderIntentCandidateRow,
)
from crypto_momentum_lab.persistence.postgres.paper_daemon_repository import (
    candidate_from_row,
    checkpoint_from_row_values,
    paper_live_run_row,
    runtime_event_row,
)
from crypto_momentum_lab.strategy_runner.daemon import StrategyRuntimeEvent
from crypto_momentum_lab.strategy_runner.portfolio import PaperExitConfig
from tests.unit.persistence.postgres.test_strategy_run_repository import (
    fixture_paper_report,
)


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


def test_paper_live_run_row_initializes_zero_count_summary() -> None:
    report = fixture_paper_report()

    row = paper_live_run_row(
        identity=report.run,
        source_description=report.source_description,
        execution=report.execution_config,
        portfolio=PaperExitConfig(),
    )

    assert row["run_id"] == report.run.run_id
    assert row["run_mode"] == "paper"
    assert row["signal_count"] == 0
    assert row["candidate_count"] == 0
    assert row["fill_count"] == 0
    assert row["execution_config"]["fills"]["taker_fee_rate"] == "0.0004"
    assert row["execution_config"]["portfolio"]["take_profit_pct"] == "0.02"


def test_candidate_from_row_restores_pending_candidate() -> None:
    report = fixture_paper_report()
    candidate = report.candidates[0]
    row = OrderIntentCandidateRow(
        candidate_id=candidate.candidate_id,
        signal_id=candidate.signal_id,
        run_id=candidate.run_id,
        strategy_name=candidate.strategy_name,
        strategy_version=candidate.strategy_version,
        config_hash=candidate.config_hash,
        symbol=candidate.symbol,
        side=candidate.side.value,
        entry_type=candidate.entry_type.value,
        limit_price=candidate.limit_price,
        desired_notional=candidate.desired_notional,
        reduce_only=candidate.reduce_only,
        expires_at=candidate.expires_at,
        created_at=candidate.created_at,
        reason=candidate.reason,
        features=candidate.features,
    )

    restored = candidate_from_row(row)

    assert restored == candidate
