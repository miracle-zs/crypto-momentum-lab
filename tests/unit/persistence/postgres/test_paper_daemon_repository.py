from datetime import UTC, datetime

import pytest

from crypto_momentum_lab.domain.strategy import StrategyCheckpoint
from crypto_momentum_lab.domain.strategy.paper_models import (
    PaperEntryFilterConfig,
    PaperExitConfig,
)
from crypto_momentum_lab.persistence.postgres.models import (
    OrderIntentCandidateRow,
)
from crypto_momentum_lab.persistence.postgres.paper_daemon_repository import (
    candidate_from_row,
    checkpoint_from_row_values,
    paper_live_run_row,
    runtime_event_row,
)
from tests.unit.persistence.postgres.test_strategy_run_repository import (
    fixture_paper_report,
)


def test_checkpoint_from_row_values_restores_checkpoint() -> None:
    checkpoint = checkpoint_from_row_values(
        last_processed_at_by_symbol={"BTCUSDT": "2026-07-04T00:00:15+00:00"},
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


def test_runtime_event_row_preserves_live_phase_fields() -> None:
    occurred_at = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)

    row = runtime_event_row(
        event_id="event-1",
        run_id="live-run-1",
        event_type="exchange_filled",
        occurred_at=occurred_at,
        symbol="BTCUSDT",
        bucket_start=occurred_at,
        details={"lane": "entry", "latency_ms_from_previous": 125.0},
    )

    assert row == {
        "event_id": "event-1",
        "run_id": "live-run-1",
        "event_type": "exchange_filled",
        "occurred_at": occurred_at,
        "symbol": "BTCUSDT",
        "bucket_start": occurred_at,
        "details": {"lane": "entry", "latency_ms_from_previous": 125.0},
    }


def test_paper_live_run_row_initializes_zero_count_summary() -> None:
    report = fixture_paper_report()

    row = paper_live_run_row(
        identity=report.run,
        source_description=report.source_description,
        execution=report.execution_config,
        portfolio=PaperExitConfig(),
        entry_filter=PaperEntryFilterConfig(),
    )

    assert row["run_id"] == report.run.run_id
    assert row["run_mode"] == "paper"
    assert row["signal_count"] == 0
    assert row["candidate_count"] == 0
    assert row["fill_count"] == 0
    assert row["execution_config"]["fills"]["taker_fee_rate"] == "0.0004"
    assert row["execution_config"]["entry_filter"] == {
        "allow_long": True,
        "allow_short": True,
        "max_abs_aggressive_imbalance": None,
        "max_cluster_trade_count": None,
        "require_price_above_ema5": False,
        "require_price_above_ema10": False,
    }
    assert row["execution_config"]["portfolio"]["exit_mode"] == "candle_15m"
    assert row["execution_config"]["portfolio"]["max_holding_buckets"] == 80


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


@pytest.mark.asyncio
async def test_save_checkpoint_commits_checkpoint_before_telemetry_event() -> None:
    from unittest.mock import AsyncMock, MagicMock

    from structlog.testing import capture_logs

    from crypto_momentum_lab.persistence.postgres.paper_daemon_repository import (
        PostgresPaperDaemonRepository,
    )

    session = AsyncMock()
    session.connection = AsyncMock()
    session.commit = AsyncMock()
    executed_statements = []

    async def fake_execute(statement, *args, **kwargs):
        executed_statements.append(statement)
        return MagicMock()

    session.execute = fake_execute

    session_factory = MagicMock()
    session_factory.return_value.__aenter__.return_value = session
    session_factory.return_value.__aexit__.return_value = None

    repo = PostgresPaperDaemonRepository(session_factory)
    run_id = "test-run"
    saved_at = datetime(2026, 9, 22, 0, 0, tzinfo=UTC)
    checkpoint = StrategyCheckpoint(
        last_processed_at_by_symbol={"BTCUSDT": saved_at},
        warmup_buckets_by_symbol={"BTCUSDT": 5},
        cooldown_buckets_remaining_by_symbol={"BTCUSDT": 0},
        payload={"foo": "bar"},
    )

    with capture_logs() as logs:
        await repo.save_checkpoint(run_id, checkpoint, saved_at)

    # session.commit must be called twice:
    # 1. to commit the checkpoint UPSERT
    # 2. to commit the telemetry event
    assert session.commit.await_count == 2
    assert len(executed_statements) == 2

    # Check log fields
    persisted_log = next(
        entry for entry in logs if entry.get("event") == "strategy_checkpoint_persisted"
    )
    assert "commit_ms" in persisted_log
    assert "total_ms" in persisted_log
    assert persisted_log["commit_ms"] >= 0
    assert persisted_log["total_ms"] >= persisted_log["commit_ms"]


@pytest.mark.asyncio
async def test_save_checkpoint_does_not_insert_event_when_commit_fails() -> None:
    from unittest.mock import AsyncMock, MagicMock

    from crypto_momentum_lab.persistence.postgres.paper_daemon_repository import (
        PostgresPaperDaemonRepository,
    )

    session = AsyncMock()
    session.connection = AsyncMock()
    # First commit fails
    session.commit = AsyncMock(side_effect=RuntimeError("disk full"))
    executed_statements = []

    async def fake_execute(statement, *args, **kwargs):
        executed_statements.append(statement)
        return MagicMock()

    session.execute = fake_execute

    session_factory = MagicMock()
    session_factory.return_value.__aenter__.return_value = session
    session_factory.return_value.__aexit__.return_value = None

    repo = PostgresPaperDaemonRepository(session_factory)
    run_id = "test-run"
    saved_at = datetime(2026, 9, 22, 0, 0, tzinfo=UTC)
    checkpoint = StrategyCheckpoint(
        last_processed_at_by_symbol={"BTCUSDT": saved_at},
        warmup_buckets_by_symbol={"BTCUSDT": 5},
        cooldown_buckets_remaining_by_symbol={"BTCUSDT": 0},
        payload={"foo": "bar"},
    )

    with pytest.raises(RuntimeError, match="disk full"):
        await repo.save_checkpoint(run_id, checkpoint, saved_at)

    # Only checkpoint UPSERT was executed; telemetry event was NOT executed
    assert len(executed_statements) == 1
