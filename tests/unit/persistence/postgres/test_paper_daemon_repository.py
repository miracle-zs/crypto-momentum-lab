from datetime import UTC, datetime
import pytest

from crypto_momentum_lab.domain.strategy import StrategyCheckpoint
from crypto_momentum_lab.persistence.postgres.models import (
    OrderIntentCandidateRow,
)
from crypto_momentum_lab.persistence.postgres.paper_daemon_repository import (
    _legacy_paper_run_upgrade_values,
    _normalize_paper_run_for_compare,
    candidate_from_row,
    checkpoint_from_row_values,
    paper_live_run_row,
    runtime_event_row,
)
from crypto_momentum_lab.strategy_runner.daemon import PaperEntryFilterConfig
from crypto_momentum_lab.strategy_runner.portfolio import PaperExitConfig
from tests.unit.persistence.postgres.test_strategy_run_repository import (
    fixture_paper_report,
)


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


def test_legacy_paper_run_without_exit_mode_defaults_to_candle_15m_for_compare() -> None:
    legacy = {
        "execution_config": {
            "portfolio": {
                "max_holding_buckets": 80,
            }
        }
    }
    current = {
        "execution_config": {
            "portfolio": {
                "max_holding_buckets": 80,
                "exit_mode": "candle_15m",
            }
        }
    }

    assert _normalize_paper_run_for_compare(legacy) == (
        _normalize_paper_run_for_compare(current)
    )


def test_legacy_unknown_commit_run_can_upgrade_new_execution_flags() -> None:
    actual = {
        "strategy_name": "compression_breakout",
        "strategy_version": "v0",
        "config_hash": "config-hash",
        "run_mode": "paper",
        "code_commit": "unknown",
        "source_description": "postgres-runtime-states:research",
        "execution_config": {
            "fills": {
                "latency_buckets": 1,
                "state_interval_seconds": 15,
                "taker_fee_rate": "0.0004",
                "slippage_bps": "0",
            },
            "portfolio": {
                "exit_mode": "candle_15m",
                "max_holding_buckets": 480,
                "state_interval_seconds": 15,
                "initial_balance": "1000",
            },
        },
    }
    expected = {
        **actual,
        "code_commit": "354faed4ae3b2075353cd921054efdd4a8b55682",
        "execution_config": {
            "fills": {
                **actual["execution_config"]["fills"],
                "require_market_quote": True,
            },
            "entry_filter": {
                "allow_long": True,
                "allow_short": True,
                "max_abs_aggressive_imbalance": None,
                "max_cluster_trade_count": None,
                "require_price_above_ema5": False,
                "require_price_above_ema10": False,
            },
            "portfolio": {
                **actual["execution_config"]["portfolio"],
                "require_executable_quote": True,
                "candle_minimum_holding_buckets": 0,
                "candle_confirmation_count": 1,
                "candle_grace_bars": 0,
                "candle_grace_profit_pct": "0",
            },
        },
    }

    assert _legacy_paper_run_upgrade_values(
        actual=actual,
        expected=expected,
    ) == {
        "code_commit": expected["code_commit"],
        "execution_config": expected["execution_config"],
    }


def test_known_commit_paper_run_can_upgrade_code_commit() -> None:
    actual = {
        "strategy_name": "compression_breakout",
        "strategy_version": "v0",
        "config_hash": "config-hash",
        "run_mode": "paper",
        "code_commit": "old-known-commit",
        "source_description": "postgres-runtime-states:research",
        "execution_config": {},
    }
    expected = {**actual, "code_commit": "new-commit"}

    assert _legacy_paper_run_upgrade_values(
        actual=actual,
        expected=expected,
    ) == {
        "code_commit": "new-commit",
        "execution_config": {},
    }


def test_paper_run_accepts_legacy_volume_hash_alias() -> None:
    actual = {
        "strategy_name": "orderflow_impulse",
        "strategy_version": "v0",
        "config_hash": "seven-dimensional-zero-volume-hash",
        "run_mode": "paper",
        "code_commit": "old-known-commit",
        "source_description": "postgres-runtime-states:research",
        "execution_config": {},
    }
    expected = {
        **actual,
        "config_hash": "six-dimensional-hash",
        "code_commit": "new-commit",
    }

    assert _legacy_paper_run_upgrade_values(
        actual=actual,
        expected=expected,
        compatible_config_hashes=(expected["config_hash"], actual["config_hash"]),
    ) == {
        "code_commit": "new-commit",
        "execution_config": {},
    }

    assert _legacy_paper_run_upgrade_values(
        actual=actual,
        expected=expected,
        compatible_config_hashes=(expected["config_hash"],),
    ) is None


def test_known_commit_paper_run_can_upgrade_candle_exit_fields() -> None:
    actual = {
        "strategy_name": "orderflow_impulse",
        "strategy_version": "v0",
        "config_hash": "config-hash",
        "run_mode": "paper",
        "code_commit": "old-known-commit",
        "source_description": "postgres-runtime-states:research",
        "execution_config": {
            "fills": {"require_market_quote": True},
            "portfolio": {
                "exit_mode": "candle_15m",
                "max_holding_buckets": 5760,
                "require_executable_quote": True,
            },
        },
    }
    expected = {
        **actual,
        "code_commit": "new-commit",
        "execution_config": {
            "fills": {"require_market_quote": True},
            "entry_filter": {
                "allow_long": True,
                "allow_short": True,
                "max_abs_aggressive_imbalance": None,
                "max_cluster_trade_count": None,
                "require_price_above_ema5": False,
                "require_price_above_ema10": False,
            },
            "portfolio": {
                "exit_mode": "candle_15m",
                "max_holding_buckets": 5760,
                "require_executable_quote": True,
                "candle_minimum_holding_buckets": 0,
                "candle_confirmation_count": 1,
                "candle_grace_bars": 0,
                "candle_grace_profit_pct": "0",
            },
        },
    }

    assert _legacy_paper_run_upgrade_values(
        actual=actual,
        expected=expected,
    ) == {
        "code_commit": "new-commit",
        "execution_config": expected["execution_config"],
    }


def test_known_commit_paper_run_rejects_parameter_changes() -> None:
    actual = {
        "strategy_name": "compression_breakout",
        "strategy_version": "v0",
        "config_hash": "config-hash",
        "run_mode": "paper",
        "code_commit": "old-known-commit",
        "source_description": "postgres-runtime-states:research",
        "execution_config": {},
    }
    expected = {
        **actual,
        "code_commit": "new-commit",
        "execution_config": {"fills": {"latency_buckets": 2}},
    }

    assert _legacy_paper_run_upgrade_values(
        actual=actual,
        expected=expected,
    ) is None


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
