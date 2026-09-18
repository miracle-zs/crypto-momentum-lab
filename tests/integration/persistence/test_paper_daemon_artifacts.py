from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker
from structlog.testing import capture_logs

from crypto_momentum_lab.domain.strategy import (
    StrategyCheckpoint,
    StrategyDecision,
)
from crypto_momentum_lab.persistence.postgres.models import (
    OrderIntentCandidateRow,
    PaperEquitySnapshotRow,
    PaperFillRow,
    PaperPositionRow,
    StrategyRunRow,
    StrategyRuntimeCheckpointRow,
    StrategySignalRow,
)
from crypto_momentum_lab.persistence.postgres.paper_daemon_repository import (
    PostgresPaperDaemonRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
    create_checkpoint_database_engine,
)
from crypto_momentum_lab.persistence.postgres.strategy_run_repository import (
    PostgresStrategyRunRepository,
)
from crypto_momentum_lab.strategy_runner.daemon import PaperEntryFilterConfig
from crypto_momentum_lab.strategy_runner.portfolio import (
    PaperExitConfig,
    PaperPositionStatus,
)
from tests.unit.persistence.postgres.test_strategy_run_repository import (
    fixture_paper_report,
)

TEST_RUN_ID = "integration-paper-daemon-artifacts"


@pytest.fixture
async def paper_artifact_repositories(
    async_database_url: str,
) -> AsyncIterator[tuple[PostgresPaperDaemonRepository, PostgresStrategyRunRepository]]:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        async with session.begin():
            for model in (
                PaperEquitySnapshotRow,
                PaperPositionRow,
                PaperFillRow,
                OrderIntentCandidateRow,
                StrategySignalRow,
                StrategyRuntimeCheckpointRow,
                StrategyRunRow,
            ):
                await session.execute(delete(model).where(model.run_id.in_((TEST_RUN_ID, "paper-test-run"))))
    yield (
        PostgresPaperDaemonRepository(factory),
        PostgresStrategyRunRepository(factory),
    )
    async with factory() as session:
        async with session.begin():
            for model in (
                PaperEquitySnapshotRow,
                PaperPositionRow,
                PaperFillRow,
                OrderIntentCandidateRow,
                StrategySignalRow,
                StrategyRuntimeCheckpointRow,
                StrategyRunRow,
            ):
                await session.execute(delete(model).where(model.run_id.in_((TEST_RUN_ID, "paper-test-run"))))
    await engine.dispose()


async def test_live_paper_artifacts_are_idempotent_and_resume_pending_candidates(
    paper_artifact_repositories: tuple[
        PostgresPaperDaemonRepository,
        PostgresStrategyRunRepository,
    ],
) -> None:
    artifacts, reports = paper_artifact_repositories
    base_report = fixture_paper_report()
    report = replace(
        base_report,
        run=replace(base_report.run, run_id=TEST_RUN_ID),
        signals=(replace(base_report.signals[0], run_id=TEST_RUN_ID),),
        candidates=(replace(base_report.candidates[0], run_id=TEST_RUN_ID),),
    )
    decision = StrategyDecision(
        signals=report.signals,
        candidates=report.candidates,
        rejections=(),
        checkpoint=report.final_checkpoint,
    )

    await artifacts.initialize_run(
        report.run,
        report.source_description,
        report.execution_config,
        PaperExitConfig(),
        PaperEntryFilterConfig(),
    )
    await artifacts.save_decision(decision)
    await artifacts.save_decision(decision)

    assert await artifacts.load_pending_candidates(report.run.run_id) == (
        report.candidates[0],
    )

    opened = await artifacts.save_fills(
        report.run.run_id,
        report.paper_fills,
    )
    await artifacts.save_fills(report.run.run_id, report.paper_fills)
    conflicting_fill = replace(
        report.paper_fills[0],
        fill_price=Decimal("123.456"),
    )
    with pytest.raises(ValueError, match="paper fill conflict"):
        await artifacts.save_fills(report.run.run_id, (conflicting_fill,))

    assert await artifacts.load_pending_candidates(report.run.run_id) == ()
    assert await artifacts.load_open_positions(report.run.run_id) == opened
    assert await artifacts.load_open_position_symbols(
        frozenset({report.run.run_id})
    ) == frozenset({opened[0].symbol})
    assert (
        await artifacts.load_open_position_symbols(frozenset({"another-run"}))
        == frozenset()
    )
    closed_at = opened[0].opened_at + timedelta(minutes=20)
    last_candle_end = closed_at - timedelta(minutes=15)
    closed = replace(
        opened[0],
        status=PaperPositionStatus.CLOSED,
        closed_at=closed_at,
        exit_price=opened[0].entry_price,
        exit_fee=Decimal("0.04"),
        unrealized_pnl=Decimal("0"),
        realized_pnl=Decimal("-0.08"),
        return_pct=Decimal("-0.0008"),
        close_reason="max_holding_period",
        updated_at=closed_at,
        last_candle_end=last_candle_end,
    )
    await artifacts.save_portfolio(
        report.run.run_id,
        (closed,),
        closed_at,
        PaperExitConfig(),
    )
    async with artifacts._session_factory() as session:
        persisted_closed = await session.get(
            PaperPositionRow,
            closed.position_id,
        )
    assert persisted_closed is not None
    assert persisted_closed.last_candle_end == last_candle_end
    assert await artifacts.load_open_positions(report.run.run_id) == ()
    assert (
        await artifacts.load_open_position_symbols(frozenset({report.run.run_id}))
        == frozenset()
    )
    summary = await reports.load_run_summary(report.run.run_id)
    assert summary["fill_count"] == 1
    assert summary["pending_candidate_count"] == 0


async def test_save_checkpoint_records_pool_and_event_loop_diagnostics(
    async_database_url: str,
) -> None:
    engine = create_checkpoint_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    artifacts = PostgresPaperDaemonRepository(factory)

    test_run_id = "test-diag-checkpoint-run"
    now = datetime(2026, 9, 18, 1, 0, tzinfo=UTC)
    checkpoint = StrategyCheckpoint(
        last_processed_at_by_symbol={"BTCUSDT": now},
        warmup_buckets_by_symbol={"BTCUSDT": 10},
        cooldown_buckets_remaining_by_symbol={"BTCUSDT": 0},
        payload={"step": 1},
    )

    try:
        with capture_logs() as logs:
            # First save: initializes and acquires a new connection from pool
            await artifacts.save_checkpoint(test_run_id, checkpoint, now)

            # Second save: reuses already checked-in connection from pool
            next_now = now + timedelta(seconds=15)
            next_checkpoint = replace(checkpoint, payload={"step": 2})
            await artifacts.save_checkpoint(test_run_id, next_checkpoint, next_now)

        checkpoint_logs = [
            entry
            for entry in logs
            if entry.get("event") == "strategy_checkpoint_persisted"
        ]
        assert len(checkpoint_logs) == 2

        first_log, second_log = checkpoint_logs[0], checkpoint_logs[1]

        # Both logs must include event_loop_lag_ms, pool_acquire_ms,
        # and pool diagnostics
        assert isinstance(first_log["event_loop_lag_ms"], float)
        assert first_log["event_loop_lag_ms"] >= 0
        assert isinstance(first_log["pool_acquire_ms"], float)
        assert first_log["pool_acquire_ms"] >= 0
        assert first_log["is_new_connection"] is True
        assert first_log["pool_checked_in"] == 0
        assert first_log["pool_checked_out"] == 0

        assert isinstance(second_log["event_loop_lag_ms"], float)
        assert second_log["event_loop_lag_ms"] >= 0
        assert isinstance(second_log["pool_acquire_ms"], float)
        assert second_log["pool_acquire_ms"] >= 0
        # Second save reuses the connection, so is_new_connection must be False
        assert second_log["is_new_connection"] is False
        assert second_log["pool_checked_in"] >= 1
        assert second_log["pool_checked_out"] == 0

        # Ensure row was written in DB
        async with factory() as session:
            persisted = await session.get(StrategyRuntimeCheckpointRow, test_run_id)
        assert persisted is not None
        assert persisted.saved_at == next_now
        assert persisted.payload == {"step": 2}
    finally:
        async with factory() as session:
            async with session.begin():
                await session.execute(
                    delete(StrategyRuntimeCheckpointRow).where(
                        StrategyRuntimeCheckpointRow.run_id == test_run_id
                    )
                )
        await engine.dispose()
