from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.strategy import StrategyDecision
from crypto_momentum_lab.persistence.postgres.models import (
    OrderIntentCandidateRow,
    PaperEquitySnapshotRow,
    PaperFillRow,
    PaperPositionRow,
    StrategyRunRow,
    StrategySignalRow,
)
from crypto_momentum_lab.persistence.postgres.paper_daemon_repository import (
    PostgresPaperDaemonRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)
from crypto_momentum_lab.persistence.postgres.strategy_run_repository import (
    PostgresStrategyRunRepository,
)
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
) -> AsyncIterator[
    tuple[PostgresPaperDaemonRepository, PostgresStrategyRunRepository]
]:
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
                StrategyRunRow,
            ):
                await session.execute(
                    delete(model).where(model.run_id == TEST_RUN_ID)
                )
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
                StrategyRunRow,
            ):
                await session.execute(
                    delete(model).where(model.run_id == TEST_RUN_ID)
                )
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
        signals=(
            replace(base_report.signals[0], run_id=TEST_RUN_ID),
        ),
        candidates=(
            replace(base_report.candidates[0], run_id=TEST_RUN_ID),
        ),
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

    assert await artifacts.load_pending_candidates(report.run.run_id) == ()
    assert await artifacts.load_open_positions(report.run.run_id) == opened
    closed_at = opened[0].opened_at + timedelta(minutes=20)
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
    )
    await artifacts.save_portfolio(
        report.run.run_id,
        (closed,),
        closed_at,
        PaperExitConfig(),
    )
    assert await artifacts.load_open_positions(report.run.run_id) == ()
    summary = await reports.load_run_summary(report.run.run_id)
    assert summary is not None
    assert summary["signal_count"] == 1
    assert summary["candidate_count"] == 1
    assert summary["fill_count"] == 1
    assert summary["pending_candidate_count"] == 0
