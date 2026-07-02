from collections.abc import AsyncIterator
from dataclasses import replace

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.persistence.postgres.models import (
    OrderIntentCandidateRow,
    PaperFillRow,
    StrategyCheckpointRow,
    StrategyRunRow,
    StrategySignalRow,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)
from crypto_momentum_lab.persistence.postgres.strategy_run_repository import (
    PostgresStrategyRunRepository,
)
from tests.unit.persistence.postgres.test_strategy_run_repository import (
    fixture_paper_report,
)


@pytest.fixture
async def strategy_run_repository(
    async_database_url: str,
) -> AsyncIterator[PostgresStrategyRunRepository]:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        async with session.begin():
            for model in (
                PaperFillRow,
                OrderIntentCandidateRow,
                StrategySignalRow,
                StrategyCheckpointRow,
                StrategyRunRow,
            ):
                await session.execute(delete(model))
    yield PostgresStrategyRunRepository(factory)
    await engine.dispose()


async def test_save_paper_report_is_idempotent(
    strategy_run_repository: PostgresStrategyRunRepository,
) -> None:
    report = fixture_paper_report()

    await strategy_run_repository.save_paper_report(report)
    await strategy_run_repository.save_paper_report(report)

    summary = await strategy_run_repository.load_run_summary(report.run.run_id)
    assert summary is not None
    assert summary["run_id"] == report.run.run_id
    assert summary["signal_count"] == 1
    assert summary["candidate_count"] == 1
    assert summary["fill_count"] == 1


async def test_save_paper_report_rejects_conflicting_run(
    strategy_run_repository: PostgresStrategyRunRepository,
) -> None:
    report = fixture_paper_report()
    await strategy_run_repository.save_paper_report(report)

    conflicting = replace(report, source_description="different-source")

    with pytest.raises(ValueError, match="strategy run conflict"):
        await strategy_run_repository.save_paper_report(conflicting)


async def test_load_paper_report_artifacts_orders_records(
    strategy_run_repository: PostgresStrategyRunRepository,
) -> None:
    report = fixture_paper_report()
    await strategy_run_repository.save_paper_report(report)

    artifacts = await strategy_run_repository.load_paper_report_artifacts(
        report.run.run_id
    )

    assert artifacts is not None
    assert artifacts["run"]["run_id"] == report.run.run_id
    assert artifacts["signals"][0]["signal_id"] == report.signals[0].signal_id
    assert (
        artifacts["candidates"][0]["candidate_id"]
        == report.candidates[0].candidate_id
    )
    assert artifacts["paper_fills"][0]["fill_id"] == report.paper_fills[0].fill_id
    assert artifacts["checkpoint"]["run_id"] == report.run.run_id
