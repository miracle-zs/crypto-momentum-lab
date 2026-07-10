from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.risk import (
    RiskDecision,
    RiskEvaluation,
    RiskHalt,
    TradingLease,
    TradingLeaseState,
)
from crypto_momentum_lab.persistence.postgres.models import (
    RiskConfigSnapshotRow,
    RiskEvaluationRow,
    RiskHaltRow,
    RiskRejectionRow,
    StrategyLiveStateRow,
    TradingLeaseRow,
)
from crypto_momentum_lab.persistence.postgres.risk_repository import (
    PostgresRiskRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)

NOW = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)


@pytest.fixture
async def risk_repository(
    async_database_url: str,
) -> AsyncIterator[tuple[PostgresRiskRepository, async_sessionmaker]]:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        async with session.begin():
            for model in (
                RiskRejectionRow,
                RiskEvaluationRow,
                StrategyLiveStateRow,
                RiskHaltRow,
                RiskConfigSnapshotRow,
                TradingLeaseRow,
            ):
                await session.execute(delete(model))
    yield PostgresRiskRepository(factory), factory
    await engine.dispose()


async def test_acquire_lease_persists_active_lease(
    risk_repository: tuple[PostgresRiskRepository, async_sessionmaker],
) -> None:
    repository, _ = risk_repository
    lease = _lease("lease-1", "worker-1")

    await repository.acquire_lease(lease)

    loaded = await repository.load_active_lease("live", "primary", NOW)
    assert loaded == lease


async def test_save_risk_evaluation_preserves_rejection_reason(
    risk_repository: tuple[PostgresRiskRepository, async_sessionmaker],
) -> None:
    repository, factory = risk_repository
    evaluation = RiskEvaluation(
        evaluation_id="evaluation-1",
        candidate_id="candidate-1",
        decision=RiskDecision.REJECTED,
        reason="max_order_notional_exceeded",
        evaluated_at=NOW,
        details={"desired_notional": "125.50", "limit": "100.00"},
    )

    await repository.save_risk_evaluation(evaluation)

    async with factory() as session:
        stored = await session.scalar(
            select(RiskEvaluationRow).where(
                RiskEvaluationRow.evaluation_id == evaluation.evaluation_id
            )
        )
        rejection = await session.scalar(
            select(RiskRejectionRow).where(
                RiskRejectionRow.evaluation_id == evaluation.evaluation_id
            )
        )
    assert stored is not None
    assert stored.reason == "max_order_notional_exceeded"
    assert stored.details["desired_notional"] == "125.50"
    assert rejection is not None
    assert rejection.reason == "max_order_notional_exceeded"


async def test_load_active_halt_returns_account_halt(
    risk_repository: tuple[PostgresRiskRepository, async_sessionmaker],
) -> None:
    repository, _ = risk_repository
    halt = RiskHalt(
        halt_id="halt-1",
        environment="live",
        account_label="primary",
        reason="operator_stop",
        active=True,
        created_at=NOW,
        details={"source": "integration-test"},
    )

    await repository.save_halt(halt)

    assert await repository.load_active_halts("live", "primary") == (halt,)


def _lease(lease_id: str, owner: str) -> TradingLease:
    return TradingLease(
        lease_id=lease_id,
        environment="live",
        account_label="primary",
        strategy_name="compression_breakout",
        owner=owner,
        state=TradingLeaseState.ACTIVE,
        acquired_at=NOW,
        expires_at=NOW + timedelta(minutes=1),
    )
