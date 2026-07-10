from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.live_rollout import (
    LIVE_APPROVAL_CONFIRMATION,
    LiveOperatorApproval,
)
from crypto_momentum_lab.persistence.postgres.live_rollout_repository import (
    PostgresLiveRolloutRepository,
)
from crypto_momentum_lab.persistence.postgres.models import (
    LiveOperatorApprovalRow,
    LiveRollbackCommandRow,
    LiveSessionTransitionRow,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)

NOW = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)


@pytest.fixture
async def live_repository(
    async_database_url: str,
) -> AsyncIterator[PostgresLiveRolloutRepository]:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        async with session.begin():
            for model in (
                LiveRollbackCommandRow,
                LiveSessionTransitionRow,
                LiveOperatorApprovalRow,
            ):
                await session.execute(delete(model))
    yield PostgresLiveRolloutRepository(factory)
    await engine.dispose()


async def test_save_and_load_matching_live_approval(
    live_repository: PostgresLiveRolloutRepository,
) -> None:
    approval = LiveOperatorApproval(
        approval_id="approval-1",
        account_label="primary",
        strategy_name="compression_breakout",
        strategy_config_hash="a" * 64,
        risk_config_hash="b" * 64,
        git_commit_hash="abc123",
        database_migration_revision="20260704_0010",
        approved_notional_cap=Decimal("25"),
        approved_max_open_positions=1,
        approved_max_daily_loss=Decimal("10"),
        approver_name="operator",
        approval_text=LIVE_APPROVAL_CONFIRMATION,
        expires_at=NOW + timedelta(hours=1),
        created_at=NOW,
    )

    await live_repository.save_approval(approval)

    loaded = await live_repository.load_active_approval(
        account_label="primary",
        strategy_name="compression_breakout",
        now=NOW,
    )
    assert loaded == approval
