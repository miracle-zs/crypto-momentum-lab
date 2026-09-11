from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.live_rollout import (
    LIVE_APPROVAL_CONFIRMATION,
    LiveOperatorApproval,
    RollbackCommand,
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


async def test_save_and_load_permanent_unbounded_live_approval(
    live_repository: PostgresLiveRolloutRepository,
) -> None:
    approval = LiveOperatorApproval(
        approval_id="approval-permanent",
        account_label="primary",
        strategy_name="orderflow_impulse",
        strategy_config_hash="a" * 64,
        risk_config_hash="b" * 64,
        git_commit_hash="abc123",
        database_migration_revision="20260814_0016",
        approved_notional_cap=None,
        approved_max_open_positions=None,
        approved_max_daily_loss=None,
        approver_name="operator",
        approval_text=LIVE_APPROVAL_CONFIRMATION,
        expires_at=None,
        created_at=NOW,
    )

    await live_repository.save_approval(approval)

    loaded = await live_repository.load_active_approval(
        account_label="primary",
        strategy_name="orderflow_impulse",
        now=NOW + timedelta(days=3650),
    )
    assert loaded == approval


async def test_live_risk_control_command_claim_is_atomic_and_idempotent(
    live_repository: PostgresLiveRolloutRepository,
) -> None:
    command = RollbackCommand(
        command_id="command-risk-control-1",
        command_type="cancel_all_open_entries",
        requested_by="operator",
        confirmation_text="CANCEL ALL OPEN LIVE ENTRIES",
        requested_at=NOW,
        idempotency_key="cancel-open-entries-1",
        account_label="primary",
        strategy_name="orderflow_impulse",
        session_id="live-primary-v1",
        status="requested",
        completed_at=None,
        failure_reason=None,
    )

    assert await live_repository.save_command(command) is True
    assert await live_repository.save_command(command) is False
    assert await live_repository.load_command(command.command_id) == command

    claimed = await live_repository.claim_command(
        command.command_id,
        account_label=command.account_label,
        strategy_name=command.strategy_name,
        session_id=command.session_id,
    )
    assert claimed is not None
    assert claimed.status == "executing"
    assert (
        await live_repository.claim_command(
            command.command_id,
            account_label=command.account_label,
            strategy_name=command.strategy_name,
            session_id=command.session_id,
        )
        is None
    )

    completed_at = NOW + timedelta(seconds=1)
    assert (
        await live_repository.complete_command(
            command.command_id,
            status="completed",
            completed_at=completed_at,
            failure_reason=None,
        )
        is True
    )
    loaded = await live_repository.load_command_by_idempotency(
        command.idempotency_key
    )
    assert loaded is not None
    assert loaded.status == "completed"
    assert loaded.completed_at == completed_at
    assert (
        await live_repository.complete_command(
            command.command_id,
            status="completed",
            completed_at=completed_at,
            failure_reason=None,
        )
        is False
    )
