from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.account import (
    AccountPositionSnapshot,
    AccountReconciliationRun,
)
from crypto_momentum_lab.persistence.postgres.account_repository import (
    PostgresAccountRepository,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountPositionSnapshotRow,
    AccountReconciliationRunRow,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)

ENVIRONMENT = "test-live-label-discovery"
NOW = datetime(2026, 9, 10, 0, 0, tzinfo=UTC)


@pytest.fixture
async def account_repository(
    async_database_url: str,
) -> AsyncIterator[PostgresAccountRepository]:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        async with session.begin():
            await session.execute(
                delete(AccountPositionSnapshotRow).where(
                    AccountPositionSnapshotRow.environment == ENVIRONMENT
                )
            )
            await session.execute(
                delete(AccountReconciliationRunRow).where(
                    AccountReconciliationRunRow.environment == ENVIRONMENT
                )
            )
    yield PostgresAccountRepository(factory)
    async with factory() as session:
        async with session.begin():
            await session.execute(
                delete(AccountPositionSnapshotRow).where(
                    AccountPositionSnapshotRow.environment == ENVIRONMENT
                )
            )
            await session.execute(
                delete(AccountReconciliationRunRow).where(
                    AccountReconciliationRunRow.environment == ENVIRONMENT
                )
            )
    await engine.dispose()


def _run(
    account_label: str,
    observed_at: datetime,
    *,
    position_count: int,
    status: str = "ready",
) -> AccountReconciliationRun:
    return AccountReconciliationRun(
        reconciliation_id=(
            f"{account_label}:{observed_at.isoformat()}:{status}"
        ),
        environment=ENVIRONMENT,
        account_label=account_label,
        status=status,
        observed_at=observed_at,
        balance_count=0,
        position_count=position_count,
        open_order_count=0,
        fill_count=0,
        mismatch_count=0,
        details={},
    )


def _position(account_label: str, observed_at: datetime) -> AccountPositionSnapshot:
    return AccountPositionSnapshot(
        environment=ENVIRONMENT,
        account_label=account_label,
        symbol="BTCUSDT",
        position_side="BOTH",
        position_amt=Decimal("1"),
        entry_price=Decimal("100"),
        mark_price=Decimal("101"),
        unrealized_pnl=Decimal("1"),
        notional=Decimal("101"),
        leverage=5,
        margin_type="CROSSED",
        observed_at=observed_at,
        raw_payload={},
    )


async def test_load_active_position_account_labels_uses_latest_ready_run(
    account_repository: PostgresAccountRepository,
) -> None:
    stopped_at = NOW
    await account_repository.save_reconciliation_run(
        _run("stopped", stopped_at, position_count=1)
    )
    await account_repository.save_position_snapshot(
        _position("stopped", stopped_at)
    )
    await account_repository.save_reconciliation_run(
        _run(
            "stopped",
            stopped_at + timedelta(minutes=1),
            position_count=0,
        )
    )

    open_at = NOW + timedelta(minutes=2)
    await account_repository.save_reconciliation_run(
        _run("open", open_at, position_count=1)
    )
    await account_repository.save_position_snapshot(_position("open", open_at))

    await account_repository.save_reconciliation_run(
        _run(
            "failed",
            NOW + timedelta(minutes=3),
            position_count=1,
            status="failed",
        )
    )

    assert await account_repository.load_active_position_account_labels(
        environment=ENVIRONMENT
    ) == frozenset({"open"})
