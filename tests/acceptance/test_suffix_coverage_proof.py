"""Live account health and recovery share the execution head facts."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.account import AccountFillEvent, AccountPositionSnapshot
from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.observation_models import Applied
from crypto_momentum_lab.operator_dashboard.fact_integrity_queries import (
    load_fact_integrity,
)
from crypto_momentum_lab.persistence.postgres.command_repository import (
    PostgresCommandRepository,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
    AsyncPostgresExecutionUnitOfWork,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountPositionSnapshotRow,
)
from crypto_momentum_lab.persistence.postgres.position_reservation_repository import (
    AsyncPostgresPositionReservationRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)

pytestmark = pytest.mark.contract


def _book(factory: async_sessionmaker, account: str) -> ExecutionBook:
    commands = PostgresCommandRepository(factory)
    reservations = AsyncPostgresPositionReservationRepository(
        factory, strategy_name="head-fact-integrity-test"
    )
    uow = AsyncPostgresExecutionUnitOfWork(
        factory,
        command_repository=commands,
        reservation_repository=reservations,
    )
    return ExecutionBook(
        command_repository=commands,
        reservation_repository=reservations,
        execution_unit_of_work=uow,
    )


@pytest.mark.asyncio
async def test_account_fact_health_and_restart_use_the_same_head(async_database_url):
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = "head-facts-" + uuid4().hex
    at = datetime.now(UTC)
    key = ExecutionScope("live", account, "BTCUSDT")
    fill = AccountFillEvent(
        environment="live",
        account_label=account,
        symbol="BTCUSDT",
        trade_id="trade-1",
        order_id="order-1",
        side="BUY",
        price=Decimal("100"),
        quantity=Decimal("1"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=at,
        raw_payload={"positionSide": "BOTH"},
    )
    snapshot = AccountPositionSnapshot(
        environment="live",
        account_label=account,
        symbol="BTCUSDT",
        position_side="BOTH",
        position_amt=Decimal("1"),
        entry_price=Decimal("100"),
        mark_price=Decimal("100"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("100"),
        leverage=1,
        margin_type="cross",
        observed_at=at,
        raw_payload={"positionSide": "BOTH"},
    )
    try:
        writer = _book(factory, account)
        await writer.restore(account_label=account)
        result = await writer.observe(
            ExecutionEvidence(
                evidence_id="open-position",
                scope=key,
                observed_at=at,
                fill=fill,
                snapshot=snapshot,
                stream_id="account-event-hub",
                stream_epoch="epoch-1",
                sequence=1,
            )
        )
        assert isinstance(result, Applied)

        restarted = _book(factory, account)
        await restarted.restore(account_label=account)
        assert (await restarted.read(key)).total_quantity == Decimal("1")

        integrity = await load_fact_integrity(
            factory,
            account_position_counts={account: 1},
            now=at,
        )
        assert integrity[account].gaps is None
        assert integrity[account].reasons == ("coverage_missing",)

        async with factory() as session:
            snapshot_count = await session.scalar(
                select(func.count())
                .select_from(AccountPositionSnapshotRow)
                .where(AccountPositionSnapshotRow.account_label == account)
            )
        assert snapshot_count == 0
    finally:
        await engine.dispose()
