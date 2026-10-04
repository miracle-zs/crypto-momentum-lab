from collections.abc import AsyncIterator

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
    ExecutionEvidenceReceiptRow,
    ExecutionOrderWatermarkRow,
    ExecutionTradeIdentityRow,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ExchangeFillRow,
    ExchangeOrderEventRow,
    ExchangeOrderRow,
    ExecutionCommandRow,
    ExecutionReconciliationEventRow,
    ExitEpisodeReservationRow,
    LiveExposureClaimRow,
    LiveSessionTransitionRow,
    OrderIntentClaimRow,
    OrderIntentExecutionRow,
    PositionReservationRow,
    RiskHaltRow,
    TradingLeaseRow,
)
from crypto_momentum_lab.persistence.postgres.position_fact_journal_models import (
    PositionFactJournalEventRow,
    PositionRecoveryCheckpointRow,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)
from tests.e2e.fake_binance_websocket import fake_binance_server
from tests.fixtures.order_rows import (
    OrderRows,
)

__all__ = ["fake_binance_server", "order_repository"]


@pytest.fixture
async def order_repository(
    async_database_url: str,
) -> AsyncIterator[
    tuple[OrderRows, async_sessionmaker[AsyncSession]]
]:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        async with session.begin():
            for model in (
                PositionReservationRow,
                ExecutionBookHeadRow,
                ExecutionEvidenceReceiptRow,
                ExecutionTradeIdentityRow,
                ExecutionOrderWatermarkRow,
                PositionFactJournalEventRow,
                PositionRecoveryCheckpointRow,
                ExchangeFillRow,
                ExchangeOrderEventRow,
                ExchangeOrderRow,
                ExitEpisodeReservationRow,
                LiveExposureClaimRow,
                OrderIntentClaimRow,
                OrderIntentExecutionRow,
                LiveSessionTransitionRow,
                TradingLeaseRow,
                ExecutionCommandRow,
                ExecutionReconciliationEventRow,
                RiskHaltRow,
            ):
                await session.execute(delete(model))
    yield OrderRows(factory), factory
    await engine.dispose()
