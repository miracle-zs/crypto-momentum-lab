from collections.abc import AsyncIterator

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

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
    RiskHaltRow,
    ShadowSuppressionEventRow,
    TradingLeaseRow,
)
from crypto_momentum_lab.persistence.postgres.order_plan_repository import (
    PostgresOrderPlanRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)
from tests.e2e.fake_binance_websocket import fake_binance_server

__all__ = ["fake_binance_server", "order_repository"]


@pytest.fixture
async def order_repository(
    async_database_url: str,
) -> AsyncIterator[tuple[PostgresOrderPlanRepository, async_sessionmaker[AsyncSession]]]:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        async with session.begin():
            for model in (
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
                ShadowSuppressionEventRow,
            ):
                await session.execute(delete(model))
    yield PostgresOrderPlanRepository(factory), factory
    await engine.dispose()
