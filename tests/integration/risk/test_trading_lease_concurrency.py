import asyncio
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.risk import TradingLease, TradingLeaseState
from crypto_momentum_lab.persistence.postgres.models import TradingLeaseRow
from crypto_momentum_lab.persistence.postgres.risk_repository import (
    LeaseAlreadyHeldError,
    PostgresRiskRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)


async def test_parallel_acquisition_allows_exactly_one_active_lease(
    async_database_url: str,
) -> None:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        async with session.begin():
            await session.execute(delete(TradingLeaseRow))
    repository = PostgresRiskRepository(factory)
    acquired_at = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
    leases = (
        _lease("lease-1", "worker-1", acquired_at),
        _lease("lease-2", "worker-2", acquired_at),
    )

    results = await asyncio.gather(
        *(repository.acquire_lease(lease) for lease in leases),
        return_exceptions=True,
    )

    assert sum(result is None for result in results) == 1
    assert sum(isinstance(result, LeaseAlreadyHeldError) for result in results) == 1
    loaded = await repository.load_active_lease("live", "primary", acquired_at)
    assert loaded is not None
    assert loaded.owner in {"worker-1", "worker-2"}
    await engine.dispose()


def _lease(
    lease_id: str,
    owner: str,
    acquired_at: datetime,
) -> TradingLease:
    return TradingLease(
        lease_id=lease_id,
        environment="live",
        account_label="primary",
        strategy_name="compression_breakout",
        owner=owner,
        state=TradingLeaseState.ACTIVE,
        acquired_at=acquired_at,
        expires_at=acquired_at + timedelta(minutes=1),
    )
