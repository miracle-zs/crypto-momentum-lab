from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.market.models import AggTradeGap
from crypto_momentum_lab.persistence.postgres.models import (
    RuntimeMarketState15sRow,
)
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    PostgresRuntimeMarketStateRepository,
    RuntimeStateCursor,
    RuntimeStateSequenceRange,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)
from tests.unit.persistence.postgres.test_runtime_state_repository import (
    fixture_state,
)


@pytest.fixture
async def runtime_state_repository(
    async_database_url: str,
) -> AsyncIterator[PostgresRuntimeMarketStateRepository]:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        async with session.begin():
            await session.execute(delete(RuntimeMarketState15sRow))
    yield PostgresRuntimeMarketStateRepository(factory)
    await engine.dispose()


async def test_save_closed_states_is_idempotent_and_ordered(
    runtime_state_repository: PostgresRuntimeMarketStateRepository,
) -> None:
    first = fixture_state("ETHUSDT", 0)
    second = fixture_state("BTCUSDT", 0)

    await runtime_state_repository.save_closed_states(
        (first, second),
        source_watermark_at=datetime(2026, 7, 3, 0, 0, 45, tzinfo=UTC),
        sequence_range=RuntimeStateSequenceRange(1, 2),
    )
    await runtime_state_repository.save_closed_states(
        (first, second),
        source_watermark_at=datetime(2026, 7, 3, 0, 0, 45, tzinfo=UTC),
        sequence_range=RuntimeStateSequenceRange(1, 2),
    )

    rows = await runtime_state_repository.load_after(
        environment="research",
        cursor=RuntimeStateCursor(),
        limit=10,
    )

    assert tuple(row.symbol for row in rows) == ("BTCUSDT", "ETHUSDT")


async def test_conflicting_closed_state_fails(
    runtime_state_repository: PostgresRuntimeMarketStateRepository,
) -> None:
    state = fixture_state("BTCUSDT", 0)
    await runtime_state_repository.save_closed_states(
        (state,),
        source_watermark_at=datetime(2026, 7, 3, 0, 0, 45, tzinfo=UTC),
        sequence_range=RuntimeStateSequenceRange(1, 1),
    )

    conflicting = replace(state, close_price=state.close_price + 1)

    with pytest.raises(ValueError, match="runtime market state conflict"):
        await runtime_state_repository.save_closed_states(
            (conflicting,),
            source_watermark_at=datetime(2026, 7, 3, 0, 0, 45, tzinfo=UTC),
            sequence_range=RuntimeStateSequenceRange(1, 1),
        )


async def test_mark_incomplete_invalidates_existing_runtime_state(
    runtime_state_repository: PostgresRuntimeMarketStateRepository,
) -> None:
    state = fixture_state("BTCUSDT", 0)
    await runtime_state_repository.save_closed_states(
        (state,),
        source_watermark_at=datetime(2026, 7, 3, 0, 0, 45, tzinfo=UTC),
        sequence_range=RuntimeStateSequenceRange(1, 3),
    )
    await runtime_state_repository.mark_incomplete(
        AggTradeGap(
            environment="research",
            symbol="BTCUSDT",
            previous_id=10,
            current_id=13,
            previous_event_at=state.bucket_start + timedelta(seconds=1),
            current_event_at=state.bucket_start + timedelta(seconds=2),
            missing_count=2,
            reason="history_incomplete",
        )
    )

    rows = await runtime_state_repository.load_after(
        environment="research",
        cursor=RuntimeStateCursor(),
        limit=10,
    )

    assert rows[0].data_complete is False
    assert rows[0].missing_agg_trade_count == 2
