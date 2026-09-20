"""Unit tests for market data operational retention loop and consumer requirements."""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from crypto_momentum_lab.apps.market_data.main import (
    _resolve_market_data_consumer_requirements,
    run_operational_database_retention_loop,
)
from crypto_momentum_lab.domain.operational.retention_contract import (
    RetentionConsumerRequirement,
)


@pytest.mark.asyncio
async def test_resolve_market_data_consumer_requirements_collects_watermarks() -> None:
    session = AsyncMock()
    # First scalar call: StrategyRuntimeCheckpointRow.saved_at
    # Second scalar call: AccountPositionSnapshotRow.observed_at
    checkpoint_time = datetime(2026, 9, 20, 10, 0, tzinfo=UTC)
    position_time = datetime(2026, 9, 20, 9, 30, tzinfo=UTC)
    session.scalar.side_effect = [checkpoint_time, position_time]

    session_ctx = AsyncMock()
    session_ctx.__aenter__.return_value = session
    session_ctx.__aexit__.return_value = None

    factory = MagicMock(return_value=session_ctx)

    requirements = await _resolve_market_data_consumer_requirements(factory)

    assert len(requirements) == 2
    assert requirements[0].consumer_id == "active_strategy_checkpoints"
    assert requirements[0].min_required_watermark == checkpoint_time
    assert requirements[1].consumer_id == "active_position_market_states"
    assert requirements[1].min_required_watermark == position_time


@pytest.mark.asyncio
async def test_resolve_market_data_consumer_requirements_empty_when_no_rows() -> None:
    session = AsyncMock()
    session.scalar.side_effect = [None, None]

    session_ctx = AsyncMock()
    session_ctx.__aenter__.return_value = session
    session_ctx.__aexit__.return_value = None

    factory = MagicMock(return_value=session_ctx)

    requirements = await _resolve_market_data_consumer_requirements(factory)

    assert requirements == ()


@pytest.mark.asyncio
async def test_market_data_retention_loop_fails_closed_when_provider_raises() -> None:
    repository = AsyncMock()
    repository.prune_contract_metadata = AsyncMock(return_value=0)
    repository.prune_runtime_market_states = AsyncMock(return_value=0)
    repository.ensure_strategy_runtime_event_partitions = AsyncMock(return_value=0)

    failing_provider = AsyncMock(
        side_effect=RuntimeError("Database connection dropped")
    )

    loop_task = asyncio.create_task(
        run_operational_database_retention_loop(
            repository=repository,
            interval_seconds=0.01,
            consumer_requirements_provider=failing_provider,
            sleeper=lambda _: asyncio.sleep(0),
        )
    )

    await asyncio.sleep(0.05)
    loop_task.cancel()
    try:
        await loop_task
    except asyncio.CancelledError:
        pass

    assert failing_provider.await_count > 0
    # Prune must be skipped because consumer requirements resolution failed
    repository.prune_contract_metadata.assert_not_awaited()
    repository.prune_runtime_market_states.assert_not_awaited()


@pytest.mark.asyncio
async def test_market_data_retention_loop_passes_consumer_requirements() -> None:
    repository = AsyncMock()
    repository.prune_contract_metadata = AsyncMock(return_value=0)
    repository.prune_runtime_market_states = AsyncMock(return_value=0)
    repository.ensure_strategy_runtime_event_partitions = AsyncMock(return_value=0)

    cutoff = datetime(2026, 9, 20, 11, 0, tzinfo=UTC)
    req = RetentionConsumerRequirement(
        consumer_id="test_consumer",
        min_required_watermark=cutoff,
        reason="test requirement",
    )
    provider = AsyncMock(return_value=(req,))

    loop_task = asyncio.create_task(
        run_operational_database_retention_loop(
            repository=repository,
            interval_seconds=0.01,
            consumer_requirements_provider=provider,
            sleeper=lambda _: asyncio.sleep(0),
        )
    )

    await asyncio.sleep(0.05)
    loop_task.cancel()
    try:
        await loop_task
    except asyncio.CancelledError:
        pass

    assert repository.prune_contract_metadata.await_count > 0
    assert repository.prune_runtime_market_states.await_count > 0
    for call in repository.prune_contract_metadata.await_args_list:
        assert call.kwargs["consumer_requirements"] == (req,)
