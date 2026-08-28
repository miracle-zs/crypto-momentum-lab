import asyncio
from datetime import UTC, datetime

import pytest

from crypto_momentum_lab.domain.strategy import StrategyCheckpoint
from crypto_momentum_lab.live_rollout.checkpoint_writer import CheckpointWriter


def _checkpoint(value: str) -> StrategyCheckpoint:
    return StrategyCheckpoint(
        last_processed_at_by_symbol={
            "BTCUSDT": datetime(2026, 8, 23, 0, 0, tzinfo=UTC)
        },
        warmup_buckets_by_symbol={"BTCUSDT": 7},
        cooldown_buckets_remaining_by_symbol={"BTCUSDT": 0},
        payload={"value": value},
    )


@pytest.mark.asyncio
async def test_checkpoint_writer_coalesces_pending_snapshots() -> None:
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    calls: list[str] = []

    async def persist(_run_id, checkpoint, _saved_at) -> None:
        calls.append(str(checkpoint.payload["value"]))
        if len(calls) == 1:
            first_started.set()
            await release_first.wait()

    writer = CheckpointWriter(
        run_id="run-1",
        persist=persist,
        retry_delay_seconds=0.01,
        flush_timeout_seconds=1,
    )
    await writer.start()
    writer.submit(_checkpoint("first"), datetime.now(tz=UTC))
    await first_started.wait()
    writer.submit(_checkpoint("second"), datetime.now(tz=UTC))
    writer.submit(_checkpoint("latest"), datetime.now(tz=UTC))
    release_first.set()

    assert await writer.flush()
    await writer.stop()

    assert calls == ["first", "latest"]
    assert writer.metrics.coalesced_count == 1


@pytest.mark.asyncio
async def test_checkpoint_writer_retries_periodic_failures() -> None:
    calls = 0

    async def persist(_run_id, _checkpoint, _saved_at) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("checkpoint timeout")

    writer = CheckpointWriter(
        run_id="run-1",
        persist=persist,
        retry_delay_seconds=0.01,
        flush_timeout_seconds=1,
    )
    await writer.start()
    writer.submit(_checkpoint("retry"), datetime.now(tz=UTC))

    assert await writer.flush()
    await writer.stop()

    assert calls == 2
    assert writer.metrics.failure_count == 1
    assert writer.metrics.persisted_count == 1
