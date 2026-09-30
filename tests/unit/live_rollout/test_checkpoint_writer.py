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


@pytest.mark.parametrize("started", [False, True])
async def test_success_notification_waits_for_persistence(started):
    entered = asyncio.Event()
    release = asyncio.Event()
    events = []

    async def persist(run_id, checkpoint, saved_at):
        assert run_id == "run-1"
        assert checkpoint.payload["value"] == "final"
        entered.set()
        await release.wait()
        events.append("persisted")

    writer = CheckpointWriter(
        run_id="run-1",
        persist=persist,
        on_persist_success=lambda: events.append("notified"),
        flush_timeout_seconds=1,
    )
    if started:
        await writer.start()
    try:
        task = asyncio.create_task(
            writer.save_now(_checkpoint("final"), datetime.now(UTC))
        )
        await asyncio.wait_for(entered.wait(), timeout=1)
        assert events == []
        assert writer.metrics.persisted_count == 0
        release.set()
        assert await task
        assert events == ["persisted", "notified"]
        assert writer.metrics.persisted_count == 1
    finally:
        release.set()
        await writer.stop()


@pytest.mark.parametrize("started", [False, True])
@pytest.mark.parametrize("cancelled", [False, True])
async def test_failed_or_cancelled_persistence_does_not_notify(started, cancelled):
    notifications = []

    async def persist(*args):
        if cancelled:
            raise asyncio.CancelledError()
        raise TimeoutError("write failed")

    writer = CheckpointWriter(
        run_id="run-1",
        persist=persist,
        on_persist_success=lambda: notifications.append("ok"),
        flush_timeout_seconds=1,
    )
    if started:
        await writer.start()
    try:
        if cancelled:
            with pytest.raises(asyncio.CancelledError):
                await writer.save_now(_checkpoint("final"), datetime.now(UTC))
        elif started:
            assert (
                await writer.save_now(_checkpoint("final"), datetime.now(UTC)) is False
            )
        else:
            with pytest.raises(TimeoutError):
                await writer.save_now(_checkpoint("final"), datetime.now(UTC))
        assert notifications == []
        assert writer.metrics.persisted_count == 0
        assert writer.last_persisted_token == 0
    finally:
        await writer.stop()


@pytest.mark.parametrize("started", [False, True])
async def test_notification_failure_preserves_critical_write_failure_policy(started):
    events = []

    async def persist(*args):
        events.append("persisted")

    def notify():
        events.append("notified")
        raise RuntimeError("health callback failed")

    writer = CheckpointWriter(
        run_id="run-1", persist=persist, on_persist_success=notify
    )
    if started:
        await writer.start()
    try:
        if started:
            assert (
                await writer.save_now(_checkpoint("final"), datetime.now(UTC)) is False
            )
        else:
            with pytest.raises(RuntimeError, match="health callback failed"):
                await writer.save_now(_checkpoint("final"), datetime.now(UTC))
        assert events == ["persisted", "notified"]
        assert writer.metrics.persisted_count == 0
        assert writer.last_persisted_token == 0
    finally:
        await writer.stop()


async def test_periodic_notification_failure_retries_without_publishing_token():
    events = []
    attempts = 0

    async def persist(*args):
        events.append("persisted")

    def notify():
        nonlocal attempts
        attempts += 1
        events.append("notified")
        assert writer.last_persisted_token == 0
        if attempts == 1:
            raise RuntimeError("health callback failed")

    writer = CheckpointWriter(
        run_id="run-1",
        persist=persist,
        on_persist_success=notify,
        retry_delay_seconds=0.01,
        flush_timeout_seconds=1,
    )
    await writer.start()
    try:
        token = writer.submit(_checkpoint("retry"), datetime.now(UTC))
        assert await writer.flush()
        assert events == ["persisted", "notified", "persisted", "notified"]
        assert writer.metrics.failure_count == 1
        assert writer.metrics.persisted_count == 1
        assert writer.last_persisted_token == token
    finally:
        await writer.stop()
