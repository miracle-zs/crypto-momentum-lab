import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from crypto_momentum_lab.domain.market.models import (
    ArchiveManifest,
    CaptureRoute,
    CaptureStream,
    MarketDataState,
)
from crypto_momentum_lab.persistence.raw_files.journal import (
    PendingManifestJournal,
    PendingProcessState,
    PendingProcessStateJournal,
)

fixture_now = datetime(2026, 6, 15, 2, 10, tzinfo=UTC)


async def test_background_manifest_failure_survives_shutdown_and_restart(tmp_path):
    directory = tmp_path / "pending"
    failed = asyncio.Event()
    manifest = _manifest(UUID(int=1), "a" * 64)

    async def unavailable(item):
        failed.set()
        raise RuntimeError("database unavailable")

    async with PendingManifestJournal(directory).background_sink(unavailable) as sink:
        await sink(manifest)
        await asyncio.wait_for(failed.wait(), 1)
    assert len(list(directory.glob("*.json"))) == 1
    saved = []

    async def save(item):
        saved.append(item)

    assert await PendingManifestJournal(directory).replay(save) == 1
    assert saved == [manifest]
    assert list(directory.glob("*.json")) == []


async def test_slow_manifest_database_does_not_backpressure_producer(tmp_path):
    directory = tmp_path / "pending"
    saving = asyncio.Event()
    release = asyncio.Event()
    saved = []

    async def save(item):
        saving.set()
        await release.wait()
        saved.append(item)

    manifests = [
        replace(
            _manifest(UUID(int=index), "a" * 64),
            relative_path=Path(f"{index}.jsonl.zst"),
        )
        for index in range(1, 21)
    ]
    async with PendingManifestJournal(directory).background_sink(save) as sink:
        await sink(manifests[0])
        await asyncio.wait_for(saving.wait(), 1)
        for manifest in manifests[1:]:
            await asyncio.wait_for(sink(manifest), 1)
        assert saved == []
        assert len(list(directory.glob("*.json"))) == 20
        release.set()
    assert saved == manifests
    assert list(directory.glob("*.json")) == []


async def test_manifest_shutdown_timeout_leaves_durable_work_and_no_worker(tmp_path):
    saving = asyncio.Event()
    canceled = asyncio.Event()
    directory = tmp_path / "pending"

    async def save(item):
        saving.set()
        try:
            await asyncio.Event().wait()
        finally:
            canceled.set()

    async with PendingManifestJournal(directory).background_sink(
        save, shutdown_seconds=0.02
    ) as sink:
        await sink(_manifest(UUID(int=1), "a" * 64))
        await asyncio.wait_for(saving.wait(), 1)
    assert canceled.is_set()
    assert len(list(directory.glob("*.json"))) == 1
    assert not any(
        task.get_name() == "pending-manifest-replay" for task in asyncio.all_tasks()
    )


@pytest.fixture
def fixture_manifests() -> tuple[ArchiveManifest, ...]:
    first = _manifest(UUID(int=1), "a" * 64)
    return (
        first,
        replace(
            first,
            manifest_id=UUID(int=2),
            relative_path=Path("second.jsonl.zst"),
            sha256="b" * 64,
        ),
    )


async def test_journal_replays_manifests_in_order(
    tmp_path: Path,
    fixture_manifests: tuple[ArchiveManifest, ...],
) -> None:
    journal = PendingManifestJournal(tmp_path / "pending")
    for manifest in fixture_manifests:
        await journal.append(manifest)

    saved: list[ArchiveManifest] = []

    async def save(manifest: ArchiveManifest) -> None:
        saved.append(manifest)

    assert await journal.replay(save) == 2

    assert saved == list(fixture_manifests)
    assert await journal.oldest_age_seconds(now=fixture_now) is None


async def test_process_state_journal_replays_critical_transition(
    tmp_path: Path,
) -> None:
    journal = PendingProcessStateJournal(tmp_path / "pending-state")
    record = PendingProcessState(
        state=MarketDataState.HALTED,
        occurred_at=datetime(2026, 6, 15, 2, 0, tzinfo=UTC),
        reason="archive failure",
    )
    await journal.append(record)
    saved: list[PendingProcessState] = []

    async def save(item: PendingProcessState) -> None:
        saved.append(item)

    assert await journal.replay(save) == 1

    assert saved == [record]


def _manifest(manifest_id: UUID, sha256: str) -> ArchiveManifest:
    at = datetime(2026, 6, 15, 2, 0, tzinfo=UTC)
    return ArchiveManifest(
        manifest_id=manifest_id,
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        route=CaptureRoute.MARKET,
        stream=CaptureStream.AGG_TRADE,
        symbol="BTCUSDT",
        utc_date=at.date(),
        utc_hour=at.hour,
        relative_path=Path("first.jsonl.zst"),
        connection_session_id=UUID(int=1),
        subscription_generation_min=1,
        subscription_generation_max=1,
        row_count=1,
        compressed_bytes=100,
        first_exchange_event_at=at,
        last_exchange_event_at=at,
        first_received_at=at,
        last_received_at=at,
        sha256=sha256,
        capture_version="test",
        recovery_status="complete",
        known_gap_count=0,
        created_at=at,
    )
