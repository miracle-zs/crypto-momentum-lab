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
