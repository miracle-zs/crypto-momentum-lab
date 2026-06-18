from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from crypto_momentum_lab.domain.market.models import ArchiveManifest, RawEnvelope
from crypto_momentum_lab.persistence.raw_files.archive import ZstdJsonlArchive
from crypto_momentum_lab.research.datasets import derive_market_datasets


async def test_derive_market_datasets_writes_events_and_states(
    tmp_path: Path,
    raw_envelope: RawEnvelope,
) -> None:
    raw_root = tmp_path / "raw"
    archive_manifests: list[ArchiveManifest] = []

    async def save_manifest(manifest: ArchiveManifest) -> None:
        archive_manifests.append(manifest)

    archive = ZstdJsonlArchive(
        root=raw_root,
        environment="test",
        capture_version="test",
        manifest_sink=save_manifest,
        known_gap_count_provider=lambda key: 0,
        zstd_level=1,
        rotation_uncompressed_bytes=10_000_000,
        max_open_writers=4,
        group_commit_max_events=1,
        group_commit_max_milliseconds=10_000,
    )
    await archive.append(
        replace(
            raw_envelope,
            exchange_event_at=datetime(2026, 6, 15, 2, 0, tzinfo=UTC),
            raw_payload={
                "e": "aggTrade",
                "E": 1781488800000,
                "s": "BTCUSDT",
                "a": 42,
                "p": "100.25",
                "q": "1",
                "m": False,
            },
        )
    )
    await archive.close()

    result = derive_market_datasets(
        raw_paths=tuple(raw_root / item.relative_path for item in archive_manifests),
        output_root=tmp_path / "derived",
    )

    assert len(result.market_event_manifests) == 1
    assert len(result.market_state_manifests) == 1
    assert result.market_event_manifests[0].row_count == 1
    assert result.market_state_manifests[0].row_count == 1
