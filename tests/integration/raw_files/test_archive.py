import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import zstandard

from crypto_momentum_lab.domain.market.models import ArchiveManifest, RawEnvelope
from crypto_momentum_lab.persistence.raw_files.archive import ZstdJsonlArchive


async def test_archive_commits_rotates_and_checksums(
    tmp_path: Path,
    raw_envelope: RawEnvelope,
) -> None:
    manifests: list[ArchiveManifest] = []

    async def save_manifest(manifest: ArchiveManifest) -> None:
        manifests.append(manifest)

    archive = ZstdJsonlArchive(
        root=tmp_path,
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

    acknowledgement = await archive.append(raw_envelope)
    await archive.close()

    assert acknowledgement.local_sequence == 1
    assert len(manifests) == 1
    manifest = manifests[0]
    final_path = tmp_path / manifest.relative_path
    assert final_path.suffixes[-2:] == [".jsonl", ".zst"]
    assert hashlib.sha256(final_path.read_bytes()).hexdigest() == manifest.sha256

    with zstandard.open(final_path, "rt", encoding="utf-8") as stream:
        rows = [json.loads(line) for line in stream]
    assert rows[0]["symbol"] == "BTCUSDT"
    assert rows[0]["local_sequence"] == 1


async def test_session_change_rotates_file(
    tmp_path: Path,
    raw_envelope: RawEnvelope,
) -> None:
    manifests: list[ArchiveManifest] = []

    async def save_manifest(manifest: ArchiveManifest) -> None:
        manifests.append(manifest)

    archive = ZstdJsonlArchive(
        root=tmp_path,
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
    await archive.append(raw_envelope)
    await archive.append(
        replace(
            raw_envelope,
            connection_session_id=UUID(int=2),
            local_sequence=1,
            received_at=datetime(2026, 6, 15, 2, 0, 1, tzinfo=UTC),
        )
    )

    await archive.close()

    assert len(manifests) == 2
    assert {item.connection_session_id for item in manifests} == {
        UUID(int=1),
        UUID(int=2),
    }
