from __future__ import annotations

import hashlib
import os
import stat
from datetime import UTC, datetime

import pytest

from crypto_momentum_lab.persistence.postgres.market_revision_archive import (
    MarketRevisionArchiveError,
    ZstdMarketRevisionPayloadArchive,
    market_revision_archive_partition,
    write_market_revision_archive,
)


def test_zstd_market_revision_archive_round_trip_and_manifest(tmp_path) -> None:
    records = [
        {
            "revision_id": "rev_b",
            "content_hash": "b" * 64,
            "payload": {"schema_version": 1, "symbol": "ETHUSDT"},
        },
        {
            "revision_id": "rev_a",
            "content_hash": "a" * 64,
            "payload": {"schema_version": 1, "symbol": "BTCUSDT"},
        },
    ]
    previous_umask = os.umask(0o077)
    try:
        archive = write_market_revision_archive(
            root=tmp_path,
            partition="scope=live/date=2026-10-01/hour=00/window=00",
            records=records,
        )
    finally:
        os.umask(previous_umask)

    reader = ZstdMarketRevisionPayloadArchive(tmp_path)
    assert reader.load_payload(
        relative_path=archive.relative_path,
        expected_sha256=archive.sha256,
        revision_id="rev_a",
        expected_content_hash="a" * 64,
    ) == {"schema_version": 1, "symbol": "BTCUSDT"}
    assert archive.record_count == 2
    assert archive.compressed_bytes < archive.uncompressed_bytes
    archive_path = tmp_path / archive.relative_path
    manifest_path = tmp_path / f"{archive.relative_path}.manifest.json"
    assert manifest_path.is_file()
    assert stat.S_IMODE(archive_path.stat().st_mode) == 0o644
    assert stat.S_IMODE(manifest_path.stat().st_mode) == 0o644
    directory = archive_path.parent
    while directory != tmp_path:
        assert stat.S_IMODE(directory.stat().st_mode) == 0o755
        directory = directory.parent

    archive_path.chmod(0o600)
    manifest_path.chmod(0o600)
    write_market_revision_archive(
        root=tmp_path,
        partition="scope=live/date=2026-10-01/hour=00/window=00",
        records=records,
    )
    assert stat.S_IMODE(archive_path.stat().st_mode) == 0o644
    assert stat.S_IMODE(manifest_path.stat().st_mode) == 0o644


def test_archive_reader_fails_closed_on_checksum_or_content_hash_mismatch(
    tmp_path,
) -> None:
    archive = write_market_revision_archive(
        root=tmp_path,
        partition="scope=live/date=2026-10-01/hour=00/window=00",
        records=[
            {
                "revision_id": "rev_a",
                "content_hash": "a" * 64,
                "payload": {"schema_version": 1},
            }
        ],
    )
    reader = ZstdMarketRevisionPayloadArchive(tmp_path)

    with pytest.raises(MarketRevisionArchiveError, match="content hash mismatch"):
        reader.load_payload(
            relative_path=archive.relative_path,
            expected_sha256=archive.sha256,
            revision_id="rev_a",
            expected_content_hash="b" * 64,
        )

    compressed_path = tmp_path / archive.relative_path
    compressed_path.write_bytes(compressed_path.read_bytes() + b"tamper")
    with pytest.raises(MarketRevisionArchiveError, match="SHA-256 mismatch"):
        ZstdMarketRevisionPayloadArchive(tmp_path).load_payload(
            relative_path=archive.relative_path,
            expected_sha256=archive.sha256,
            revision_id="rev_a",
            expected_content_hash="a" * 64,
        )


def test_archive_reader_rejects_traversal_and_missing_revision(tmp_path) -> None:
    archive = write_market_revision_archive(
        root=tmp_path,
        partition="scope=live/date=2026-10-01/hour=00/window=00",
        records=[
            {
                "revision_id": "rev_a",
                "content_hash": "a" * 64,
                "payload": {"schema_version": 1},
            }
        ],
    )
    reader = ZstdMarketRevisionPayloadArchive(tmp_path)
    with pytest.raises(MarketRevisionArchiveError, match="safe relative path"):
        reader.load_payload(
            relative_path="../outside.jsonl.zst",
            expected_sha256=archive.sha256,
            revision_id="rev_a",
            expected_content_hash="a" * 64,
        )
    with pytest.raises(MarketRevisionArchiveError, match="absent from archive"):
        reader.load_payload(
            relative_path=archive.relative_path,
            expected_sha256=archive.sha256,
            revision_id="rev_missing",
            expected_content_hash="a" * 64,
        )


def test_archive_writer_is_content_addressed_and_partition_is_utc(tmp_path) -> None:
    start = datetime(2026, 10, 1, 0, 14, tzinfo=UTC)
    partition = market_revision_archive_partition("live", start)
    record = {
        "revision_id": "rev_a",
        "content_hash": "a" * 64,
        "payload": {"schema_version": 1},
    }
    first = write_market_revision_archive(
        root=tmp_path,
        partition=partition,
        records=[record],
    )
    second = write_market_revision_archive(
        root=tmp_path,
        partition=partition,
        records=[record],
    )
    assert partition.endswith("window=00")
    assert first == second
    assert hashlib.sha256(
        (tmp_path / first.relative_path).read_bytes()
    ).hexdigest() == (first.sha256)


def test_market_revision_archive_partition_requires_timezone() -> None:
    with pytest.raises(ValueError, match="timezone"):
        market_revision_archive_partition("live", datetime(2026, 10, 1))
