import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from crypto_momentum_lab.persistence.postgres.market_revision_archive import (
    MarketRevisionArchiveError,
)
from crypto_momentum_lab.persistence.postgres.market_revision_metadata_archive import (
    SqliteMarketRevisionMetadataArchive,
    encode_metadata,
)
from crypto_momentum_lab.persistence.postgres.models import MarketRevisionRefRow


def row(identity="old", canonical=True, offset=0):
    start = datetime(2026, 9, 25, tzinfo=UTC)
    return MarketRevisionRefRow(
        revision_id=identity,
        scope="research",
        symbol="BTCUSDT",
        interval="15s",
        bucket_start=start,
        bucket_end=start + timedelta(seconds=15),
        published_at=start + timedelta(seconds=offset),
        content_hash="a" * 64,
        source_epoch="test",
        visibility_mode="canonical",
        is_canonical=canonical,
        lineage={"observed_at": start.isoformat()},
        payload=None,
        payload_archive_path="payload.jsonl.zst",
        payload_archive_sha256="b" * 64,
    )


def test_cold_metadata_roundtrip_and_canonical_reselection(tmp_path):
    catalog = SqliteMarketRevisionMetadataArchive(tmp_path)
    old = row()
    new = row("new", offset=10)
    catalog.store([old, new])
    assert encode_metadata(catalog.get("old")) == encode_metadata(old)
    assert [
        x.revision_id
        for x in catalog.for_bucket(
            old.scope, old.symbol, old.interval, old.bucket_start, canonical=True
        )
    ] == ["new"]
    assert {
        x.revision_id
        for x in catalog.for_bucket(
            old.scope, old.symbol, old.interval, old.bucket_start
        )
    } == {"old", "new"}
    assert len(catalog.dates_and_symbols("research", "15s")) == 1
    assert catalog.path.stat().st_mode & 0o777 == 0o644


def test_conflicting_identity_rolls_back_whole_cold_batch(tmp_path):
    catalog = SqliteMarketRevisionMetadataArchive(tmp_path)
    old = row()
    catalog.store([old])
    conflicting = row()
    conflicting.content_hash = "c" * 64
    with pytest.raises(MarketRevisionArchiveError, match="identity conflict"):
        catalog.store([row("new", offset=-10), conflicting])
    assert catalog.get("new") is None
    assert encode_metadata(catalog.get("old")) == encode_metadata(old)


def test_corrupt_cold_metadata_fails_closed(tmp_path):
    catalog = SqliteMarketRevisionMetadataArchive(tmp_path)
    catalog.store([row()])
    with sqlite3.connect(catalog.path) as connection:
        connection.execute("UPDATE revisions SET digest=?", ("0" * 64,))
    with pytest.raises(MarketRevisionArchiveError, match="checksum"):
        catalog.get("old")


def test_payload_must_be_archived_before_metadata(tmp_path):
    catalog = SqliteMarketRevisionMetadataArchive(tmp_path)
    pending = row()
    pending.payload = {"schema_version": 1}
    with pytest.raises(MarketRevisionArchiveError, match="before payload"):
        catalog.store([pending])


def test_cold_index_rebuild_preserves_latest_canonical_selection(tmp_path):
    from crypto_momentum_lab.persistence.postgres.market_revision_metadata_archive import (
        rebuild_metadata_catalog,
    )

    catalog = SqliteMarketRevisionMetadataArchive(tmp_path)
    old = row()
    new = row("new", offset=10)
    catalog.store([old])
    catalog.store([new])
    catalog.path.write_bytes(b"broken index")
    assert rebuild_metadata_catalog(tmp_path) == 2
    assert encode_metadata(catalog.get("old")) == encode_metadata(old)
    assert [
        r.revision_id
        for r in catalog.for_bucket(
            old.scope, old.symbol, old.interval, old.bucket_start, canonical=True
        )
    ] == ["new"]


def test_missing_index_requires_rebuild_when_snapshots_exist(tmp_path):
    catalog = SqliteMarketRevisionMetadataArchive(tmp_path)
    catalog.store([row()])
    catalog.path.unlink()
    with pytest.raises(MarketRevisionArchiveError, match="index is missing"):
        catalog.get("old")
    with pytest.raises(MarketRevisionArchiveError, match="index is missing"):
        catalog.store([row("new")])
