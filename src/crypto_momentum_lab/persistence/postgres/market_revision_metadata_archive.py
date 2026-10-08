"""Compact, checksummed cold revision metadata with indexed historical lookup."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import zlib
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from crypto_momentum_lab.persistence.postgres.market_revision_archive import (
    MarketRevisionArchiveError,
    write_market_revision_archive,
)
from crypto_momentum_lab.persistence.postgres.models import MarketRevisionRefRow

_DATE_FIELDS = ("bucket_start", "bucket_end", "published_at")
_FIELDS = tuple(column.name for column in MarketRevisionRefRow.__table__.columns)


def encode_metadata(row: MarketRevisionRefRow) -> dict:
    if (
        row.payload is not None
        or not row.payload_archive_path
        or not row.payload_archive_sha256
    ):
        raise MarketRevisionArchiveError("Metadata cannot move before payload archival")
    values = {name: getattr(row, name) for name in _FIELDS}
    for name in _DATE_FIELDS:
        value = values[name]
        if value.tzinfo is None:
            raise MarketRevisionArchiveError("Revision time must include a timezone")
        values[name] = value.astimezone(UTC).isoformat(timespec="microseconds")
    return values


def metadata_row(values):
    values = dict(values)
    if set(values) != set(_FIELDS):
        raise MarketRevisionArchiveError("Unsupported cold metadata schema")
    for name in _DATE_FIELDS:
        values[name] = datetime.fromisoformat(values[name])
    return MarketRevisionRefRow(**values)


class SqliteMarketRevisionMetadataArchive:
    """One durable local cold index; PostgreSQL remains authoritative for hot rows."""

    def __init__(self, root: Path):
        self.path = root / "metadata.sqlite3"

    def _read(self):
        if not self.path.exists():
            if any(
                (self.path.parent / "metadata" / "batches").glob("*/*.manifest.json")
            ):
                raise MarketRevisionArchiveError(
                    "Cold metadata index is missing; rebuild it before reading history"
                )
            return None
        connection = sqlite3.connect(
            f"{self.path.resolve().as_uri()}?mode=ro", uri=True, timeout=5
        )
        connection.row_factory = sqlite3.Row
        return connection

    @staticmethod
    def _decode(record):
        try:
            decoder = zlib.decompressobj()
            raw = decoder.decompress(record["metadata"], 1024 * 1024 + 1)
            if not decoder.eof or decoder.unused_data:
                raise ValueError("invalid metadata compression")
            if (
                len(raw) > 1024 * 1024
                or hashlib.sha256(raw).hexdigest() != record["digest"]
            ):
                raise ValueError("metadata checksum mismatch")
            values = json.loads(raw)
            if values["revision_id"] != record["revision_id"]:
                raise ValueError("metadata identity mismatch")
            for name in ("scope", "symbol", "interval", "bucket_start", "published_at"):
                if values[name] != record[name]:
                    raise ValueError("metadata index mismatch")
            return metadata_row(values)
        except (ValueError, KeyError, zlib.error) as error:
            raise MarketRevisionArchiveError(
                f"Cold metadata is invalid: {error}"
            ) from error

    def store(self, rows, *, emit_snapshot=True):
        rows = list(rows)
        if emit_snapshot and not self.path.exists():
            self._read()  # Existing snapshots require rebuilding a lost catalog.
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path, timeout=5) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("""CREATE TABLE IF NOT EXISTS revisions (
                revision_id TEXT PRIMARY KEY, scope TEXT NOT NULL, symbol TEXT NOT NULL,
                interval TEXT NOT NULL, bucket_start TEXT NOT NULL, published_at TEXT NOT NULL,
                is_canonical INTEGER NOT NULL, metadata BLOB NOT NULL, digest TEXT NOT NULL)""")
            connection.execute(
                "CREATE INDEX IF NOT EXISTS revision_buckets ON revisions(scope,symbol,interval,bucket_start,is_canonical)"
            )
            connection.execute("BEGIN IMMEDIATE")
            ordered = sorted(rows, key=lambda item: item.published_at)
            for row in ordered:
                values = encode_metadata(row)
                previous = connection.execute(
                    "SELECT * FROM revisions WHERE revision_id=?", (row.revision_id,)
                ).fetchone()
                if previous is not None:
                    old = encode_metadata(self._decode(previous))
                    for mutable in (
                        "is_canonical",
                        "payload_archive_path",
                        "payload_archive_sha256",
                    ):
                        old.pop(mutable)
                        values.pop(mutable)
                    if old != values:
                        raise MarketRevisionArchiveError(
                            "Immutable cold revision identity conflict"
                        )
                    values = encode_metadata(row)
            if emit_snapshot:
                write_market_revision_archive(
                    root=self.path.parent / "metadata",
                    partition="batches/"
                    + datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
                    + "_"
                    + uuid4().hex,
                    records=[
                        {
                            "revision_id": row.revision_id,
                            "content_hash": row.content_hash,
                            "payload": encode_metadata(row),
                        }
                        for row in ordered
                    ],
                )
            for row in ordered:
                values = encode_metadata(row)
                raw = json.dumps(
                    values, sort_keys=True, separators=(",", ":"), allow_nan=False
                ).encode()
                if row.is_canonical:
                    connection.execute(
                        "UPDATE revisions SET is_canonical=0 WHERE scope=? AND symbol=? AND interval=? AND bucket_start=?",
                        (row.scope, row.symbol, row.interval, values["bucket_start"]),
                    )
                connection.execute(
                    "INSERT OR REPLACE INTO revisions VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        row.revision_id,
                        row.scope,
                        row.symbol,
                        row.interval,
                        values["bucket_start"],
                        values["published_at"],
                        int(row.is_canonical),
                        zlib.compress(raw),
                        hashlib.sha256(raw).hexdigest(),
                    ),
                )
        self.path.chmod(0o644)
        # SQLite FULL commit precedes any PostgreSQL deletion. Verify actual bytes
        # through a new read connection, including replay identity and pointers.
        verified = self.get_many([row.revision_id for row in rows])
        for row in rows:
            if encode_metadata(verified[row.revision_id]) != encode_metadata(row):
                raise MarketRevisionArchiveError("Cold metadata verification failed")

    def get(self, revision_id):
        return self.get_many([revision_id]).get(revision_id)

    def get_many(self, identities):
        connection = self._read()
        if connection is None:
            return {}
        result = {}
        try:
            identities = list(identities)
            for offset in range(0, len(identities), 500):
                chunk = identities[offset : offset + 500]
                placeholders = ",".join("?" for _ in chunk)
                for record in connection.execute(
                    f"SELECT * FROM revisions WHERE revision_id IN ({placeholders})",
                    chunk,
                ):
                    row = self._decode(record)
                    result[row.revision_id] = row
            return result
        finally:
            connection.close()

    def for_bucket(self, scope, symbol, interval, bucket_start, *, canonical=False):
        return self.in_range(
            scope,
            (symbol,),
            interval,
            bucket_start,
            bucket_start,
            exact=True,
            canonical=canonical,
        )

    def in_range(
        self, scope, symbols, interval, start, end, *, exact=False, canonical=True
    ):
        connection = self._read()
        if connection is None or not symbols:
            if connection is not None:
                connection.close()
            return []
        try:
            placeholders = ",".join("?" for _ in symbols)
            time_filter = (
                "bucket_start=?" if exact else "bucket_start>=? AND bucket_start<?"
            )
            times = (
                (start.astimezone(UTC).isoformat(timespec="microseconds"),)
                if exact
                else (
                    start.astimezone(UTC).isoformat(timespec="microseconds"),
                    end.astimezone(UTC).isoformat(timespec="microseconds"),
                )
            )
            canonical_filter = " AND is_canonical=1" if canonical else ""
            records = connection.execute(
                f"SELECT * FROM revisions WHERE scope=? AND symbol IN ({placeholders}) AND interval=? AND {time_filter}{canonical_filter} ORDER BY published_at DESC",
                (scope, *symbols, interval, *times),
            )
            for row in records:
                yield self._decode(row)
        finally:
            connection.close()

    def dates_and_symbols(self, scope, interval):
        connection = self._read()
        if connection is None:
            return []
        try:
            return connection.execute(
                "SELECT DISTINCT substr(bucket_start,1,10),symbol FROM revisions WHERE scope=? AND interval=? AND is_canonical=1 ORDER BY 1,2",
                (scope, interval),
            ).fetchall()
        finally:
            connection.close()


def rebuild_metadata_catalog(root: Path) -> int:
    """Rebuild a derived index from checksum-verified immutable batch snapshots."""
    from crypto_momentum_lab.persistence.postgres.market_revision_archive import (
        ZstdMarketRevisionPayloadArchive,
    )

    target = root / ("metadata.rebuild-" + uuid4().hex + ".sqlite3")
    catalog = SqliteMarketRevisionMetadataArchive(root)
    catalog.path = target
    reader = ZstdMarketRevisionPayloadArchive(root / "metadata", cache_max_bytes=0)
    count = 0
    manifests = sorted((root / "metadata" / "batches").glob("*/*.manifest.json"))
    if not manifests:
        raise MarketRevisionArchiveError("No immutable metadata snapshots to rebuild")
    try:
        for manifest_path in manifests:
            manifest = json.loads(manifest_path.read_text())
            if not 1 <= manifest["record_count"] <= 1000:
                raise MarketRevisionArchiveError("Invalid metadata snapshot size")
            relative = (
                manifest_path.relative_to(root / "metadata")
                .as_posix()
                .removesuffix(".manifest.json")
            )
            rows = []
            for record in reader.iter_records(
                relative_path=relative, expected_sha256=manifest["sha256"]
            ):
                row = metadata_row(record["payload"])
                if (
                    row.revision_id != record["revision_id"]
                    or row.content_hash != record["content_hash"]
                ):
                    raise MarketRevisionArchiveError(
                        "Metadata snapshot identity mismatch"
                    )
                rows.append(row)
            if len(rows) != manifest["record_count"]:
                raise MarketRevisionArchiveError("Metadata snapshot count mismatch")
            catalog.store(rows, emit_snapshot=False)
            count += len(rows)
        with sqlite3.connect(target) as connection:
            if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise MarketRevisionArchiveError("Rebuilt cold catalog is invalid")
        os.replace(target, root / "metadata.sqlite3")
        directory = os.open(root, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return count
    finally:
        target.unlink(missing_ok=True)
