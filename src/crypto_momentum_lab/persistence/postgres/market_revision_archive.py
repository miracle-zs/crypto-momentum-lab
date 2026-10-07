"""Verified zstd archive support for large immutable market-state payloads.

The archive keeps revision payloads out of PostgreSQL while the compact
``market_revision_refs`` rows continue to provide identity and replay indexes.
Archive files are immutable, content-addressed JSONL zstd files.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import tempfile
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Protocol

import zstandard


class MarketRevisionArchiveError(RuntimeError):
    """An archived market payload is missing or failed integrity checks."""


class MarketRevisionPayloadArchive(Protocol):
    """Read-only seam used by the Postgres market-book repository."""

    def load_payload(
        self,
        *,
        relative_path: str,
        expected_sha256: str,
        revision_id: str,
        expected_content_hash: str,
    ) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class MarketRevisionArchiveFile:
    relative_path: str
    sha256: str
    record_count: int
    compressed_bytes: int
    uncompressed_bytes: int


def market_revision_archive_partition(scope: str, bucket_start: datetime) -> str:
    """Return a stable 15-minute partition path for a timezone-aware bucket."""
    if bucket_start.tzinfo is None or bucket_start.utcoffset() is None:
        raise ValueError("bucket_start must include a timezone")
    timestamp = bucket_start.astimezone(UTC)
    safe_scope = re.sub(r"[^A-Za-z0-9_.-]", "_", scope)
    if not safe_scope or safe_scope in {".", ".."}:
        raise ValueError("scope must contain a safe path component")
    quarter_minute = (timestamp.minute // 15) * 15
    return PurePosixPath(
        f"scope={safe_scope}",
        f"date={timestamp:%Y-%m-%d}",
        f"hour={timestamp:%H}",
        f"window={quarter_minute:02d}",
    ).as_posix()


def write_market_revision_archive(
    *,
    root: Path,
    partition: str,
    records: Iterable[Mapping[str, object]],
    zstd_level: int = 3,
) -> MarketRevisionArchiveFile:
    """Atomically write and verify one bounded archive batch plus a manifest."""
    partition_path = _safe_relative_path(partition)
    ordered = sorted(records, key=lambda item: str(item.get("revision_id", "")))
    if not ordered:
        raise ValueError("cannot write an empty market revision archive")

    normalized_records: list[dict[str, object]] = []
    seen_ids: set[str] = set()
    for item in ordered:
        revision_id = item.get("revision_id")
        content_hash = item.get("content_hash")
        payload = item.get("payload")
        if not isinstance(revision_id, str) or not revision_id:
            raise ValueError("archive record revision_id must be a non-empty string")
        if revision_id in seen_ids:
            raise ValueError(f"duplicate archive revision_id: {revision_id}")
        if not isinstance(content_hash, str) or not re.fullmatch(
            r"[0-9a-f]{64}", content_hash
        ):
            raise ValueError(f"archive record {revision_id} has invalid content_hash")
        if not isinstance(payload, Mapping):
            raise ValueError(f"archive record {revision_id} payload must be an object")
        seen_ids.add(revision_id)
        normalized_records.append(
            {
                "revision_id": revision_id,
                "content_hash": content_hash,
                "payload": dict(payload),
            }
        )

    archive_root = root.resolve()
    output_directory = (archive_root / Path(*partition_path.parts)).resolve()
    if not output_directory.is_relative_to(archive_root):
        raise ValueError("archive partition escapes the configured archive root")
    output_directory.mkdir(parents=True, exist_ok=True)

    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            prefix=".market-revisions-",
            suffix=".tmp",
            dir=output_directory,
            delete=False,
        ) as output:
            temp_path = Path(output.name)
            compressor = zstandard.ZstdCompressor(level=zstd_level)
            uncompressed_bytes = 0
            with compressor.stream_writer(output, closefd=False) as writer:
                for record in normalized_records:
                    line = _json_line(record)
                    writer.write(line)
                    uncompressed_bytes += len(line)
            output.flush()
            os.fsync(output.fileno())

        digest = _sha256_file(temp_path)
        filename = f"payloads-{digest}.jsonl.zst"
        relative_path = (partition_path / filename).as_posix()
        destination = output_directory / filename
        compressed_bytes = temp_path.stat().st_size
        _verify_archive_file(temp_path, digest, len(normalized_records))

        if destination.exists():
            if _sha256_file(destination) != digest:
                raise MarketRevisionArchiveError(
                    "content-addressed archive path has conflicting bytes: "
                    f"{relative_path}"
                )
            temp_path.unlink()
            temp_path = None
        else:
            os.replace(temp_path, destination)
            temp_path = None
            _fsync_directory(output_directory)

        manifest = {
            "format": "market-revision-jsonl-zstd",
            "schema_version": 1,
            "partition": partition_path.as_posix(),
            "sha256": digest,
            "record_count": len(normalized_records),
            "compressed_bytes": compressed_bytes,
            "uncompressed_bytes": uncompressed_bytes,
            "first_revision_id": normalized_records[0]["revision_id"],
            "last_revision_id": normalized_records[-1]["revision_id"],
        }
        manifest_path = destination.with_name(destination.name + ".manifest.json")
        _write_manifest(manifest_path, manifest)
        return MarketRevisionArchiveFile(
            relative_path=relative_path,
            sha256=digest,
            record_count=len(normalized_records),
            compressed_bytes=compressed_bytes,
            uncompressed_bytes=uncompressed_bytes,
        )
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


class ZstdMarketRevisionPayloadArchive:
    """Verified archive reader with a bounded cache for replay batches."""

    def __init__(self, root: Path, *, cache_max_bytes: int = 8 * 1024 * 1024) -> None:
        if cache_max_bytes < 0:
            raise ValueError("cache_max_bytes cannot be negative")
        self._root = root.resolve()
        self._cache_max_bytes = cache_max_bytes
        self._cached_bytes = 0
        self._cache: OrderedDict[
            tuple[str, str], tuple[dict[str, dict[str, object]], int]
        ] = OrderedDict()
        self._verified: OrderedDict[str, tuple[int, int, str]] = OrderedDict()

    def load_payload(
        self,
        *,
        relative_path: str,
        expected_sha256: str,
        revision_id: str,
        expected_content_hash: str,
    ) -> dict[str, object]:
        """Load one payload only after checking its archive and row identity."""
        if not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
            raise MarketRevisionArchiveError("invalid archive SHA-256 pointer")
        try:
            path = self._resolve_path(relative_path)
            self._verify_file(path, relative_path, expected_sha256)
        except MarketRevisionArchiveError:
            raise
        except (OSError, ValueError) as error:
            raise MarketRevisionArchiveError(
                f"cannot resolve market revision archive {relative_path}: {error}"
            ) from error

        cache_key = (relative_path, expected_sha256)
        cached = self._cache.get(cache_key)
        if cached is not None:
            self._cache.move_to_end(cache_key)
            record = cached[0].get(revision_id)
            if record is None:
                raise MarketRevisionArchiveError(
                    f"revision {revision_id} is absent from archive {relative_path}"
                )
            return _validated_payload(record, revision_id, expected_content_hash)

        records: dict[str, dict[str, object]] | None = {}
        cache_bytes = 0
        target: dict[str, object] | None = None
        try:
            with path.open("rb") as compressed:
                decompressor = zstandard.ZstdDecompressor()
                with decompressor.stream_reader(compressed) as stream:
                    text_stream = io.TextIOWrapper(stream, encoding="utf-8")
                    for line in text_stream:
                        try:
                            raw_record = json.loads(line)
                        except (json.JSONDecodeError, UnicodeDecodeError) as error:
                            raise MarketRevisionArchiveError(
                                "invalid JSONL in market revision archive "
                                f"{relative_path}"
                            ) from error
                        record = _parse_record(raw_record, relative_path)
                        record_id = str(record["revision_id"])
                        if record_id == revision_id:
                            target = record
                        if records is not None:
                            if record_id in records:
                                raise MarketRevisionArchiveError(
                                    f"duplicate revision {record_id} in archive "
                                    f"{relative_path}"
                                )
                            records[record_id] = record
                            cache_bytes += len(line.encode("utf-8"))
                            if cache_bytes > self._cache_max_bytes:
                                records = None
            if target is None:
                raise MarketRevisionArchiveError(
                    f"revision {revision_id} is absent from archive {relative_path}"
                )
            if records is not None and cache_bytes <= self._cache_max_bytes:
                self._cache_partition(cache_key, records, cache_bytes)
            return _validated_payload(target, revision_id, expected_content_hash)
        except (OSError, UnicodeDecodeError, zstandard.ZstdError) as error:
            raise MarketRevisionArchiveError(
                f"cannot read market revision archive {relative_path}: {error}"
            ) from error

    def _resolve_path(self, relative_path: str) -> Path:
        relative = _safe_relative_path(relative_path)
        path = (self._root / Path(*relative.parts)).resolve(strict=True)
        if not path.is_relative_to(self._root):
            raise MarketRevisionArchiveError(
                "market revision archive pointer escapes configured archive root"
            )
        if not path.is_file():
            raise MarketRevisionArchiveError(
                f"market revision archive is not a file: {relative_path}"
            )
        return path

    def _verify_file(
        self, path: Path, relative_path: str, expected_sha256: str
    ) -> None:
        stat = path.stat()
        signature = (stat.st_size, stat.st_mtime_ns, expected_sha256)
        verified = self._verified.get(relative_path)
        if verified == signature:
            self._verified.move_to_end(relative_path)
            return
        actual = _sha256_file(path)
        if actual != expected_sha256:
            raise MarketRevisionArchiveError(
                f"SHA-256 mismatch for market revision archive {relative_path}"
            )
        self._verified[relative_path] = signature
        self._verified.move_to_end(relative_path)
        while len(self._verified) > 512:
            self._verified.popitem(last=False)

    def _cache_partition(
        self,
        key: tuple[str, str],
        records: dict[str, dict[str, object]],
        cache_bytes: int,
    ) -> None:
        previous = self._cache.pop(key, None)
        if previous is not None:
            self._cached_bytes -= previous[1]
        if cache_bytes > self._cache_max_bytes:
            return
        self._cache[key] = (records, cache_bytes)
        self._cached_bytes += cache_bytes
        while self._cached_bytes > self._cache_max_bytes and self._cache:
            _, (_, evicted_bytes) = self._cache.popitem(last=False)
            self._cached_bytes -= evicted_bytes


def _validated_payload(
    record: Mapping[str, object], revision_id: str, expected_content_hash: str
) -> dict[str, object]:
    if record.get("content_hash") != expected_content_hash:
        raise MarketRevisionArchiveError(
            f"content hash mismatch for archived revision {revision_id}"
        )
    payload = record.get("payload")
    if not isinstance(payload, dict):
        raise MarketRevisionArchiveError(
            f"payload for archived revision {revision_id} is not an object"
        )
    return dict(payload)


def _parse_record(raw_record: object, relative_path: str) -> dict[str, object]:
    if not isinstance(raw_record, dict):
        raise MarketRevisionArchiveError(
            f"archive record is not an object in {relative_path}"
        )
    revision_id = raw_record.get("revision_id")
    content_hash = raw_record.get("content_hash")
    payload = raw_record.get("payload")
    if (
        not isinstance(revision_id, str)
        or not revision_id
        or not isinstance(content_hash, str)
        or not re.fullmatch(r"[0-9a-f]{64}", content_hash)
        or not isinstance(payload, dict)
    ):
        raise MarketRevisionArchiveError(f"invalid archive record in {relative_path}")
    return raw_record


def _json_line(record: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            record,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        + b"\n"
    )


def _safe_relative_path(value: str) -> PurePosixPath:
    relative = PurePosixPath(value)
    if (
        not value
        or relative.is_absolute()
        or any(part in {"", ".", ".."} for part in relative.parts)
    ):
        raise ValueError("archive path must be a non-empty safe relative path")
    return relative


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _verify_archive_file(path: Path, expected_sha256: str, expected_count: int) -> None:
    if _sha256_file(path) != expected_sha256:
        raise MarketRevisionArchiveError("archive SHA-256 changed during write")
    count = 0
    with path.open("rb") as compressed:
        with zstandard.ZstdDecompressor().stream_reader(compressed) as stream:
            text_stream = io.TextIOWrapper(stream, encoding="utf-8")
            for line in text_stream:
                try:
                    _parse_record(json.loads(line), str(path))
                except (json.JSONDecodeError, UnicodeDecodeError) as error:
                    raise MarketRevisionArchiveError(
                        f"invalid JSONL in newly written archive: {path}"
                    ) from error
                count += 1
    if count != expected_count:
        raise MarketRevisionArchiveError(
            f"archive row count mismatch: expected {expected_count}, found {count}"
        )


def _write_manifest(path: Path, manifest: Mapping[str, object]) -> None:
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            prefix=".manifest-",
            suffix=".tmp",
            dir=path.parent,
            delete=False,
        ) as output:
            temp_path = Path(output.name)
            output.write(_json_line(manifest))
            output.flush()
            os.fsync(output.fileno())
        os.replace(temp_path, path)
        temp_path = None
        _fsync_directory(path.parent)
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def _fsync_directory(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


__all__ = [
    "MarketRevisionArchiveError",
    "MarketRevisionArchiveFile",
    "MarketRevisionPayloadArchive",
    "ZstdMarketRevisionPayloadArchive",
    "market_revision_archive_partition",
    "write_market_revision_archive",
]
