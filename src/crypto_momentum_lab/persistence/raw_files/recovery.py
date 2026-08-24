import asyncio
import json
import os
import shutil
from collections.abc import Iterator
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from uuid import UUID, uuid4

import zstandard

from crypto_momentum_lab.domain.market.models import (
    ArchiveManifest,
    CaptureRoute,
    CaptureStream,
    RawEnvelope,
)
from crypto_momentum_lab.persistence.raw_files.archive import ZstdJsonlArchive

_RECOVERY_BATCH_SIZE = 250


class EmptyTemporaryArchiveError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class RecoveryResult:
    manifest: ArchiveManifest
    quarantined_path: Path
    discarded_bytes: int


async def recover_archive_root(
    root: Path,
    *,
    environment: str,
    capture_version: str,
) -> tuple[RecoveryResult, ...]:
    await asyncio.to_thread(_cleanup_recovery_working, root)
    temporary_paths = await asyncio.to_thread(
        lambda: tuple(
            sorted(
                path
                for path in root.rglob("*.tmp")
                if not {
                    ".recovery-quarantine",
                    ".recovery-working",
                }.intersection(path.parts)
            )
        )
    )
    results = []
    for temporary in temporary_paths:
        try:
            results.append(
                await recover_temporary_archive(
                    temporary,
                    archive_root=root,
                    environment=environment,
                    capture_version=capture_version,
                )
            )
        except EmptyTemporaryArchiveError:
            await asyncio.to_thread(
                _quarantine_temporary,
                temporary,
                root,
            )
    return tuple(results)


async def recover_temporary_archive(
    temporary: Path,
    *,
    archive_root: Path,
    environment: str,
    capture_version: str,
) -> RecoveryResult:
    decode_stats = _RecoveryDecodeStats()
    envelopes = _envelopes_from_temporary(temporary, decode_stats)
    try:
        first_envelope = next(envelopes)
    except StopIteration:
        raise EmptyTemporaryArchiveError(
            "temporary archive contains no complete records"
        ) from None

    staging_root = archive_root / ".recovery-working" / uuid4().hex
    manifests: list[ArchiveManifest] = []

    async def save_manifest(manifest: ArchiveManifest) -> None:
        manifests.append(manifest)

    archive = ZstdJsonlArchive(
        root=staging_root,
        environment=environment,
        capture_version=capture_version,
        manifest_sink=save_manifest,
        known_gap_count_provider=lambda key: 0,
        zstd_level=1,
        rotation_uncompressed_bytes=10_000_000_000,
        max_open_writers=4,
        group_commit_max_events=_RECOVERY_BATCH_SIZE,
        group_commit_max_milliseconds=250,
    )
    try:
        await _append_in_batches(
            archive,
            _prepend(first_envelope, envelopes),
            batch_size=_RECOVERY_BATCH_SIZE,
        )
        await archive.close()
        await asyncio.to_thread(
            _promote_recovered_files,
            manifests,
            staging_root,
            archive_root,
        )
    except Exception:
        try:
            await archive.close()
        except Exception:
            pass
        await asyncio.to_thread(shutil.rmtree, staging_root, True)
        raise

    await asyncio.to_thread(shutil.rmtree, staging_root, True)

    quarantined_path = await asyncio.to_thread(
        _quarantine_temporary,
        temporary,
        archive_root,
    )

    if not manifests:
        raise RuntimeError("recovery produced no archive manifest")

    return RecoveryResult(
        manifest=replace(manifests[0], recovery_status="recovered"),
        quarantined_path=quarantined_path,
        discarded_bytes=decode_stats.discarded_bytes,
    )


@dataclass(slots=True)
class _RecoveryDecodeStats:
    discarded_bytes: int = 0


def _envelopes_from_temporary(
    temporary: Path,
    stats: _RecoveryDecodeStats,
) -> Iterator[RawEnvelope]:
    decompressor = zstandard.ZstdDecompressor().decompressobj()
    pending = b""
    with temporary.open("rb") as compressed:
        while chunk := compressed.read(1024 * 1024):
            try:
                decompressed = decompressor.decompress(chunk)
            except zstandard.ZstdError:
                return
            pending += decompressed
            while b"\n" in pending:
                line, pending = pending.split(b"\n", 1)
                if line:
                    yield _envelope_from_json_line(line)
            if decompressor.eof:
                stats.discarded_bytes += len(decompressor.unused_data)
                stats.discarded_bytes += sum(
                    len(remaining)
                    for remaining in iter(
                        lambda: compressed.read(1024 * 1024),
                        b"",
                    )
                )
                break

    if pending.strip():
        try:
            yield _envelope_from_json_line(pending)
        except json.JSONDecodeError:
            # A writer may have been interrupted in the middle of its last row.
            return


def _envelope_from_json_line(line: bytes) -> RawEnvelope:
    payload = json.loads(line)
    if not isinstance(payload, dict):
        raise ValueError("temporary archive row must be a JSON object")
    return _envelope_from_payload(payload)


async def _append_in_batches(
    archive: ZstdJsonlArchive,
    envelopes: Iterator[RawEnvelope],
    *,
    batch_size: int,
) -> None:
    batch: list[RawEnvelope] = []
    for envelope in envelopes:
        batch.append(envelope)
        if len(batch) >= batch_size:
            await asyncio.gather(*(archive.append(item) for item in batch))
            batch.clear()
    if batch:
        await asyncio.gather(*(archive.append(item) for item in batch))


def _prepend(
    first: RawEnvelope,
    remaining: Iterator[RawEnvelope],
) -> Iterator[RawEnvelope]:
    yield first
    yield from remaining


def _envelope_from_payload(payload: dict[str, object]) -> RawEnvelope:
    return RawEnvelope(
        schema_version=_int_value(payload["schema_version"]),
        exchange=_str_value(payload["exchange"]),
        environment=_str_value(payload["environment"]),
        route=CaptureRoute(_str_value(payload["route"])),
        stream=CaptureStream(_str_value(payload["stream"])),
        symbol=_optional_str_value(payload["symbol"]),
        exchange_event_at=_optional_datetime_value(
            payload["exchange_event_at"]
        ),
        received_at=_datetime_value(payload["received_at"]),
        received_monotonic_ns=_int_value(payload["received_monotonic_ns"]),
        connection_session_id=UUID(_str_value(payload["connection_session_id"])),
        local_sequence=_int_value(payload["local_sequence"]),
        exchange_sequence=_optional_str_value(payload["exchange_sequence"]),
        subscription_generation=_int_value(payload["subscription_generation"]),
        raw_payload=payload["raw_payload"],  # type: ignore[arg-type]
        recovered=_bool_value(payload.get("recovered", False)),
    )


def _quarantine_temporary(temporary: Path, archive_root: Path) -> Path:
    relative = temporary.relative_to(archive_root)
    destination = (
        archive_root
        / ".recovery-quarantine"
        / relative.with_name(f"{relative.name}.quarantined")
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    os.replace(temporary, destination)
    _fsync_directory(destination.parent)
    _fsync_directory(temporary.parent)
    return destination


def _promote_recovered_files(
    manifests: list[ArchiveManifest],
    staging_root: Path,
    archive_root: Path,
) -> None:
    for manifest in manifests:
        source = staging_root / manifest.relative_path
        destination = archive_root / manifest.relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        os.replace(source, destination)
        _fsync_directory(destination.parent)


def _cleanup_recovery_working(archive_root: Path) -> None:
    shutil.rmtree(archive_root / ".recovery-working", ignore_errors=True)


def _str_value(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("expected string value")
    return value


def _optional_str_value(value: object) -> str | None:
    if value is None:
        return None
    return _str_value(value)


def _int_value(value: object) -> int:
    if not isinstance(value, int):
        raise ValueError("expected integer value")
    return value


def _bool_value(value: object) -> bool:
    if not isinstance(value, bool):
        raise ValueError("expected boolean value")
    return value


def _datetime_value(value: object) -> datetime:
    return datetime.fromisoformat(_str_value(value))


def _optional_datetime_value(value: object) -> datetime | None:
    if value is None:
        return None
    return _datetime_value(value)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
