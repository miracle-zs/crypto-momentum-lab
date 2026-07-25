import asyncio
import json
import os
from collections.abc import Iterator
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from uuid import UUID

import zstandard

from crypto_momentum_lab.domain.market.models import (
    ArchiveManifest,
    CaptureRoute,
    CaptureStream,
    RawEnvelope,
)
from crypto_momentum_lab.persistence.raw_files.archive import ZstdJsonlArchive

_RECOVERY_BATCH_SIZE = 250


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
    temporary_paths = await asyncio.to_thread(
        lambda: tuple(
            sorted(
                path
                for path in root.rglob("*.tmp")
                if ".recovery-quarantine" not in path.parts
            )
        )
    )
    results = []
    for temporary in temporary_paths:
        results.append(
            await recover_temporary_archive(
                temporary,
                archive_root=root,
                environment=environment,
                capture_version=capture_version,
            )
        )
    return tuple(results)


async def recover_temporary_archive(
    temporary: Path,
    *,
    archive_root: Path,
    environment: str,
    capture_version: str,
) -> RecoveryResult:
    data = await asyncio.to_thread(temporary.read_bytes)
    decompressed, discarded_bytes = await asyncio.to_thread(
        _decompress_complete_frame,
        data,
    )
    envelopes = _envelopes_from_jsonl(decompressed)
    try:
        first_envelope = next(envelopes)
    except StopIteration:
        raise ValueError(
            "temporary archive contains no complete records"
        ) from None

    quarantined_path = await asyncio.to_thread(
        _quarantine_temporary,
        temporary,
        archive_root,
    )

    manifests: list[ArchiveManifest] = []

    async def save_manifest(manifest: ArchiveManifest) -> None:
        manifests.append(manifest)

    archive = ZstdJsonlArchive(
        root=archive_root,
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
    await _append_in_batches(
        archive,
        _prepend(first_envelope, envelopes),
        batch_size=_RECOVERY_BATCH_SIZE,
    )
    await archive.close()

    return RecoveryResult(
        manifest=replace(manifests[0], recovery_status="recovered"),
        quarantined_path=quarantined_path,
        discarded_bytes=discarded_bytes,
    )


def _decompress_complete_frame(data: bytes) -> tuple[bytes, int]:
    decompressor = zstandard.ZstdDecompressor().decompressobj()
    decompressed = decompressor.decompress(data)
    return decompressed, len(decompressor.unused_data)


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


def _envelopes_from_jsonl(data: bytes) -> Iterator[RawEnvelope]:
    for line in data.splitlines():
        if not line:
            continue
        payload = json.loads(line)
        yield _envelope_from_payload(payload)


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
