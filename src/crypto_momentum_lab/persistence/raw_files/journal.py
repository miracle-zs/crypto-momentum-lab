import asyncio
import json
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from crypto_momentum_lab.domain.market.models import (
    ArchiveManifest,
    CaptureRoute,
    CaptureStream,
    MarketDataState,
)


@dataclass(frozen=True, slots=True)
class PendingProcessState:
    state: MarketDataState
    occurred_at: datetime
    reason: str | None


class PendingManifestJournal:
    def __init__(self, directory: Path) -> None:
        self._directory = directory

    async def append(self, manifest: ArchiveManifest) -> None:
        await asyncio.to_thread(
            _write_json_atomic,
            self._directory,
            _manifest_filename(manifest),
            _manifest_to_payload(manifest),
        )

    async def replay(
        self,
        save: Callable[[ArchiveManifest], Awaitable[None]],
    ) -> int:
        count = 0
        for path in await asyncio.to_thread(_journal_entries, self._directory):
            manifest = await asyncio.to_thread(
                _manifest_from_payload,
                _read_json(path),
            )
            await save(manifest)
            await asyncio.to_thread(_delete_journal_entry, path)
            count += 1
        return count

    async def oldest_age_seconds(
        self,
        *,
        now: datetime,
    ) -> float | None:
        entries = await asyncio.to_thread(_journal_entries, self._directory)
        if not entries:
            return None
        manifest = await asyncio.to_thread(
            _manifest_from_payload,
            _read_json(entries[0]),
        )
        return max(0.0, (now - manifest.created_at).total_seconds())


class PendingProcessStateJournal:
    def __init__(self, directory: Path) -> None:
        self._directory = directory

    async def append(self, record: PendingProcessState) -> None:
        await asyncio.to_thread(
            _write_json_atomic,
            self._directory,
            _process_state_filename(record),
            _process_state_to_payload(record),
        )

    async def replay(
        self,
        save: Callable[[PendingProcessState], Awaitable[None]],
    ) -> int:
        count = 0
        for path in await asyncio.to_thread(_journal_entries, self._directory):
            record = await asyncio.to_thread(
                _process_state_from_payload,
                _read_json(path),
            )
            await save(record)
            await asyncio.to_thread(_delete_journal_entry, path)
            count += 1
        return count


def _manifest_filename(manifest: ArchiveManifest) -> str:
    created_at = manifest.created_at.astimezone(UTC)
    timestamp = created_at.strftime("%Y%m%dT%H%M%S.%fZ")
    return f"{timestamp}-{manifest.manifest_id}.json"


def _process_state_filename(record: PendingProcessState) -> str:
    occurred_at = record.occurred_at.astimezone(UTC)
    timestamp = occurred_at.strftime("%Y%m%dT%H%M%S.%fZ")
    return f"{timestamp}-{record.state.value}.json"


def _manifest_to_payload(manifest: ArchiveManifest) -> dict[str, Any]:
    return {
        "manifest_id": str(manifest.manifest_id),
        "schema_version": manifest.schema_version,
        "exchange": manifest.exchange,
        "environment": manifest.environment,
        "route": manifest.route.value,
        "stream": manifest.stream.value,
        "symbol": manifest.symbol,
        "utc_date": manifest.utc_date.isoformat(),
        "utc_hour": manifest.utc_hour,
        "relative_path": str(manifest.relative_path),
        "connection_session_id": str(manifest.connection_session_id),
        "subscription_generation_min": manifest.subscription_generation_min,
        "subscription_generation_max": manifest.subscription_generation_max,
        "row_count": manifest.row_count,
        "compressed_bytes": manifest.compressed_bytes,
        "first_exchange_event_at": _datetime_to_json(
            manifest.first_exchange_event_at
        ),
        "last_exchange_event_at": _datetime_to_json(
            manifest.last_exchange_event_at
        ),
        "first_received_at": manifest.first_received_at.isoformat(),
        "last_received_at": manifest.last_received_at.isoformat(),
        "sha256": manifest.sha256,
        "capture_version": manifest.capture_version,
        "recovery_status": manifest.recovery_status,
        "known_gap_count": manifest.known_gap_count,
        "created_at": manifest.created_at.isoformat(),
    }


def _manifest_from_payload(payload: dict[str, Any]) -> ArchiveManifest:
    return ArchiveManifest(
        manifest_id=UUID(payload["manifest_id"]),
        schema_version=payload["schema_version"],
        exchange=payload["exchange"],
        environment=payload["environment"],
        route=CaptureRoute(payload["route"]),
        stream=CaptureStream(payload["stream"]),
        symbol=payload["symbol"],
        utc_date=date.fromisoformat(payload["utc_date"]),
        utc_hour=payload["utc_hour"],
        relative_path=Path(payload["relative_path"]),
        connection_session_id=UUID(payload["connection_session_id"]),
        subscription_generation_min=payload["subscription_generation_min"],
        subscription_generation_max=payload["subscription_generation_max"],
        row_count=payload["row_count"],
        compressed_bytes=payload["compressed_bytes"],
        first_exchange_event_at=_datetime_from_json(
            payload["first_exchange_event_at"]
        ),
        last_exchange_event_at=_datetime_from_json(
            payload["last_exchange_event_at"]
        ),
        first_received_at=datetime.fromisoformat(payload["first_received_at"]),
        last_received_at=datetime.fromisoformat(payload["last_received_at"]),
        sha256=payload["sha256"],
        capture_version=payload["capture_version"],
        recovery_status=payload["recovery_status"],
        known_gap_count=payload["known_gap_count"],
        created_at=datetime.fromisoformat(payload["created_at"]),
    )


def _process_state_to_payload(
    record: PendingProcessState,
) -> dict[str, str | None]:
    return {
        "state": record.state.value,
        "occurred_at": record.occurred_at.isoformat(),
        "reason": record.reason,
    }


def _process_state_from_payload(
    payload: dict[str, Any],
) -> PendingProcessState:
    return PendingProcessState(
        state=MarketDataState(payload["state"]),
        occurred_at=datetime.fromisoformat(payload["occurred_at"]),
        reason=payload["reason"],
    )


def _datetime_to_json(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _datetime_from_json(value: str | None) -> datetime | None:
    return None if value is None else datetime.fromisoformat(value)


def _write_json_atomic(
    directory: Path,
    filename: str,
    payload: dict[str, Any],
) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    temporary = directory / f".{filename}.tmp"
    final = directory / filename
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(payload, stream, sort_keys=True, separators=(",", ":"))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, final)
    _fsync_directory(directory)


def _read_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as stream:
        data = json.load(stream)
    if not isinstance(data, dict):
        raise ValueError("journal entry must be a JSON object")
    return data


def _journal_entries(directory: Path) -> tuple[Path, ...]:
    if not directory.exists():
        return ()
    return tuple(sorted(path for path in directory.glob("*.json") if path.is_file()))


def _delete_journal_entry(path: Path) -> None:
    path.unlink()
    _fsync_directory(path.parent)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
