import json
from collections.abc import Iterable, Iterator
from datetime import datetime
from pathlib import Path
from typing import cast
from uuid import UUID

import zstandard

from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    CaptureStream,
    JsonValue,
    RawEnvelope,
)


class RawArchiveRowError(ValueError):
    pass


def deserialize_envelope_row(row: str) -> RawEnvelope:
    try:
        decoded = json.loads(row)
    except json.JSONDecodeError as exc:
        raise RawArchiveRowError("archive row is not valid JSON") from exc
    if not isinstance(decoded, dict):
        raise RawArchiveRowError("archive row must be an object")

    try:
        route = CaptureRoute(_required_str(decoded, "route"))
        stream = CaptureStream(_required_str(decoded, "stream"))
        exchange_event_at = _optional_datetime(decoded, "exchange_event_at")
        received_at = _required_datetime(decoded, "received_at")
        return RawEnvelope(
            schema_version=_required_int(decoded, "schema_version"),
            exchange=_required_str(decoded, "exchange"),
            environment=_required_str(decoded, "environment"),
            route=route,
            stream=stream,
            symbol=_optional_str(decoded, "symbol"),
            exchange_event_at=exchange_event_at,
            received_at=received_at,
            received_monotonic_ns=_required_int(
                decoded,
                "received_monotonic_ns",
            ),
            connection_session_id=UUID(
                _required_str(decoded, "connection_session_id")
            ),
            local_sequence=_required_int(decoded, "local_sequence"),
            exchange_sequence=_optional_str(decoded, "exchange_sequence"),
            subscription_generation=_required_int(
                decoded,
                "subscription_generation",
            ),
            raw_payload=cast(JsonValue, decoded.get("raw_payload")),
            recovered=_optional_bool(decoded, "recovered", default=False),
        )
    except (TypeError, ValueError) as exc:
        if isinstance(exc, RawArchiveRowError):
            raise
        raise RawArchiveRowError("archive row has invalid fields") from exc


def iter_archive_file(path: Path) -> Iterator[RawEnvelope]:
    with zstandard.open(path, "rt", encoding="utf-8") as stream:
        for line in stream:
            yield deserialize_envelope_row(line)


def replay_envelopes(paths: Iterable[Path]) -> tuple[RawEnvelope, ...]:
    envelopes = [
        envelope
        for path in paths
        for envelope in iter_archive_file(path)
    ]
    return tuple(
        sorted(
            envelopes,
            key=lambda item: (
                item.received_at,
                item.received_monotonic_ns,
                str(item.connection_session_id),
                item.local_sequence,
            ),
        )
    )


def _required_str(row: dict[object, object], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str):
        raise RawArchiveRowError(f"{key} must be a string")
    return value


def _optional_str(row: dict[object, object], key: str) -> str | None:
    value = row.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise RawArchiveRowError(f"{key} must be a string or null")
    return value


def _optional_bool(
    row: dict[object, object],
    key: str,
    *,
    default: bool,
) -> bool:
    if key not in row:
        return default
    value = row[key]
    if not isinstance(value, bool):
        raise RawArchiveRowError(f"{key} must be a boolean")
    return value


def _required_int(row: dict[object, object], key: str) -> int:
    value = row.get(key)
    if not isinstance(value, int):
        raise RawArchiveRowError(f"{key} must be an integer")
    return value


def _required_datetime(row: dict[object, object], key: str) -> datetime:
    value = _required_str(row, key)
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RawArchiveRowError(f"{key} must be timezone-aware")
    return parsed


def _optional_datetime(row: dict[object, object], key: str) -> datetime | None:
    value = row.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise RawArchiveRowError(f"{key} must be a string or null")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RawArchiveRowError(f"{key} must be timezone-aware")
    return parsed
