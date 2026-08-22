"""Best-effort persistence adapter for live runtime telemetry.

This adapter deliberately has no checkpoint, order, or market-state methods.
Keeping that narrow seam lets the live process place telemetry on its own pool
and use a non-durable commit policy without weakening the execution journal.
"""

from collections.abc import Mapping, Sequence
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.persistence.postgres.models import StrategyRuntimeEventRow


class PostgresRuntimeTelemetryRepository:
    """Persist only the runtime-event stream on an observability session pool."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def save_runtime_events(
        self,
        events: Sequence[Mapping[str, object]],
    ) -> None:
        if not events:
            return
        values = tuple(_runtime_event_row(event) for event in events)
        async with self._session_factory() as session:
            async with session.begin():
                # Telemetry is diagnostic, not an execution record.  Do not
                # make an order submit wait for a WAL flush caused by this
                # best-effort batch.
                await session.execute(text("SET LOCAL synchronous_commit = OFF"))
                await session.execute(
                    insert(StrategyRuntimeEventRow)
                    .values(values)
                    .on_conflict_do_nothing(index_elements=["event_id"])
                )


def _runtime_event_row(event: Mapping[str, object]) -> dict[str, object]:
    event_id = _required_text(event, "event_id")
    run_id = _required_text(event, "run_id")
    event_type = _required_text(event, "event_type")
    occurred_at = _required_datetime(event, "occurred_at")
    bucket_start = _optional_datetime(event, "bucket_start")
    symbol = _optional_string(event, "symbol")
    details = event.get("details", {})
    if not isinstance(details, Mapping):
        raise ValueError("runtime event details must be an object")
    return {
        "event_id": event_id,
        "run_id": run_id,
        "event_type": event_type,
        "occurred_at": occurred_at,
        "symbol": symbol,
        "bucket_start": bucket_start,
        "details": _jsonable(dict(details)),
    }


def _required_text(event: Mapping[str, object], field_name: str) -> str:
    value = event.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"runtime event {field_name} must be non-empty")
    return value


def _required_datetime(event: Mapping[str, object], field_name: str) -> datetime:
    value = event.get(field_name)
    if not isinstance(value, datetime):
        raise ValueError(f"runtime event {field_name} must be a datetime")
    _require_aware(value, field_name)
    return value


def _optional_datetime(
    event: Mapping[str, object],
    field_name: str,
) -> datetime | None:
    value = event.get(field_name)
    if value is not None and not isinstance(value, datetime):
        raise ValueError(f"runtime event {field_name} must be a datetime or None")
    if value is not None:
        _require_aware(value, field_name)
    return value


def _optional_string(
    event: Mapping[str, object],
    field_name: str,
) -> str | None:
    value = event.get(field_name)
    if value is not None and not isinstance(value, str):
        raise ValueError(f"runtime event {field_name} must be a string or None")
    if value is not None and not value.strip():
        raise ValueError(f"runtime event {field_name} must not be blank")
    return value


def _jsonable(value: object) -> object:
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    return str(value)


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


__all__ = ["PostgresRuntimeTelemetryRepository"]
