from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, cast

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.domain.strategy import StrategyCheckpoint
from crypto_momentum_lab.persistence.postgres.models import (
    StrategyRuntimeCheckpointRow,
    StrategyRuntimeEventRow,
)
from crypto_momentum_lab.strategy_runner.daemon import StrategyRuntimeEvent


def runtime_event_row(event: StrategyRuntimeEvent) -> dict[str, object]:
    return {
        "event_id": event.event_id,
        "run_id": event.run_id,
        "event_type": event.event_type,
        "occurred_at": event.occurred_at,
        "symbol": event.symbol,
        "bucket_start": event.bucket_start,
        "details": _jsonable(event.details),
    }


def checkpoint_row_values(
    *,
    run_id: str,
    checkpoint: StrategyCheckpoint,
    saved_at: datetime,
) -> dict[str, object]:
    if not run_id.strip():
        raise ValueError("run_id must not be empty")
    _require_aware(saved_at, "saved_at")
    return {
        "run_id": run_id,
        "last_processed_at_by_symbol": _jsonable(
            checkpoint.last_processed_at_by_symbol
        ),
        "warmup_buckets_by_symbol": _jsonable(
            checkpoint.warmup_buckets_by_symbol
        ),
        "cooldown_buckets_remaining_by_symbol": _jsonable(
            checkpoint.cooldown_buckets_remaining_by_symbol
        ),
        "payload": _jsonable(checkpoint.payload),
        "saved_at": saved_at,
    }


def checkpoint_from_row_values(
    *,
    last_processed_at_by_symbol: dict[str, object],
    warmup_buckets_by_symbol: dict[str, int],
    cooldown_buckets_remaining_by_symbol: dict[str, int],
    payload: dict[str, JsonValue],
) -> StrategyCheckpoint:
    return StrategyCheckpoint(
        last_processed_at_by_symbol={
            symbol: _parse_datetime(value)
            for symbol, value in last_processed_at_by_symbol.items()
        },
        warmup_buckets_by_symbol=warmup_buckets_by_symbol,
        cooldown_buckets_remaining_by_symbol=cooldown_buckets_remaining_by_symbol,
        payload=payload,
    )


class PostgresPaperDaemonRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory

    async def save_runtime_event(self, event: StrategyRuntimeEvent) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                await _insert_idempotent(
                    session,
                    StrategyRuntimeEventRow,
                    runtime_event_row(event),
                    "strategy runtime event conflict",
                )

    async def save_checkpoint(
        self,
        run_id: str,
        checkpoint: StrategyCheckpoint,
        saved_at: datetime,
    ) -> None:
        values = checkpoint_row_values(
            run_id=run_id,
            checkpoint=checkpoint,
            saved_at=saved_at,
        )
        async with self._session_factory() as session:
            async with session.begin():
                existing = await session.scalar(
                    select(StrategyRuntimeCheckpointRow).where(
                        StrategyRuntimeCheckpointRow.run_id == run_id
                    )
                )
                if existing is None:
                    await session.execute(
                        insert(StrategyRuntimeCheckpointRow).values(values)
                    )
                    return
                for key, value in values.items():
                    setattr(existing, key, value)

    async def load_checkpoint(self, run_id: str) -> StrategyCheckpoint | None:
        if not run_id.strip():
            raise ValueError("run_id must not be empty")
        async with self._session_factory() as session:
            row = await session.scalar(
                select(StrategyRuntimeCheckpointRow).where(
                    StrategyRuntimeCheckpointRow.run_id == run_id
                )
            )
        if row is None:
            return None
        return checkpoint_from_row_values(
            last_processed_at_by_symbol=row.last_processed_at_by_symbol,
            warmup_buckets_by_symbol=row.warmup_buckets_by_symbol,
            cooldown_buckets_remaining_by_symbol=(
                row.cooldown_buckets_remaining_by_symbol
            ),
            payload=cast(dict[str, JsonValue], row.payload),
        )


async def _insert_idempotent(
    session: AsyncSession,
    model: type[StrategyRuntimeEventRow],
    values: dict[str, object],
    conflict_message: str,
) -> None:
    model_any = cast(Any, model)
    primary_key = tuple(
        column.name for column in model_any.__table__.primary_key.columns
    )
    existing = await session.scalar(
        select(model).where(
            *(getattr(model_any, key) == values[key] for key in primary_key)
        )
    )
    if existing is not None:
        existing_values = {key: getattr(existing, key) for key in values}
        if _normalize_for_compare(existing_values) != _normalize_for_compare(values):
            raise ValueError(conflict_message)
        return
    await session.execute(insert(model).values(values))


def _parse_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value)
    else:
        raise ValueError("checkpoint timestamp must be datetime or ISO string")
    _require_aware(parsed, "checkpoint timestamp")
    return parsed


def _jsonable(value: object) -> JsonValue:
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    return str(value)


def _normalize_for_compare(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value.normalize())
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, dict):
        return {
            str(key): _normalize_for_compare(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, list | tuple):
        return [_normalize_for_compare(item) for item in value]
    return value


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
