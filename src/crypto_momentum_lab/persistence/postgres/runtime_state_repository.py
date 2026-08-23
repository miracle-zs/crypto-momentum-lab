from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast

from sqlalchemy import and_, func, or_, select, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.persistence.postgres.models import (
    RuntimeMarketState15sRow,
)

_MAX_RUNTIME_STATE_INSERT_ROWS = 500
type _RuntimeStateKey = tuple[str, str, datetime]


@dataclass(frozen=True, slots=True)
class RuntimeStateCursor:
    bucket_start: datetime | None = None
    symbol: str | None = None


@dataclass(frozen=True, slots=True)
class RuntimeStateSequenceRange:
    minimum: int | None = None
    maximum: int | None = None


def validate_closed_states(states: tuple[MarketState15s, ...]) -> None:
    seen: set[tuple[str, str, datetime]] = set()
    for state in states:
        _require_aware(state.bucket_start, "bucket_start")
        _require_aware(state.bucket_end, "bucket_end")
        if state.first_received_at is not None:
            _require_aware(state.first_received_at, "first_received_at")
        if state.last_received_at is not None:
            _require_aware(state.last_received_at, "last_received_at")
        if state.bucket_end <= state.bucket_start:
            raise ValueError("bucket_end must be after bucket_start")
        if not state.environment.strip():
            raise ValueError("environment must not be empty")
        if not state.symbol.strip():
            raise ValueError("symbol must not be empty")
        key = (state.environment, state.symbol, state.bucket_start)
        if key in seen:
            raise ValueError("duplicate runtime market state")
        seen.add(key)


def runtime_state_row(
    state: MarketState15s,
    *,
    source_watermark_at: datetime,
    input_sequence_min: int | None,
    input_sequence_max: int | None,
    closure_reason: str = "watermark_elapsed",
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
) -> dict[str, object]:
    _require_aware(source_watermark_at, "source_watermark_at")
    if created_at is not None:
        _require_aware(created_at, "created_at")
    if updated_at is not None:
        _require_aware(updated_at, "updated_at")
    captured_at = datetime.now(UTC)
    return {
        "environment": state.environment,
        "symbol": state.symbol,
        "bucket_start": state.bucket_start,
        "schema_version": state.schema_version,
        "exchange": state.exchange,
        "bucket_end": state.bucket_end,
        "open_price": state.open_price,
        "high_price": state.high_price,
        "low_price": state.low_price,
        "close_price": state.close_price,
        "trade_count": state.trade_count,
        "trade_notional": state.trade_notional,
        "aggressive_buy_notional": state.aggressive_buy_notional,
        "aggressive_sell_notional": state.aggressive_sell_notional,
        "last_bid_price": state.last_bid_price,
        "last_ask_price": state.last_ask_price,
        "spread": state.spread,
        "midpoint": state.midpoint,
        "liquidation_count": state.liquidation_count,
        "liquidation_notional": state.liquidation_notional,
        "mark_price": state.mark_price,
        "closed_kline_count": state.closed_kline_count,
        "closed_kline_1m_open_time": state.closed_kline_1m_open_time,
        "closed_kline_1m_close_time": state.closed_kline_1m_close_time,
        "closed_kline_1m_open_price": state.closed_kline_1m_open_price,
        "closed_kline_1m_close_price": state.closed_kline_1m_close_price,
        "source_event_count": state.source_event_count,
        "first_received_at": state.first_received_at,
        "last_received_at": state.last_received_at,
        "created_at": captured_at if created_at is None else created_at,
        "updated_at": captured_at if updated_at is None else updated_at,
        "source_watermark_at": source_watermark_at,
        "closure_reason": closure_reason,
        "input_sequence_min": input_sequence_min,
        "input_sequence_max": input_sequence_max,
    }


def market_state_from_row(row: RuntimeMarketState15sRow) -> MarketState15s:
    return MarketState15s(
        schema_version=row.schema_version,
        exchange=row.exchange,
        environment=row.environment,
        symbol=row.symbol,
        bucket_start=row.bucket_start,
        bucket_end=row.bucket_end,
        open_price=row.open_price,
        high_price=row.high_price,
        low_price=row.low_price,
        close_price=row.close_price,
        trade_count=row.trade_count,
        trade_notional=row.trade_notional,
        aggressive_buy_notional=row.aggressive_buy_notional,
        aggressive_sell_notional=row.aggressive_sell_notional,
        last_bid_price=row.last_bid_price,
        last_ask_price=row.last_ask_price,
        spread=row.spread,
        midpoint=row.midpoint,
        liquidation_count=row.liquidation_count,
        liquidation_notional=row.liquidation_notional,
        mark_price=row.mark_price,
        closed_kline_count=row.closed_kline_count,
        closed_kline_1m_open_time=row.closed_kline_1m_open_time,
        closed_kline_1m_close_time=row.closed_kline_1m_close_time,
        closed_kline_1m_open_price=row.closed_kline_1m_open_price,
        closed_kline_1m_close_price=row.closed_kline_1m_close_price,
        source_event_count=row.source_event_count,
        first_received_at=row.first_received_at,
        last_received_at=row.last_received_at,
    )


class PostgresRuntimeMarketStateRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory

    async def save_closed_states(
        self,
        states: tuple[MarketState15s, ...],
        *,
        source_watermark_at: datetime,
        sequence_range: RuntimeStateSequenceRange,
    ) -> None:
        validate_closed_states(states)
        _require_aware(source_watermark_at, "source_watermark_at")
        _validate_sequence_range(sequence_range)
        if not states:
            return
        values = [
            runtime_state_row(
                state,
                source_watermark_at=source_watermark_at,
                input_sequence_min=sequence_range.minimum,
                input_sequence_max=sequence_range.maximum,
            )
            for state in states
        ]
        async with self._session_factory() as session:
            async with session.begin():
                await _insert_many_idempotent(session, values)

    async def load_after(
        self,
        *,
        environment: str,
        cursor: RuntimeStateCursor,
        limit: int,
    ) -> tuple[MarketState15s, ...]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        if not environment.strip():
            raise ValueError("environment must not be empty")
        if cursor.bucket_start is not None:
            _require_aware(cursor.bucket_start, "cursor.bucket_start")

        statement = (
            select(RuntimeMarketState15sRow)
            .where(RuntimeMarketState15sRow.environment == environment)
            .order_by(
                RuntimeMarketState15sRow.bucket_start,
                RuntimeMarketState15sRow.symbol,
            )
            .limit(limit)
        )
        if cursor.bucket_start is not None:
            symbol = "" if cursor.symbol is None else cursor.symbol
            statement = statement.where(
                or_(
                    RuntimeMarketState15sRow.bucket_start
                    > cursor.bucket_start,
                    (
                        RuntimeMarketState15sRow.bucket_start
                        == cursor.bucket_start
                    )
                    & (RuntimeMarketState15sRow.symbol > symbol),
                )
            )

        async with self._session_factory() as session:
            rows = (await session.execute(statement)).scalars()
            return tuple(market_state_from_row(row) for row in rows)

    async def load_latest_bucket(self, *, environment: str) -> datetime | None:
        if not environment.strip():
            raise ValueError("environment must not be empty")
        async with self._session_factory() as session:
            latest = await session.scalar(
                select(func.max(RuntimeMarketState15sRow.bucket_start)).where(
                    RuntimeMarketState15sRow.environment == environment
                )
            )
        return latest

    async def load_recovery_window(
        self,
        *,
        environment: str,
        last_processed_at_by_symbol: Mapping[str, datetime],
        lookback_seconds: int,
        limit: int,
    ) -> tuple[MarketState15s, ...]:
        """Load only the derivable history needed to rebuild a checkpoint.

        Each symbol has its own recovery boundary because a checkpoint can
        contain the latest state for symbols at different times.  The primary
        key's ``(environment, symbol, bucket_start)`` order makes these
        bounded per-symbol ranges indexable without scanning the full hot
        table.
        """
        if not environment.strip():
            raise ValueError("environment must not be empty")
        if lookback_seconds <= 0:
            raise ValueError("lookback_seconds must be positive")
        if limit <= 0:
            raise ValueError("limit must be positive")
        if not last_processed_at_by_symbol:
            return ()

        conditions = []
        for symbol, upper_bound in last_processed_at_by_symbol.items():
            if not symbol.strip():
                raise ValueError("checkpoint symbols must not be empty")
            _require_aware(upper_bound, "last_processed_at_by_symbol value")
            conditions.append(
                and_(
                    RuntimeMarketState15sRow.symbol == symbol,
                    RuntimeMarketState15sRow.bucket_start
                    > upper_bound - timedelta(seconds=lookback_seconds),
                    RuntimeMarketState15sRow.bucket_start <= upper_bound,
                )
            )

        statement = (
            select(RuntimeMarketState15sRow)
            .where(
                RuntimeMarketState15sRow.environment == environment,
                or_(*conditions),
            )
            .order_by(
                RuntimeMarketState15sRow.bucket_start,
                RuntimeMarketState15sRow.symbol,
            )
            .limit(limit)
        )
        async with self._session_factory() as session:
            rows = (await session.execute(statement)).scalars()
            return tuple(market_state_from_row(row) for row in rows)


async def _insert_many_idempotent(
    session: AsyncSession,
    values: list[dict[str, object]],
) -> None:
    for start in range(0, len(values), _MAX_RUNTIME_STATE_INSERT_ROWS):
        await _insert_batch_idempotent(
            session,
            values[start : start + _MAX_RUNTIME_STATE_INSERT_ROWS],
        )


async def _insert_batch_idempotent(
    session: AsyncSession,
    values: list[dict[str, object]],
) -> None:
    inserted_rows = await session.execute(
        insert(RuntimeMarketState15sRow)
        .values(values)
        .on_conflict_do_nothing()
        .returning(
            RuntimeMarketState15sRow.environment,
            RuntimeMarketState15sRow.symbol,
            RuntimeMarketState15sRow.bucket_start,
        )
    )
    inserted_keys: set[_RuntimeStateKey] = {
        cast(_RuntimeStateKey, tuple(row)) for row in inserted_rows.all()
    }
    keys: set[_RuntimeStateKey] = {
        _runtime_state_key(item) for item in values
    }
    missing_keys = keys - inserted_keys
    if not missing_keys:
        return

    existing_rows = (
        await session.scalars(
            select(RuntimeMarketState15sRow).where(
                tuple_(
                    RuntimeMarketState15sRow.environment,
                    RuntimeMarketState15sRow.symbol,
                    RuntimeMarketState15sRow.bucket_start,
                ).in_(missing_keys)
            )
        )
    ).all()
    existing_by_key = {
        (row.environment, row.symbol, row.bucket_start): row for row in existing_rows
    }
    values_by_key = {_runtime_state_key(item): item for item in values}
    for key in missing_keys:
        existing = existing_by_key.get(key)
        if existing is None:
            raise RuntimeError(
                "runtime market state insert conflicted but the existing row "
                "was not found"
            )
        item = values_by_key[key]
        compare_keys = tuple(
            name for name in item if name not in {"created_at", "updated_at"}
        )
        existing_values = {name: getattr(existing, name) for name in compare_keys}
        new_values = {name: item[name] for name in compare_keys}
        if not _core_fields_match(existing_values, new_values):
            raise ValueError("runtime market state conflict")


def _validate_sequence_range(sequence_range: RuntimeStateSequenceRange) -> None:
    if (
        sequence_range.minimum is not None
        and sequence_range.maximum is not None
        and sequence_range.minimum > sequence_range.maximum
    ):
        raise ValueError("sequence minimum must be <= maximum")


def _runtime_state_key(values: dict[str, object]) -> _RuntimeStateKey:
    return (
        cast(str, values["environment"]),
        cast(str, values["symbol"]),
        cast(datetime, values["bucket_start"]),
    )


def _core_fields_match(
    left: dict[str, object],
    right: dict[str, object],
) -> bool:
    return _normalize_for_compare(left) == _normalize_for_compare(right)


def _normalize_for_compare(value: object) -> object:
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
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
