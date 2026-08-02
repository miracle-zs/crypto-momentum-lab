from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import func, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.persistence.postgres.models import (
    RuntimeMarketState15sRow,
)


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
        async with self._session_factory() as session:
            async with session.begin():
                for state in states:
                    await _insert_idempotent(
                        session,
                        runtime_state_row(
                            state,
                            source_watermark_at=source_watermark_at,
                            input_sequence_min=sequence_range.minimum,
                            input_sequence_max=sequence_range.maximum,
                        ),
                    )

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

async def _insert_idempotent(
    session: AsyncSession,
    values: dict[str, object],
) -> None:
    inserted = await session.scalar(
        insert(RuntimeMarketState15sRow)
        .values(values)
        .on_conflict_do_nothing()
        .returning(RuntimeMarketState15sRow.environment)
    )
    if inserted is not None:
        return

    existing = await session.scalar(
        select(RuntimeMarketState15sRow).where(
            RuntimeMarketState15sRow.environment == values["environment"],
            RuntimeMarketState15sRow.symbol == values["symbol"],
            RuntimeMarketState15sRow.bucket_start == values["bucket_start"],
        )
    )
    if existing is not None:
        compare_keys = tuple(
            key for key in values if key not in {"created_at", "updated_at"}
        )
        existing_values = {
            key: getattr(existing, key) for key in compare_keys
        }
        new_values = {key: values[key] for key in compare_keys}
        if not _core_fields_match(existing_values, new_values):
            raise ValueError("runtime market state conflict")
        return
    raise RuntimeError(
        "runtime market state insert conflicted but the existing row was not found"
    )


def _validate_sequence_range(sequence_range: RuntimeStateSequenceRange) -> None:
    if (
        sequence_range.minimum is not None
        and sequence_range.maximum is not None
        and sequence_range.minimum > sequence_range.maximum
    ):
        raise ValueError("sequence minimum must be <= maximum")


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

