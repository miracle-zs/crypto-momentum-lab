from dataclasses import asdict
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, cast
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategyCheckpoint,
    StrategyDecision,
    StrategyRunIdentity,
    StrategySide,
)
from crypto_momentum_lab.persistence.postgres.models import (
    OrderIntentCandidateRow,
    PaperEquitySnapshotRow,
    PaperFillRow,
    PaperPositionRow,
    StrategyRunRow,
    StrategyRuntimeCheckpointRow,
    StrategyRuntimeEventRow,
    StrategySignalRow,
)
from crypto_momentum_lab.persistence.postgres.strategy_run_repository import (
    order_intent_candidate_row,
    paper_fill_row,
    strategy_signal_row,
)
from crypto_momentum_lab.strategy_runner.daemon import StrategyRuntimeEvent
from crypto_momentum_lab.strategy_runner.fills import (
    ReplayExecutionConfig,
    SimulatedFill,
)
from crypto_momentum_lab.strategy_runner.portfolio import (
    PaperExitConfig,
    PaperExitMode,
    PaperPosition,
    PaperPositionStatus,
    position_from_entry_fill,
)


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


def paper_live_run_row(
    *,
    identity: StrategyRunIdentity,
    source_description: str,
    execution: ReplayExecutionConfig,
    portfolio: PaperExitConfig,
) -> dict[str, object]:
    return {
        "run_id": identity.run_id,
        "strategy_name": identity.strategy_name,
        "strategy_version": identity.strategy_version,
        "config_hash": identity.config_hash,
        "run_mode": identity.run_mode.value,
        "code_commit": identity.code_commit,
        "created_at": identity.created_at,
        "generated_at": identity.created_at,
        "schema_version": 1,
        "source_paths": list(identity.source_paths),
        "source_description": source_description,
        "execution_config": {
            "fills": _jsonable(asdict(execution)),
            "portfolio": _jsonable(asdict(portfolio)),
        },
        "input_state_count": 0,
        "processed_symbol_count": 0,
        "signal_count": 0,
        "candidate_count": 0,
        "fill_count": 0,
        "pending_candidate_count": 0,
        "rejection_summary": {},
        "summary_counts": {},
        "fill_summary": {},
    }


def candidate_from_row(row: OrderIntentCandidateRow) -> OrderIntentCandidate:
    return OrderIntentCandidate(
        candidate_id=row.candidate_id,
        signal_id=row.signal_id,
        run_id=row.run_id,
        strategy_name=row.strategy_name,
        strategy_version=row.strategy_version,
        config_hash=row.config_hash,
        symbol=row.symbol,
        side=StrategySide(row.side),
        entry_type=EntryType(row.entry_type),
        limit_price=row.limit_price,
        desired_notional=row.desired_notional,
        reduce_only=row.reduce_only,
        expires_at=row.expires_at,
        created_at=row.created_at,
        reason=row.reason,
        features=cast(dict[str, JsonValue], row.features),
    )


def paper_position_row(position: PaperPosition) -> dict[str, object]:
    return {
        "position_id": position.position_id,
        "run_id": position.run_id,
        "entry_fill_id": position.entry_fill_id,
        "signal_id": position.signal_id,
        "symbol": position.symbol,
        "side": position.side.value,
        "status": position.status.value,
        "opened_at": position.opened_at,
        "closed_at": position.closed_at,
        "entry_price": position.entry_price,
        "exit_price": position.exit_price,
        "quantity": position.quantity,
        "entry_notional": position.entry_notional,
        "entry_fee": position.entry_fee,
        "exit_fee": position.exit_fee,
        "last_mark_price": position.last_mark_price,
        "unrealized_pnl": position.unrealized_pnl,
        "realized_pnl": position.realized_pnl,
        "return_pct": position.return_pct,
        "close_reason": position.close_reason,
        "updated_at": position.updated_at,
    }


def paper_position_from_row(row: PaperPositionRow) -> PaperPosition:
    return PaperPosition(
        position_id=row.position_id,
        run_id=row.run_id,
        entry_fill_id=row.entry_fill_id,
        signal_id=row.signal_id,
        symbol=row.symbol,
        side=StrategySide(row.side),
        status=PaperPositionStatus(row.status),
        opened_at=row.opened_at,
        closed_at=row.closed_at,
        entry_price=row.entry_price,
        exit_price=row.exit_price,
        quantity=row.quantity,
        entry_notional=row.entry_notional,
        entry_fee=row.entry_fee,
        exit_fee=row.exit_fee,
        last_mark_price=row.last_mark_price,
        unrealized_pnl=row.unrealized_pnl,
        realized_pnl=row.realized_pnl,
        return_pct=row.return_pct,
        close_reason=row.close_reason,
        updated_at=row.updated_at,
    )


class PostgresPaperDaemonRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory

    async def initialize_run(
        self,
        identity: StrategyRunIdentity,
        source_description: str,
        execution: ReplayExecutionConfig,
        portfolio: PaperExitConfig,
    ) -> None:
        values = paper_live_run_row(
            identity=identity,
            source_description=source_description,
            execution=execution,
            portfolio=portfolio,
        )
        async with self._session_factory() as session:
            async with session.begin():
                existing = await session.scalar(
                    select(StrategyRunRow).where(
                        StrategyRunRow.run_id == identity.run_id
                    )
                )
                if existing is None:
                    await session.execute(insert(StrategyRunRow).values(values))
                    return
                expected = {
                    key: values[key]
                    for key in (
                        "strategy_name",
                        "strategy_version",
                        "config_hash",
                        "run_mode",
                        "code_commit",
                        "source_description",
                        "execution_config",
                    )
                }
                actual = {key: getattr(existing, key) for key in expected}
                if _normalize_paper_run_for_compare(
                    actual
                ) != _normalize_paper_run_for_compare(expected):
                    raise ValueError("paper live run conflict")

    async def load_pending_candidates(
        self,
        run_id: str,
    ) -> tuple[OrderIntentCandidate, ...]:
        if not run_id.strip():
            raise ValueError("run_id must not be empty")
        async with self._session_factory() as session:
            rows = (
                await session.scalars(
                    select(OrderIntentCandidateRow)
                    .outerjoin(
                        PaperFillRow,
                        PaperFillRow.candidate_id
                        == OrderIntentCandidateRow.candidate_id,
                    )
                    .where(
                        OrderIntentCandidateRow.run_id == run_id,
                        PaperFillRow.fill_id.is_(None),
                    )
                    .order_by(
                        OrderIntentCandidateRow.created_at,
                        OrderIntentCandidateRow.candidate_id,
                    )
                )
            ).all()
        return tuple(candidate_from_row(row) for row in rows)

    async def save_decision(self, decision: StrategyDecision) -> None:
        if not decision.signals and not decision.candidates:
            return
        run_ids = {
            *(signal.run_id for signal in decision.signals),
            *(candidate.run_id for candidate in decision.candidates),
        }
        if len(run_ids) != 1:
            raise ValueError("paper decision must reference one run")
        run_id = next(iter(run_ids))
        async with self._session_factory() as session:
            async with session.begin():
                run = await _load_run(session, run_id)
                for signal in decision.signals:
                    await _insert_idempotent(
                        session,
                        StrategySignalRow,
                        strategy_signal_row(signal),
                        "strategy signal conflict",
                    )
                for candidate in decision.candidates:
                    await _insert_idempotent(
                        session,
                        OrderIntentCandidateRow,
                        order_intent_candidate_row(candidate),
                        "order intent candidate conflict",
                    )
                await _refresh_run_counts(session, run)

    async def save_fills(
        self,
        run_id: str,
        fills: tuple[SimulatedFill, ...],
    ) -> tuple[PaperPosition, ...]:
        if not fills:
            return ()
        positions = tuple(
            position
            for fill in fills
            if (position := position_from_entry_fill(run_id, fill)) is not None
        )
        async with self._session_factory() as session:
            async with session.begin():
                run = await _load_run(session, run_id)
                for fill in fills:
                    await _insert_idempotent(
                        session,
                        PaperFillRow,
                        paper_fill_row(fill, run_id=run_id),
                        "paper fill conflict",
                    )
                for position in positions:
                    await _insert_idempotent(
                        session,
                        PaperPositionRow,
                        paper_position_row(position),
                        "paper position conflict",
                    )
                await _refresh_run_counts(session, run)
        return positions

    async def load_open_positions(
        self,
        run_id: str,
    ) -> tuple[PaperPosition, ...]:
        async with self._session_factory() as session:
            rows = (
                await session.scalars(
                    select(PaperPositionRow)
                    .where(
                        PaperPositionRow.run_id == run_id,
                        PaperPositionRow.status
                        == PaperPositionStatus.OPEN.value,
                    )
                    .order_by(
                        PaperPositionRow.opened_at,
                        PaperPositionRow.position_id,
                    )
                )
            ).all()
        return tuple(paper_position_from_row(row) for row in rows)

    async def save_portfolio(
        self,
        run_id: str,
        positions: tuple[PaperPosition, ...],
        observed_at: datetime,
        config: PaperExitConfig,
    ) -> None:
        _require_aware(observed_at, "observed_at")
        async with self._session_factory() as session:
            async with session.begin():
                await _load_run(session, run_id)
                for position in positions:
                    if position.run_id != run_id:
                        raise ValueError("paper position run_id mismatch")
                    row = await session.scalar(
                        select(PaperPositionRow).where(
                            PaperPositionRow.position_id
                            == position.position_id
                        )
                    )
                    if row is None:
                        raise ValueError("paper position is not initialized")
                    for key, value in paper_position_row(position).items():
                        setattr(row, key, value)
                await session.flush()
                snapshot = await _portfolio_snapshot(
                    session=session,
                    run_id=run_id,
                    observed_at=observed_at,
                    config=config,
                )
                statement = insert(PaperEquitySnapshotRow).values(snapshot)
                await session.execute(
                    statement.on_conflict_do_update(
                        index_elements=["snapshot_id"],
                        set_={
                            key: value
                            for key, value in snapshot.items()
                            if key != "snapshot_id"
                        },
                    )
                )

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
    model: type[Any],
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


async def _load_run(session: AsyncSession, run_id: str) -> StrategyRunRow:
    run = await session.scalar(
        select(StrategyRunRow).where(StrategyRunRow.run_id == run_id)
    )
    if run is None:
        raise ValueError("paper live run is not initialized")
    return run


async def _refresh_run_counts(
    session: AsyncSession,
    run: StrategyRunRow,
) -> None:
    run.signal_count = await _row_count(
        session,
        StrategySignalRow,
        StrategySignalRow.run_id == run.run_id,
    )
    run.candidate_count = await _row_count(
        session,
        OrderIntentCandidateRow,
        OrderIntentCandidateRow.run_id == run.run_id,
    )
    run.fill_count = await _row_count(
        session,
        PaperFillRow,
        PaperFillRow.run_id == run.run_id,
    )
    run.pending_candidate_count = max(run.candidate_count - run.fill_count, 0)


async def _portfolio_snapshot(
    *,
    session: AsyncSession,
    run_id: str,
    observed_at: datetime,
    config: PaperExitConfig,
) -> dict[str, object]:
    realized_pnl = await _decimal_sum(
        session,
        PaperPositionRow.realized_pnl,
        PaperPositionRow.run_id == run_id,
        PaperPositionRow.status == PaperPositionStatus.CLOSED.value,
    )
    unrealized_pnl = await _decimal_sum(
        session,
        PaperPositionRow.unrealized_pnl,
        PaperPositionRow.run_id == run_id,
        PaperPositionRow.status == PaperPositionStatus.OPEN.value,
    )
    entry_fees = await _decimal_sum(
        session,
        PaperPositionRow.entry_fee,
        PaperPositionRow.run_id == run_id,
    )
    exit_fees = await _decimal_sum(
        session,
        PaperPositionRow.exit_fee,
        PaperPositionRow.run_id == run_id,
    )
    open_position_count = await _row_count(
        session,
        PaperPositionRow,
        PaperPositionRow.run_id == run_id,
        PaperPositionRow.status == PaperPositionStatus.OPEN.value,
    )
    balance = config.initial_balance + realized_pnl
    return {
        "snapshot_id": (
            f"equity_{uuid5(NAMESPACE_URL, f'{run_id}:{observed_at.isoformat()}')}"
        ),
        "run_id": run_id,
        "observed_at": observed_at,
        "balance": balance,
        "equity": balance + unrealized_pnl,
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized_pnl,
        "total_fees": entry_fees + exit_fees,
        "open_position_count": open_position_count,
    }


async def _decimal_sum(
    session: AsyncSession,
    column: Any,
    *conditions: Any,
) -> Decimal:
    value = await session.scalar(
        select(func.coalesce(func.sum(column), Decimal("0"))).where(*conditions)
    )
    return Decimal(value or 0)


async def _row_count(
    session: AsyncSession,
    model: type[Any],
    *conditions: Any,
) -> int:
    value = await session.scalar(
        select(func.count()).select_from(model).where(*conditions)
    )
    return int(value or 0)


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


def _normalize_paper_run_for_compare(
    value: dict[str, object],
) -> dict[str, object]:
    normalized = cast(dict[str, object], _normalize_for_compare(value))
    execution_config = normalized.get("execution_config")
    if not isinstance(execution_config, dict):
        return normalized
    portfolio = execution_config.get("portfolio")
    if isinstance(portfolio, dict):
        # Runs created before candle exits were introduced imply fixed exits.
        portfolio.setdefault("exit_mode", PaperExitMode.FIXED.value)
    return normalized


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
