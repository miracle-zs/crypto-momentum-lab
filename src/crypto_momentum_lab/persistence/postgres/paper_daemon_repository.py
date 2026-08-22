from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, cast
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import case, func, select
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
    StrategySignalRow,
)
from crypto_momentum_lab.persistence.postgres.strategy_run_repository import (
    order_intent_candidate_row,
    paper_fill_row,
    strategy_signal_row,
)
from crypto_momentum_lab.strategy_runner.daemon import PaperEntryFilterConfig
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

_NEW_EXECUTION_FIELDS = {
    "fills": ("require_market_quote",),
    "entry_filter": (
        "allow_long",
        "allow_short",
        "max_abs_aggressive_imbalance",
        "max_cluster_trade_count",
        "require_price_above_ema5",
        "require_price_above_ema10",
    ),
    "portfolio": (
        "require_executable_quote",
        "candle_minimum_holding_buckets",
        "candle_confirmation_count",
        "candle_grace_bars",
        "candle_grace_profit_pct",
    ),
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
    entry_filter: PaperEntryFilterConfig,
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
            "entry_filter": _jsonable(asdict(entry_filter)),
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
        "grace_exit_started_at": position.grace_exit_started_at,
        "grace_exit_deadline": position.grace_exit_deadline,
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
        grace_exit_started_at=row.grace_exit_started_at,
        grace_exit_deadline=row.grace_exit_deadline,
        updated_at=row.updated_at,
    )


class PostgresPaperDaemonRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory
        self._portfolio_stats: dict[str, _PortfolioStats] = {}

    async def initialize_run(
        self,
        identity: StrategyRunIdentity,
        source_description: str,
        execution: ReplayExecutionConfig,
        portfolio: PaperExitConfig,
        entry_filter: PaperEntryFilterConfig,
    ) -> None:
        values = paper_live_run_row(
            identity=identity,
            source_description=source_description,
            execution=execution,
            portfolio=portfolio,
            entry_filter=entry_filter,
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
                    upgrade_values = _legacy_paper_run_upgrade_values(
                        actual=actual,
                        expected=expected,
                    )
                    if upgrade_values is None:
                        raise ValueError("paper live run conflict")
                    existing.code_commit = cast(
                        str,
                        upgrade_values["code_commit"],
                    )
                    existing.execution_config = cast(
                        dict[str, object],
                        upgrade_values["execution_config"],
                    )
        self._portfolio_stats.pop(identity.run_id, None)

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
                inserted_signal_count = 0
                for signal in decision.signals:
                    inserted_signal_count += int(
                        await _insert_idempotent(
                            session,
                            StrategySignalRow,
                            strategy_signal_row(signal),
                            "strategy signal conflict",
                        )
                    )
                inserted_candidate_count = 0
                for candidate in decision.candidates:
                    inserted_candidate_count += int(
                        await _insert_idempotent(
                            session,
                            OrderIntentCandidateRow,
                            order_intent_candidate_row(candidate),
                            "order intent candidate conflict",
                        )
                    )
                run.signal_count += inserted_signal_count
                run.candidate_count += inserted_candidate_count
                run.pending_candidate_count = max(
                    run.candidate_count - run.fill_count,
                    0,
                )

    async def save_fills(
        self,
        run_id: str,
        fills: tuple[SimulatedFill, ...],
    ) -> tuple[PaperPosition, ...]:
        if not fills:
            return ()
        opened_positions: list[PaperPosition] = []
        async with self._session_factory() as session:
            async with session.begin():
                run = await _load_run(session, run_id)
                inserted_fill_count = 0
                for fill in fills:
                    inserted = await _insert_idempotent(
                        session,
                        PaperFillRow,
                        paper_fill_row(fill, run_id=run_id),
                        "paper fill conflict",
                    )
                    if not inserted:
                        continue
                    inserted_fill_count += 1
                    position = position_from_entry_fill(run_id, fill)
                    if position is None:
                        continue
                    await _insert_idempotent(
                        session,
                        PaperPositionRow,
                        paper_position_row(position),
                        "paper position conflict",
                    )
                    opened_positions.append(position)
                run.fill_count += inserted_fill_count
                run.pending_candidate_count = max(
                    run.candidate_count - run.fill_count,
                    0,
                )
        if opened_positions:
            # A newly filled candidate creates an open position before the next
            # mark update. Reconcile the cached aggregate on the next snapshot.
            self._portfolio_stats.pop(run_id, None)
        return tuple(opened_positions)

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

    async def load_open_position_symbols(
        self,
        run_ids: frozenset[str],
    ) -> frozenset[str]:
        if not run_ids:
            return frozenset()
        async with self._session_factory() as session:
            symbols = (
                await session.scalars(
                    select(PaperPositionRow.symbol)
                    .where(
                        PaperPositionRow.run_id.in_(run_ids),
                        PaperPositionRow.status
                        == PaperPositionStatus.OPEN.value,
                    )
                    .distinct()
                    .order_by(PaperPositionRow.symbol)
                )
            ).all()
        return frozenset(symbols)

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
                stats = self._portfolio_stats.get(run_id)
                if stats is None:
                    stats = await _load_portfolio_stats(session, run_id)
                next_stats = replace(stats)
                for position in positions:
                    row = await session.scalar(
                        select(PaperPositionRow).where(
                            PaperPositionRow.position_id
                            == position.position_id
                        )
                    )
                    if row is None:
                        raise ValueError("paper position is not initialized")
                    next_stats.apply(row, -1)
                    for key, value in paper_position_row(position).items():
                        setattr(row, key, value)
                    next_stats.apply(position, 1)
                await session.flush()
                snapshot = await _portfolio_snapshot(
                    run_id=run_id,
                    observed_at=observed_at,
                    config=config,
                    stats=next_stats,
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
                self._portfolio_stats[run_id] = next_stats

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
) -> bool:
    model_any = cast(Any, model)
    primary_key = tuple(
        column.name for column in model_any.__table__.primary_key.columns
    )
    inserted = (
        await session.execute(
            insert(model)
            .values(values)
            .on_conflict_do_nothing()
            .returning(
                *tuple(getattr(model_any, key) for key in primary_key)
            )
        )
    ).first()
    if inserted is not None:
        return True

    existing = await session.scalar(
        select(model).where(
            *(getattr(model_any, key) == values[key] for key in primary_key)
        )
    )
    if existing is not None:
        existing_values = {key: getattr(existing, key) for key in values}
        if _normalize_for_compare(existing_values) != _normalize_for_compare(values):
            raise ValueError(conflict_message)
        return False
    raise RuntimeError(
        f"{conflict_message}: conflicting row disappeared before comparison"
    )


async def _load_run(session: AsyncSession, run_id: str) -> StrategyRunRow:
    run = await session.scalar(
        select(StrategyRunRow).where(StrategyRunRow.run_id == run_id)
    )
    if run is None:
        raise ValueError("paper live run is not initialized")
    return run


@dataclass(slots=True)
class _PortfolioStats:
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    entry_fees: Decimal = Decimal("0")
    exit_fees: Decimal = Decimal("0")
    open_position_count: int = 0

    def apply(
        self,
        position: PaperPosition | PaperPositionRow,
        multiplier: int,
    ) -> None:
        status = position.status
        status_value = getattr(status, "value", status)
        if status_value == PaperPositionStatus.CLOSED.value:
            self.realized_pnl += (position.realized_pnl or Decimal("0")) * multiplier
        elif status_value == PaperPositionStatus.OPEN.value:
            self.unrealized_pnl += position.unrealized_pnl * multiplier
            self.open_position_count += multiplier
        else:
            raise ValueError(f"unknown paper position status: {status_value}")
        self.entry_fees += position.entry_fee * multiplier
        self.exit_fees += position.exit_fee * multiplier


async def _load_portfolio_stats(
    session: AsyncSession,
    run_id: str,
) -> _PortfolioStats:
    statement = select(
        func.coalesce(
            func.sum(
                case(
                    (
                        PaperPositionRow.status
                        == PaperPositionStatus.CLOSED.value,
                        PaperPositionRow.realized_pnl,
                    ),
                    else_=Decimal("0"),
                )
            ),
            Decimal("0"),
        ),
        func.coalesce(
            func.sum(
                case(
                    (
                        PaperPositionRow.status
                        == PaperPositionStatus.OPEN.value,
                        PaperPositionRow.unrealized_pnl,
                    ),
                    else_=Decimal("0"),
                )
            ),
            Decimal("0"),
        ),
        func.coalesce(func.sum(PaperPositionRow.entry_fee), Decimal("0")),
        func.coalesce(func.sum(PaperPositionRow.exit_fee), Decimal("0")),
        func.coalesce(
            func.sum(
                case(
                    (
                        PaperPositionRow.status
                        == PaperPositionStatus.OPEN.value,
                        1,
                    ),
                    else_=0,
                )
            ),
            0,
        ),
    ).where(PaperPositionRow.run_id == run_id)
    values = (await session.execute(statement)).one()
    return _PortfolioStats(
        realized_pnl=Decimal(values[0] or 0),
        unrealized_pnl=Decimal(values[1] or 0),
        entry_fees=Decimal(values[2] or 0),
        exit_fees=Decimal(values[3] or 0),
        open_position_count=int(values[4] or 0),
    )


async def _portfolio_snapshot(
    *,
    run_id: str,
    observed_at: datetime,
    config: PaperExitConfig,
    stats: _PortfolioStats,
) -> dict[str, object]:
    balance = config.initial_balance + stats.realized_pnl
    return {
        "snapshot_id": (
            f"equity_{uuid5(NAMESPACE_URL, f'{run_id}:{observed_at.isoformat()}')}"
        ),
        "run_id": run_id,
        "observed_at": observed_at,
        "balance": balance,
        "equity": balance + stats.unrealized_pnl,
        "realized_pnl": stats.realized_pnl,
        "unrealized_pnl": stats.unrealized_pnl,
        "total_fees": stats.entry_fees + stats.exit_fees,
        "open_position_count": stats.open_position_count,
    }


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
        return format(value.normalize(), "f")
    if isinstance(value, datetime):
        return (
            value.astimezone(UTC).isoformat()
            if value.tzinfo is not None and value.utcoffset() is not None
            else value.isoformat()
        )
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    return str(value)


def _normalize_for_compare(value: object) -> object:
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, datetime):
        return (
            value.astimezone(UTC).isoformat()
            if value.tzinfo is not None and value.utcoffset() is not None
            else value.isoformat()
        )
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


def _legacy_paper_run_upgrade_values(
    *,
    actual: dict[str, object],
    expected: dict[str, object],
) -> dict[str, object] | None:
    """Return safe in-place upgrades for compatible paper runs."""
    normalized_actual = _normalize_paper_run_for_compare(actual)
    normalized_expected = _normalize_paper_run_for_compare(expected)
    actual_without_commit = {
        key: value
        for key, value in normalized_actual.items()
        if key != "code_commit"
    }
    expected_without_commit = {
        key: value
        for key, value in normalized_expected.items()
        if key != "code_commit"
    }
    if actual_without_commit == expected_without_commit:
        return {
            "code_commit": expected["code_commit"],
            "execution_config": expected["execution_config"],
        }

    actual_execution = normalized_actual.get("execution_config")
    expected_execution = normalized_expected.get("execution_config")
    if not isinstance(actual_execution, dict) or not isinstance(
        expected_execution, dict
    ):
        return None

    upgraded_execution = dict(actual_execution)
    for section, field_names in _NEW_EXECUTION_FIELDS.items():
        actual_section = actual_execution.get(section)
        expected_section = expected_execution.get(section)
        if not isinstance(expected_section, dict):
            return None
        if actual_section is not None and not isinstance(actual_section, dict):
            return None
        upgraded_section = (
            {} if actual_section is None else dict(actual_section)
        )
        for field_name in field_names:
            if field_name in upgraded_section:
                continue
            if field_name not in expected_section:
                return None
            upgraded_section[field_name] = expected_section[field_name]
        upgraded_execution[section] = upgraded_section

    upgraded_actual = dict(normalized_actual)
    upgraded_actual["code_commit"] = normalized_expected.get("code_commit")
    upgraded_actual["execution_config"] = upgraded_execution
    if upgraded_actual != normalized_expected:
        return None
    return {
        "code_commit": expected["code_commit"],
        "execution_config": expected["execution_config"],
    }


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
