from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, cast

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.persistence.postgres.models import (
    OrderIntentCandidateRow,
    PaperFillRow,
    StrategyCheckpointRow,
    StrategyRunRow,
    StrategySignalRow,
)
from crypto_momentum_lab.strategy_runner.paper import PaperTradingRunReport

type RowModel = (
    type[StrategyRunRow]
    | type[StrategySignalRow]
    | type[OrderIntentCandidateRow]
    | type[PaperFillRow]
    | type[StrategyCheckpointRow]
)


@dataclass(frozen=True, slots=True)
class StrategyRunReportRows:
    run: dict[str, object]
    signals: tuple[dict[str, object], ...]
    candidates: tuple[dict[str, object], ...]
    fills: tuple[dict[str, object], ...]
    checkpoint: dict[str, object]


def validate_paper_report(report: PaperTradingRunReport) -> None:
    run_id = report.run.run_id
    signal_ids = {signal.signal_id for signal in report.signals}
    candidate_ids = {candidate.candidate_id for candidate in report.candidates}

    for signal in report.signals:
        if signal.run_id != run_id:
            raise ValueError("signal run_id mismatch")
        if signal.strategy_name != report.run.strategy_name:
            raise ValueError("signal strategy_name mismatch")
        if signal.strategy_version != report.run.strategy_version:
            raise ValueError("signal strategy_version mismatch")
        if signal.config_hash != report.run.config_hash:
            raise ValueError("signal config_hash mismatch")

    for candidate in report.candidates:
        if candidate.run_id != run_id:
            raise ValueError("candidate run_id mismatch")
        if candidate.strategy_name != report.run.strategy_name:
            raise ValueError("candidate strategy_name mismatch")
        if candidate.strategy_version != report.run.strategy_version:
            raise ValueError("candidate strategy_version mismatch")
        if candidate.config_hash != report.run.config_hash:
            raise ValueError("candidate config_hash mismatch")
        if candidate.signal_id not in signal_ids:
            raise ValueError("candidate references unknown signal_id")

    for fill in report.paper_fills:
        if fill.candidate_id not in candidate_ids:
            raise ValueError("fill references unknown candidate_id")
        if fill.signal_id not in signal_ids:
            raise ValueError("fill references unknown signal_id")


def strategy_run_report_rows(
    report: PaperTradingRunReport,
) -> StrategyRunReportRows:
    validate_paper_report(report)
    return StrategyRunReportRows(
        run={
            "run_id": report.run.run_id,
            "strategy_name": report.run.strategy_name,
            "strategy_version": report.run.strategy_version,
            "config_hash": report.run.config_hash,
            "run_mode": report.run.run_mode.value,
            "code_commit": report.run.code_commit,
            "created_at": report.run.created_at,
            "generated_at": report.generated_at,
            "schema_version": report.schema_version,
            "source_paths": list(report.run.source_paths),
            "source_description": report.source_description,
            "execution_config": _jsonable(asdict(report.execution_config)),
            "input_state_count": report.input_state_count,
            "processed_symbol_count": report.processed_symbol_count,
            "signal_count": len(report.signals),
            "candidate_count": len(report.candidates),
            "fill_count": len(report.paper_fills),
            "pending_candidate_count": report.pending_candidate_count,
            "rejection_summary": _jsonable(report.rejection_summary),
            "summary_counts": _jsonable(report.summary_counts),
            "fill_summary": _jsonable(report.fill_summary),
        },
        signals=tuple(_signal_row(signal) for signal in report.signals),
        candidates=tuple(
            _candidate_row(candidate) for candidate in report.candidates
        ),
        fills=tuple(
            _fill_row(fill, run_id=report.run.run_id)
            for fill in report.paper_fills
        ),
        checkpoint={
            "run_id": report.run.run_id,
            "last_processed_at_by_symbol": _jsonable(
                report.final_checkpoint.last_processed_at_by_symbol
            ),
            "warmup_buckets_by_symbol": _jsonable(
                report.final_checkpoint.warmup_buckets_by_symbol
            ),
            "cooldown_buckets_remaining_by_symbol": _jsonable(
                report.final_checkpoint.cooldown_buckets_remaining_by_symbol
            ),
            "payload": _jsonable(report.final_checkpoint.payload),
            "saved_at": report.generated_at,
        },
    )


def core_fields_match(left: dict[str, object], right: dict[str, object]) -> bool:
    return _normalize_for_compare(left) == _normalize_for_compare(right)


class PostgresStrategyRunRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory

    async def save_paper_report(self, report: PaperTradingRunReport) -> None:
        rows = strategy_run_report_rows(report)
        async with self._session_factory() as session:
            async with session.begin():
                await _insert_idempotent(
                    session,
                    StrategyRunRow,
                    rows.run,
                    "strategy run conflict",
                )
                await _insert_many_idempotent(
                    session,
                    StrategySignalRow,
                    rows.signals,
                    "signal conflict",
                )
                await _insert_many_idempotent(
                    session,
                    OrderIntentCandidateRow,
                    rows.candidates,
                    "candidate conflict",
                )
                await _insert_many_idempotent(
                    session,
                    PaperFillRow,
                    rows.fills,
                    "paper fill conflict",
                )
                await _insert_idempotent(
                    session,
                    StrategyCheckpointRow,
                    rows.checkpoint,
                    "checkpoint conflict",
                )

    async def load_run_summary(
        self,
        run_id: str,
    ) -> dict[str, object] | None:
        async with self._session_factory() as session:
            row = await session.scalar(
                select(StrategyRunRow).where(StrategyRunRow.run_id == run_id)
            )
            if row is None:
                return None
            return _model_values(row)

    async def load_paper_report_artifacts(
        self,
        run_id: str,
    ) -> dict[str, object] | None:
        async with self._session_factory() as session:
            run = await session.scalar(
                select(StrategyRunRow).where(StrategyRunRow.run_id == run_id)
            )
            if run is None:
                return None
            signals = tuple(
                (
                    await session.execute(
                        select(StrategySignalRow)
                        .where(StrategySignalRow.run_id == run_id)
                        .order_by(
                            StrategySignalRow.detected_at,
                            StrategySignalRow.symbol,
                            StrategySignalRow.signal_id,
                        )
                    )
                ).scalars()
            )
            candidates = tuple(
                (
                    await session.execute(
                        select(OrderIntentCandidateRow)
                        .where(OrderIntentCandidateRow.run_id == run_id)
                        .order_by(
                            OrderIntentCandidateRow.created_at,
                            OrderIntentCandidateRow.symbol,
                            OrderIntentCandidateRow.candidate_id,
                        )
                    )
                ).scalars()
            )
            fills = tuple(
                (
                    await session.execute(
                        select(PaperFillRow)
                        .where(PaperFillRow.run_id == run_id)
                        .order_by(
                            PaperFillRow.target_fill_at,
                            PaperFillRow.symbol,
                            PaperFillRow.fill_id,
                        )
                    )
                ).scalars()
            )
            checkpoint = await session.scalar(
                select(StrategyCheckpointRow).where(
                    StrategyCheckpointRow.run_id == run_id
                )
            )

        return {
            "run": _model_values(run),
            "signals": tuple(_model_values(row) for row in signals),
            "candidates": tuple(_model_values(row) for row in candidates),
            "paper_fills": tuple(_model_values(row) for row in fills),
            "checkpoint": None
            if checkpoint is None
            else _model_values(checkpoint),
        }


def _signal_row(signal: Any) -> dict[str, object]:
    return {
        "signal_id": signal.signal_id,
        "run_id": signal.run_id,
        "strategy_name": signal.strategy_name,
        "strategy_version": signal.strategy_version,
        "config_hash": signal.config_hash,
        "symbol": signal.symbol,
        "side": signal.side.value,
        "detected_at": signal.detected_at,
        "source_state_at": signal.source_state_at,
        "reason": signal.reason,
        "features": _jsonable(signal.features),
        "reference_prices": _jsonable(signal.reference_prices),
    }


def _candidate_row(candidate: Any) -> dict[str, object]:
    return {
        "candidate_id": candidate.candidate_id,
        "signal_id": candidate.signal_id,
        "run_id": candidate.run_id,
        "strategy_name": candidate.strategy_name,
        "strategy_version": candidate.strategy_version,
        "config_hash": candidate.config_hash,
        "symbol": candidate.symbol,
        "side": candidate.side.value,
        "entry_type": candidate.entry_type.value,
        "limit_price": candidate.limit_price,
        "desired_notional": candidate.desired_notional,
        "reduce_only": candidate.reduce_only,
        "expires_at": candidate.expires_at,
        "created_at": candidate.created_at,
        "reason": candidate.reason,
        "features": _jsonable(candidate.features),
    }


def _fill_row(fill: Any, *, run_id: str) -> dict[str, object]:
    return {
        "fill_id": fill.fill_id,
        "candidate_id": fill.candidate_id,
        "signal_id": fill.signal_id,
        "run_id": run_id,
        "symbol": fill.symbol,
        "side": fill.side.value,
        "status": fill.status.value,
        "target_fill_at": fill.target_fill_at,
        "filled_at": fill.filled_at,
        "requested_notional": fill.requested_notional,
        "filled_notional": fill.filled_notional,
        "quantity": fill.quantity,
        "reference_midpoint": fill.reference_midpoint,
        "spread": fill.spread,
        "fill_price": fill.fill_price,
        "fee": fill.fee,
        "total_cost": fill.total_cost,
        "cost_bps": fill.cost_bps,
        "reason": fill.reason,
    }


async def _insert_many_idempotent(
    session: AsyncSession,
    model: RowModel,
    values: Sequence[dict[str, object]],
    conflict_message: str,
) -> None:
    for item in values:
        await _insert_idempotent(session, model, item, conflict_message)


async def _insert_idempotent(
    session: AsyncSession,
    model: RowModel,
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
        if not core_fields_match(existing_values, values):
            raise ValueError(conflict_message)
        return
    await session.execute(insert(model).values(values))


def _model_values(row: object) -> dict[str, object]:
    row_any = cast(Any, row)
    return {
        column.name: getattr(row_any, column.name)
        for column in row_any.__table__.columns
    }


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
        return str(value)
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
