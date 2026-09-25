"""Risk and exchange-order queries for the operator dashboard.

This module owns the fail-closed read model for active risk halts, recent risk
decisions, and exchange orders.  The public interface is intentionally one
operation; order-state classification and response shaping stay behind this
seam so callers cannot accidentally treat an unknown state as safe.
"""

from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution import ExchangeOrderState
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.operator_dashboard.schemas import RiskExecutionResponse
from crypto_momentum_lab.operator_dashboard.status import OperationalStatus
from crypto_momentum_lab.persistence.postgres.models import (
    ExchangeOrderRow,
    MonitoringMembershipRow,
    RiskEvaluationRow,
    RiskHaltRow,
    RuntimeMarketState15sRow,
    UniverseSnapshotRow,
)

_CONFIRMED_OPEN_ORDER_STATES = frozenset(
    {
        ExchangeOrderState.ACKNOWLEDGED.value,
        ExchangeOrderState.PARTIALLY_FILLED.value,
    }
)
_RECENT_DECISION_LIMIT = 30
_RECENT_ORDER_LIMIT = 30


def split_exchange_orders(
    rows: Sequence[ExchangeOrderRow],
) -> tuple[list[ExchangeOrderRow], list[ExchangeOrderRow]]:
    """Separate confirmed resting orders from genuinely uncertain orders."""
    terminal_states = {
        state.value for state in ExchangeOrderState if state.terminal
    }
    pending: list[ExchangeOrderRow] = []
    ambiguous: list[ExchangeOrderRow] = []
    for row in rows:
        if row.state in terminal_states:
            continue
        if row.state in _CONFIRMED_OPEN_ORDER_STATES:
            pending.append(row)
        else:
            # Unknown/non-terminal states must remain fail-closed.
            ambiguous.append(row)
    return pending, ambiguous


def exchange_order(row: ExchangeOrderRow) -> dict[str, JsonValue]:
    return {
        "client_order_id": row.client_order_id,
        "exchange_order_id": row.exchange_order_id,
        "symbol": row.symbol,
        "side": row.side,
        "state": row.state,
        "quantity": str(row.quantity),
        "updated_at": row.updated_at.isoformat(),
    }


class RiskExecutionQueries:
    """Deep query module for the dashboard risk/execution read model."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        environment: str = "live",
        required_symbols: Sequence[str] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._environment = environment
        self._required_symbols = (
            tuple(required_symbols) if required_symbols is not None else None
        )

    async def risk_execution(self) -> RiskExecutionResponse:
        async with self._session_factory() as session:
            halts = (
                await session.scalars(
                    select(RiskHaltRow)
                    .where(RiskHaltRow.active.is_(True))
                    .order_by(RiskHaltRow.created_at.desc())
                )
            ).all()
            decisions = (
                await session.scalars(
                    select(RiskEvaluationRow)
                    .order_by(RiskEvaluationRow.evaluated_at.desc())
                    .limit(_RECENT_DECISION_LIMIT)
                )
            ).all()
            orders = (
                await session.scalars(
                    select(ExchangeOrderRow)
                    .order_by(ExchangeOrderRow.updated_at.desc())
                    .limit(_RECENT_ORDER_LIMIT)
                )
            ).all()

            pending, ambiguous = split_exchange_orders(orders)

            # Determine strategy-required symbols to evaluate coverage & freshness
            if self._required_symbols is not None:
                required_symbols = set(self._required_symbols)
            else:
                required_symbols = set()
                # Include symbols from active open/ambiguous orders
                for o in (*pending, *ambiguous):
                    if o.symbol:
                        required_symbols.add(o.symbol)
                # Include monitored symbols from latest active universe snapshot
                try:
                    snapshot_id = await session.scalar(
                        select(UniverseSnapshotRow.snapshot_id)
                        .where(UniverseSnapshotRow.activated.is_(True))
                        .order_by(UniverseSnapshotRow.observed_at.desc())
                        .limit(1)
                    )
                    if snapshot_id is not None and isinstance(snapshot_id, (str, int)):
                        monitored = (
                            await session.scalars(
                                select(MonitoringMembershipRow.symbol).where(
                                    MonitoringMembershipRow.snapshot_id == snapshot_id
                                )
                            )
                        ).all()
                        required_symbols.update(monitored)
                except Exception:
                    pass

            market_query = (
                select(
                    RuntimeMarketState15sRow.symbol,
                    func.max(RuntimeMarketState15sRow.bucket_end),
                )
                .where(
                    RuntimeMarketState15sRow.environment == self._environment,
                    RuntimeMarketState15sRow.data_complete.is_(True),
                )
                .group_by(RuntimeMarketState15sRow.symbol)
            )
            if required_symbols:
                market_query = market_query.where(
                    RuntimeMarketState15sRow.symbol.in_(required_symbols)
                )

            market_rows = (await session.execute(market_query)).all()

        now = datetime.now(UTC)
        symbol_times: dict[str, datetime] = {
            row[0]: (
                row[1]
                if row[1].tzinfo is not None
                else row[1].replace(tzinfo=UTC)
            )
            for row in market_rows
            if row[1] is not None
        }

        if required_symbols:
            missing_symbols = required_symbols - set(symbol_times.keys())
            coverage_complete = (len(missing_symbols) == 0)
        else:
            missing_symbols = set()
            coverage_complete = bool(symbol_times)

        if symbol_times:
            # Multi-symbol worst-case freshness: determined by the oldest symbol
            worst_market_time = min(symbol_times.values())
            observed_at = worst_market_time
            data_age_seconds = round(
                max(0.0, (now - worst_market_time).total_seconds()), 1
            )
            is_stale = (data_age_seconds > 120.0) or (not coverage_complete)
            if halts or ambiguous:
                status = OperationalStatus.HALTED
                source_status = "HALTED"
            elif is_stale:
                status = OperationalStatus.STALE
                source_status = "STALE"
            else:
                status = OperationalStatus.READY
                source_status = "LIVE"
        else:
            candidate_timestamps: list[datetime] = []
            for h in halts:
                if h.created_at is not None:
                    candidate_timestamps.append(
                        h.created_at
                        if h.created_at.tzinfo is not None
                        else h.created_at.replace(tzinfo=UTC)
                    )
            for d in decisions:
                if d.evaluated_at is not None:
                    candidate_timestamps.append(
                        d.evaluated_at
                        if d.evaluated_at.tzinfo is not None
                        else d.evaluated_at.replace(tzinfo=UTC)
                    )
            for o in orders:
                if o.updated_at is not None:
                    candidate_timestamps.append(
                        o.updated_at
                        if o.updated_at.tzinfo is not None
                        else o.updated_at.replace(tzinfo=UTC)
                    )

            if not candidate_timestamps:
                observed_at = None
                data_age_seconds = None
                source_status = "NO_DATA"
                status = (
                    OperationalStatus.HALTED
                    if halts or ambiguous
                    else OperationalStatus.NO_DATA
                )
            else:
                observed_at = max(candidate_timestamps)
                data_age_seconds = round(
                    max(0.0, (now - observed_at).total_seconds()), 1
                )
                if halts or ambiguous:
                    status = OperationalStatus.HALTED
                    source_status = "HALTED"
                else:
                    status = OperationalStatus.NO_DATA
                    source_status = "NO_DATA"

        return RiskExecutionResponse(
            status=status,
            active_halts=[
                {"reason": row.reason, "created_at": row.created_at.isoformat()}
                for row in halts
            ],
            latest_risk_decisions=[
                {
                    "candidate_id": row.candidate_id,
                    "decision": row.decision,
                    "reason": row.reason,
                    "evaluated_at": row.evaluated_at.isoformat(),
                }
                for row in decisions
            ],
            exchange_orders=[exchange_order(row) for row in orders],
            pending_orders=[exchange_order(row) for row in pending],
            ambiguous_orders=[exchange_order(row) for row in ambiguous],
            observed_at=observed_at,
            source_status=source_status,
            data_age_seconds=data_age_seconds,
        )


__all__ = [
    "RiskExecutionQueries",
    "exchange_order",
    "split_exchange_orders",
]
