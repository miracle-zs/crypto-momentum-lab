from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    ExchangeOrderFill,
    ExchangeOrderState,
    OrderExecutionPlan,
    ShadowSuppressionEvent,
)
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.domain.risk import RiskDecision, RiskEvaluation
from crypto_momentum_lab.domain.strategy import OrderIntentCandidate
from crypto_momentum_lab.persistence.postgres.models import (
    ExchangeFillRow,
    ExchangeOrderEventRow,
    ExchangeOrderRow,
    ExecutionCommandRow,
    ExecutionReconciliationEventRow,
    OrderIntentClaimRow,
    OrderIntentExecutionRow,
    ShadowSuppressionEventRow,
)


@dataclass(frozen=True, slots=True)
class PersistedExchangeOrder:
    plan: OrderExecutionPlan
    state: ExchangeOrderState
    exchange_order_id: str | None
    updated_at: datetime


class PostgresOrderRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory

    async def save_approved_intent(
        self,
        intent: OrderIntentCandidate,
        evaluation: RiskEvaluation,
    ) -> None:
        if evaluation.decision is not RiskDecision.APPROVED:
            raise ValueError("risk evaluation must approve the intent")
        if evaluation.candidate_id != intent.candidate_id:
            raise ValueError("risk evaluation must reference the intent")
        values = {
            "intent_id": intent.candidate_id,
            "candidate_id": intent.candidate_id,
            "run_id": intent.run_id,
            "risk_evaluation_id": evaluation.evaluation_id,
            "strategy_name": intent.strategy_name,
            "symbol": intent.symbol,
            "state": ExchangeOrderState.INTENT_APPROVED.value,
            "approved_at": evaluation.evaluated_at,
            "details": _jsonable(asdict(intent)),
        }
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    insert(OrderIntentExecutionRow)
                    .values(values)
                    .on_conflict_do_nothing()
                )

    async def claim_intent(
        self,
        intent_id: str,
        worker_id: str,
        claimed_at: datetime,
        expires_at: datetime,
    ) -> bool:
        if expires_at <= claimed_at:
            raise ValueError("claim expiration must be after claim time")
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    delete(OrderIntentClaimRow).where(
                        OrderIntentClaimRow.intent_id == intent_id,
                        OrderIntentClaimRow.expires_at <= claimed_at,
                    )
                )
                claimed = await session.scalar(
                    insert(OrderIntentClaimRow)
                    .values(
                        intent_id=intent_id,
                        worker_id=worker_id,
                        claimed_at=claimed_at,
                        expires_at=expires_at,
                    )
                    .on_conflict_do_nothing()
                    .returning(OrderIntentClaimRow.intent_id)
                )
                if claimed is not None:
                    await session.execute(
                        update(OrderIntentExecutionRow)
                        .where(OrderIntentExecutionRow.intent_id == intent_id)
                        .values(state=ExchangeOrderState.CLAIMED.value)
                    )
        return claimed is not None

    async def save_planned_order(self, plan: OrderExecutionPlan) -> None:
        values = {
            "client_order_id": plan.client_order_id,
            "intent_id": plan.intent_id,
            "run_id": plan.run_id,
            "exchange_order_id": None,
            "symbol": plan.symbol,
            "side": plan.side,
            "order_type": plan.order_type,
            "quantity": plan.quantity,
            "price": plan.price,
            "reduce_only": plan.reduce_only,
            "state": ExchangeOrderState.PLANNED.value,
            "created_at": plan.created_at,
            "updated_at": plan.created_at,
        }
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    insert(ExchangeOrderRow)
                    .values(values)
                    .on_conflict_do_nothing()
                )
                await session.execute(
                    update(OrderIntentExecutionRow)
                    .where(OrderIntentExecutionRow.intent_id == plan.intent_id)
                    .values(state=ExchangeOrderState.PLANNED.value)
                )

    async def append_order_event(self, event: ExchangeOrderEvent) -> bool:
        values = {
            "event_id": event.event_id,
            "client_order_id": event.client_order_id,
            "state": event.state.value,
            "occurred_at": event.occurred_at,
            "exchange_order_id": event.exchange_order_id,
            "details": _jsonable(event.details),
        }
        async with self._session_factory() as session:
            async with session.begin():
                inserted = await session.scalar(
                    insert(ExchangeOrderEventRow)
                    .values(values)
                    .on_conflict_do_nothing()
                    .returning(ExchangeOrderEventRow.event_id)
                )
                if inserted is not None:
                    order_values: dict[str, object] = {
                        "state": event.state.value,
                        "updated_at": event.occurred_at,
                    }
                    if event.exchange_order_id is not None:
                        order_values["exchange_order_id"] = event.exchange_order_id
                    await session.execute(
                        update(ExchangeOrderRow)
                        .where(
                            ExchangeOrderRow.client_order_id
                            == event.client_order_id
                        )
                        .values(order_values)
                    )
        return inserted is not None

    async def save_fill(self, fill: ExchangeOrderFill) -> bool:
        async with self._session_factory() as session:
            async with session.begin():
                inserted = await session.scalar(
                    insert(ExchangeFillRow)
                    .values(
                        fill_id=fill.fill_id,
                        client_order_id=fill.client_order_id,
                        exchange_trade_id=fill.exchange_trade_id,
                        price=fill.price,
                        quantity=fill.quantity,
                        fee=fill.fee,
                        fee_asset=fill.fee_asset,
                        filled_at=fill.filled_at,
                        details=_jsonable(fill.details),
                    )
                    .on_conflict_do_nothing()
                    .returning(ExchangeFillRow.fill_id)
                )
        return inserted is not None

    async def save_shadow_suppression(
        self,
        event: ShadowSuppressionEvent,
    ) -> None:
        await self._insert_immutable(
            ShadowSuppressionEventRow,
            {
                "order_plan_id": event.order_plan_id,
                "client_order_id": event.client_order_id,
                "suppressed_at": event.suppressed_at,
                "reason": event.reason,
                "order_payload": _jsonable(event.order_payload),
            },
        )

    async def load_unresolved_orders(self) -> tuple[PersistedExchangeOrder, ...]:
        terminal_states = tuple(
            state.value for state in ExchangeOrderState if state.terminal
        )
        async with self._session_factory() as session:
            rows = (
                await session.scalars(
                    select(ExchangeOrderRow)
                    .where(ExchangeOrderRow.state.not_in(terminal_states))
                    .order_by(
                        ExchangeOrderRow.updated_at,
                        ExchangeOrderRow.client_order_id,
                    )
                )
            ).all()
        return tuple(_persisted_order(row) for row in rows)

    async def load_order(
        self,
        client_order_id: str,
    ) -> PersistedExchangeOrder | None:
        async with self._session_factory() as session:
            row = await session.scalar(
                select(ExchangeOrderRow).where(
                    ExchangeOrderRow.client_order_id == client_order_id
                )
            )
        return None if row is None else _persisted_order(row)

    async def save_execution_command(
        self,
        *,
        command_id: str,
        client_order_id: str | None,
        command: str,
        status: str,
        requested_at: datetime,
        details: dict[str, JsonValue],
    ) -> None:
        await self._insert_immutable(
            ExecutionCommandRow,
            {
                "command_id": command_id,
                "client_order_id": client_order_id,
                "command": command,
                "status": status,
                "requested_at": requested_at,
                "details": _jsonable(details),
            },
        )

    async def save_reconciliation_event(
        self,
        *,
        reconciliation_event_id: str,
        client_order_id: str,
        outcome: str,
        occurred_at: datetime,
        details: dict[str, JsonValue],
    ) -> None:
        await self._insert_immutable(
            ExecutionReconciliationEventRow,
            {
                "reconciliation_event_id": reconciliation_event_id,
                "client_order_id": client_order_id,
                "outcome": outcome,
                "occurred_at": occurred_at,
                "details": _jsonable(details),
            },
        )

    async def _insert_immutable(
        self,
        model: Any,
        values: dict[str, object],
    ) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    insert(model).values(values).on_conflict_do_nothing()
                )


def _persisted_order(row: ExchangeOrderRow) -> PersistedExchangeOrder:
    return PersistedExchangeOrder(
        plan=OrderExecutionPlan(
            intent_id=row.intent_id,
            run_id=row.run_id,
            client_order_id=row.client_order_id,
            symbol=row.symbol,
            side=row.side,
            order_type=row.order_type,
            quantity=row.quantity,
            price=row.price,
            reduce_only=row.reduce_only,
            created_at=row.created_at,
            quantized=True,
        ),
        state=ExchangeOrderState(row.state),
        exchange_order_id=row.exchange_order_id,
        updated_at=row.updated_at,
    )


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
