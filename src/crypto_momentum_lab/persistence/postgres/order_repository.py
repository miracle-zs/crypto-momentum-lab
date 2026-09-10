from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import and_, case, delete, func, literal, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    ExchangeOrderFill,
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
    ShadowSuppressionEvent,
)
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.domain.risk import RiskDecision, RiskEvaluation
from crypto_momentum_lab.domain.strategy import OrderIntentCandidate
from crypto_momentum_lab.execution_account.orders.state_machine import (
    PreparedOrderSubmission,
)
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
from crypto_momentum_lab.persistence.postgres.serialization import jsonable


@dataclass(frozen=True, slots=True)
class PersistedExchangeOrder:
    plan: OrderExecutionPlan
    state: ExchangeOrderState
    exchange_order_id: str | None
    updated_at: datetime
    executed_quantity: Decimal = Decimal("0")


class _SubmissionAlreadyPrepared(Exception):
    """Abort the transaction when the client order ID already exists."""


def _same_order_identity(
    existing_order: ExchangeOrderRow,
    expected_values: Mapping[str, object],
) -> bool:
    """Keep idempotency scoped to the exact durable order identity."""

    return all(
        getattr(existing_order, field_name) == expected_values[field_name]
        for field_name in (
            "intent_id",
            "run_id",
            "symbol",
            "side",
            "order_type",
            "quantity",
            "price",
            "time_in_force",
            "expires_at",
            "reduce_only",
            "position_side",
            "created_at",
        )
    )


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
            "details": jsonable(asdict(intent)),
        }
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    insert(OrderIntentExecutionRow)
                    .values(values)
                    .on_conflict_do_nothing()
                )

    async def prepare_submission(
        self,
        *,
        intent: OrderIntentCandidate,
        evaluation: RiskEvaluation,
        plan: OrderExecutionPlan,
        prepared_at: datetime,
    ) -> PreparedOrderSubmission | None:
        """Grant one durable submission; existing orders must be reconciled.

        A client ID is never reusable, including after a terminal outcome or
        process restart. The unique insert arbitrates concurrent exit lanes.
        """

        if evaluation.decision is not RiskDecision.APPROVED:
            raise ValueError("risk evaluation must approve the intent")
        if evaluation.candidate_id != intent.candidate_id:
            raise ValueError("risk evaluation must reference the intent")
        if plan.intent_id != intent.candidate_id:
            raise ValueError("order plan must reference the approved intent")
        if prepared_at.tzinfo is None or prepared_at.utcoffset() is None:
            raise ValueError("prepared_at must be timezone-aware")

        intent_values = {
            "intent_id": intent.candidate_id,
            "candidate_id": intent.candidate_id,
            "run_id": intent.run_id,
            "risk_evaluation_id": evaluation.evaluation_id,
            "strategy_name": intent.strategy_name,
            "symbol": intent.symbol,
            "state": ExchangeOrderState.SUBMITTING.value,
            "approved_at": evaluation.evaluated_at,
            "details": jsonable(asdict(intent)),
        }
        order_values = {
            "client_order_id": plan.client_order_id,
            "intent_id": plan.intent_id,
            "run_id": plan.run_id,
            "exchange_order_id": None,
            "symbol": plan.symbol,
            "side": plan.side,
            "order_type": plan.order_type,
            "quantity": plan.quantity,
            "price": plan.price,
            "time_in_force": plan.time_in_force,
            "expires_at": plan.expires_at,
            "executed_quantity": Decimal("0"),
            "reduce_only": plan.reduce_only,
            "position_side": plan.position_side.value,
            "state": ExchangeOrderState.SUBMITTING.value,
            "created_at": plan.created_at,
            "updated_at": prepared_at,
        }
        submitting_event = ExchangeOrderEvent(
            event_id=_order_event_id(
                plan.client_order_id,
                ExchangeOrderState.SUBMITTING,
                prepared_at,
            ),
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.SUBMITTING,
            occurred_at=prepared_at,
            exchange_order_id=None,
            details={},
        )
        event_values = {
            "event_id": submitting_event.event_id,
            "client_order_id": submitting_event.client_order_id,
            "state": submitting_event.state.value,
            "occurred_at": submitting_event.occurred_at,
            "exchange_order_id": submitting_event.exchange_order_id,
            "details": jsonable(submitting_event.details),
        }
        try:
            async with self._session_factory() as session:
                async with session.begin():
                    await session.execute(
                        insert(OrderIntentExecutionRow)
                        .values(intent_values)
                        .on_conflict_do_nothing()
                    )
                    inserted_order = await session.scalar(
                        insert(ExchangeOrderRow)
                        .values(order_values)
                        .on_conflict_do_nothing()
                        .returning(ExchangeOrderRow.client_order_id)
                    )
                    if inserted_order is None:
                        existing_order = await session.scalar(
                            select(ExchangeOrderRow).where(
                                ExchangeOrderRow.client_order_id
                                == plan.client_order_id
                            )
                        )
                        if existing_order is None:
                            raise RuntimeError(
                                "client order ID conflict could not be reconciled"
                            )
                        if not _same_order_identity(
                            existing_order,
                            order_values,
                        ):
                            raise ValueError(
                                "client order ID is already bound to a "
                                "different order"
                            )
                        # A restarted or concurrent worker already owns the
                        # same order. Roll back any new intent atomically.
                        raise _SubmissionAlreadyPrepared
                    await session.execute(
                        insert(ExchangeOrderEventRow)
                        .values(event_values)
                        .on_conflict_do_nothing()
                    )
                    await session.execute(
                        update(OrderIntentExecutionRow)
                        .where(
                            OrderIntentExecutionRow.intent_id == plan.intent_id
                        )
                        .values(state=ExchangeOrderState.SUBMITTING.value)
                    )
        except _SubmissionAlreadyPrepared:
            return None
        return PreparedOrderSubmission(
            plan=plan,
            submitting_event=submitting_event,
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
            "time_in_force": plan.time_in_force,
            "expires_at": plan.expires_at,
            "executed_quantity": Decimal("0"),
            "reduce_only": plan.reduce_only,
            "position_side": plan.position_side.value,
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
            "details": jsonable(event.details),
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
                    terminal_states = tuple(
                        state.value
                        for state in ExchangeOrderState
                        if state.terminal
                    )
                    terminal_transition = and_(
                        literal(event.state.terminal),
                        # FILLED is the strongest terminal observation; a
                        # later cancel/reject event must not erase it.
                        or_(
                            ExchangeOrderRow.state
                            != ExchangeOrderState.FILLED.value,
                            literal(event.state is ExchangeOrderState.FILLED),
                        ),
                    )
                    advance_state = and_(
                        ExchangeOrderRow.updated_at <= event.occurred_at,
                        or_(
                            ExchangeOrderRow.state.not_in(terminal_states),
                            terminal_transition,
                        ),
                    )
                    order_values: dict[str, object] = {
                        "state": case(
                            (advance_state, event.state.value),
                            else_=ExchangeOrderRow.state,
                        ),
                        "updated_at": case(
                            (
                                advance_state,
                                func.greatest(
                                    ExchangeOrderRow.updated_at,
                                    event.occurred_at,
                                ),
                            ),
                            else_=ExchangeOrderRow.updated_at,
                        ),
                    }
                    if event.exchange_order_id is not None:
                        order_values["exchange_order_id"] = event.exchange_order_id
                    executed_quantity = _event_executed_quantity(event)
                    if executed_quantity is not None:
                        # Exchange snapshots are cumulative, but callbacks can
                        # arrive out of order after a reconnect. Never let an
                        # older snapshot lower the reservation baseline.
                        order_values["executed_quantity"] = func.greatest(
                            ExchangeOrderRow.executed_quantity,
                            executed_quantity,
                        )
                    await session.execute(
                        update(ExchangeOrderRow)
                        .where(
                            ExchangeOrderRow.client_order_id
                            == event.client_order_id,
                            # Preserve the immutable exchange identity. Keep
                            # conflicting legacy events in the event journal,
                            # but never merge another order into this row.
                            or_(
                                literal(event.exchange_order_id is None),
                                ExchangeOrderRow.exchange_order_id.is_(None),
                                ExchangeOrderRow.exchange_order_id
                                == event.exchange_order_id,
                            ),
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
                        details=jsonable(fill.details),
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
                "order_payload": jsonable(event.order_payload),
            },
        )

    async def load_unresolved_orders(
        self,
        run_id: str | None = None,
    ) -> tuple[PersistedExchangeOrder, ...]:
        terminal_states = tuple(
            state.value for state in ExchangeOrderState if state.terminal
        )
        async with self._session_factory() as session:
            query = select(ExchangeOrderRow).where(
                ExchangeOrderRow.state.not_in(terminal_states)
            )
            if run_id is not None:
                query = query.where(ExchangeOrderRow.run_id == run_id)
            rows = (
                await session.scalars(
                    query
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
                "details": jsonable(details),
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
                "details": jsonable(details),
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
            position_side=FuturesPositionSide(row.position_side),
            quantized=True,
            time_in_force=row.time_in_force,
            expires_at=row.expires_at,
        ),
        state=ExchangeOrderState(row.state),
        exchange_order_id=row.exchange_order_id,
        updated_at=row.updated_at,
        executed_quantity=row.executed_quantity or Decimal("0"),
    )


def _event_executed_quantity(event: ExchangeOrderEvent) -> Decimal | None:
    value = event.details.get("executed_quantity")
    if value is None:
        return None
    try:
        quantity = Decimal(str(value))
    except ArithmeticError:
        return None
    return quantity if quantity >= 0 else None


def _order_event_id(
    client_order_id: str,
    state: ExchangeOrderState,
    occurred_at: datetime,
) -> str:
    return str(
        uuid5(
            NAMESPACE_URL,
            f"order-event:{client_order_id}:{state.value}:{occurred_at.isoformat()}",
        )
    )
