from datetime import datetime
from decimal import Decimal
from typing import Any, cast

from sqlalchemy import (
    CursorResult,
    and_,
    case,
    func,
    literal,
    or_,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    ExchangeOrderFill,
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
    ShadowSuppressionEvent,
    order_read_models,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ExchangeFillRow,
    ExchangeOrderEventRow,
    ExchangeOrderRow,
    ExitEpisodeReservationRow,
    LiveExposureClaimRow,
    OrderIntentExecutionRow,
    ShadowSuppressionEventRow,
)
from crypto_momentum_lab.persistence.postgres.serialization import jsonable
from crypto_momentum_lab.persistence.postgres.submission_identity import (
    _same_order_identity,
)


class PostgresOrderRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory

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
                    insert(ExchangeOrderRow).values(values).on_conflict_do_nothing()
                )
                await session.execute(
                    update(OrderIntentExecutionRow)
                    .where(OrderIntentExecutionRow.intent_id == plan.intent_id)
                    .values(state=ExchangeOrderState.PLANNED.value)
                )

    async def adopt_external_order_for_cancellation(
        self,
        plan: OrderExecutionPlan,
        *,
        exchange_order_id: str,
        observed_at: datetime,
    ) -> None:
        """Journal an exchange-visible order before coordinator cancellation.

        A rolling worker can discover an entry on Binance before the local
        worker that created it has committed its order row.  The synthetic
        intent gives the normal state machine a valid foreign-key target, so
        the cancel still shares the same coordinator and durable event path.
        """

        if not exchange_order_id.strip():
            raise ValueError("exchange_order_id must not be empty")
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        synthetic_intent_id = f"orphan-cancel:{plan.client_order_id}"
        intent_values = {
            "intent_id": synthetic_intent_id,
            "candidate_id": synthetic_intent_id,
            "run_id": plan.run_id,
            "risk_evaluation_id": synthetic_intent_id,
            "strategy_name": "orphan-cancel",
            "symbol": plan.symbol,
            "state": ExchangeOrderState.SUBMITTED.value,
            "approved_at": observed_at,
            "details": {
                "reason": "exchange_visible_order_missing_from_local_journal",
                "client_order_id": plan.client_order_id,
            },
        }
        order_values = {
            "client_order_id": plan.client_order_id,
            "intent_id": synthetic_intent_id,
            "run_id": plan.run_id,
            "exchange_order_id": exchange_order_id,
            "symbol": plan.symbol,
            "side": plan.side,
            "order_type": plan.order_type,
            "quantity": plan.quantity,
            "price": plan.price,
            "time_in_force": plan.time_in_force,
            "expires_at": plan.expires_at,
            "executed_quantity": Decimal("0"),
            "reduce_only": False,
            "position_side": plan.position_side.value,
            "state": ExchangeOrderState.SUBMITTED.value,
            "created_at": plan.created_at,
            "updated_at": observed_at,
        }
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    insert(OrderIntentExecutionRow)
                    .values(intent_values)
                    .on_conflict_do_nothing()
                )
                inserted = await session.scalar(
                    insert(ExchangeOrderRow)
                    .values(order_values)
                    .on_conflict_do_nothing()
                    .returning(ExchangeOrderRow.client_order_id)
                )
                if inserted is None:
                    existing_order = await session.scalar(
                        select(ExchangeOrderRow).where(
                            ExchangeOrderRow.client_order_id == plan.client_order_id
                        )
                    )
                    if existing_order is None or not _same_order_identity(
                        existing_order,
                        order_values,
                    ):
                        raise ValueError(
                            "client order ID is already bound to a different order"
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
                        state.value for state in ExchangeOrderState if state.terminal
                    )
                    terminal_transition = and_(
                        literal(event.state.terminal),
                        # FILLED is the strongest terminal observation; a
                        # later cancel/reject event must not erase it.
                        or_(
                            ExchangeOrderRow.state != ExchangeOrderState.FILLED.value,
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
                    order_intent_id = await session.scalar(
                        select(ExchangeOrderRow.intent_id).where(
                            ExchangeOrderRow.client_order_id == event.client_order_id
                        )
                    )
                    order_update = await session.execute(
                        update(ExchangeOrderRow)
                        .where(
                            ExchangeOrderRow.client_order_id == event.client_order_id,
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
                    current_order_state = await session.scalar(
                        select(ExchangeOrderRow.state).where(
                            ExchangeOrderRow.client_order_id == event.client_order_id
                        )
                    )
                    if (
                        # session.execute() is typed as Result[Any]; for an
                        # UPDATE it is a CursorResult, which carries rowcount.
                        cast(CursorResult[Any], order_update).rowcount
                        and order_intent_id is not None
                        and current_order_state is not None
                    ):
                        if current_order_state == event.state.value:
                            await session.execute(
                                update(ExitEpisodeReservationRow)
                                .where(
                                    ExitEpisodeReservationRow.intent_id
                                    == order_intent_id
                                )
                                .values(
                                    state=event.state.value,
                                    updated_at=event.occurred_at,
                                )
                            )
                        if current_order_state in terminal_states:
                            await session.execute(
                                update(ExitEpisodeReservationRow)
                                .where(
                                    ExitEpisodeReservationRow.intent_id
                                    == order_intent_id,
                                    ExitEpisodeReservationRow.active.is_(True),
                                )
                                .values(
                                    active=False,
                                    updated_at=event.occurred_at,
                                )
                            )
                            await session.execute(
                                update(LiveExposureClaimRow)
                                .where(
                                    LiveExposureClaimRow.intent_id == order_intent_id,
                                    LiveExposureClaimRow.active.is_(True),
                                )
                                .values(
                                    active=False,
                                    updated_at=event.occurred_at,
                                )
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
    ) -> tuple[order_read_models.PersistedExchangeOrder, ...]:
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
                    query.order_by(
                        ExchangeOrderRow.updated_at,
                        ExchangeOrderRow.client_order_id,
                    )
                )
            ).all()
        return tuple(_persisted_order(row) for row in rows)

    async def load_order(
        self,
        client_order_id: str,
    ) -> order_read_models.PersistedExchangeOrder | None:
        async with self._session_factory() as session:
            row = await session.scalar(
                select(ExchangeOrderRow).where(
                    ExchangeOrderRow.client_order_id == client_order_id
                )
            )
        return None if row is None else _persisted_order(row)


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


def _persisted_order(row: ExchangeOrderRow) -> order_read_models.PersistedExchangeOrder:
    return order_read_models.PersistedExchangeOrder(
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
