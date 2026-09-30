from decimal import Decimal
from typing import Any

from sqlalchemy import (
    update,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderState,
    OrderExecutionPlan,
    ShadowSuppressionEvent,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ExchangeOrderRow,
    OrderIntentExecutionRow,
    ShadowSuppressionEventRow,
)
from crypto_momentum_lab.persistence.postgres.serialization import jsonable


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
