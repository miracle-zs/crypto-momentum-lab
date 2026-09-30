from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    select,
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
