"""Read durable exchange orders without exposing lifecycle writes."""

from decimal import Decimal

from sqlalchemy import (
    select,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import crypto_momentum_lab.domain.execution.order_read_models as order_read_models
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ExchangeOrderRow,
)


class PostgresOrderReadRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory

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
