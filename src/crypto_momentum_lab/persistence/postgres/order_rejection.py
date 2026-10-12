"""Cascading rejection logic for durable order submission and execution."""

from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderRejectionOutcome,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ExchangeOrderEventRow,
    ExchangeOrderRow,
    ExitEpisodeReservationRow,
    LiveExposureClaimRow,
    OrderIntentExecutionRow,
)
from crypto_momentum_lab.persistence.postgres.serialization import (
    jsonable,
)


async def execute_order_rejection_cascade(
    session: AsyncSession,
    client_order_id: str,
    *,
    reason: str,
    occurred_at: datetime | None = None,
    intent_id: str | None = None,
) -> OrderRejectionOutcome:
    """Durably reject an exchange order and release its claims and reservations."""
    at = occurred_at or datetime.now(UTC)
    terminal_states = tuple(
        state.value for state in ExchangeOrderState if state.terminal
    )
    order_row = await session.scalar(
        select(ExchangeOrderRow)
        .where(ExchangeOrderRow.client_order_id == client_order_id)
        .with_for_update()
    )
    if order_row is None:
        return OrderRejectionOutcome.ABSENT
    if order_row.state in terminal_states:
        return OrderRejectionOutcome.TERMINAL
    if intent_id is not None and order_row.intent_id != intent_id:
        return OrderRejectionOutcome.INTENT_MISMATCH

    order_row.state = ExchangeOrderState.REJECTED.value
    order_row.updated_at = at

    await session.execute(
        insert(ExchangeOrderEventRow)
        .values(
            event_id=f"evt-{client_order_id}-rejected",
            client_order_id=client_order_id,
            state=ExchangeOrderState.REJECTED.value,
            occurred_at=at,
            exchange_order_id=None,
            details=jsonable({"reason": reason}),
        )
        .on_conflict_do_nothing()
    )
    order_intent_id = order_row.intent_id
    if order_intent_id is not None:
        await session.execute(
            update(OrderIntentExecutionRow)
            .where(OrderIntentExecutionRow.intent_id == order_intent_id)
            .values(state=ExchangeOrderState.REJECTED.value)
        )
        await session.execute(
            update(ExitEpisodeReservationRow)
            .where(
                ExitEpisodeReservationRow.intent_id == order_intent_id,
                ExitEpisodeReservationRow.active.is_(True),
            )
            .values(
                active=False,
                state=ExchangeOrderState.REJECTED.value,
                updated_at=at,
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
                updated_at=at,
            )
        )
    return OrderRejectionOutcome.REJECTED
