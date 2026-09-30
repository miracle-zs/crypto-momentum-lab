"""Atomic order observations, terminal claim release and trade deduplication."""

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
)
from crypto_momentum_lab.persistence.postgres.models import (
    ExchangeFillRow,
    ExchangeOrderEventRow,
    ExchangeOrderRow,
    ExitEpisodeReservationRow,
    LiveExposureClaimRow,
)
from crypto_momentum_lab.persistence.postgres.serialization import jsonable


class PostgresOrderEventRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory

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


def _event_executed_quantity(event: ExchangeOrderEvent) -> Decimal | None:
    value = event.details.get("executed_quantity")
    if value is None:
        return None
    try:
        quantity = Decimal(str(value))
    except ArithmeticError:
        return None
    return quantity if quantity >= 0 else None
