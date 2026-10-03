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

from crypto_momentum_lab.domain.execution.order_state import (
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
    OrderIntentExecutionRow,
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
                            ExchangeOrderRow.state.not_in(terminal_states),
                            literal(event.state is ExchangeOrderState.FILLED),
                            and_(
                                ExchangeOrderRow.state != ExchangeOrderState.FILLED.value,
                                ExchangeOrderRow.updated_at <= event.occurred_at,
                            ),
                        ),
                    )
                    # REST receipt time and exchange event time use different
                    # clocks. A verified terminal fact dominates a nonterminal
                    # receipt even when its exchange timestamp is earlier.
                    advance_state = or_(
                        terminal_transition,
                        and_(
                            ExchangeOrderRow.updated_at <= event.occurred_at,
                            ExchangeOrderRow.state.not_in(terminal_states),
                            or_(
                                ExchangeOrderRow.state != ExchangeOrderState.PARTIALLY_FILLED.value,
                                literal(event.state not in (
                                    ExchangeOrderState.ACKNOWLEDGED,
                                    ExchangeOrderState.SUBMITTED,
                                )),
                            ),
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
                            if current_order_state == ExchangeOrderState.FILLED.value:
                                await session.execute(
                                    update(LiveExposureClaimRow)
                                    .where(
                                        LiveExposureClaimRow.intent_id
                                        == order_intent_id,
                                        LiveExposureClaimRow.active.is_(True),
                                    )
                                    .values(
                                        updated_at=event.occurred_at,
                                    )
                                )
                            else:
                                order_row = await session.scalar(
                                    select(ExchangeOrderRow).where(
                                        ExchangeOrderRow.client_order_id
                                        == event.client_order_id
                                    )
                                )
                                event_qty = _event_executed_quantity(event)
                                cumulative_executed_qty = (
                                    order_row.executed_quantity
                                    if order_row is not None
                                    and order_row.executed_quantity is not None
                                    else Decimal("0")
                                )
                                if (
                                    event_qty is not None
                                    and event_qty > cumulative_executed_qty
                                ):
                                    cumulative_executed_qty = event_qty

                                if cumulative_executed_qty <= Decimal("0"):
                                    await session.execute(
                                        update(LiveExposureClaimRow)
                                        .where(
                                            LiveExposureClaimRow.intent_id
                                            == order_intent_id,
                                            LiveExposureClaimRow.active.is_(True),
                                        )
                                        .values(
                                            active=False,
                                            updated_at=event.occurred_at,
                                        )
                                    )
                                else:
                                    order_qty = (
                                        order_row.quantity
                                        if order_row is not None
                                        and order_row.quantity is not None
                                        else Decimal("1")
                                    )
                                    claim_row = await session.scalar(
                                        select(LiveExposureClaimRow).where(
                                            LiveExposureClaimRow.intent_id
                                            == order_intent_id,
                                            LiveExposureClaimRow.active.is_(True),
                                        )
                                    )
                                    if (
                                        claim_row is not None
                                        and order_row is not None
                                        and order_qty > Decimal("0")
                                    ):
                                        planned_notional = (
                                            await _resolve_order_planned_notional(
                                                session, order_row, order_intent_id
                                            )
                                        )
                                        if (
                                            planned_notional is not None
                                            and planned_notional > Decimal("0")
                                        ):
                                            claim_row.notional = (
                                                cumulative_executed_qty / order_qty
                                            ) * planned_notional
                                        elif (
                                            order_row.price is not None
                                            and order_row.price > Decimal("0")
                                        ):
                                            claim_row.notional = (
                                                cumulative_executed_qty * order_row.price
                                            )
                                        claim_row.updated_at = event.occurred_at
                                    else:
                                        await session.execute(
                                            update(LiveExposureClaimRow)
                                            .where(
                                                LiveExposureClaimRow.intent_id
                                                == order_intent_id,
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


async def _resolve_order_planned_notional(
    session: AsyncSession,
    order_row: ExchangeOrderRow,
    order_intent_id: str,
) -> Decimal | None:
    if (
        order_row.price is not None
        and order_row.price > Decimal("0")
        and order_row.quantity is not None
        and order_row.quantity > Decimal("0")
    ):
        return order_row.quantity * order_row.price
    intent_row = await session.scalar(
        select(OrderIntentExecutionRow).where(
            OrderIntentExecutionRow.intent_id == order_intent_id
        )
    )
    if intent_row is not None and isinstance(intent_row.details, dict):
        raw = intent_row.details.get("desired_notional")
        if raw is not None:
            try:
                notional = Decimal(str(raw))
                if notional > Decimal("0"):
                    return notional
            except (ArithmeticError, ValueError):
                pass
    return None


def _event_executed_quantity(event: ExchangeOrderEvent) -> Decimal | None:
    value = event.details.get("executed_quantity")
    if value is None:
        return None
    try:
        quantity = Decimal(str(value))
    except ArithmeticError:
        return None
    return quantity if quantity >= 0 else None
