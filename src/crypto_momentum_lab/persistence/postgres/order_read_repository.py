"""Read durable exchange orders without exposing lifecycle writes."""

from dataclasses import replace
from decimal import Decimal

from sqlalchemy import (
    select,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import crypto_momentum_lab.domain.execution.order_read_models as order_read_models
from crypto_momentum_lab.domain.account.models import extract_fill_position_side
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.persistence.postgres.account_fact_rows import (
    account_fill_from_row,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountFillEventRow,
    ExchangeFillRow,
    ExchangeOrderEventRow,
    ExchangeOrderRow,
    ExecutionCommandRow,
    LiveExposureClaimRow,
    OrderIntentExecutionRow,
)


class PostgresOrderReadRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory

    async def load_approved_intent_notional(
        self,
        intent_id: str,
    ) -> Decimal | None:
        async with self._session_factory() as session:
            details = await session.scalar(
                select(OrderIntentExecutionRow.details).where(
                    OrderIntentExecutionRow.intent_id == intent_id
                )
            )
        if not isinstance(details, dict):
            return None
        value = details.get("desired_notional")
        return None if value is None else Decimal(str(value))

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
            market_intent_ids = [r.intent_id for r in rows if r.price is None]
            claim_notionals: dict[str, Decimal] = {}
            if market_intent_ids:
                try:
                    claim_rows = (
                        await session.execute(
                            select(
                                LiveExposureClaimRow.intent_id,
                                LiveExposureClaimRow.notional,
                            ).where(
                                LiveExposureClaimRow.intent_id.in_(market_intent_ids)
                            )
                        )
                    ).all()
                    claim_notionals = {
                        order_id: notional for order_id, notional in claim_rows
                    }
                except Exception:
                    pass
        return tuple(
            _persisted_order(
                row,
                reference_price=(claim_notionals[row.intent_id] / row.quantity)
                if (row.intent_id in claim_notionals and row.quantity > 0)
                else None,
            )
            for row in rows
        )

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
            if row is None:
                return None
            ref_price: Decimal | None = None
            if row.price is None and row.intent_id is not None:
                try:
                    claims = (
                        await session.scalars(
                            select(LiveExposureClaimRow.notional).where(
                                LiveExposureClaimRow.intent_id == row.intent_id
                            )
                        )
                    ).all()
                    if claims and row.quantity > 0:
                        ref_price = claims[0] / row.quantity
                except Exception:
                    pass
            order = _persisted_order(row, reference_price=ref_price)
            receipt = None
            if order.state.terminal:
                quantity = order.executed_quantity
                settlement_fills = ()
                average = Decimal(0) if quantity == 0 else None
                if quantity > 0:
                    events = (
                        await session.scalars(
                            select(ExchangeOrderEventRow)
                            .where(
                                ExchangeOrderEventRow.client_order_id
                                == client_order_id,
                            )
                            .order_by(ExchangeOrderEventRow.occurred_at.desc())
                        )
                    ).all()
                    for event in events:
                        details = event.details
                        event_quantity = details.get("executed_quantity")
                        price = details.get("average_price")
                        if (
                            event_quantity is not None
                            and price is not None
                            and Decimal(str(event_quantity)) == quantity
                        ):
                            parsed_price = Decimal(str(price))
                            if not parsed_price.is_finite() or parsed_price < 0:
                                raise ValueError("persisted order price is invalid")
                            if parsed_price > 0:
                                average = parsed_price
                                break
                    if average is None:
                        fills = (
                            await session.scalars(
                                select(ExchangeFillRow).where(
                                    ExchangeFillRow.client_order_id == client_order_id,
                                )
                            )
                        ).all()
                        if (
                            sum((fill.quantity for fill in fills), Decimal(0))
                            == quantity
                        ):
                            average = (
                                sum(
                                    (fill.quantity * fill.price for fill in fills),
                                    Decimal(0),
                                )
                                / quantity
                            )
                    if order.exchange_order_id is not None:
                        scope = await session.scalar(
                            select(ExecutionCommandRow.details["scope"]).where(
                                ExecutionCommandRow.command_id == client_order_id,
                            )
                        )
                        if scope is not None:
                            if (
                                scope["symbol"] != row.symbol
                                or scope["position_side"] != row.position_side
                            ):
                                raise ValueError(
                                    "persisted command and order position disagree"
                                )
                            account_fills = (
                                await session.scalars(
                                    select(AccountFillEventRow).where(
                                        AccountFillEventRow.environment
                                        == scope["environment"],
                                        AccountFillEventRow.account_label
                                        == scope["account_label"],
                                        AccountFillEventRow.symbol == row.symbol,
                                        AccountFillEventRow.order_id
                                        == order.exchange_order_id,
                                        AccountFillEventRow.side == row.side,
                                    )
                                )
                            ).all()
                            matching_fills = [
                                fill
                                for fill in account_fills
                                if (
                                    extract_fill_position_side(fill.raw_payload)
                                    or "BOTH"
                                )
                                == row.position_side
                            ]
                            traded_quantity = sum(
                                (fill.quantity for fill in matching_fills), Decimal(0)
                            )
                            traded_quote = sum(
                                (fill.quantity * fill.price for fill in matching_fills),
                                Decimal(0),
                            )
                            if traded_quantity == quantity:
                                settlement_fills = tuple(
                                    account_fill_from_row(fill)
                                    for fill in matching_fills
                                )
                                if average is None:
                                    average = traded_quote / quantity
                if average is not None:
                    receipt = order_read_models.PersistedOrderReceipt(
                        client_order_id,
                        order.state,
                        order.exchange_order_id,
                        quantity,
                        average,
                        settlement_fills,
                    )
            return replace(order, terminal_receipt=receipt)


def _persisted_order(
    row: ExchangeOrderRow,
    reference_price: Decimal | None = None,
) -> order_read_models.PersistedExchangeOrder:
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
            reference_price=reference_price,
        ),
        state=ExchangeOrderState(row.state),
        exchange_order_id=row.exchange_order_id,
        updated_at=row.updated_at,
        executed_quantity=row.executed_quantity or Decimal("0"),
    )
