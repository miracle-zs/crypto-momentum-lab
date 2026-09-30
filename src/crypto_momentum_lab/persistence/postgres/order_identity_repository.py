"""Postgres order-identity evidence loading and ORM-to-value mapping."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.order_read_models import (
    OrderIdentityEvent,
    OrderObservation,
    PositionObservation,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountFillEventRow,
    AccountPositionSnapshotRow,
    ExchangeOrderEventRow,
    ExchangeOrderRow,
)


@dataclass(frozen=True, slots=True)
class OrderIdentityMetadata:
    events_by_client_order_id: Mapping[str, tuple[OrderIdentityEvent, ...]]
    account_fills: tuple[AccountFillEvent, ...]


def order_observation(row: ExchangeOrderRow) -> OrderObservation:
    return OrderObservation(
        symbol=row.symbol,
        position_side=row.position_side,
        side=row.side,
        reduce_only=row.reduce_only,
        order_type=row.order_type,
        quantity=row.quantity,
        executed_quantity=row.executed_quantity,
        state=row.state,
        client_order_id=row.client_order_id,
        exchange_order_id=row.exchange_order_id,
        created_at=row.created_at,
        updated_at=row.updated_at,
        price=row.price,
    )


def position_observation(row: AccountPositionSnapshotRow) -> PositionObservation:
    return PositionObservation(
        symbol=row.symbol,
        position_side=row.position_side,
        position_amt=row.position_amt,
        entry_price=row.entry_price,
        observed_at=row.observed_at,
    )


def order_identity_event(row: ExchangeOrderEventRow) -> OrderIdentityEvent:
    return OrderIdentityEvent(
        client_order_id=row.client_order_id,
        exchange_order_id=row.exchange_order_id,
        state=row.state,
        occurred_at=row.occurred_at,
        details=dict(row.details) if isinstance(row.details, Mapping) else {},
    )


def _fill_raw_payload(raw: object, *, is_system: bool) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if isinstance(raw, dict):
        payload.update(raw)
    payload["is_system"] = is_system
    return payload


async def load_order_identity_metadata(
    session: AsyncSession,
    orders: Sequence[ExchangeOrderRow],
    *,
    account_label: str,
    since: datetime | None = None,
) -> OrderIdentityMetadata:
    """Load the event/fill evidence needed to split legacy order attempts.

    ``exchange_orders`` is keyed by client order ID, but older execution paths
    reused that ID for more than one exchange order.  The event journal keeps
    the exchange identities, and account fills provide the authoritative
    quantity for each identity.  Runtime position reconstruction must use both
    before it decides where an exit boundary belongs.
    """

    client_order_ids = tuple(
        sorted({order.client_order_id for order in orders if order.client_order_id})
    )
    events_by_client: dict[str, list[OrderIdentityEvent]] = {}
    exchange_order_ids: set[str] = set()
    if client_order_ids:
        event_rows = tuple(
            (
                await session.scalars(
                    select(ExchangeOrderEventRow).where(
                        ExchangeOrderEventRow.client_order_id.in_(client_order_ids)
                    )
                )
            ).all()
        )
        for event in event_rows:
            events_by_client.setdefault(event.client_order_id, []).append(
                order_identity_event(event)
            )
            if event.exchange_order_id:
                exchange_order_ids.add(event.exchange_order_id)
    row_exchange_order_ids = {
        order.exchange_order_id for order in orders if order.exchange_order_id
    }
    exchange_order_ids.update(row_exchange_order_ids)
    active_symbols = tuple(sorted({order.symbol for order in orders if order.symbol}))
    if not exchange_order_ids and not active_symbols:
        return OrderIdentityMetadata(
            {key: tuple(value) for key, value in events_by_client.items()},
            (),
        )

    predicates = [
        AccountFillEventRow.environment == "live",
        AccountFillEventRow.account_label == account_label,
    ]

    # Resolve symbol scan lower bound to protect against full table scan
    symbol_since = since
    if symbol_since is None:
        order_times = [
            order.created_at for order in orders if getattr(order, "created_at", None)
        ]
        if order_times:
            symbol_since = min(order_times) - timedelta(hours=24)
        else:
            symbol_since = datetime.now(UTC) - timedelta(days=30)

    # Dual-track query:
    # 1. Exact track: exchange_order_ids are matched by ID without wall-clock cutoff.
    # 2. Range track: active_symbols are scanned from symbol_since.
    if exchange_order_ids and active_symbols:
        predicates.append(
            or_(
                AccountFillEventRow.order_id.in_(tuple(exchange_order_ids)),
                and_(
                    AccountFillEventRow.symbol.in_(active_symbols),
                    AccountFillEventRow.trade_at >= symbol_since,
                ),
            )
        )
    elif exchange_order_ids:
        predicates.append(AccountFillEventRow.order_id.in_(tuple(exchange_order_ids)))
    elif active_symbols:
        predicates.append(
            and_(
                AccountFillEventRow.symbol.in_(active_symbols),
                AccountFillEventRow.trade_at >= symbol_since,
            )
        )

    account_fills = tuple(
        (
            await session.scalars(
                select(AccountFillEventRow)
                .where(*predicates)
                .order_by(AccountFillEventRow.trade_at.asc())
            )
        ).all()
    )
    system_order_id_set = {str(oid) for oid in exchange_order_ids}
    domain_account_fills = tuple(
        AccountFillEvent(
            environment=row.environment,
            account_label=row.account_label,
            symbol=row.symbol,
            trade_id=str(row.trade_id),
            order_id=str(row.order_id),
            side=row.side,
            price=Decimal(str(row.price)),
            quantity=Decimal(str(row.quantity)),
            realized_pnl=Decimal(str(row.realized_pnl)),
            fee=Decimal(str(row.fee)),
            fee_asset=row.fee_asset,
            trade_at=row.trade_at,
            raw_payload=_fill_raw_payload(
                row.raw_payload,
                is_system=str(row.order_id) in system_order_id_set,
            ),
        )
        for row in account_fills
    )
    return OrderIdentityMetadata(
        {key: tuple(value) for key, value in events_by_client.items()},
        domain_account_fills,
    )
