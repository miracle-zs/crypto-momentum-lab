"""Legacy client-order identity expansion from storage-independent observations.

Moved out of postgres_runtime so the live context provider stays focused on
orchestration and SQL loading. No ORM types enter these calculations.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
    PositionOrderFact,
)
from crypto_momentum_lab.domain.execution.order_read_models import (
    OrderIdentityEvent,
    OrderObservation,
    PersistedExchangeOrder,
)

_PositionOrder = PositionOrderFact


def _legacy_order_identity_is_ambiguous(
    events: Sequence[OrderIdentityEvent],
) -> bool:
    exchange_order_ids = {
        event.exchange_order_id for event in events if event.exchange_order_id
    }
    return len(exchange_order_ids) > 1


def _legacy_order_identity_is_reconstructible(
    events: Sequence[OrderIdentityEvent],
    account_fill_quantities: Mapping[str, Decimal],
) -> bool:
    exchange_order_ids = {
        event.exchange_order_id for event in events if event.exchange_order_id
    }
    by_exchange_order_id = {
        exchange_order_id: Decimal("0") for exchange_order_id in exchange_order_ids
    }
    for event in events:
        exchange_order_id = event.exchange_order_id
        if not exchange_order_id:
            continue
        event_quantity = _event_executed_quantity(event)
        if event_quantity is not None:
            by_exchange_order_id[exchange_order_id] = max(
                by_exchange_order_id.get(exchange_order_id, Decimal("0")),
                event_quantity,
            )
    for exchange_order_id, quantity in account_fill_quantities.items():
        if exchange_order_id in by_exchange_order_id:
            by_exchange_order_id[exchange_order_id] = max(
                by_exchange_order_id[exchange_order_id],
                _decimal_or_zero(quantity),
            )
    return bool(by_exchange_order_id) and all(
        quantity > 0 for quantity in by_exchange_order_id.values()
    )


def _legacy_order_identity_is_zero_fill_terminal(
    events: Sequence[OrderIdentityEvent],
    account_fill_quantities: Mapping[str, Decimal],
) -> bool:
    """Recognize a harmless legacy collision with no possible fill.

    Older runs could reuse one client ID for multiple protective-order
    attempts.  If every attempt is explicitly terminal without execution and
    the account-fill ledger agrees, the collision cannot explain a live
    position and must not block a separately evidenced current entry.  Any
    active, filled, malformed, or incomplete evidence remains fail-closed.
    """
    events_by_exchange_order_id: dict[str, list[OrderIdentityEvent]] = {}
    for event in events:
        if event.exchange_order_id:
            events_by_exchange_order_id.setdefault(
                event.exchange_order_id,
                [],
            ).append(event)
    if not events_by_exchange_order_id:
        return False
    zero_fill_terminal_states = frozenset(
        {
            ExchangeOrderState.CANCELED,
            ExchangeOrderState.ABSENT_RECONCILED,
            ExchangeOrderState.REJECTED,
            ExchangeOrderState.EXPIRED,
            ExchangeOrderState.SUPPRESSED,
        }
    )
    for exchange_order_id, order_events in events_by_exchange_order_id.items():
        latest_event = max(
            order_events,
            key=lambda event: event.occurred_at,
        )
        try:
            latest_state = ExchangeOrderState(latest_event.state)
        except (TypeError, ValueError):
            return False
        if latest_state not in zero_fill_terminal_states:
            return False
        if any(
            _decimal_or_zero(_event_executed_quantity(event)) > 0
            for event in order_events
        ):
            return False
        if _decimal_or_zero(account_fill_quantities.get(exchange_order_id)) > 0:
            return False
    return True


def _expand_legacy_order_row(
    row: OrderObservation,
    *,
    plan: OrderExecutionPlan | None,
    fallback_state: ExchangeOrderState | None,
    fallback_executed_quantity: Decimal | None,
    events: Sequence[OrderIdentityEvent],
    account_fill_quantities: Mapping[str, Decimal],
) -> tuple[_PositionOrder, ...] | None:
    if not _legacy_order_identity_is_ambiguous(events):
        return None
    if not _legacy_order_identity_is_reconstructible(
        events,
        account_fill_quantities,
    ):
        return None
    base = _position_order_from_row(
        row,
        plan=plan,
        fallback_state=fallback_state,
        fallback_executed_quantity=fallback_executed_quantity,
    )
    if base is None:
        return None
    exchange_order_ids = sorted(
        {event.exchange_order_id for event in events if event.exchange_order_id}
    )
    expanded: list[_PositionOrder] = []
    for exchange_order_id in exchange_order_ids:
        identity_events = [
            event for event in events if event.exchange_order_id == exchange_order_id
        ]
        event_quantity = max(
            (
                quantity
                for event in identity_events
                if (quantity := _event_executed_quantity(event)) is not None
            ),
            default=Decimal("0"),
        )
        fill_quantity = _decimal_or_zero(account_fill_quantities.get(exchange_order_id))
        quantity = max(event_quantity, fill_quantity)
        if quantity <= 0:
            return None
        created_at = min(event.occurred_at for event in identity_events)
        updated_at = max(event.occurred_at for event in identity_events)
        latest_event = max(identity_events, key=lambda event: event.occurred_at)
        expanded.append(
            replace(
                base,
                exchange_order_id=exchange_order_id,
                quantity=quantity,
                executed_quantity=quantity,
                state=_normalise_order_state(
                    latest_event.state,
                    fallback=base.state,
                ),
                created_at=created_at,
                updated_at=updated_at,
            )
        )
    return tuple(expanded)


def _position_order_from_row(
    row: OrderObservation,
    *,
    plan: OrderExecutionPlan | None,
    fallback_state: ExchangeOrderState | None,
    fallback_executed_quantity: Decimal | None,
) -> _PositionOrder | None:
    try:
        position_side = FuturesPositionSide(
            getattr(row, "position_side", FuturesPositionSide.BOTH)
        )
    except (TypeError, ValueError):
        return None
    created_at = getattr(row, "created_at", None)
    updated_at = getattr(row, "updated_at", None)
    if created_at is None:
        created_at = plan.created_at if plan is not None else None
    if updated_at is None:
        updated_at = created_at
    if created_at is None or updated_at is None:
        return None
    state = _normalise_order_state(
        getattr(row, "state", None),
        fallback=fallback_state,
    )
    executed_quantity = _decimal_or_zero(getattr(row, "executed_quantity", None))
    if fallback_executed_quantity is not None:
        executed_quantity = max(
            executed_quantity,
            _decimal_or_zero(fallback_executed_quantity),
        )
    quantity = _decimal_or_zero(
        getattr(row, "quantity", None)
        if getattr(row, "quantity", None) is not None
        else (None if plan is None else plan.quantity)
    )
    quantity = max(quantity, executed_quantity)
    if quantity <= 0:
        return None
    price_value = getattr(row, "price", None)
    if price_value is None and plan is not None:
        price_value = plan.price
    price = None if price_value is None else _decimal_or_zero(price_value)
    return _PositionOrder(
        symbol=str(getattr(row, "symbol", plan.symbol if plan else "")),
        position_side=position_side,
        side=str(getattr(row, "side", plan.side if plan else "")).upper(),
        reduce_only=bool(
            getattr(row, "reduce_only", plan.reduce_only if plan else False)
        ),
        order_type=str(
            getattr(row, "order_type", plan.order_type if plan else "")
        ).upper(),
        quantity=quantity,
        executed_quantity=executed_quantity,
        state=state,
        client_order_id=_optional_text(
            getattr(row, "client_order_id", plan.client_order_id if plan else None)
        ),
        exchange_order_id=_optional_text(getattr(row, "exchange_order_id", None)),
        created_at=created_at,
        updated_at=updated_at,
        price=price,
        plan=plan,
    )


def _position_order_from_plan(item: PersistedExchangeOrder) -> _PositionOrder:
    plan = item.plan
    return _PositionOrder(
        symbol=plan.symbol,
        position_side=FuturesPositionSide(plan.position_side),
        side=plan.side.upper(),
        reduce_only=plan.reduce_only,
        order_type=plan.order_type.upper(),
        quantity=plan.quantity,
        executed_quantity=max(Decimal("0"), item.executed_quantity),
        state=_normalise_order_state(item.state),
        client_order_id=plan.client_order_id,
        exchange_order_id=item.exchange_order_id,
        created_at=plan.created_at,
        updated_at=item.updated_at,
        price=plan.price,
        plan=plan,
    )


def _ms_to_dt(value: int | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000.0, tz=UTC)


def _normalise_order_state(
    value: object,
    *,
    fallback: ExchangeOrderState | None = None,
) -> ExchangeOrderState:
    if isinstance(value, ExchangeOrderState):
        return value
    if value is not None:
        try:
            return ExchangeOrderState(str(value))
        except ValueError:
            pass
    if fallback is not None:
        return fallback
    return ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _decimal_or_zero(value: object) -> Decimal:
    if value is None:
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (ArithmeticError, TypeError, ValueError):
        return Decimal("0")


def _event_executed_quantity(
    event: OrderIdentityEvent,
) -> Decimal | None:
    details = event.details
    if not isinstance(details, dict):
        return None
    value = details.get("executed_quantity")
    if value is None:
        return None
    quantity = _decimal_or_zero(value)
    return quantity if quantity >= 0 else None


