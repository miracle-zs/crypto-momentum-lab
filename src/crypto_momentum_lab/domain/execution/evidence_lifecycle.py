"""Pure command lifecycle plans derived from exchange order observations."""

from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal

from crypto_momentum_lab.domain.account.models import AccountFillEvent
from crypto_momentum_lab.domain.execution.command_models import (
    DispatchState,
    OutboxEntry,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
)
from crypto_momentum_lab.domain.execution.trade_command import PositionReservation


@dataclass(frozen=True, slots=True)
class OrderEventPlan:
    updated: OutboxEntry | None = None
    release_reason: str | None = None
    recovery_diagnostic: str | None = None
    pending_trade_diagnostic: str | None = None
    requires_dispatch_reconciliation: bool = False
    dispatch_reconciled: bool = False


def plan_order_event(
    outbox: OutboxEntry | None,
    event: ExchangeOrderEvent,
    *,
    observed_at: datetime,
    account_fills: tuple[AccountFillEvent, ...],
    durable: bool,
    has_active_reservations: bool,
) -> OrderEventPlan:
    if outbox is None or outbox.state in (
        DispatchState.TERMINAL,
        DispatchState.REJECTED,
    ):
        return OrderEventPlan()
    state = event.state
    acknowledged = state in (
        ExchangeOrderState.ACKNOWLEDGED, ExchangeOrderState.SUBMITTED,
    ) or (state is ExchangeOrderState.PARTIALLY_FILLED
          and event.exchange_order_id is not None)
    eligible = outbox.state in (
        DispatchState.PREPARED,
        DispatchState.DISPATCHING,
        DispatchState.UNKNOWN,
    ) or (outbox.state is DispatchState.ACKNOWLEDGED
          and event.exchange_order_id is not None)
    if acknowledged and eligible:
        return OrderEventPlan(
            updated=replace(
                outbox,
                state=DispatchState.ACKNOWLEDGED,
                updated_at=observed_at,
            ),
            dispatch_reconciled=event.exchange_order_id is not None,
        )
    if state in (
        ExchangeOrderState.CANCELED,
        ExchangeOrderState.EXPIRED,
        ExchangeOrderState.REJECTED,
        ExchangeOrderState.ABSENT_RECONCILED,
        ExchangeOrderState.FILLED,
    ):
        updated = replace(
            outbox,
            state=DispatchState.REJECTED
            if state == ExchangeOrderState.REJECTED
            else DispatchState.TERMINAL,
            last_error=f"Order {state.value}"
            if state != ExchangeOrderState.FILLED
            else outbox.last_error,
            updated_at=observed_at,
        )
        if state != ExchangeOrderState.FILLED:
            return OrderEventPlan(
                updated=updated,
                release_reason=f"order_finished_{state.value.lower()}",
                dispatch_reconciled=True,
            )
        confirmed = sum(
            (
                fill.quantity
                for fill in account_fills
                if fill.order_id in (event.client_order_id, outbox.external_order_id)
            ),
            Decimal("0"),
        )
        if confirmed >= outbox.command.requested_quantity or not durable:
            return OrderEventPlan(
                updated=updated,
                release_reason="order_filled_with_confirmed_trades"
                if confirmed >= outbox.command.requested_quantity
                else "order_finished_filled",
                dispatch_reconciled=True,
            )
        if not has_active_reservations:
            return OrderEventPlan(
                updated=updated,
                pending_trade_diagnostic="Order settled; awaiting real account trades",
                dispatch_reconciled=True,
            )
        return OrderEventPlan(
            updated=updated,
            recovery_diagnostic=(
                "Filled terminal lacks complete account trade facts; "
                "active reservation is retained for recovery"
            ),
            dispatch_reconciled=True,
        )
    if state == ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION:
        return OrderEventPlan(
            updated=replace(
                outbox,
                state=DispatchState.UNKNOWN,
                last_error="Pending reconciliation",
                updated_at=observed_at,
            ),
            requires_dispatch_reconciliation=True,
        )
    return OrderEventPlan()


def terminal_settlement_is_confirmed(
    outbox: OutboxEntry,
    *,
    account_fills: tuple[AccountFillEvent, ...],
    cumulative_quantity: Decimal,
    reservations: tuple[PositionReservation | None, ...],
) -> bool:
    """Prove a terminal command's trade and reservation quantities agree."""
    if outbox.state is not DispatchState.TERMINAL:
        return False
    confirmed = sum(
        (fill.quantity for fill in account_fills
         if fill.order_id in (outbox.command_id, outbox.external_order_id)),
        Decimal("0"),
    )
    if (
        confirmed != outbox.command.requested_quantity
        or confirmed != cumulative_quantity
    ):
        return False
    if not outbox.command.reduce_only:
        return True
    if not reservations or any(reservation is None for reservation in reservations):
        return False
    consumed = Decimal("0")
    for reservation in reservations:
        assert reservation is not None
        if (
            reservation.command_id != outbox.command_id
            or reservation.position_key != outbox.scope.to_position_key()
            or reservation.active_quantity != 0
            or reservation.released_quantity != 0
        ):
            return False
        consumed += reservation.consumed_quantity
    return consumed == confirmed
