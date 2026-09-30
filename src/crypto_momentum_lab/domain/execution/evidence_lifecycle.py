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


@dataclass(frozen=True, slots=True)
class OrderEventPlan:
    updated: OutboxEntry | None = None
    release_reason: str | None = None
    recovery_diagnostic: str | None = None
    requires_dispatch_reconciliation: bool = False
    dispatch_reconciled: bool = False


def plan_order_event(
    outbox: OutboxEntry | None,
    event: ExchangeOrderEvent,
    *,
    observed_at: datetime,
    account_fills: tuple[AccountFillEvent, ...],
    durable: bool,
) -> OrderEventPlan:
    if outbox is None or outbox.state in (
        DispatchState.TERMINAL,
        DispatchState.REJECTED,
    ):
        return OrderEventPlan()
    state = event.state
    if state in (
        ExchangeOrderState.ACKNOWLEDGED,
        ExchangeOrderState.SUBMITTED,
    ) and outbox.state in (
        DispatchState.PREPARED,
        DispatchState.DISPATCHING,
        DispatchState.UNKNOWN,
    ):
        return OrderEventPlan(
            updated=replace(
                outbox,
                state=DispatchState.ACKNOWLEDGED,
                updated_at=observed_at,
            )
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
                if fill.order_id == event.client_order_id
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
