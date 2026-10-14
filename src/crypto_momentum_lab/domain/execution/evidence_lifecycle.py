"""Pure command lifecycle plans derived from exchange order observations."""

from dataclasses import dataclass, replace
from datetime import datetime

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
    requires_dispatch_reconciliation: bool = False
    dispatch_reconciled: bool = False


def plan_order_event(
    outbox: OutboxEntry | None,
    event: ExchangeOrderEvent,
    *,
    observed_at: datetime,
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
        return OrderEventPlan(
            updated=updated,
            release_reason="order_finished_filled",
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
