"""Compute command transitions and reservation updates; callers own persistence.

UNKNOWN never releases capacity. Cumulative settlement changes reservations only,
not account fill facts. Input values are immutable and never modified here.
"""

from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal

from crypto_momentum_lab.domain.execution.command_models import (
    DispatchState,
    OutboxEntry,
)
from crypto_momentum_lab.domain.execution.trade_command import PositionReservation


@dataclass(frozen=True, slots=True)
class CommandTransition:
    updated: OutboxEntry
    requires_reconciliation: bool = False
    release_reason: str | None = None


def plan_command_transition(
    entry: OutboxEntry,
    target: DispatchState,
    *,
    at: datetime,
    external_order_id: str | None = None,
    reason: str = "",
) -> CommandTransition:
    if target == DispatchState.DISPATCHING:
        if entry.state != DispatchState.PREPARED:
            raise ValueError(
                f"Cannot dispatch outbox entry in state {entry.state.value}"
            )
        return CommandTransition(
            replace(
                entry,
                state=target,
                attempt_count=entry.attempt_count + 1,
                updated_at=at,
            )
        )
    if target == DispatchState.ACKNOWLEDGED:
        return CommandTransition(
            replace(
                entry, state=target, external_order_id=external_order_id, updated_at=at
            )
        )
    if target == DispatchState.UNKNOWN:
        if entry.state in (DispatchState.TERMINAL, DispatchState.REJECTED):
            return CommandTransition(entry)
        return CommandTransition(
            replace(entry, state=target, last_error=reason, updated_at=at),
            requires_reconciliation=True,
        )
    if target == DispatchState.REJECTED:
        return CommandTransition(
            replace(entry, state=target, last_error=reason, updated_at=at),
            release_reason="command_rejected",
        )
    if target == DispatchState.TERMINAL:
        return CommandTransition(
            replace(
                entry,
                state=target,
                last_error=reason if reason else entry.last_error,
                updated_at=at,
            ),
            release_reason=reason or "command_terminal",
        )
    raise ValueError(f"Unsupported command transition target {target.value}")


@dataclass(frozen=True, slots=True)
class ReservationSettlement:
    updates: tuple[PositionReservation, ...]
    consumed_quantity: Decimal
    recovery_required: bool
    diagnostic: str | None = None


def plan_reservation_settlement(
    linked: tuple[PositionReservation, ...],
    *,
    order_id: str,
    quantity: Decimal,
    reported_quantity: Decimal,
) -> ReservationSettlement:
    if quantity <= Decimal("0"):
        return ReservationSettlement((), Decimal("0"), False)
    if not linked:
        return ReservationSettlement(
            (),
            Decimal("0"),
            True,
            f"No active reservation is linked to filled command {order_id}",
        )
    remaining = quantity
    consumed_total = Decimal("0")
    updates = []
    for reservation in linked:
        if remaining <= Decimal("0"):
            break
        consumed = min(remaining, reservation.active_quantity)
        if consumed <= Decimal("0"):
            continue
        updates.append(reservation.consume(consumed))
        consumed_total += consumed
        remaining -= consumed
    if remaining > Decimal("0"):
        return ReservationSettlement(
            tuple(updates),
            consumed_total,
            True,
            f"Cumulative fill {reported_quantity} exceeds linked active "
            f"reservations by {remaining}",
        )
    return ReservationSettlement(tuple(updates), consumed_total, False)


@dataclass(frozen=True, slots=True)
class ReservationRelease:
    updates: tuple[PositionReservation, ...]
    released_quantity: Decimal


def plan_reservation_release(
    linked: tuple[PositionReservation, ...],
) -> ReservationRelease:
    updates = tuple(r.release(r.active_quantity) for r in linked)
    return ReservationRelease(
        updates, sum((r.active_quantity for r in linked), Decimal("0"))
    )
