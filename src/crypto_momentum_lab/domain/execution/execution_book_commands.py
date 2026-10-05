"""Command-state mutation applied through an execution book's persistence hooks."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.execution.command_lifecycle import (
    plan_command_transition,
    plan_reservation_release,
    plan_reservation_settlement,
)
from crypto_momentum_lab.domain.execution.command_models import (
    DispatchState,
    OutboxEntry,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.execution.trade_command import PositionReservation


@dataclass(slots=True)
class CommandLifecycleState:
    outbox_by_command_id: dict[str, OutboxEntry]
    dispatch_reconciliation_required_commands: set[str]
    recovery_required_commands: set[str]


@dataclass(frozen=True, slots=True)
class CommandPersistenceDependencies:
    """Persistence operations needed by command and reservation transitions."""

    persist_outbox: Callable[[OutboxEntry], Awaitable[None]]
    persist_reservation: Callable[[PositionReservation, str | None], Awaitable[None]]
    active_reservations_for_command: Callable[[str], tuple[PositionReservation, ...]]
    order_watermark_key: Callable[[PositionKey, str], str]
    advance_context_revision: Callable[[], None]
    external_recovery_positions: dict[str, PositionKey]


async def persist_command_transition(
    state: CommandLifecycleState,
    dependencies: CommandPersistenceDependencies,
    updated: OutboxEntry,
) -> None:
    """Persist and publish exactly one outbox state transition."""
    await dependencies.persist_outbox(updated)
    state.outbox_by_command_id[updated.command_id] = updated
    dependencies.advance_context_revision()


async def release_command_reservations(
    dependencies: CommandPersistenceDependencies,
    command_id: str,
    *,
    reason: str,
) -> Decimal:
    """Release all remaining capacity held by one terminal command."""
    plan = plan_reservation_release(
        dependencies.active_reservations_for_command(command_id)
    )
    for reservation in plan.updates:
        await dependencies.persist_reservation(reservation, reason)
    return plan.released_quantity


async def settle_command_reservations(
    state: CommandLifecycleState,
    dependencies: CommandPersistenceDependencies,
    order_id: str,
    quantity: Decimal,
    *,
    reported_quantity: Decimal,
    key: PositionKey,
) -> tuple[Decimal, bool, str | None]:
    """Settle fill quantity against the command's active reservations."""
    plan = plan_reservation_settlement(
        dependencies.active_reservations_for_command(order_id),
        order_id=order_id,
        quantity=quantity,
        reported_quantity=reported_quantity,
    )
    for reservation in plan.updates:
        await dependencies.persist_reservation(reservation, None)
    if plan.recovery_required:
        if order_id in state.outbox_by_command_id:
            state.recovery_required_commands.add(order_id)
        else:
            identity = f"external:{dependencies.order_watermark_key(key, order_id)}"
            dependencies.external_recovery_positions[identity] = key
            state.recovery_required_commands.add(identity)
    return plan.consumed_quantity, plan.recovery_required, plan.diagnostic


async def apply_command_transition(
    state: CommandLifecycleState,
    command_id: str,
    target: DispatchState,
    *,
    at: datetime | None = None,
    external_order_id: str | None = None,
    reason: str = "",
    persist_transition: Callable[[OutboxEntry], Awaitable[None]],
    release_reservations: Callable[[str, str], Awaitable[object]],
    seal_persistence: Callable[[], None],
) -> OutboxEntry:
    """Apply one planned command transition through the book's durable hooks."""
    entry = state.outbox_by_command_id.get(command_id)
    if entry is None:
        raise KeyError(f"Outbox entry {command_id} not found")
    plan = plan_command_transition(
        entry,
        target,
        at=at or datetime.now(UTC),
        external_order_id=external_order_id,
        reason=reason,
    )
    if plan.updated is entry:
        return entry
    if plan.requires_reconciliation:
        state.dispatch_reconciliation_required_commands.add(command_id)
    elif target in (DispatchState.REJECTED, DispatchState.TERMINAL):
        state.dispatch_reconciliation_required_commands.discard(command_id)
        state.recovery_required_commands.discard(command_id)
        if entry.external_order_id:
            state.recovery_required_commands.discard(entry.external_order_id)
    try:
        await persist_transition(plan.updated)
    except Exception:
        if plan.requires_reconciliation:
            # A submit may have reached the exchange. Seal against resubmit even
            # if persisting its UNKNOWN state failed.
            state.outbox_by_command_id[command_id] = plan.updated
            seal_persistence()
        raise
    if plan.release_reason is not None:
        await release_reservations(command_id, plan.release_reason)
    return state.outbox_by_command_id[command_id]
