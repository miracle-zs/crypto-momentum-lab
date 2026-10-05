"""State and pure values used by the execution-action path."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from crypto_momentum_lab.domain.execution.command_models import (
    DispatchState,
    ExecutionScope,
    OutboxEntry,
)
from crypto_momentum_lab.domain.execution.trade_command import TradeCommand


@dataclass(slots=True)
class ExecutionActionState:
    """Mutable command/request state owned by action acceptance."""

    requests_by_id: dict[str, object]
    receipts_by_id: dict[str, object]
    outbox_by_command_id: dict[str, OutboxEntry]
    command_reservations: dict[str, list[str]]
    recovery_required_commands: set[str]
    dispatch_reconciliation_required_commands: set[str]


def build_prepared_outbox(
    *,
    request_id: str,
    scope: ExecutionScope,
    command: TradeCommand,
    created_at: datetime,
) -> OutboxEntry:
    """Create the sole prepared outbox record for an accepted command."""
    return OutboxEntry(
        command_id=command.command_id,
        request_id=request_id,
        scope=scope,
        command=command,
        state=DispatchState.PREPARED,
        created_at=created_at,
        updated_at=created_at,
    )


def register_prepared_outbox(
    state: ExecutionActionState,
    *,
    request_id: str,
    scope: ExecutionScope,
    command: TradeCommand,
    created_at: datetime,
    reservation_ids: tuple[str, ...] = (),
) -> OutboxEntry:
    """Register one prepared command and its optional reservation linkage."""
    entry = build_prepared_outbox(
        request_id=request_id,
        scope=scope,
        command=command,
        created_at=created_at,
    )
    state.outbox_by_command_id[command.command_id] = entry
    if reservation_ids:
        state.command_reservations[command.command_id] = list(reservation_ids)
    return entry
