"""State and pure values used by the execution-action path."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from crypto_momentum_lab.domain.execution.command_models import (
    DispatchState,
    ExecutionScope,
    OutboxEntry,
)
from crypto_momentum_lab.domain.execution.trade_command import TradeCommand

if TYPE_CHECKING:
    from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook


def build_prepared_outbox(
    *,
    request_id: str,
    scope: ExecutionScope,
    command: TradeCommand,
    created_at: datetime,
    intent_id: str | None = None,
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
        intent_id=intent_id,
    )


def register_prepared_outbox(
    book: ExecutionBook,
    *,
    request_id: str,
    scope: ExecutionScope,
    command: TradeCommand,
    created_at: datetime,
    reservation_ids: tuple[str, ...] = (),
    intent_id: str | None = None,
) -> OutboxEntry:
    """Register one prepared command and its optional reservation linkage."""
    entry = build_prepared_outbox(
        request_id=request_id,
        scope=scope,
        command=command,
        created_at=created_at,
        intent_id=intent_id,
    )
    book._outbox_by_command_id[command.command_id] = entry
    if reservation_ids:
        book._command_reservations[command.command_id] = list(reservation_ids)
    return entry
