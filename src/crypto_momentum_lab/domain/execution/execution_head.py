"""Durable execution-head serialization."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.evidence_codec import (
    recovery_checkpoint_head_binding,
)
from crypto_momentum_lab.domain.execution.evidence_digest import view_projection_digest
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.domain.execution.trade_command import PositionReservation

if TYPE_CHECKING:
    from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook


def build_execution_head_payload(
    book: ExecutionBook,
    key: PositionKey,
    facts_hash: str,
    *,
    ensure_book: Callable[[PositionKey], PositionBook],
    ensure_journal: Callable[[PositionKey], AccountJournal],
    active_reservations: Callable[[PositionKey], tuple[PositionReservation, ...]],
) -> dict[str, object]:
    """Build the complete durable head from the published execution state."""
    scope = book._stream_scopes.get(key.canonical_id)
    if scope is None:
        raise RuntimeError("durable execution head requires a stream scope")
    view = ensure_book(key).get_view()
    facts = ensure_journal(key).read_cut()
    projection = PositionLedger(key).project(facts)
    checkpoint = facts.recovery_checkpoint
    journal_revisions = book._journal_revisions
    last_sequences = book._last_sequences
    seen_trade_ids = book._seen_trade_ids
    external_recovery_positions = book._external_recovery_positions
    recovery_required_commands = book._recovery_required_commands
    outbox_by_command_id = book._outbox_by_command_id
    return {
        "schema_version": 1,
        "position_key": {
            "environment": key.environment,
            "account_label": key.account_label,
            "symbol": key.symbol,
            "position_side": key.position_side.value,
        },
        "stream_scope": {
            "stream_id": scope.stream_id,
            "stream_epoch": scope.stream_epoch,
        },
        "facts_hash": facts_hash,
        "projection_digest": PositionRecoveryCodec.compute_projection_digest(
            projection
        ),
        "view_digest": view_projection_digest(view),
        "recovery_checkpoint": recovery_checkpoint_head_binding(checkpoint),
        "journal_revision": journal_revisions.get(key.canonical_id, 0),
        "last_sequence": last_sequences.get(key.canonical_id),
        "seen_trade_count": len(seen_trade_ids),
        "active_reservation_ids": sorted(
            reservation.reservation_id for reservation in active_reservations(key)
        ),
        "external_recovery_ids": sorted(
            identity
            for identity, position in external_recovery_positions.items()
            if position == key
        ),
        "recovery_command_ids": sorted(
            identity
            for identity in recovery_required_commands
            if identity in outbox_by_command_id
            and outbox_by_command_id[identity].scope.to_position_key() == key
        ),
        "active_batches": [
            PositionRecoveryCodec.encode_batch(batch) for batch in view.batches
        ],
        "projection": PositionRecoveryCodec.encode_projection(projection),
        "coverage": (
            PositionRecoveryCodec.encode_coverage(view.coverage)
            if view.coverage is not None
            else None
        ),
        "total_active_quantity": format(view.total_quantity, "f"),
        "zero_position_snapshot_confirmed": view.zero_position_snapshot_confirmed,
        "active_trade_ids": [fill.trade_id for fill in facts.fills],
        "active_fills": [
            PositionRecoveryCodec.encode_fill(fill) for fill in facts.fills
        ],
        "active_exit_boundaries": [
            PositionRecoveryCodec.encode_boundary(b) for b in facts.exit_boundaries
        ],
        "active_snapshots": [
            PositionRecoveryCodec.encode_snapshot(s) for s in facts.snapshots
        ],
    }
