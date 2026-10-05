"""Durable execution-head serialization."""

from __future__ import annotations

from collections.abc import Callable

from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.evidence_codec import (
    recovery_checkpoint_head_binding,
)
from crypto_momentum_lab.domain.execution.evidence_digest import view_projection_digest
from crypto_momentum_lab.domain.execution.execution_evidence_state import EvidenceState
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.domain.execution.trade_command import PositionReservation


def build_execution_head_payload(
    state: EvidenceState,
    key: PositionKey,
    facts_hash: str,
    *,
    ensure_book: Callable[[PositionKey], PositionBook],
    ensure_journal: Callable[[PositionKey], AccountJournal],
    active_reservations: Callable[[PositionKey], tuple[PositionReservation, ...]],
) -> dict[str, object]:
    """Build the complete durable head from the published execution state."""
    scope = state.stream_scopes.get(key.canonical_id)
    if scope is None:
        raise RuntimeError("durable execution head requires a stream scope")
    view = ensure_book(key).get_view()
    facts = ensure_journal(key).read_cut()
    projection = PositionLedger(key).project(facts)
    checkpoint = facts.recovery_checkpoint
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
        "journal_revision": state.journal_revisions.get(key.canonical_id, 0),
        "last_sequence": state.last_sequences.get(key.canonical_id),
        "seen_trade_count": len(state.seen_trade_ids),
        "active_reservation_ids": sorted(
            reservation.reservation_id for reservation in active_reservations(key)
        ),
        "external_recovery_ids": sorted(
            identity
            for identity, position in state.external_recovery_positions.items()
            if position == key
        ),
        "recovery_command_ids": sorted(
            identity
            for identity in state.recovery_required_commands
            if identity in state.outbox_by_command_id
            and state.outbox_by_command_id[identity].scope.to_position_key() == key
        ),
    }
