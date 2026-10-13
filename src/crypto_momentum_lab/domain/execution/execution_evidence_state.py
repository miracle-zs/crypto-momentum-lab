"""State owned by the evidence-ingestion path."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import TYPE_CHECKING

from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.evidence_digest import digest_json_payload
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.evidence_rules import (
    _canonical_evidence_payload,
)
from crypto_momentum_lab.domain.execution.observation_models import (
    Applied,
    EvidencePendingReason,
    WaitingForEvidence,
)
from crypto_momentum_lab.domain.execution.ports import ExecutionEvidenceIdentity
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.trade_command import PositionReservation

if TYPE_CHECKING:
    from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook


def apply_flat_snapshot(
    book: ExecutionBook,
    evidence: ExecutionEvidence,
    *,
    scope: AccountFactStreamScope,
    requires_verified_stream_adoption: Callable[[PositionKey], bool],
) -> Applied | WaitingForEvidence | None:
    """Apply the no-exposure snapshot fast path without opening a transaction."""
    key = evidence.scope.to_position_key()
    canon = key.canonical_id
    current_book = book._books.get(canon)
    is_local_flat = current_book is None or (
        current_book.get_view().total_quantity == Decimal("0")
        and not current_book.get_view().batches
        and not current_book.get_view().unallocated_quantity
    )
    is_evidence_flat = (
        evidence.snapshot is not None
        and evidence.snapshot.position_amt == Decimal("0")
        and not (evidence.fills or evidence.fill)
    )
    has_no_reservations = not book._coordinator.get_active_reservations(key)
    has_no_commands = not any(
        entry.scope.to_position_key() == key
        for entry in book._outbox_by_command_id.values()
    )
    if not (
        is_local_flat
        and is_evidence_flat
        and has_no_reservations
        and has_no_commands
        and evidence.coverage_evidence is None
        and evidence.stream_checkpoint_adoption is None
    ):
        return None
    current_scope = book._stream_scopes.get(canon)
    if requires_verified_stream_adoption(key) and current_scope != scope:
        return WaitingForEvidence(
            evidence_id=evidence.evidence_id,
            reason=EvidencePendingReason.STREAM_RECOVERY_PROOF_REQUIRED,
        )
    if canon not in book._journals or current_scope != scope:
        book._journals[canon] = AccountJournal(key, stream_scope=scope)
        book._books[canon] = PositionBook(book._journals[canon])
        book._journal_revisions[canon] = 0
        book._last_sequences.pop(canon, None)
    book._stream_scopes[canon] = scope
    return Applied(
        evidence_id=evidence.evidence_id,
        updated_view_token=book._books[canon].get_view().projection_version,
    )


def evaluate_stream_rollover(
    *,
    current_scope: AccountFactStreamScope | None,
    is_flat: bool,
    active_reservations: tuple[PositionReservation, ...],
    evidence: ExecutionEvidence,
    is_unfilled_terminal_order: bool,
    requires_verified_stream_adoption: bool,
) -> tuple[bool, bool]:
    """Return ``(can_rollover, requires_recovery_proof)`` for a new stream.

    Pure computation receiving explicit inputs and returning decision tuple.
    """
    key = evidence.scope.to_position_key()
    reservations_held_by_event = (
        is_unfilled_terminal_order
        and evidence.order_event is not None
        and all(
            reservation.command_id == evidence.order_event.client_order_id
            for reservation in active_reservations
        )
    )
    has_blocking_reservations = (
        bool(active_reservations) and not reservations_held_by_event
    )
    can_rollover = (
        not requires_verified_stream_adoption
        and evidence.coverage_evidence is None
        and evidence.stream_checkpoint_adoption is None
        and evidence.source_anchor_snapshot is None
        and is_flat
        and (
            (
                evidence.snapshot is not None
                and evidence.snapshot.position_amt == Decimal("0")
            )
            or is_unfilled_terminal_order
        )
        and not has_blocking_reservations
    )
    target_scope = AccountFactStreamScope.for_position_key(
        key, stream_id=evidence.stream_id, stream_epoch=evidence.stream_epoch
    )
    recovery_proof_required = (
        current_scope is not None
        and current_scope != target_scope
        and not can_rollover
        and not is_unfilled_terminal_order
        and (
            evidence.coverage_evidence is None
            or evidence.fill_load_provenance is None
            or not evidence.fill_load_provenance.is_complete
        )
    )
    return can_rollover, recovery_proof_required


def apply_stream_rollover(
    book: ExecutionBook,
    *,
    key: PositionKey,
    scope: AccountFactStreamScope,
    can_rollover: bool,
    is_unfilled_terminal_order: bool,
) -> None:
    """Apply an already-validated stream transition to execution book state."""
    canon = key.canonical_id
    if can_rollover:
        book._stream_scopes[canon] = scope
        journal = book._journals.get(canon)
        if journal is not None:
            journal.adopt_stream_scope(scope)
        book._recovery_adoption_scope = scope
        book._last_sequences.pop(canon, None)
    elif is_unfilled_terminal_order:
        if canon not in book._journals:
            book._journals[canon] = AccountJournal(key, stream_scope=scope)
            book._books[canon] = PositionBook(book._journals[canon])
            book._journal_revisions[canon] = 0
        book._stream_scopes[canon] = scope
    else:
        book._journals[canon] = AccountJournal(key, stream_scope=scope)
        book._books[canon] = PositionBook(book._journals[canon])
        book._stream_scopes[canon] = scope
        book._journal_revisions[canon] = 0
        book._last_sequences.pop(canon, None)
        book._recovery_adoption_scope = scope


def durable_evidence_identity(evidence: ExecutionEvidence) -> ExecutionEvidenceIdentity:
    """Build the canonical durable identity for one source evidence event."""
    return ExecutionEvidenceIdentity(
        evidence_id=evidence.evidence_id,
        payload_digest=digest_json_payload(_canonical_evidence_payload(evidence)),
        accepted_at=evidence.observed_at,
        sequence=evidence.sequence,
    )
