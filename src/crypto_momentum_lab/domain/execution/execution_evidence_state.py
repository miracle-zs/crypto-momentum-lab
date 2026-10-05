"""State owned by the evidence-ingestion path."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.command_models import OutboxEntry
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
from crypto_momentum_lab.domain.execution.reservation_registry import (
    ReservationRegistry,
)


@dataclass(slots=True)
class EvidenceState:
    """Mutable facts required to ingest one account evidence stream."""

    books: dict[str, PositionBook]
    journals: dict[str, AccountJournal]
    stream_scopes: dict[str, AccountFactStreamScope]
    active_streams: set[tuple[str, str, str, str]]
    head_revisions: dict[str, int]
    journal_revisions: dict[str, int]
    last_sequences: dict[str, int]
    seen_evidence_ids: set[str]
    seen_trade_ids: set[str]
    outbox_by_command_id: dict[str, OutboxEntry]
    command_reservations: dict[str, list[str]]
    order_cumulative_fills: dict[str, Decimal]
    order_cumulative_quotes: dict[str, Decimal]
    recovery_required_commands: set[str]
    external_recovery_positions: dict[str, PositionKey]
    dispatch_reconciliation_required_commands: set[str]
    coordinator: ReservationRegistry
    recovery_adoption_scope: AccountFactStreamScope | None = None

    def register_active_stream(self, evidence: ExecutionEvidence) -> None:
        """Record the source stream that supplied this evidence, when present."""
        if evidence.stream_id and evidence.stream_epoch:
            self.active_streams.add(
                (
                    evidence.scope.environment,
                    evidence.scope.account_label,
                    evidence.stream_id,
                    evidence.stream_epoch,
                )
            )


def apply_flat_snapshot(
    state: EvidenceState,
    evidence: ExecutionEvidence,
    *,
    scope: AccountFactStreamScope,
    requires_verified_stream_adoption: Callable[[PositionKey], bool],
) -> Applied | WaitingForEvidence | None:
    """Apply the no-exposure snapshot fast path without opening a transaction."""
    key = evidence.scope.to_position_key()
    canon = key.canonical_id
    current_book = state.books.get(canon)
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
    has_no_reservations = not state.coordinator.get_active_reservations(key)
    has_no_commands = not any(
        entry.scope.to_position_key() == key
        for entry in state.outbox_by_command_id.values()
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
    current_scope = state.stream_scopes.get(canon)
    if requires_verified_stream_adoption(key) and current_scope != scope:
        return WaitingForEvidence(
            evidence_id=evidence.evidence_id,
            reason=EvidencePendingReason.STREAM_RECOVERY_PROOF_REQUIRED,
        )
    if canon not in state.journals or current_scope != scope:
        state.journals[canon] = AccountJournal(key, stream_scope=scope)
        state.books[canon] = PositionBook(state.journals[canon])
        state.journal_revisions[canon] = 0
        state.last_sequences.pop(canon, None)
    state.stream_scopes[canon] = scope
    return Applied(
        evidence_id=evidence.evidence_id,
        updated_view_token=state.books[canon].get_view().projection_version,
    )


def evaluate_stream_rollover(
    state: EvidenceState,
    evidence: ExecutionEvidence,
    *,
    is_unfilled_terminal_order: bool,
    requires_verified_stream_adoption: Callable[[PositionKey], bool],
) -> tuple[bool, bool]:
    """Return ``(can_rollover, requires_recovery_proof)`` for a new stream."""
    key = evidence.scope.to_position_key()
    current_scope = state.stream_scopes.get(key.canonical_id)
    current_book = state.books.get(key.canonical_id)
    active_reservations = state.coordinator.get_active_reservations(key)
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
    is_flat = current_book is not None and (
        current_book.get_view().total_quantity == Decimal("0")
        and not current_book.get_view().batches
        and not current_book.get_view().unallocated_quantity
    )
    can_rollover = (
        not requires_verified_stream_adoption(key)
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
    recovery_proof_required = (
        current_scope is not None
        and current_scope != AccountFactStreamScope.for_position_key(
            key, stream_id=evidence.stream_id, stream_epoch=evidence.stream_epoch
        )
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
    state: EvidenceState,
    *,
    key: PositionKey,
    scope: AccountFactStreamScope,
    can_rollover: bool,
    is_unfilled_terminal_order: bool,
) -> None:
    """Apply an already-validated stream transition to evidence state."""
    canon = key.canonical_id
    if can_rollover:
        state.stream_scopes[canon] = scope
        journal = state.journals.get(canon)
        if journal is not None:
            journal.adopt_stream_scope(scope)
        state.recovery_adoption_scope = scope
        state.last_sequences.pop(canon, None)
    elif is_unfilled_terminal_order:
        if canon not in state.journals:
            state.journals[canon] = AccountJournal(key, stream_scope=scope)
            state.books[canon] = PositionBook(state.journals[canon])
            state.journal_revisions[canon] = 0
        state.stream_scopes[canon] = scope
    else:
        state.journals[canon] = AccountJournal(key, stream_scope=scope)
        state.books[canon] = PositionBook(state.journals[canon])
        state.stream_scopes[canon] = scope
        state.journal_revisions[canon] = 0
        state.last_sequences.pop(canon, None)
        state.recovery_adoption_scope = scope


def durable_evidence_identity(evidence: ExecutionEvidence) -> ExecutionEvidenceIdentity:
    """Build the canonical durable identity for one source evidence event."""
    return ExecutionEvidenceIdentity(
        evidence_id=evidence.evidence_id,
        payload_digest=digest_json_payload(_canonical_evidence_payload(evidence)),
        accepted_at=evidence.observed_at,
        sequence=evidence.sequence,
    )
