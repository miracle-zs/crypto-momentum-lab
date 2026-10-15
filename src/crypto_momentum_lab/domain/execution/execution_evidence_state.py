"""State owned by the evidence-ingestion path."""

from __future__ import annotations

from collections.abc import Callable, Collection
from decimal import Decimal
from typing import TYPE_CHECKING

from crypto_momentum_lab.domain.execution.command_models import DispatchState
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
    PositionHealthStatus,
    PositionKey,
    PositionView,
)
from crypto_momentum_lab.domain.execution.recovery_models import AccountFacts
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
    current_view = current_book.get_view() if current_book is not None else None
    is_local_flat = current_view is None or (
        current_view.total_quantity == Decimal("0")
        and not current_view.batches
        and not current_view.unallocated_quantity
    )
    is_evidence_flat = (
        evidence.snapshot is not None
        and evidence.snapshot.position_amt == Decimal("0")
        and not (evidence.fills or evidence.fill)
    )
    has_no_reservations = not book._coordinator.get_active_reservations(key)
    has_any_commands = any(
        entry.scope.to_position_key() == key
        for entry in book._outbox_by_command_id.values()
    )
    has_no_active_commands = not _has_active_commands(book, key)
    account_flat_rebase = (
        evidence.account_snapshot_is_flat
        and is_evidence_flat
        and has_no_reservations
        and has_no_active_commands
    )
    if not (
        (is_local_flat or account_flat_rebase)
        and is_evidence_flat
        and has_no_reservations
        and (not has_any_commands or account_flat_rebase)
        and evidence.coverage_evidence is None
        and evidence.stream_checkpoint_adoption is None
    ):
        return None
    snapshot = evidence.snapshot
    if snapshot is None:
        return None
    current_scope = book._stream_scopes.get(canon)
    if (
        book._execution_unit_of_work is not None
        and book._head_revisions.get(canon, 0) > 0
        and account_flat_rebase
    ):
        # A confirmed account-flat baseline must update the durable head too.
        return None
    if (
        requires_verified_stream_adoption(key)
        and current_scope != scope
        and not account_flat_rebase
    ):
        return WaitingForEvidence(
            evidence_id=evidence.evidence_id,
            reason=EvidencePendingReason.STREAM_RECOVERY_PROOF_REQUIRED,
        )
    if canon not in book._books or current_scope != scope:
        book._books[canon] = PositionBook(key, stream_scope=scope)
        book._last_sequences.pop(canon, None)
    book._stream_scopes[canon] = scope
    position_book = book._ensure_book(key)
    if account_flat_rebase:
        position_book.reset_stream(scope, preserve_episode_sequence=True)
        _clear_flat_account_recovery(book, key)
    elif position_book.stream_scope != scope:
        position_book.reset_stream(scope)
    position_book.apply_delta(
        AccountFacts(
            position_key=key,
            snapshots=(snapshot,),
            stream_scope=scope,
            prefix_facts_complete=False,
        )
    )
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
    has_active_commands: bool,
    matching_nonflat_snapshot: bool,
) -> tuple[bool, bool]:
    """Return ``(can_rollover, requires_recovery_proof)`` for a new stream.

    Pure computation receiving explicit inputs and returning decision tuple.
    """
    key = evidence.scope.to_position_key()
    if evidence.stream_id is None or evidence.stream_epoch is None:
        return False, False
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
    has_plain_flat_snapshot = (
        evidence.snapshot is not None
        and evidence.snapshot.position_amt == Decimal("0")
        and not (evidence.fills or evidence.fill)
        and evidence.coverage_evidence is None
        and evidence.stream_checkpoint_adoption is None
        and evidence.source_anchor_snapshot is None
    )
    can_rollover = (
        not has_blocking_reservations
        and evidence.coverage_evidence is None
        and evidence.stream_checkpoint_adoption is None
        and evidence.source_anchor_snapshot is None
        and (
            (
                evidence.account_snapshot_is_flat
                and has_plain_flat_snapshot
                and not has_active_commands
            )
            or (matching_nonflat_snapshot and not has_active_commands)
            or (
                not requires_verified_stream_adoption
                and is_flat
                and (has_plain_flat_snapshot or is_unfilled_terminal_order)
            )
        )
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


def matching_nonflat_snapshot_can_rollover(
    *,
    current_view: PositionView | None,
    current_scope: AccountFactStreamScope | None,
    evidence: ExecutionEvidence,
    known_trade_ids: Collection[str] = (),
) -> bool:
    """Whether a snapshot and its fills continue the allocated current position."""
    fills = evidence.fills or ((evidence.fill,) if evidence.fill is not None else ())
    return matching_nonflat_position_snapshot_matches(
        current_view=current_view,
        current_scope=current_scope,
        evidence=evidence,
    ) and all(fill.trade_id in known_trade_ids for fill in fills)


def matching_nonflat_position_snapshot_matches(
    *,
    current_view: PositionView | None,
    current_scope: AccountFactStreamScope | None,
    evidence: ExecutionEvidence,
) -> bool:
    """Whether the snapshot matches a healthy, allocated position head."""
    snapshot = evidence.snapshot
    if current_view is None or current_scope is None or snapshot is None:
        return False
    key = evidence.scope.to_position_key()
    return (
        current_view.stream_scope == current_scope
        and current_view.health_status is not PositionHealthStatus.CONFLICT
        and current_view.discrepancy is None
        and current_view.reconciliation_gap in (None, Decimal("0"))
        and current_view.unallocated_quantity == Decimal("0")
        and current_view.total_quantity > Decimal("0")
        and snapshot.position_amt != Decimal("0")
        and snapshot.position_amt.copy_abs() == current_view.total_quantity
        and snapshot.environment == key.environment
        and snapshot.account_label == key.account_label
        and snapshot.symbol == key.symbol
        and snapshot.position_side == key.position_side.value
        and (
            current_view.event_cut is None
            or snapshot.observed_at >= current_view.event_cut
        )
        and evidence.boundary is None
        and evidence.order_event is None
        and evidence.coverage is None
        and evidence.coverage_evidence is None
        and evidence.stream_checkpoint_adoption is None
        and evidence.source_anchor_snapshot is None
        and evidence.fill_load_provenance is None
        and evidence.cumulative_order is None
    )


def apply_stream_rollover(
    book: ExecutionBook,
    *,
    key: PositionKey,
    scope: AccountFactStreamScope,
    can_rollover: bool,
    is_unfilled_terminal_order: bool,
    account_snapshot_is_flat: bool,
    allow_recovery_prefix: bool = True,
) -> None:
    """Apply an already-validated stream transition to execution book state."""
    canon = key.canonical_id
    position_book = book._books.get(canon)
    if can_rollover:
        book._stream_scopes[canon] = scope
        if position_book is None:
            position_book = PositionBook(key, stream_scope=scope)
            book._books[canon] = position_book
        elif account_snapshot_is_flat:
            position_book.reset_stream(scope, preserve_episode_sequence=True)
        else:
            position_book.adopt_stream_scope(scope)
        if account_snapshot_is_flat:
            _clear_flat_account_recovery(book, key)
        book._last_sequences.pop(canon, None)
        book._recovery_adoption_scope = scope if allow_recovery_prefix else None
    elif is_unfilled_terminal_order:
        if position_book is None:
            book._books[canon] = PositionBook(key, stream_scope=scope)
        else:
            position_book.adopt_stream_scope(scope)
        book._stream_scopes[canon] = scope
    else:
        book._books[canon] = PositionBook(key, stream_scope=scope)
        book._stream_scopes[canon] = scope
        book._last_sequences.pop(canon, None)
        book._recovery_adoption_scope = scope if allow_recovery_prefix else None


def _has_active_commands(book: ExecutionBook, key: PositionKey) -> bool:
    return any(
        entry.scope.to_position_key() == key
        and entry.state not in (DispatchState.TERMINAL, DispatchState.REJECTED)
        for entry in book._outbox_by_command_id.values()
    )


def _clear_flat_account_recovery(book: ExecutionBook, key: PositionKey) -> None:
    canon = key.canonical_id
    recovery_ids = {
        identity
        for identity, position_key in book._external_recovery_positions.items()
        if position_key == key
    }
    recovery_ids.add(f"reservation_divergence:{canon}")
    for entry in book._outbox_by_command_id.values():
        if entry.scope.to_position_key() == key and entry.state in (
            DispatchState.TERMINAL,
            DispatchState.REJECTED,
        ):
            recovery_ids.add(entry.command_id)
    for identity in recovery_ids:
        book._recovery_required_commands.discard(identity)
        book._dispatch_reconciliation_required_commands.discard(identity)
        book._external_recovery_positions.pop(identity, None)
    book._head_expected_reservation_ids.pop(canon, None)


def durable_evidence_identity(evidence: ExecutionEvidence) -> ExecutionEvidenceIdentity:
    """Build the canonical durable identity for one source evidence event."""
    return ExecutionEvidenceIdentity(
        evidence_id=evidence.evidence_id,
        payload_digest=digest_json_payload(_canonical_evidence_payload(evidence)),
        accepted_at=evidence.observed_at,
        sequence=evidence.sequence,
    )
