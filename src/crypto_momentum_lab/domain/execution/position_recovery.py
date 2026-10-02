"""Recovery calculation independent of ExecutionBook, transactions and storage.

Callers own loading, locks and publication. Only newly built journal/book values
are mutated here; migration diagnostics are returned to the caller.
"""

from dataclasses import dataclass, replace
from decimal import Decimal

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.evidence_codec import (
    recovery_checkpoint_head_binding,
)
from crypto_momentum_lab.domain.execution.evidence_digest import (
    trade_payload_digest,
    view_projection_digest,
)
from crypto_momentum_lab.domain.execution.ports import DurableExecutionPositionState
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger import (
    PositionLedger,
    checkpoint_suffix_facts,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    AccountFillLoadProvenance,
    CoverageEvidence,
    FactCoverageStatus,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.domain.execution.recovery_models import (
    DurableJournalCut,
    PositionRecoveryCheckpoint,
    StreamCheckpointAdoption,
)


def rebuild_ordered_scan_journal(
    *,
    journal: AccountJournal,
    proof: CoverageEvidence,
    provenance: AccountFillLoadProvenance,
    scanned_fills: tuple[AccountFillEvent, ...],
) -> AccountJournal:
    """Reconstruct arrival-order lateness only from an exhaustive matching scan.

    Facts at/before a folded checkpoint and conflicting facts cannot be healed
    by replay. Missing previously observed trades also invalidate the scan.
    The caller must still validate and commit the resulting checkpoint.
    """
    facts = journal.read_cut()
    start, end = provenance.source_anchor_event_cut, proof.checkpoint_event_cut
    if (
        not facts.has_late_events
        or not facts.late_fills
        or facts.conflicting_fills
        or facts.fact_conflicts
        or facts.has_synthetic_fills
        or facts.integrity_issues
        or proof.load_provenance != provenance
        or journal.stream_scope != provenance.stream_scope
        or end is None
        or not proof.proves_complete(start, end, expected_scope=journal.stream_scope)
    ):
        return journal
    checkpoint = facts.recovery_checkpoint
    if any(
        not (start < fill.trade_at <= end)
        or (checkpoint is not None and fill.trade_at <= checkpoint.event_cut)
        for fill in facts.late_fills
    ):
        return journal
    scanned: dict[str, str] = {}
    for fill in scanned_fills:
        digest = trade_payload_digest(fill)
        if fill.trade_id in scanned and scanned[fill.trade_id] != digest:
            return journal
        scanned[fill.trade_id] = digest
    if any(
        scanned.get(fill.trade_id) != trade_payload_digest(fill)
        for fill in facts.fills
        if start < fill.trade_at <= end
    ):
        return journal
    # Re-run normal journal ingestion in event-time order. No facts are deleted,
    # and no source coverage, conflicts or checkpoint prefix are manufactured.
    ordered = AccountJournal(facts.position_key, stream_scope=journal.stream_scope)
    for fill in sorted(facts.fills, key=lambda item: (item.trade_at, item.trade_id)):
        ordered.append_fill(fill)
    rebuilt = replace(
        facts,
        has_late_events=ordered.read_cut().has_late_events,
        late_fills=ordered.read_cut().late_fills,
    )
    assert journal.stream_scope is not None
    return AccountJournal.from_durable_cut(
        DurableJournalCut(
            scope=journal.stream_scope,
            facts=rebuilt,
            revision=journal.revision,
            as_of=end,
            checkpoint=checkpoint,
        )
    )


def create_verified_recovery_checkpoint(
    *,
    key: PositionKey,
    scope: AccountFactStreamScope,
    journal: AccountJournal,
    proof: CoverageEvidence | None,
    provenance: AccountFillLoadProvenance | None,
    adoption: StreamCheckpointAdoption | None,
    adopting_epoch: bool,
) -> PositionRecoveryCheckpoint | None:
    if proof is None or provenance is None:
        return None
    coverage_start = provenance.source_anchor_event_cut
    if (
        proof.load_provenance != provenance
        or proof.stream_scope != scope
        or provenance.stream_scope != scope
        or not provenance.is_complete
        or not proof.page_exhausted
        or not proof.not_truncated
        or proof.fill_load_start is None
        or proof.checkpoint_event_cut is None
        or not proof.proves_complete(
            coverage_start,
            proof.checkpoint_event_cut,
            expected_scope=scope,
        )
    ):
        return None

    facts = journal.read_cut()
    previous_checkpoint = facts.recovery_checkpoint
    if previous_checkpoint is not None and (
        previous_checkpoint.event_cut >= proof.checkpoint_event_cut
    ):
        return None
    if provenance.source_anchor_kind == "recovery_checkpoint":
        if adoption is not None:
            parent = adoption.parent_checkpoint
            if (
                parent.checkpoint_id != provenance.source_anchor_id
                or parent.event_cut != provenance.source_anchor_event_cut
                or parent.key.canonical_id != key.canonical_id
            ):
                return None
        elif (
            previous_checkpoint is None
            or previous_checkpoint.checkpoint_id != provenance.source_anchor_id
            or previous_checkpoint.event_cut != provenance.source_anchor_event_cut
        ):
            return None
    elif provenance.source_anchor_kind == "zero_snapshot":
        if adopting_epoch and previous_checkpoint is not None:
            return None
    else:
        return None

    checkpoint_facts = facts
    if adoption is not None:
        checkpoint_facts = replace(facts, prefix_facts_complete=False)
    elif (
        provenance.source_anchor_kind == "recovery_checkpoint"
        and previous_checkpoint is not None
    ):
        checkpoint_facts = replace(
            checkpoint_suffix_facts(facts, previous_checkpoint.event_cut),
            recovery_checkpoint=previous_checkpoint,
        )
    checkpoint = PositionLedger(key).create_recovery_checkpoint(
        checkpoint_facts,
        source_revision=journal.revision,
        event_cut=proof.checkpoint_event_cut,
        stream_adoption=adoption,
    )
    if (
        checkpoint.coverage is None
        or checkpoint.coverage.status != FactCoverageStatus.CONFIRMED
        or checkpoint.coverage.stream_scope != scope
        or not checkpoint.coverage.covers_range(
            coverage_start,
            proof.checkpoint_event_cut,
        )
        or checkpoint.has_conflicts
        or checkpoint.has_synthetic_fills
        or checkpoint.has_late_events
        or checkpoint.integrity_issues
        or not checkpoint.projection.is_comparable
        or checkpoint.projection.health_status.value != "READY"
        or checkpoint.projection.reconciliation_gap != Decimal("0")
    ):
        return None

    facts_at_cut = journal.read_cut(proof.checkpoint_event_cut)
    anchor_snapshots = tuple(
        snapshot
        for snapshot in facts_at_cut.snapshots
        if snapshot.observed_at == provenance.source_anchor_event_cut
        and snapshot.environment == key.environment
        and snapshot.account_label == key.account_label
        and snapshot.symbol == key.symbol
        and snapshot.position_side == key.position_side.value
    )
    if provenance.source_anchor_kind == "zero_snapshot":
        if provenance.source_anchor_event_cut != proof.fill_load_start or not any(
            snapshot.position_amt == Decimal("0") for snapshot in anchor_snapshots
        ):
            return None
    elif adoption is not None:
        if (
            adoption.target_scope != scope
            or adoption.fill_load_provenance != provenance
            or adoption.target_event_cut != proof.checkpoint_event_cut
            or adoption.parent_checkpoint.event_cut
            != provenance.source_anchor_event_cut
            or adoption.parent_checkpoint.checkpoint_id != provenance.source_anchor_id
        ):
            return None
    elif not anchor_snapshots and previous_checkpoint is None:
        return None

    latest_snapshot = max(
        facts_at_cut.snapshots,
        key=lambda snapshot: snapshot.observed_at,
        default=None,
    )
    if (
        latest_snapshot is None
        or latest_snapshot.observed_at != proof.checkpoint_event_cut
    ):
        return None
    if latest_snapshot.position_amt == Decimal("0"):
        if checkpoint.projection.total_active_quantity != Decimal("0"):
            return None
    elif (
        checkpoint.projection.total_active_quantity != abs(latest_snapshot.position_amt)
        or not checkpoint.projection.active_batches
    ):
        return None
    return checkpoint


@dataclass(frozen=True, slots=True)
class RecoveredPosition:
    key: PositionKey
    journal: AccountJournal
    book: PositionBook
    head_revision: int
    projection_digest: str | None
    reservation_ids: frozenset[str]
    last_sequence: int | None
    diagnostics: tuple[tuple[str, dict[str, object]], ...]


def recover_durable_position(state: DurableExecutionPositionState) -> RecoveredPosition:
    scope = state.scope
    key = PositionKey(
        scope.environment, scope.account_label, scope.symbol, scope.position_side
    )
    diagnostics: list[tuple[str, dict[str, object]]] = []
    projection_digest = None
    reservation_ids: set[str] = set()
    last_sequence = None
    if state.cut.scope != scope or state.cut.facts.position_key != key:
        raise RuntimeError("durable position recovery identity mismatch")
    journal = AccountJournal.from_durable_cut(state.cut)
    book = PositionBook(journal)
    head = state.head
    if head is not None:
        payload = head.state_payload
        stored_reservations = payload.get("active_reservation_ids")
        expected_key = {
            "environment": key.environment,
            "account_label": key.account_label,
            "symbol": key.symbol,
            "position_side": key.position_side.value,
        }
        expected_scope = {
            "stream_id": scope.stream_id,
            "stream_epoch": scope.stream_epoch,
        }
        if (
            head.revision < 1
            or payload.get("schema_version") != 1
            or payload.get("position_key") != expected_key
            or (payload.get("stream_scope") != expected_scope)
            or (not isinstance(payload.get("facts_hash"), str))
            or (not isinstance(payload.get("projection_digest"), str))
            or (not isinstance(payload.get("view_digest"), str))
            or (type(payload.get("journal_revision")) is not int)
            or (payload.get("journal_revision") != journal.revision)
            or (not isinstance(stored_reservations, list))
            or any(
                not isinstance(value, str) or not value for value in stored_reservations
            )
        ):
            raise RuntimeError("durable execution head is malformed")
        facts = journal.read_cut()
        expected_checkpoint = recovery_checkpoint_head_binding(
            facts.recovery_checkpoint
        )
        if payload.get("recovery_checkpoint") != expected_checkpoint:
            diagnostics.append(
                (
                    "execution_head_recovery_checkpoint_migrated",
                    {
                        "account_label": key.account_label,
                        "symbol": key.symbol,
                        "position_side": key.position_side.value,
                        "old_checkpoint": payload.get("recovery_checkpoint"),
                        "new_checkpoint": expected_checkpoint,
                    },
                )
            )
        if (
            facts.prefix_facts_complete
            and payload["facts_hash"] != facts.compute_facts_hash()
        ):
            diagnostics.append(
                (
                    "execution_head_facts_migrated",
                    {
                        "account_label": key.account_label,
                        "symbol": key.symbol,
                        "position_side": key.position_side.value,
                        "old_facts_hash": payload["facts_hash"],
                        "new_facts_hash": facts.compute_facts_hash(),
                        "has_active_reservations": bool(
                            payload.get("active_reservation_ids")
                        ),
                    },
                )
            )
        projection = PositionLedger(key).project(facts)
        projection_digest = PositionRecoveryCodec.compute_projection_digest(projection)
        if payload["projection_digest"] != projection_digest:
            diagnostics.append(
                (
                    "execution_head_projection_migrated",
                    {
                        "account_label": key.account_label,
                        "symbol": key.symbol,
                        "position_side": key.position_side.value,
                        "old_projection_digest": payload["projection_digest"],
                        "new_projection_digest": projection_digest,
                        "has_active_reservations": bool(
                            payload.get("active_reservation_ids")
                        ),
                    },
                )
            )
        view = book.get_view()
        if payload["view_digest"] != view_projection_digest(view):
            diagnostics.append(
                (
                    "execution_head_view_migrated",
                    {
                        "account_label": key.account_label,
                        "symbol": key.symbol,
                        "position_side": key.position_side.value,
                        "old_view_digest": payload["view_digest"],
                        "new_view_digest": view_projection_digest(view),
                        "has_active_reservations": bool(
                            payload.get("active_reservation_ids")
                        ),
                    },
                )
            )
        projection_version = (
            head.projection_version.strip()
            if head.projection_version and head.projection_version.strip()
            else view.projection_version
        )
        book.use_durable_projection_version(
            projection_version, event_cut=view.event_cut
        )
        stored_sequence = payload.get("last_sequence")
        if stored_sequence is None:
            last_sequence = None
        elif type(stored_sequence) is int and stored_sequence >= 0:
            last_sequence = stored_sequence
        else:
            diagnostics.append(
                (
                    "durable_execution_head_sequence_invalid",
                    {"sequence": stored_sequence},
                )
            )
            last_sequence = 0
        head_revision = head.revision
        reservation_ids = set(stored_reservations)
    else:
        head_revision = 0
    return RecoveredPosition(
        key,
        journal,
        book,
        head_revision,
        projection_digest,
        frozenset(reservation_ids),
        last_sequence,
        tuple(diagnostics),
    )
