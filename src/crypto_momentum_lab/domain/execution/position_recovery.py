"""Recovery calculations over the current position state."""

from dataclasses import dataclass, replace
from decimal import Decimal

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.execution_head import (
    decode_execution_head_state,
    is_supported_execution_head_schema,
)
from crypto_momentum_lab.domain.execution.ports import (
    DecisionCommitConflict,
    DurableExecutionPositionState,
)
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger import (
    PositionLedger,
    checkpoint_suffix_facts,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFillLoadProvenance,
    CoverageEvidence,
    FactCoverageStatus,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.domain.execution.recovery_models import (
    AccountFacts,
    PositionRecoveryCheckpoint,
    StreamCheckpointAdoption,
)

def create_current_state_checkpoint(
    *,
    book: PositionBook,
    proof: CoverageEvidence,
    provenance: AccountFillLoadProvenance,
    scanned_fills: tuple[AccountFillEvent, ...],
    adoption: StreamCheckpointAdoption | None,
    adopting_epoch: bool,
    source_revision: int,
) -> PositionRecoveryCheckpoint:
    """Build a checkpoint from one complete scan and the current state only."""
    key = book.position_key
    scope = book.stream_scope
    if scope is None or proof.load_provenance != provenance:
        raise ValueError("checkpoint scan identity is incomplete")
    coverage_start = provenance.source_anchor_event_cut
    coverage_end = proof.checkpoint_event_cut
    if (
        proof.stream_scope != scope
        or provenance.stream_scope != scope
        or not provenance.is_complete
        or not proof.proves_complete(
            coverage_start, coverage_end, expected_scope=scope
        )
    ):
        raise ValueError("checkpoint scan does not prove complete coverage")

    current = book.current_facts
    parent = adoption.parent_checkpoint if adoption is not None else current.recovery_checkpoint
    if parent is not None and adoption is None and coverage_end <= parent.event_cut:
        return None
    latest_snapshot = max(
        current.snapshots, key=lambda snapshot: snapshot.observed_at, default=None
    )
    if latest_snapshot is None or latest_snapshot.observed_at != coverage_end:
        return None
    if provenance.source_anchor_kind == "recovery_checkpoint":
        if (
            parent is None
            or parent.checkpoint_id != provenance.source_anchor_id
            or parent.event_cut != provenance.source_anchor_event_cut
            or parent.key != key
        ):
            raise ValueError("checkpoint scan parent does not match its anchor")
    elif provenance.source_anchor_kind == "zero_snapshot":
        if parent is not None and adopting_epoch:
            raise ValueError("zero-snapshot adoption cannot replace a checkpoint")
    else:
        raise ValueError("unsupported checkpoint source anchor")

    facts = AccountFacts(
        position_key=key,
        fills=tuple(sorted(scanned_fills, key=lambda fill: (fill.trade_at, fill.trade_id))),
        snapshots=current.snapshots,
        exit_boundaries=current.exit_boundaries,
        coverage=current.coverage,
        stream_scope=scope,
        recovery_checkpoint=(
            None if adoption is not None else parent
        ),
        fill_cursor_provenance=current.fill_cursor_provenance,
        fill_load_provenance=provenance,
        prefix_facts_complete=parent is None,
    )
    if parent is not None:
        facts = checkpoint_suffix_facts(facts, parent.event_cut)
    if adoption is not None:
        facts = replace(facts, prefix_facts_complete=False)
    elif parent is not None:
        facts = replace(facts, recovery_checkpoint=parent)
    checkpoint = PositionLedger(key).create_recovery_checkpoint(
        facts,
        source_revision=source_revision,
        event_cut=coverage_end,
        stream_adoption=adoption,
    )
    if (
        checkpoint.coverage is None
        or checkpoint.coverage.status != FactCoverageStatus.CONFIRMED
        or checkpoint.coverage.stream_scope != scope
        or not checkpoint.coverage.covers_range(coverage_start, coverage_end)
        or checkpoint.has_conflicts
        or checkpoint.has_synthetic_fills
        or checkpoint.has_late_events
        or checkpoint.integrity_issues
        or not checkpoint.projection.is_comparable
        or checkpoint.projection.health_status.value != "READY"
        or checkpoint.projection.reconciliation_gap != Decimal("0")
    ):
        raise ValueError("complete source scan did not produce a ready checkpoint")

    anchors = tuple(
        snapshot
        for snapshot in current.snapshots
        if snapshot.observed_at == provenance.source_anchor_event_cut
    )
    if provenance.source_anchor_kind == "zero_snapshot":
        if proof.fill_load_start is None or proof.fill_load_start > provenance.source_anchor_event_cut:
            raise ValueError("zero-snapshot scan begins after its source anchor")
        if not any(snapshot.position_amt == 0 for snapshot in anchors):
            raise ValueError("zero-snapshot source anchor is missing")
    if adoption is not None and (
        adoption.target_scope != scope
        or adoption.fill_load_provenance != provenance
        or adoption.target_event_cut != coverage_end
        or adoption.parent_checkpoint != parent
    ):
        raise ValueError("checkpoint adoption does not match the complete scan")

    if latest_snapshot.position_amt == 0:
        if checkpoint.projection.total_active_quantity != 0:
            raise ValueError("flat snapshot disagrees with checkpoint projection")
    elif (
        checkpoint.projection.total_active_quantity != abs(latest_snapshot.position_amt)
        or not checkpoint.projection.active_batches
    ):
        raise ValueError("account snapshot disagrees with checkpoint projection")
    return checkpoint


@dataclass(frozen=True, slots=True)
class RecoveredPosition:
    key: PositionKey
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
    if state.cut.scope != scope or state.cut.facts.position_key != key:
        raise RuntimeError("durable position recovery identity mismatch")
    head = state.head
    if head is not None:
        payload = head.state_payload
        if head.revision < 1:
            raise DecisionCommitConflict("durable execution head format is unsupported")
        try:
            book = decode_execution_head_state(payload, key=key, scope=scope)
        except Exception as err:
            message = (
                "durable execution head position state is malformed"
                if is_supported_execution_head_schema(payload.get("schema_version"))
                else "durable execution head format is unsupported"
            )
            raise DecisionCommitConflict(
                message
            ) from err
        stored_reservations = payload.get("active_reservation_ids")
        recovery_command_ids = payload.get("recovery_command_ids")
        external_recovery_ids = payload.get("external_recovery_ids")
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
            payload.get("position_key") != expected_key
            or (payload.get("stream_scope") != expected_scope)
            or book.current_facts != state.cut.facts
            or book.stream_scope != scope
            or "last_sequence" not in payload
            or (
                payload["last_sequence"] is not None
                and (
                    type(payload["last_sequence"]) is not int
                    or payload["last_sequence"] < 0
                )
            )
            or (not isinstance(stored_reservations, list))
            or any(
                not isinstance(value, str) or not value for value in stored_reservations
            )
            or (not isinstance(recovery_command_ids, list))
            or any(
                not isinstance(value, str) or not value
                for value in recovery_command_ids
            )
            or (not isinstance(external_recovery_ids, list))
            or any(
                not isinstance(value, str) or not value
                for value in external_recovery_ids
            )
        ):
            raise RuntimeError("durable execution head is malformed")
        if not all(
            isinstance(value, list)
            for value in (
                stored_reservations,
                recovery_command_ids,
                external_recovery_ids,
            )
        ):
            raise RuntimeError("durable execution head recovery links are malformed")
        if any(
            not isinstance(value, str) or not value
            for values in (
                stored_reservations,
                recovery_command_ids,
                external_recovery_ids,
            )
            for value in values
        ):
            raise RuntimeError("durable execution head recovery links are malformed")
        last_sequence = payload["last_sequence"]
        head_revision = head.revision
        reservation_ids = set(stored_reservations)
        view = book.get_view()
        book.use_durable_projection_version(
            head.projection_version,
            event_cut=view.event_cut,
        )
        projection_digest = PositionRecoveryCodec.compute_projection_digest(
            book.projection
        )
    else:
        book = PositionBook.from_facts(state.cut.facts)
        head_revision = 0
        projection_digest = None
        reservation_ids = set()
        last_sequence = None
    return RecoveredPosition(
        key,
        book,
        head_revision,
        projection_digest,
        frozenset(reservation_ids),
        last_sequence,
        (),
    )
