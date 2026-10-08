"""Atomic evidence-observation transaction coordinator."""

from __future__ import annotations

from dataclasses import replace

import structlog

from crypto_momentum_lab.domain.execution.durable_evidence import (
    DurableEvidenceConflict,
    changed_order_watermarks,
    prepare_durable_evidence,
)
from crypto_momentum_lab.domain.execution.evidence_digest import trade_payload_digest
from crypto_momentum_lab.domain.execution.evidence_grouping import (
    observe_evidence_group,
)
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.execution_evidence_processor import (
    is_unfilled_terminal_order,
)
from crypto_momentum_lab.domain.execution.execution_evidence_state import (
    apply_flat_snapshot,
    apply_stream_rollover,
    durable_evidence_identity,
    evaluate_stream_rollover,
)
from crypto_momentum_lab.domain.execution.execution_head import (
    build_execution_head_payload,
)
from crypto_momentum_lab.domain.execution.observation_models import (
    Applied,
    Duplicate,
    EvidenceConflict,
    EvidencePendingReason,
    ExecutionObserveResult,
    WaitingForEvidence,
)
from crypto_momentum_lab.domain.execution.ports import (
    DecisionCommitConflict,
    ExecutionTradeIdentity,
)
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
)
from crypto_momentum_lab.domain.execution.position_recovery import (
    create_verified_recovery_checkpoint,
    rebuild_ordered_scan_journal,
)

log = structlog.get_logger(__name__)


class AbortObservation(Exception):
    def __init__(self, result: ExecutionObserveResult) -> None:
        self.result = result


async def observe_evidence_transaction(
    book: object, evidence: ExecutionEvidence
) -> ExecutionObserveResult:
    """Atomically accept source evidence and publish its projection."""
    if book._execution_unit_of_work is None:
        result = await observe_evidence_group(
            evidence,
            observe_one=book._observe_mutating,
            forget_identity=book._seen_evidence_ids.discard,
        )
        # Without a durable transaction there is nothing to persist, so the
        # append-only delta must not accumulate in memory.
        for journal in book._journals.values():
            journal.mark_facts_persisted()
        book._context_revision += 1
        return result
    try:
        durable_input = prepare_durable_evidence(evidence)
    except DurableEvidenceConflict as err:
        return EvidenceConflict(evidence_id=evidence.evidence_id, reason=str(err))
    evidence = durable_input.evidence
    scope = durable_input.scope
    key = evidence.scope.to_position_key()
    canon = key.canonical_id
    book.evidence_state.register_active_stream(evidence)

    async with book._mutation_lock(key):
        if book._persistence_failed and not book._stream_scopes:
            raise RuntimeError("execution facts require initial durable restoration")
        # A truly flat position on exchange with no active local exposure,
        # reservations, or pending commands can adopt or confirm the stream
        # epoch in memory without cloning the book or opening a database
        # transaction. This eliminates thousands of redundant staged copies
        # and transactions on every snapshot cycle.
        flat_result = apply_flat_snapshot(
            book.evidence_state,
            evidence,
            scope=scope,
            requires_verified_stream_adoption=book._requires_verified_stream_adoption,
        )
        if flat_result is not None:
            return flat_result

        # A restored non-flat position from an earlier stream cannot be adopted by
        # an ordinary snapshot. Reject it before cloning its journal or
        # opening a transaction; account snapshots may contain thousands
        # of historical symbols on every refresh.
        terminal_without_fill = is_unfilled_terminal_order(evidence)
        can_rollover, recovery_proof_required = evaluate_stream_rollover(
            book.evidence_state,
            evidence,
            is_unfilled_terminal_order=terminal_without_fill,
            requires_verified_stream_adoption=book._requires_verified_stream_adoption,
        )
        if recovery_proof_required:
            return WaitingForEvidence(
                evidence_id=evidence.evidence_id,
                reason=EvidencePendingReason.STREAM_RECOVERY_PROOF_REQUIRED,
            )
        candidate = book._staged_copy(key=key)
        try:
            async with book._execution_unit_of_work.transaction(key) as tx:
                head = await tx.load_head(key)
                adopting_epoch = False
                if head is None:
                    expected_head_revision = 0
                    if book._head_revisions.get(canon, 0) != 0:
                        raise RuntimeError(
                            "local execution head exists but durable head is missing"
                        )
                else:
                    expected_head_revision = head.revision
                    if book._head_revisions.get(canon) != head.revision:
                        raise RuntimeError(
                            "execution head changed in another process;"
                            " restore required"
                        )
                    if (
                        head.stream_id != scope.stream_id
                        or head.stream_epoch != scope.stream_epoch
                    ):
                        adopting_epoch = True
                        if (
                            not can_rollover
                            and not terminal_without_fill
                            and (
                                evidence.coverage_evidence is None
                                or evidence.fill_load_provenance is None
                                or not evidence.fill_load_provenance.is_complete
                            )
                        ):
                            raise AbortObservation(
                                WaitingForEvidence(
                                    evidence_id=evidence.evidence_id,
                                    reason=(
                                        EvidencePendingReason.STREAM_RECOVERY_PROOF_REQUIRED
                                    ),
                                )
                            )
                        current_book = candidate._books.get(canon)
                        if (
                            current_book is None
                            or current_book.get_view().projection_version
                            != head.projection_version
                        ):
                            reloaded = await candidate._reload_position(key)
                            if (
                                reloaded is None
                                or reloaded.projection_version
                                != head.projection_version
                            ):
                                raise AbortObservation(
                                    EvidenceConflict(
                                        evidence_id=evidence.evidence_id,
                                        reason=(
                                            "local position facts do not match the "
                                            "durable head before stream adoption"
                                        ),
                                    )
                                )
                    else:
                        current_view = candidate._ensure_book(key).get_view()
                        if current_view.projection_version != head.projection_version:
                            reloaded = await candidate._reload_position(key)
                            if (
                                reloaded is None
                                or reloaded.projection_version
                                != head.projection_version
                            ):
                                raise AbortObservation(
                                    WaitingForEvidence(
                                        evidence_id=evidence.evidence_id,
                                        reason=(
                                            EvidencePendingReason.STREAM_RECOVERY_PROOF_REQUIRED
                                        ),
                                    )
                                )

                current_scope = candidate._stream_scopes.get(canon)
                if current_scope is not None and current_scope != scope:
                    adopting_epoch = True
                    if (
                        not can_rollover
                        and not terminal_without_fill
                        and (
                            evidence.coverage_evidence is None
                            or evidence.fill_load_provenance is None
                            or not evidence.fill_load_provenance.is_complete
                        )
                    ):
                        raise AbortObservation(
                            WaitingForEvidence(
                                evidence_id=evidence.evidence_id,
                                reason=(
                                    EvidencePendingReason.STREAM_RECOVERY_PROOF_REQUIRED
                                ),
                            )
                        )
                    if not adopting_epoch:
                        raise RuntimeError("stream adoption state is inconsistent")

                adoption = evidence.stream_checkpoint_adoption
                if adoption is not None:
                    if not adopting_epoch or head is None:
                        raise AbortObservation(
                            EvidenceConflict(
                                evidence_id=evidence.evidence_id,
                                reason=(
                                    "checkpoint adoption requires an existing "
                                    "durable parent stream head"
                                ),
                            )
                        )
                    parent_scope = AccountFactStreamScope.for_position_key(
                        key,
                        stream_id=head.stream_id,
                        stream_epoch=head.stream_epoch,
                    )
                    local_journal = candidate._journals.get(canon)
                    local_parent = (
                        local_journal.read_cut().recovery_checkpoint
                        if local_journal is not None
                        and local_journal.stream_scope == parent_scope
                        else None
                    )
                    if (
                        adoption.target_scope != scope
                        or adoption.parent_checkpoint.stream_scope != parent_scope
                        or adoption.parent_checkpoint != local_parent
                    ):
                        raise AbortObservation(
                            EvidenceConflict(
                                evidence_id=evidence.evidence_id,
                                reason=(
                                    "checkpoint adoption does not bind the current "
                                    "durable parent checkpoint"
                                ),
                            )
                        )
                    persisted_parent = await tx.load_checkpoint_by_id(
                        scope=parent_scope,
                        checkpoint_id=adoption.parent_checkpoint.checkpoint_id,
                    )
                    if persisted_parent != adoption.parent_checkpoint:
                        raise AbortObservation(
                            EvidenceConflict(
                                evidence_id=evidence.evidence_id,
                                reason=(
                                    "checkpoint adoption parent differs from "
                                    "its immutable durable checkpoint"
                                ),
                            )
                        )

                if adopting_epoch:
                    apply_stream_rollover(
                        candidate.evidence_state,
                        key=key,
                        scope=scope,
                        can_rollover=can_rollover,
                        is_unfilled_terminal_order=terminal_without_fill,
                    )
                    candidate._recovery_adoption_scope = (
                        candidate.evidence_state.recovery_adoption_scope
                    )
                else:
                    candidate._journal_for_scope(key, scope)
                try:
                    accepted = await tx.record_evidence(
                        key=key,
                        stream_id=scope.stream_id,
                        stream_epoch=scope.stream_epoch,
                        evidence=durable_evidence_identity(evidence),
                    )
                except DecisionCommitConflict as err:
                    raise AbortObservation(
                        EvidenceConflict(
                            evidence_id=evidence.evidence_id,
                            reason=str(err),
                        )
                    ) from err
                if not accepted:
                    return Duplicate(
                        evidence_id=evidence.evidence_id,
                        view_token=candidate._ensure_book(key)
                        .get_view()
                        .projection_version,
                    )

                # Check the durable identity before sequence monotonicity.
                # An exact retry after restart carries its original sequence
                # and must be acknowledged as a duplicate. A different event
                # at that sequence is still rejected below.
                previous_sequence = candidate._last_sequences.get(canon)
                if (
                    not adopting_epoch
                    and evidence.sequence is not None
                    and previous_sequence is not None
                    and evidence.sequence <= previous_sequence
                ):
                    raise AbortObservation(
                        EvidenceConflict(
                            evidence_id=evidence.evidence_id,
                            reason=(
                                f"account event sequence {evidence.sequence} "
                                f"does not advance prior sequence "
                                f"{previous_sequence}"
                            ),
                        )
                    )

                if evidence.source_anchor_snapshot is not None:
                    # Only an explicit flat source row may seed a new baseline.
                    candidate._ensure_journal(key).record_snapshot(
                        evidence.source_anchor_snapshot
                    )
                facts_before = candidate._ensure_journal(key).read_cut()
                fills_to_record = evidence.fills or (
                    (evidence.fill,) if evidence.fill is not None else ()
                )
                for fill in fills_to_record:
                    try:
                        identity_inserted = await tx.record_trade(
                            key=key,
                            stream_id=scope.stream_id,
                            stream_epoch=scope.stream_epoch,
                            trade=ExecutionTradeIdentity(
                                trade_id=fill.trade_id,
                                order_id=fill.order_id,
                                quantity=fill.quantity,
                                price=fill.price,
                                side=fill.side,
                                payload_digest=trade_payload_digest(fill),
                                first_seen_at=fill.trade_at,
                            ),
                        )
                    except DecisionCommitConflict as err:
                        raise AbortObservation(
                            EvidenceConflict(
                                evidence_id=evidence.evidence_id,
                                reason=str(err),
                            )
                        ) from err
                    prior_fill = next(
                        (
                            item
                            for item in facts_before.fills
                            if item.trade_id == fill.trade_id
                        ),
                        None,
                    )
                    if (
                        not identity_inserted
                        and prior_fill is None
                        and not adopting_epoch
                    ):
                        raise AbortObservation(
                            EvidenceConflict(
                                evidence_id=evidence.evidence_id,
                                reason=(
                                    f"trade {fill.trade_id} was already consumed "
                                    "but its journal facts are unavailable"
                                ),
                            )
                        )

                candidate._active_transaction = tx
                result = await observe_evidence_group(
                    evidence,
                    observe_one=candidate._observe_mutating,
                    forget_identity=candidate._seen_evidence_ids.discard,
                )
                if isinstance(result, (EvidenceConflict, WaitingForEvidence)):
                    raise AbortObservation(result)
                journal = candidate._ensure_journal(key)
                fact_delta = journal.pending_fact_delta()
                checkpoint = None
                if (
                    evidence.coverage_evidence is not None
                    and evidence.fill_load_provenance is not None
                ):
                    rebuilt = rebuild_ordered_scan_journal(
                        journal=journal,
                        proof=evidence.coverage_evidence,
                        provenance=evidence.fill_load_provenance,
                        scanned_fills=fills_to_record,
                    )
                    rebuilt_order = rebuilt is not journal
                    if rebuilt is not journal:
                        journal = rebuilt
                        candidate._journals[canon] = journal
                        candidate._books[canon] = PositionBook(journal)
                    checkpoint = create_verified_recovery_checkpoint(
                        key=key,
                        scope=scope,
                        journal=candidate._ensure_journal(key),
                        proof=evidence.coverage_evidence,
                        provenance=evidence.fill_load_provenance,
                        adoption=evidence.stream_checkpoint_adoption,
                        adopting_epoch=adopting_epoch,
                        force_checkpoint=rebuilt_order,
                    )
                    if rebuilt_order and checkpoint is None:
                        raise AbortObservation(
                            EvidenceConflict(
                                evidence_id=evidence.evidence_id,
                                reason=(
                                    "ordered replay did not produce a"
                                    " verified checkpoint"
                                ),
                            )
                        )
                if adopting_epoch and not can_rollover and checkpoint is None:
                    raise AbortObservation(
                        EvidenceConflict(
                            evidence_id=evidence.evidence_id,
                            reason=(
                                "stream adoption did not produce a validated "
                                "recovery checkpoint"
                            ),
                        )
                    )
                journal = candidate._ensure_journal(key)
                if checkpoint is not None:
                    journal.set_recovery_checkpoint(checkpoint)
                if isinstance(result, Applied):
                    result = replace(
                        result,
                        updated_view_token=candidate._ensure_book(key)
                        .get_view(now=evidence.observed_at)
                        .projection_version,
                    )
                facts = journal.read_cut()
                persist_result = await tx.persist_facts(
                    scope=scope,
                    facts=facts,
                    revision=journal.revision,
                    checkpoint=checkpoint,
                    delta=fact_delta,
                )
                if persist_result.has_conflicts:
                    raise AbortObservation(
                        EvidenceConflict(
                            evidence_id=evidence.evidence_id,
                            reason=("durable account journal reported a fact conflict"),
                        )
                    )
                candidate._journal_revisions[canon] = persist_result.revision
                if evidence.sequence is not None:
                    candidate._last_sequences[canon] = evidence.sequence

                watermarks = changed_order_watermarks(
                    key,
                    before_quantities=book._order_cumulative_fills,
                    before_quotes=book._order_cumulative_quotes,
                    after_quantities=candidate._order_cumulative_fills,
                    after_quotes=candidate._order_cumulative_quotes,
                    observed_at=evidence.observed_at,
                )
                for watermark in watermarks:
                    await tx.persist_watermark(
                        key=key,
                        stream_id=scope.stream_id,
                        stream_epoch=scope.stream_epoch,
                        watermark=watermark,
                    )
                head_payload = build_execution_head_payload(
                    candidate.evidence_state,
                    key,
                    facts.compute_facts_hash(),
                    ensure_book=candidate._ensure_book,
                    ensure_journal=candidate._ensure_journal,
                    active_reservations=candidate.get_active_reservations,
                )
                candidate._head_revisions[canon] = await tx.persist_head(
                    key=key,
                    stream_id=scope.stream_id,
                    stream_epoch=scope.stream_epoch,
                    expected_revision=expected_head_revision,
                    projection_version=candidate._ensure_book(key)
                    .get_view(now=evidence.observed_at)
                    .projection_version,
                    state_payload=head_payload,
                    updated_at=evidence.observed_at,
                    stream_adoption_checkpoint_id=(
                        checkpoint.checkpoint_id
                        if adopting_epoch and checkpoint is not None
                        else None
                    ),
                    is_flat_adoption=can_rollover,
                )
            candidate._active_transaction = None
            book._publish_candidate(candidate)
            return result
        except AbortObservation as abort:
            return abort.result
        except Exception as err:
            book._persistence_failed = True
            log.error(
                "atomic_execution_observation_failed",
                position_key=key.canonical_id,
                evidence_id=evidence.evidence_id,
                error=str(err),
            )
            raise RuntimeError(
                f"execution evidence was not durably accepted: {err}"
            ) from err
