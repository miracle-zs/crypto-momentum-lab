"""Atomic evidence-observation transaction coordinator."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from functools import partial
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook

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
from crypto_momentum_lab.domain.execution.evidence_rules import (
    _scoped_evidence_identity,
)
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
    create_current_state_checkpoint,
)
from crypto_momentum_lab.domain.execution.recovery_models import (
    AccountFacts,
    StreamCheckpointAdoption,
)

log = structlog.get_logger(__name__)


class AbortObservation(Exception):
    def __init__(self, result: ExecutionObserveResult) -> None:
        self.result = result


async def observe_evidence_transaction(
    book: ExecutionBook, evidence: ExecutionEvidence
) -> ExecutionObserveResult:
    """Atomically accept source evidence and publish its projection."""
    if book._execution_unit_of_work is None:
        result = await observe_evidence_group(
            evidence,
            observe_one=book._observe_mutating,
            forget_identity=book._seen_evidence_ids.discard,
        )
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
    book.register_active_stream(evidence)

    for obs_attempt in range(3):
        async with book._mutation_lock(key):
            if book._persistence_failed and not book._stream_scopes:
                raise RuntimeError(
                    "execution facts require initial durable restoration"
                )
            # A truly flat position on exchange with no active local exposure,
            # reservations, or pending commands can adopt or confirm the stream
            # epoch in memory without cloning the book or opening a database
            # transaction. This eliminates thousands of redundant staged copies
            # and transactions on every snapshot cycle.
            flat_result = apply_flat_snapshot(
                book,
                evidence,
                scope=scope,
                requires_verified_stream_adoption=book._requires_verified_stream_adoption,
            )
            if flat_result is not None:
                return flat_result

            # A restored non-flat position from an earlier stream cannot be adopted by
            # an ordinary snapshot. Reject it before cloning current state or
            # opening a transaction; account snapshots may contain thousands
            # of historical symbols on every refresh.
            terminal_without_fill = is_unfilled_terminal_order(evidence)
            pos_book = book._books.get(canon)
            is_flat = pos_book is not None and (
                pos_book.get_view().total_quantity == Decimal("0")
                and not pos_book.get_view().batches
                and not pos_book.get_view().unallocated_quantity
            )
            can_rollover, recovery_proof_required = evaluate_stream_rollover(
                current_scope=book._stream_scopes.get(canon),
                is_flat=is_flat,
                active_reservations=book._coordinator.get_active_reservations(key),
                evidence=evidence,
                is_unfilled_terminal_order=terminal_without_fill,
                requires_verified_stream_adoption=book._requires_verified_stream_adoption(
                    key
                ),
            )
            if recovery_proof_required:
                return WaitingForEvidence(
                    evidence_id=evidence.evidence_id,
                    reason=EvidencePendingReason.STREAM_RECOVERY_PROOF_REQUIRED,
                )
            snapshot_head_revision = book._head_revisions.get(canon, 0)
            candidate = book._staged_copy(key=key)

        # Phase 2: Lock-free durable I/O and CAS commit (I2: no lock across I/O)
        try:
            async with book._execution_unit_of_work.transaction(key) as tx:
                head = await tx.load_head(key)
                adopting_epoch = False
                if head is None:
                    expected_head_revision = 0
                    if (
                        snapshot_head_revision != 0
                        or book._head_revisions.get(canon, 0) != 0
                    ):
                        if obs_attempt < 2:
                            await book.reload_position(key)
                            continue
                        raise RuntimeError(
                            "local execution head exists but durable head is missing"
                        )
                else:
                    expected_head_revision = head.revision
                    if (
                        head.revision != snapshot_head_revision
                        or book._head_revisions.get(canon, 0) != snapshot_head_revision
                    ):
                        if obs_attempt < 2:
                            await book.reload_position(key)
                            continue
                        raise RuntimeError(
                            "execution head changed in another transaction or process;"
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
                if adoption is None and adopting_epoch and head is not None:
                    prov = evidence.fill_load_provenance
                    if (
                        prov is not None
                        and prov.source_anchor_kind == "recovery_checkpoint"
                        and prov.is_complete
                    ):
                        parent_scope = AccountFactStreamScope.for_position_key(
                            key,
                            stream_id=head.stream_id,
                            stream_epoch=head.stream_epoch,
                        )
                        if parent_scope != scope:
                            persisted_parent = await tx.load_checkpoint_from_head(
                                scope=parent_scope,
                                checkpoint_id=prov.source_anchor_id,
                            )
                            if persisted_parent is None:
                                raise AbortObservation(
                                    WaitingForEvidence(
                                        evidence_id=evidence.evidence_id,
                                        reason=EvidencePendingReason.PARENT_CHECKPOINT_UNAVAILABLE,
                                    )
                                )
                            if persisted_parent.checkpoint_id != prov.source_anchor_id:
                                raise AbortObservation(
                                    EvidenceConflict(
                                        evidence_id=evidence.evidence_id,
                                        reason=(
                                            "fill scan parent checkpoint "
                                            "identity mismatch"
                                        ),
                                    )
                                )
                            local_book = candidate._books.get(canon)
                            local_parent = (
                                local_book.current_facts.recovery_checkpoint
                                if local_book is not None
                                and local_book.stream_scope == parent_scope
                                else None
                            )
                            if (
                                local_parent is not None
                                and local_parent != persisted_parent
                            ):
                                raise AbortObservation(
                                    EvidenceConflict(
                                        evidence_id=evidence.evidence_id,
                                        reason=(
                                            "checkpoint adoption parent differs from "
                                            "its immutable durable checkpoint"
                                        ),
                                    )
                                )
                            parent_checkpoint = local_parent or persisted_parent
                            target_event_cut = (
                                prov.observed_at
                                if prov.observed_at > parent_checkpoint.event_cut
                                else (
                                    evidence.source_anchor_snapshot.observed_at
                                    if evidence.source_anchor_snapshot is not None
                                    and evidence.source_anchor_snapshot.observed_at
                                    > parent_checkpoint.event_cut
                                    else evidence.observed_at
                                )
                            )
                            try:
                                adoption = StreamCheckpointAdoption(
                                    parent_checkpoint=parent_checkpoint,
                                    target_scope=scope,
                                    fill_load_provenance=prov,
                                    target_event_cut=target_event_cut,
                                )
                            except ValueError as err:
                                raise AbortObservation(
                                    EvidenceConflict(
                                        evidence_id=evidence.evidence_id,
                                        reason=str(err),
                                    )
                                ) from err

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
                    local_book = candidate._books.get(canon)
                    local_parent = (
                        local_book.current_facts.recovery_checkpoint
                        if local_book is not None
                        and local_book.stream_scope == parent_scope
                        else None
                    )
                    if (
                        adoption.target_scope != scope
                        or adoption.parent_checkpoint.stream_scope != parent_scope
                        or (
                            local_parent is not None
                            and adoption.parent_checkpoint != local_parent
                        )
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
                    persisted_parent = await tx.load_checkpoint_from_head(
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
                        candidate,
                        key=key,
                        scope=scope,
                        can_rollover=can_rollover,
                        is_unfilled_terminal_order=terminal_without_fill,
                    )
                else:
                    candidate._book_for_scope(key, scope)
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
                    accepted
                    and not adopting_epoch
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
                    candidate._ensure_book(key).apply_delta(
                        AccountFacts(
                            position_key=key,
                            snapshots=(evidence.source_anchor_snapshot,),
                            stream_scope=scope,
                            prefix_facts_complete=False,
                        )
                    )
                book_before = candidate._ensure_book(key)
                facts_before = book_before.current_facts
                position_state_before = book_before.encode_current_state()
                fills_to_record = evidence.fills or (
                    (evidence.fill,) if evidence.fill is not None else ()
                )
                checkpoint = facts_before.recovery_checkpoint
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
                    if not identity_inserted:
                        candidate._seen_trade_ids.add(fill.trade_id)
                observation_evidence = evidence

                if (
                    not adopting_epoch
                    and checkpoint is not None
                    and checkpoint.stream_scope == scope
                ):
                    prefix_snapshot = observation_evidence.snapshot
                    prefix_boundary = observation_evidence.boundary
                    if (
                        prefix_snapshot is not None
                        and prefix_snapshot.observed_at <= checkpoint.event_cut
                    ):
                        same_time = tuple(
                            item
                            for item in facts_before.snapshots
                            if item.observed_at == prefix_snapshot.observed_at
                        )
                        if same_time and prefix_snapshot not in same_time:
                            raise AbortObservation(
                                EvidenceConflict(
                                    evidence_id=evidence.evidence_id,
                                    reason="snapshot identity conflicts with head facts",
                                )
                            )
                    observation_evidence = replace(
                        observation_evidence,
                        snapshot=(
                            None
                            if prefix_snapshot is not None
                            and prefix_snapshot.observed_at <= checkpoint.event_cut
                            and prefix_snapshot in same_time
                            else prefix_snapshot
                        ),
                        boundary=prefix_boundary,
                    )

                candidate._active_transaction = tx
                result = await observe_evidence_group(
                    observation_evidence,
                    observe_one=candidate._observe_mutating,
                    forget_identity=candidate._seen_evidence_ids.discard,
                )
                if isinstance(result, (EvidenceConflict, WaitingForEvidence)):
                    raise AbortObservation(result)
                receipt_only = (
                    head is not None
                    and current_scope == scope
                    and not adopting_epoch
                    and candidate._ensure_book(key).encode_current_state()
                    == position_state_before
                    and candidate._outbox_by_command_id == book._outbox_by_command_id
                    and candidate.get_active_reservations(key)
                    == book.get_active_reservations(key)
                    and candidate._order_cumulative_fills
                    == book._order_cumulative_fills
                    and candidate._order_cumulative_quotes
                    == book._order_cumulative_quotes
                    and candidate._recovery_required_commands
                    == book._recovery_required_commands
                    and candidate._external_recovery_positions
                    == book._external_recovery_positions
                    and candidate._dispatch_reconciliation_required_commands
                    == book._dispatch_reconciliation_required_commands
                )

                if receipt_only:
                    candidate._active_transaction = None
                else:
                    checkpoint = None
                    if (
                        evidence.coverage_evidence is not None
                        and evidence.fill_load_provenance is not None
                    ):
                        current_book = candidate._ensure_book(key)
                        try:
                            checkpoint = create_current_state_checkpoint(
                                book=current_book,
                                proof=evidence.coverage_evidence,
                                provenance=evidence.fill_load_provenance,
                                scanned_fills=fills_to_record,
                                adoption=adoption,
                                adopting_epoch=adopting_epoch,
                                source_revision=current_book._revision,
                            )
                        except ValueError as err:
                            raise AbortObservation(
                                EvidenceConflict(
                                    evidence_id=evidence.evidence_id,
                                    reason=str(err),
                                )
                            ) from err
                        if checkpoint is not None:
                            verification = replace(
                                current_book.current_facts,
                                recovery_checkpoint=checkpoint,
                                has_late_events=False,
                                has_synthetic_fills=False,
                                conflicting_fills=(),
                                fact_conflicts=(),
                                integrity_issues=(),
                            )
                            candidate._books[canon] = PositionBook.from_current_state(
                                position_key=key,
                                stream_scope=scope,
                                projection=checkpoint.projection,
                                verification=verification,
                                episode_sequence=checkpoint.projection.episode_sequence,
                                batch_sequence=checkpoint.projection.batch_sequence,
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
                    if isinstance(result, Applied):
                        result = replace(
                            result,
                            updated_view_token=candidate._ensure_book(key)
                            .get_view(now=evidence.observed_at)
                            .projection_version,
                        )
                    if evidence.sequence is not None:
                        candidate._last_sequences[canon] = max(
                            candidate._last_sequences.get(canon, evidence.sequence),
                            evidence.sequence,
                        )

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
                        candidate,
                        key,
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

            if receipt_only:
                async with book._mutation_lock(key):
                    view_token = (
                        book._ensure_book(key)
                        .get_view(now=evidence.observed_at)
                        .projection_version
                    )
                    book._seen_evidence_ids.add(
                        _scoped_evidence_identity(scope, evidence.evidence_id)
                    )
                    if (
                        evidence.sequence is not None
                        and book._stream_scopes.get(canon) == scope
                    ):
                        book._last_sequences[canon] = max(
                            book._last_sequences.get(canon, evidence.sequence),
                            evidence.sequence,
                        )
                return Duplicate(
                    evidence_id=evidence.evidence_id,
                    view_token=view_token,
                )

            # The evidence is already committed; publication must not replay it.
            def _read_published_view(
                previous: ExecutionObserveResult,
            ) -> ExecutionObserveResult:
                token = (
                    book._ensure_book(key)
                    .get_view(now=evidence.observed_at)
                    .projection_version
                )
                if isinstance(previous, Applied):
                    return replace(previous, updated_view_token=token)
                if isinstance(previous, Duplicate):
                    return replace(previous, view_token=token)
                return previous

            published_result = await book._publish_committed_candidate(
                candidate,
                key=key,
                after=partial(_read_published_view, result),
            )
            if published_result is None:
                raise RuntimeError("published evidence result vanished")
            return published_result
        except AbortObservation as abort:
            return abort.result
        except Exception as err:
            if obs_attempt < 2 and (
                "execution head changed" in str(err) or "restore required" in str(err)
            ):
                await book.reload_position(key)
                continue
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
    raise RuntimeError("exhausted maximum observation attempts")
