"""Atomic command acceptance transaction coordinator."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import replace
from decimal import Decimal

import structlog

from crypto_momentum_lab.domain.execution.command_models import DispatchState
from crypto_momentum_lab.domain.execution.execution_action_models import (
    Accepted,
    Blocked,
    ExecutionActResult,
    ExecutionRequest,
)
from crypto_momentum_lab.domain.execution.execution_head import (
    build_execution_head_payload,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderPreSubmissionError,
    OrderProjectionConflictError,
    PreparedOrderSubmission,
)
from crypto_momentum_lab.domain.execution.ports import ExecutionTransactionPort
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    ExitOrderSubmissionFact,
)

log = structlog.get_logger(__name__)


async def accept_action_transaction(
    book: object,
    request: ExecutionRequest,
    *,
    prepare_submission: Callable[
        [ExecutionTransactionPort | None], Awaitable[PreparedOrderSubmission]
    ]
    | None = None,
) -> ExecutionActResult:
    """Accept a command atomically when backed by the durable UoW."""
    if book._execution_unit_of_work is None:
        res = await book._act_mutating(request)
        if isinstance(res, Accepted) and prepare_submission is not None:
            prepared = await prepare_submission(None)
            await book.mark_dispatching(request.request_id)
            res = replace(res, prepared_submission=prepared)
        return res
    key = request.scope.to_position_key()
    canon = key.canonical_id
    async with book._mutation_lock(key):
        stream_scope = book._stream_scopes.get(canon)
        if stream_scope is None:
            return Blocked(
                reason=(
                    "Position has no restored account stream identity; "
                    "execution is fail-closed"
                )
            )
        candidate = book._staged_copy(key=key)
        try:
            account_scope = (
                f"{key.environment}:{key.account_label}:{request.strategy_name}"
            )
            async with book._execution_unit_of_work.transaction(
                key, account_scope=account_scope
            ) as tx:
                head = await tx.load_head(key)
                adopting_epoch = False
                current_view = candidate._ensure_book(key).get_view()
                is_candidate_flat = (
                    current_view.total_quantity == Decimal("0")
                    and not current_view.batches
                )
                if head is None:
                    if is_candidate_flat:
                        expected_revision = 0
                    else:
                        return Blocked(reason="Position facts are not durably restored")
                else:
                    expected_revision = head.revision
                    if (
                        head.stream_id != stream_scope.stream_id
                        or head.stream_epoch != stream_scope.stream_epoch
                    ):
                        is_head_flat = (
                            not head.state_payload.get("active_reservation_ids")
                            and is_candidate_flat
                        )
                        if is_head_flat:
                            adopting_epoch = True
                        else:
                            return Blocked(
                                reason=(
                                    "Position source stream changed without a "
                                    "validated recovery checkpoint"
                                )
                            )
                    else:
                        if book._head_revisions.get(canon) != head.revision:
                            return Blocked(
                                reason=(
                                    "Position projection is stale; reload durable facts"
                                )
                            )
                        if current_view.projection_version != head.projection_version:
                            return Blocked(
                                reason=(
                                    "Position projection differs from its "
                                    "durable head; reload before trading"
                                )
                            )
                candidate._active_transaction = tx
                result = await candidate._act_mutating(request)
                if not isinstance(result, Accepted):
                    return result
                if result.receipt.reservations:
                    batch_capacities = {
                        batch.batch_id: batch.quantity for batch in current_view.batches
                    }
                    await tx.save_reservations(
                        result.receipt.reservations,
                        batch_quantities=batch_capacities,
                        proven_position_quantity=current_view.total_quantity,
                    )
                if prepare_submission is not None:
                    prepared = await prepare_submission(tx)
                    if prepared.plan.reduce_only and request.target_batch_ids:
                        journal = candidate._ensure_journal(key)
                        for batch_id in dict.fromkeys(request.target_batch_ids):
                            journal.record_boundary(
                                ExitOrderSubmissionFact(
                                    order_id=f"{request.request_id}:{batch_id}",
                                    submitted_at=prepared.plan.created_at,
                                    symbol=key.symbol,
                                    position_side=key.position_side,
                                    client_order_id=request.request_id,
                                    target_batch_id=batch_id,
                                )
                            )
                        boundary_facts = journal.read_cut()
                        persisted = await tx.persist_facts(
                            scope=stream_scope,
                            facts=boundary_facts,
                            revision=journal.revision,
                            checkpoint=boundary_facts.recovery_checkpoint,
                            delta=journal.pending_fact_delta(),
                        )
                        if persisted.has_conflicts:
                            raise RuntimeError("exit boundary persistence conflict")
                        candidate._journal_revisions[canon] = persisted.revision
                    await candidate._apply_command_transition(
                        request.request_id,
                        DispatchState.DISPATCHING,
                        at=prepared.submitting_event.occurred_at,
                    )
                    result = replace(result, prepared_submission=prepared)
                facts = candidate._ensure_journal(key).read_cut()
                head_payload = build_execution_head_payload(
                    candidate.evidence_state,
                    key,
                    facts.compute_facts_hash(),
                    ensure_book=candidate._ensure_book,
                    ensure_journal=candidate._ensure_journal,
                    active_reservations=candidate.get_active_reservations,
                )
                next_revision = await tx.persist_head(
                    key=key,
                    stream_id=stream_scope.stream_id,
                    stream_epoch=stream_scope.stream_epoch,
                    expected_revision=expected_revision,
                    projection_version=candidate._ensure_book(key)
                    .get_view()
                    .projection_version,
                    state_payload=head_payload,
                    updated_at=request.created_at,
                    is_flat_adoption=adopting_epoch,
                )
                candidate._head_revisions[canon] = next_revision
            candidate._active_transaction = None
            book._publish_candidate(candidate)
            return result
        except (OrderPreSubmissionError, OrderProjectionConflictError):
            raise
        except Exception as err:
            book._persistence_failed = True
            log.error(
                "atomic_execution_acceptance_failed",
                position_key=key.canonical_id,
                request_id=request.request_id,
                error=str(err),
            )
            return Blocked(
                reason="Execution command was not durably accepted",
                diagnostics=(f"{type(err).__name__}: {err}",),
            )
