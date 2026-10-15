"""Evidence mutation rules independent from the execution-book facade."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import TYPE_CHECKING

from crypto_momentum_lab.domain.execution.command_models import (
    OutboxEntry,
)
from crypto_momentum_lab.domain.execution.cumulative_report import (
    plan_cumulative_report,
)
from crypto_momentum_lab.domain.execution.evidence_lifecycle import (
    plan_order_event,
)
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.evidence_rules import _evidence_identity
from crypto_momentum_lab.domain.execution.fill_attribution import plan_fill_observation

if TYPE_CHECKING:
    from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.observation_models import (
    Applied,
    Duplicate,
    EvidenceConflict,
    EvidencePendingReason,
    ExecutionObserveResult,
    WaitingForEvidence,
)
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_models import AccountFacts
from crypto_momentum_lab.domain.execution.trade_command import PositionReservation


@dataclass(frozen=True, slots=True)
class EvidenceMutationDependencies:
    """Boundary services required by one evidence mutation."""

    ensure_book: Callable[[PositionKey], PositionBook]
    book_for_scope: Callable[[PositionKey, AccountFactStreamScope], PositionBook]
    command_order_id: Callable[[PositionKey, str], str]
    order_watermark_key: Callable[[PositionKey, str], str]
    active_reservations_for_command: Callable[[str], tuple[PositionReservation, ...]]
    active_reservations: Callable[[PositionKey], tuple[PositionReservation, ...]]
    settle_reservation_quantity: Callable[
        ..., Awaitable[tuple[Decimal, bool, str | None]]
    ]
    persist_transition: Callable[[OutboxEntry], Awaitable[None]]
    release_command_reservations: Callable[..., Awaitable[Decimal]]
    persist_outbox_state: Callable[[OutboxEntry], Awaitable[None]]
    in_transaction: bool


def is_unfilled_terminal_order(evidence: ExecutionEvidence) -> bool:
    event = evidence.order_event
    return (
        event is not None
        and event.state.terminal
        and not evidence.fills
        and evidence.fill is None
        and (
            evidence.cumulative_order is None
            or evidence.cumulative_order.cumulative_quantity == Decimal("0")
        )
    )


def reservations_held_by_terminal_order(
    evidence: ExecutionEvidence, reservations: tuple[PositionReservation, ...]
) -> bool:
    event = evidence.order_event
    return (
        is_unfilled_terminal_order(evidence)
        and event is not None
        and all(
            reservation.command_id == event.client_order_id
            for reservation in reservations
        )
    )


async def mutate_evidence(
    execution_book: ExecutionBook,
    dependencies: EvidenceMutationDependencies,
    evidence: ExecutionEvidence,
) -> ExecutionObserveResult:
    """Idempotently ingest exchange evidence and settle allocations."""
    identity = _evidence_identity(evidence)
    if identity in execution_book._seen_evidence_ids:
        key = evidence.scope.to_position_key()
        book = dependencies.ensure_book(key)
        return Duplicate(
            evidence_id=evidence.evidence_id,
            view_token=book.get_view().projection_version,
        )

    key = evidence.scope.to_position_key()
    terminal_without_fill = is_unfilled_terminal_order(evidence)
    if evidence.stream_id is not None and evidence.stream_epoch is not None:
        scope = AccountFactStreamScope.for_position_key(
            key,
            stream_id=evidence.stream_id,
            stream_epoch=evidence.stream_epoch,
        )
        current_scope = execution_book._stream_scopes.get(key.canonical_id)
        if current_scope is not None and current_scope != scope:
            current_book = execution_book._books.get(key.canonical_id)
            active_res = dependencies.active_reservations(key)
            res_held_by_order = reservations_held_by_terminal_order(
                evidence, active_res
            )
            has_blocking_reservations = bool(active_res) and not res_held_by_order
            current_view = current_book.get_view() if current_book is not None else None
            can_rollover = (
                evidence.coverage_evidence is None
                and current_book is not None
                and (
                    (
                        evidence.snapshot is not None
                        and evidence.snapshot.position_amt == Decimal("0")
                    )
                    or (
                        terminal_without_fill
                        and current_view is not None
                        and current_view.total_quantity == Decimal("0")
                    )
                )
                and current_view is not None
                and current_view.total_quantity == Decimal("0")
                and not current_view.batches
                and not current_view.unallocated_quantity
                and not has_blocking_reservations
            )
            if can_rollover:
                execution_book._stream_scopes[key.canonical_id] = scope
                if current_book is not None:
                    current_book.adopt_stream_scope(scope)
                execution_book._last_sequences.pop(key.canonical_id, None)
            elif not terminal_without_fill:
                return WaitingForEvidence(
                    evidence_id=evidence.evidence_id,
                    reason=EvidencePendingReason.STREAM_RECOVERY_PROOF_REQUIRED,
                )
        try:
            book = dependencies.book_for_scope(key, scope)
        except RuntimeError as err:
            return EvidenceConflict(evidence.evidence_id, str(err))
    else:
        book = dependencies.ensure_book(key)

    consumed_qty = Decimal("0")
    released_qty = Decimal("0")
    settlement_recovery_required = False
    diagnostics: tuple[str, ...] = ()
    pending_watermark: tuple[str, Decimal, Decimal] | None = None
    dispatch_reconciled_command_id: str | None = None
    applied_fills = []

    # Bind exchange identity before processing trades or cumulative reports.
    # Late/terminal observations still supply missing identity, but never
    # rewrite the true order_id of an account trade.
    if evidence.order_event is not None:
        event = evidence.order_event
        entry = execution_book._outbox_by_command_id.get(event.client_order_id)
        if entry is not None and event.exchange_order_id is not None:
            if entry.scope.to_position_key() != key or entry.external_order_id not in (
                None,
                event.exchange_order_id,
            ):
                return EvidenceConflict(
                    evidence.evidence_id, "exchange order identity mismatch"
                )
            owner = dependencies.command_order_id(key, event.exchange_order_id)
            if owner not in (event.exchange_order_id, entry.command_id):
                return EvidenceConflict(
                    evidence.evidence_id, "exchange order already bound"
                )
            if entry.external_order_id is None:
                bound = replace(entry, external_order_id=event.exchange_order_id)
                await dependencies.persist_transition(bound)

    # 1. Process Fill
    if evidence.fill is not None:
        fill = evidence.fill
        trade_id = fill.trade_id
        try:
            fill_plan = plan_fill_observation(
                fill,
                trade_seen=trade_id in execution_book._seen_trade_ids,
                can_adopt_prefix=(
                    dependencies.in_transaction
                    and execution_book._recovery_adoption_scope == book.stream_scope
                    and book.stream_scope is not None
                ),
            )
        except ValueError as error:
            return EvidenceConflict(evidence.evidence_id, str(error))
        if fill_plan.is_new_trade:
            applied_fills.append(fill_plan.fill)
            if not fill_plan.adopted_prefix_trade:
                execution_book._seen_trade_ids.add(fill.trade_id)

    report = evidence.cumulative_order
    if report is not None:
        report = replace(
            report, order_id=dependencies.command_order_id(key, report.order_id)
        )
        watermark_key = dependencies.order_watermark_key(key, report.order_id)
        previous_quantity = execution_book._order_cumulative_fills.get(
            watermark_key, Decimal("0")
        )
        previous_quote = execution_book._order_cumulative_quotes.get(
            watermark_key, Decimal("0")
        )
        try:
            report_plan = plan_cumulative_report(
                report,
                previous_watermark=(previous_quantity, previous_quote),
                outbox=execution_book._outbox_by_command_id.get(report.order_id),
                has_active_reservations=bool(
                    dependencies.active_reservations_for_command(report.order_id)
                ),
            )
        except ValueError as error:
            return EvidenceConflict(evidence.evidence_id, str(error))
        if report_plan.watermark is not None:
            pending_watermark = (watermark_key, *report_plan.watermark)
            if report_plan.settlement_quantity > Decimal("0"):
                (
                    consumed,
                    needs_recovery,
                    settlement_diagnostic,
                ) = await dependencies.settle_reservation_quantity(
                    report.order_id,
                    report_plan.settlement_quantity,
                    reported_quantity=report_plan.reported_quantity,
                    key=key,
                )
                consumed_qty += consumed
                settlement_recovery_required = (
                    settlement_recovery_required or needs_recovery
                )
                if settlement_diagnostic:
                    diagnostics = (settlement_diagnostic,)

    # 2. Process Snapshot
    # 4. Process Order Event
    if evidence.order_event is not None:
        cmd_id = evidence.order_event.client_order_id
        outbox = execution_book._outbox_by_command_id.get(cmd_id)
        event_plan = plan_order_event(
            outbox,
            evidence.order_event,
            observed_at=evidence.observed_at,
        )
        if event_plan.updated is not None:
            assert outbox is not None
            if event_plan.requires_dispatch_reconciliation:
                execution_book._dispatch_reconciliation_required_commands.add(cmd_id)
            await dependencies.persist_transition(event_plan.updated)
            if event_plan.release_reason is not None:
                released_qty += await dependencies.release_command_reservations(
                    cmd_id,
                    reason=event_plan.release_reason,
                )
            if event_plan.dispatch_reconciled:
                dispatch_reconciled_command_id = cmd_id
        if evidence.order_event.state.terminal and not settlement_recovery_required:
            execution_book._recovery_required_commands.discard(cmd_id)

    if pending_watermark is not None:
        watermark_key, cumulative_qty, cumulative_quote = pending_watermark
        execution_book._order_cumulative_fills[watermark_key] = cumulative_qty
        execution_book._order_cumulative_quotes[watermark_key] = cumulative_quote
        if report is not None:
            outbox = execution_book._outbox_by_command_id.get(report.order_id)
            if outbox is not None:
                await dependencies.persist_outbox_state(outbox)

    if dispatch_reconciled_command_id is not None:
        execution_book._dispatch_reconciliation_required_commands.discard(
            dispatch_reconciled_command_id
        )

    delta_snapshots = tuple(
        snapshot
        for snapshot in (evidence.source_anchor_snapshot, evidence.snapshot)
        if snapshot is not None
    )
    if (
        applied_fills
        or delta_snapshots
        or evidence.coverage is not None
        or evidence.boundary is not None
        or evidence.fill_load_provenance is not None
    ):
        try:
            book.apply_delta(
                AccountFacts(
                    position_key=key,
                    fills=tuple(applied_fills),
                    snapshots=delta_snapshots,
                    exit_boundaries=(
                        (evidence.boundary,) if evidence.boundary is not None else ()
                    ),
                    coverage=evidence.coverage,
                    has_synthetic_fills=any(
                        bool(fill.raw_payload.get("synthetic_from_order", False))
                        for fill in applied_fills
                        if isinstance(fill.raw_payload, dict)
                    ),
                    stream_scope=book.stream_scope,
                    fill_load_provenance=evidence.fill_load_provenance,
                    prefix_facts_complete=False,
                )
            )
        except ValueError as error:
            return EvidenceConflict(evidence.evidence_id, str(error))

    if (
        evidence.order_event is not None
        and evidence.order_event.state.terminal
        and not dependencies.active_reservations_for_command(
            evidence.order_event.client_order_id
        )
    ):
        book.remove_exit_boundaries(evidence.order_event.client_order_id)

    execution_book._seen_evidence_ids.add(identity)
    updated_view = book.get_view(now=evidence.observed_at)

    # An external exit has no command or reservation lifecycle. Resolve
    # its position-scoped quarantine only from complete real facts and a
    # subsequent matching account cut, never merely from an order receipt.
    facts = book.current_facts
    latest_snapshot = max(facts.snapshots, key=lambda s: s.observed_at, default=None)
    latest_trade_at = book.latest_trade_at
    coverage = updated_view.coverage
    if (
        updated_view.is_ready_for_trade
        and not dependencies.active_reservations(key)
        and latest_snapshot is not None
        and latest_trade_at is not None
        and latest_snapshot.observed_at >= latest_trade_at
        and abs(latest_snapshot.position_amt) == updated_view.total_quantity
        and coverage is not None
        and coverage.is_authoritative
        and coverage.end_at >= latest_trade_at
    ):
        for identity, position in tuple(
            execution_book._external_recovery_positions.items()
        ):
            if position == key:
                execution_book._external_recovery_positions.pop(identity)
                execution_book._recovery_required_commands.discard(identity)

    return Applied(
        evidence_id=evidence.evidence_id,
        updated_view_token=updated_view.projection_version,
        consumed_quantity=consumed_qty,
        released_quantity=released_qty,
        recovery_required=settlement_recovery_required,
        diagnostics=diagnostics,
    )
