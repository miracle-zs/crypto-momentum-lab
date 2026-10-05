"""Evidence mutation rules independent from the execution-book facade."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from decimal import Decimal

from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.command_models import OutboxEntry
from crypto_momentum_lab.domain.execution.cumulative_report import (
    plan_cumulative_report,
    plan_watermark_publication,
)
from crypto_momentum_lab.domain.execution.evidence_lifecycle import (
    plan_order_event,
    settlement_trade_facts,
    terminal_settlement_is_confirmed,
)
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.evidence_rules import _evidence_identity
from crypto_momentum_lab.domain.execution.evidence_settlement import account_trade_delta
from crypto_momentum_lab.domain.execution.execution_evidence_state import EvidenceState
from crypto_momentum_lab.domain.execution.fill_attribution import (
    is_exit_fill,
    plan_fill_observation,
)
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
from crypto_momentum_lab.domain.execution.trade_command import PositionReservation


@dataclass(frozen=True, slots=True)
class EvidenceMutationDependencies:
    """Boundary services required by one evidence mutation."""

    ensure_book: Callable[[PositionKey], PositionBook]
    ensure_journal: Callable[[PositionKey], AccountJournal]
    journal_for_scope: Callable[[PositionKey, AccountFactStreamScope], AccountJournal]
    command_order_id: Callable[[PositionKey, str], str]
    command_order_ids: Callable[[PositionKey, str], frozenset[str]]
    order_watermark_key: Callable[[PositionKey, str], str]
    active_reservations_for_command: Callable[[str], tuple[PositionReservation, ...]]
    active_reservations: Callable[[PositionKey], tuple[PositionReservation, ...]]
    settle_reservation_quantity: Callable[
        ..., Awaitable[tuple[Decimal, bool, str | None]]
    ]
    persist_transition: Callable[[OutboxEntry], Awaitable[None]]
    release_command_reservations: Callable[..., Awaitable[Decimal]]
    persist_outbox_state: Callable[[OutboxEntry], Awaitable[None]]
    durable: bool
    in_transaction: bool


def is_unfilled_terminal_order(evidence: ExecutionEvidence) -> bool:
    event = evidence.order_event
    return (
        event is not None
        and event.state.terminal
        and not evidence.settlement_fills
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
    state: EvidenceState,
    dependencies: EvidenceMutationDependencies,
    evidence: ExecutionEvidence,
) -> ExecutionObserveResult:
    """Idempotently ingest exchange evidence and settle allocations."""
    identity = _evidence_identity(evidence)
    if identity in state.seen_evidence_ids:
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
        current_scope = state.stream_scopes.get(key.canonical_id)
        if current_scope is not None and current_scope != scope:
            current_book = state.books.get(key.canonical_id)
            active_res = dependencies.active_reservations(key)
            res_held_by_order = reservations_held_by_terminal_order(
                evidence, active_res
            )
            has_blocking_reservations = bool(active_res) and not res_held_by_order
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
                        and current_book.get_view().total_quantity == Decimal("0")
                    )
                )
                and current_book.get_view().total_quantity == Decimal("0")
                and not current_book.get_view().batches
                and not current_book.get_view().unallocated_quantity
                and not has_blocking_reservations
            )
            if can_rollover:
                state.stream_scopes[key.canonical_id] = scope
                journal = state.journals.get(key.canonical_id)
                if journal is not None:
                    journal.adopt_stream_scope(scope)
                state.last_sequences.pop(key.canonical_id, None)
            elif terminal_without_fill:
                pass
            else:
                return WaitingForEvidence(
                    evidence_id=evidence.evidence_id,
                    reason=EvidencePendingReason.STREAM_RECOVERY_PROOF_REQUIRED,
                )
        try:
            journal = dependencies.journal_for_scope(key, scope)
        except RuntimeError as err:
            if terminal_without_fill:
                journal = dependencies.ensure_journal(key)
            else:
                return EvidenceConflict(evidence.evidence_id, str(err))
    else:
        journal = dependencies.ensure_journal(key)
    book = dependencies.ensure_book(key)
    if evidence.fill_load_provenance is not None:
        journal.record_fill_load_provenance(evidence.fill_load_provenance)

    consumed_qty = Decimal("0")
    released_qty = Decimal("0")
    settlement_recovery_required = False
    diagnostics: tuple[str, ...] = ()
    pending_watermark: tuple[str, Decimal, Decimal] | None = None
    dispatch_reconciled_command_id: str | None = None

    # Bind exchange identity before processing trades or cumulative reports.
    # Late/terminal observations still supply missing identity, but never
    # rewrite the true order_id of an account trade.
    if evidence.order_event is not None:
        event = evidence.order_event
        entry = state.outbox_by_command_id.get(event.client_order_id)
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
                watermark_key = dependencies.order_watermark_key(key, entry.command_id)
                trade_delta = account_trade_delta(
                    entry.command_id,
                    previous_quantity=state.order_cumulative_fills.get(
                        watermark_key, Decimal("0")
                    ),
                    previous_quote=state.order_cumulative_quotes.get(
                        watermark_key, Decimal("0")
                    ),
                    account_fills=journal.read_cut().fills,
                    order_ids=frozenset({entry.command_id, event.exchange_order_id}),
                )
                if trade_delta.watermark is not None:
                    quantity, quote = trade_delta.watermark
                    if entry.command.reduce_only:
                        (
                            consumed,
                            needs_recovery,
                            diagnostic,
                        ) = await dependencies.settle_reservation_quantity(
                            entry.command_id,
                            trade_delta.quantity,
                            reported_quantity=quantity,
                            key=key,
                        )
                        consumed_qty += consumed
                        settlement_recovery_required = needs_recovery
                        if diagnostic:
                            diagnostics = (diagnostic,)
                    # These real trades predate the identity receipt. Their
                    # canonical watermark prevents the report counting them
                    # a second time in this same transaction.
                    state.order_cumulative_fills[watermark_key] = quantity
                    state.order_cumulative_quotes[watermark_key] = quote
                    pending_watermark = (watermark_key, quantity, quote)

    # 1. Process Fill
    if evidence.fill is not None:
        fill = evidence.fill
        trade_id = fill.trade_id
        order_id = dependencies.command_order_id(key, fill.order_id)
        existing_trade = next(
            (prior for prior in journal.read_cut().fills if prior.trade_id == trade_id),
            None,
        )
        watermark_key = dependencies.order_watermark_key(key, order_id)
        try:
            fill_plan = plan_fill_observation(
                fill,
                existing_trade=existing_trade,
                trade_seen=trade_id in state.seen_trade_ids,
                previous_quantity=state.order_cumulative_fills.get(
                    watermark_key, Decimal("0")
                ),
                previous_quote=state.order_cumulative_quotes.get(
                    watermark_key, Decimal("0")
                ),
                can_adopt_prefix=(
                    dependencies.in_transaction
                    and state.recovery_adoption_scope == journal.stream_scope
                    and journal.stream_scope is not None
                ),
            )
        except ValueError as error:
            return EvidenceConflict(evidence.evidence_id, str(error))
        delta_qty = fill_plan.quantity
        settlement_delta_qty = fill_plan.settlement_quantity
        cumulative_qty = fill_plan.cumulative_quantity
        applied_fill = fill_plan.fill
        is_cumulative = fill_plan.is_cumulative
        is_new_trade = fill_plan.is_new_trade
        adopted_prefix_trade = fill_plan.adopted_prefix_trade
        if fill_plan.watermark is not None:
            pending_watermark = (watermark_key, *fill_plan.watermark)

        if is_new_trade:
            if delta_qty > Decimal("0"):
                accepted = journal.append_fill(applied_fill)
                if not accepted and journal.has_conflicts:
                    return EvidenceConflict(
                        evidence_id=evidence.evidence_id,
                        reason=(
                            f"Fill {fill.trade_id} conflicted with "
                            "existing journal records"
                        ),
                    )
                if not adopted_prefix_trade:
                    state.seen_trade_ids.add(trade_id)
            elif not is_cumulative:
                state.seen_trade_ids.add(trade_id)

        if not is_cumulative and is_new_trade and not adopted_prefix_trade:
            watermark_key = dependencies.order_watermark_key(key, order_id)
            previous_quantity = state.order_cumulative_fills.get(
                watermark_key, Decimal("0")
            )
            previous_quote = state.order_cumulative_quotes.get(
                watermark_key, Decimal("0")
            )
            try:
                trade_delta = account_trade_delta(
                    order_id,
                    previous_quantity=previous_quantity,
                    previous_quote=previous_quote,
                    account_fills=journal.read_cut().fills,
                    order_ids=dependencies.command_order_ids(key, order_id),
                )
            except ValueError as error:
                return EvidenceConflict(evidence.evidence_id, str(error))
            settlement_delta_qty = trade_delta.quantity
            if trade_delta.watermark is not None:
                pending_watermark = (watermark_key, *trade_delta.watermark)

        active_episode = book.get_view().active_episode
        if is_exit_fill(
            fill,
            position_side=key.position_side,
            episode_side=active_episode.side if active_episode is not None else None,
            has_active_reservations=bool(
                dependencies.active_reservations_for_command(order_id)
            ),
            order_event=evidence.order_event,
        ) and settlement_delta_qty > Decimal("0"):
            (
                consumed,
                needs_recovery,
                settlement_diagnostic,
            ) = await dependencies.settle_reservation_quantity(
                order_id,
                settlement_delta_qty,
                reported_quantity=(
                    cumulative_qty if is_cumulative else settlement_delta_qty
                ),
                key=key,
            )
            consumed_qty += consumed
            settlement_recovery_required = (
                settlement_recovery_required or needs_recovery
            )
            if settlement_diagnostic:
                diagnostics = (settlement_diagnostic,)

    try:
        settlement_facts = settlement_trade_facts(
            journal.read_cut().fills,
            evidence.settlement_fills,
        )
    except ValueError as error:
        return EvidenceConflict(evidence.evidence_id, str(error))
    report = evidence.cumulative_order
    if report is not None:
        report = replace(
            report, order_id=dependencies.command_order_id(key, report.order_id)
        )
        watermark_key = dependencies.order_watermark_key(key, report.order_id)
        previous_quantity = state.order_cumulative_fills.get(
            watermark_key, Decimal("0")
        )
        previous_quote = state.order_cumulative_quotes.get(watermark_key, Decimal("0"))
        try:
            report_plan = plan_cumulative_report(
                report,
                previous_watermark=(previous_quantity, previous_quote),
                account_fills=settlement_facts,
                outbox=state.outbox_by_command_id.get(report.order_id),
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
    if evidence.snapshot is not None:
        journal.record_snapshot(evidence.snapshot)

    # 2b. Process Coverage
    if evidence.coverage is not None:
        journal.set_coverage(evidence.coverage)

    # 3. Process Boundary
    if evidence.boundary is not None:
        journal.record_boundary(evidence.boundary)

    # 4. Process Order Event
    if evidence.order_event is not None:
        cmd_id = evidence.order_event.client_order_id
        outbox = state.outbox_by_command_id.get(cmd_id)
        event_plan = plan_order_event(
            outbox,
            evidence.order_event,
            observed_at=evidence.observed_at,
            account_fills=settlement_facts,
            durable=dependencies.durable,
            has_active_reservations=bool(
                dependencies.active_reservations_for_command(cmd_id)
            ),
        )
        if event_plan.updated is not None:
            assert outbox is not None
            if event_plan.requires_dispatch_reconciliation:
                state.dispatch_reconciliation_required_commands.add(cmd_id)
            await dependencies.persist_transition(event_plan.updated)
            if event_plan.release_reason is not None:
                released_qty += await dependencies.release_command_reservations(
                    cmd_id,
                    reason=event_plan.release_reason,
                )
            if event_plan.pending_trade_diagnostic is not None:
                # Keep admission closed while real position facts lag, but
                # let the account consumer continue receiving those facts.
                state.recovery_required_commands.add(cmd_id)
                if not diagnostics:
                    diagnostics = (event_plan.pending_trade_diagnostic,)
            if event_plan.recovery_diagnostic is not None:
                state.recovery_required_commands.add(cmd_id)
                settlement_recovery_required = True
                diagnostics = (event_plan.recovery_diagnostic,)
            if event_plan.dispatch_reconciled:
                dispatch_reconciled_command_id = cmd_id

    if pending_watermark is not None:
        watermark_key, cumulative_qty, cumulative_quote = pending_watermark
        state.order_cumulative_fills[watermark_key] = cumulative_qty
        state.order_cumulative_quotes[watermark_key] = cumulative_quote
        publication = plan_watermark_publication(
            evidence,
            outbox_by_command_id=state.outbox_by_command_id,
        )
        if publication.outbox is not None:
            await dependencies.persist_outbox_state(publication.outbox)
        elif publication.recovery_diagnostic is not None:
            state.recovery_required_commands.add(publication.command_id)
            settlement_recovery_required = True
            diagnostics = (publication.recovery_diagnostic,)

    if dispatch_reconciled_command_id is not None:
        state.dispatch_reconciliation_required_commands.discard(
            dispatch_reconciled_command_id
        )

    recovery_commands = state.recovery_required_commands | {
        entry.command_id
        for entry in state.outbox_by_command_id.values()
        if entry.scope.to_position_key() == key
        and (
            entry.external_order_id in state.recovery_required_commands
            or "external:"
            + dependencies.order_watermark_key(key, entry.external_order_id)
            in state.recovery_required_commands
        )
    }
    for command_id in recovery_commands:
        pending = state.outbox_by_command_id.get(command_id)
        if pending is None or pending.scope.to_position_key() != key:
            continue
        if terminal_settlement_is_confirmed(
            pending,
            account_fills=settlement_facts,
            cumulative_quantity=state.order_cumulative_fills.get(
                dependencies.order_watermark_key(key, command_id), Decimal("0")
            ),
            reservations=tuple(
                state.coordinator.get_reservation(reservation_id)
                for reservation_id in state.command_reservations.get(command_id, ())
            ),
        ):
            state.recovery_required_commands.discard(command_id)
            if pending.external_order_id is not None:
                state.recovery_required_commands.discard(pending.external_order_id)
                watermark = dependencies.order_watermark_key(
                    key, pending.external_order_id
                )
                external_identity = f"external:{watermark}"
                state.recovery_required_commands.discard(external_identity)
                state.external_recovery_positions.pop(external_identity, None)

    state.seen_evidence_ids.add(identity)
    updated_view = book.get_view(now=evidence.observed_at)

    # An external exit has no command or reservation lifecycle. Resolve
    # its position-scoped quarantine only from complete real facts and a
    # subsequent matching account cut, never merely from an order receipt.
    facts = journal.read_cut()
    latest_snapshot = max(facts.snapshots, key=lambda s: s.observed_at, default=None)
    latest_trade_at = max((f.trade_at for f in facts.fills), default=None)
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
        for identity, position in tuple(state.external_recovery_positions.items()):
            if position == key:
                state.external_recovery_positions.pop(identity)
                state.recovery_required_commands.discard(identity)

    return Applied(
        evidence_id=evidence.evidence_id,
        updated_view_token=updated_view.projection_version,
        consumed_quantity=consumed_qty,
        released_quantity=released_qty,
        recovery_required=settlement_recovery_required,
        diagnostics=diagnostics,
    )
