"""Durable recovery for the in-memory execution aggregate.

Recovery mutates an :class:`ExecutionBook` only while it is being initialized;
keeping it here prevents the live command and evidence paths from carrying the
startup persistence detail.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

import structlog

from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.command_codec import (
    decode_active_commands,
    decode_order_watermark,
)
from crypto_momentum_lab.domain.execution.command_models import OutboxEntry
from crypto_momentum_lab.domain.execution.evidence_rules import (
    _scoped_evidence_identity,
)
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.position_recovery import (
    recover_durable_position,
)
from crypto_momentum_lab.domain.execution.position_repair import (
    validate_repaired_position,
)
from crypto_momentum_lab.domain.execution.reservation_registry import (
    ReservationRegistry,
)

if TYPE_CHECKING:
    pass


log = structlog.get_logger(__name__)


@dataclass(slots=True)
class ExecutionRecoveryState:
    """Mutable state owned by recovery, separate from the book façade."""

    books: dict[str, PositionBook]
    journals: dict[str, AccountJournal]
    stream_scopes: dict[str, AccountFactStreamScope]
    head_revisions: dict[str, int]
    head_expected_reservation_ids: dict[str, set[str]]
    journal_revisions: dict[str, int]
    last_sequences: dict[str, int]
    seen_evidence_ids: set[str]
    seen_trade_ids: set[str]
    order_cumulative_fills: dict[str, Decimal]
    order_cumulative_quotes: dict[str, Decimal]
    outbox_by_command_id: dict[str, OutboxEntry]
    command_reservations: dict[str, list[str]]
    recovery_required_commands: set[str]
    external_recovery_positions: dict[str, PositionKey]
    dispatch_reconciliation_required_commands: set[str]
    coordinator: ReservationRegistry

    def clear(self, *, clear_requests: Callable[[], None]) -> None:
        for values in (
            self.books,
            self.journals,
            self.stream_scopes,
            self.head_revisions,
            self.head_expected_reservation_ids,
            self.journal_revisions,
            self.last_sequences,
            self.seen_evidence_ids,
            self.seen_trade_ids,
            self.order_cumulative_fills,
            self.order_cumulative_quotes,
            self.outbox_by_command_id,
            self.command_reservations,
            self.recovery_required_commands,
            self.external_recovery_positions,
            self.dispatch_reconciliation_required_commands,
        ):
            values.clear()
        clear_requests()
        self.coordinator.clear_reservations()


@dataclass(frozen=True, slots=True)
class RecoveryDependencies:
    """Startup-only operations required to restore an execution state."""

    command_repository: object | None
    reservation_repository: object | None
    unit_of_work: object | None
    clear_requests: Callable[[], None]
    persist_outbox_state: Callable[[OutboxEntry], Awaitable[None]]
    mark_unknown: Callable[[str, str, datetime], Awaitable[object]]
    set_persistence_failed: Callable[[bool], None]
    watermark_key: Callable[[PositionKey, str], str]


async def restore_execution_state(
    state: ExecutionRecoveryState,
    dependencies: RecoveryDependencies,
    account_label: str | None = None,
    *,
    environment: str = "live",
    as_of: datetime | None = None,
) -> None:
    """Restore one book's durable facts, commands, and active reservations."""
    dependencies.set_persistence_failed(True)
    command_repository = dependencies.command_repository
    deferred_unknown_commands: list[str] = []
    unit_of_work = dependencies.unit_of_work
    if unit_of_work is not None:
        if not account_label:
            raise ValueError("durable restore requires an account_label")
        if command_repository is None:
            raise RuntimeError(
                "durable execution restore requires a command repository"
            )
        as_of = as_of or datetime.now(UTC)
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise ValueError("restore as_of must be timezone-aware")
        for values in (
            state.books,
            state.journals,
            state.stream_scopes,
            state.head_revisions,
            state.head_expected_reservation_ids,
            state.journal_revisions,
            state.last_sequences,
            state.seen_evidence_ids,
            state.seen_trade_ids,
            state.order_cumulative_fills,
            state.order_cumulative_quotes,
            state.outbox_by_command_id,
            state.command_reservations,
            state.recovery_required_commands,
            state.external_recovery_positions,
            state.dispatch_reconciliation_required_commands,
        ):
            values.clear()
        dependencies.clear_requests()
        state.coordinator.clear_reservations()
        await restore_durable_positions(
            state,
            unit_of_work=unit_of_work,
            account_label=account_label,
            environment=environment,
            as_of=as_of,
            watermark_key=dependencies.watermark_key,
        )

    if command_repository is not None:
        try:
            active_cmds = await command_repository.load_active_execution_commands(
                account_label=account_label
            )
            parsed_commands = decode_active_commands(
                active_cmds, account_label=account_label, restored_at=datetime.now(UTC)
            )
            for recovered in parsed_commands:
                entry = recovered.entry
                command_id = entry.command_id
                state.outbox_by_command_id[command_id] = entry
                state.command_reservations[command_id] = list(recovered.reservation_ids)
                if recovered.requires_reconciliation:
                    state.dispatch_reconciliation_required_commands.add(command_id)
                if recovered.needs_unknown_write:
                    if unit_of_work is not None:
                        deferred_unknown_commands.append(command_id)
                    else:
                        await dependencies.persist_outbox_state(entry)
        except Exception as err:
            log.error("restore_active_commands_failed", error=str(err))
            raise RuntimeError("Failed to restore active execution commands") from err

        if unit_of_work is None:
            try:
                state.seen_evidence_ids.update(
                    await command_repository.load_seen_event_ids()
                )
            except Exception as err:
                raise RuntimeError(
                    "Failed to restore execution event identities"
                ) from err
            try:
                state.seen_trade_ids.update(
                    await command_repository.load_seen_fill_trade_ids()
                )
            except Exception as err:
                raise RuntimeError("Failed to restore fill identities") from err
            try:
                watermark_rows = (
                    await command_repository.load_execution_order_watermarks(
                        account_label=account_label
                    )
                )
                for row in watermark_rows:
                    restored = decode_order_watermark(row, account_label=account_label)
                    if restored is None:
                        continue
                    watermark_key = dependencies.watermark_key(
                        restored.scope.to_position_key(), restored.order_id
                    )
                    state.order_cumulative_fills[watermark_key] = max(
                        state.order_cumulative_fills.get(watermark_key, Decimal("0")),
                        restored.quantity,
                    )
                    state.order_cumulative_quotes[watermark_key] = max(
                        state.order_cumulative_quotes.get(watermark_key, Decimal("0")),
                        restored.quote,
                    )
            except Exception as err:
                log.error("restore_watermarks_failed", error=str(err))
                raise RuntimeError(
                    f"Failed to restore cumulative fill watermarks: {err}"
                ) from err

    if dependencies.reservation_repository is not None:
        try:
            active_reservations = (
                await dependencies.reservation_repository.load_active_reservations()
            )
            for reservation in active_reservations:
                if (
                    account_label is None
                    or reservation.position_key.account_label == account_label
                ):
                    state.coordinator.register_reservation(reservation)
            for command_id, reservation_ids in state.command_reservations.items():
                entry = state.outbox_by_command_id[command_id]
                for reservation_id in reservation_ids:
                    if state.coordinator.get_reservation(reservation_id) is not None:
                        continue
                    reservation = (
                        await dependencies.reservation_repository.load_reservation(
                            reservation_id
                        )
                    )
                    if reservation is None:
                        continue
                    if (
                        reservation.command_id != command_id
                        or reservation.position_key != entry.scope.to_position_key()
                    ):
                        raise ValueError("restored command reservation scope mismatch")
                    state.coordinator.register_reservation(reservation)
        except Exception as err:
            raise RuntimeError("Failed to restore command reservations") from err

    diverged_canons: set[str] = set()
    for canon, expected_ids in state.head_expected_reservation_ids.items():
        journal = state.journals.get(canon)
        if journal is None:
            log.warning("restored_reservation_head_has_no_journal", canonical_id=canon)
            continue
        actual_ids = {
            reservation.reservation_id
            for reservation in state.coordinator.get_active_reservations(
                journal.position_key
            )
        }
        if actual_ids != expected_ids:
            log.warning(
                "restored_active_reservations_diverged",
                canonical_id=canon,
                expected_ids=sorted(expected_ids),
                actual_ids=sorted(actual_ids),
            )
            diverged_canons.add(canon)
            identity = f"reservation_divergence:{canon}"
            state.external_recovery_positions[identity] = journal.position_key
            state.recovery_required_commands.add(identity)
    for canon in tuple(state.head_expected_reservation_ids):
        if canon not in diverged_canons:
            state.head_expected_reservation_ids.pop(canon, None)
    dependencies.set_persistence_failed(False)
    if unit_of_work is not None:
        for command_id in deferred_unknown_commands:
            await dependencies.mark_unknown(
                command_id,
                "restored dispatch requires reconciliation",
                datetime.now(UTC),
            )


async def restore_durable_positions(
    state: ExecutionRecoveryState,
    *,
    unit_of_work: object,
    account_label: str,
    environment: str,
    as_of: datetime,
    watermark_key: Callable[[PositionKey, str], str],
) -> None:
    """Load durable position heads without command or reservation recovery."""
    states = await unit_of_work.load_positions(
        environment=environment, account_label=account_label, as_of=as_of
    )
    benign_facts_migration_count = 0
    for durable_state in states:
        scope = durable_state.scope
        key = PositionKey(
            environment=scope.environment,
            account_label=scope.account_label,
            symbol=scope.symbol,
            position_side=scope.position_side,
        )
        canon = key.canonical_id
        recovered = recover_durable_position(durable_state)
        for event, values in recovered.diagnostics:
            if (
                event == "execution_head_facts_migrated"
                and not values.get("has_active_reservations")
            ):
                benign_facts_migration_count += 1
                continue
            if event == "execution_head_view_migrated" and not values.get(
                "has_active_reservations"
            ):
                log.info(event, **values)
            else:
                log.warning(event, **values)
        state.head_revisions[canon] = recovered.head_revision
        if durable_state.head is not None:
            state.recovery_required_commands.update(
                durable_state.head.state_payload["recovery_command_ids"]
            )
            for identity in durable_state.head.state_payload["external_recovery_ids"]:
                state.external_recovery_positions[identity] = key
                state.recovery_required_commands.add(identity)
            state.head_expected_reservation_ids[canon] = set(recovered.reservation_ids)
            if recovered.last_sequence is not None:
                state.last_sequences[canon] = recovered.last_sequence
        state.journals[canon] = recovered.journal
        state.books[canon] = recovered.book
        state.stream_scopes[canon] = scope
        state.journal_revisions[canon] = durable_state.cut.revision
        state.seen_trade_ids.update(durable_state.trade_ids)
        state.seen_evidence_ids.update(
            _scoped_evidence_identity(scope, evidence_id)
            for evidence_id in durable_state.evidence_ids
        )
        for watermark in durable_state.watermarks:
            key_for_order = watermark_key(key, watermark.order_id)
            state.order_cumulative_fills[key_for_order] = watermark.cumulative_quantity
            state.order_cumulative_quotes[key_for_order] = watermark.cumulative_quote
    if benign_facts_migration_count:
        log.info(
            "execution_head_facts_migrations_recovered",
            account_label=account_label,
            migration_count=benign_facts_migration_count,
        )


@dataclass(frozen=True, slots=True)
class ReloadDependencies:
    load_position: Callable[..., Awaitable[object | None]] | None
    active_reservations: Callable[[PositionKey], tuple[object, ...]]
    order_watermark_key: Callable[[PositionKey, str], str]
    advance_context_revision: Callable[[], None]


async def reload_execution_position(
    state: ExecutionRecoveryState,
    dependencies: ReloadDependencies,
    key: PositionKey,
    *,
    as_of: datetime | None = None,
    expected_scope: AccountFactStreamScope | None = None,
    expected_quantity: Decimal | None = None,
) -> object | None:
    if dependencies.load_position is None:
        return None
    as_of = as_of or datetime.now(UTC)
    canon = key.canonical_id
    target_state = await dependencies.load_position(
        key,
        as_of=as_of,
    )
    if target_state is None:
        return None
    journal = AccountJournal.from_durable_cut(target_state.cut)
    book = PositionBook(journal)
    if expected_scope is not None:
        validate_repaired_position(
            state=target_state,
            scope=expected_scope,
            expected_quantity=expected_quantity,
            reservation_ids={
                r.reservation_id for r in dependencies.active_reservations(key)
            },
        )
    head = target_state.head
    scope = target_state.cut.scope
    if head is not None:
        payload = head.state_payload
        active_res = payload.get("active_reservation_ids")
        if not isinstance(active_res, list) or any(
            not isinstance(identity, str) or not identity for identity in active_res
        ):
            raise RuntimeError("durable execution head has malformed reservations")
        if "last_sequence" not in payload:
            raise RuntimeError("durable execution head is missing last_sequence")
        last_sequence = payload["last_sequence"]
        if last_sequence is not None and (
            type(last_sequence) is not int or last_sequence < 0
        ):
            raise RuntimeError("durable execution head has malformed last_sequence")
        view = book.get_view()
        book.use_durable_projection_version(
            head.projection_version,
            event_cut=view.event_cut,
        )
        state.head_revisions[canon] = head.revision
        expected_res_set = set(active_res)
        state.head_expected_reservation_ids[canon] = expected_res_set
        divergence_identity = f"reservation_divergence:{canon}"
        actual_res_ids = {
            r.reservation_id for r in dependencies.active_reservations(key)
        }
        if (
            actual_res_ids == expected_res_set
            and divergence_identity in state.recovery_required_commands
        ):
            state.recovery_required_commands.discard(divergence_identity)
            state.external_recovery_positions.pop(divergence_identity, None)
            state.head_expected_reservation_ids.pop(canon, None)
        if last_sequence is not None:
            state.last_sequences[canon] = last_sequence
        else:
            state.last_sequences.pop(canon, None)
    else:
        state.head_revisions[canon] = 0

    state.journals[canon] = journal
    state.books[canon] = book
    dependencies.advance_context_revision()
    state.stream_scopes[canon] = scope
    state.journal_revisions[canon] = target_state.cut.revision
    state.seen_trade_ids.update(target_state.trade_ids)
    state.seen_evidence_ids.update(
        _scoped_evidence_identity(scope, identity)
        for identity in target_state.evidence_ids
    )
    for watermark in target_state.watermarks:
        watermark_key = dependencies.order_watermark_key(key, watermark.order_id)
        state.order_cumulative_fills[watermark_key] = watermark.cumulative_quantity
        state.order_cumulative_quotes[watermark_key] = watermark.cumulative_quote
    return book.get_view()
