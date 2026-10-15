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

from crypto_momentum_lab.domain.execution.command_codec import (
    decode_active_commands,
)
from crypto_momentum_lab.domain.execution.command_models import OutboxEntry
from crypto_momentum_lab.domain.execution.execution_head import (
    decode_execution_head_state,
)
from crypto_momentum_lab.domain.execution.ports import DecisionCommitConflict
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

if TYPE_CHECKING:
    from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook


log = structlog.get_logger(__name__)


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
    book: ExecutionBook,
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
    if unit_of_work is None:
        raise RuntimeError(
            "durable execution restore requires a unit of work; an in-memory "
            "book has no durable facts to restore"
        )
    if not account_label:
        raise ValueError("durable restore requires an account_label")
    if command_repository is None:
        raise RuntimeError("durable execution restore requires a command repository")
    as_of = as_of or datetime.now(UTC)
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("restore as_of must be timezone-aware")
    book.clear(clear_requests=dependencies.clear_requests)
    await restore_durable_positions(
        book,
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
                book._outbox_by_command_id[command_id] = entry
                book._command_reservations[command_id] = list(recovered.reservation_ids)
                if recovered.requires_reconciliation:
                    book._dispatch_reconciliation_required_commands.add(command_id)
                if recovered.needs_unknown_write:
                    deferred_unknown_commands.append(command_id)
                pos_key = entry.scope.to_position_key()
                canon = pos_key.canonical_id
                if canon not in book._books:
                    acc_scope = next(
                        (
                            s
                            for s in book._stream_scopes.values()
                            if s.account_label == pos_key.account_label
                        ),
                        None,
                    )
                    stream_scope = (
                        AccountFactStreamScope.for_position_key(
                            pos_key,
                            stream_id=acc_scope.stream_id,
                            stream_epoch=acc_scope.stream_epoch,
                        )
                        if acc_scope is not None
                        else None
                    )
                    book._books[canon] = PositionBook(pos_key, stream_scope=stream_scope)
                    book._head_revisions[canon] = 0
                    if stream_scope is not None:
                        book._stream_scopes[canon] = stream_scope
        except Exception as err:
            log.error("restore_active_commands_failed", error=str(err))
            raise RuntimeError("Failed to restore active execution commands") from err

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
                    book._coordinator.register_reservation(reservation)
                    pos_key = reservation.position_key
                    canon = pos_key.canonical_id
                    if canon not in book._books:
                        acc_scope = next(
                            (
                                s
                                for s in book._stream_scopes.values()
                                if s.account_label == pos_key.account_label
                            ),
                            None,
                        )
                        stream_scope = (
                            AccountFactStreamScope.for_position_key(
                                pos_key,
                                stream_id=acc_scope.stream_id,
                                stream_epoch=acc_scope.stream_epoch,
                            )
                            if acc_scope is not None
                            else None
                        )
                        book._books[canon] = PositionBook(pos_key, stream_scope=stream_scope)
                        book._head_revisions[canon] = 0
                        if stream_scope is not None:
                            book._stream_scopes[canon] = stream_scope
            for command_id, reservation_ids in book._command_reservations.items():
                entry = book._outbox_by_command_id[command_id]
                for reservation_id in reservation_ids:
                    if book._coordinator.get_reservation(reservation_id) is not None:
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
                    book._coordinator.register_reservation(reservation)
        except Exception as err:
            raise RuntimeError("Failed to restore command reservations") from err

    diverged_canons: set[str] = set()
    for canon, expected_ids in book._head_expected_reservation_ids.items():
        position_book = book._books.get(canon)
        if position_book is None:
            log.warning("restored_reservation_head_has_no_position", canonical_id=canon)
            continue
        actual_ids = {
            reservation.reservation_id
            for reservation in book._coordinator.get_active_reservations(
                position_book.position_key
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
            book._external_recovery_positions[identity] = position_book.position_key
            book._recovery_required_commands.add(identity)
    for canon in tuple(book._head_expected_reservation_ids):
        if canon not in diverged_canons:
            book._head_expected_reservation_ids.pop(canon, None)
    dependencies.set_persistence_failed(False)
    if unit_of_work is not None:
        for command_id in deferred_unknown_commands:
            await dependencies.mark_unknown(
                command_id,
                "restored dispatch requires reconciliation",
                datetime.now(UTC),
            )


async def restore_durable_positions(
    book: ExecutionBook,
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
            if event in (
                "execution_head_projection_migrated",
                "execution_head_view_migrated",
            ):
                raise DecisionCommitConflict(
                    f"execution head digest mismatch for {canon}: {event} {values}"
                )
            if event == "execution_head_facts_migrated" and not values.get(
                "has_active_reservations"
            ):
                benign_facts_migration_count += 1
                continue
            log.warning(event, **values)
        book._head_revisions[canon] = recovered.head_revision
        if durable_state.head is not None:
            recovery_ids = (
                durable_state.head.state_payload.get("recovery_command_ids") or ()
            )
            book._recovery_required_commands.update(recovery_ids)
            for identity in (
                durable_state.head.state_payload.get("external_recovery_ids") or ()
            ):
                book._external_recovery_positions[identity] = key
                book._recovery_required_commands.add(identity)
            book._head_expected_reservation_ids[canon] = set(recovered.reservation_ids)
            eff_seq = recovered.last_sequence
            if eff_seq is not None:
                book._last_sequences[canon] = eff_seq
        book._books[canon] = recovered.book
        book._stream_scopes[canon] = scope
        for watermark in durable_state.watermarks:
            key_for_order = watermark_key(key, watermark.order_id)
            book._order_cumulative_fills[key_for_order] = watermark.cumulative_quantity
            book._order_cumulative_quotes[key_for_order] = watermark.cumulative_quote
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


def apply_loaded_execution_position(
    execution_book: ExecutionBook,
    dependencies: ReloadDependencies,
    key: PositionKey,
    target_state: object | None,
    *,
    expected_scope: AccountFactStreamScope | None = None,
    expected_quantity: Decimal | None = None,
) -> object | None:
    if target_state is None:
        return None
    canon = key.canonical_id
    current_head_rev = execution_book._head_revisions.get(canon, 0)
    target_head_rev = target_state.head.revision if target_state.head is not None else 0
    if target_head_rev < current_head_rev:
        log.warning(
            "ignoring_stale_loaded_position_regression",
            canonical_id=canon,
            current_revision=current_head_rev,
            stale_loaded_revision=target_head_rev,
        )
        existing = execution_book._books.get(canon)
        return existing.get_view() if existing is not None else None
    head = target_state.head
    book = (
        decode_execution_head_state(
            head.state_payload, key=key, scope=target_state.cut.scope
        )
        if head is not None
        else PositionBook.from_facts(target_state.cut.facts)
    )
    if expected_scope is not None:
        validate_repaired_position(
            state=target_state,
            scope=expected_scope,
            expected_quantity=expected_quantity,
            reservation_ids={
                r.reservation_id for r in dependencies.active_reservations(key)
            },
        )
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
        execution_book._head_revisions[canon] = head.revision
        expected_res_set = set(active_res)
        execution_book._head_expected_reservation_ids[canon] = expected_res_set
        divergence_identity = f"reservation_divergence:{canon}"
        actual_res_ids = {
            r.reservation_id for r in dependencies.active_reservations(key)
        }
        if (
            actual_res_ids == expected_res_set
            and divergence_identity in execution_book._recovery_required_commands
        ):
            execution_book._recovery_required_commands.discard(divergence_identity)
            execution_book._external_recovery_positions.pop(divergence_identity, None)
            execution_book._head_expected_reservation_ids.pop(canon, None)
        eff_seq = last_sequence
        if eff_seq is not None:
            execution_book._last_sequences[canon] = eff_seq
        else:
            execution_book._last_sequences.pop(canon, None)
    else:
        execution_book._head_revisions[canon] = 0

    execution_book._books[canon] = book
    dependencies.advance_context_revision()
    execution_book._stream_scopes[canon] = scope
    for watermark in target_state.watermarks:
        watermark_key = dependencies.order_watermark_key(key, watermark.order_id)
        execution_book._order_cumulative_fills[watermark_key] = (
            watermark.cumulative_quantity
        )
        execution_book._order_cumulative_quotes[watermark_key] = (
            watermark.cumulative_quote
        )
    return book.get_view()


async def reload_execution_position(
    execution_book: ExecutionBook,
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
    target_state = await dependencies.load_position(
        key,
        as_of=as_of,
    )
    return apply_loaded_execution_position(
        execution_book,
        dependencies,
        key,
        target_state,
        expected_scope=expected_scope,
        expected_quantity=expected_quantity,
    )
