"""Command acceptance, independent from the execution-book transaction facade."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.execution.command_models import OutboxEntry
from crypto_momentum_lab.domain.execution.execution_action_models import (
    Accepted,
    AlreadyAccepted,
    Blocked,
    CommandConflict,
    ExecutionActResult,
    ExecutionReceipt,
    ExecutionRecoveryPending,
    ExecutionRequest,
    PositionNotReady,
    StaleView,
)
from crypto_momentum_lab.domain.execution.execution_action_state import (
    ExecutionActionState,
    register_prepared_outbox,
)
from crypto_momentum_lab.domain.execution.order_state import ExitAllocation
from crypto_momentum_lab.domain.execution.ports import ReservationRepository
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.reservation_registry import (
    ExecutionReadinessError,
    ReservationConflictError,
    ReservationRegistry,
    VersionConflictError,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitAllocationPlan,
    PositionReservation,
    TradeCommand,
    TradeCommandType,
    plan_exit_allocations,
)
from crypto_momentum_lab.domain.trading import OrderType as EntryType


@dataclass(frozen=True, slots=True)
class ExecutionActionDependencies:
    """Services owned by the composition root, supplied to command acceptance."""

    book_for_key: Callable[[object], PositionBook]
    coordinator: ReservationRegistry
    reservation_repository: ReservationRepository | None
    persistence_failed: Callable[[], bool]
    mark_persistence_failed: Callable[[], None]
    persist_outbox: Callable[[OutboxEntry], Awaitable[None]]
    advance_context_revision: Callable[[], None]


async def accept_execution_command(
    state: ExecutionActionState,
    dependencies: ExecutionActionDependencies,
    request: ExecutionRequest,
    *,
    stream_scopes_present: bool,
) -> ExecutionActResult:
    """Validate, reserve and durably prepare exactly one trade command."""
    if dependencies.persistence_failed() and not stream_scopes_present:
        return Blocked(reason="Initial durable restore is required before trading")
    key = request.scope.to_position_key()
    if request.request_id in (
        state.recovery_required_commands
        | state.dispatch_reconciliation_required_commands
    ):
        return ExecutionRecoveryPending(
            reason="This execution command requires reconciliation",
            diagnostics=(request.request_id,),
        )
    view = dependencies.book_for_key(key).get_view()
    effective_view_token = view.projection_version
    existing_request = state.requests_by_id.get(request.request_id)
    if existing_request is not None:
        if _same_execution_request_payload(existing_request, request):
            return AlreadyAccepted(state.receipts_by_id[request.request_id])  # type: ignore[arg-type]
        return CommandConflict(
            request_id=request.request_id,
            reason="Conflicting payload for identical request_id",
        )
    restored_entry = state.outbox_by_command_id.get(request.request_id)
    if restored_entry is not None:
        return Blocked(
            reason="Execution command already exists and requires reconciliation",
            diagnostics=(restored_entry.state.value,),
        )
    if (
        request.expected_view_token not in ("*", "pv_initial")
        and request.expected_view_token != view.projection_version
    ):
        return StaleView(
            expected_token=request.expected_view_token,
            current_token=view.projection_version,
            reason=(
                f"Expected view token {request.expected_view_token} does not match "
                f"current projection {view.projection_version}"
            ),
        )

    reservations: tuple[PositionReservation, ...] = ()
    order_type = EntryType(request.order_type.lower())
    if request.action == TradeCommandType.EXIT:
        if request.target_batch_ids and request.batch_quantities is not None:
            allocation_plan = ExitAllocationPlan(
                position_key=key,
                allocations=tuple(
                    ExitAllocation(
                        batch_id=batch_id,
                        allocated_quantity=request.batch_quantities[batch_id],
                    )
                    for batch_id in request.target_batch_ids
                ),
                total_allocated_quantity=request.requested_quantity,
                policy=request.exit_policy_mode,
                reason=f"exit_{request.decision_ref}",
                projection_version=effective_view_token,
            )
        else:
            allocation_plan = plan_exit_allocations(
                view,
                target_batch_ids=request.target_batch_ids or None,
                requested_quantity=request.requested_quantity,
                policy=request.exit_policy_mode,
                reason=f"exit_{request.decision_ref}",
            )
        if allocation_plan.total_allocated_quantity <= 0:
            return Blocked(
                reason="Insufficient active batch capacity for requested exit quantity",
                diagnostics=(
                    f"Requested: {request.requested_quantity}, "
                    f"Total active: {view.total_quantity}",
                ),
            )
        command = TradeCommand(
            command_id=request.request_id,
            position_key=key,
            command_type=TradeCommandType.EXIT,
            side=request.side,
            order_type=order_type,
            requested_quantity=allocation_plan.total_allocated_quantity,
            limit_price=request.limit_price,
            reduce_only=True,
            expected_projection_version=effective_view_token,
            allocation_plan=allocation_plan,
            created_at=request.created_at,
        )
        if view.batches:
            try:
                reservations = dependencies.coordinator.reserve_exit(command, view)
            except (
                ReservationConflictError,
                VersionConflictError,
                ExecutionReadinessError,
            ) as err:
                result_type = (
                    PositionNotReady
                    if isinstance(err, ExecutionReadinessError)
                    else Blocked
                )
                return result_type(reason=str(err), diagnostics=(type(err).__name__,))
        else:
            reservations = tuple(
                PositionReservation(
                    reservation_id=(
                        f"res_{command.command_id}"
                        if len(allocation_plan.allocations) == 1
                        else f"res_{command.command_id}_{index}"
                    ),
                    command_id=command.command_id,
                    position_key=key,
                    batch_id=allocation.batch_id,
                    reserved_quantity=allocation.allocated_quantity,
                    created_at=command.created_at,
                )
                for index, allocation in enumerate(allocation_plan.allocations)
            )
            for reservation in reservations:
                dependencies.coordinator.register_reservation(reservation)
        repository_result = await _save_new_reservations(
            dependencies,
            state,
            command,
            reservations,
            {
                item.batch_id: item.allocated_quantity
                for item in allocation_plan.allocations
            },
        )
        if repository_result is not None:
            return repository_result
    else:
        command = TradeCommand(
            command_id=request.request_id,
            position_key=key,
            command_type=request.action,
            side=request.side,
            order_type=order_type,
            requested_quantity=request.requested_quantity,
            limit_price=request.limit_price,
            reduce_only=request.reduce_only,
            expected_projection_version=effective_view_token,
            created_at=request.created_at,
        )

    committed_at = datetime.now(UTC)
    outbox = register_prepared_outbox(
        state,
        request_id=request.request_id,
        scope=request.scope,
        command=command,
        created_at=committed_at,
        reservation_ids=tuple(item.reservation_id for item in reservations),
    )
    try:
        await dependencies.persist_outbox(outbox)
    except Exception as error:
        rollback_errors = await _rollback_outbox_acceptance(
            dependencies, state, command, reservations
        )
        diagnostics = [f"outbox persistence failed: {error}"]
        if rollback_errors:
            diagnostics.append(
                "reservation rollback failed: " + "; ".join(rollback_errors)
            )
        return Blocked(
            reason="Execution command was not durably accepted",
            diagnostics=tuple(diagnostics),
        )
    receipt = ExecutionReceipt(
        request_id=request.request_id,
        scope=request.scope,
        command=command,
        reservations=reservations,
        committed_at=committed_at,
        view_token=effective_view_token,
        outbox_entry=outbox,
    )
    state.requests_by_id[request.request_id] = request
    state.receipts_by_id[request.request_id] = receipt
    dependencies.advance_context_revision()
    return Accepted(receipt)


def _same_execution_request_payload(
    existing: object, retry: ExecutionRequest
) -> bool:
    """Treat a retry timestamp as metadata; compare every command field."""
    return isinstance(existing, ExecutionRequest) and replace(
        existing, created_at=retry.created_at
    ) == retry


async def _save_new_reservations(
    dependencies: ExecutionActionDependencies,
    state: ExecutionActionState,
    command: TradeCommand,
    reservations: tuple[PositionReservation, ...],
    batch_quantities: dict[str, Decimal],
) -> CommandConflict | Blocked | None:
    repository = dependencies.reservation_repository
    if repository is None:
        return None
    to_save: list[PositionReservation] = []
    for reservation in reservations:
        try:
            existing = await repository.load_reservation(reservation.reservation_id)
        except ReservationConflictError:
            raise
        except Exception as error:
            dependencies.mark_persistence_failed()
            state.recovery_required_commands.add(command.command_id)
            return Blocked(
                reason="Reservation identity lookup failed; restore is required",
                diagnostics=(str(error),),
            )
        if existing is None:
            to_save.append(reservation)
        elif (
            existing.batch_id != reservation.batch_id
            or existing.reserved_quantity != reservation.reserved_quantity
            or existing.position_key != reservation.position_key
        ):
            return CommandConflict(
                request_id=command.command_id,
                reason=(
                    f"Reservation {reservation.reservation_id} already exists with "
                    "different parameters"
                ),
            )
    if to_save:
        try:
            await repository.save_reservations(
                tuple(to_save), batch_quantities=batch_quantities
            )
        except Exception as error:
            return CommandConflict(
                request_id=command.command_id,
                reason=f"Reservation already exists or save conflict: {error}",
            )
    return None


async def _rollback_outbox_acceptance(
    dependencies: ExecutionActionDependencies,
    state: ExecutionActionState,
    command: TradeCommand,
    reservations: tuple[PositionReservation, ...],
) -> list[str]:
    state.outbox_by_command_id.pop(command.command_id, None)
    state.command_reservations.pop(command.command_id, None)
    errors: list[str] = []
    for reservation in reservations:
        current = dependencies.coordinator.get_reservation(reservation.reservation_id)
        if current is None or current.active_quantity <= Decimal("0"):
            continue
        try:
            released = dependencies.coordinator.release_reservation(
                current.reservation_id, current.active_quantity
            )
            if dependencies.reservation_repository is not None:
                await dependencies.reservation_repository.update_reservation(
                    released, release_reason="outbox_acceptance_failed"
                )
        except Exception as error:
            errors.append(str(error))
    return errors
