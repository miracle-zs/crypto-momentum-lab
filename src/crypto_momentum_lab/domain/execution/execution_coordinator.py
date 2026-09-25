"""ExecutionCoordinator domain service for transactional lot reservation and exit coordination.

Obays RFC 2026-09-25:
1. Version-pinned lot reservation (CAS check on projection version);
2. Prevents concurrent exits from double-dipping on the same batch lot;
3. Tracks reservation consumption upon fills and releases upon order completion/cancellation;
4. Enforces strict allocation invariants: submitters cannot expand quantity or swap batches.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionKey,
    PositionView,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    PositionReservation,
    TradeCommand,
    TradeCommandType,
)


class ReservationConflictError(Exception):
    """Raised when an exit command cannot reserve requested batch quantities."""


class VersionConflictError(Exception):
    """Raised when command expected_projection_version does not match PositionView version."""


class ExecutionReadinessError(Exception):
    """Raised when PositionView is not ready for trade (gap, incomplete coverage, conflict)."""


class ExecutionCoordinator:
    """Coordinates transactional lot reservations and prevents concurrent exit over-allocation."""

    def __init__(self) -> None:
        self._reservations_by_id: dict[str, PositionReservation] = {}

    def get_active_reservations(self, key: PositionKey) -> tuple[PositionReservation, ...]:
        """Returns all currently active reservations for a given PositionKey."""
        return tuple(
            r for r in self._reservations_by_id.values()
            if r.position_key.canonical_id == key.canonical_id and r.active_quantity > Decimal("0")
        )

    def get_available_batch_quantity(self, view: PositionView, batch_id: str) -> Decimal:
        """Returns the unreserved, available quantity for a specific batch."""
        matching_batch = next((b for b in view.batches if b.batch_id == batch_id), None)
        if matching_batch is None:
            return Decimal("0")

        reserved = sum(
            (r.active_quantity for r in self.get_active_reservations(view.key) if r.batch_id == batch_id),
            start=Decimal("0"),
        )
        return max(Decimal("0"), matching_batch.quantity - reserved)

    def reserve_exit(
        self,
        command: TradeCommand,
        view: PositionView,
    ) -> tuple[PositionReservation, ...]:
        """Transactionally reserves batch quantities for an exit trade command.

        Raises:
            ExecutionReadinessError: If PositionView is not ready for trading;
            VersionConflictError: If command expected_projection_version mismatches view;
            ReservationConflictError: If any target batch has insufficient unreserved quantity.
        """
        if command.position_key.canonical_id != view.key.canonical_id:
            raise ValueError(
                f"Command position key {command.position_key.canonical_id} does not "
                f"match view position key {view.key.canonical_id}"
            )

        if not view.is_ready_for_trade:
            raise ExecutionReadinessError(
                f"PositionView for {view.key.canonical_id} is not ready for trade "
                f"(health={view.health_status.value}, gap={view.reconciliation_gap}, "
                f"unallocated={view.unallocated_quantity})"
            )

        if (
            command.expected_projection_version is not None
            and command.expected_projection_version != view.projection_version
        ):
            raise VersionConflictError(
                f"CAS version mismatch: command expected version "
                f"{command.expected_projection_version}, but view is {view.projection_version}"
            )

        if command.command_type != TradeCommandType.EXIT or command.allocation_plan is None:
            return ()

        # 1. Check unreserved quantity across all target batches
        batches_by_id = {b.batch_id: b for b in view.batches}
        active_reservations = self.get_active_reservations(view.key)

        for allocation in command.allocation_plan.allocations:
            batch = batches_by_id.get(allocation.batch_id)
            if batch is None:
                raise ReservationConflictError(
                    f"Target batch {allocation.batch_id} not found in view batches"
                )

            currently_reserved = sum(
                (r.active_quantity for r in active_reservations if r.batch_id == allocation.batch_id),
                start=Decimal("0"),
            )
            available = batch.quantity - currently_reserved
            if available < allocation.allocated_quantity:
                raise ReservationConflictError(
                    f"Batch {allocation.batch_id} insufficient available quantity: "
                    f"requested {allocation.allocated_quantity}, available {available} "
                    f"(batch_qty={batch.quantity}, reserved={currently_reserved})"
                )

        # 2. Commit reservations
        created_reservations: list[PositionReservation] = []
        for allocation in command.allocation_plan.allocations:
            res_id = f"res_{allocation.batch_id}_{uuid4().hex[:8]}"
            res = PositionReservation(
                reservation_id=res_id,
                command_id=command.command_id,
                position_key=view.key,
                batch_id=allocation.batch_id,
                reserved_quantity=allocation.allocated_quantity,
                created_at=datetime.now(UTC),
            )
            self._reservations_by_id[res_id] = res
            created_reservations.append(res)

        return tuple(created_reservations)

    def reconcile_fill(
        self,
        reservation_id: str,
        filled_quantity: Decimal,
    ) -> PositionReservation:
        """Consumes reserved quantity upon receiving fill confirmation."""
        res = self._reservations_by_id.get(reservation_id)
        if res is None:
            raise KeyError(f"Reservation {reservation_id} not found")

        new_consumed = res.consumed_quantity + filled_quantity
        if new_consumed + res.released_quantity > res.reserved_quantity:
            raise ValueError(
                f"Consumed ({new_consumed}) + released ({res.released_quantity}) "
                f"exceeds reserved ({res.reserved_quantity})"
            )

        updated = PositionReservation(
            reservation_id=res.reservation_id,
            command_id=res.command_id,
            position_key=res.position_key,
            batch_id=res.batch_id,
            reserved_quantity=res.reserved_quantity,
            consumed_quantity=new_consumed,
            released_quantity=res.released_quantity,
            created_at=res.created_at,
        )
        self._reservations_by_id[reservation_id] = updated
        return updated

    def release_reservation(
        self,
        reservation_id: str,
        quantity: Decimal | None = None,
    ) -> PositionReservation:
        """Releases active reserved quantity (e.g. upon order cancellation or rejection)."""
        res = self._reservations_by_id.get(reservation_id)
        if res is None:
            raise KeyError(f"Reservation {reservation_id} not found")

        to_release = res.active_quantity if quantity is None else quantity
        if to_release > res.active_quantity:
            raise ValueError(
                f"Release quantity {to_release} exceeds active reserved quantity {res.active_quantity}"
            )

        updated = PositionReservation(
            reservation_id=res.reservation_id,
            command_id=res.command_id,
            position_key=res.position_key,
            batch_id=res.batch_id,
            reserved_quantity=res.reserved_quantity,
            consumed_quantity=res.consumed_quantity,
            released_quantity=res.released_quantity + to_release,
            created_at=res.created_at,
        )
        self._reservations_by_id[reservation_id] = updated
        return updated
