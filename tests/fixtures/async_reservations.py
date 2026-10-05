"""Awaited in-memory reservation repository for current execution-port tests."""

from decimal import Decimal

from crypto_momentum_lab.domain.execution.reservation_registry import (
    ReservationConflictError,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.execution.trade_command import PositionReservation


class InMemoryPositionReservationRepository:
    """In-memory reference implementation of PositionReservationRepository."""

    def __init__(self) -> None:
        self._reservations: dict[str, PositionReservation] = {}

    async def save_reservation(
        self,
        reservation: PositionReservation,
        batch_quantity: Decimal | None = None,
    ) -> None:
        await self.save_reservations(
            (reservation,),
            batch_quantities=(
                {reservation.batch_id: batch_quantity}
                if batch_quantity is not None
                else None
            ),
        )

    async def save_reservations(
        self,
        reservations: tuple[PositionReservation, ...],
        batch_quantities: dict[str, Decimal] | None = None,
    ) -> None:
        batch_quantities = batch_quantities or {}
        pending: dict[str, Decimal] = {}
        to_commit: list[PositionReservation] = []
        for reservation in reservations:
            existing = self._reservations.get(reservation.reservation_id)
            if existing is not None:
                if existing.active_quantity <= Decimal("0"):
                    raise ReservationConflictError(
                        f"reservation {reservation.reservation_id} already "
                        "exists in a terminal state"
                    )
                if (
                    existing.position_key.canonical_id
                    != reservation.position_key.canonical_id
                    or existing.command_id != reservation.command_id
                    or existing.batch_id != reservation.batch_id
                    or existing.reserved_quantity != reservation.reserved_quantity
                ):
                    raise ReservationConflictError(
                        f"reservation {reservation.reservation_id} already "
                        f"exists with batch {existing.batch_id} qty "
                        f"{existing.reserved_quantity}"
                    )
                continue
            limit = batch_quantities.get(reservation.batch_id)
            if limit is not None:
                already = pending.get(reservation.batch_id, Decimal("0"))
                for other in self._reservations.values():
                    if (
                        other.batch_id == reservation.batch_id
                        and other.active_quantity > Decimal("0")
                    ):
                        already += other.active_quantity
                if already + reservation.reserved_quantity > limit:
                    raise ReservationConflictError(
                        f"batch {reservation.batch_id} over-reserved"
                    )
                pending[reservation.batch_id] = (
                    pending.get(reservation.batch_id, Decimal("0"))
                    + reservation.reserved_quantity
                )
            to_commit.append(reservation)

        for res in to_commit:
            self._reservations[res.reservation_id] = res

    async def update_reservation(
        self, reservation: PositionReservation, release_reason: str | None = None
    ) -> None:
        self._reservations[reservation.reservation_id] = reservation

    async def load_active_reservations(
        self, key: PositionKey | None = None
    ) -> tuple[PositionReservation, ...]:
        active = [
            r
            for r in self._reservations.values()
            if r.active_quantity > Decimal("0")
            and (key is None or r.position_key.canonical_id == key.canonical_id)
        ]
        active.sort(key=lambda r: (r.created_at, r.reservation_id))
        return tuple(active)

    async def load_reservation(self, reservation_id: str) -> PositionReservation | None:
        return self._reservations.get(reservation_id)
