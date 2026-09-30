"""Awaited reservation capabilities used outside the execution transaction."""

from decimal import Decimal
from typing import Protocol

from crypto_momentum_lab.domain.execution.trade_command import PositionReservation


class ReservationRepository(Protocol):
    async def load_reservation(
        self, reservation_id: str
    ) -> PositionReservation | None: ...
    async def load_active_reservations(self) -> tuple[PositionReservation, ...]: ...
    async def save_reservations(
        self,
        reservations: tuple[PositionReservation, ...],
        *,
        expected_projection_version: str | None = None,
        batch_quantities: dict[str, Decimal] | None = None,
    ) -> None: ...
    async def update_reservation(
        self,
        reservation: PositionReservation,
        release_reason: str | None = None,
    ) -> None: ...
