"""Reservation writes performed within an execution-owned SQL session."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import TYPE_CHECKING, Protocol

from crypto_momentum_lab.domain.execution.trade_command import PositionReservation

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class ExecutionReservationStore(Protocol):
    async def save_reservations_in_session(
        self,
        session: AsyncSession,
        reservations: Sequence[PositionReservation],
        *,
        expected_projection_version: str | None = None,
        batch_quantities: Mapping[str, Decimal] | None = None,
        proven_position_quantity: Decimal | None = None,
    ) -> None: ...

    async def update_reservation_in_session(
        self,
        session: AsyncSession,
        reservation: PositionReservation,
        *,
        release_reason: str | None = None,
    ) -> None: ...
