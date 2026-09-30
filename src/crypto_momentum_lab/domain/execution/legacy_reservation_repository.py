"""Explicit legacy reservation adaptation and synchronous constructor assembly."""

import inspect
from decimal import Decimal
from typing import TYPE_CHECKING, cast

from crypto_momentum_lab.domain.execution.command_repository import CommandRepository
from crypto_momentum_lab.domain.execution.execution_coordinator import (
    ExecutionCoordinator,
    PositionReservationRepository,
)
from crypto_momentum_lab.domain.execution.ports import ExecutionUnitOfWorkPort
from crypto_momentum_lab.domain.execution.trade_command import PositionReservation

if TYPE_CHECKING:
    from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook


class LegacyReservationRepositoryAdapter:
    def __init__(self, repository: object) -> None:
        self._repository = repository

    async def _call(self, name: str, *args: object, **kwargs: object) -> object:
        method = getattr(self._repository, name, None)
        if not callable(method):
            raise RuntimeError(
                f"legacy reservation repository does not implement {name}"
            )
        result = method(*args, **kwargs)
        if inspect.isawaitable(result):
            result = await result
        return result

    async def load_reservation(self, reservation_id: str) -> PositionReservation | None:
        # Legacy single-save repositories did not expose identity lookup.
        if not callable(getattr(self._repository, "load_reservation", None)):
            return None
        return cast(
            PositionReservation | None,
            await self._call("load_reservation", reservation_id),
        )

    async def load_active_reservations(self) -> tuple[PositionReservation, ...]:
        return tuple(
            cast(
                tuple[PositionReservation, ...],
                await self._call("load_active_reservations"),
            )
        )

    async def save_reservations(
        self,
        reservations: tuple[PositionReservation, ...],
        *,
        expected_projection_version: str | None = None,
        batch_quantities: dict[str, Decimal] | None = None,
    ) -> None:
        if callable(getattr(self._repository, "save_reservations", None)):
            await self._call(
                "save_reservations",
                reservations,
                expected_projection_version=expected_projection_version,
                batch_quantities=batch_quantities,
            )
        else:
            for reservation in reservations:
                await self._call(
                    "save_reservation",
                    reservation,
                    expected_projection_version=expected_projection_version,
                )

    async def update_reservation(
        self,
        reservation: PositionReservation,
        release_reason: str | None = None,
    ) -> None:
        if release_reason is None:
            await self._call("update_reservation", reservation)
        else:
            await self._call(
                "update_reservation", reservation, release_reason=release_reason
            )


def assemble_legacy_execution_book(
    *,
    reservation_repository: object | None = None,
    coordinator: ExecutionCoordinator | None = None,
    command_repository: CommandRepository | None = None,
    execution_unit_of_work: ExecutionUnitOfWorkPort | None = None,
) -> "ExecutionBook":
    from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook

    if coordinator is None and reservation_repository is not None:
        loader = getattr(reservation_repository, "load_active_reservations", None)
        if execution_unit_of_work is None and not inspect.iscoroutinefunction(loader):
            coordinator = ExecutionCoordinator(
                repository=cast(PositionReservationRepository, reservation_repository)
            )
    return ExecutionBook(
        coordinator=coordinator,
        reservation_repository=(
            LegacyReservationRepositoryAdapter(reservation_repository)
            if reservation_repository is not None
            else None
        ),
        command_repository=command_repository,
        execution_unit_of_work=execution_unit_of_work,
    )
