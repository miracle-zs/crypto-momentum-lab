"""Execution facts required to build operational position context."""

from collections.abc import Callable
from typing import Protocol

from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionView,
)
from crypto_momentum_lab.domain.execution.position_repair_models import (
    PositionRepairRequest,
    PositionRepairUnitOfWork,
    PublishedPositionRepair,
)


class PositionRepairBook(Protocol):
    async def repair_position(
        self,
        request: PositionRepairRequest,
        *,
        uow: PositionRepairUnitOfWork,
        is_current: Callable[[], bool] | None = None,
    ) -> PublishedPositionRepair: ...


class PositionContextBook(PositionRepairBook, Protocol):
    """Read current account positions and reload committed repairs."""

    @property
    def context_revision(self) -> int: ...

    async def list_position_views(
        self,
        *,
        environment: str,
        account_label: str,
        symbols: frozenset[str] | None = None,
    ) -> tuple[PositionView, ...]: ...

    def get_active_stream(
        self, environment: str, account_label: str
    ) -> tuple[str, str] | None: ...
