"""Execution facts required to build operational position context."""

from typing import Protocol

from crypto_momentum_lab.domain.execution.position_ledger_models import PositionView
from crypto_momentum_lab.domain.execution.position_repair import PositionRepairBook


class PositionContextBook(PositionRepairBook, Protocol):
    """Read current account positions and reload committed repairs."""

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
