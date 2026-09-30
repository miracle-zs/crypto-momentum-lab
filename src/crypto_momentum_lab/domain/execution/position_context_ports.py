"""Execution facts required to build operational position context."""

from decimal import Decimal
from typing import Protocol

from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
    PositionView,
)


class PositionRepairBook(Protocol):
    async def reload_position(
        self,
        key: PositionKey,
        *,
        expected_scope: AccountFactStreamScope,
        expected_quantity: Decimal,
    ) -> PositionView | None: ...


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
