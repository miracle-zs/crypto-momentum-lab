"""The durable order observations needed by reconciliation and live gates."""

from typing import Protocol

from crypto_momentum_lab.domain.execution.order_read_models import (
    PersistedExchangeOrder,
)


class OrderReadRepository(Protocol):
    async def load_unresolved_orders(
        self,
        run_id: str | None = None,
    ) -> tuple[PersistedExchangeOrder, ...]: ...

    async def load_order(
        self,
        client_order_id: str,
    ) -> PersistedExchangeOrder | None: ...
