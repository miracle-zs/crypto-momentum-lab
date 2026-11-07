"""Durable paging and recovery reads consumed by market-state use cases."""

from datetime import datetime
from typing import Protocol

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.market.runtime_state_models import RuntimeStateCursor


class RuntimeMarketStatePageReader(Protocol):
    """Read committed states; persistence and session ownership stay in adapters."""

    async def load_latest_bucket(self, *, environment: str) -> datetime | None: ...

    async def load_after(
        self,
        *,
        environment: str,
        cursor: RuntimeStateCursor,
        limit: int,
    ) -> tuple[MarketState15s, ...]:
        """Read after the exclusive tuple cursor, ordered by bucket then symbol."""
        ...
