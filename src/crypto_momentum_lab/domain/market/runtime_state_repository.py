"""Durable market-state reading capabilities required by startup recovery."""

from collections.abc import Collection, Mapping
from datetime import datetime
from typing import Protocol

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.market.runtime_state_models import RuntimeStateCursor


class RuntimeMarketStateReadRepository(Protocol):
    """Read committed states; persistence and session ownership stay in adapters."""

    async def load_latest_bucket(self, *, environment: str) -> datetime | None: ...

    async def load_after(
        self,
        *,
        environment: str,
        cursor: RuntimeStateCursor,
        limit: int,
        upper_bound: datetime | None = None,
        symbols: Collection[str] | None = None,
    ) -> tuple[MarketState15s, ...]:
        """Read after the exclusive tuple cursor, ordered by bucket then symbol.

        upper_bound is inclusive; absent symbol filtering reads all symbols,
        whereas an empty symbol collection selects none.
        """
        ...

    async def load_recovery_window(
        self,
        *,
        environment: str,
        last_processed_at_by_symbol: Mapping[str, datetime],
        lookback_seconds: int,
        limit: int,
        upper_bound: datetime | None = None,
    ) -> tuple[MarketState15s, ...]:
        """Read bounded derivable history for each checkpoint symbol."""
        ...
