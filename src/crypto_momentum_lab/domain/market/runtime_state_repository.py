"""Durable paging and recovery reads consumed by market-state use cases."""

from collections.abc import Collection, Mapping
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
        upper_bound: datetime | None = None,
        symbols: Collection[str] | None = None,
    ) -> tuple[MarketState15s, ...]:
        """Read after the exclusive tuple cursor, ordered by bucket then symbol.

        upper_bound is inclusive; absent symbol filtering reads all symbols,
        whereas an empty symbol collection selects none.
        """
        ...


class RuntimeMarketStateReadRepository(RuntimeMarketStatePageReader, Protocol):
    """Paging and per-checkpoint recovery reads required by strategy startup."""

    async def load_symbols_at(
        self, *, environment: str, observed_at: datetime
    ) -> frozenset[str]:
        """Symbols in the newest durable bucket at or before observed_at."""
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
