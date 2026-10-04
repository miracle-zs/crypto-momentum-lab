"""Strategy runtime contract shared by construction and live consumers."""

from collections.abc import Collection
from datetime import datetime, timedelta
from typing import Protocol

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy.models import (
    StrategyCheckpoint,
    StrategyDataRequirement,
    StrategyDecision,
    StrategyMetadata,
)


class RuntimeStrategy(Protocol):
    @property
    def buffered_symbol_count(self) -> int: ...

    @property
    def buffered_state_count(self) -> int: ...

    def cache_protected_symbols(self) -> frozenset[str]: ...

    def prune_inactive_symbols(
        self,
        *,
        now: datetime,
        protected_symbols: Collection[str],
        inactive_after: timedelta,
    ) -> tuple[str, ...]: ...

    def metadata(self) -> StrategyMetadata: ...

    def required_data(self) -> StrategyDataRequirement: ...

    def restore_checkpoint(self, checkpoint: StrategyCheckpoint) -> None: ...

    def on_market_state(self, state: MarketState15s) -> StrategyDecision: ...

    def checkpoint(
        self,
        *,
        include_market_state_buffers: bool = True,
    ) -> StrategyCheckpoint: ...

    def warm_market_state(self, state: MarketState15s) -> None: ...

    def clear_market_state_buffers(self) -> None: ...

    def reset_symbol(self, symbol: str) -> None: ...
