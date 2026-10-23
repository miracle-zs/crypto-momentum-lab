"""Strategy runtime contract shared by construction and live consumers."""

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
    def buffered_state_count(self) -> int: ...

    def metadata(self) -> StrategyMetadata: ...

    def required_data(self) -> StrategyDataRequirement: ...

    def on_market_state(self, state: MarketState15s) -> StrategyDecision: ...

    def checkpoint(self) -> StrategyCheckpoint: ...

    def warm_market_state(self, state: MarketState15s) -> None: ...

    def clear_market_state_buffers(self) -> None: ...

    def reset_symbol(self, symbol: str) -> None: ...
