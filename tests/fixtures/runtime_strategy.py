"""Recording strategy fixture for the Live market data contract."""

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import (
    StrategyCheckpoint,
    StrategyDataRequirement,
    StrategyDecision,
)


class FakeStrategy:
    def __init__(self) -> None:
        self.restored_checkpoint: StrategyCheckpoint | None = None
        self.processed: list[MarketState15s] = []
        self.reset_symbols: list[str] = []
        self._checkpoint: StrategyCheckpoint | None = None
        self.checkpoint_calls = 0

    def reset_symbol(self, symbol: str) -> None:
        self.reset_symbols.append(symbol)

    def required_data(self) -> StrategyDataRequirement:
        return StrategyDataRequirement(
            base_state_interval_seconds=15,
            warmup_buckets=1,
            required_fields=("close_price",),
            max_gap_seconds=30,
            allow_entries_before_warmup=False,
        )

    def restore_checkpoint(self, checkpoint: StrategyCheckpoint) -> None:
        self.restored_checkpoint = checkpoint

    def on_market_state(self, state: MarketState15s) -> StrategyDecision:
        self.processed.append(state)
        checkpoint = StrategyCheckpoint(
            last_processed_at_by_symbol={state.symbol: state.bucket_start},
            warmup_buckets_by_symbol={state.symbol: len(self.processed)},
            cooldown_buckets_remaining_by_symbol={state.symbol: 0},
            payload={"last_symbol": state.symbol},
        )
        self._checkpoint = checkpoint
        return StrategyDecision(
            signals=(),
            candidates=(),
            rejections=(),
            checkpoint=checkpoint,
        )

    def checkpoint(self) -> StrategyCheckpoint:
        self.checkpoint_calls += 1
        if self._checkpoint is None:
            raise AssertionError("checkpoint requested before processing a state")
        return self._checkpoint
