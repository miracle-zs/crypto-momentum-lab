from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import (
    OrderIntentCandidate,
    RejectionReason,
    StrategyCheckpoint,
    StrategyDecision,
    StrategyRejection,
    StrategySignal,
)


@dataclass(slots=True)
class StrategyRuntimeState:
    """Bookkeeping for the order-flow strategy's rolling market-state buffer."""

    buffers: dict[str, deque[MarketState15s]] = field(default_factory=dict)
    warmup: dict[str, int] = field(default_factory=dict)
    cooldown_remaining: dict[str, int] = field(default_factory=dict)
    last_processed: dict[str, datetime] = field(default_factory=dict)
    signal_sequence: int = 0

    def append_market_state(
        self,
        state: MarketState15s,
        *,
        max_buffer_length: int,
    ) -> deque[MarketState15s]:
        if max_buffer_length <= 0:
            raise ValueError("max_buffer_length must be positive")
        buffer = self.buffers.setdefault(
            state.symbol,
            deque(maxlen=max_buffer_length),
        )
        buffer.append(state)
        self.warmup[state.symbol] = len(buffer)
        return buffer

    def warm_market_state(
        self,
        state: MarketState15s,
        *,
        max_buffer_length: int,
    ) -> None:
        if state.effective_price is None:
            previous = self.last_processed.get(state.symbol)
            if previous is None or state.bucket_start >= previous:
                # A durable bucket with no close price is still a real
                # market-state watermark when it has no usable quote either.
                # Leaving the old checkpoint timestamp in place makes the
                # first valid bucket after a data outage look like a
                # continuity break and sends every live worker into a restart
                # loop.
                self.last_processed[state.symbol] = state.bucket_start
            return
        self.append_market_state(
            state,
            max_buffer_length=max_buffer_length,
        )
        previous = self.last_processed.get(state.symbol)
        if previous is None or state.bucket_start >= previous:
            # Recovery replays market data without evaluating it, but the
            # replayed watermark still represents the latest contiguous state
            # known to the strategy.  Keeping the old checkpoint watermark
            # would make the first live state look like a gap and immediately
            # erase the buffer we just restored.
            self.last_processed[state.symbol] = state.bucket_start

    def clear_market_state_buffers(self) -> None:
        """Drop rolling market data while preserving trading control state."""
        self.buffers = {}
        self.warmup = {}

    def reset_symbol(self, symbol: str) -> None:
        self.buffers.pop(symbol, None)
        self.warmup.pop(symbol, None)
        self.cooldown_remaining.pop(symbol, None)
        self.last_processed.pop(symbol, None)

    def checkpoint(self) -> StrategyCheckpoint:
        return StrategyCheckpoint(
            last_processed_at_by_symbol=dict(self.last_processed),
            warmup_buckets_by_symbol=dict(self.warmup),
            cooldown_buckets_remaining_by_symbol=dict(self.cooldown_remaining),
            payload={"signal_sequence": self.signal_sequence},
        )


def evaluate_buffered_state[EventT](
    runtime: StrategyRuntimeState,
    state: MarketState15s,
    *,
    warmup_buckets: int,
    max_buffer_length: int,
    cooldown_buckets: int,
    find_event: Callable[[tuple[MarketState15s, ...], MarketState15s], EventT | None],
    build_signal_and_candidate: Callable[
        [EventT, datetime], tuple[StrategySignal, OrderIntentCandidate]
    ],
) -> StrategyDecision:
    """Run the shared warmup/cooldown loop around a strategy-specific event."""

    runtime.last_processed[state.symbol] = state.bucket_start
    if state.effective_price is None:
        return StrategyDecision(
            signals=(),
            candidates=(),
            rejections=(
                StrategyRejection(
                    reason=RejectionReason.MISSING_REQUIRED_PRICE,
                    symbol=state.symbol,
                    bucket_start=state.bucket_start,
                    details={"field": "effective_price"},
                ),
            ),
        )

    buffer = runtime.append_market_state(
        state,
        max_buffer_length=max_buffer_length,
    )
    if len(buffer) < warmup_buckets:
        return StrategyDecision(
            signals=(),
            candidates=(),
            rejections=(
                StrategyRejection(
                    reason=RejectionReason.INSUFFICIENT_WARMUP,
                    symbol=state.symbol,
                    bucket_start=state.bucket_start,
                    details={
                        "have": len(buffer),
                        "need": warmup_buckets,
                    },
                ),
            ),
        )

    cooldown = runtime.cooldown_remaining.get(state.symbol, 0)
    if cooldown > 0:
        runtime.cooldown_remaining[state.symbol] = cooldown - 1
        return StrategyDecision(
            signals=(),
            candidates=(),
            rejections=(
                StrategyRejection(
                    reason=RejectionReason.COOLDOWN_ACTIVE,
                    symbol=state.symbol,
                    bucket_start=state.bucket_start,
                    details={"remaining": cooldown},
                ),
            ),
        )

    event = find_event(tuple(buffer), state)
    if event is None:
        return StrategyDecision(
            signals=(),
            candidates=(),
            rejections=(
                StrategyRejection(
                    reason=RejectionReason.NO_SIGNAL,
                    symbol=state.symbol,
                    bucket_start=state.bucket_start,
                    details={"state": "evaluated"},
                ),
            ),
        )

    signal, candidate = build_signal_and_candidate(event, state.bucket_end)
    runtime.cooldown_remaining[state.symbol] = cooldown_buckets
    return StrategyDecision(
        signals=(signal,),
        candidates=(candidate,),
        rejections=(),
    )
