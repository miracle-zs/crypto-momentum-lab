"""Strategy, result and gap-recovery contracts for the live market runtime.

These values do not import the daemon, market loop, exchange or persistence.
Lifecycle and recovery callers share the same types as the ordered loop.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime

from crypto_momentum_lab.domain.market.models import MarketState15s


class LiveMarketStateContinuityError(RuntimeError):
    """Raised when an ordered live state stream skips a required bucket."""

    def __init__(
        self,
        *,
        symbol: str,
        previous_at: datetime,
        current_at: datetime,
        expected_interval_seconds: int,
    ) -> None:
        observed_delta_seconds = int((current_at - previous_at).total_seconds())
        super().__init__(
            "missing market-state bucket: "
            f"symbol={symbol} previous={previous_at.isoformat()} "
            f"current={current_at.isoformat()} "
            f"expected_interval_seconds={expected_interval_seconds} "
            f"observed_delta_seconds={observed_delta_seconds}"
        )
        self.symbol = symbol
        self.previous_at = previous_at
        self.current_at = current_at
        self.expected_interval_seconds = expected_interval_seconds
        self.observed_delta_seconds = observed_delta_seconds


MarketStateGapRecovery = Callable[
    [LiveMarketStateContinuityError], Awaitable[Sequence[MarketState15s]]
]


@dataclass(frozen=True, slots=True)
class LiveDaemonResult:
    processed_state_count: int
    approved_intent_count: int
    submitted_order_count: int
    halt_reason: str | None
    final_state_at: datetime | None
