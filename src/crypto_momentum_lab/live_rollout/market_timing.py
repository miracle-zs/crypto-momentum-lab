"""Bounded correlation of Hub publication and strategy ingress timestamps."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from crypto_momentum_lab.domain.market.models import MarketState15s

if TYPE_CHECKING:
    from crypto_momentum_lab.market_data.hub import MarketStateBatch


@dataclass(frozen=True, slots=True)
class MarketStateTiming:
    """Transport timestamps for one state observed by a Hub consumer."""

    published_at: datetime
    socket_received_at: datetime


class LiveMarketTimingTracker:
    """Keep a small, bounded correlation window for live Hub states.

    The Hub source deliberately yields plain ``MarketState15s`` values to its
    callers.  This tracker preserves the transport metadata at that boundary
    without coupling the decision path to the WebSocket protocol.
    """

    def __init__(self, *, max_entries: int = 4_096) -> None:
        if max_entries <= 0:
            raise ValueError("max_entries must be positive")
        self._max_entries = max_entries
        self._timings: dict[tuple[str, datetime], MarketStateTiming] = {}
        self._order: deque[tuple[str, datetime]] = deque()

    def observe_batch(
        self,
        batch: MarketStateBatch,
        *,
        received_at: datetime | None = None,
    ) -> None:
        """Record the point at which a decoded Hub batch reached this process."""
        observed_at = received_at or datetime.now(tz=UTC)
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("received_at must be timezone-aware")
        for state in batch.states:
            key = (state.symbol, state.bucket_start)
            if key not in self._timings:
                self._order.append(key)
            self._timings[key] = MarketStateTiming(
                published_at=batch.published_at,
                socket_received_at=observed_at,
            )
        while len(self._timings) > self._max_entries:
            key = self._order.popleft()
            self._timings.pop(key, None)

    def timing_for(self, state: MarketState15s) -> MarketStateTiming | None:
        """Return transport timestamps if this state came from the live Hub."""
        return self._timings.get((state.symbol, state.bucket_start))


__all__ = ["LiveMarketTimingTracker", "MarketStateTiming"]
