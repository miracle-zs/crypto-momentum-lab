"""Processing and lane capabilities consumed by exit event coordination."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Literal, Protocol

from crypto_momentum_lab.domain.market.models import MarketState15s, RealtimeMarketQuote
from crypto_momentum_lab.live_rollout.exit_lane import ExitLaneOutcome

if TYPE_CHECKING:
    from crypto_momentum_lab.live_rollout.closed_candle_feed import ClosedCandle15mEvent
    from crypto_momentum_lab.live_rollout.context import LiveDaemonRuntimeContext


class ExitEventProcessor(Protocol):
    async def handle_trigger(
        self,
        trigger: Literal["state", "closed_candle", "grace_timeout", "quote"],
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
        *,
        event: ClosedCandle15mEvent | None = None,
        quote: RealtimeMarketQuote | None = None,
        now: datetime | None = None,
    ) -> ExitLaneOutcome: ...


class ExitEventLane(Protocol):
    async def start(self) -> None: ...

    async def submit_market(self, state: MarketState15s) -> None: ...

    async def submit_quote(
        self,
        quote: RealtimeMarketQuote,
        state: MarketState15s,
    ) -> None: ...
