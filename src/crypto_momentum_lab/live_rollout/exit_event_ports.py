"""Processing and lane capabilities consumed by exit event coordination."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Protocol

from crypto_momentum_lab.domain.market.models import MarketState15s, RealtimeMarketQuote

if TYPE_CHECKING:
    from crypto_momentum_lab.live_rollout.closed_candle_feed import ClosedCandle15mEvent
    from crypto_momentum_lab.live_rollout.context import LiveDaemonRuntimeContext


class ExitFailureResult(Protocol):
    @property
    def failure(self) -> str | None: ...


class ExitEventProcessor(Protocol):
    async def process_state(
        self, state: MarketState15s, context: LiveDaemonRuntimeContext
    ) -> ExitFailureResult: ...

    async def process_quote(
        self,
        quote: RealtimeMarketQuote,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
    ) -> ExitFailureResult: ...

    async def process_closed_candle(
        self,
        event: ClosedCandle15mEvent,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
        latest_quote: RealtimeMarketQuote | None,
    ) -> ExitFailureResult: ...

    async def process_grace_timeout(
        self,
        state: MarketState15s,
        now: datetime,
        context: LiveDaemonRuntimeContext,
        latest_quote: RealtimeMarketQuote | None,
    ) -> ExitFailureResult: ...


class ExitEventLane(Protocol):
    async def start(self) -> None: ...

    async def submit_account(
        self, state: MarketState15s, context: LiveDaemonRuntimeContext
    ) -> ExitFailureResult: ...

    async def submit_quote(
        self,
        quote: RealtimeMarketQuote,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
        *,
        wait: bool = False,
    ) -> ExitFailureResult | None: ...
