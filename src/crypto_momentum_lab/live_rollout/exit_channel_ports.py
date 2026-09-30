"""Exit-processing capabilities used by the independent live channels."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Protocol

from crypto_momentum_lab.domain.market.models import MarketState15s

if TYPE_CHECKING:
    from crypto_momentum_lab.domain.market.models import RealtimeMarketQuote
    from crypto_momentum_lab.live_rollout.closed_candle_feed import ClosedCandle15mEvent


class ExitChannelProcessor(Protocol):
    @property
    def managed_position_symbols(self) -> frozenset[str]: ...

    async def process_market_quote(
        self, quote: RealtimeMarketQuote, state: MarketState15s
    ) -> str | None: ...

    async def process_closed_candle(
        self,
        event: ClosedCandle15mEvent,
        *,
        latest_quote: RealtimeMarketQuote | None = None,
    ) -> str | None: ...

    async def process_grace_timeout(
        self,
        state: MarketState15s,
        *,
        now: datetime,
        latest_quote: RealtimeMarketQuote | None = None,
    ) -> str | None: ...
