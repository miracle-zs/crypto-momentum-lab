"""Account-triggered exit processing consumed by the account event channel."""

from typing import Protocol

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.market_data.quote_hub import RealtimeMarketQuote


class AccountEventExitProcessor(Protocol):
    async def process_account_event(
        self, state: MarketState15s, *, quote: RealtimeMarketQuote | None = None
    ) -> str | None: ...
