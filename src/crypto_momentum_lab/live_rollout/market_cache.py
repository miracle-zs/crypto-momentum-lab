"""Latest-value caches shared by live market and exit event channels."""

from __future__ import annotations

from crypto_momentum_lab.domain.market.models import MarketState15s, RealtimeMarketQuote


class LatestMarketStateCache:
    """Keep the newest state per symbol for event-driven exit recovery."""

    def __init__(self) -> None:
        self._states: dict[str, MarketState15s] = {}

    def observe(self, state: MarketState15s) -> None:
        previous = self._states.get(state.symbol)
        if previous is None or state.bucket_start >= previous.bucket_start:
            self._states[state.symbol] = state

    def for_symbols(
        self,
        symbols: tuple[str, ...],
    ) -> tuple[MarketState15s, ...]:
        if symbols:
            selected = [
                self._states[symbol]
                for symbol in symbols
                if symbol in self._states
            ]
        else:
            selected = list(self._states.values())
        return tuple(
            sorted(selected, key=lambda state: (state.bucket_start, state.symbol))
        )


class LatestMarketQuoteCache:
    """Keep the newest quote per symbol for quote/candle/grace exits."""

    def __init__(self) -> None:
        self._quotes: dict[str, RealtimeMarketQuote] = {}

    def observe(self, quote: RealtimeMarketQuote) -> None:
        previous = self._quotes.get(quote.symbol)
        if previous is None or quote.received_at >= previous.received_at:
            self._quotes[quote.symbol] = quote

    def for_symbols(
        self,
        symbols: tuple[str, ...],
    ) -> tuple[RealtimeMarketQuote, ...]:
        if symbols:
            selected = [
                self._quotes[symbol]
                for symbol in symbols
                if symbol in self._quotes
            ]
        else:
            selected = list(self._quotes.values())
        return tuple(
            sorted(selected, key=lambda quote: (quote.received_at, quote.symbol))
        )


__all__ = ["LatestMarketQuoteCache", "LatestMarketStateCache"]
