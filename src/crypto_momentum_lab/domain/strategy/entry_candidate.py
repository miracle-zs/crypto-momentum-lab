"""Prepare entry candidates for order submission."""

from dataclasses import replace
from datetime import datetime, timedelta

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy.models import EntryType, OrderIntentCandidate


def prepare_entry_candidate(
    candidate: OrderIntentCandidate,
    *,
    state: MarketState15s,
    execution_now: datetime,
    entry_order_type: EntryType,
    limit_ttl_seconds: int,
) -> OrderIntentCandidate:
    if candidate.reduce_only or entry_order_type is EntryType.MARKET:
        return candidate
    signal_price = candidate.limit_price
    if signal_price is None:
        if candidate.side.value == "long" and state.last_ask_price is not None:
            signal_price = (
                min(state.last_ask_price, state.close_price)
                if state.close_price is not None
                else state.last_ask_price
            )
        elif state.close_price is not None:
            signal_price = state.close_price
        elif state.mark_price is not None:
            signal_price = state.mark_price
        else:
            signal_price = state.midpoint
    return replace(
        candidate,
        entry_type=EntryType.LIMIT,
        limit_price=signal_price,
        expires_at=execution_now + timedelta(seconds=limit_ttl_seconds),
    )
