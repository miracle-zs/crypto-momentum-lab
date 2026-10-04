"""Pure entry candidate preparation shared by observation and submission."""

from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy.models import EntryType, OrderIntentCandidate


def entry_limit_price(
    candidate: OrderIntentCandidate,
    *,
    state: MarketState15s,
) -> tuple[Decimal | None, str | None]:
    if candidate.limit_price is not None:
        return candidate.limit_price, "candidate.limit_price"
    if (
        candidate.side.value == "long"
        and state.last_ask_price is not None
        and state.close_price is not None
    ):
        return (
            min(state.last_ask_price, state.close_price),
            "min(state.last_ask_price,state.close_price)",
        )
    if candidate.side.value == "long" and state.last_ask_price is not None:
        return state.last_ask_price, "state.last_ask_price"
    if state.close_price is not None:
        return state.close_price, "state.close_price"
    if state.mark_price is not None:
        return state.mark_price, "state.mark_price"
    if state.midpoint is not None:
        return state.midpoint, "state.midpoint"
    return None, None


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
    signal_price, _ = entry_limit_price(candidate, state=state)
    return replace(
        candidate,
        entry_type=EntryType.LIMIT,
        limit_price=signal_price,
        expires_at=execution_now + timedelta(seconds=limit_ttl_seconds),
    )
