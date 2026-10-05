from collections.abc import Collection
from dataclasses import dataclass

from crypto_momentum_lab.domain.execution.order_read_models import (
    PersistedExchangeOrder,
)
from crypto_momentum_lab.domain.execution.order_state import ExchangeOrderState


@dataclass(frozen=True, slots=True)
class LiveGateContext:
    live_submit_enabled: bool
    account_label: str
    strategy_name: str
    strategy_config_hash: str


def order_state_is_uncertain(state: ExchangeOrderState) -> bool:
    """Classify ambiguous outcomes for local conflicts and risk occupancy."""
    return state in {
        ExchangeOrderState.INTENT_APPROVED,
        ExchangeOrderState.CLAIMED,
        ExchangeOrderState.PLANNED,
        ExchangeOrderState.SUBMITTING,
        ExchangeOrderState.CANCELING,
        ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
    }


def has_entry_order_conflict(
    symbol: str,
    orders: Collection[PersistedExchangeOrder],
    states: Collection[ExchangeOrderState] = (),
) -> bool:
    """Reject conflicting identities or risk without a finite priced bound.

    A states-only view cannot locate or bound an uncertain order. Keep that
    missing-evidence case closed rather than treating it as an empty account.
    """
    uncertain = tuple(
        order for order in orders if order_state_is_uncertain(order.state)
    )
    if sum(order_state_is_uncertain(state) for state in states) > len(uncertain):
        return True
    for order in uncertain:
        plan = order.plan
        if plan.symbol == symbol:
            return True
        if not plan.reduce_only and (
            plan.price is None
            or not plan.price.is_finite()
            or plan.price <= 0
            or not plan.quantity.is_finite()
            or plan.quantity <= 0
        ):
            return True
    return False
