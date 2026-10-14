"""Identity-based admission for actual account trade facts."""

from dataclasses import dataclass

from crypto_momentum_lab.domain.account.models import AccountFillEvent


@dataclass(frozen=True, slots=True)
class FillObservationPlan:
    fill: AccountFillEvent
    is_new_trade: bool
    adopted_prefix_trade: bool


def plan_fill_observation(
    fill: AccountFillEvent,
    *,
    trade_seen: bool,
    can_adopt_prefix: bool,
) -> FillObservationPlan:
    """Apply each exchange trade identity once; order reports are separate."""
    raw = fill.raw_payload if isinstance(fill.raw_payload, dict) else {}
    if raw.get("is_cumulative") or "cum_qty" in raw:
        raise ValueError("account fill must be an individual trade, not an order total")
    adopted = trade_seen and can_adopt_prefix
    return FillObservationPlan(
        fill=fill,
        is_new_trade=not trade_seen or adopted,
        adopted_prefix_trade=adopted,
    )
