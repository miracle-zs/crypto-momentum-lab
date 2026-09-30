"""Pure fill identity and exit-settlement decisions; no journal mutation."""

from dataclasses import dataclass
from decimal import Decimal

from crypto_momentum_lab.domain.account.models import AccountFillEvent
from crypto_momentum_lab.domain.execution.evidence_settlement import (
    cumulative_fill_delta,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    FuturesPositionSide,
)
from crypto_momentum_lab.domain.strategy import StrategySide


@dataclass(frozen=True, slots=True)
class FillObservationPlan:
    fill: AccountFillEvent
    quantity: Decimal
    settlement_quantity: Decimal
    cumulative_quantity: Decimal | None
    watermark: tuple[Decimal, Decimal] | None
    is_cumulative: bool
    is_new_trade: bool
    adopted_prefix_trade: bool


def plan_fill_observation(
    fill: AccountFillEvent,
    *,
    existing_trade: AccountFillEvent | None,
    trade_seen: bool,
    previous_quantity: Decimal,
    previous_quote: Decimal,
    can_adopt_prefix: bool,
) -> FillObservationPlan:
    raw = fill.raw_payload if isinstance(fill.raw_payload, dict) else {}
    is_cumulative = bool(raw.get("is_cumulative") or "cum_qty" in raw)
    quantity = fill.quantity
    applied_fill = fill
    cumulative_quantity = None
    watermark = None
    adopted_prefix_trade = False
    if is_cumulative:
        delta = cumulative_fill_delta(
            fill,
            previous_quantity=previous_quantity,
            previous_quote=previous_quote,
            trade_seen=trade_seen,
        )
        quantity = delta.quantity
        applied_fill = delta.fill
        cumulative_quantity = delta.cumulative_quantity
        watermark = delta.watermark
    settlement_quantity = quantity
    if not is_cumulative and existing_trade is not None:
        if (
            existing_trade.quantity != fill.quantity
            or existing_trade.price != fill.price
            or existing_trade.side.upper() != fill.side.upper()
            or existing_trade.symbol != fill.symbol
        ):
            raise ValueError(
                f"Fill {fill.trade_id} conflicts with existing journal records"
            )
        quantity = Decimal("0")
        settlement_quantity = Decimal("0")
    elif not is_cumulative and trade_seen:
        if can_adopt_prefix:
            adopted_prefix_trade = True
            settlement_quantity = Decimal("0")
        else:
            raise ValueError(
                f"Fill {fill.trade_id} was already seen but its journal facts "
                "are unavailable; recovery is required"
            )
    return FillObservationPlan(
        fill=applied_fill,
        quantity=quantity,
        settlement_quantity=settlement_quantity,
        cumulative_quantity=cumulative_quantity,
        watermark=watermark,
        is_cumulative=is_cumulative,
        is_new_trade=not trade_seen or adopted_prefix_trade,
        adopted_prefix_trade=adopted_prefix_trade,
    )


def is_exit_fill(
    fill: AccountFillEvent,
    *,
    position_side: FuturesPositionSide,
    episode_side: StrategySide | None,
    has_active_reservations: bool,
    order_event: ExchangeOrderEvent | None,
) -> bool:
    raw = fill.raw_payload if isinstance(fill.raw_payload, dict) else {}
    return (
        (fill.side.upper() == "SELL" and position_side == FuturesPositionSide.LONG)
        or (fill.side.upper() == "BUY" and position_side == FuturesPositionSide.SHORT)
        or has_active_reservations
        or bool(raw.get("reduce_only"))
        or (
            order_event is not None
            and bool((order_event.details or {}).get("is_reduce_only"))
        )
        or (episode_side == StrategySide.LONG and fill.side.upper() == "SELL")
        or (episode_side == StrategySide.SHORT and fill.side.upper() == "BUY")
    )
