"""Pure Binance exit recovery matching without exchange requests."""

from decimal import Decimal

from crypto_momentum_lab.domain.account.models import (
    AccountOpenOrderSnapshot,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.order_state import (
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.exit_recovery import (
    ExitRecoveryInspectionUnknownError,
)


def exit_position_quantity(
    position: AccountPositionSnapshot,
    plan: OrderExecutionPlan,
) -> Decimal:
    if position.symbol != plan.symbol:
        return Decimal("0")
    try:
        position_side = FuturesPositionSide(position.position_side.upper())
    except ValueError as exc:
        raise ExitRecoveryInspectionUnknownError(
            "Binance returned an unsupported position side"
        ) from exc
    if position_side is not plan.position_side:
        return Decimal("0")
    if position.position_amt == 0:
        return Decimal("0")
    if plan.position_side is FuturesPositionSide.BOTH:
        if plan.side == "SELL" and position.position_amt <= 0:
            return Decimal("0")
        if plan.side == "BUY" and position.position_amt >= 0:
            return Decimal("0")
    return abs(position.position_amt)


def open_order_matches_exit(
    order: AccountOpenOrderSnapshot,
    plan: OrderExecutionPlan,
) -> bool:
    if order.symbol != plan.symbol or order.side.upper() != plan.side.upper():
        return False
    raw_position_side = order.raw_payload.get("positionSide")
    if raw_position_side is None:
        if plan.position_side is not FuturesPositionSide.BOTH:
            raise ExitRecoveryInspectionUnknownError(
                "Binance hedge-mode open order omitted positionSide"
            )
        position_side = FuturesPositionSide.BOTH
    else:
        try:
            position_side = FuturesPositionSide(str(raw_position_side).upper())
        except ValueError as exc:
            raise ExitRecoveryInspectionUnknownError(
                "Binance returned an unsupported open-order position side"
            ) from exc
    if position_side is not plan.position_side:
        return False
    # In one-way mode a matching side alone is not enough: an opposite-side
    # opening order could otherwise suppress the recovery of a close order.
    # Hedge mode scopes the order by positionSide because Binance does not
    # accept reduceOnly there.
    return plan.position_side is not FuturesPositionSide.BOTH or order.reduce_only
