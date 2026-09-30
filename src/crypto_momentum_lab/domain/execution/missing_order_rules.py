"""Fail-closed rules for operator confirmation of an absent reduce-only order."""

from datetime import datetime
from decimal import Decimal

from crypto_momentum_lab.domain.execution.order_state import ExchangeOrderState


def validate_missing_order_resolution(
    *,
    state: str,
    reduce_only: bool,
    exchange_order_id: str | None,
    created_at: datetime,
    now: datetime,
    order_quantity: Decimal,
    executed_quantity: Decimal,
    position_quantity: Decimal,
    exchange_order_found: bool,
    matching_open_order_found: bool,
    min_missing_age_seconds: float,
) -> None:
    """Fail closed before an operator resolves an unknown order as absent."""
    if state != ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION.value:
        raise RuntimeError(
            "order is not pending reconciliation; no manual resolution is allowed"
        )
    if not reduce_only:
        raise RuntimeError("only reduce-only orders may be manually resolved")
    if exchange_order_id is not None:
        raise RuntimeError("exchange order id is already recorded")
    if executed_quantity != 0:
        raise RuntimeError("executed quantity is non-zero")
    age_seconds = (now - created_at).total_seconds()
    if age_seconds < min_missing_age_seconds:
        raise RuntimeError(f"order is younger than {min_missing_age_seconds:g} seconds")
    if exchange_order_found:
        raise RuntimeError("exchange order still exists")
    if matching_open_order_found:
        raise RuntimeError("matching open order still exists")
    if position_quantity != order_quantity:
        raise RuntimeError("position quantity changed; manual resolution is unsafe")
