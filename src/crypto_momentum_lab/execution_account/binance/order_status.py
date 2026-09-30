"""Binance order status rules independent of transport and merge state."""

from decimal import Decimal

from crypto_momentum_lab.domain.execution.order_state import ExchangeOrderState

_OPEN_ORDER_STATUSES = frozenset({"NEW", "PARTIALLY_FILLED"})
_NO_FILL_TERMINAL_ORDER_STATUSES = frozenset(
    {"CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}
)


def exchange_order_state(status: str) -> ExchangeOrderState:
    states = {
        "NEW": ExchangeOrderState.ACKNOWLEDGED,
        "PARTIALLY_FILLED": ExchangeOrderState.PARTIALLY_FILLED,
        "FILLED": ExchangeOrderState.FILLED,
        "CANCELED": ExchangeOrderState.CANCELED,
        "REJECTED": ExchangeOrderState.REJECTED,
        "EXPIRED": ExchangeOrderState.EXPIRED,
        "EXPIRED_IN_MATCH": ExchangeOrderState.EXPIRED,
    }
    try:
        return states[status]
    except KeyError as exc:
        raise ValueError(f"unsupported Binance order status: {status}") from exc


def is_open_order_status(status: str) -> bool:
    return status in _OPEN_ORDER_STATUSES


def should_discard_position_expectation(
    status: str, executed_quantity: Decimal
) -> bool:
    return status in _NO_FILL_TERMINAL_ORDER_STATUSES and executed_quantity == 0
