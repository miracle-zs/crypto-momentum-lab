from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.execution.order_state import ExchangeOrderState
from crypto_momentum_lab.execution_account.binance.order_status import (
    exchange_order_state,
    is_open_order_status,
    should_discard_position_expectation,
)


@pytest.mark.parametrize(
    "status,state,is_open,discard",
    [
        ("NEW", ExchangeOrderState.ACKNOWLEDGED, True, False),
        ("PARTIALLY_FILLED", ExchangeOrderState.PARTIALLY_FILLED, True, False),
        ("FILLED", ExchangeOrderState.FILLED, False, False),
        ("CANCELED", ExchangeOrderState.CANCELED, False, True),
        ("REJECTED", ExchangeOrderState.REJECTED, False, True),
        ("EXPIRED", ExchangeOrderState.EXPIRED, False, True),
        ("EXPIRED_IN_MATCH", ExchangeOrderState.EXPIRED, False, True),
    ],
)
def test_known_status_mapping_and_zero_fill_expectation_policy(
    status, state, is_open, discard
):
    assert exchange_order_state(status) is state
    assert is_open_order_status(status) is is_open
    assert should_discard_position_expectation(status, Decimal("0")) is discard
    assert should_discard_position_expectation(status, Decimal("0.001")) is False


@pytest.mark.parametrize("status", ["", "UNKNOWN", "new", " NEW "])
def test_unknown_status_remains_strict_for_rest_and_nonterminal_for_expectations(
    status,
):
    with pytest.raises(ValueError, match="unsupported Binance order status") as caught:
        exchange_order_state(status)
    assert isinstance(caught.value.__cause__, KeyError)
    assert is_open_order_status(status) is False
    assert should_discard_position_expectation(status, Decimal("0")) is False
