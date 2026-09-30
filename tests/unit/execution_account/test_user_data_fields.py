from decimal import Decimal

import pytest

from crypto_momentum_lab.execution_account.user_data_fields import initial_mark_price


@pytest.mark.parametrize(
    "entry,amount,pnl,expected",
    [
        ("10", "2", "4", "12"),
        ("10", "-2", "4", "8"),
        ("10", "2", "-4", "8"),
        ("10", "0", "4", "10"),
        ("10", "2", "0", "10"),
        ("10", "2", "-20", "10"),
        ("10", "2", "-22", "10"),
        ("0", "0", "0", "0"),
        ("-1", "0", "0", "0"),
    ],
)
def test_provisional_mark_handles_long_short_and_safe_fallback(
    entry, amount, pnl, expected
):
    assert initial_mark_price(
        entry_price=Decimal(entry),
        position_amt=Decimal(amount),
        unrealized_pnl=Decimal(pnl),
    ) == Decimal(expected)
