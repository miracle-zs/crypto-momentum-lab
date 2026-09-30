from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account.models import AccountBalanceSnapshot
from crypto_momentum_lab.execution_account.balance_history import select_balance_history


def balance(values, asset="USDT"):
    return AccountBalanceSnapshot(
        environment="live",
        account_label="primary",
        asset=asset,
        wallet_balance=Decimal(values[0]),
        available_balance=Decimal(values[1]),
        unrealized_pnl=Decimal(values[2]),
        observed_at=datetime(2026, 10, 1, tzinfo=UTC),
        raw_payload={},
    )


@pytest.mark.parametrize(
    "values,previous,retained",
    [
        (("0", "0", "0"), None, False),
        (("0", "0", "0"), ("0", "0", "0"), False),
        (("0", "0", "0"), ("1", "0", "0"), True),
        (("0", "0", "0"), ("0", "1", "0"), True),
        (("0", "0", "0"), ("0", "0", "-1"), True),
        (("1", "0", "0"), None, True),
        (("0", "1", "0"), None, True),
        (("0", "0", "-1"), None, True),
    ],
)
def test_zero_transition_and_each_nonzero_balance_dimension(values, previous, retained):
    snapshot = balance(values)
    history = {} if previous is None else {"USDT": tuple(Decimal(v) for v in previous)}
    before = dict(history)
    assert select_balance_history((snapshot,), history) == (
        (snapshot,) if retained else ()
    )
    assert history == before


def test_selection_keeps_input_order_identity_and_asset_scope():
    first = balance(("1", "0", "0"), "BTC")
    omitted = balance(("0", "0", "0"), "ETH")
    last = balance(("0", "0", "0"), "USDT")
    previous = {"USDT": (Decimal("1"), Decimal("0"), Decimal("0"))}
    selected = select_balance_history((first, omitted, last), previous)
    assert selected == (first, last)
    assert selected[0] is first
    assert selected[1] is last
