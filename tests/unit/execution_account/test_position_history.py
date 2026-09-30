from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account.models import AccountPositionSnapshot
from crypto_momentum_lab.execution_account.position_history import (
    should_persist_position,
)


@pytest.mark.parametrize(
    "amount,entry,previous,elapsed,retained",
    [
        ("0", "0", None, 0, False),
        ("0", "0", ("0", "0"), 0, False),
        ("0", "0", ("1", "10"), 0, True),
        ("1", "10", None, 0, True),
        ("1", "10", ("1", "10"), 1.999, False),
        ("1", "10", ("1", "10"), 2, True),
        ("1", "10", ("1", "10"), -1, False),
        ("2", "10", ("1", "10"), 0, True),
        ("1", "11", ("1", "10"), 0, True),
    ],
)
def test_zero_transitions_changes_and_exact_coalesce_window(
    amount, entry, previous, elapsed, retained
):
    timestamp = datetime(2026, 10, 1, tzinfo=UTC)
    position = AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side="LONG",
        position_amt=Decimal(amount),
        entry_price=Decimal(entry),
        mark_price=Decimal("10"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("10"),
        leverage=5,
        margin_type="CROSSED",
        observed_at=timestamp,
        raw_payload={},
    )
    signature = (
        None
        if previous is None
        else (Decimal(previous[0]), Decimal(previous[1]), timestamp)
    )
    assert (
        should_persist_position(
            position, signature, observed_at=timestamp + timedelta(seconds=elapsed)
        )
        is retained
    )
