from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account.models import AccountFillEvent
from crypto_momentum_lab.execution_account.fill_progress import fill_scan_load_id


def fills():
    first = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="1",
        order_id="10",
        side="BUY",
        price=Decimal("50000.00"),
        quantity=Decimal("0.010"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0.1"),
        fee_asset="USDT",
        trade_at=datetime(2026, 10, 1, tzinfo=UTC),
        raw_payload={},
    )
    return (
        first,
        replace(
            first,
            trade_id="2",
            order_id="11",
            quantity=Decimal("0.02"),
            price=Decimal("51000"),
        ),
    )


def test_empty_scan_keeps_fixed_identifier():
    assert fill_scan_load_id("BTCUSDT", 0, 1000, ()) == (
        "fillscan_99c1084bd2de971847f426faa21debcded712801004845c9292c51932f211303"
    )


def test_populated_scan_keeps_fixed_identifier_and_inputs():
    observations = fills()
    assert fill_scan_load_id("BTCUSDT", 0, 1000, observations) == (
        "fillscan_57f2931095b1b037d20fdfdce0a190def7d81dd2c1954eb8ad06bac7807bcf08"
    )
    assert observations == fills()


@pytest.mark.parametrize(
    "change",
    [
        "symbol",
        "start",
        "end",
        "trade",
        "order",
        "quantity",
        "price",
        "sequence",
        "decimal_text",
    ],
)
def test_identity_preserves_each_input_and_order_or_decimal_representation(change):
    observations = fills()
    symbol, start, end = "BTCUSDT", 0, 1000
    baseline = fill_scan_load_id(symbol, start, end, observations)
    if change == "symbol":
        symbol = "btcusdt"
    elif change == "start":
        start = 1
    elif change == "end":
        end = 1001
    elif change == "sequence":
        observations = tuple(reversed(observations))
    else:
        changes = {
            "trade": {"trade_id": "3"},
            "order": {"order_id": "12"},
            "quantity": {"quantity": Decimal("0.011")},
            "price": {"price": Decimal("50001")},
            "decimal_text": {"quantity": Decimal("0.01")},
        }
        observations = (replace(observations[0], **changes[change]), observations[1])
    assert fill_scan_load_id(symbol, start, end, observations) != baseline


def test_identity_ignores_metadata_not_in_the_historical_digest():
    observations = fills()
    changed = tuple(
        replace(
            item,
            account_label="other",
            environment="testnet",
            side="SELL",
            fee=Decimal("0.2"),
            realized_pnl=Decimal("1"),
            raw_payload={"extra": True},
            trade_at=datetime(2026, 10, 2, tzinfo=UTC),
        )
        for item in observations
    )
    assert fill_scan_load_id("BTCUSDT", 0, 1000, changed) == fill_scan_load_id(
        "BTCUSDT", 0, 1000, observations
    )
