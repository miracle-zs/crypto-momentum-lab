from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from crypto_momentum_lab.domain.account.models import AccountFillEvent
from crypto_momentum_lab.domain.execution.evidence_digest import (
    trade_payload_digest,
)


def test_trade_digest_preserves_durable_identity_across_timezone_and_key_order():
    fill = AccountFillEvent(
        environment="live",
        account_label="account-3",
        symbol="BTCUSDT",
        trade_id="trade-1",
        order_id="order-1",
        side="BUY",
        price=Decimal("100.00"),
        quantity=Decimal("2.0"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0.1"),
        fee_asset="USDT",
        trade_at=datetime(2026, 9, 30, 8, tzinfo=timezone(timedelta(hours=8))),
        raw_payload={"positionSide": "LONG", "extra": {"b": 2, "a": 1}},
    )
    rest = replace(
        fill,
        price=Decimal("100.000000000000000000"),
        quantity=Decimal("2.000000000000000000"),
        fee=Decimal("0.100000000000000000"),
        raw_payload={"positionSide": "LONG", "source": "rest"},
    )
    assert trade_payload_digest(fill) == trade_payload_digest(rest)
    for changed in (
        replace(rest, quantity=Decimal("3")),
        replace(rest, fee=Decimal("0.2")),
        replace(rest, trade_at=rest.trade_at + timedelta(milliseconds=1)),
        replace(rest, raw_payload={"positionSide": "SHORT"}),
    ):
        assert trade_payload_digest(fill) != trade_payload_digest(changed)
