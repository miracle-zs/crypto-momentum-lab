from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

from crypto_momentum_lab.domain.account.models import AccountFillEvent
from crypto_momentum_lab.domain.execution.evidence_digest import trade_payload_digest


def test_trade_digest_preserves_durable_identity_across_timezone_and_key_order():
    fill = AccountFillEvent(
        environment="live", account_label="account-3", symbol="BTCUSDT",
        trade_id="trade-1", order_id="order-1", side="BUY",
        price=Decimal("100.00"), quantity=Decimal("2.0"),
        realized_pnl=Decimal("0"), fee=Decimal("0.1"), fee_asset="USDT",
        trade_at=datetime(2026, 9, 30, 8, tzinfo=timezone(timedelta(hours=8))),
        raw_payload={"positionSide": "LONG", "extra": {"b": 2, "a": 1}},
    )
    # Captured from the original writer before moving the digest implementation.
    expected = "9ce4e461246c5c8d2b5f7ffa953a9c81c2122414767fc8134ef9a37daec2c420"
    assert trade_payload_digest(fill) == expected
    equivalent = replace(
        fill, trade_at=fill.trade_at.astimezone(UTC),
        raw_payload={"extra": {"a": 1, "b": 2}, "positionSide": "LONG"},
    )
    assert trade_payload_digest(equivalent) == expected
    assert trade_payload_digest(replace(fill, quantity=Decimal("3.0"))) != expected
