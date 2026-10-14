from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.execution.recovery_models import AccountFacts

START = datetime(2026, 9, 29, tzinfo=UTC)


def _facts() -> AccountFacts:
    key = PositionKey("live", "acc", "BTCUSDT", FuturesPositionSide.BOTH)
    snapshots = tuple(
        AccountPositionSnapshot(
            environment="live",
            account_label="acc",
            symbol="BTCUSDT",
            position_side="BOTH",
            position_amt=Decimal("1"),
            entry_price=Decimal("65000"),
            mark_price=Decimal("65001"),
            unrealized_pnl=Decimal("1"),
            notional=Decimal("65001"),
            leverage=1,
            margin_type="isolated",
            observed_at=START + timedelta(seconds=index),
            raw_payload={"positionAmt": "1"},
        )
        for index in range(4)
    )
    fill = AccountFillEvent(
        environment="live",
        account_label="acc",
        symbol="BTCUSDT",
        trade_id="t1",
        order_id="o1",
        side="BUY",
        price=Decimal("65000.25"),
        quantity=Decimal("0.001"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=START,
        raw_payload={"positionSide": "BOTH"},
    )
    return AccountFacts(position_key=key, fills=(fill,), snapshots=snapshots)


def test_facts_hash_is_independent_of_unordered_input_order() -> None:
    facts = _facts()
    reordered = AccountFacts(
        position_key=facts.position_key,
        fills=tuple(reversed(facts.fills)),
        snapshots=tuple(reversed(facts.snapshots)),
    )

    assert facts.compute_facts_hash() == reordered.compute_facts_hash()
