"""Frozen SAND quantities/times; read-only probe against the real ledger.

The final assertion intentionally fails until comparison handles mixed cuts.
"""

from datetime import datetime
from decimal import Decimal as D

from crypto_momentum_lab.domain.account import AccountFillEvent, AccountPositionSnapshot
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import AccountFacts, PositionKey


def at(value: str) -> datetime:
    return datetime.fromisoformat("2026-09-25T07:25:" + value + "+00:00")


def fill(trade_id: str, quantity: str, when: str) -> AccountFillEvent:
    return AccountFillEvent(
        environment="live", account_label="account-3", symbol="SANDUSDT",
        trade_id=trade_id, order_id="23149283042", side="BUY", price=D(".04568"),
        quantity=D(quantity), realized_pnl=D(0), fee=D(0), fee_asset="USDT",
        trade_at=at(when), raw_payload={"positionSide": "LONG", "is_system": True},
    )


def snapshot(quantity: str, when: str) -> AccountPositionSnapshot:
    return AccountPositionSnapshot(
        environment="live", account_label="account-3", symbol="SANDUSDT",
        position_side="LONG", position_amt=D(quantity), entry_price=D(".04568"),
        mark_price=D(".04568"), unrealized_pnl=D(0), notional=D(quantity)*D(".04568"),
        leverage=None, margin_type=None, observed_at=at(when), raw_payload={},
    )


if __name__ == "__main__":
    key = PositionKey("live", "account-3", "SANDUSDT", FuturesPositionSide.LONG)
    first = fill("1000604297", "1403", "20.591")
    second = fill("1000604298", "786", "20.899")
    early = snapshot("1403", "20.594640")
    late = snapshot("2189", "20.902586")
    for name, fills, observation in (
        ("aligned_early", (first,), early),
        ("aligned_late", (first, second), late),
        ("mixed_cut", (first, second), early),
    ):
        result = PositionLedger(key).project(AccountFacts(key, fills, (observation,)))
        print(f"{name}: ledger={result.total_active_quantity} snapshot={observation.position_amt} gap={result.reconciliation_gap}")
        assert result.reconciliation_gap == 0, "mixed cuts must be aligned or explicitly incomparable"
