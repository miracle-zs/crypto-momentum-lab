"""Fact hashes remain stable across detached journal cuts."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey

START = datetime(2026, 9, 29, tzinfo=UTC)


def _journal() -> AccountJournal:
    key = PositionKey("live", "acc", "BTCUSDT", FuturesPositionSide.BOTH)
    journal = AccountJournal(key)
    for index in range(4):
        journal.record_snapshot(
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
        )
    journal.append_fill(
        AccountFillEvent(
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
    )
    return journal


def test_independent_journal_cuts_hash_identically() -> None:
    journal = _journal()
    cached_facts = journal.read_cut()

    cached_hash = cached_facts.compute_facts_hash()
    uncached_hash = _journal().read_cut().compute_facts_hash()

    assert cached_hash == uncached_hash
