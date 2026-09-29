"""The canonical encoding cache must not change any facts hash."""

from dataclasses import replace
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


def test_cached_and_uncached_encodings_hash_identically() -> None:
    journal = _journal()
    cached_facts = journal.read_cut()

    cached_hash = cached_facts.compute_facts_hash()
    uncached_hash = replace(
        cached_facts, _canonical_fact_cache=None
    ).compute_facts_hash()

    assert cached_hash == uncached_hash


def test_repeated_hashes_reuse_the_cache_across_snapshots() -> None:
    journal = _journal()
    cache = journal._canonical_fact_cache
    facts = journal.read_cut()

    for _ in range(3):
        replace(facts, _canonical_fact_cache=cache).compute_facts_hash()

    # First pass encodes every fact; later passes must be pure cache hits.
    assert cache.misses > 0
    hits_after_first = cache.hits
    replace(facts, _canonical_fact_cache=cache).compute_facts_hash()
    assert cache.hits > hits_after_first
    assert cache.hits >= cache.misses


def test_transaction_candidate_shares_the_encoding_cache() -> None:
    journal = _journal()
    candidate = journal.copy_for_transaction()
    assert candidate._canonical_fact_cache is journal._canonical_fact_cache
