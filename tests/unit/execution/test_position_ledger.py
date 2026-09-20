"""Comprehensive test suite for PositionLedger domain service and models."""

import random
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFacts,
    ExitOrderSubmissionFact,
    PositionKey,
)
from crypto_momentum_lab.domain.strategy import StrategySide
from tests.fixtures.b2_anonymized_timeline import get_b2_account_fill_events


def _key(symbol: str = "BTCUSDT") -> PositionKey:
    return PositionKey(
        environment="live",
        account_label="primary",
        symbol=symbol,
        position_side=FuturesPositionSide.BOTH,
    )


def _fill(
    trade_id: str,
    side: str,
    qty: str,
    price: str,
    trade_at: datetime,
    symbol: str = "BTCUSDT",
    order_id: str = "ord_1",
    is_system: bool = True,
) -> AccountFillEvent:
    return AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol=symbol,
        trade_id=trade_id,
        order_id=order_id,
        side=side,
        price=Decimal(price),
        quantity=Decimal(qty),
        realized_pnl=Decimal("0.0"),
        fee=Decimal("0.01"),
        fee_asset="USDT",
        trade_at=trade_at,
        raw_payload={"is_system": is_system},
    )


def test_position_key_canonical_id_and_validation() -> None:
    key = _key("ETHUSDT")
    assert key.canonical_id == "live:primary:ETHUSDT:BOTH"

    with pytest.raises(ValueError, match="symbol must not be empty"):
        PositionKey(environment="live", account_label="a", symbol="")


def test_position_ledger_single_entry() -> None:
    key = _key()
    ledger = PositionLedger(key)
    t0 = datetime(2026, 9, 20, 10, 0, tzinfo=UTC)

    f1 = _fill("t1", "BUY", "10", "60000", t0)
    facts = AccountFacts(position_key=key, fills=(f1,))

    proj = ledger.project(facts)

    assert proj.active_episode is not None
    assert proj.active_episode.side == StrategySide.LONG
    assert proj.active_episode.is_active is True
    assert proj.total_active_quantity == Decimal("10")
    assert len(proj.active_batches) == 1
    assert proj.active_batches[0].quantity == Decimal("10")
    assert proj.active_batches[0].entry_price == Decimal("60000")
    assert len(proj.archived_episodes) == 0


def test_position_ledger_consecutive_adds_without_exit_boundary_aggregate_batch() -> None:
    """Per CONTEXT.md and Astra critique S1: consecutive adds before exit boundary

    belong to the SAME batch with updated anchor and weighted-average entry price.
    """
    key = _key()
    ledger = PositionLedger(key)
    t0 = datetime(2026, 9, 20, 10, 0, tzinfo=UTC)

    f1 = _fill("t1", "BUY", "10", "60000", t0)
    f2 = _fill("t2", "BUY", "15", "61000", t0 + timedelta(minutes=5))

    facts = AccountFacts(position_key=key, fills=(f1, f2))
    proj = ledger.project(facts)

    assert proj.total_active_quantity == Decimal("25")
    assert len(proj.active_batches) == 1
    batch = proj.active_batches[0]
    assert batch.quantity == Decimal("25")
    assert batch.original_quantity == Decimal("25")
    # Weighted average: (10 * 60000 + 15 * 61000) / 25 = 60600
    assert batch.entry_price == Decimal("60600")
    # Anchor updated to the latest add-on entry
    assert batch.opened_at == t0 + timedelta(minutes=5)


def test_position_ledger_scaling_adds_and_fifo_reduction() -> None:
    """Consecutive entries without exit boundary merge into one batch; reduction deducts from it."""
    key = _key()
    ledger = PositionLedger(key)
    t0 = datetime(2026, 9, 20, 10, 0, tzinfo=UTC)

    # Entry 10, then add 15 -> merged total 25
    f1 = _fill("t1", "BUY", "10", "60000", t0)
    f2 = _fill("t2", "BUY", "15", "61000", t0 + timedelta(minutes=5))

    # Sell 12 -> deducts 12 from merged batch 1 (was 25) -> remaining 13
    f3 = _fill("t3", "SELL", "12", "62000", t0 + timedelta(minutes=10))

    facts = AccountFacts(position_key=key, fills=(f1, f2, f3))
    proj = ledger.project(facts)

    assert proj.total_active_quantity == Decimal("13")
    assert len(proj.active_batches) == 1
    assert proj.active_batches[0].original_quantity == Decimal("25")
    assert proj.active_batches[0].quantity == Decimal("13")
    assert proj.active_batches[0].entry_price == Decimal("60600")
    assert proj.active_batches[0].opened_at == t0 + timedelta(minutes=5)

    # Check reduction record
    assert proj.active_episode is not None
    assert len(proj.active_episode.reductions) == 1
    red = proj.active_episode.reductions[0]
    assert red.quantity == Decimal("12")
    assert len(red.attributions) == 1
    assert red.attributions[0].quantity == Decimal("12")
    assert red.is_system is True


def test_position_ledger_exit_boundary_separates_batches_and_fifo_deduction() -> None:
    """Exit boundary between entries creates separate batches; reduction uses FIFO."""
    key = _key()
    ledger = PositionLedger(key)
    t0 = datetime(2026, 9, 20, 10, 0, tzinfo=UTC)

    # Entry 10 at t0
    f1 = _fill("t1", "BUY", "10", "60000", t0, order_id="ord_entry_1")

    # Exit boundary submitted at t0 + 2m
    sub1 = ExitOrderSubmissionFact(
        order_id="ord_exit_1",
        submitted_at=t0 + timedelta(minutes=2),
        symbol=key.symbol,
        position_side=key.position_side,
    )

    # Entry 15 at t0 + 5m (after exit boundary -> MUST start batch 2!)
    f2 = _fill("t2", "BUY", "15", "61000", t0 + timedelta(minutes=5), order_id="ord_entry_2")

    # Sell 12 at t0 + 10m -> FIFO deducts 10 from batch 1, 2 from batch 2 -> remaining 13 in batch 2
    f3 = _fill("t3", "SELL", "12", "62000", t0 + timedelta(minutes=10), order_id="ord_exit_1")

    facts = AccountFacts(
        position_key=key,
        fills=(f1, f2, f3),
        exit_boundaries=(sub1,),
    )
    proj = ledger.project(facts)

    assert proj.total_active_quantity == Decimal("13")
    assert len(proj.active_batches) == 1
    assert proj.active_batches[0].batch_id.endswith("_b2")
    assert proj.active_batches[0].original_quantity == Decimal("15")
    assert proj.active_batches[0].quantity == Decimal("13")
    assert proj.active_batches[0].entry_price == Decimal("61000")

    # Check reduction record across both batches
    assert proj.active_episode is not None
    assert len(proj.active_episode.reductions) == 1
    red = proj.active_episode.reductions[0]
    assert red.quantity == Decimal("12")
    assert len(red.attributions) == 2
    assert red.attributions[0].quantity == Decimal("10")
    assert red.attributions[1].quantity == Decimal("2")
    assert red.is_system is True


def test_position_ledger_zero_crossing_isolates_new_episode() -> None:
    key = _key()
    ledger = PositionLedger(key)
    t0 = datetime(2026, 9, 20, 10, 0, tzinfo=UTC)

    # Buy 20, Sell 20 -> Zero crossing!
    f1 = _fill("t1", "BUY", "20", "60000", t0)
    f2 = _fill("t2", "SELL", "20", "61000", t0 + timedelta(minutes=10))

    facts = AccountFacts(position_key=key, fills=(f1, f2))
    proj = ledger.project(facts)

    assert proj.total_active_quantity == Decimal("0")
    assert proj.active_episode is None
    assert len(proj.active_batches) == 0
    assert len(proj.archived_episodes) == 1
    assert proj.archived_episodes[0].closed_at == t0 + timedelta(minutes=10)
    assert proj.archived_episodes[0].is_active is False

    # Now open a new position with Buy 30
    f3 = _fill("t3", "BUY", "30", "62000", t0 + timedelta(minutes=20))
    facts2 = AccountFacts(position_key=key, fills=(f1, f2, f3))
    proj2 = ledger.project(facts2)

    assert proj2.total_active_quantity == Decimal("30")
    assert proj2.active_episode is not None
    assert proj2.active_episode.is_active is True
    assert proj2.active_episode.opened_at == t0 + timedelta(minutes=20)
    assert len(proj2.active_batches) == 1
    assert proj2.active_batches[0].quantity == Decimal("30")
    assert len(proj2.archived_episodes) == 1


def test_position_ledger_reversal_flip() -> None:
    """Test position flipping directly from LONG to SHORT."""
    key = _key()
    ledger = PositionLedger(key)
    t0 = datetime(2026, 9, 20, 10, 0, tzinfo=UTC)

    # Buy 10, then Sell 25 -> Flips to SHORT 15
    f1 = _fill("t1", "BUY", "10", "60000", t0)
    f2 = _fill("t2", "SELL", "25", "61000", t0 + timedelta(minutes=5))

    facts = AccountFacts(position_key=key, fills=(f1, f2))
    proj = ledger.project(facts)

    assert len(proj.archived_episodes) == 1
    assert proj.archived_episodes[0].side == StrategySide.LONG
    assert proj.archived_episodes[0].closed_at == t0 + timedelta(minutes=5)

    assert proj.active_episode is not None
    assert proj.active_episode.side == StrategySide.SHORT
    assert proj.total_active_quantity == Decimal("15")
    assert len(proj.active_batches) == 1
    assert proj.active_batches[0].quantity == Decimal("15")


def test_position_ledger_b2_timeline_full_replay() -> None:
    """Replay the real B2USDT incident timeline and verify perfect accounting."""
    key = PositionKey(
        environment="live",
        account_label="account-3",
        symbol="B2USDT",
        position_side=FuturesPositionSide.BOTH,
    )
    ledger = PositionLedger(key)

    fills = get_b2_account_fill_events()
    facts = AccountFacts(position_key=key, fills=fills)

    proj = ledger.project(facts)

    # The timeline ends with an external sell of 7 coins, returning balance to 0
    assert proj.total_active_quantity == Decimal("0")
    assert proj.active_episode is None

    # Episodes created:
    # Episode 1: Initial entries (120+127+127) -> partial system sell (120) -> external manual sell (254) -> Closed!
    # Episode 2: Post-zero buy (172) -> system sells (112+22) -> add (174) -> system sells (167+38) -> external sell (7) -> Closed!
    assert len(proj.archived_episodes) == 2

    ep1 = proj.archived_episodes[0]
    assert ep1.cumulative_bought == Decimal("374")
    assert ep1.cumulative_sold == Decimal("374")
    assert ep1.is_active is False

    ep2 = proj.archived_episodes[1]
    assert ep2.cumulative_bought == Decimal("346")  # 172 + 174 = 346
    assert ep2.cumulative_sold == Decimal("346")    # 134 + 167 + 38 + 7 = 346
    assert ep2.is_active is False


def test_position_ledger_idempotency_and_out_of_order_resilience() -> None:
    """Verify that duplicate fills and shuffled fills produce identical results."""
    key = _key()
    ledger = PositionLedger(key)
    t0 = datetime(2026, 9, 20, 10, 0, tzinfo=UTC)

    fills = [
        _fill("t1", "BUY", "10", "60000", t0),
        _fill("t2", "BUY", "15", "61000", t0 + timedelta(minutes=5)),
        _fill("t3", "SELL", "12", "62000", t0 + timedelta(minutes=10)),
        _fill("t4", "BUY", "8", "60500", t0 + timedelta(minutes=15)),
    ]

    # Baseline projection
    baseline = ledger.project(AccountFacts(position_key=key, fills=tuple(fills)))

    # Shuffled order with duplicates
    shuffled_with_dups = fills * 2
    random.seed(42)
    random.shuffle(shuffled_with_dups)

    proj = ledger.project(AccountFacts(position_key=key, fills=tuple(shuffled_with_dups)))

    assert proj.total_active_quantity == baseline.total_active_quantity
    assert len(proj.active_batches) == len(baseline.active_batches)
    assert [b.quantity for b in proj.active_batches] == [b.quantity for b in baseline.active_batches]
    assert proj.high_watermark_trade_at == baseline.high_watermark_trade_at
