"""Comprehensive test suite for PositionLedger domain service and models."""

import random
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    ExitOrderSubmissionFact,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.domain.execution.recovery_models import (
    AccountFacts,
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


def test_position_ledger_consecutive_adds_without_exit_boundary_aggregate_batch() -> (
    None
):
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


@pytest.mark.parametrize("side", ["BUY", "SELL"])
def test_two_opening_orders_share_price_anchor_and_survive_checkpoint(
    side: str,
) -> None:
    key = _key()
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="hub", stream_epoch="e1"
    )
    ledger = PositionLedger(key)
    t0 = datetime(2026, 9, 20, 10, 0, tzinfo=UTC)
    first = replace(
        _fill("a1", side, "2", "100", t0, order_id="a"),
        raw_payload={"is_system": True, "client_order_id": "ca"},
    )
    second = replace(
        _fill("b1", side, "3", "110", t0 + timedelta(seconds=10), order_id="b"),
        raw_payload={"is_system": True, "client_order_id": "cb"},
    )
    prefix = AccountFacts(position_key=key, stream_scope=scope, fills=(first, second))
    projection = ledger.project(prefix)
    assert len(projection.active_batches) == 1
    batch = projection.active_batches[0]
    assert batch.entry_price == Decimal("106")
    assert batch.opened_at == second.trade_at
    assert batch.entry_order_ids == ("a", "b")
    assert batch.entry_client_order_ids == ("ca", "cb")

    checkpoint = ledger.create_recovery_checkpoint(
        prefix, source_revision=2, event_cut=second.trade_at
    )
    checkpoint = PositionRecoveryCodec.decode_checkpoint(
        PositionRecoveryCodec.encode_checkpoint(checkpoint)
    )
    late_partial = replace(
        _fill("a2", side, "1", "120", t0 + timedelta(seconds=20), order_id="a"),
        raw_payload={"is_system": True, "client_order_id": "ca"},
    )
    recovered = ledger.project(
        AccountFacts(
            position_key=key,
            stream_scope=scope,
            recovery_checkpoint=checkpoint,
            prefix_facts_complete=False,
            fills=(late_partial,),
        )
    )
    complete = ledger.project(
        AccountFacts(
            position_key=key, stream_scope=scope, fills=(first, second, late_partial)
        )
    )
    assert recovered.active_batches == complete.active_batches
    batch = recovered.active_batches[0]
    assert batch.quantity == Decimal("6")
    assert batch.entry_price == Decimal("650") / Decimal("6")
    assert batch.opened_at == second.trade_at
    assert batch.entry_order_ids == ("a", "b")


def test_position_ledger_scaling_adds_and_fifo_reduction() -> None:
    """Consecutive entries without exit boundary merge into one batch;
    reduction deducts from it.
    """
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
    f2 = _fill(
        "t2", "BUY", "15", "61000", t0 + timedelta(minutes=5), order_id="ord_entry_2"
    )

    # Sell 12 at t0 + 10m -> FIFO deducts 10 from batch 1, 2 from batch 2
    # -> remaining 13 in batch 2
    f3 = _fill(
        "t3", "SELL", "12", "62000", t0 + timedelta(minutes=10), order_id="ord_exit_1"
    )

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
    # Episode 1: Initial entries (120+127+127) -> partial system sell (120)
    #            -> external manual sell (254) -> Closed!
    # Episode 2: Post-zero buy (172) -> system sells (112+22) -> add (174)
    #            -> system sells (167+38) -> external sell (7) -> Closed!
    assert len(proj.archived_episodes) == 2

    ep1 = proj.archived_episodes[0]
    assert ep1.cumulative_bought == Decimal("374")
    assert ep1.cumulative_sold == Decimal("374")
    assert ep1.is_active is False

    ep2 = proj.archived_episodes[1]
    assert ep2.cumulative_bought == Decimal("346")  # 172 + 174 = 346
    assert ep2.cumulative_sold == Decimal("346")  # 134 + 167 + 38 + 7 = 346
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

    proj = ledger.project(
        AccountFacts(position_key=key, fills=tuple(shuffled_with_dups))
    )

    assert proj.total_active_quantity == baseline.total_active_quantity
    assert len(proj.active_batches) == len(baseline.active_batches)
    assert [b.quantity for b in proj.active_batches] == [
        b.quantity for b in baseline.active_batches
    ]
    assert proj.high_watermark_trade_at == baseline.high_watermark_trade_at


def test_position_ledger_interleaved_exit_fill_does_not_split_new_batch() -> None:
    """Astra S1 reproduction:
    BUY 10 -> Submit Exit Order for batch 1 (boundary) -> BUY 5
    -> Exit fill SELL 3 (old order) -> BUY 5.
    Must produce 2 batches with quantities [7, 10], NOT 3 batches [7, 5, 5].
    """
    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    t0 = datetime(2026, 8, 1, 10, 0, tzinfo=UTC)
    t_exit_sub = datetime(2026, 8, 1, 10, 5, tzinfo=UTC)
    t_buy1 = datetime(2026, 8, 1, 10, 6, tzinfo=UTC)
    t_exit_fill = datetime(2026, 8, 1, 10, 7, tzinfo=UTC)
    t_buy2 = datetime(2026, 8, 1, 10, 8, tzinfo=UTC)

    fills = [
        # Step 1: BUY 10
        _fill("t1", "BUY", "10", "50000", t0, order_id="ord_entry_1"),
        # Step 3: BUY 5
        _fill("t2", "BUY", "5", "51000", t_buy1, order_id="ord_entry_2"),
        # Step 4: Old exit order fill SELL 3
        _fill("t3", "SELL", "3", "52000", t_exit_fill, order_id="ord_exit_1"),
        # Step 5: BUY 5
        _fill("t4", "BUY", "5", "51500", t_buy2, order_id="ord_entry_3"),
    ]

    boundaries = [
        # Step 2: Exit order submitted for batch 1
        ExitOrderSubmissionFact(
            order_id="ord_exit_1",
            submitted_at=t_exit_sub,
            symbol="BTCUSDT",
            position_side="BOTH",
        ),
    ]

    facts = AccountFacts(
        position_key=key,
        fills=tuple(fills),
        exit_boundaries=tuple(boundaries),
    )

    ledger = PositionLedger(key)
    proj = ledger.project(facts)

    assert len(proj.active_batches) == 2, (
        f"Expected 2 batches, got {[b.quantity for b in proj.active_batches]}"
    )
    assert proj.active_batches[0].quantity == Decimal("7")
    assert proj.active_batches[0].exit_order_submitted_at == t_exit_sub
    assert proj.active_batches[1].quantity == Decimal("10")
    assert proj.active_batches[1].opened_at == t_buy2
    assert proj.active_batches[1].exit_order_submitted_at is None
    assert proj.total_active_quantity == Decimal("17")


def test_position_ledger_external_reduction_does_not_fabricate_exit_boundary() -> None:
    """Astra S1 critique: BUY 10 -> BUY 10 -> external SELL 15 -> BUY 5.

    Without ExitOrderSubmissionFact, the external reduction must NOT fabricate
    an exit_order_submitted_at on the remaining batch. Subsequent add (BUY 5)
    must aggregate with the remaining 5 to form a single batch of quantity 10.
    """
    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    t0 = datetime(2026, 8, 1, 10, 0, tzinfo=UTC)
    fills = [
        _fill("t1", "BUY", "10", "50000", t0, is_system=True),
        _fill("t2", "BUY", "10", "51000", t0 + timedelta(minutes=1), is_system=True),
        _fill("t3", "SELL", "15", "52000", t0 + timedelta(minutes=2), is_system=False),
        _fill("t4", "BUY", "5", "51500", t0 + timedelta(minutes=3), is_system=True),
    ]
    facts = AccountFacts(position_key=key, fills=tuple(fills))
    proj = PositionLedger(key).project(facts)

    assert len(proj.active_batches) == 1, (
        f"Expected 1 batch, got {[b.quantity for b in proj.active_batches]}"
    )
    batch = proj.active_batches[0]
    assert batch.quantity == Decimal("10")
    assert batch.exit_order_submitted_at is None
    assert proj.total_active_quantity == Decimal("10")


def test_position_ledger_nested_raw_payload_position_side_matches_and_closes() -> None:
    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="牛来USDT",
        position_side=FuturesPositionSide.LONG,
    )
    t0 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    f1 = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="牛来USDT",
        trade_id="t1",
        order_id="o1",
        side="BUY",
        price=Decimal("0.11"),
        quantity=Decimal("100"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=t0,
        raw_payload={"positionSide": "LONG"},
    )
    f2 = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="牛来USDT",
        trade_id="t2",
        order_id="o2",
        side="SELL",
        price=Decimal("0.12"),
        quantity=Decimal("100"),
        realized_pnl=Decimal("1"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=t0 + timedelta(minutes=1),
        raw_payload={"row": {"ps": "LONG"}},
    )
    facts = AccountFacts(position_key=key, fills=(f1, f2))
    proj = PositionLedger(key).project(facts)
    assert proj.total_active_quantity == Decimal("0")
    assert len(proj.active_batches) == 0
    assert not proj.diagnostics


@pytest.mark.parametrize("client_id", [None, "entry-client", "", 12, True, {}, []])
def test_payload_client_order_id_is_optional_text_without_changing_quantity(client_id):
    key = _key()
    fill = _fill(
        "client-id-trade", "BUY", "2", "100", datetime(2026, 9, 30, tzinfo=UTC)
    )
    fill = replace(fill, raw_payload={"is_system": True, "client_order_id": client_id})
    projection = PositionLedger(key).project(
        AccountFacts(position_key=key, fills=(fill,))
    )
    assert projection.total_active_quantity == Decimal("2")
    assert len(projection.active_batches) == 1
    expected = client_id if isinstance(client_id, str) else None
    assert projection.active_batches[0].client_order_id == expected


@pytest.mark.parametrize("entry_side,exit_side", [("BUY", "SELL"), ("SELL", "BUY")])
@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize("match_client_id", [False, True])
def test_new_batch_exit_filling_first_does_not_reduce_old_batch(
    entry_side: str, exit_side: str, restart: bool, match_client_id: bool
) -> None:
    key = _key()
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="hub", stream_epoch="targeted-exit"
    )
    ledger = PositionLedger(key)
    t0 = datetime(2026, 10, 8, tzinfo=UTC)
    first = _fill("open-old", entry_side, "10", "100", t0, order_id="open-old")
    old_id = (
        ledger.project(AccountFacts(position_key=key, fills=(first,)))
        .active_batches[0]
        .batch_id
    )
    old_exit = ExitOrderSubmissionFact(
        order_id="exit-old",
        submitted_at=t0 + timedelta(seconds=1),
        symbol=key.symbol,
        position_side=key.position_side,
        target_batch_id=old_id,
    )
    second = _fill(
        "open-new",
        entry_side,
        "5",
        "110",
        t0 + timedelta(seconds=2),
        order_id="open-new",
    )
    initial = ledger.project(
        AccountFacts(
            position_key=key, fills=(first, second), exit_boundaries=(old_exit,)
        )
    )
    new_id = initial.active_batches[1].batch_id
    new_exit = replace(
        old_exit,
        order_id="exit-new",
        submitted_at=t0 + timedelta(seconds=3),
        target_batch_id=new_id,
        client_order_id="client-exit-new",
    )
    new_fill = _fill(
        "close-new",
        exit_side,
        "5",
        "120",
        t0 + timedelta(seconds=4),
        order_id="exit-new",
    )
    if match_client_id:
        new_fill = replace(
            new_fill,
            order_id="exchange-exit-new",
            raw_payload={"is_system": True, "client_order_id": "client-exit-new"},
        )
    prefix = AccountFacts(
        position_key=key,
        stream_scope=scope,
        fills=(first, second),
        exit_boundaries=(old_exit, new_exit),
    )
    checkpoint = None
    if restart:
        checkpoint = ledger.create_recovery_checkpoint(
            prefix, source_revision=4, event_cut=new_exit.submitted_at
        )
        checkpoint = PositionRecoveryCodec.decode_checkpoint(
            PositionRecoveryCodec.encode_checkpoint(checkpoint)
        )
    projection = ledger.project(
        AccountFacts(
            position_key=key,
            stream_scope=scope,
            fills=(new_fill,) if restart else (first, second, new_fill),
            exit_boundaries=(old_exit, new_exit),
            recovery_checkpoint=checkpoint,
            prefix_facts_complete=not restart,
        )
    )
    assert [(b.batch_id, b.quantity) for b in projection.active_batches] == [
        (old_id, Decimal("10"))
    ]
    old_fill = _fill(
        "close-old",
        exit_side,
        "3",
        "120",
        t0 + timedelta(seconds=5),
        order_id="exit-old",
    )
    later = ledger.project(
        AccountFacts(
            position_key=key,
            stream_scope=scope,
            fills=(new_fill, old_fill)
            if restart
            else (first, second, new_fill, old_fill),
            exit_boundaries=(old_exit, new_exit),
            recovery_checkpoint=checkpoint,
            prefix_facts_complete=not restart,
        )
    )
    assert [(b.batch_id, b.quantity) for b in later.active_batches] == [
        (old_id, Decimal("7"))
    ]
    oversize = ledger.project(
        replace(
            prefix,
            fills=(first, second, replace(new_fill, quantity=Decimal("6"))),
        )
    )
    assert not oversize.is_comparable
    assert [(b.batch_id, b.quantity) for b in oversize.active_batches] == [
        (old_id, Decimal("10"))
    ]
    assert any("exceeds its target" in message for message in oversize.diagnostics)


def test_repeated_targeted_exit_boundary_never_starts_another_batch() -> None:
    key = _key()
    opened_at = datetime(2026, 10, 4, 0, 0, tzinfo=UTC)
    first_exit_at = opened_at + timedelta(minutes=15)
    fills = (
        _fill("entry-1", "BUY", "1", "100", opened_at, order_id="entry-1"),
        _fill(
            "entry-2",
            "BUY",
            "2",
            "100",
            opened_at + timedelta(minutes=20),
            order_id="entry-2",
        ),
    )
    ledger = PositionLedger(key)
    initial = ledger.project(AccountFacts(position_key=key, fills=fills))
    target = initial.active_batches[0].batch_id
    boundary = ExitOrderSubmissionFact(
        order_id="exit-1",
        submitted_at=first_exit_at,
        symbol=key.symbol,
        position_side=key.position_side,
        target_batch_id=target,
    )
    projection = ledger.project(
        AccountFacts(
            position_key=key,
            fills=fills,
            exit_boundaries=(
                boundary,
                replace(
                    boundary,
                    order_id="exit-2",
                    submitted_at=opened_at + timedelta(minutes=45),
                ),
            ),
        )
    )
    assert projection.active_batches[0].exit_order_submitted_at == first_exit_at
    assert projection.active_batches[1].exit_order_submitted_at is None
