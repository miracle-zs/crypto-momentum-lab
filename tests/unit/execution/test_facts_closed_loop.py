"""Unit tests for Phase P1 Facts Closed Loop per architecture RFC 2026-09-25.

Validates:
1. Fact coverage completeness, gap detection, and historical truncation;
2. Revocation of authority from synthetic fills;
3. Idempotency, conflict detection, and late fill handling in AccountJournal;
4. Point-in-time PositionBook view projection and freshness constraints;
5. End-to-end AKE external close and fresh lifecycle reconstruction.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.account_journal import (
    AccountFactEnvelope,
    AccountJournal,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFacts,
    DiscrepancyKind,
    FactCoverageInterval,
    FactCoverageStatus,
    FreshnessRequirement,
    PositionCheckpoint,
    PositionHealthStatus,
    PositionKey,
)
from crypto_momentum_lab.domain.strategy import StrategySide


def _dt(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2026, 9, 25, hour, minute, second, tzinfo=UTC)


def _fill(
    trade_id: str,
    qty: str,
    price: str,
    when: datetime,
    side: str = "BUY",
    symbol: str = "SANDUSDT",
    is_synthetic: bool = False,
) -> AccountFillEvent:
    return AccountFillEvent(
        environment="live",
        account_label="account-3",
        symbol=symbol,
        trade_id=trade_id,
        order_id="ord_1",
        side=side,
        price=Decimal(price),
        quantity=Decimal(qty),
        realized_pnl=Decimal("0"),
        fee=Decimal("0.01"),
        fee_asset="USDT",
        trade_at=when,
        raw_payload={
            "positionSide": "LONG",
            "synthetic_from_order": is_synthetic,
        },
    )


def _snapshot(
    qty: str,
    price: str,
    when: datetime,
    symbol: str = "SANDUSDT",
) -> AccountPositionSnapshot:
    return AccountPositionSnapshot(
        environment="live",
        account_label="account-3",
        symbol=symbol,
        position_side="LONG",
        position_amt=Decimal(qty),
        entry_price=Decimal(price),
        mark_price=Decimal(price),
        unrealized_pnl=Decimal("0"),
        notional=Decimal(qty) * Decimal(price),
        leverage=5,
        margin_type="cross",
        observed_at=when,
        raw_payload={},
    )


def test_coverage_gap_detected_emits_incomplete_status() -> None:
    key = PositionKey("live", "account-3", "SANDUSDT", FuturesPositionSide.LONG)
    t1 = _dt(10, 0)
    t2 = _dt(10, 5)
    f = _fill("t1", "100", "1.5", t1)
    s = _snapshot("100", "1.5", t2)

    coverage = FactCoverageInterval(
        start_at=t1 - timedelta(minutes=10),
        end_at=t2,
        has_known_gaps=True,
        status=FactCoverageStatus.GAP_DETECTED,
    )
    facts = AccountFacts(
        position_key=key,
        fills=(f,),
        snapshots=(s,),
        coverage=coverage,
    )
    ledger = PositionLedger(key)
    proj = ledger.project(facts)

    assert proj.health_status == PositionHealthStatus.INCOMPLETE
    assert proj.discrepancy is not None
    assert proj.discrepancy.kind == DiscrepancyKind.INPUT_MISSING
    assert "known gaps" in proj.discrepancy.details


def test_coverage_truncated_before_episode_opened_emits_incomplete() -> None:
    key = PositionKey("live", "account-3", "SANDUSDT", FuturesPositionSide.LONG)
    t_open = _dt(10, 0)
    t_cov_start = _dt(10, 10)  # Coverage starts 10 minutes AFTER position opened
    t_now = _dt(10, 15)

    f = _fill("t1", "100", "1.5", t_open)
    s = _snapshot("100", "1.5", t_now)

    coverage = FactCoverageInterval(
        start_at=t_cov_start,
        end_at=t_now,
        has_known_gaps=False,
        status=FactCoverageStatus.CONFIRMED,
    )
    facts = AccountFacts(
        position_key=key,
        fills=(f,),
        snapshots=(s,),
        coverage=coverage,
    )
    ledger = PositionLedger(key)
    proj = ledger.project(facts)

    assert proj.health_status == PositionHealthStatus.INCOMPLETE
    assert proj.discrepancy is not None
    assert proj.discrepancy.kind == DiscrepancyKind.INPUT_MISSING
    assert "does not cover" in proj.discrepancy.details


def test_synthetic_fills_downgraded_and_denied_authoritative_status() -> None:
    key = PositionKey("live", "account-3", "SANDUSDT", FuturesPositionSide.LONG)
    t = _dt(10, 0)
    syn_f = _fill("syn_t1", "100", "1.5", t, is_synthetic=True)
    s = _snapshot("100", "1.5", t)

    facts = AccountFacts(
        position_key=key,
        fills=(syn_f,),
        snapshots=(s,),
        has_synthetic_fills=True,
    )
    ledger = PositionLedger(key)
    proj = ledger.project(facts)

    assert proj.health_status == PositionHealthStatus.INCOMPLETE
    assert proj.is_comparable is False
    assert proj.discrepancy is not None
    assert proj.discrepancy.kind == DiscrepancyKind.INPUT_MISSING
    assert "Synthetic fills present" in proj.discrepancy.details


def test_account_journal_deduplication_and_conflict_detection() -> None:
    key = PositionKey("live", "account-3", "SANDUSDT", FuturesPositionSide.LONG)
    journal = AccountJournal(key)
    t = _dt(10, 0)

    f1 = _fill("t100", "50", "1.50", t)
    f1_dup = _fill("t100", "50", "1.50", t)
    f1_conflict = _fill("t100", "75", "1.50", t)  # Same trade_id, different quantity!

    # 1. First append succeeds
    assert journal.append_fill(f1) is True
    assert not journal.has_conflicts

    # 2. Idempotent duplicate returns False without conflict
    assert journal.append_fill(f1_dup) is False
    assert not journal.has_conflicts

    # 3. Divergent trade_id returns False and triggers conflict
    assert journal.append_fill(f1_conflict) is False
    assert journal.has_conflicts

    # 4. Reading facts and replaying reveals conflict
    facts = journal.read_cut()
    ledger = PositionLedger(key)
    # Add conflicting fill to facts to simulate persisted conflict
    conflicting_facts = AccountFacts(
        position_key=key,
        fills=(f1, f1_conflict),
        snapshots=(_snapshot("50", "1.50", t),),
    )
    proj = ledger.project(conflicting_facts)
    assert proj.health_status == PositionHealthStatus.CONFLICT
    assert proj.discrepancy is not None
    assert proj.discrepancy.kind == DiscrepancyKind.IDENTITY_MISMATCH


def test_account_journal_late_fill_detection() -> None:
    key = PositionKey("live", "account-3", "SANDUSDT", FuturesPositionSide.LONG)
    journal = AccountJournal(key)

    t_early = _dt(10, 0)
    t_late = _dt(10, 10)
    t_middle = _dt(10, 5)

    journal.append_fill(_fill("t1", "10", "1.0", t_early))
    journal.append_fill(_fill("t2", "10", "1.0", t_late))
    assert not journal.has_late_events

    # Late fill arrives timestamped between t_early and t_late
    journal.append_fill(_fill("t3", "10", "1.0", t_middle))
    assert journal.has_late_events


def test_position_book_get_view_and_freshness_constraints() -> None:
    key = PositionKey("live", "account-3", "SANDUSDT", FuturesPositionSide.LONG)
    journal = AccountJournal(key)
    t = _dt(10, 0)

    journal.append_fill(_fill("t1", "500", "0.045", t))
    journal.record_snapshot(_snapshot("500", "0.045", t))

    book = PositionBook(journal)

    # 1. Fresh view within 15 seconds
    view = book.get_view(now=t + timedelta(seconds=5))
    assert view.health_status == PositionHealthStatus.READY
    assert view.is_ready_for_trade is True
    assert view.total_quantity == Decimal("500")

    # 2. Stale view beyond 15 seconds
    view_stale = book.get_view(
        requirement=FreshnessRequirement(max_staleness=timedelta(seconds=10)),
        now=t + timedelta(seconds=20),
    )
    assert view_stale.health_status == PositionHealthStatus.CATCHING_UP
    assert view_stale.is_ready_for_trade is False


def test_ake_external_close_and_reopen_lifecycle_via_journal_and_book() -> None:
    """Full architectural verification of AKEUSDT scenario through AccountJournal and PositionBook."""
    key = PositionKey("live", "primary", "AKEUSDT", FuturesPositionSide.LONG)
    journal = AccountJournal(key)

    t_sep20_buy = datetime(2026, 9, 20, 10, 21, 15, tzinfo=UTC)
    t_sep20_sell = datetime(2026, 9, 20, 10, 31, 16, tzinfo=UTC)
    t_sep25_buy = datetime(2026, 9, 25, 4, 59, 17, tzinfo=UTC)

    # Sep 20 Buy 944 @ 0.105886
    journal.append_fill(
        _fill("t_sep20_buy", "944", "0.105886", t_sep20_buy, symbol="AKEUSDT")
    )
    # Sep 20 External Sell 944 @ 0.103064 (position drops to 0)
    journal.append_fill(
        _fill("t_sep20_sell", "944", "0.103064", t_sep20_sell, side="SELL", symbol="AKEUSDT")
    )
    # Sep 25 New Buy 2618 @ 0.038197
    journal.append_fill(
        _fill("t_sep25_buy", "2618", "0.038197", t_sep25_buy, symbol="AKEUSDT")
    )
    journal.record_snapshot(
        _snapshot("2618", "0.038197", t_sep25_buy, symbol="AKEUSDT")
    )

    # Verify zero crossing discovery
    zero_cross = journal.find_latest_zero_crossing()
    assert zero_cross == t_sep20_sell

    # Project view via PositionBook
    book = PositionBook(journal)
    view = book.get_view(now=t_sep25_buy)

    assert view.health_status == PositionHealthStatus.READY
    assert view.is_ready_for_trade is True
    assert view.total_quantity == Decimal("2618")
    assert len(view.batches) == 1
    # Entry price must be exactly 0.038197, NOT contaminated by Sep 20 0.105886!
    assert view.batches[0].entry_price == Decimal("0.038197")
    assert view.batches[0].quantity == Decimal("2618")
    assert view.reconciliation_gap == Decimal("0")
