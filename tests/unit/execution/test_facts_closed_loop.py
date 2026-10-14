"""Unit tests for Phase P1 Facts Closed Loop per architecture RFC 2026-09-25.

Validates:
1. Fact coverage completeness, gap detection, and historical truncation;
2. Revocation of authority from synthetic fills;
3. Fill identity conflicts and late fill handling in the current position state;
4. Point-in-time PositionBook view projection and freshness constraints;
5. End-to-end AKE external close and fresh lifecycle reconstruction.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    DiscrepancyKind,
    FactCoverageInterval,
    FactCoverageStatus,
    FreshnessRequirement,
    PositionHealthStatus,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_models import (
    AccountFacts,
)


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
    account_label: str = "account-3",
) -> AccountFillEvent:
    return AccountFillEvent(
        environment="live",
        account_label=account_label,
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
    account_label: str = "account-3",
) -> AccountPositionSnapshot:
    return AccountPositionSnapshot(
        environment="live",
        account_label=account_label,
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


def test_position_ledger_applies_event_delta_to_current_projection() -> None:
    key = PositionKey("live", "account-3", "SANDUSDT", FuturesPositionSide.LONG)
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="user-data", stream_epoch="epoch-1"
    )
    fills = (
        _fill("t1", "3", "100", _dt(10, 0)),
        _fill("t2", "2", "110", _dt(10, 1)),
        _fill("t3", "1", "105", _dt(10, 2), side="SELL"),
    )
    ledger = PositionLedger(key)
    current = ledger.project(
        AccountFacts(position_key=key, fills=fills[:1], stream_scope=scope)
    )

    for fill in fills[1:]:
        current = ledger.apply_delta(
            current,
            AccountFacts(
                position_key=key,
                fills=(fill,),
                stream_scope=scope,
                prefix_facts_complete=False,
            ),
        )

    replayed = ledger.project(
        AccountFacts(position_key=key, fills=fills, stream_scope=scope)
    )
    assert current.active_episode == replayed.active_episode
    assert current.active_batches == replayed.active_batches
    assert current.total_active_quantity == Decimal("4")
    assert current.high_watermark_trade_at == fills[-1].trade_at


def test_position_ledger_does_not_apply_late_delta_to_current_projection() -> None:
    key = PositionKey("live", "account-3", "SANDUSDT", FuturesPositionSide.LONG)
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="user-data", stream_epoch="epoch-1"
    )
    ledger = PositionLedger(key)
    current = ledger.project(
        AccountFacts(
            position_key=key,
            fills=(_fill("t2", "2", "100", _dt(10, 2)),),
            stream_scope=scope,
        )
    )

    updated = ledger.apply_delta(
        current,
        AccountFacts(
            position_key=key,
            fills=(_fill("late", "1", "100", _dt(10, 1)),),
            stream_scope=scope,
            prefix_facts_complete=False,
        ),
    )

    assert updated.total_active_quantity == current.total_active_quantity
    assert updated.health_status == PositionHealthStatus.INCOMPLETE
    assert any("Late account facts" in item for item in updated.diagnostics)


def test_coverage_pending_status_downgrades_to_catching_up() -> None:
    key = PositionKey("live", "account-3", "SANDUSDT", FuturesPositionSide.LONG)
    t = _dt(10, 0)
    f = _fill("t1", "100", "1.5", t)
    s = _snapshot("100", "1.5", t)

    coverage = FactCoverageInterval(
        start_at=t - timedelta(minutes=5),
        end_at=t,
        has_known_gaps=False,
        status=FactCoverageStatus.PENDING,
    )
    facts = AccountFacts(
        position_key=key,
        fills=(f,),
        snapshots=(s,),
        coverage=coverage,
    )
    ledger = PositionLedger(key)
    proj = ledger.project(facts)

    assert proj.health_status == PositionHealthStatus.CATCHING_UP
    assert proj.is_comparable is False


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
    t = _dt(10, 0)

    f1 = _fill("t100", "50", "1.50", t)
    f1_dup = _fill("t100", "50", "1.50", t)
    f1_conflict = _fill("t100", "75", "1.50", t)  # Same trade_id, different quantity!

    assert f1 == f1_dup
    ledger = PositionLedger(key)
    conflicting_facts = AccountFacts(
        position_key=key,
        fills=(f1, f1_conflict),
        snapshots=(_snapshot("50", "1.50", t),),
    )
    proj = ledger.project(conflicting_facts)
    assert proj.health_status == PositionHealthStatus.CONFLICT
    assert proj.discrepancy is not None
    assert proj.discrepancy.kind == DiscrepancyKind.IDENTITY_MISMATCH


def test_position_book_late_fill_detection() -> None:
    key = PositionKey("live", "account-3", "SANDUSDT", FuturesPositionSide.LONG)
    book = PositionBook.from_facts(AccountFacts(position_key=key))

    t_early = _dt(10, 0)
    t_late = _dt(10, 10)
    t_middle = _dt(10, 5)

    book.apply_delta(
        AccountFacts(
            position_key=key,
            fills=(_fill("t1", "10", "1.0", t_early),),
            prefix_facts_complete=False,
        )
    )
    book.apply_delta(
        AccountFacts(
            position_key=key,
            fills=(_fill("t2", "10", "1.0", t_late),),
            prefix_facts_complete=False,
        )
    )
    assert not book.current_facts.has_late_events

    book.apply_delta(
        AccountFacts(
            position_key=key,
            fills=(_fill("t3", "10", "1.0", t_middle),),
            prefix_facts_complete=False,
        )
    )
    assert book.current_facts.has_late_events


def test_position_book_get_view_and_freshness_constraints() -> None:
    key = PositionKey("live", "account-3", "SANDUSDT", FuturesPositionSide.LONG)
    t = _dt(10, 0)
    facts = AccountFacts(
        position_key=key,
        fills=(_fill("t1", "500", "0.045", t),),
        snapshots=(_snapshot("500", "0.045", t),),
        coverage=FactCoverageInterval(
            start_at=t,
            end_at=t,
            status=FactCoverageStatus.CONFIRMED,
        ),
    )
    book = PositionBook.from_facts(facts)

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


def test_ake_external_close_and_reopen_lifecycle_in_current_position_book() -> None:
    """Verify an external AKEUSDT close and reopen in the position book."""
    key = PositionKey("live", "primary", "AKEUSDT", FuturesPositionSide.LONG)

    t_sep20_buy = datetime(2026, 9, 20, 10, 21, 15, tzinfo=UTC)
    t_sep20_sell = datetime(2026, 9, 20, 10, 31, 16, tzinfo=UTC)
    t_sep25_buy = datetime(2026, 9, 25, 4, 59, 17, tzinfo=UTC)

    # Sep 20 Buy 944 @ 0.105886
    buy_before_close = _fill(
            "t_sep20_buy",
            "944",
            "0.105886",
            t_sep20_buy,
            symbol="AKEUSDT",
            account_label="primary",
        )
    # Sep 20 External Sell 944 @ 0.103064 (position drops to 0)
    external_close = _fill(
            "t_sep20_sell",
            "944",
            "0.103064",
            t_sep20_sell,
            side="SELL",
            symbol="AKEUSDT",
            account_label="primary",
        )
    # Sep 25 New Buy 2618 @ 0.038197
    reopen = _fill(
            "t_sep25_buy",
            "2618",
            "0.038197",
            t_sep25_buy,
            symbol="AKEUSDT",
            account_label="primary",
        )
    current_snapshot = _snapshot(
            "2618",
            "0.038197",
            t_sep25_buy,
            symbol="AKEUSDT",
            account_label="primary",
        )
    facts = AccountFacts(
        position_key=key,
        fills=(buy_before_close, external_close, reopen),
        snapshots=(current_snapshot,),
        coverage=FactCoverageInterval(
            start_at=t_sep20_buy,
            end_at=t_sep25_buy,
            status=FactCoverageStatus.CONFIRMED,
        ),
    )

    book = PositionBook.from_facts(facts)
    view = book.get_view(now=t_sep25_buy)

    assert view.health_status == PositionHealthStatus.READY
    assert view.is_ready_for_trade is True
    assert view.total_quantity == Decimal("2618")
    assert len(view.batches) == 1
    # Entry price must be exactly 0.038197, NOT contaminated by Sep 20 0.105886!
    assert view.batches[0].entry_price == Decimal("0.038197")
    assert view.batches[0].quantity == Decimal("2618")
    assert view.reconciliation_gap == Decimal("0")
