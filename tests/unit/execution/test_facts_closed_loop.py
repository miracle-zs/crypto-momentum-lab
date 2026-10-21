"""Unit tests for recovery fact coverage and authority contracts."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    DiscrepancyKind,
    FactCoverageInterval,
    FactCoverageStatus,
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
