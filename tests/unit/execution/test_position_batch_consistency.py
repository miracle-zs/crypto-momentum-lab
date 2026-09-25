"""P0 regression tests for position batch consistency per architecture RFC 2026-09-25.

Tests:
1. SAND mixed-cut reconciliation consistency (308ms race condition avoidance);
2. AKE external close episode isolation (pre-zero lot does not contaminate entry price);
3. PositionHealthStatus & DiscrepancyKind state transitions and deterministic audit hashing;
4. True discrepancy detection vs transient stream lag.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFacts,
    DiscrepancyKind,
    PositionDiscrepancy,
    PositionHealthStatus,
    PositionKey,
)
from crypto_momentum_lab.domain.strategy import StrategySide
from crypto_momentum_lab.live_rollout.position_ledger_shadow import (
    PositionLedgerShadowComparator,
    ShadowDiffCategory,
)


def _at(time_str: str) -> datetime:
    return datetime.fromisoformat(f"2026-09-25T07:25:{time_str}+00:00")


def _fill(
    trade_id: str,
    quantity: str,
    when: str,
    symbol: str = "SANDUSDT",
    side: str = "BUY",
    price: str = "0.04568",
    is_system: bool = True,
) -> AccountFillEvent:
    return AccountFillEvent(
        environment="live",
        account_label="account-3",
        symbol=symbol,
        trade_id=trade_id,
        order_id="23149283042",
        side=side,
        price=Decimal(price),
        quantity=Decimal(quantity),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=_at(when),
        raw_payload={"positionSide": "LONG", "is_system": is_system},
    )


def _snapshot(
    quantity: str,
    when: str,
    symbol: str = "SANDUSDT",
) -> AccountPositionSnapshot:
    qty = Decimal(quantity)
    price = Decimal("0.04568")
    return AccountPositionSnapshot(
        environment="live",
        account_label="account-3",
        symbol=symbol,
        position_side="LONG",
        position_amt=qty,
        entry_price=price,
        mark_price=price,
        unrealized_pnl=Decimal("0"),
        notional=qty * price,
        leverage=5,
        margin_type="cross",
        observed_at=_at(when),
        raw_payload={},
    )


def test_sand_mixed_cut_reconciliation_consistency() -> None:
    """Validate that temporal discrepancies in partial fills do not generate spurious gaps.

    Reproduces the exact SANDUSDT on-site sequence:
    - 07:25:20.591: BUY 1403
    - 07:25:20.592: Exchange snapshot 1403 (observed at 20.594640)
    - 07:25:20.899: BUY 786 (total 2189)
    - 07:25:20.899: Exchange snapshot 2189 (observed at 20.902586)
    """
    key = PositionKey("live", "account-3", "SANDUSDT", FuturesPositionSide.LONG)
    first = _fill("1000604297", "1403", "20.591")
    second = _fill("1000604298", "786", "20.899")
    early = _snapshot("1403", "20.594640")
    late = _snapshot("2189", "20.902586")

    ledger = PositionLedger(key)

    # 1. Aligned early cut
    proj_early = ledger.project(AccountFacts(key, (first,), (early,)))
    assert proj_early.total_active_quantity == Decimal("1403")
    assert proj_early.reconciliation_gap == Decimal("0")
    assert proj_early.health_status == PositionHealthStatus.READY
    assert proj_early.is_comparable is True

    # 2. Aligned late cut
    proj_late = ledger.project(AccountFacts(key, (first, second), (late,)))
    assert proj_late.total_active_quantity == Decimal("2189")
    assert proj_late.reconciliation_gap == Decimal("0")
    assert proj_late.health_status == PositionHealthStatus.READY
    assert proj_late.is_comparable is True

    # 3. Mixed cut: earlier snapshot held while both fills present in DB
    proj_mixed = ledger.project(AccountFacts(key, (first, second), (early,)))
    assert proj_mixed.total_active_quantity == Decimal("2189")
    # Must NOT report spurious -786 gap!
    assert proj_mixed.reconciliation_gap == Decimal("0")
    assert proj_mixed.health_status == PositionHealthStatus.CATCHING_UP
    assert proj_mixed.is_comparable is False


def test_sand_true_cut_divergence_detected() -> None:
    """Validate that when cut quantity does NOT match snapshot, CONFLICT is emitted."""
    key = PositionKey("live", "account-3", "SANDUSDT", FuturesPositionSide.LONG)
    # Only 1000 filled, but snapshot says 1403
    first_corrupt = _fill("1000604297", "1000", "20.591")
    second = _fill("1000604298", "786", "20.899")
    early = _snapshot("1403", "20.594640")

    ledger = PositionLedger(key)
    proj = ledger.project(AccountFacts(key, (first_corrupt, second), (early,)))

    # At cut 20.594640, cut_qty=1000 != 1403, gap=403
    assert proj.reconciliation_gap == Decimal("403")
    assert proj.health_status == PositionHealthStatus.CONFLICT
    assert proj.is_comparable is True
    assert proj.discrepancy is not None
    assert proj.discrepancy.kind == DiscrepancyKind.QUANTITY_MISMATCH


def test_ake_external_close_episode_isolation() -> None:
    """Validate that an external close completely bounds an episode and prevents cost contamination.

    Reproduces the exact AKEUSDT scenario:
    - 09-20 10:21: BUY 944 @ 0.105886
    - 09-20 10:31: External SELL 944 @ 0.150294 (positions drops to 0)
    - 09-25 04:59: BUY 2618 @ 0.038197
    """
    key = PositionKey("live", "primary", "AKEUSDT", FuturesPositionSide.LONG)
    t_sep20_buy = datetime(2026, 9, 20, 10, 21, 15, tzinfo=UTC)
    t_sep20_sell = datetime(2026, 9, 20, 10, 31, 16, tzinfo=UTC)
    t_sep25_buy = datetime(2026, 9, 25, 4, 59, 17, tzinfo=UTC)

    f_old_buy = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="AKEUSDT",
        trade_id="trade_sep20_buy",
        order_id="ord_sep20_buy",
        side="BUY",
        price=Decimal("0.105886"),
        quantity=Decimal("944"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0.02"),
        fee_asset="USDT",
        trade_at=t_sep20_buy,
        raw_payload={"positionSide": "LONG", "is_system": True},
    )
    f_old_sell = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="AKEUSDT",
        trade_id="trade_sep20_sell",
        order_id="ord_sep20_ext_sell",
        side="SELL",
        price=Decimal("0.150294"),
        quantity=Decimal("944"),
        realized_pnl=Decimal("41.9"),
        fee=Decimal("0.02"),
        fee_asset="USDT",
        trade_at=t_sep20_sell,
        raw_payload={"positionSide": "LONG", "is_system": False},
    )
    f_new_buy = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="AKEUSDT",
        trade_id="trade_sep25_buy",
        order_id="ord_sep25_buy",
        side="BUY",
        price=Decimal("0.038197"),
        quantity=Decimal("2618"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0.02"),
        fee_asset="USDT",
        trade_at=t_sep25_buy,
        raw_payload={"positionSide": "LONG", "is_system": True},
    )

    ledger = PositionLedger(key)
    facts = AccountFacts(
        position_key=key,
        fills=(f_old_buy, f_old_sell, f_new_buy),
    )
    proj = ledger.project(facts)

    # 1. Total quantity must equal exactly the new lot
    assert proj.total_active_quantity == Decimal("2618")
    assert len(proj.active_batches) == 1

    # 2. Entry price must NOT be contaminated by 0.105886 (should be exactly 0.038197, NOT 0.0561359)
    active_batch = proj.active_batches[0]
    assert active_batch.quantity == Decimal("2618")
    assert active_batch.entry_price == Decimal("0.038197")
    assert active_batch.opened_at == t_sep25_buy

    # 3. Old episode must be strictly archived
    assert len(proj.archived_episodes) == 1
    assert proj.archived_episodes[0].cumulative_bought == Decimal("944")
    assert proj.archived_episodes[0].cumulative_sold == Decimal("944")
    assert proj.archived_episodes[0].closed_at == t_sep20_sell
    assert proj.health_status == PositionHealthStatus.READY


def test_position_health_status_and_discrepancy_model() -> None:
    """Validate structure and immutability of PositionHealthStatus and PositionDiscrepancy."""
    assert PositionHealthStatus.READY == "READY"
    assert PositionHealthStatus.CATCHING_UP == "CATCHING_UP"
    assert PositionHealthStatus.INCOMPLETE == "INCOMPLETE"
    assert PositionHealthStatus.CONFLICT == "CONFLICT"

    key = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.BOTH)
    now = datetime.now(UTC)

    disc = PositionDiscrepancy(
        discrepancy_id="disc_123",
        key=key,
        kind=DiscrepancyKind.QUANTITY_MISMATCH,
        first_seen_at=now,
        last_seen_at=now,
        count=1,
        input_hash="abc123hash",
        details="Quantity mismatch: ledger=10, snapshot=12",
        resolution_evidence=None,
    )
    assert disc.kind == DiscrepancyKind.QUANTITY_MISMATCH
    assert disc.is_reconciled is False
    assert disc.count == 1
