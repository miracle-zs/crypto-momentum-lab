"""Unit and regression tests for Astra Review Round 6 fixes (2026-09-24).

Verifies the 4 critical findings from round 6 review:
1. Sub-grid short trades (entering and exiting within a single 15s sampling tick)
   accurately track margin and position concurrency, enforcing margin caps
   across both fast MTM metrics and full equity curve evaluations.
2. Scheduled risk window carry-in position modifications are preserved and
   propagated to admitted_trades and MTM evaluation, ensuring terminal equity
   matches ledger state_out.
3. Dict-to-TradeRecord conversion handles standard datetime instances properly
   (not leaving them open) and calculates missing PnL dynamically instead of
   defaulting to 0.0.
4. Compounding evaluation properly updates state_out so that
   terminal_equity == total_equity_mtm and active_positions reflect
   compounded notionals.
"""

from __future__ import annotations

from datetime import UTC, datetime, time, timedelta

import pytest

from crypto_momentum_lab.live_rollout.scheduled_risk_window import (
    ScheduledRiskWindowConfig,
)
from local_optimization.evaluation_context import (
    EvaluationContext,
    ScenarioSpec,
    evaluate_candidate,
    to_trade_records,
)
from local_optimization.mtm_engine import TradeRecord
from local_optimization.opportunity import OpportunityStatus, RawOpportunity
from local_optimization.simulation_ledger import PortfolioState


def _make_opp(
    start: datetime,
    entry: datetime,
    exit_time: datetime,
    exit_price: float,
    symbol: str = "BTCUSDT",
    entry_price: float = 100.0,
    opp_id: str = "opp_1",
) -> RawOpportunity:
    return RawOpportunity(
        opportunity_id=opp_id,
        symbol=symbol,
        direction="LONG",
        detected_at=start,
        detected_epoch=start.timestamp(),
        entry_eligible_at=entry,
        entry_reference_price=entry_price,
        impulse_window_buckets=2,
        confirmation_buckets=1,
        impulse_return_pct=0.8,
        aggressive_imbalance=0.4,
        confirmation_min_imbalance=0.4,
        notional_intensity=3.5,
        volume_ratio=1.5,
        exit_time=exit_time,
        exit_price=exit_price,
        status=OpportunityStatus.DETECTED,
    )


def test_sub_grid_short_trades_margin_cap_enforced() -> None:
    """Verify that trades lasting <15s and falling between 15s grid points

    are accurately tracked for peak margin, and violate margin_cap as expected.
    """
    start = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    short_end = start + timedelta(minutes=1)

    # Trade lasts 1 second (10:00:01 to 10:00:02), falls within the first 15s bucket
    brief = _make_opp(
        start=start,
        entry=start + timedelta(seconds=1),
        exit_time=start + timedelta(seconds=2),
        exit_price=110.0,
    )
    short_prices = {
        "BTCUSDT": ([start.timestamp(), short_end.timestamp()], [100.0, 110.0])
    }

    # Test both fast evaluation (curve=False) and full curve (curve=True)
    for curve in (False, True):
        ctx = EvaluationContext(start, short_end, short_prices, events=[brief])
        # Compounding f=0.5 on 1000 equity: notional 500U, margin = 100U at 5x lev
        res = evaluate_candidate(
            ctx,
            {},
            ScenarioSpec(compounding=True, f=0.5, margin_cap=30.0),
            include_curve=curve,
        )

        assert res.peak_margin == 100.0
        assert res.admitted_trades[0].initial_margin_usdt == 100.0
        assert res.is_feasible is False
        assert "exceeds cap 30.00" in (res.infeasible_reason or "")


def test_overlapping_sub_grid_trades_accumulate_peak_margin() -> None:
    """Verify multiple concurrent sub-grid trades accurately sum their peak margin."""
    start = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    short_end = start + timedelta(minutes=1)

    # Trade 1: 10:00:01 to 10:00:05 (margin 20U at 100U notional)
    # Trade 2: 10:00:03 to 10:00:07 (margin 20U at 100U notional)
    # Overlap between 10:00:03 and 10:00:05 should reach 40U
    opp1 = _make_opp(
        start,
        start + timedelta(seconds=1),
        start + timedelta(seconds=5),
        110.0,
        symbol="BTCUSDT",
        opp_id="o1",
    )
    opp2 = _make_opp(
        start,
        start + timedelta(seconds=3),
        start + timedelta(seconds=7),
        110.0,
        symbol="ETHUSDT",
        opp_id="o2",
    )
    prices = {
        "BTCUSDT": ([start.timestamp(), short_end.timestamp()], [100.0, 110.0]),
        "ETHUSDT": ([start.timestamp(), short_end.timestamp()], [100.0, 110.0]),
    }

    ctx = EvaluationContext(start, short_end, prices, events=[opp1, opp2])
    res = evaluate_candidate(
        ctx,
        {"max_open_positions": 2},
        ScenarioSpec(compounding=False, margin_cap=50.0),
        include_curve=False,
    )
    assert res.peak_margin == 40.0
    assert res.is_feasible is True

    # Under compounding f=0.5 (scaled to 100U margin each), concurrent peak is 200U
    # Ledger admits both (20+20=40 <= 150), downstream MTM detects 200U > 150U
    res_tight = evaluate_candidate(
        ctx,
        {"max_open_positions": 2},
        ScenarioSpec(compounding=True, f=0.5, margin_cap=150.0),
        include_curve=False,
    )
    assert res_tight.peak_margin == 200.0
    assert res_tight.is_feasible is False
    assert "exceeds cap 150.00" in (res_tight.infeasible_reason or "")


def test_scheduled_risk_window_carry_in_exit_propagated() -> None:
    """Verify carry-in positions flattened by scheduled risk window

    are reflected in admitted_trades and terminal equity matches state_out.
    """
    start = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    end = start + timedelta(hours=1)
    prices = {
        "BTCUSDT": (
            [
                start.timestamp(),
                (start + timedelta(minutes=30)).timestamp(),
                end.timestamp(),
            ],
            [100.0, 105.0, 120.0],
        )
    }

    # Position entered at 09:00 at 100.0, originally scheduled to exit at 12:00 at 130.0
    carry = TradeRecord(
        trade_id="carry",
        symbol="BTCUSDT",
        entry_time=start - timedelta(hours=1),
        entry_price=100.0,
        exit_time=end + timedelta(hours=1),
        exit_price=130.0,
        notional_usdt=100.0,
        leverage=5.0,
        fee_rate=0.0005,
        slippage_rate=0.0002,
    )
    state = PortfolioState.create(start, 1000.0, 999.93, (carry,))

    # Scheduled risk window flattens at 10:30 (when price is 105.0)
    risk = ScheduledRiskWindowConfig(
        timezone="UTC",
        entry_stop_at=time(10, 30),
        flatten_start_at=time(10, 30),
        flatten_deadline_at=time(10, 30, 15),
        verify_at=time(10, 30, 30),
        reopen_at=time(10, 31),
    )

    for curve in (False, True):
        ctx = EvaluationContext(
            start, end, prices, state_in=state, scheduled_risk_window=risk
        )
        res = evaluate_candidate(ctx, {}, include_curve=curve)

        # Expected terminal equity: 1000 cash - fees + gain from 100 to 105
        # Entry fee (100*0.0007) + exit fee (105*0.0007) = 0.1435
        # Net gain: 5.0 - 0.1435 = 4.8565 -> Total equity: 1004.8565
        assert pytest.approx(res.terminal_equity, rel=1e-5) == 1004.8565
        assert pytest.approx(res.state_out.total_equity_mtm, rel=1e-5) == 1004.8565
        assert res.terminal_equity == res.state_out.total_equity_mtm

        # Trade should have been flattened at 10:30 at 105.0, not 12:00 at 120.0
        admitted = res.admitted_trades[0]
        assert admitted.exit_time == start + timedelta(minutes=30)
        assert admitted.exit_price == 105.0
        assert len(res.state_out.active_positions) == 0


def test_dict_conversion_datetime_and_missing_pnl() -> None:
    """Verify to_trade_records converts datetime objects and computes missing PnL."""
    start = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    end = datetime(2026, 9, 1, 11, 0, tzinfo=UTC)

    # 1. Native datetime in exit_at should not be left open
    closed = {
        "symbol": "BTCUSDT",
        "entry_at": start,
        "entry_price": 100.0,
        "exit_at": end,
        "exit_price": 110.0,
        "net_pnl_usdt": 9.86,
    }
    converted = to_trade_records([closed])[0]
    assert converted.exit_time == end
    assert converted.is_open is False

    # 2. Missing net_pnl_usdt should be calculated dynamically, NOT coerced to 0.0
    missing_pnl = dict(closed)
    del missing_pnl["net_pnl_usdt"]
    converted_missing = to_trade_records([missing_pnl])[0]

    expected_tr = TradeRecord(
        trade_id="expected",
        symbol="BTCUSDT",
        entry_time=start,
        entry_price=100.0,
        exit_time=end,
        exit_price=110.0,
        fee_rate=0.0005,
        slippage_rate=0.0002,
    )
    assert converted_missing.calculated_net_pnl == pytest.approx(
        expected_tr.calculated_net_pnl, rel=1e-5
    )
    assert converted_missing.calculated_net_pnl == pytest.approx(9.853, rel=1e-3)


def test_compounding_state_out_synchronization() -> None:
    """Verify that compounding evaluations synchronize state_out so that

    terminal_equity matches total_equity_mtm and active_positions retain notional.
    """
    start = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    end = start + timedelta(hours=1)
    prices = {
        "BTCUSDT": (
            [
                start.timestamp(),
                (start + timedelta(minutes=30)).timestamp(),
                end.timestamp(),
            ],
            [100.0, 110.0, 120.0],
        )
    }

    # Position enters at 10:01 and remains open past window_end (until 12:00)
    new_opp = _make_opp(
        start=start,
        entry=start + timedelta(minutes=1),
        exit_time=end + timedelta(hours=1),
        exit_price=130.0,
    )

    ctx = EvaluationContext(start, end, prices, events=[new_opp])
    # f=0.2 on 1000 initial equity scales base 100 notional to 200 notional
    res = evaluate_candidate(ctx, {}, ScenarioSpec(compounding=True, f=0.2))

    # Check terminal equity matches state_out.total_equity_mtm
    assert res.terminal_equity == pytest.approx(
        res.state_out.total_equity_mtm, rel=1e-5
    )
    assert res.terminal_equity == pytest.approx(1039.86, rel=1e-3)

    # Check admitted trades notional was scaled to 200.0
    assert res.admitted_trades[0].notional_usdt == 200.0

    # Check state_out.active_positions also scaled to 200.0 (not reverting to 100.0)
    assert len(res.state_out.active_positions) == 1
    assert res.state_out.active_positions[0].notional_usdt == 200.0
