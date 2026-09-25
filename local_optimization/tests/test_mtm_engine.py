"""Unit tests for continuous mark-to-market replay engine."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from local_optimization.mtm_engine import (
    TradeRecord,
    evaluate_legacy_trade_level_curve,
    reconstruct_mtm_equity,
)


def test_mtm_captures_intra_trade_floating_loss() -> None:
    """Validate that MTM captures intra-trade floating losses
    that legacy replay misses.
    """
    t0 = datetime(2026, 9, 14, 10, 0, tzinfo=UTC)
    t1 = t0 + timedelta(minutes=5)
    t2 = t0 + timedelta(minutes=10)

    # Trade enters at 10:00 at 100.0, exits at 10:10 at 105.0 (+5% gross, +4.9U net)
    trade = TradeRecord(
        trade_id="T001",
        symbol="BTCUSDT",
        entry_time=t0,
        entry_price=100.0,
        exit_time=t2,
        exit_price=105.0,
        notional_usdt=100.0,
        net_pnl_usdt=4.9,
    )

    # 1. Legacy trade-level curve:
    # Only records at exit (10:10). Sees 0 drawdown!
    legacy_metrics = evaluate_legacy_trade_level_curve([trade], initial_equity=1000.0)
    assert legacy_metrics.max_drawdown_usdt == pytest.approx(0.0)
    assert legacy_metrics.ulcer_index == pytest.approx(0.0)

    # 2. Continuous price series: price plunged to 80.0 at 10:05 (-20% floating loss!)
    # then bounced back to 105.0 at 10:10.
    price_series = {
        "BTCUSDT": (
            [t0.timestamp(), t1.timestamp(), t2.timestamp()],
            [100.0, 80.0, 105.0],
        )
    }

    mtm_points = reconstruct_mtm_equity(
        trades=[trade],
        price_series=price_series,
        initial_equity=1000.0,
        grid_seconds=15,
    )

    from local_optimization.equity import evaluate_equity_curve

    mtm_metrics = evaluate_equity_curve(mtm_points, initial_equity=1000.0)

    # Net PnL is identical (+4.9U)
    assert mtm_metrics.net_pnl == pytest.approx(legacy_metrics.net_pnl, abs=0.2)
    # BUT MTM accurately uncovers the massive floating drawdown (~20U drop)!
    assert mtm_metrics.max_drawdown_usdt >= 20.0
    assert mtm_metrics.ulcer_index > 0.005


def test_concurrent_active_trades_margin_and_floating_pnl() -> None:
    """Test that concurrent trades correctly aggregate margin and floating PnL."""
    t0 = datetime(2026, 9, 14, 10, 0, tzinfo=UTC)
    t1 = t0 + timedelta(minutes=10)

    # Two concurrent trades on different symbols
    t_btc = TradeRecord(
        trade_id="T_BTC",
        symbol="BTCUSDT",
        entry_time=t0,
        entry_price=100.0,
        exit_time=t1,
        exit_price=100.0,
        notional_usdt=100.0,
        leverage=5.0,
    )
    t_eth = TradeRecord(
        trade_id="T_ETH",
        symbol="ETHUSDT",
        entry_time=t0,
        entry_price=200.0,
        exit_time=t1,
        exit_price=200.0,
        notional_usdt=100.0,
        leverage=5.0,
    )

    points = reconstruct_mtm_equity(
        trades=[t_btc, t_eth],
        price_series={},
        initial_equity=1000.0,
        grid_seconds=60,
    )

    # While both are open, active positions should be 2, margin should be 40U (20U each)
    mid_point = points[5]
    assert mid_point.active_positions == 2
    assert mid_point.peak_initial_margin == pytest.approx(40.0)
