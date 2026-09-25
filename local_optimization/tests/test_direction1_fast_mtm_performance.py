"""Tests for Direction 1:
AlignedPriceGrid, FastMtmMetrics, and reconstruct_mtm_metrics_fast.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from local_optimization.equity import evaluate_equity_curve
from local_optimization.mtm_engine import (
    AlignedPriceGrid,
    TradeRecord,
    reconstruct_mtm_equity,
    reconstruct_mtm_metrics_fast,
)


def test_aligned_price_grid_construction_and_caching() -> None:
    """Verify AlignedPriceGrid builds exact timestamps and caches properly."""
    t0 = datetime(2026, 9, 20, 0, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(minutes=10)

    epochs = [t0.timestamp() + i * 15 for i in range(41)]
    prices = [100.0 + i * 0.5 for i in range(41)]
    raw_series = {"BTCUSDT": (epochs, prices)}

    grid = AlignedPriceGrid.build(raw_series, t0, t1, grid_seconds=15)
    assert len(grid.sampling_epochs) == 41
    assert "BTCUSDT" in grid.aligned_prices
    assert len(grid.aligned_prices["BTCUSDT"]) == 41
    assert grid.aligned_prices["BTCUSDT"][0] == 100.0
    assert grid.aligned_prices["BTCUSDT"][-1] == pytest.approx(120.0)

    # Calling build with existing matching grid should return the same object
    rebuilt = AlignedPriceGrid.build(grid, t0, t1, grid_seconds=15)
    assert rebuilt is grid


def test_fast_mtm_metrics_exact_equivalence_with_legacy_evaluation() -> None:
    """Verify reconstruct_mtm_metrics_fast produces mathematically identical results
    to reconstruct_mtm_equity + evaluate_equity_curve.
    """
    t0 = datetime(2026, 9, 20, 0, 0, 0, tzinfo=UTC)
    t_end = t0 + timedelta(minutes=30)

    trades = [
        TradeRecord(
            trade_id="T1",
            symbol="BTCUSDT",
            entry_time=t0 + timedelta(minutes=2),
            entry_price=50000.0,
            exit_time=t0 + timedelta(minutes=10),
            exit_price=51000.0,
            notional_usdt=100.0,
            fee_rate=0.0005,
            slippage_rate=0.0002,
            direction="LONG",
        ),
        TradeRecord(
            trade_id="T2",
            symbol="ETHUSDT",
            entry_time=t0 + timedelta(minutes=5),
            entry_price=3000.0,
            exit_time=t0 + timedelta(minutes=20),
            exit_price=2940.0,
            notional_usdt=200.0,
            fee_rate=0.0005,
            slippage_rate=0.0002,
            direction="SHORT",
        ),
        TradeRecord(
            trade_id="T3",
            symbol="BTCUSDT",
            entry_time=t0 + timedelta(minutes=15),
            entry_price=50500.0,
            exit_time=None,  # still open at window end
            notional_usdt=150.0,
            fee_rate=0.0005,
            slippage_rate=0.0002,
            direction="LONG",
            is_open=True,
        ),
    ]

    epochs = [t0.timestamp() + i * 15 for i in range(121)]
    btc_px = [50000.0 + (i % 10) * 50 - (i % 7) * 40 for i in range(121)]
    eth_px = [3000.0 - (i % 8) * 10 + (i % 5) * 8 for i in range(121)]
    price_series = {"BTCUSDT": (epochs, btc_px), "ETHUSDT": (epochs, eth_px)}

    # Legacy path
    pts = reconstruct_mtm_equity(
        trades=trades,
        price_series=price_series,
        initial_equity=1000.0,
        grid_seconds=15,
        start_time=t0,
        end_time=t_end,
    )
    legacy_m = evaluate_equity_curve(pts, initial_equity=1000.0)
    legacy_peak_margin = max((p.peak_initial_margin for p in pts), default=0.0)

    # Fast path
    grid = AlignedPriceGrid.build(price_series, t0, t_end, grid_seconds=15)
    fast_m = reconstruct_mtm_metrics_fast(
        trades=trades,
        price_series=grid,
        initial_equity=1000.0,
        grid_seconds=15,
        start_time=t0,
        end_time=t_end,
    )

    assert fast_m is not None
    assert fast_m.is_feasible is True
    assert fast_m.net_pnl == pytest.approx(legacy_m.net_pnl, abs=1e-4)
    assert fast_m.max_drawdown_usdt == pytest.approx(
        legacy_m.max_drawdown_usdt, abs=1e-4
    )
    assert fast_m.max_drawdown_pct == pytest.approx(legacy_m.max_drawdown_pct, abs=1e-4)
    assert fast_m.ulcer_index == pytest.approx(legacy_m.ulcer_index, abs=1e-5)
    assert fast_m.cdar_95 == pytest.approx(legacy_m.cdar_95, abs=1e-4)
    assert fast_m.peak_margin == pytest.approx(legacy_peak_margin, abs=1e-2)
    assert fast_m.terminal_equity == pytest.approx(pts[-1].equity, abs=1e-4)


def test_fast_mtm_metrics_early_rejection_margin_cap() -> None:
    """Verify that margin cap violations trigger immediate rejection."""
    t0 = datetime(2026, 9, 20, 0, 0, 0, tzinfo=UTC)
    trade = TradeRecord(
        trade_id="T_big",
        symbol="BTCUSDT",
        entry_time=t0,
        entry_price=100.0,
        exit_time=t0 + timedelta(minutes=5),
        exit_price=105.0,
        notional_usdt=500.0,
        leverage=5.0,  # margin = 100 USDT
    )
    price_series = {
        "BTCUSDT": (
            [t0.timestamp(), (t0 + timedelta(minutes=5)).timestamp()],
            [100.0, 105.0],
        )
    }

    # margin_cap=50 < 100 -> should reject (return None)
    res = reconstruct_mtm_metrics_fast(
        trades=[trade],
        price_series=price_series,
        initial_equity=1000.0,
        start_time=t0,
        end_time=t0 + timedelta(minutes=5),
        margin_cap=50.0,
    )
    assert res is None

    # margin_cap=150 >= 100 -> should admit
    res_ok = reconstruct_mtm_metrics_fast(
        trades=[trade],
        price_series=price_series,
        initial_equity=1000.0,
        start_time=t0,
        end_time=t0 + timedelta(minutes=5),
        margin_cap=150.0,
    )
    assert res_ok is not None
    assert res_ok.peak_margin == pytest.approx(100.0)


def test_fast_mtm_metrics_early_rejection_insolvency() -> None:
    """Verify that catastrophic losses leading to negative equity trigger rejection."""
    t0 = datetime(2026, 9, 20, 0, 0, 0, tzinfo=UTC)
    trade = TradeRecord(
        trade_id="T_insolvent",
        symbol="BTCUSDT",
        entry_time=t0,
        entry_price=100.0,
        exit_time=t0 + timedelta(minutes=5),
        exit_price=10.0,  # massive 90% loss on 10x notional
        notional_usdt=2000.0,
        leverage=5.0,
    )
    price_series = {
        "BTCUSDT": (
            [t0.timestamp(), (t0 + timedelta(minutes=5)).timestamp()],
            [100.0, 10.0],
        )
    }

    res = reconstruct_mtm_metrics_fast(
        trades=[trade],
        price_series=price_series,
        initial_equity=1000.0,
        start_time=t0,
        end_time=t0 + timedelta(minutes=5),
    )
    assert res is None
