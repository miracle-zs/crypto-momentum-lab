"""Unit tests for equity curve and path risk metrics."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

import numpy as np
import pytest

from local_optimization.equity import (
    EquityPoint,
    calc_cdar,
    calc_ulcer_index,
    evaluate_equity_curve,
)


def test_monotonically_increasing_equity() -> None:
    """Monotonic gains should have zero drawdown, UI, and CDaR."""
    t0 = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)
    points = [
        EquityPoint(timestamp=t0 + timedelta(minutes=15 * i), equity=1000.0 + 10.0 * i)
        for i in range(10)
    ]
    metrics = evaluate_equity_curve(points, initial_equity=1000.0)

    assert metrics.is_feasible is True
    assert metrics.net_pnl == pytest.approx(90.0)
    assert metrics.net_return_pct == pytest.approx(9.0)
    assert metrics.max_drawdown_pct == pytest.approx(0.0)
    assert metrics.max_drawdown_usdt == pytest.approx(0.0)
    assert metrics.ulcer_index == pytest.approx(0.0)
    assert metrics.cdar_95 == pytest.approx(0.0)
    assert metrics.average_drawdown == pytest.approx(0.0)
    assert metrics.underwater_time_pct == pytest.approx(0.0)
    assert metrics.is_right_censored is False


def test_ulcer_index_distinguishes_duration_with_same_mdd() -> None:
    """Validate Astra's example:
    Two paths with same 5% MDD have drastically different UI.

    Path A: 99 steps at 1000, 1 step at 950 (5% DD for 1% of the time).
            UI = sqrt(0.01 * 0.05^2) = 0.005 (0.5%)
    Path B: 50 steps at 1000, 50 steps at 950 (5% DD for 50% of the time).
            UI = sqrt(0.50 * 0.05^2) = 0.035355 (3.54%)
    """
    pct_dd_a = np.array([0.0] * 99 + [0.05], dtype=np.float64)
    ui_a = calc_ulcer_index(pct_dd_a)
    assert ui_a == pytest.approx(0.005, abs=1e-5)

    pct_dd_b = np.array([0.0] * 50 + [0.05] * 50, dtype=np.float64)
    ui_b = calc_ulcer_index(pct_dd_b)
    assert ui_b == pytest.approx(0.05 * math.sqrt(0.5), abs=1e-5)
    assert ui_b > ui_a * 7.0


def test_cdar_tail_drawdown() -> None:
    """CDaR 95% should accurately isolate the worst 5% tail average."""
    # 100 observations: 95 with 0.01 drawdown, 5 with 0.10 drawdown.
    # The worst 5% tail is exactly the 5 observations at 0.10.
    # CDaR95 should equal 0.10.
    pct_dd = np.array([0.01] * 95 + [0.10] * 5, dtype=np.float64)
    cdar = calc_cdar(pct_dd, alpha=0.95)
    assert cdar == pytest.approx(0.10, abs=1e-3)


def test_intraday_rms_vs_cross_day_drawdown() -> None:
    """Test intraday drawdown reset behavior across UTC midnights."""
    # Day 1: 1000 -> 980 (2% drop)
    # Day 2: 980 -> 960.4 (2% intraday drop relative to 980)
    t1 = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
    t2 = datetime(2026, 9, 1, 18, 0, tzinfo=UTC)
    t3 = datetime(2026, 9, 2, 6, 0, tzinfo=UTC)
    t4 = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)

    points = [
        EquityPoint(timestamp=t1, equity=1000.0),
        EquityPoint(timestamp=t2, equity=980.0),
        EquityPoint(timestamp=t3, equity=980.0),
        EquityPoint(timestamp=t4, equity=960.4),
    ]
    metrics = evaluate_equity_curve(points, initial_equity=1000.0)

    # Intraday drop on Day 1 is (1000-980)/1000 = 0.02
    # Intraday drop on Day 2 is (980-960.4)/980 = 0.02
    # Intraday RMS should be 0.02
    assert metrics.intraday_rms_drawdown == pytest.approx(0.02, abs=1e-4)

    # But overall MDD from initial high water mark is (1000 - 960.4)/1000 = 0.0396
    assert metrics.max_drawdown_pct == pytest.approx(0.0396, abs=1e-4)
    assert metrics.is_right_censored is True


def test_infeasible_when_equity_reaches_zero_or_negative() -> None:
    """A curve dropping to zero or negative triggers infeasible."""
    t0 = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)
    points = [
        EquityPoint(timestamp=t0, equity=1000.0),
        EquityPoint(timestamp=t0 + timedelta(hours=1), equity=500.0),
        EquityPoint(timestamp=t0 + timedelta(hours=2), equity=-10.0),
    ]
    metrics = evaluate_equity_curve(points, initial_equity=1000.0)
    assert metrics.is_feasible is False
    assert metrics.infeasible_reason is not None
    assert "non-positive" in metrics.infeasible_reason.lower()


def test_empty_points_evaluation() -> None:
    """Empty equity points list should return cleanly with zeroed metrics."""
    metrics = evaluate_equity_curve([], initial_equity=1000.0)
    assert metrics.is_feasible is True
    assert metrics.net_pnl == 0.0
    assert metrics.ulcer_index == 0.0
    assert metrics.n_points == 0
