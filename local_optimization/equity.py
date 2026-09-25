"""Equity curve calculation, mark-to-market accounting, and path risk metrics.

Implements rigorous, vectorized risk metrics:
- Mark-to-market (MTM) account equity
- High-water mark (HWM) and percentage drawdowns
- Ulcer Index (UI) - primary path risk metric
- CDaR 95% - tail drawdown expectation
- Underwater duration and right-censoring detection
- Intraday drawdown distribution and RMS
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

import numpy as np


@dataclass(frozen=True)
class EquityPoint:
    """A discrete observation on the account equity curve."""

    timestamp: datetime
    equity: float
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    fee: float = 0.0
    funding: float = 0.0
    peak_initial_margin: float = 0.0
    active_positions: int = 0


@dataclass(frozen=True)
class EquityMetrics:
    """Standardized performance and path-dependent risk metrics."""

    initial_equity: float
    final_equity: float
    net_pnl: float
    net_return_pct: float
    daily_log_return: float
    max_drawdown_pct: float
    max_drawdown_usdt: float
    ulcer_index: float
    cdar_95: float
    average_drawdown: float
    underwater_time_pct: float
    max_underwater_seconds: float
    is_right_censored: bool
    intraday_rms_drawdown: float
    intraday_mean_drawdown: float
    intraday_worst_drawdown: float
    n_points: int
    n_days: float
    is_feasible: bool
    interval_pnl: float = 0.0
    infeasible_reason: str | None = None


def compute_drawdown_series(
    equity: np.ndarray,
    initial_equity: float | None = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Compute high-water mark, absolute drawdown, and percentage drawdown.

    Args:
        equity: 1D numpy array of account equity values over time.
        initial_equity: Optional initial account capital benchmark (E0).
            If provided, HWM is anchored at max(E0, equity[0]) to accurately
            capture immediate losses from the initial deposit.

    Returns:
        tuple of (hwm, abs_drawdown, pct_drawdown).
        pct_drawdown is clipped to [0.0, 1.0] for non-negative equity.
    """
    if len(equity) == 0:
        empty = np.array([], dtype=np.float64)
        return empty, empty, empty

    equity = np.asarray(equity, dtype=np.float64)
    if initial_equity is not None and initial_equity > 0:
        eq_with_init = np.concatenate(([float(initial_equity)], equity))
        hwm = np.maximum.accumulate(eq_with_init)[1:]
    else:
        hwm = np.maximum.accumulate(equity)
    abs_dd = np.maximum(0.0, hwm - equity)

    # Percentage drawdown: D(t) / H(t). If H(t) <= 0, drawdown is undefined (1.0).
    with np.errstate(divide="ignore", invalid="ignore"):
        pct_dd = np.where(hwm > 0, abs_dd / hwm, 1.0)
    pct_dd = np.clip(pct_dd, 0.0, 1.0)
    return hwm, abs_dd, pct_dd


def calc_ulcer_index(pct_dd: np.ndarray) -> float:
    """Calculate the Ulcer Index (UI) from a percentage drawdown series.

    UI = sqrt(mean(d(t)^2))

    Penalizes both the depth and duration of being underwater.
    """
    if len(pct_dd) == 0:
        return 0.0
    sq_dd = np.square(pct_dd)
    return float(np.sqrt(np.mean(sq_dd)))


def calc_cdar(pct_dd: np.ndarray, alpha: float = 0.95) -> float:
    """Calculate Conditional Drawdown at Risk (CDaR) at confidence level alpha.

    Implements exact discrete tail average CVaR/CDaR:
    Computes the weighted mean of the worst (1 - alpha) fraction of the drawdown sample.
    Guarantees that CDaR is strictly bounded: mean(pct_dd) <= CDaR <= max(pct_dd).

    Args:
        pct_dd: 1D array of percentage drawdowns in [0, 1].
        alpha: Confidence level, typically 0.95.

    Returns:
        CDaR value in [0, 1].
    """
    if len(pct_dd) == 0:
        return 0.0
    arr = np.asarray(pct_dd, dtype=np.float64)
    if alpha <= 0.0:
        return float(np.mean(arr))
    if alpha >= 1.0:
        return float(np.max(arr))

    n = len(arr)
    sorted_dd = np.sort(arr)[::-1]
    tail_mass = n * (1.0 - alpha)

    if tail_mass <= 1.0:
        return float(sorted_dd[0])

    m = int(math.floor(tail_mass))
    remainder = tail_mass - m

    tail_sum = float(np.sum(sorted_dd[:m]))
    if remainder > 0.0 and m < n:
        tail_sum += remainder * float(sorted_dd[m])

    cdar = tail_sum / tail_mass
    return float(np.clip(cdar, 0.0, float(np.max(arr))))


def calc_underwater_stats(
    pct_dd: np.ndarray,
    timestamps: Sequence[datetime],
    eps: float = 1e-6,
) -> tuple[float, float, bool]:
    """Calculate underwater time fraction, maximum duration, and right-censoring.

    Args:
        pct_dd: 1D array of percentage drawdowns.
        timestamps: Monotonically increasing datetimes matching pct_dd.
        eps: Tolerance threshold for considering equity at a new peak.

    Returns:
        tuple of (underwater_time_pct, max_underwater_seconds, is_right_censored).
    """
    n = len(pct_dd)
    if n == 0 or len(timestamps) != n:
        return 0.0, 0.0, False

    underwater_mask = pct_dd > eps
    underwater_time_pct = float(np.mean(underwater_mask) * 100.0)

    max_duration_sec = 0.0
    cur_start: datetime | None = None
    is_right_censored = bool(underwater_mask[-1])

    for i in range(n):
        if underwater_mask[i]:
            if cur_start is None:
                cur_start = timestamps[i]
            cur_duration = (timestamps[i] - cur_start).total_seconds()
            if cur_duration > max_duration_sec:
                max_duration_sec = cur_duration
        else:
            if cur_start is not None:
                cur_duration = (timestamps[i] - cur_start).total_seconds()
                if cur_duration > max_duration_sec:
                    max_duration_sec = cur_duration
                cur_start = None

    return underwater_time_pct, max_duration_sec, is_right_censored


def calc_intraday_drawdown_stats(
    equity: np.ndarray,
    timestamps: Sequence[datetime],
) -> dict[str, float]:
    """Calculate daily intraday drawdown statistics with daily midnight reset.

    Each day's local high water mark starts from the day's first observed equity.
    Captures intraday holding volatility independently from cross-day trends.

    Args:
        equity: 1D array of equity values.
        timestamps: Sequence of datetimes in UTC.

    Returns:
        dict with rms, mean, worst, and p95 intraday drawdowns.
    """
    if len(equity) == 0 or len(timestamps) != len(equity):
        return {
            "intraday_rms": 0.0,
            "intraday_mean": 0.0,
            "intraday_worst": 0.0,
            "intraday_p95": 0.0,
        }

    # Group equity points by calendar date (UTC)
    day_indices: dict[date, list[int]] = {}
    for idx, ts in enumerate(timestamps):
        d = ts.astimezone(UTC).date()
        day_indices.setdefault(d, []).append(idx)

    daily_max_dds: list[float] = []
    for _d, indices in day_indices.items():
        sub_equity = equity[indices]
        _, _, sub_pct_dd = compute_drawdown_series(sub_equity)
        daily_max_dds.append(float(np.max(sub_pct_dd)) if len(sub_pct_dd) > 0 else 0.0)

    if not daily_max_dds:
        return {
            "intraday_rms": 0.0,
            "intraday_mean": 0.0,
            "intraday_worst": 0.0,
            "intraday_p95": 0.0,
        }

    arr = np.array(daily_max_dds, dtype=np.float64)
    rms = float(np.sqrt(np.mean(np.square(arr))))
    mean = float(np.mean(arr))
    worst = float(np.max(arr))
    p95 = float(np.quantile(arr, 0.95))

    return {
        "intraday_rms": rms,
        "intraday_mean": mean,
        "intraday_worst": worst,
        "intraday_p95": p95,
    }


def evaluate_equity_curve(
    points: Sequence[EquityPoint],
    initial_equity: float = 1000.0,
    calendar_days: float | None = None,
) -> EquityMetrics:
    """Evaluate full performance and path risk metrics from an equity point series.

    Args:
        points: Ordered sequence of EquityPoint observations.
        initial_equity: Initial capital E0.
        calendar_days: Optional calendar days D for daily log growth.

    Returns:
        EquityMetrics object.
    """
    if initial_equity <= 0:
        raise ValueError(f"Initial equity must be positive, got {initial_equity}")

    if len(points) == 0:
        return EquityMetrics(
            initial_equity=initial_equity,
            final_equity=initial_equity,
            net_pnl=0.0,
            net_return_pct=0.0,
            daily_log_return=0.0,
            max_drawdown_pct=0.0,
            max_drawdown_usdt=0.0,
            ulcer_index=0.0,
            cdar_95=0.0,
            average_drawdown=0.0,
            underwater_time_pct=0.0,
            max_underwater_seconds=0.0,
            is_right_censored=False,
            intraday_rms_drawdown=0.0,
            intraday_mean_drawdown=0.0,
            intraday_worst_drawdown=0.0,
            n_points=0,
            n_days=0.0,
            is_feasible=True,
        )

    eq_arr = np.array([p.equity for p in points], dtype=np.float64)
    ts_list = [p.timestamp for p in points]
    n_points = len(eq_arr)

    # Check finite values
    if not np.all(np.isfinite(eq_arr)):
        return EquityMetrics(
            initial_equity=initial_equity,
            final_equity=0.0,
            net_pnl=-initial_equity,
            net_return_pct=-100.0,
            daily_log_return=-math.inf,
            max_drawdown_pct=1.0,
            max_drawdown_usdt=initial_equity,
            ulcer_index=1.0,
            cdar_95=1.0,
            average_drawdown=1.0,
            underwater_time_pct=100.0,
            max_underwater_seconds=0.0,
            is_right_censored=True,
            intraday_rms_drawdown=1.0,
            intraday_mean_drawdown=1.0,
            intraday_worst_drawdown=1.0,
            n_points=n_points,
            n_days=0.0,
            is_feasible=False,
            interval_pnl=-initial_equity,
            infeasible_reason="Non-finite values encountered in equity series",
        )

    # Check feasibility: non-positive equity triggers liquidation / infeasible
    min_eq = float(np.min(eq_arr))
    if min_eq <= 0:
        return EquityMetrics(
            initial_equity=initial_equity,
            final_equity=float(eq_arr[-1]),
            net_pnl=float(eq_arr[-1] - initial_equity),
            net_return_pct=float(
                (eq_arr[-1] - initial_equity) / initial_equity * 100.0
            ),
            daily_log_return=-math.inf,
            max_drawdown_pct=1.0,
            max_drawdown_usdt=float(initial_equity),
            ulcer_index=1.0,
            cdar_95=1.0,
            average_drawdown=1.0,
            underwater_time_pct=100.0,
            max_underwater_seconds=(ts_list[-1] - ts_list[0]).total_seconds(),
            is_right_censored=True,
            intraday_rms_drawdown=1.0,
            intraday_mean_drawdown=1.0,
            intraday_worst_drawdown=1.0,
            n_points=n_points,
            n_days=(ts_list[-1] - ts_list[0]).total_seconds() / 86400.0,
            is_feasible=False,
            interval_pnl=float(eq_arr[-1] - eq_arr[0]),
            infeasible_reason=f"Non-positive equity reached: {min_eq:.4f}",
        )

    hwm, abs_dd, pct_dd = compute_drawdown_series(eq_arr, initial_equity=initial_equity)

    final_eq = float(eq_arr[-1])
    net_pnl = final_eq - initial_equity
    interval_pnl = final_eq - float(eq_arr[0])
    net_return_pct = (net_pnl / initial_equity) * 100.0

    if calendar_days is not None and calendar_days > 0:
        eff_days = float(calendar_days)
    else:
        total_seconds = max(1.0, (ts_list[-1] - ts_list[0]).total_seconds())
        eff_days = total_seconds / 86400.0

    n_days = eff_days
    if n_days > 0 and final_eq > 0 and initial_equity > 0:
        daily_log_return = math.log(final_eq / initial_equity) / n_days
    else:
        daily_log_return = -math.inf if final_eq <= 0 else 0.0

    mdd_pct = float(np.max(pct_dd))
    mdd_usdt = float(np.max(abs_dd))
    ui = calc_ulcer_index(pct_dd)
    cdar = calc_cdar(pct_dd, alpha=0.95)
    add = float(np.mean(pct_dd))

    underwater_pct, max_underwater_sec, right_censored = calc_underwater_stats(
        pct_dd, ts_list
    )
    intraday_stats = calc_intraday_drawdown_stats(eq_arr, ts_list)

    return EquityMetrics(
        initial_equity=initial_equity,
        final_equity=final_eq,
        net_pnl=net_pnl,
        net_return_pct=net_return_pct,
        daily_log_return=daily_log_return,
        max_drawdown_pct=mdd_pct,
        max_drawdown_usdt=mdd_usdt,
        ulcer_index=ui,
        cdar_95=cdar,
        average_drawdown=add,
        underwater_time_pct=underwater_pct,
        max_underwater_seconds=max_underwater_sec,
        is_right_censored=right_censored,
        intraday_rms_drawdown=intraday_stats["intraday_rms"],
        intraday_mean_drawdown=intraday_stats["intraday_mean"],
        intraday_worst_drawdown=intraday_stats["intraday_worst"],
        n_points=n_points,
        n_days=n_days,
        is_feasible=True,
        interval_pnl=interval_pnl,
    )


def calc_daily_log_growth(
    equity_series: Sequence[EquityPoint] | np.ndarray,
    calendar_days: float,
    initial_equity: float = 1000.0,
) -> float:
    """Calculate the average daily net log growth rate g = log(E_end / E_start) / D.

    Args:
        equity_series: Sequence of EquityPoint or array of equity values.
        calendar_days: Total active calendar days D (including zero-trade days).
        initial_equity: Initial capital E0.

    Returns:
        Average daily net log growth rate g. If end equity <= 0, returns -inf.
    """
    if calendar_days <= 0:
        return 0.0
    if len(equity_series) == 0:
        return 0.0
    if isinstance(equity_series[0], EquityPoint):
        final_eq = float(equity_series[-1].equity)
        start_eq = float(equity_series[0].equity)
    else:
        final_eq = float(equity_series[-1])
        start_eq = float(equity_series[0])

    base_eq = start_eq if start_eq > 0 else initial_equity
    if final_eq <= 0 or base_eq <= 0:
        return -math.inf
    return math.log(final_eq / base_eq) / calendar_days


def calc_time_weighted_return(
    points: Sequence[EquityPoint],
    cash_flows: Sequence[Any],
    initial_equity: float = 1000.0,
) -> dict[str, float]:
    """Calculate cash-flow neutral Time-Weighted Return (TWR).

    Chains sub-period growth factors:
        factor_k = E_end_before_flow / E_start_after_previous_flow
        total_factor = product(factor_k)

    Prevents external deposits from being counted as trading alpha or resetting HWM.
    """
    if len(points) == 0:
        return {
            "twr_factor": 1.0,
            "twr_return_pct": 0.0,
            "neutral_final_equity": initial_equity,
        }

    if len(cash_flows) == 0:
        factor = float(points[-1].equity / initial_equity)
        return {
            "twr_factor": factor,
            "twr_return_pct": (factor - 1.0) * 100.0,
            "neutral_final_equity": initial_equity * factor,
        }

    sorted_flows = sorted(cash_flows, key=lambda x: x.timestamp)
    current_start_eq = (
        float(points[0].equity) if points[0].equity > 0 else initial_equity
    )
    cumulative_factor = 1.0
    p_idx = 0

    for flow in sorted_flows:
        # Find latest point before or at flow.timestamp
        flow_time = flow.timestamp
        prev_eq = current_start_eq
        while p_idx < len(points) and points[p_idx].timestamp <= flow_time:
            prev_eq = float(points[p_idx].equity)
            p_idx += 1

        if current_start_eq > 0:
            sub_factor = prev_eq / current_start_eq
            cumulative_factor *= sub_factor

        # Adjust start equity after flow
        current_start_eq = max(1e-6, prev_eq + float(flow.amount_usdt))

    # Remaining period after last flow
    if p_idx < len(points):
        final_eq = float(points[-1].equity)
        if current_start_eq > 0:
            cumulative_factor *= final_eq / current_start_eq

    return {
        "twr_factor": cumulative_factor,
        "twr_return_pct": (cumulative_factor - 1.0) * 100.0,
        "neutral_final_equity": initial_equity * cumulative_factor,
    }
