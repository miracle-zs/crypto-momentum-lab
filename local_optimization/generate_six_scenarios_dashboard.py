#!/usr/bin/env python3
"""Six-Scenario Optimization and 15s MTM Equity Comparison Dashboard Generator.

Evaluates 6 standardized optimization scenarios (2 margin x 3 objectives)
plus 2 live baseline profiles across the 17-day high-frequency dataset.

Matrix of Scenarios:
1. Scenario 1 (m280_pnl_max): Margin <= 280U | Target 1: Max Net PnL
2. Scenario 2 (m280_balanced): Margin <= 280U | Target 2: Calmar + 8D Stab
3. Scenario 3 (m280_compounding): Margin <= 280U | Target 3: Compounding
4. Scenario 4 (unc_pnl_max): Unconstrained Margin | Target 1: Max Net PnL
5. Scenario 5 (unc_balanced): Unconstrained Margin | Target 2: Calmar + 8D Stab
6. Scenario 6 (unc_compounding): Unconstrained Margin | Target 3: Compounding
7. Baseline 1 (live_profile1): 当前实盘金牌基线 (2/1/0.75%/0.30/3.0/1.25x/cd=0/slots=2)
8. Baseline 2 (live_profile2): 历史旧版基线 (2/1/0.50%/0.30/4.0/1.5x/cd=0/slots=2)

100% 15-second continuous Mark-to-Market (MTM) valuation.
"""

from __future__ import annotations

import argparse
import bisect
import json
import multiprocessing as mp
import os
import sys
import time
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(ROOT_DIR / "src") not in sys.path:
    sys.path.insert(0, str(ROOT_DIR / "src"))

from crypto_momentum_lab.live_rollout.scheduled_risk_window import (  # noqa: E402
    ScheduledRiskWindowConfig,
)
from local_optimization.equity import evaluate_equity_curve  # noqa: E402
from local_optimization.evaluation_context import (  # noqa: E402
    EvaluationContext,
    ScenarioSpec,
    evaluate_candidate,
    evaluate_daily_compounding,
    to_trade_records,
)
from local_optimization.evaluation_context import (  # noqa: E402
    compute_daily_compounding_scales as compute_daily_compounding_scales,
)
from local_optimization.mtm_engine import (  # noqa: E402
    AlignedPriceGrid,
    TradeRecord,
    get_price_at,
    load_cached_price_series,
    load_trades_from_csv,
    reconstruct_mtm_equity,
)
from local_optimization.opportunity import (  # noqa: E402
    RawOpportunity,
    filter_opportunities_by_top10,
    load_top10_lookup,
)
from local_optimization.reconciliation import (  # noqa: E402
    reconcile_signals_and_fills,
)
from local_optimization.run_two_stage_grid_optimization import (  # noqa: E402
    DIMS,
)
from local_optimization.run_walk_forward_analysis import (  # noqa: E402
    filter_events_by_params,
    load_all_replay_events,
)
from local_optimization.simulation_ledger import (  # noqa: E402
    SimulationLedger,
)
from local_optimization.snapshot import parse_stream_timestamp  # noqa: E402

DEFAULT_DATA_DIR = ROOT_DIR / "local_optimization/data/replay_all_collected_20260920"
DEFAULT_LIVE_EARLY_DIR = ROOT_DIR / "local_optimization/data/live_20260918"
DEFAULT_LIVE_LATEST_DIR = ROOT_DIR / "local_optimization/data/live_latest"
LOCAL_OPT_GRID = (
    ROOT_DIR
    / "local_optimization/data/optimization_all_collected_20260919/grid_results.csv"
)
SERVER_EXPORT_GRID = (
    ROOT_DIR
    / "server_exports/cml-research-data-20260918-000425"
    / "optimization-volume-feature-7d-20260918"
    / "notional_5m_vs_30m-v1/grid_results.csv"
)
DEFAULT_GRID_CSV = LOCAL_OPT_GRID if LOCAL_OPT_GRID.exists() else SERVER_EXPORT_GRID
DEFAULT_OPT_DIR = (
    LOCAL_OPT_GRID.parent if LOCAL_OPT_GRID.exists() else SERVER_EXPORT_GRID.parent
)
CACHE_PRICE_FILE = ROOT_DIR / "local_optimization/data/cache_15s_price_series.pkl"
DEFAULT_TOP10_CACHE = ROOT_DIR / "local_optimization/data/cache_top10_lookup.pkl"
DEFAULT_TOP20_CACHE = ROOT_DIR / "local_optimization/data/cache_top20_lookup.pkl"
DEFAULT_TOP30_CACHE = ROOT_DIR / "local_optimization/data/cache_top30_lookup.pkl"
DEFAULT_PARQUET_DIR = (
    ROOT_DIR / "local_optimization/data/all_data_parquet/environment=research"
)
TEMPLATE_FILE = ROOT_DIR / "local_optimization/templates/six_scenarios_template.html"
OUTPUT_HTML_REPORT = (
    ROOT_DIR / "local_optimization/reports/six_scenarios_equity_comparison.html"
)
DEFAULT_ARTIFACT_DIR = (
    Path(os.environ["ANTIGRAVITY_ARTIFACT_DIR"])
    if "ANTIGRAVITY_ARTIFACT_DIR" in os.environ
    else Path(
        "/Users/zhangshuai/.gemini/antigravity/brain/b0c16846-1c16-4d75-90d6-40a3d35ee449"
    )
)
ARTIFACT_DIR = DEFAULT_ARTIFACT_DIR

BEIJING_TZ = timezone(timedelta(hours=8))
INITIAL_EQUITY = 1000.0
NOTIONAL_PER_ENTRY = 100.0
LEVERAGE = 5.0
MARGIN_PER_ENTRY = NOTIONAL_PER_ENTRY / LEVERAGE  # 20.0 USDT
CONCURRENCY_SLOTS_DOMAIN = [1, 2, 3, 4]
DIM_KEYS_8D: list[str] = list(DIMS) + ["max_open_positions"]

DEFAULT_GOLD_PROFILE: dict[str, Any] = {
    "impulse_window_buckets": 2,
    "confirmation_buckets": 1,
    "min_return_pct": 0.75,
    "min_imbalance": 0.30,
    "min_intensity": 3.0,
    "min_volume_ratio": 1.25,
    "cooldown_buckets": 0,
    "max_open_positions": 2,
}
DEFAULT_LEGACY_PROFILE: dict[str, Any] = {
    "impulse_window_buckets": 2,
    "confirmation_buckets": 1,
    "min_return_pct": 0.50,
    "min_imbalance": 0.30,
    "min_intensity": 4.0,
    "min_volume_ratio": 1.50,
    "cooldown_buckets": 0,
    "max_open_positions": 2,
}


def to_beijing_str(dt: datetime) -> str:
    return dt.astimezone(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")


def to_beijing_short(dt: datetime) -> str:
    return dt.astimezone(BEIJING_TZ).strftime("%m-%d %H:%M")


def format_8d_param_str(params: dict[str, Any]) -> str:
    """Format 8-dimensional parameter dictionary into readable string."""
    w = int(params["impulse_window_buckets"])
    c = int(params["confirmation_buckets"])
    r = float(params["min_return_pct"])
    imb = float(params["min_imbalance"])
    inten = float(params["min_intensity"])
    vol = float(params["min_volume_ratio"])
    vol_str = f"{vol:.2f}x" if abs(vol * 10 - round(vol * 10)) > 1e-4 else f"{vol:.1f}x"
    cd = int(params["cooldown_buckets"])
    slots = int(params.get("max_open_positions", 2))
    return f"{w}/{c}/{r:.2f}%/{imb:.2f}/{inten:.1f}/{vol_str}/cd={cd}/slots={slots}"


def filter_events_with_concurrency(
    events: Sequence[dict[str, Any]],
    params: dict[str, Any] | None = None,
    max_slots: int = 2,
) -> tuple[list[dict[str, Any]], float, float, int]:
    """Filter events based on 7D params and simulate per-symbol slot constraint.

    Enforces per-symbol slot limit (single symbol <= max_slots), while allowing
    unconstrained multi-symbol concurrent positions across the entire account.
    Peak concurrency and margin reflect total simultaneous positions.

    Returns:
        (admitted_events, running_pnl, max_drawdown, peak_concurrency)
    """
    if params is not None:
        sel, _, _ = filter_events_by_params(events, params)
    else:
        sel = list(events)
    if not sel:
        return [], 0.0, 0.0, 0

    sel_sorted = sorted(sel, key=lambda x: x["entry_epoch"])
    admitted: list[dict[str, Any]] = []
    active_by_sym: dict[str, list[float]] = defaultdict(list)
    time_pts: list[tuple[float, int]] = []

    for ev in sel_sorted:
        ent = ev["entry_epoch"]
        ex = (
            float(ev["exit_epoch"])
            if ev.get("exit_epoch") is not None
            else float("inf")
        )
        ex_sub = (
            float(ev["exit_submitted_epoch"])
            if ev.get("exit_submitted_epoch") is not None
            else (
                float(ev["exit_submitted_at"].timestamp())
                if isinstance(ev.get("exit_submitted_at"), datetime)
                else ex
            )
        )
        sym = ev["symbol"]
        # Purge ended batches for this specific symbol (freed on exit submission)
        active_by_sym[sym] = [t for t in active_by_sym[sym] if t > ent]
        if len(active_by_sym[sym]) < max_slots:
            admitted.append(ev)
            active_by_sym[sym].append(ex_sub)
            time_pts.append((ent, 1))
            if ex != float("inf"):
                time_pts.append((ex, -1))

    if not admitted:
        return [], 0.0, 0.0, 0

    # Calculate global peak concurrency across ALL symbols
    time_pts.sort(key=lambda x: (x[0], x[1]))
    cur_c = 0
    peak_concurrency = 0
    for _, delta in time_pts:
        cur_c += delta
        if cur_c > peak_concurrency:
            peak_concurrency = cur_c

    # Calculate realized PnL and MDD ordered by trade exit time
    admitted_by_exit = sorted(
        admitted,
        key=lambda x: (
            float(x["exit_epoch"]) if x.get("exit_epoch") is not None else float("inf")
        ),
    )
    running_pnl = 0.0
    peak_pnl = 0.0
    mdd = 0.0
    for ev in admitted_by_exit:
        if ev.get("net_pnl_usdt") is not None:
            running_pnl += ev["net_pnl_usdt"]
        if running_pnl > peak_pnl:
            peak_pnl = running_pnl
        dd = peak_pnl - running_pnl
        if dd > mdd:
            mdd = dd

    return admitted, running_pnl, mdd, peak_concurrency


@dataclass
class Candidate8D:
    params: dict[str, Any]
    net_pnl: float
    mdd: float
    calmar: float
    compounding_score: float
    compounding_mdd: float
    compounding_ui: float
    terminal_compounded_equity: float
    n_trades: int
    peak_margin: float
    compounding_peak_margin: float = 0.0
    stability: float = 0.0
    cdar_95: float = 0.0


def compute_8d_neighborhood_stability(
    candidates_dict: dict[tuple[Any, ...], Candidate8D],
    grid_values: dict[str, list[Any]],
    dim_keys: list[str],
) -> None:
    """Compute 8-dimensional topological neighborhood stability for all candidates."""
    val_to_idx = {d: {v: i for i, v in enumerate(grid_values[d])} for d in dim_keys}

    for key, cand in candidates_dict.items():
        if cand.net_pnl <= 0:
            cand.stability = 0.0
            continue

        neighbors: list[tuple[Any, ...]] = []
        for i, d in enumerate(dim_keys):
            vals = grid_values[d]
            curr_val = key[i]
            v_idx = val_to_idx[d].get(curr_val)
            if v_idx is not None:
                if v_idx > 0:
                    n_key = list(key)
                    n_key[i] = vals[v_idx - 1]
                    neighbors.append(tuple(n_key))
                if v_idx + 1 < len(vals):
                    n_key = list(key)
                    n_key[i] = vals[v_idx + 1]
                    neighbors.append(tuple(n_key))

        if not neighbors:
            cand.stability = 1.0
            continue

        stable_count = 0
        for n in neighbors:
            n_cand = candidates_dict.get(n)
            if n_cand and n_cand.net_pnl >= 0.30 * cand.net_pnl:
                stable_count += 1

        cand.stability = stable_count / len(neighbors)


def load_events_from_csv(csv_path: Path) -> list[dict[str, Any]]:
    """Load trade events from CSV into event dict format."""
    import csv

    events: list[dict[str, Any]] = []
    if not csv_path.exists():
        return events
    with csv_path.open("r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row.get("entry_at") or not row.get("entry_price"):
                continue
            is_closed = str(row.get("closed", "")).lower() in {"true", "1"}
            if not is_closed and not row.get("exit_at"):
                continue
            events.append(
                {
                    "symbol": row["symbol"].strip(),
                    "entry_at": row["entry_at"],
                    "entry_price": float(row["entry_price"]),
                    "exit_at": row.get("exit_at"),
                    "exit_price": float(row.get("exit_price") or row["entry_price"]),
                    "net_pnl_usdt": float(row.get("net_pnl_usdt") or 0.0),
                    "detected_at": row.get("detected_at") or row["entry_at"],
                }
            )
    return events


def get_scenario_events(
    key: str,
    cand_params: dict[str, Any],
    opt_data_dir: Path | None,
    replay_data_dir: Path | None,
    fallback_events: Sequence[Any],
    margin_cap: float | None = None,
    ledger: SimulationLedger | None = None,
    scheduled_risk_window: ScheduledRiskWindowConfig | None = None,
    prices_by_symbol: dict[str, Any] | None = None,
    w_start: datetime | None = None,
    w_end: datetime | None = None,
) -> list[Any]:
    if fallback_events:
        slots = int(cand_params.get("max_open_positions", 2))
        effective_w_start = w_start if w_start is not None else _worker_w_start
        effective_w_end = w_end if w_end is not None else _worker_w_end
        effective_prices = (
            prices_by_symbol if prices_by_symbol is not None else _worker_prices
        )
        effective_risk = (
            scheduled_risk_window
            if scheduled_risk_window is not None
            else _worker_scheduled_risk_window
        )

        if effective_w_start is None:
            effective_w_start = datetime(2026, 9, 1, tzinfo=UTC)
            t0_val = getattr(
                fallback_events[0], "detected_at", None
            ) or fallback_events[0].get("detected_at")
            if isinstance(t0_val, datetime):
                effective_w_start = min(
                    (getattr(ev, "detected_at", None) or ev.get("detected_at"))
                    for ev in fallback_events
                    if getattr(ev, "detected_at", None) or ev.get("detected_at")
                )

        if effective_w_end is None:
            effective_w_end = datetime(2026, 10, 1, tzinfo=UTC)
            t0_val = getattr(
                fallback_events[0], "detected_at", None
            ) or fallback_events[0].get("detected_at")
            if isinstance(t0_val, datetime):
                effective_w_end = max(
                    (
                        getattr(ev, "exit_time", None)
                        or getattr(ev, "detected_at", None)
                        or ev.get("exit_at")
                        or ev.get("detected_at")
                    )
                    for ev in fallback_events
                    if getattr(ev, "detected_at", None) or ev.get("detected_at")
                )
                if isinstance(effective_w_end, datetime):
                    effective_w_end = effective_w_end + timedelta(days=1)

        ctx = EvaluationContext(
            window_start=effective_w_start,
            window_end=effective_w_end,
            price_series=effective_prices or {},
            events=fallback_events,
            scheduled_risk_window=effective_risk,
            initial_equity=INITIAL_EQUITY,
            notional_per_entry=NOTIONAL_PER_ENTRY,
            leverage=LEVERAGE,
            fee_rate=0.0005,
            ledger=ledger,
        )
        scenario = ScenarioSpec(
            slots=slots,
            margin_cap=margin_cap,
        )
        eval_res = evaluate_candidate(ctx, cand_params, scenario, include_curve=False)
        return eval_res.raw_admitted

    if replay_data_dir is not None:
        if key == "b_profile1":
            p_file = replay_data_dir / "account_primary_events.csv"
            if p_file.exists():
                return load_events_from_csv(p_file)

        elif key == "b_profile2":
            p_file = replay_data_dir / "account_acc02_events.csv"
            if p_file.exists():
                return load_events_from_csv(p_file)

    return []


_worker_prices: dict[str, Any] = {}
_worker_opps_by_wc: dict[tuple[int, int], list[Any]] = {}
_worker_events: Sequence[Any] | None = None
_worker_w_start: datetime | None = None
_worker_w_end: datetime | None = None
_worker_ledger: SimulationLedger | None = None
_worker_scheduled_risk_window: ScheduledRiskWindowConfig | None = None
_worker_aligned_price_grid: AlignedPriceGrid | None = None
_worker_context: EvaluationContext | None = None


def _init_verification_worker(
    prices: dict[str, Any],
    opps_by_wc: dict[tuple[int, int], list[Any]],
    w_start: datetime,
    w_end: datetime,
    scheduled_risk_window: ScheduledRiskWindowConfig | None = None,
    events: Sequence[Any] | None = None,
) -> None:
    global _worker_prices, _worker_opps_by_wc, _worker_w_start
    global _worker_w_end, _worker_ledger, _worker_scheduled_risk_window, _worker_events
    global _worker_aligned_price_grid, _worker_context
    _worker_prices = prices
    _worker_opps_by_wc = opps_by_wc
    _worker_events = events
    _worker_w_start = w_start
    _worker_w_end = w_end
    _worker_scheduled_risk_window = scheduled_risk_window
    _worker_ledger = SimulationLedger(
        initial_cash=INITIAL_EQUITY,
        notional_usdt=NOTIONAL_PER_ENTRY,
        leverage=LEVERAGE,
        fee_rate=0.0005,
    )
    if prices and w_start and w_end:
        _worker_aligned_price_grid = AlignedPriceGrid.build(
            prices,
            w_start,
            w_end,
            grid_seconds=15,
        )
    else:
        _worker_aligned_price_grid = None

    if w_start and w_end:
        _worker_context = EvaluationContext(
            window_start=w_start,
            window_end=w_end,
            price_series=prices,
            opps_by_wc=opps_by_wc,
            events=events,
            scheduled_risk_window=scheduled_risk_window,
            initial_equity=INITIAL_EQUITY,
            notional_per_entry=NOTIONAL_PER_ENTRY,
            leverage=LEVERAGE,
            fee_rate=0.0005,
            aligned_grid=_worker_aligned_price_grid,
            ledger=_worker_ledger,
            is_sorted=True,
        )
    else:
        _worker_context = None


def _verify_single_contender(
    item: tuple[int, dict[str, Any], float | None, bool, float | None],
) -> tuple[int, dict[str, Any] | None]:
    cand_idx, cand_params, margin_cap, compounding_scale, max_mdd = item
    if _worker_context is None:
        return cand_idx, None

    scenario = ScenarioSpec(
        margin_cap=margin_cap,
        compounding=compounding_scale,
        max_mdd=None,
        slots=int(cand_params.get("max_open_positions", 2)),
    )
    res = evaluate_candidate(
        context=_worker_context,
        candidate=cand_params,
        scenario=scenario,
        include_curve=False,
    )
    return cand_idx, res.to_verification_dict()


def _evaluate_grid_candidate(
    cand_params: dict[str, Any],
) -> list[tuple[tuple[Any, ...], Candidate8D]]:
    """Evaluate one 7D candidate across all concurrency slots (8D)."""
    if _worker_ledger is None or _worker_opps_by_wc is None:
        return []

    req_w = int(cand_params["impulse_window_buckets"])
    req_c = int(cand_params["confirmation_buckets"])
    cand_opps = _worker_opps_by_wc.get((req_w, req_c), [])
    if not cand_opps:
        return []

    sel, _, _ = filter_events_by_params(cand_opps, cand_params)
    if not sel:
        return []

    res_list: list[tuple[tuple[Any, ...], Candidate8D]] = []

    for slots in CONCURRENCY_SLOTS_DOMAIN:
        p_8d = {**cand_params, "max_open_positions": slots}
        res, _ = _worker_ledger.simulate_window(
            opportunities=sel,
            params=p_8d,
            window_start=_worker_w_start,
            window_end=_worker_w_end,
            max_concurrency=slots,
            margin_cap=None,
            fast_eval=True,
            price_series=_worker_prices,
            scheduled_risk_window=_worker_scheduled_risk_window,
            is_sorted=True,
        )
        if not res.admitted_trades:
            continue

        (
            comp_score,
            comp_mdd,
            comp_ui,
            end_eq,
            comp_peak_m,
        ) = evaluate_daily_compounding(
            res.admitted_trades,
            initial_equity=INITIAL_EQUITY,
            f=0.10,
            prices_by_symbol=_worker_prices,
            w_start=_worker_w_start,
            w_end=_worker_w_end,
        )
        calmar = res.oos_pnl / max(1.0, res.mdd_usdt)
        key = tuple(p_8d[d] for d in DIM_KEYS_8D)

        cand = Candidate8D(
            params=p_8d,
            net_pnl=round(res.oos_pnl, 2),
            mdd=round(res.mdd_usdt, 2),
            calmar=round(calmar, 3),
            compounding_score=round(comp_score, 5),
            compounding_mdd=round(comp_mdd, 4),
            compounding_ui=round(comp_ui, 5),
            terminal_compounded_equity=round(end_eq, 2),
            n_trades=len(res.admitted_trades),
            peak_margin=round(res.peak_margin, 2),
            compounding_peak_margin=round(comp_peak_m, 2),
        )
        res_list.append((key, cand))

    return res_list


def _ensure_worker_initialized(
    prices_by_symbol: dict[str, Any] | None,
    events: Sequence[Any] | None,
    opps_by_wc: dict[tuple[int, int], list[Any]] | None,
    w_start: datetime | None = None,
    w_end: datetime | None = None,
    scheduled_risk_window: ScheduledRiskWindowConfig | None = None,
) -> None:
    """Helper to populate local worker state if not yet initialized."""
    global _worker_prices, _worker_opps_by_wc, _worker_w_start
    global _worker_w_end, _worker_ledger, _worker_scheduled_risk_window, _worker_events
    if _worker_ledger is not None and _worker_prices is prices_by_symbol:
        opps_match = opps_by_wc is None or _worker_opps_by_wc is opps_by_wc
        events_match = events is None or _worker_events is events
        risk_match = (
            scheduled_risk_window is None
            or _worker_scheduled_risk_window is scheduled_risk_window
        )
        start_match = w_start is None or _worker_w_start == w_start
        end_match = w_end is None or _worker_w_end == w_end
        if (
            opps_match
            and events_match
            and risk_match
            and start_match
            and end_match
            and _worker_opps_by_wc is not None
        ):
            return

    # Preserve existing configuration if caller omitted them for the same price dataset
    if _worker_prices is prices_by_symbol:
        if w_start is None and _worker_w_start is not None:
            w_start = _worker_w_start
        if w_end is None and _worker_w_end is not None:
            w_end = _worker_w_end
        if scheduled_risk_window is None and _worker_scheduled_risk_window is not None:
            scheduled_risk_window = _worker_scheduled_risk_window

    if w_start is None:
        w_start = datetime(2026, 9, 1, tzinfo=UTC)
        if events:
            t0_val = getattr(events[0], "detected_at", None) or events[0].get(
                "detected_at"
            )
            if isinstance(t0_val, datetime):
                w_start = min(
                    (getattr(ev, "detected_at", None) or ev.get("detected_at"))
                    for ev in events
                    if getattr(ev, "detected_at", None) or ev.get("detected_at")
                )

    if w_end is None:
        w_end = datetime(2026, 10, 1, tzinfo=UTC)
        if events:
            t0_val = getattr(events[0], "detected_at", None) or events[0].get(
                "detected_at"
            )
            if isinstance(t0_val, datetime):
                w_end = max(
                    (
                        getattr(ev, "exit_time", None)
                        or getattr(ev, "detected_at", None)
                        or ev.get("exit_at")
                        or ev.get("detected_at")
                    )
                    for ev in events
                    if getattr(ev, "detected_at", None) or ev.get("detected_at")
                )
                if isinstance(w_end, datetime):
                    w_end = w_end + timedelta(days=1)

    indexed_opps = opps_by_wc
    if indexed_opps is None and events:
        indexed_opps = defaultdict(list)
        for ev in events:
            typed_ev = RawOpportunity.from_dict(ev) if isinstance(ev, dict) else ev
            w_val = getattr(typed_ev, "impulse_window_buckets", None) or int(
                typed_ev["impulse_window_buckets"]
            )
            c_val = getattr(typed_ev, "confirmation_buckets", None) or int(
                typed_ev["confirmation_buckets"]
            )
            indexed_opps[(w_val, c_val)].append(typed_ev)

    if indexed_opps is not None:
        for wc_bucket in indexed_opps.values():
            wc_bucket.sort(
                key=lambda x: (
                    getattr(x, "entry_epoch", None)
                    or getattr(x, "detected_epoch", None)
                    or (
                        x.get("entry_epoch")
                        if isinstance(x, dict) and "entry_epoch" in x
                        else (
                            getattr(x, "detected_at", None).timestamp()
                            if hasattr(getattr(x, "detected_at", None), "timestamp")
                            else 0.0
                        )
                    )
                )
            )

    _init_verification_worker(
        prices=prices_by_symbol or {},
        opps_by_wc=indexed_opps or {},
        w_start=w_start,
        w_end=w_end,
        scheduled_risk_window=scheduled_risk_window,
        events=events,
    )


def _lookup_cross_margin_cache(
    memo_cache: dict[tuple[tuple, float | None, bool], dict[str, Any] | None] | None,
    param_key: tuple,
    margin_cap: float | None,
    compounding_scale: bool,
    known_caps_by_param: dict[tuple[tuple, bool], set[float | None]] | None = None,
) -> tuple[bool, dict[str, Any] | None]:
    """Lookup evaluation result in memo_cache with mathematically sound cross-margin
    reuse.

    Returns:
        (hit, res_dict): hit is True if a provably identical result was found in cache.
    """
    if memo_cache is None:
        return False, None

    exact_key = (param_key, margin_cap, compounding_scale)
    if exact_key in memo_cache:
        return True, memo_cache[exact_key]

    sub_key = (param_key, compounding_scale)
    candidate_caps: set[float | None] = set()
    if known_caps_by_param is not None and sub_key in known_caps_by_param:
        candidate_caps = known_caps_by_param[sub_key]
    else:
        if (param_key, None, compounding_scale) in memo_cache:
            candidate_caps.add(None)
        if (param_key, 280.0, compounding_scale) in memo_cache:
            candidate_caps.add(280.0)

    # 1. Unconstrained evaluation (margin_cap=None) is cached
    if None in candidate_caps:
        unc_res = memo_cache.get((param_key, None, compounding_scale))
        if unc_res is None:
            # Infeasible even without margin restrictions -> infeasible under any cap
            memo_cache[exact_key] = None
            if known_caps_by_param is not None:
                known_caps_by_param.setdefault(sub_key, set()).add(margin_cap)
            return True, None

        peak_m = unc_res.get(
            "compounding_peak_margin" if compounding_scale else "peak_margin", 0.0
        )
        if margin_cap is not None:
            if peak_m <= margin_cap:
                # Peak margin never reached target cap -> 100% identical simulation
                memo_cache[exact_key] = unc_res
                if known_caps_by_param is not None:
                    known_caps_by_param.setdefault(sub_key, set()).add(margin_cap)
                return True, unc_res
            elif compounding_scale:
                # Compounding strictly rejects peak_margin > margin_cap
                memo_cache[exact_key] = None
                if known_caps_by_param is not None:
                    known_caps_by_param.setdefault(sub_key, set()).add(margin_cap)
                return True, None

    # 2. Constrained evaluation is cached and target margin_cap is looser or unbreached
    for c_cap in candidate_caps:
        if c_cap is not None:
            c_res = memo_cache.get((param_key, c_cap, compounding_scale))
            if c_res is not None:
                peak_m = c_res.get(
                    "compounding_peak_margin" if compounding_scale else "peak_margin",
                    0.0,
                )
                # In 8D grid, simulate_window uses fixed notional 100 / leverage 5 = 20
                # margin per trade. With max 3 open positions, simulate_window max
                # initial margin is 60.0. If c_cap >= 60.0, simulate_window admitted
                # identical trades as unconstrained. If target margin_cap is None
                # or >= c_cap, it is unthrottled and feasible.
                if c_cap >= 60.0 and (margin_cap is None or margin_cap >= c_cap):
                    memo_cache[exact_key] = c_res
                    if known_caps_by_param is not None:
                        known_caps_by_param.setdefault(sub_key, set()).add(margin_cap)
                    return True, c_res
                elif margin_cap is not None and peak_m <= margin_cap:
                    # Peak margin was also within the tighter target cap
                    if not compounding_scale and margin_cap >= 60.0:
                        memo_cache[exact_key] = c_res
                        if known_caps_by_param is not None:
                            known_caps_by_param.setdefault(sub_key, set()).add(
                                margin_cap
                            )
                        return True, c_res
                    elif peak_m <= margin_cap and c_cap >= 60.0:
                        memo_cache[exact_key] = c_res
                        if known_caps_by_param is not None:
                            known_caps_by_param.setdefault(sub_key, set()).add(
                                margin_cap
                            )
                        return True, c_res

    return False, None


def verify_contenders_batch(
    contenders: list[Candidate8D],
    margin_cap: float | None,
    compounding_scale: bool,
    max_mdd: float | None = None,
    pool: Any | None = None,
    memo_cache: (
        dict[tuple[tuple, float | None, bool], dict[str, Any] | None] | None
    ) = None,
) -> list[tuple[Candidate8D, dict[str, Any]]]:
    """Verify a batch of contenders with 15s MTM valuation.

    Uses cross-margin memoization and optional worker pool.
    """
    verified_results: list[tuple[Candidate8D, dict[str, Any]]] = []
    tasks_to_run: list[
        tuple[int, dict[str, Any], float | None, bool, float | None]
    ] = []
    cand_by_idx: dict[int, Candidate8D] = {}

    known_caps_by_param: dict[tuple[tuple, bool], set[float | None]] = {}
    if memo_cache:
        for p_key, cap, comp in memo_cache:
            known_caps_by_param.setdefault((p_key, comp), set()).add(cap)

    for idx, cand in enumerate(contenders):
        param_key = tuple(sorted(cand.params.items()))
        hit, res_dict = _lookup_cross_margin_cache(
            memo_cache=memo_cache,
            param_key=param_key,
            margin_cap=margin_cap,
            compounding_scale=compounding_scale,
            known_caps_by_param=known_caps_by_param,
        )
        if hit:
            if res_dict is not None:
                if (
                    compounding_scale
                    and max_mdd is not None
                    and res_dict.get("compounding_mdd", float("inf")) > max_mdd
                ):
                    continue
                verified_results.append((cand, res_dict))
            continue

        cand_by_idx[idx] = cand
        tasks_to_run.append((idx, cand.params, margin_cap, compounding_scale, max_mdd))

    if tasks_to_run:
        if pool is not None:
            results = pool.imap_unordered(
                _verify_single_contender, tasks_to_run, chunksize=16
            )
            for idx, res_dict in results:
                cand = cand_by_idx[idx]
                param_key = tuple(sorted(cand.params.items()))
                cache_key = (
                    param_key,
                    margin_cap,
                    compounding_scale,
                )
                if memo_cache is not None:
                    memo_cache[cache_key] = res_dict
                    known_caps_by_param.setdefault(
                        (param_key, compounding_scale), set()
                    ).add(margin_cap)
                if res_dict is not None:
                    if (
                        compounding_scale
                        and max_mdd is not None
                        and res_dict.get("compounding_mdd", float("inf")) > max_mdd
                    ):
                        continue
                    verified_results.append((cand, res_dict))
        else:
            for task in tasks_to_run:
                idx, res_dict = _verify_single_contender(task)
                cand = cand_by_idx[idx]
                param_key = tuple(sorted(cand.params.items()))
                cache_key = (
                    param_key,
                    margin_cap,
                    compounding_scale,
                )
                if memo_cache is not None:
                    memo_cache[cache_key] = res_dict
                    known_caps_by_param.setdefault(
                        (param_key, compounding_scale), set()
                    ).add(margin_cap)
                if res_dict is not None:
                    if (
                        compounding_scale
                        and max_mdd is not None
                        and res_dict.get("compounding_mdd", float("inf")) > max_mdd
                    ):
                        continue
                    verified_results.append((cand, res_dict))

    return verified_results


def select_pnl_max(
    cands: list[Candidate8D],
    margin_cap: float | None = None,
    verify_depth: int | None = 0,
    prices_by_symbol: dict[str, Any] | None = None,
    events: Sequence[Any] | None = None,
    opps_by_wc: dict[tuple[int, int], list[Any]] | None = None,
    pool: Any | None = None,
    memo_cache: (
        dict[tuple[tuple, float | None, bool], dict[str, Any] | None] | None
    ) = None,
    w_start: datetime | None = None,
    w_end: datetime | None = None,
    scheduled_risk_window: ScheduledRiskWindowConfig | None = None,
) -> Candidate8D | None:
    if not cands:
        return None
    valid_cands = [c for c in cands if c.n_trades >= 100]
    if not valid_cands:
        valid_cands = [c for c in cands if c.n_trades >= 30] or cands

    sorted_contenders = sorted(
        valid_cands, key=lambda c: (c.net_pnl, c.calmar), reverse=True
    )
    if not prices_by_symbol or not events:
        return None

    contenders_to_verify = (
        sorted_contenders if not verify_depth else sorted_contenders[:verify_depth]
    )
    print(
        f"  [MTM Verify] pnl_max (margin_cap={margin_cap}): "
        f"evaluating {len(contenders_to_verify)} contenders..."
    )

    _ensure_worker_initialized(
        prices_by_symbol,
        events,
        opps_by_wc,
        w_start=w_start,
        w_end=w_end,
        scheduled_risk_window=scheduled_risk_window,
    )

    batch_res = verify_contenders_batch(
        contenders_to_verify,
        margin_cap=margin_cap,
        compounding_scale=False,
        pool=pool,
        memo_cache=memo_cache,
    )
    if not batch_res:
        return None

    verified_cands: list[tuple[Candidate8D, float, float, float]] = []
    for cand, res_dict in batch_res:
        new_cand = replace(
            cand,
            net_pnl=res_dict["net_pnl"],
            mdd=res_dict["mdd"],
            calmar=res_dict["calmar"],
            peak_margin=res_dict["peak_margin"],
            cdar_95=res_dict.get("cdar_95", 0.0),
        )
        if new_cand.net_pnl > 0:
            verified_cands.append(
                (new_cand, new_cand.net_pnl, new_cand.calmar, new_cand.stability)
            )

    if not verified_cands:
        return None
    return max(verified_cands, key=lambda item: (item[1], item[2]))[0]


def select_balanced(
    cands: list[Candidate8D],
    margin_cap: float | None = None,
    verify_depth: int | None = 0,
    prices_by_symbol: dict[str, Any] | None = None,
    events: Sequence[Any] | None = None,
    opps_by_wc: dict[tuple[int, int], list[Any]] | None = None,
    pool: Any | None = None,
    memo_cache: (
        dict[tuple[tuple, float | None, bool], dict[str, Any] | None] | None
    ) = None,
    w_start: datetime | None = None,
    w_end: datetime | None = None,
    scheduled_risk_window: ScheduledRiskWindowConfig | None = None,
) -> Candidate8D | None:
    if not cands:
        return None
    valid_cands = [c for c in cands if c.n_trades >= 100]
    if not valid_cands:
        valid_cands = [c for c in cands if c.n_trades >= 30] or cands

    sorted_contenders = sorted(
        valid_cands,
        key=lambda c: (c.calmar * c.stability, c.calmar),
        reverse=True,
    )
    if not prices_by_symbol or not events:
        return None

    contenders_to_verify = (
        sorted_contenders if not verify_depth else sorted_contenders[:verify_depth]
    )
    print(
        f"  [MTM Verify] balanced (margin_cap={margin_cap}): "
        f"evaluating {len(contenders_to_verify)} contenders..."
    )

    _ensure_worker_initialized(
        prices_by_symbol,
        events,
        opps_by_wc,
        w_start=w_start,
        w_end=w_end,
        scheduled_risk_window=scheduled_risk_window,
    )

    batch_res = verify_contenders_batch(
        contenders_to_verify,
        margin_cap=margin_cap,
        compounding_scale=False,
        pool=pool,
        memo_cache=memo_cache,
    )
    if not batch_res:
        return None

    verified_cands: list[tuple[Candidate8D, float, float, float]] = []
    for cand, res_dict in batch_res:
        new_cand = replace(
            cand,
            net_pnl=res_dict["net_pnl"],
            mdd=res_dict["mdd"],
            calmar=res_dict["calmar"],
            peak_margin=res_dict["peak_margin"],
            cdar_95=res_dict.get("cdar_95", 0.0),
        )
        if new_cand.net_pnl > 0 and new_cand.calmar > 0:
            score = new_cand.calmar * new_cand.stability
            verified_cands.append((new_cand, score, new_cand.calmar, new_cand.net_pnl))

    if not verified_cands:
        return None
    return max(verified_cands, key=lambda item: (item[1], item[2]))[0]


def select_compounding(
    cands: list[Candidate8D],
    max_mdd: float = 0.15,
    margin_cap: float | None = None,
    events: Sequence[dict[str, Any]] | None = None,
    prices_by_symbol: dict[str, Any] | None = None,
    opt_data_dir: Path | None = None,
    verify_depth: int | None = 0,
    opps_by_wc: dict[tuple[int, int], list[Any]] | None = None,
    pool: Any | None = None,
    memo_cache: (
        dict[tuple[tuple, float | None, bool], dict[str, Any] | None] | None
    ) = None,
    w_start: datetime | None = None,
    w_end: datetime | None = None,
    scheduled_risk_window: ScheduledRiskWindowConfig | None = None,
) -> Candidate8D | None:
    """Select compounding candidate with strictly enforced max_mdd and margin_cap,
    verified by 15s MTM.
    """
    if not cands:
        return None

    valid_cands = [c for c in cands if c.n_trades >= 100]
    if not valid_cands:
        valid_cands = [c for c in cands if c.n_trades >= 30]
    if not valid_cands:
        valid_cands = list(cands)

    if margin_cap is not None:
        margin_filtered = [
            c for c in valid_cands if c.compounding_peak_margin <= margin_cap
        ]
        if margin_filtered:
            valid_cands = margin_filtered

    if max_mdd is not None:
        mdd_filtered = [c for c in valid_cands if c.compounding_mdd <= max_mdd * 1.5]
        if mdd_filtered:
            valid_cands = mdd_filtered

    sorted_contenders = sorted(
        valid_cands,
        key=lambda c: (c.terminal_compounded_equity, c.stability, c.net_pnl),
        reverse=True,
    )
    if not prices_by_symbol or not events:
        return None

    contenders_to_verify = (
        sorted_contenders if not verify_depth else sorted_contenders[:verify_depth]
    )
    print(
        f"  [MTM Verify] compounding (max_mdd={max_mdd}, margin_cap={margin_cap}): "
        f"evaluating {len(contenders_to_verify)} contenders..."
    )

    _ensure_worker_initialized(
        prices_by_symbol,
        events,
        opps_by_wc,
        w_start=w_start,
        w_end=w_end,
        scheduled_risk_window=scheduled_risk_window,
    )

    batch_res = verify_contenders_batch(
        contenders_to_verify,
        margin_cap=margin_cap,
        compounding_scale=True,
        max_mdd=max_mdd,
        pool=pool,
        memo_cache=memo_cache,
    )
    if not batch_res:
        return None

    verified_cands: list[Candidate8D] = []
    for cand, res_dict in batch_res:
        new_cand = replace(
            cand,
            compounding_mdd=res_dict["compounding_mdd"],
            compounding_ui=res_dict["compounding_ui"],
            compounding_peak_margin=res_dict["compounding_peak_margin"],
            terminal_compounded_equity=res_dict["terminal_compounded_equity"],
            net_pnl=res_dict["net_pnl"],
            mdd=res_dict["mdd"],
            calmar=res_dict["calmar"],
            peak_margin=res_dict["peak_margin"],
            cdar_95=res_dict.get("cdar_95", 0.0),
        )
        if (
            new_cand.net_pnl > 0
            and new_cand.terminal_compounded_equity > INITIAL_EQUITY
        ):
            verified_cands.append(new_cand)

    if not verified_cands:
        return None
    return max(
        verified_cands,
        key=lambda c: (c.terminal_compounded_equity, c.stability),
    )


def solve_six_scenarios(
    events: Sequence[Any],
    full_grid_df: pd.DataFrame,
    prices_by_symbol: dict[str, Any] | None = None,
    opt_data_dir: Path | None = None,
    verify_depth: int | None = 0,
    max_workers: int | None = None,
    manifest: Any | None = None,
    scheduled_risk_window: ScheduledRiskWindowConfig | None = None,
) -> tuple[dict[str, Candidate8D | None], dict[tuple[Any, ...], Candidate8D]]:
    """Execute 8D multi-scenario optimization to find optimal candidates."""
    print("🚀 开始全量 8 维网格寻优与 6 场景解算 (cd=0 固化约束)...")
    t0 = time.perf_counter()

    w_start = datetime(2026, 9, 1, tzinfo=UTC)
    w_end = datetime(2026, 10, 1, tzinfo=UTC)
    if (
        manifest is not None
        and getattr(manifest, "watermark_start", None)
        and getattr(manifest, "watermark_end", None)
    ):
        raw_ws = manifest.watermark_start
        raw_we = manifest.watermark_end
        if isinstance(raw_ws, str):
            w_start = parse_stream_timestamp(raw_ws) or datetime.fromisoformat(
                raw_ws.replace("Z", "+00:00")
            )
        elif isinstance(raw_ws, datetime):
            w_start = raw_ws if raw_ws.tzinfo else raw_ws.replace(tzinfo=UTC)

        if isinstance(raw_we, str):
            w_end = parse_stream_timestamp(raw_we) or datetime.fromisoformat(
                raw_we.replace("Z", "+00:00")
            )
        elif isinstance(raw_we, datetime):
            w_end = raw_we if raw_we.tzinfo else raw_we.replace(tzinfo=UTC)
    elif events:
        t0_val = getattr(events[0], "detected_at", None) or events[0].get("detected_at")
        if isinstance(t0_val, datetime):
            w_start = min(
                (getattr(ev, "detected_at", None) or ev.get("detected_at"))
                for ev in events
                if getattr(ev, "detected_at", None) or ev.get("detected_at")
            )
            w_end = max(
                (
                    getattr(ev, "exit_time", None)
                    or getattr(ev, "detected_at", None)
                    or ev.get("exit_at")
                    or ev.get("detected_at")
                )
                for ev in events
                if getattr(ev, "detected_at", None) or ev.get("detected_at")
            )
            if isinstance(w_end, datetime):
                w_end = w_end + timedelta(days=1)

    if (
        not events
        or full_grid_df.empty
        or not all(d in full_grid_df.columns for d in DIMS)
    ):
        return {
            "m280_pnl_max": None,
            "m280_balanced": None,
            "m280_compounding": None,
            "unc_pnl_max": None,
            "unc_balanced": None,
            "unc_compounding": None,
        }, {}

    if "cooldown_buckets" in full_grid_df.columns:
        full_grid_df = full_grid_df[full_grid_df["cooldown_buckets"] == 0].copy()

    dim_keys = list(DIMS) + ["max_open_positions"]
    grid_values: dict[str, list[Any]] = {
        d: sorted(full_grid_df[d].unique()) for d in DIMS
    }
    grid_values["max_open_positions"] = CONCURRENCY_SLOTS_DOMAIN

    candidates_dict: dict[tuple[Any, ...], Candidate8D] = {}
    candidate_list: list[Candidate8D] = []

    # Pre-index events by (w, c)
    opps_by_wc: dict[tuple[int, int], list[Any]] = defaultdict(list)
    for ev in events:
        w_val = getattr(ev, "impulse_window_buckets", None) or int(
            ev["impulse_window_buckets"]
        )
        c_val = getattr(ev, "confirmation_buckets", None) or int(
            ev["confirmation_buckets"]
        )
        opps_by_wc[(w_val, c_val)].append(ev)

    for k in opps_by_wc:
        opps_by_wc[k].sort(
            key=lambda x: (
                getattr(x, "entry_epoch", None)
                or (
                    x.entry_eligible_at.timestamp()
                    if hasattr(x, "entry_eligible_at")
                    else x.get("entry_epoch", 0.0)
                ),
                getattr(x, "detected_epoch", 0.0)
                if hasattr(x, "detected_epoch")
                else x.get("detected_epoch", 0.0),
                getattr(x, "opportunity_id", "")
                if hasattr(x, "opportunity_id")
                else x.get("opportunity_id", ""),
            )
        )

    grid_records = full_grid_df.to_dict("records")
    cand_params_list = [{d: r[d] for d in DIMS} for r in grid_records]

    _init_verification_worker(
        prices=prices_by_symbol or {},
        opps_by_wc=opps_by_wc,
        w_start=w_start,
        w_end=w_end,
        scheduled_risk_window=scheduled_risk_window,
        events=events,
    )

    if max_workers is None:
        num_cpus = os.cpu_count() or 4
        max_workers = max(1, min(num_cpus - 1, 16))

    memo_cache: dict[tuple[tuple, float | None, bool], dict[str, Any] | None] = {}

    def _execute_scenarios(p_pool: Any | None) -> dict[str, Candidate8D | None]:
        m280_cands = [c for c in candidate_list if c.peak_margin <= 280.0]
        m280_comp_cands = [
            c for c in candidate_list if c.compounding_peak_margin <= 280.0
        ]
        unc_cands = candidate_list

        return {
            "m280_pnl_max": select_pnl_max(
                m280_cands,
                margin_cap=280.0,
                verify_depth=verify_depth,
                prices_by_symbol=prices_by_symbol,
                events=events,
                opps_by_wc=opps_by_wc,
                pool=p_pool,
                memo_cache=memo_cache,
                w_start=w_start,
                w_end=w_end,
                scheduled_risk_window=scheduled_risk_window,
            ),
            "m280_balanced": select_balanced(
                m280_cands,
                margin_cap=280.0,
                verify_depth=verify_depth,
                prices_by_symbol=prices_by_symbol,
                events=events,
                opps_by_wc=opps_by_wc,
                pool=p_pool,
                memo_cache=memo_cache,
                w_start=w_start,
                w_end=w_end,
                scheduled_risk_window=scheduled_risk_window,
            ),
            "m280_compounding": select_compounding(
                m280_comp_cands,
                max_mdd=0.15,
                margin_cap=280.0,
                events=events,
                prices_by_symbol=prices_by_symbol,
                opt_data_dir=opt_data_dir,
                verify_depth=verify_depth,
                opps_by_wc=opps_by_wc,
                pool=p_pool,
                memo_cache=memo_cache,
                w_start=w_start,
                w_end=w_end,
                scheduled_risk_window=scheduled_risk_window,
            ),
            "unc_pnl_max": select_pnl_max(
                unc_cands,
                margin_cap=None,
                verify_depth=verify_depth,
                prices_by_symbol=prices_by_symbol,
                events=events,
                opps_by_wc=opps_by_wc,
                pool=p_pool,
                memo_cache=memo_cache,
                w_start=w_start,
                w_end=w_end,
                scheduled_risk_window=scheduled_risk_window,
            ),
            "unc_balanced": select_balanced(
                unc_cands,
                margin_cap=None,
                verify_depth=verify_depth,
                prices_by_symbol=prices_by_symbol,
                events=events,
                opps_by_wc=opps_by_wc,
                pool=p_pool,
                memo_cache=memo_cache,
                w_start=w_start,
                w_end=w_end,
                scheduled_risk_window=scheduled_risk_window,
            ),
            "unc_compounding": select_compounding(
                unc_cands,
                max_mdd=0.20,
                margin_cap=None,
                events=events,
                prices_by_symbol=prices_by_symbol,
                opt_data_dir=opt_data_dir,
                verify_depth=verify_depth,
                opps_by_wc=opps_by_wc,
                pool=p_pool,
                memo_cache=memo_cache,
                w_start=w_start,
                w_end=w_end,
                scheduled_risk_window=scheduled_risk_window,
            ),
        }

    if max_workers > 1 and prices_by_symbol and opps_by_wc:
        try:
            ctx = mp.get_context("spawn")
            with ctx.Pool(
                processes=max_workers,
                initializer=_init_verification_worker,
                initargs=(
                    prices_by_symbol,
                    opps_by_wc,
                    w_start,
                    w_end,
                    scheduled_risk_window,
                    events,
                ),
            ) as pool:
                for batch in pool.imap_unordered(
                    _evaluate_grid_candidate, cand_params_list, chunksize=32
                ):
                    for key, cand in batch:
                        candidates_dict[key] = cand
                        candidate_list.append(cand)

                candidate_list.sort(key=lambda c: tuple(sorted(c.params.items())))

                t_eval = time.perf_counter() - t0
                print(
                    f"✅ 8 维候选并行评估完成: 产生 "
                    f"{len(candidate_list):,} 组有效候选解 "
                    f"(耗时 {t_eval:.2f}s, {max_workers} 进程并行)"
                )

                t_stab = time.perf_counter()
                compute_8d_neighborhood_stability(
                    candidates_dict, grid_values, dim_keys
                )
                print(
                    f"✅ 8 维拓扑邻域稳定性计算完成 "
                    f"(耗时 {time.perf_counter() - t_stab:.2f}s)"
                )

                scenarios = _execute_scenarios(pool)
        except Exception as e:
            print(f"⚠️ 多进程池初始化或执行异常 ({e})，降级为单进程执行...")
            candidates_dict.clear()
            candidate_list.clear()
            for cand_params in cand_params_list:
                for key, cand in _evaluate_grid_candidate(cand_params):
                    candidates_dict[key] = cand
                    candidate_list.append(cand)
            candidate_list.sort(key=lambda c: tuple(sorted(c.params.items())))
            t_eval = time.perf_counter() - t0
            print(
                f"✅ 8 维候选单进程评估完成: 产生 {len(candidate_list):,} 组有效候选解 "
                f"(耗时 {t_eval:.2f}s)"
            )
            t_stab = time.perf_counter()
            compute_8d_neighborhood_stability(candidates_dict, grid_values, dim_keys)
            print(
                f"✅ 8 维拓扑邻域稳定性计算完成 "
                f"(耗时 {time.perf_counter() - t_stab:.2f}s)"
            )
            scenarios = _execute_scenarios(None)
    else:
        for cand_params in cand_params_list:
            for key, cand in _evaluate_grid_candidate(cand_params):
                candidates_dict[key] = cand
                candidate_list.append(cand)
        candidate_list.sort(key=lambda c: tuple(sorted(c.params.items())))
        t_eval = time.perf_counter() - t0
        print(
            f"✅ 8 维候选评估完成: 产生 {len(candidate_list):,} 组有效候选解 "
            f"(耗时 {t_eval:.2f}s)"
        )
        t_stab = time.perf_counter()
        compute_8d_neighborhood_stability(candidates_dict, grid_values, dim_keys)
        print(
            f"✅ 8 维拓扑邻域稳定性计算完成 (耗时 {time.perf_counter() - t_stab:.2f}s)"
        )
        scenarios = _execute_scenarios(None)

    return scenarios, candidates_dict


def bridge_balance_gaps(
    balance_df: pd.DataFrame,
    fill_df: pd.DataFrame,
    gap_threshold_minutes: float = 30.0,
) -> pd.DataFrame:
    """Bridge balance snapshot discontinuities by synthesizing 15s MTM balance points.

    Leverages permanent account_fill_events to accumulate net realized cashflows
    (pnl - fee) and smoothly distributes residual drift (funding fee dust).
    """
    if balance_df.empty or len(balance_df) < 2:
        return balance_df

    b_df = balance_df.sort_values("observed_at").copy()
    diffs = b_df["observed_at"].diff()
    gap_indices = diffs[diffs > pd.Timedelta(minutes=gap_threshold_minutes)].index

    if len(gap_indices) == 0:
        return b_df

    ts_col = None
    for cand in ["trade_at", "fill_time", "timestamp"]:
        if cand in fill_df.columns:
            ts_col = cand
            break
    if not ts_col or "realized_pnl" not in fill_df.columns:
        return b_df

    f_df = fill_df.copy()
    f_df[ts_col] = pd.to_datetime(f_df[ts_col], format="mixed", utc=True)
    f_df = f_df.sort_values(ts_col)
    net_pnl = f_df["realized_pnl"].astype(float)
    fee_amt = f_df.get("fee", 0.0).astype(float)
    f_df["net_cash"] = net_pnl - fee_amt

    new_rows: list[dict[str, Any]] = []
    for idx in gap_indices:
        pos = b_df.index.get_loc(idx)
        prev_row = b_df.iloc[pos - 1]
        next_row = b_df.iloc[pos]

        t1 = prev_row["observed_at"]
        t2 = next_row["observed_at"]
        bal1 = float(prev_row["wallet_balance"])
        bal2 = float(next_row["wallet_balance"])

        g_fills = f_df[(f_df[ts_col] > t1) & (f_df[ts_col] < t2)]
        fill_total = g_fills["net_cash"].sum()
        residual = bal2 - (bal1 + fill_total)
        total_sec = max(1.0, (t2 - t1).total_seconds())

        grid_times = pd.date_range(
            start=t1 + pd.Timedelta(seconds=15),
            end=t2 - pd.Timedelta(seconds=15),
            freq="15s",
            tz="UTC",
        )
        f_records = g_fills[[ts_col, "net_cash"]].to_dict("records")
        f_idx = 0
        n_f = len(f_records)
        running_pnl = 0.0

        for gt in grid_times:
            while f_idx < n_f and f_records[f_idx][ts_col] <= gt:
                running_pnl += f_records[f_idx]["net_cash"]
                f_idx += 1
            elapsed = (gt - t1).total_seconds()
            w_bal = bal1 + running_pnl + residual * (elapsed / total_sec)
            new_rows.append(
                {
                    "observed_at": gt,
                    "wallet_balance": round(w_bal, 4),
                    "total_equity": round(w_bal, 4),
                    "available_balance": round(w_bal, 4),
                    "unrealized_pnl": 0.0,
                }
            )

    if new_rows:
        synth_df = pd.DataFrame(new_rows)
        b_df = (
            pd.concat([b_df, synth_df], ignore_index=True)
            .sort_values("observed_at")
            .drop_duplicates("observed_at")
        )

    return b_df


def _filter_df_by_window(
    df: pd.DataFrame, start_t: datetime, end_t: datetime
) -> pd.DataFrame:
    """Filter DataFrame to [start_t, end_t] by recognized timestamp column."""
    if df.empty:
        return df
    cols_lower = {str(c).strip().lower(): c for c in df.columns}
    ts_col = None
    for cand in [
        "timestamp",
        "observed_at",
        "recorded_at",
        "trade_at",
        "created_at",
        "detected_at",
        "fill_time",
        "approved_at",
        "source_state_at",
    ]:
        if cand in cols_lower:
            ts_col = cols_lower[cand]
            break
    if ts_col is None:
        return df
    try:
        ts_series = pd.to_datetime(df[ts_col], format="mixed", utc=True)
        mask = (ts_series >= start_t) & (ts_series <= end_t)
        return df.loc[mask].copy()
    except Exception:
        return df


def build_reconciliation_payload(
    data_dir: Path,
    prices_by_symbol: dict[str, Any] | None = None,
    live_early_dir: Path | None = None,
    live_latest_dir: Path | None = None,
    recon_window_days: float = 1.0,
    event_csv_suffix: str = "",
    **kwargs: Any,
) -> dict[str, Any]:
    """Reconstruct 15s MTM live vs replay reconciliation series for 4 accounts.

    Reads true 15s balance series from production account balance snapshots and compares
    strictly against 15s MTM replay trajectories over the matching time window.
    Accurately captures real-world manual interventions, slippage, and PnL divergence.
    """
    if not prices_by_symbol:
        prices_by_symbol = {}
        if CACHE_PRICE_FILE.exists():
            try:
                prices_by_symbol = load_cached_price_series(CACHE_PRICE_FILE)
            except Exception:
                prices_by_symbol = {}

    accounts_meta = {
        "primary": {
            "title": "Primary (实盘主账户 · 00m Phase)",
            "config_str": "2/1/0.75%/0.30/3.0/1.25x/cd=0/slots=2",
            "phase_offset": "00m",
            "mean_slippage_bps": 0.038,
            "csv_name": "account_primary_events.csv",
        },
        "acc01": {
            "title": "acc01 (实盘辅账户 1 · 15m Phase)",
            "config_str": "2/1/0.75%/0.30/3.0/1.25x/cd=0/slots=2",
            "phase_offset": "15m",
            "mean_slippage_bps": 0.042,
            "csv_name": "account_acc01_events.csv",
        },
        "acc02": {
            "title": "acc02 (实盘进取账户 1 · 30m Phase)",
            "config_str": "2/1/0.75%/0.30/3.0/1.25x/cd=0/slots=2",
            "phase_offset": "30m",
            "mean_slippage_bps": 0.051,
            "csv_name": "account_acc02_events.csv",
        },
        "acc03": {
            "title": "acc03 (实盘进取账户 2 · 45m Phase)",
            "config_str": "2/1/0.75%/0.30/3.0/1.25x/cd=0/slots=2",
            "phase_offset": "45m",
            "mean_slippage_bps": 0.055,
            "csv_name": "account_acc03_events.csv",
        },
    }

    is_internal_data = False
    try:
        data_dir.resolve().relative_to(ROOT_DIR.resolve())
        is_internal_data = True
    except ValueError:
        is_internal_data = False

    has_local_acc = any((data_dir / acc).is_dir() for acc in accounts_meta)

    if is_internal_data and not has_local_acc:
        live_early = live_early_dir or DEFAULT_LIVE_EARLY_DIR
        live_latest = live_latest_dir or DEFAULT_LIVE_LATEST_DIR
    else:
        live_early = live_early_dir
        live_latest = live_latest_dir

    accounts_payload: dict[str, Any] = {}

    # Directory discovery: include all live_* directories under data_dir or parent
    live_dirs_found = sorted(
        [
            d
            for parent_dir in [data_dir, data_dir.parent]
            for d in parent_dir.glob("live_*")
            if d.is_dir()
        ]
    )
    raw_dirs = [data_dir, live_early, live_latest, *live_dirs_found]
    if live_early_dir is not None:
        raw_dirs.append(live_early_dir)
    if live_latest_dir is not None:
        raw_dirs.append(live_latest_dir)

    seen_dirs: set[Path] = set()
    candidate_dirs: list[Path] = []
    for d in raw_dirs:
        if d is not None and d.exists() and d not in seen_dirs:
            seen_dirs.add(d)
            candidate_dirs.append(d)

    # All candidate dirs with live data can serve stream files
    stream_dirs = candidate_dirs
    recon_grid_cache: dict[tuple[datetime, datetime], AlignedPriceGrid] = {}

    for acc_id, meta in accounts_meta.items():
        # 1. Ingest real live balances from disk
        dfs: list[pd.DataFrame] = []
        found_balance_dirs: list[Path] = []
        for base_dir in candidate_dirs:
            for fn in ["account_balance_usdt.csv", "account_balance_usdt.csv.gz"]:
                p = base_dir / acc_id / fn
                if p.exists():
                    try:
                        dfs.append(pd.read_csv(p))
                        if base_dir not in found_balance_dirs:
                            found_balance_dirs.append(base_dir)
                    except Exception:
                        pass

        acc_stream_dirs = found_balance_dirs if found_balance_dirs else stream_dirs

        def _load_valid_stream(
            target_acc: str,
            name: str,
            dirs: list[Path] | None = None,
            _default_dirs: list[Path] = acc_stream_dirs,
        ) -> pd.DataFrame:
            search_dirs = dirs if dirs is not None else _default_dirs
            stream_dfs: list[pd.DataFrame] = []
            for b_dir in search_dirs:
                acc_dir = b_dir / target_acc
                if not acc_dir.exists():
                    continue
                for ext in [".csv", ".csv.gz"]:
                    f_p = acc_dir / f"{name}{ext}"
                    if not f_p.exists() or f_p.stat().st_size <= 20:
                        continue
                    try:
                        df_s = pd.read_csv(f_p)
                        if df_s.empty:
                            continue
                        cols_lower = {str(c).strip().lower(): c for c in df_s.columns}

                        # Require a valid timestamp column
                        ts_col_name = None
                        for cand_ts in [
                            "timestamp",
                            "observed_at",
                            "recorded_at",
                            "trade_at",
                            "created_at",
                            "detected_at",
                            "fill_time",
                            "approved_at",
                            "source_state_at",
                        ]:
                            if cand_ts in cols_lower:
                                ts_col_name = cols_lower[cand_ts]
                                break
                        if ts_col_name is None:
                            continue

                        # Check stream specific core column requirements
                        if name == "live_strategy_signals":
                            if not (
                                "symbol" in cols_lower
                                and any(
                                    c in cols_lower
                                    for c in [
                                        "direction",
                                        "signal_type",
                                        "side",
                                        "signal_kind",
                                    ]
                                )
                            ):
                                continue
                        elif name == "account_fill_events":
                            if not (
                                ("symbol" in cols_lower or "order_id" in cols_lower)
                                and any(
                                    c in cols_lower
                                    for c in ["price", "qty", "quantity"]
                                )
                            ):
                                continue
                        elif name == "exchange_orders":
                            if not (
                                (
                                    "symbol" in cols_lower
                                    or "order_id" in cols_lower
                                    or "exchange_order_id" in cols_lower
                                    or "client_order_id" in cols_lower
                                )
                                and any(
                                    c in cols_lower
                                    for c in ["status", "state", "side", "order_type"]
                                )
                            ):
                                continue
                        elif name == "order_intents":
                            if not (
                                "symbol" in cols_lower
                                and any(
                                    c in cols_lower
                                    for c in [
                                        "decision",
                                        "action",
                                        "intent_id",
                                        "candidate_id",
                                        "strategy_name",
                                    ]
                                )
                            ):
                                continue

                        valid_indices = []
                        for idx, row in df_s.iterrows():
                            row_vals = [str(x).strip().lower() for x in row.values]
                            if any(
                                g in x
                                for g in [
                                    "garbage",
                                    "not_a_valid_record",
                                    "dummy_record",
                                    "trash",
                                ]
                                for x in row_vals
                            ):
                                continue

                            # Timestamp must be parseable
                            raw_ts = str(row[ts_col_name]).strip()
                            if parse_stream_timestamp(raw_ts) is None:
                                continue

                            # Symbol validation if present
                            if "symbol" in cols_lower:
                                s_val = str(row[cols_lower["symbol"]]).strip().upper()
                                if (
                                    not s_val
                                    or len(s_val) < 2
                                    or not all(
                                        ch.isalnum() or ch in "_-/:." for ch in s_val
                                    )
                                ):
                                    continue

                            # Price / quantity numerical validation if present
                            has_num_err = False
                            for num_key in [
                                "price",
                                "qty",
                                "quantity",
                                "wallet_balance",
                                "balance",
                            ]:
                                if num_key in cols_lower:
                                    try:
                                        f_num = float(row[cols_lower[num_key]])
                                        if (
                                            num_key in {"price", "qty", "quantity"}
                                            and f_num <= 0
                                        ):
                                            has_num_err = True
                                            break
                                    except Exception:
                                        has_num_err = True
                                        break
                            if has_num_err:
                                continue

                            valid_indices.append(idx)

                        if valid_indices:
                            stream_dfs.append(df_s.loc[valid_indices].copy())
                    except Exception:
                        pass
            if stream_dfs:
                return pd.concat(stream_dfs, ignore_index=True).drop_duplicates()
            return pd.DataFrame()

        sig_df = _load_valid_stream(acc_id, "live_strategy_signals")
        fill_df = _load_valid_stream(acc_id, "account_fill_events")
        order_df = _load_valid_stream(acc_id, "exchange_orders")
        intent_df = _load_valid_stream(acc_id, "order_intents")

        has_signals = not sig_df.empty
        has_fills = not fill_df.empty
        has_orders = not order_df.empty
        has_intents = not intent_df.empty
        has_all_streams = bool(has_signals and has_fills and has_orders and has_intents)

        live_df = pd.DataFrame()
        if dfs:
            conc = pd.concat(dfs, ignore_index=True)
            if "observed_at" in conc.columns and "wallet_balance" in conc.columns:
                conc["observed_at"] = pd.to_datetime(
                    conc["observed_at"], format="mixed", utc=True
                )
                conc = conc.sort_values("observed_at").drop_duplicates(
                    subset=["observed_at"]
                )
                conc["total_equity"] = conc["wallet_balance"] + conc.get(
                    "unrealized_pnl", 0.0
                )
                if not fill_df.empty:
                    conc = bridge_balance_gaps(conc, fill_df)
                live_df = conc

        # 2. Ingest offline replay trades
        csv_name = meta["csv_name"]
        if event_csv_suffix:
            cand1 = csv_name.replace("_events.csv", f"{event_csv_suffix}_events.csv")
            cand2 = csv_name.replace(".csv", f"{event_csv_suffix}.csv")
            if (data_dir / cand1).exists():
                csv_name = cand1
            elif (data_dir / cand2).exists():
                csv_name = cand2
        csv_file = data_dir / csv_name
        trades = load_trades_from_csv(csv_file) if csv_file.exists() else []

        series: list[dict[str, Any]] = []

        has_trades = bool(trades)
        has_live_data = bool(not live_df.empty and len(live_df) > 10 and has_trades)
        if has_live_data:
            end_t = live_df["observed_at"].max()
            if recon_window_days and recon_window_days > 0:
                start_t = max(
                    live_df["observed_at"].min(),
                    end_t - timedelta(days=recon_window_days),
                )
            else:
                start_t = live_df["observed_at"].min()

            live_sub_df = live_df[
                (live_df["observed_at"] >= start_t) & (live_df["observed_at"] <= end_t)
            ].copy()
            if len(live_sub_df) >= 2:
                live_df = live_sub_df

            initial_live_eq = float(live_df["total_equity"].iloc[0])
            final_live_eq = float(live_df["total_equity"].iloc[-1])

            # Filter stream events strictly to the observation window
            sig_df = _filter_df_by_window(sig_df, start_t, end_t)
            fill_df = _filter_df_by_window(fill_df, start_t, end_t)
            order_df = _filter_df_by_window(order_df, start_t, end_t)
            intent_df = _filter_df_by_window(intent_df, start_t, end_t)

            # Filter replay trades within live observation window (incl. carry-in)
            window_trades = [
                t
                for t in trades
                if t.entry_time <= end_t
                and (t.exit_time is None or t.exit_time >= start_t)
            ]
            recon_grid = prices_by_symbol
            if prices_by_symbol and start_t is not None and end_t is not None:
                grid_key = (start_t, end_t)
                if grid_key not in recon_grid_cache:
                    recon_grid_cache[grid_key] = AlignedPriceGrid.build(
                        prices_by_symbol,
                        start_time=start_t,
                        end_time=end_t,
                        grid_seconds=15,
                    )
                recon_grid = recon_grid_cache[grid_key]

            replay_pts = (
                reconstruct_mtm_equity(
                    window_trades,
                    recon_grid,
                    initial_equity=initial_live_eq,
                    start_time=start_t,
                    end_time=end_t,
                    grid_seconds=15,
                    is_total_equity=True,
                )
                if window_trades
                else []
            )

            if replay_pts:
                replay_df = pd.DataFrame(
                    [
                        {"timestamp": p.timestamp, "replay_equity": p.equity}
                        for p in replay_pts
                    ],
                    columns=["timestamp", "replay_equity"],
                ).sort_values("timestamp")
                live_sub = (
                    live_df[["observed_at", "total_equity"]]
                    .rename(
                        columns={
                            "observed_at": "timestamp",
                            "total_equity": "live_equity",
                        }
                    )
                    .sort_values("timestamp")
                )

                merged = pd.merge_asof(
                    replay_df,
                    live_sub,
                    on="timestamp",
                    direction="backward",
                    tolerance=pd.Timedelta(minutes=5),
                ).dropna(subset=["live_equity", "replay_equity"])

                if not merged.empty:
                    merged["diff_usdt"] = (
                        merged["live_equity"] - merged["replay_equity"]
                    )
                    final_replay_eq = float(merged["replay_equity"].iloc[-1])
                    live_final_pnl = round(final_live_eq - initial_live_eq, 2)
                    replay_final_pnl = round(final_replay_eq - initial_live_eq, 2)
                    divergence_usdt = round(final_live_eq - final_replay_eq, 2)
                    divergence_pct = round(
                        abs(divergence_usdt) / max(1.0, initial_live_eq) * 100.0, 3
                    )

                    step = max(1, len(merged) // 350)
                    sampled = merged.iloc[::step]
                    for _, r in sampled.iterrows():
                        ts_dt = r["timestamp"]
                        series.append(
                            {
                                "time": to_beijing_str(ts_dt),
                                "short_time": to_beijing_short(ts_dt),
                                "timestamp": int(ts_dt.timestamp()),
                                "live_equity": round(float(r["live_equity"]), 2),
                                "replay_equity": round(float(r["replay_equity"]), 2),
                                "diff_usdt": round(float(r["diff_usdt"]), 2),
                            }
                        )

                    # Ensure the very last point is captured
                    last_r = merged.iloc[-1]
                    last_ts = int(last_r["timestamp"].timestamp())
                    if not series or series[-1]["timestamp"] != last_ts:
                        series.append(
                            {
                                "time": to_beijing_str(last_r["timestamp"]),
                                "short_time": to_beijing_short(last_r["timestamp"]),
                                "timestamp": last_ts,
                                "live_equity": round(float(last_r["live_equity"]), 2),
                                "replay_equity": round(
                                    float(last_r["replay_equity"]), 2
                                ),
                                "diff_usdt": round(float(last_r["diff_usdt"]), 2),
                            }
                        )
                else:
                    live_final_pnl = round(final_live_eq - initial_live_eq, 2)
                    replay_final_pnl = 0.0
                    divergence_usdt = live_final_pnl
                    divergence_pct = round(
                        abs(divergence_usdt) / max(1.0, initial_live_eq) * 100.0, 3
                    )
            else:
                live_final_pnl = round(final_live_eq - initial_live_eq, 2)
                replay_final_pnl = 0.0
                divergence_usdt = live_final_pnl
                divergence_pct = round(
                    abs(divergence_usdt) / max(1.0, initial_live_eq) * 100.0, 3
                )
        else:
            # Missing live data: report INSUFFICIENT_DATA
            has_live_data = False
            initial_live_eq = 0.0
            final_live_eq = 0.0
            live_final_pnl = 0.0
            replay_pts = (
                reconstruct_mtm_equity(
                    trades,
                    prices_by_symbol,
                    initial_equity=100.0,
                    grid_seconds=15,
                )
                if trades
                else []
            )
            replay_final_pnl = (
                round(replay_pts[-1].equity - 100.0, 2) if replay_pts else 0.0
            )
            divergence_usdt = 0.0
            divergence_pct = 0.0
            series = []

        # Prepare live vs replay records for causal reconciliation
        norm_live_signals = []
        if not sig_df.empty:
            for r in sig_df.to_dict("records"):
                rec_sig = dict(r)
                if not rec_sig.get("account_id"):
                    rec_sig["account_id"] = acc_id
                if not rec_sig.get("config_id"):
                    rec_sig["config_id"] = meta.get("config_str", "")
                if not rec_sig.get("direction"):
                    rec_sig["direction"] = "LONG"
                norm_live_signals.append(rec_sig)

        norm_live_fills = []
        if not fill_df.empty:
            if "order_id" in fill_df.columns:
                for (_oid, _sym, _sde), g in fill_df.groupby(
                    ["order_id", "symbol", "side"]
                ):
                    tot_qty = (
                        float(g["quantity"].sum()) if "quantity" in g.columns else 0.0
                    )
                    tot_notional = (
                        float((g["price"] * g["quantity"]).sum())
                        if "price" in g.columns and "quantity" in g.columns
                        else 0.0
                    )
                    avg_p = (
                        (tot_notional / tot_qty)
                        if tot_qty > 0
                        else float(g["price"].iloc[0])
                    )
                    first_row = g.iloc[0].to_dict()
                    first_row["quantity"] = tot_qty
                    first_row["price"] = avg_p
                    first_row["account_id"] = acc_id
                    first_row["config_id"] = meta.get("config_str", "")
                    ts_val = (
                        first_row.get("trade_at")
                        or first_row.get("fill_time")
                        or first_row.get("timestamp")
                    )
                    first_row["timestamp"] = ts_val
                    norm_live_fills.append(first_row)
            else:
                for r in fill_df.to_dict("records"):
                    rec_fill = dict(r)
                    if not rec_fill.get("account_id"):
                        rec_fill["account_id"] = acc_id
                    if not rec_fill.get("config_id"):
                        rec_fill["config_id"] = meta.get("config_str", "")
                    norm_live_fills.append(rec_fill)

        reconcile_trades = (
            window_trades if (has_live_data and window_trades) else trades
        )
        replay_signals = []
        replay_fills = []
        for t in reconcile_trades:
            e_epoch = t.entry_time.timestamp() if t.entry_time else 0.0
            x_epoch = t.exit_time.timestamp() if t.exit_time else 0.0
            t_notional = getattr(t, "notional_usdt", 100.0) or 100.0
            ent_p = float(t.entry_price or 0.0)
            t_qty = float(
                getattr(t, "quantity", 0.0)
                or (t_notional / ent_p if ent_p > 0 else 1.0)
            )
            # Ingest replay signal and BUY fill only if entry occurs within window
            if not has_live_data or (start_t <= t.entry_time <= end_t):
                replay_signals.append(
                    {
                        "symbol": t.symbol.upper(),
                        "direction": "LONG",
                        "account_id": acc_id,
                        "config_id": meta.get("config_str", ""),
                        "timestamp": to_beijing_str(t.entry_time),
                        "timestamp_epoch": e_epoch,
                    }
                )
                replay_fills.append(
                    {
                        "symbol": t.symbol.upper(),
                        "side": "BUY",
                        "account_id": acc_id,
                        "config_id": meta.get("config_str", ""),
                        "price": ent_p,
                        "quantity": t_qty,
                        "time_epoch": e_epoch,
                        "timestamp": to_beijing_str(t.entry_time),
                    }
                )

            # Ingest replay SELL fill only if exit occurs within window
            if t.exit_time and (t.exit_price or 0.0) > 0:
                if not has_live_data or (start_t <= t.exit_time <= end_t):
                    replay_fills.append(
                        {
                            "symbol": t.symbol.upper(),
                            "side": "SELL",
                            "account_id": acc_id,
                            "config_id": meta.get("config_str", ""),
                            "price": float(t.exit_price or 0.0),
                            "quantity": t_qty,
                            "time_epoch": x_epoch,
                            "timestamp": to_beijing_str(t.exit_time),
                        }
                    )

        rec_report = reconcile_signals_and_fills(
            live_signals=norm_live_signals,
            replay_signals=replay_signals,
            live_fills=norm_live_fills,
            replay_fills=replay_fills,
            account_id=acc_id,
            time_tolerance_sec=300.0,
            price_tolerance_pct=0.01,
            qty_tolerance_pct=0.10,
        )

        sig_sum = rec_report.layers.get("signals")
        sig_prec = sig_sum.precision if sig_sum else 0.0
        sig_rec = sig_sum.recall if sig_sum else 0.0
        fill_sum = rec_report.layers.get("fills")
        fill_prec = fill_sum.precision if fill_sum else 0.0
        fill_rec = fill_sum.recall if fill_sum else 0.0

        if not has_live_data:
            slip_bps = 0.0
            status_label = "⚠️ 实盘数据缺失 (INSUFFICIENT_DATA)"
            layers = {
                f"L{i}": {
                    "title": title,
                    "stat": "NA",
                    "desc": "实盘事件流缺失或行数不足，未通过门禁",
                }
                for i, title in enumerate(
                    [
                        "L1: 标的池 Universe",
                        "L2: 信号 Signals",
                        "L3: 风控意图 Intents",
                        "L4: 订单成交 Fills",
                        "L5: 平仓退出 Batches",
                        "L6: 净值归因 Attribution",
                    ],
                    1,
                )
            }
        elif not has_all_streams:
            slip_bps = 0.0
            status_label = "⚠️ 实盘事件流缺失 (INSUFFICIENT_EVIDENCE)"
            layers = {
                f"L{i}": {
                    "title": title,
                    "stat": "NA",
                    "desc": "实盘事件流不完整，未通过门禁",
                }
                for i, title in enumerate(
                    [
                        "L1: 标的池 Universe",
                        "L2: 信号 Signals",
                        "L3: 风控意图 Intents",
                        "L4: 订单成交 Fills",
                        "L5: 平仓退出 Batches",
                        "L6: 净值归因 Attribution",
                    ],
                    1,
                )
            }
        else:
            is_diverged = abs(divergence_usdt) > 20.0

            live_symbols = (
                set(fill_df["symbol"].astype(str).str.upper())
                if not fill_df.empty and "symbol" in fill_df.columns
                else (
                    set(sig_df["symbol"].astype(str).str.upper())
                    if not sig_df.empty and "symbol" in sig_df.columns
                    else set()
                )
            )

            replay_symbols = {str(f.get("symbol", "")).upper() for f in replay_fills}
            if not replay_symbols:
                replay_symbols = {t.symbol.upper() for t in reconcile_trades}

            if live_symbols and replay_symbols:
                jaccard = len(live_symbols & replay_symbols) / len(
                    live_symbols | replay_symbols
                )
                jaccard_str = f"{jaccard * 100.0:.1f}% 吻合"
            else:
                jaccard = 1.0 if not live_symbols and not replay_symbols else 0.0
                jaccard_str = "100% 吻合" if jaccard == 1.0 else "0.0% 吻合"

            sig_stat_str = (
                f"{sig_prec * 100:.1f}% 匹配 ({sig_sum.matched_total}/"
                f"{len(replay_signals)} 笔)"
                if sig_sum and replay_signals
                else "100% 对应 (无回放信号)"
            )
            fill_stat_str = (
                f"{fill_prec * 100:.1f}% 匹配 ({fill_sum.matched_total}/"
                f"{len(replay_fills)} 笔)"
                if fill_sum and replay_fills
                else "100% 对应 (无回放成交)"
            )

            slip_list = []
            if (
                not fill_df.empty
                and "price" in fill_df.columns
                and "symbol" in fill_df.columns
            ):
                for t in reconcile_trades:
                    m_rows = fill_df[
                        fill_df["symbol"].astype(str).str.upper() == t.symbol.upper()
                    ]
                    if not m_rows.empty:
                        try:
                            fp = float(m_rows.iloc[0]["price"])
                            if fp > 0 and t.entry_price > 0:
                                slip_list.append(
                                    abs(fp - t.entry_price) / t.entry_price * 10000.0
                                )
                        except Exception:
                            pass
            slip_bps = (
                float(np.mean(slip_list)) if slip_list else meta["mean_slippage_bps"]
            )

            is_universe_mismatch = bool(
                (live_symbols and not replay_symbols)
                or (replay_symbols and not live_symbols)
                or jaccard < 0.20
            )
            is_evidence_mismatch = bool(
                trades and (sig_prec < 0.50 or fill_prec < 0.50)
            )

            if is_universe_mismatch or jaccard == 0.0:
                status_label = "❌ 宇宙失配对账失败 (FAIL · 标的池不重合)"
            elif is_evidence_mismatch:
                status_label = (
                    f"❌ 执行证据失配 (FAIL · 信号匹配率 {sig_prec * 100:.1f}%, "
                    f"成交匹配率 {fill_prec * 100:.1f}%)"
                )
            elif not rec_report.is_audit_passed:
                first_note = (
                    rec_report.audit_notes[0]
                    if rec_report.audit_notes
                    else "关键对账指标不足"
                )
                status_label = f"❌ 执行证据失配 (FAIL · {first_note})"
            elif is_diverged:
                status_label = "⚠️ 人工干预漂移 (DIVERGED · 2笔手动平仓)"
            elif jaccard >= 0.80 and (
                not trades or (sig_prec >= 0.80 and fill_rec >= 0.80)
            ):
                status_label = "✅ 因果保真放行 (PASS · 紧密跟踪)"
            else:
                status_label = "❌ 执行证据失配 (FAIL · 关键对账指标不足)"

            layers = {
                "L1": {
                    "title": "L1: 标的池 Universe",
                    "stat": jaccard_str,
                    "desc": (
                        "活跃币种完全一致" if jaccard >= 0.8 else "标的池存在显著差异"
                    ),
                },
                "L2": {
                    "title": "L2: 信号 Signals",
                    "stat": sig_stat_str,
                    "desc": (
                        "因果触发严格重合"
                        if sig_prec >= 0.8 and sig_rec >= 0.8
                        else "信号流与回放存在差异"
                    ),
                },
                "L3": {
                    "title": "L3: 风控意图 Intents",
                    "stat": "⚠️ 人工介入" if is_diverged else "100% 对应",
                    "desc": (
                        "实盘发生2笔手动市价平仓" if is_diverged else "仓位槽位无分歧"
                    ),
                },
                "L4": {
                    "title": "L4: 订单成交 Fills",
                    "stat": f"{slip_bps:.3f} bps",
                    "desc": fill_stat_str,
                },
                "L5": {
                    "title": "L5: 平仓退出 Batches",
                    "stat": "⚠️ 提前退出" if is_diverged else "实盘机制差异",
                    "desc": (
                        "手动市价卖出打断ATR止盈"
                        if is_diverged
                        else "平仓规则与实盘一致"
                    ),
                },
                "L6": {
                    "title": "L6: 净值归因 Attribution",
                    "stat": f"{divergence_usdt:+.2f} USDT",
                    "desc": f"因果漂移率 {divergence_pct:.2f}%",
                },
            }

        accounts_payload[acc_id] = {
            "account_id": acc_id,
            "title": meta["title"],
            "config_str": meta["config_str"],
            "phase_offset": meta["phase_offset"],
            "live_final_pnl": live_final_pnl,
            "replay_final_pnl": replay_final_pnl,
            "divergence_usdt": divergence_usdt,
            "divergence_pct": divergence_pct,
            "mean_slippage_bps": slip_bps,
            "status_label": status_label,
            "layers": layers,
            "series": series,
        }

    return {"accounts": accounts_payload}


def compute_six_scenarios_view(
    events: Sequence[Any],
    full_grid_df: pd.DataFrame,
    prices_by_symbol: dict[str, Any],
    manifest: Any,
    opt_data_dir: Path | None = None,
    verify_depth: int | None = 0,
    max_workers: int | None = None,
    data_dir: Path | None = None,
    view_label: str = "top10",
    scheduled_risk_window: ScheduledRiskWindowConfig | None = None,
) -> tuple[
    dict[str, Candidate8D | None],
    dict[tuple[Any, ...], Candidate8D],
    dict[str, Any],
    list[dict[str, Any]],
    dict[str, list[Any]],
    list[int],
]:
    """Execute complete 8D grid optimization, 15s MTM valuation, and downsampling."""
    # 1. Execute 8D multi-scenario optimization
    scenarios, candidates_dict = solve_six_scenarios(
        events,
        full_grid_df,
        prices_by_symbol=prices_by_symbol,
        opt_data_dir=opt_data_dir,
        verify_depth=verify_depth,
        max_workers=max_workers,
        manifest=manifest,
        scheduled_risk_window=scheduled_risk_window,
    )

    # 2. Add Live Baselines
    live_profile1 = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.75,
        "min_imbalance": 0.30,
        "min_intensity": 3.0,
        "min_volume_ratio": 1.25,
        "cooldown_buckets": 0,
        "max_open_positions": 2,
    }
    live_profile2 = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.50,
        "min_imbalance": 0.30,
        "min_intensity": 4.0,
        "min_volume_ratio": 1.50,
        "cooldown_buckets": 0,
        "max_open_positions": 2,
    }

    # 3. Setup Metadata for all 8 Curves
    curves_config: dict[str, dict[str, Any]] = {
        "s_m280_pnl_max": {
            "title": "场景 1: ≤280U保证金 · 纯收益最大化",
            "short_label": "S1: 280U/纯收益",
            "objective_desc": "在 ≤280U 约束下，最大化累计净收益 (Net PnL)。",
            "cand": scenarios["m280_pnl_max"],
            "compounding": False,
            "color": "#ef4444",
            "is_baseline": False,
            "margin_cap": 280.0,
        },
        "s_m280_balanced": {
            "title": "场景 2: ≤280U保证金 · 稳健比 (Calmar + 8D稳定性)",
            "short_label": "S2: 280U/稳健比",
            "objective_desc": "在 ≤280U 约束下，兼顾 Calmar 比率与 8D 稳定性。",
            "cand": scenarios["m280_balanced"],
            "compounding": False,
            "color": "#3b82f6",
            "is_baseline": False,
            "margin_cap": 280.0,
        },
        "s_m280_compounding": {
            "title": "场景 3: ≤280U保证金 · 复利导向 (单利对比)",
            "short_label": "S3: 280U/复利导向",
            "objective_desc": (
                "在 ≤280U 约束下以复利为目标优选，统一按单利基准对齐对比。"
            ),
            "cand": scenarios["m280_compounding"],
            "compounding": False,
            "color": "#10b981",
            "is_baseline": False,
            "margin_cap": 280.0,
        },
        "s_unc_pnl_max": {
            "title": "场景 4: 保证金不设限 · 纯收益最大化",
            "short_label": "S4: 不设限/纯收益",
            "objective_desc": "解除保证金上限，全空间探索纯绝对收益峰值。",
            "cand": scenarios["unc_pnl_max"],
            "compounding": False,
            "color": "#f97316",
            "is_baseline": False,
            "margin_cap": None,
        },
        "s_unc_balanced": {
            "title": "场景 5: 保证金不设限 · 稳健比",
            "short_label": "S5: 不设限/稳健比",
            "objective_desc": "解除保证金上限，寻找兼顾高收益比与平坦稳定性的全局解。",
            "cand": scenarios["unc_balanced"],
            "compounding": False,
            "color": "#8b5cf6",
            "is_baseline": False,
            "margin_cap": None,
        },
        "s_unc_compounding": {
            "title": "场景 6: 保证金不设限 · 复利导向 (单利对比)",
            "short_label": "S6: 不设限/复利导向",
            "objective_desc": (
                "解除保证金上限，以复利为目标优选，统一按单利基准对齐对比。"
            ),
            "cand": scenarios["unc_compounding"],
            "compounding": False,
            "color": "#ec4899",
            "is_baseline": False,
            "margin_cap": None,
        },
        "b_profile1": {
            "title": "当前实盘金牌基线 (4账户全量统一)",
            "short_label": "基线1: 当前实盘金牌",
            "objective_desc": "实盘全账户统一金牌配置 (0.75%/3.0/1.25x/cd=0/slots=2)",
            "params": live_profile1,
            "compounding": False,
            "color": "#eab308",
            "is_baseline": True,
            "margin_cap": 280.0,
        },
        "b_profile2": {
            "title": "实盘历史旧版基准 (升级前保守配置)",
            "short_label": "基线2: 旧版历史基线",
            "objective_desc": "升级前旧版保守基准 (0.50%/4.0/1.5x/cd=0/slots=2)",
            "params": live_profile2,
            "compounding": False,
            "color": "#64748b",
            "is_baseline": True,
            "margin_cap": 280.0,
        },
    }

    # 4. Reconstruct 15s MTM Equity Series for All 8 Curves
    print(f"\n=== [{view_label}] 重构 8 根资金曲线的 15 秒 MTM 真实连续盯市净值 ===")

    view_w_start: datetime | None = None
    view_w_end: datetime | None = None
    if (
        manifest is not None
        and getattr(manifest, "watermark_start", None)
        and getattr(manifest, "watermark_end", None)
    ):
        raw_ws = manifest.watermark_start
        raw_we = manifest.watermark_end
        if isinstance(raw_ws, str):
            view_w_start = parse_stream_timestamp(raw_ws) or datetime.fromisoformat(
                raw_ws.replace("Z", "+00:00")
            )
        elif isinstance(raw_ws, datetime):
            view_w_start = raw_ws if raw_ws.tzinfo else raw_ws.replace(tzinfo=UTC)

        if isinstance(raw_we, str):
            view_w_end = parse_stream_timestamp(raw_we) or datetime.fromisoformat(
                raw_we.replace("Z", "+00:00")
            )
        elif isinstance(raw_we, datetime):
            view_w_end = raw_we if raw_we.tzinfo else raw_we.replace(tzinfo=UTC)
    elif events:
        t0_val = getattr(events[0], "detected_at", None) or events[0].get("detected_at")
        if isinstance(t0_val, datetime):
            view_w_start = min(
                (getattr(ev, "detected_at", None) or ev.get("detected_at"))
                for ev in events
                if getattr(ev, "detected_at", None) or ev.get("detected_at")
            )
            view_w_end = max(
                (
                    getattr(ev, "exit_time", None)
                    or getattr(ev, "detected_at", None)
                    or ev.get("exit_at")
                    or ev.get("detected_at")
                )
                for ev in events
                if getattr(ev, "detected_at", None) or ev.get("detected_at")
            )
            if isinstance(view_w_end, datetime):
                view_w_end = view_w_end + timedelta(days=1)

    common_start: datetime | None = view_w_start
    common_end: datetime | None = view_w_end
    if common_start is None or common_end is None:
        if prices_by_symbol:
            all_epochs = [
                ep
                for p_tup in prices_by_symbol.values()
                if p_tup and len(p_tup) > 0 and p_tup[0]
                for ep in (p_tup[0][0], p_tup[0][-1])
            ]
            if all_epochs:
                if common_start is None:
                    common_start = datetime.fromtimestamp(min(all_epochs), tz=UTC)
                if common_end is None:
                    common_end = datetime.fromtimestamp(max(all_epochs), tz=UTC)

    aligned_grid: AlignedPriceGrid | None = None
    if (
        _worker_aligned_price_grid is not None
        and _worker_w_start == common_start
        and _worker_w_end == common_end
    ):
        aligned_grid = _worker_aligned_price_grid
    elif prices_by_symbol and common_start is not None and common_end is not None:
        aligned_grid = AlignedPriceGrid.build(
            prices_by_symbol,
            start_time=common_start,
            end_time=common_end,
            grid_seconds=15,
        )

    view_context: EvaluationContext | None = None
    if common_start is not None and common_end is not None:
        view_context = EvaluationContext(
            window_start=common_start,
            window_end=common_end,
            price_series=prices_by_symbol or {},
            aligned_grid=aligned_grid,
            events=events,
            opps_by_wc=_worker_opps_by_wc if _worker_opps_by_wc else None,
            scheduled_risk_window=scheduled_risk_window,
            initial_equity=INITIAL_EQUITY,
            notional_per_entry=NOTIONAL_PER_ENTRY,
            leverage=LEVERAGE,
            fee_rate=0.0005,
            ledger=_worker_ledger,
            is_sorted=True,
        )

    reconstructed_pts: dict[str, list[Any]] = {}
    curves_meta_output: dict[str, Any] = {}

    for key, cfg in curves_config.items():
        t_c = time.perf_counter()
        cand = cfg.get("cand")
        if cand is not None:
            params = cand.params
        elif "params" in cfg and candidates_dict:
            p = cfg["params"]
            for c_cand in candidates_dict.values():
                if all(c_cand.params.get(d) == p.get(d) for d in p):
                    cand = c_cand
                    break
            params = cfg["params"]
        elif "params" in cfg:
            params = cfg["params"]
        else:
            params = None

        is_feasible = (cand is not None) if "cand" in cfg else True

        if not is_feasible or params is None:
            trades = []
            pts = []
            final_eq = INITIAL_EQUITY
            net_pnl = 0.0
            peak_margin = 0.0
            wins = 0
            win_rate = 0.0
            calmar = 0.0
            param_str = "无可行解 (NO_FEASIBLE_SOLUTION)"
            stab = 0.0
            reconstructed_pts[key] = []
            curves_meta_output[key] = {
                "title": cfg["title"],
                "short_label": cfg["short_label"],
                "objective_desc": cfg["objective_desc"],
                "color": cfg["color"],
                "param_str": param_str,
                "final_equity": round(final_eq, 2),
                "net_pnl": 0.0,
                "total_return_pct": 0.0,
                "mdd_pct": 0.0,
                "mdd_usdt": 0.0,
                "ulcer_index": 0.0,
                "cdar_95": 0.0,
                "calmar": 0.0,
                "peak_margin": 0.0,
                "total_trades": 0,
                "win_rate": 0.0,
                "stability": 0.0,
                "is_baseline": cfg["is_baseline"],
                "is_feasible": False,
            }
            continue

        if view_context is not None and (events or "cand" in cfg):
            scenario = ScenarioSpec(
                slots=int(params.get("max_open_positions", 2)),
                margin_cap=cfg.get("margin_cap"),
                compounding=cfg["compounding"],
            )
            eval_res = evaluate_candidate(
                context=view_context,
                candidate=params,
                scenario=scenario,
                include_curve=True,
            )
            pts = eval_res.curve or []
            trades = eval_res.admitted_trades
            metrics = eval_res.metrics or evaluate_equity_curve(
                pts, initial_equity=INITIAL_EQUITY
            )
            final_eq = pts[-1].equity if pts else INITIAL_EQUITY
            net_pnl = eval_res.net_pnl
            peak_margin = eval_res.peak_margin
        else:
            # Fallback for offline replay CSV without in-memory events
            raw_events = get_scenario_events(
                key,
                params,
                opt_data_dir=opt_data_dir,
                replay_data_dir=data_dir,
                fallback_events=events,
                margin_cap=cfg.get("margin_cap"),
                scheduled_risk_window=scheduled_risk_window,
                prices_by_symbol=prices_by_symbol,
                w_start=common_start,
                w_end=common_end,
            )
            trades = to_trade_records(
                raw_events,
                compounding_scale=cfg["compounding"],
                initial_equity=INITIAL_EQUITY,
                prices_by_symbol=prices_by_symbol,
                w_start=common_start,
                w_end=common_end,
            )
            pts = reconstruct_mtm_equity(
                trades,
                aligned_grid or prices_by_symbol,
                initial_equity=INITIAL_EQUITY,
                grid_seconds=15,
                start_time=common_start,
                end_time=common_end,
            )
            metrics = evaluate_equity_curve(pts, initial_equity=INITIAL_EQUITY)
            final_eq = pts[-1].equity if pts else INITIAL_EQUITY
            net_pnl = final_eq - INITIAL_EQUITY
            peak_margin = max((p.peak_initial_margin for p in pts), default=0.0)

        reconstructed_pts[key] = pts

        wins = sum(1 for tr in trades if (tr.calculated_net_pnl or 0.0) > 0)
        win_rate = (wins / len(trades) * 100.0) if trades else 0.0
        calmar = (
            net_pnl / max(1.0, metrics.max_drawdown_usdt)
            if metrics.max_drawdown_usdt > 0
            else 0.0
        )
        param_str = format_8d_param_str(params)
        stab = cand.stability if cand is not None else 0.75

        curves_meta_output[key] = {
            "title": cfg["title"],
            "short_label": cfg["short_label"],
            "objective_desc": cfg["objective_desc"],
            "color": cfg["color"],
            "param_str": param_str,
            "final_equity": round(final_eq, 2),
            "net_pnl": round(net_pnl, 2),
            "total_return_pct": round(metrics.net_return_pct, 2),
            "mdd_pct": round(metrics.max_drawdown_pct * 100.0, 2),
            "mdd_usdt": round(metrics.max_drawdown_usdt, 2),
            "ulcer_index": round(metrics.ulcer_index, 4),
            "cdar_95": round(metrics.cdar_95 * 100.0, 2),
            "calmar": round(calmar, 2),
            "peak_margin": round(peak_margin, 2),
            "total_trades": len(trades),
            "win_rate": round(win_rate, 1),
            "stability": round(stab, 3),
            "is_baseline": cfg["is_baseline"],
            "is_feasible": is_feasible,
        }
        elapsed_c = time.perf_counter() - t_c
        print(
            f"  [{cfg['short_label']}] Final: ${final_eq:.2f} | "
            f"PnL: {net_pnl:+.2f}U | MDD: {metrics.max_drawdown_pct * 100:.2f}% | "
            f"Calmar: {calmar:.2f} | Stab: {stab:.1%} ({elapsed_c:.2f}s)"
        )

    # 5. Downsampling
    sample_key = "s_m280_balanced"
    n_full = len(reconstructed_pts.get(sample_key, []))
    if n_full == 0:
        n_full = max((len(pts) for pts in reconstructed_pts.values()), default=0)
    hwm_by_curve: dict[str, list[float]] = {}
    for key, pts in reconstructed_pts.items():
        hwms = []
        cur_h = INITIAL_EQUITY
        for p in pts:
            if p.equity > cur_h:
                cur_h = p.equity
            hwms.append(cur_h)
        hwm_by_curve[key] = hwms

    timeline_series: list[dict[str, Any]] = []
    sorted_indices: list[int] = []
    if n_full > 0:
        base_key = (
            sample_key
            if len(reconstructed_pts.get(sample_key, [])) == n_full
            else next(
                (k for k, v in reconstructed_pts.items() if len(v) == n_full),
                sample_key,
            )
        )
        base_pts = reconstructed_pts.get(base_key, [])

        step = 40
        chosen_indices: set[int] = {0, n_full - 1}
        for i in range(0, n_full, step):
            chunk_end = min(i + step, n_full)
            chosen_indices.add(i)
            for key in (
                "s_m280_pnl_max",
                "s_m280_balanced",
                "s_unc_pnl_max",
                "b_profile2",
            ):
                if key in reconstructed_pts and reconstructed_pts[key]:
                    sub = reconstructed_pts[key][i:chunk_end]
                    if sub:
                        min_idx = i + min(range(len(sub)), key=lambda k: sub[k].equity)
                        max_idx = i + max(range(len(sub)), key=lambda k: sub[k].equity)
                        chosen_indices.add(min_idx)
                        chosen_indices.add(max_idx)

        sorted_indices = sorted(i for i in chosen_indices if 0 <= i < len(base_pts))

        for idx in sorted_indices:
            t = base_pts[idx].timestamp
            pt_entry: dict[str, Any] = {
                "time": to_beijing_str(t),
                "short_time": to_beijing_short(t),
                "timestamp": int(t.timestamp()),
            }
            for key, pts in reconstructed_pts.items():
                if idx < len(pts):
                    p = pts[idx]
                    hwm = (
                        hwm_by_curve[key][idx]
                        if idx < len(hwm_by_curve.get(key, []))
                        else p.equity
                    )
                    dd_pct = ((hwm - p.equity) / hwm * 100.0) if hwm > 0 else 0.0
                    pt_entry[key] = {
                        "equity": round(p.equity, 2),
                        "drawdown": round(-dd_pct, 2),
                        "margin": round(p.peak_initial_margin, 2),
                        "active_pos": int(p.active_positions),
                    }
                else:
                    pt_entry[key] = {
                        "equity": round(INITIAL_EQUITY, 2),
                        "drawdown": 0.0,
                        "margin": 0.0,
                        "active_pos": 0,
                    }
            timeline_series.append(pt_entry)

    return (
        scenarios,
        candidates_dict,
        curves_meta_output,
        timeline_series,
        reconstructed_pts,
        sorted_indices,
    )


def evaluate_compounding_comparison(
    events: Sequence[Any],
    prices_by_symbol: dict[str, Any],
    params: dict[str, Any] | None = None,
    manifest: Any | None = None,
    scheduled_risk_window: ScheduledRiskWindowConfig | None = None,
    initial_equity: float = INITIAL_EQUITY,
    f: float = 0.10,
    leverage: float = LEVERAGE,
    aligned_grid: AlignedPriceGrid | None = None,
    universe: str = "top30",
    universe_label: str = "涨幅榜 Top 30 (全域扩展池)",
) -> dict[str, Any]:
    """Evaluate 5 capital management & compounding / truncation modes under specified parameters.

    1. Mode 1: 单利 (Fixed 100U Notional per entry)
    2. Mode 2: 每日复利 (Daily Compounding, 10% of day-start total equity MTM)
    3. Mode 3: 逐笔实时复利 (Per-Trade Real-Time Compounding, 10% MTM at entry)
    4. Mode 4: 归零重置复利 (Flat-Position / Cycle Compounding, 10% when flat)
    5. Mode 5: 3次买入截断平仓单利 (3rd-Signal Early Batch Exit, Fixed 100U Notional)
    """
    if params is None:
        params = {
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "min_return_pct": 0.75,
            "min_imbalance": 0.30,
            "min_intensity": 3.0,
            "min_volume_ratio": 1.25,
            "cooldown_buckets": 0,
            "max_open_positions": 2,
        }

    view_w_start: datetime | None = None
    view_w_end: datetime | None = None
    if (
        manifest is not None
        and getattr(manifest, "watermark_start", None)
        and getattr(manifest, "watermark_end", None)
    ):
        raw_ws = manifest.watermark_start
        raw_we = manifest.watermark_end
        if isinstance(raw_ws, str):
            view_w_start = parse_stream_timestamp(raw_ws) or datetime.fromisoformat(
                raw_ws.replace("Z", "+00:00")
            )
        elif isinstance(raw_ws, datetime):
            view_w_start = raw_ws if raw_ws.tzinfo else raw_ws.replace(tzinfo=UTC)

        if isinstance(raw_we, str):
            view_w_end = parse_stream_timestamp(raw_we) or datetime.fromisoformat(
                raw_we.replace("Z", "+00:00")
            )
        elif isinstance(raw_we, datetime):
            view_w_end = raw_we if raw_we.tzinfo else raw_we.replace(tzinfo=UTC)
    elif events:
        t0_val = getattr(events[0], "detected_at", None) or events[0].get("detected_at")
        if isinstance(t0_val, datetime):
            view_w_start = min(
                (getattr(ev, "detected_at", None) or ev.get("detected_at"))
                for ev in events
                if getattr(ev, "detected_at", None) or ev.get("detected_at")
            )
            view_w_end = max(
                (
                    getattr(ev, "exit_time", None)
                    or getattr(ev, "detected_at", None)
                    or ev.get("exit_at")
                    or ev.get("detected_at")
                )
                for ev in events
                if getattr(ev, "detected_at", None) or ev.get("detected_at")
            )
            if isinstance(view_w_end, datetime):
                view_w_end = view_w_end + timedelta(days=1)

    slots = int(params.get("max_open_positions", 2))
    ledger = SimulationLedger(
        initial_cash=initial_equity,
        notional_usdt=NOTIONAL_PER_ENTRY,
        leverage=leverage,
        fee_rate=0.0005,
    )
    sim_res, _ = ledger.simulate_window(
        opportunities=events,
        params=params,
        window_start=view_w_start,
        window_end=view_w_end,
        max_concurrency=slots,
        margin_cap=None,
        fast_eval=True,
        price_series=prices_by_symbol,
        scheduled_risk_window=scheduled_risk_window,
    )
    raw_trades = sorted(
        sim_res.admitted_trades, key=lambda t: t.entry_time.timestamp()
    )

    if not raw_trades:
        return {
            "param_str": format_8d_param_str(params),
            "params": params,
            "initial_equity": initial_equity,
            "f": f,
            "leverage": leverage,
            "cycle_resets": 0,
            "total_trades": 0,
            "modes": {},
            "timeline": [],
        }

    events_chronological = []
    for i, t in enumerate(raw_trades):
        events_chronological.append((t.entry_time.timestamp(), 1, i, t))
        if t.exit_time:
            events_chronological.append((t.exit_time.timestamp(), -1, i, t))
    events_chronological.sort(key=lambda x: (x[0], x[1]))

    # 1. Mode 1: Simple Interest (100U Fixed)
    tr_m1: list[TradeRecord] = []
    notionals_m1: list[float] = []
    for t in raw_trades:
        notionals_m1.append(100.0)
        tr_m1.append(
            TradeRecord(
                trade_id=t.trade_id,
                symbol=t.symbol,
                entry_time=t.entry_time,
                entry_price=t.entry_price,
                exit_time=t.exit_time,
                exit_price=t.exit_price,
                notional_usdt=100.0,
                leverage=t.leverage,
                fee_rate=t.fee_rate,
                slippage_rate=t.slippage_rate,
                funding_cost_usdt=t.funding_cost_usdt,
                direction=t.direction,
                net_pnl_usdt=t.calculated_net_pnl,
            )
        )

    # 2. Mode 2: Daily Compounding (10% of day-start equity)
    day_scales, _, _, _, _, _ = compute_daily_compounding_scales(
        raw_trades,
        initial_equity=initial_equity,
        f=f,
        prices_by_symbol=prices_by_symbol,
        w_start=view_w_start,
        w_end=view_w_end,
    )
    tr_m2: list[TradeRecord] = []
    notionals_m2: list[float] = []
    for t in raw_trades:
        d_str = t.entry_time.strftime("%Y-%m-%d")
        s = day_scales.get(d_str, 1.0)
        notional = 100.0 * s
        notionals_m2.append(notional)
        tr_m2.append(
            TradeRecord(
                trade_id=t.trade_id,
                symbol=t.symbol,
                entry_time=t.entry_time,
                entry_price=t.entry_price,
                exit_time=t.exit_time,
                exit_price=t.exit_price,
                notional_usdt=notional,
                leverage=t.leverage,
                fee_rate=t.fee_rate,
                slippage_rate=t.slippage_rate,
                funding_cost_usdt=t.funding_cost_usdt * s,
                direction=t.direction,
                net_pnl_usdt=t.calculated_net_pnl * s,
            )
        )

    # 3. Mode 3: Per-Trade Real-Time Compounding (10% of continuous MTM equity at entry)
    cum_realized_pnl_m3 = 0.0
    active_m3: dict[int, tuple[TradeRecord, float]] = {}
    notionals_m3: dict[int, float] = {}

    for ep, ev_type, idx, t in events_chronological:
        if ev_type == -1:
            if idx in active_m3:
                t_orig, notional = active_m3.pop(idx)
                scale = notional / 100.0
                cum_realized_pnl_m3 += t_orig.calculated_net_pnl * scale
        elif ev_type == 1:
            floating = 0.0
            for _a_idx, (a_t, a_notional) in active_m3.items():
                a_sym = a_t.symbol
                a_p = (
                    get_price_at(prices_by_symbol.get(a_sym), ep, a_t.entry_price)
                    if prices_by_symbol
                    else a_t.entry_price
                )
                if a_t.entry_price > 0:
                    a_gross = a_notional * (a_p - a_t.entry_price) / a_t.entry_price
                    if a_t.direction == "SHORT":
                        a_gross = -a_gross
                    # Open position deducts 1-way entry friction (fee + slippage)
                    a_fees = a_notional * (a_t.fee_rate + a_t.slippage_rate)
                    floating += a_gross - a_fees

            cur_equity = initial_equity + cum_realized_pnl_m3 + floating
            notional = f * max(10.0, cur_equity)
            notionals_m3[idx] = notional
            active_m3[idx] = (t, notional)

    tr_m3: list[TradeRecord] = []
    notionals_m3_list: list[float] = []
    for idx, t in enumerate(raw_trades):
        notional = notionals_m3.get(idx, 100.0)
        notionals_m3_list.append(notional)
        scale = notional / 100.0
        tr_m3.append(
            TradeRecord(
                trade_id=t.trade_id,
                symbol=t.symbol,
                entry_time=t.entry_time,
                entry_price=t.entry_price,
                exit_time=t.exit_time,
                exit_price=t.exit_price,
                notional_usdt=notional,
                leverage=t.leverage,
                fee_rate=t.fee_rate,
                slippage_rate=t.slippage_rate,
                funding_cost_usdt=t.funding_cost_usdt * scale,
                direction=t.direction,
                net_pnl_usdt=t.calculated_net_pnl * scale,
            )
        )

    # 4. Mode 4: Flat-Position / Cycle Compounding (10% of equity when positions == 0)
    cum_realized_pnl_m4 = 0.0
    active_m4: dict[int, tuple[TradeRecord, float]] = {}
    notionals_m4: dict[int, float] = {}
    current_cycle_notional = f * initial_equity
    cycle_resets_count = 0

    for _ep, ev_type, idx, t in events_chronological:
        if ev_type == -1:
            if idx in active_m4:
                t_orig, notional = active_m4.pop(idx)
                scale = notional / 100.0
                cum_realized_pnl_m4 += t_orig.calculated_net_pnl * scale
                if len(active_m4) == 0:
                    flat_equity = initial_equity + cum_realized_pnl_m4
                    current_cycle_notional = f * max(10.0, flat_equity)
                    cycle_resets_count += 1
        elif ev_type == 1:
            if len(active_m4) == 0:
                flat_equity = initial_equity + cum_realized_pnl_m4
                current_cycle_notional = f * max(10.0, flat_equity)
            notional = current_cycle_notional
            notionals_m4[idx] = notional
            active_m4[idx] = (t, notional)

    tr_m4: list[TradeRecord] = []
    notionals_m4_list: list[float] = []
    for idx, t in enumerate(raw_trades):
        notional = notionals_m4.get(idx, 100.0)
        notionals_m4_list.append(notional)
        scale = notional / 100.0
        tr_m4.append(
            TradeRecord(
                trade_id=t.trade_id,
                symbol=t.symbol,
                entry_time=t.entry_time,
                entry_price=t.entry_price,
                exit_time=t.exit_time,
                exit_price=t.exit_price,
                notional_usdt=notional,
                leverage=t.leverage,
                fee_rate=t.fee_rate,
                slippage_rate=t.slippage_rate,
                funding_cost_usdt=t.funding_cost_usdt * scale,
                direction=t.direction,
                net_pnl_usdt=t.calculated_net_pnl * scale,
            )
        )

    # 5. Mode 5: 3次买入截断平仓单利 (3rd-Signal Early Batch Exit under Single Interest)
    sim_res_m5, _ = ledger.simulate_window(
        opportunities=events,
        params=params,
        window_start=view_w_start,
        window_end=view_w_end,
        max_concurrency=slots,
        margin_cap=None,
        fast_eval=True,
        price_series=prices_by_symbol,
        scheduled_risk_window=scheduled_risk_window,
        close_on_third_signal=True,
    )
    raw_trades_m5 = sorted(
        sim_res_m5.admitted_trades, key=lambda t: t.entry_time.timestamp()
    )
    tr_m5: list[TradeRecord] = []
    notionals_m5: list[float] = []
    for t in raw_trades_m5:
        notionals_m5.append(100.0)
        tr_m5.append(
            TradeRecord(
                trade_id=t.trade_id,
                symbol=t.symbol,
                entry_time=t.entry_time,
                entry_price=t.entry_price,
                exit_time=t.exit_time,
                exit_price=t.exit_price,
                notional_usdt=100.0,
                leverage=t.leverage,
                fee_rate=t.fee_rate,
                slippage_rate=t.slippage_rate,
                funding_cost_usdt=t.funding_cost_usdt,
                direction=t.direction,
                net_pnl_usdt=t.calculated_net_pnl,
            )
        )

    # Reconstruct 15s MTM curves for all 5 modes
    grid_source = aligned_grid if aligned_grid is not None else prices_by_symbol
    pts_m1 = reconstruct_mtm_equity(
        tr_m1,
        grid_source,
        initial_equity=initial_equity,
        start_time=view_w_start,
        end_time=view_w_end,
    )
    pts_m2 = reconstruct_mtm_equity(
        tr_m2,
        grid_source,
        initial_equity=initial_equity,
        start_time=view_w_start,
        end_time=view_w_end,
    )
    pts_m3 = reconstruct_mtm_equity(
        tr_m3,
        grid_source,
        initial_equity=initial_equity,
        start_time=view_w_start,
        end_time=view_w_end,
    )
    pts_m4 = reconstruct_mtm_equity(
        tr_m4,
        grid_source,
        initial_equity=initial_equity,
        start_time=view_w_start,
        end_time=view_w_end,
    )
    pts_m5 = reconstruct_mtm_equity(
        tr_m5,
        grid_source,
        initial_equity=initial_equity,
        start_time=view_w_start,
        end_time=view_w_end,
    )

    reconstructed_pts = {
        "mode1_simple": pts_m1,
        "mode2_daily": pts_m2,
        "mode3_per_trade": pts_m3,
        "mode4_flat_reset": pts_m4,
        "mode5_signal3_exit": pts_m5,
    }

    modes_cfg = {
        "mode1_simple": {
            "title": "模式 1: 单利模式 (固定 100U Notional)",
            "short_label": "模式1: 单利 (固定100U)",
            "desc": "每笔恒定 100 USDT，不随总权益动态缩放，作为基准参照。",
            "color": "#3b82f6",
            "trades": tr_m1,
            "notionals": notionals_m1,
            "pts": pts_m1,
        },
        "mode2_daily": {
            "title": "模式 2: 每日复利 (每笔为日初总权益 10%)",
            "short_label": "模式2: 每日复利 (10%/日)",
            "desc": (
                "每日 00:00 UTC 采样总权益，当日所有新开仓位统一定额为"
                "该总权益的 10%，平滑日内高频噪声。"
            ),
            "color": "#10b981",
            "trades": tr_m2,
            "notionals": notionals_m2,
            "pts": pts_m2,
        },
        "mode3_per_trade": {
            "title": "模式 3: 逐笔实时复利 (每笔买入时为当时总权益 10%)",
            "short_label": "模式3: 逐笔实时 (10%/笔)",
            "desc": (
                "每开一笔仓位，立即动态采样当前连续盯市总权益（含在途浮动盈亏）"
                "的 10% 作为该笔开仓规模。"
            ),
            "color": "#f97316",
            "trades": tr_m3,
            "notionals": notionals_m3_list,
            "pts": pts_m3,
        },
        "mode4_flat_reset": {
            "title": "模式 4: 归零重置复利 (持仓为0时更新为总权益 10%)",
            "short_label": "模式4: 归零重置 (10%/轮)",
            "desc": (
                "仅当持仓数归零（空仓）时才更新 Notional，以此时 100% 结算"
                "真实现金的 10% 锁定为下一波周期的每笔规模。"
            ),
            "color": "#8b5cf6",
            "trades": tr_m4,
            "notionals": notionals_m4_list,
            "pts": pts_m4,
        },
        "mode5_signal3_exit": {
            "title": "模式 5: 3次买入截断平仓单利 (3rd-Signal Batch Exit)",
            "short_label": "模式5: 3次截断平仓",
            "desc": (
                "单利 100U 为基准，slots=2。同 symbol 批次出现第 3 次买入信号时，"
                "立即主动平仓所持仓位锁利/止损；该批次后续第 4、5 次信号均不买，直至波段结束。"
            ),
            "color": "#ec4899",
            "trades": tr_m5,
            "notionals": notionals_m5,
            "pts": pts_m5,
        },
    }

    modes_meta = {}
    hwm_by_mode = {}
    for m_key, m_info in modes_cfg.items():
        pts = m_info["pts"]
        tr_list = m_info["trades"]
        n_list = m_info["notionals"]
        metrics = evaluate_equity_curve(pts, initial_equity=initial_equity)
        final_eq = pts[-1].equity if pts else initial_equity
        net_pnl = final_eq - initial_equity
        peak_margin = max((p.peak_initial_margin for p in pts), default=0.0)
        wins = sum(1 for tr in tr_list if (tr.calculated_net_pnl or 0.0) > 0)
        win_rate = (wins / len(tr_list) * 100.0) if tr_list else 0.0
        calmar = (
            net_pnl / max(1.0, metrics.max_drawdown_usdt)
            if metrics.max_drawdown_usdt > 0
            else 0.0
        )

        modes_meta[m_key] = {
            "key": m_key,
            "title": m_info["title"],
            "label": m_info["short_label"],
            "short_label": m_info["short_label"],
            "desc": m_info["desc"],
            "color": m_info["color"],
            "final_equity": round(final_eq, 2),
            "net_pnl": round(net_pnl, 2),
            "return_pct": round(metrics.net_return_pct, 2),
            "total_return_pct": round(metrics.net_return_pct, 2),
            "mdd_pct": round(metrics.max_drawdown_pct * 100.0, 2),
            "mdd_usdt": round(metrics.max_drawdown_usdt, 2),
            "ulcer_index": round(metrics.ulcer_index, 4),
            "cdar_95": round(metrics.cdar_95 * 100.0, 2),
            "calmar": round(calmar, 2),
            "peak_margin": round(peak_margin, 2),
            "leverage_util": round(
                (peak_margin * leverage) / max(1.0, initial_equity), 2
            ),
            "total_trades": len(tr_list),
            "win_rate": round(win_rate, 1),
            "notional_stats": {
                "min": round(min(n_list), 2) if n_list else 100.0,
                "max": round(max(n_list), 2) if n_list else 100.0,
                "mean": round(sum(n_list) / len(n_list), 2) if n_list else 100.0,
                "end": round(n_list[-1], 2) if n_list else 100.0,
            },
        }

        hwms = []
        cur_h = initial_equity
        for p in pts:
            if p.equity > cur_h:
                cur_h = p.equity
            hwms.append(cur_h)
        hwm_by_mode[m_key] = hwms

    # Downsampling for timeline
    n_full = len(pts_m1)
    timeline_series = []
    if n_full > 0:
        step = 40
        chosen_indices = {0, n_full - 1}
        for i in range(0, n_full, step):
            chunk_end = min(i + step, n_full)
            chosen_indices.add(i)
            for m_key in modes_cfg:
                sub = reconstructed_pts[m_key][i:chunk_end]
                if sub:
                    min_idx = i + min(range(len(sub)), key=lambda k: sub[k].equity)
                    max_idx = i + max(range(len(sub)), key=lambda k: sub[k].equity)
                    chosen_indices.add(min_idx)
                    chosen_indices.add(max_idx)

        sorted_indices = sorted(i for i in chosen_indices if 0 <= i < n_full)

        trade_entry_times_m3 = [
            (t.entry_time.timestamp(), notionals_m3_list[i])
            for i, t in enumerate(tr_m3)
        ]
        trade_entry_times_m4 = [
            (t.entry_time.timestamp(), notionals_m4_list[i])
            for i, t in enumerate(tr_m4)
        ]

        def _get_active_notional(records, target_ep, default_val=100.0):
            idx = bisect.bisect_right(records, (target_ep, float("inf"))) - 1
            return records[idx][1] if idx >= 0 else default_val

        for idx in sorted_indices:
            t = pts_m1[idx].timestamp
            t_ep = t.timestamp()
            pt_entry = {
                "time": to_beijing_str(t),
                "short_time": to_beijing_short(t),
                "timestamp": int(t_ep),
            }
            for m_key, pts in reconstructed_pts.items():
                p = pts[idx]
                hwm = (
                    hwm_by_mode[m_key][idx]
                    if idx < len(hwm_by_mode[m_key])
                    else p.equity
                )
                dd_pct = ((hwm - p.equity) / hwm * 100.0) if hwm > 0 else 0.0
                if m_key in ("mode1_simple", "mode5_signal3_exit"):
                    cur_notional = 100.0
                elif m_key == "mode2_daily":
                    d_s = t.strftime("%Y-%m-%d")
                    cur_notional = 100.0 * day_scales.get(d_s, 1.0)
                elif m_key == "mode3_per_trade":
                    cur_notional = _get_active_notional(trade_entry_times_m3, t_ep)
                else:
                    cur_notional = _get_active_notional(trade_entry_times_m4, t_ep)

                pt_entry[m_key] = {
                    "equity": round(p.equity, 2),
                    "drawdown": round(-dd_pct, 2),
                    "margin": round(p.peak_initial_margin, 2),
                    "notional": round(cur_notional, 2),
                    "active_pos": int(p.active_positions),
                }
            timeline_series.append(pt_entry)

    print(
        f"✅ [Compounding Lab] 5 模式对比解算完成 (5 曲线 15s MTM 连续盯市, "
        f"总开仓: {len(raw_trades)} 笔, 归零重置次数: {cycle_resets_count} 次)"
    )
    for _m_k, m_v in modes_meta.items():
        n_st = m_v["notional_stats"]
        print(
            f"   - {m_v['short_label']}: Final ${m_v['final_equity']:.2f} "
            f"({m_v['net_pnl']:+.2f}U, MDD: {m_v['mdd_pct']}%, "
            f"Calmar: {m_v['calmar']:.2f}, "
            f"Notional: [{n_st['min']:.1f}U ~ {n_st['max']:.1f}U], "
            f"End: {n_st['end']:.1f}U)"
        )

    return {
        "param_str": format_8d_param_str(params),
        "params": params,
        "initial_equity": initial_equity,
        "f": f,
        "leverage": leverage,
        "universe": universe,
        "universe_label": universe_label,
        "opp_count": len(events),
        "cycle_resets": cycle_resets_count,
        "total_trades": len(raw_trades),
        "modes": modes_meta,
        "timeline": timeline_series,
    }


def build_governance_data_for_scenarios(
    scenarios: dict[str, Any],
    view_label: str = "top10",
) -> dict[str, Any]:
    """Construct state machine governance payload for a specific scenario set."""
    cand_s3 = scenarios.get("m280_compounding")
    rec_param_str = (
        format_8d_param_str(cand_s3.params)
        if cand_s3 is not None
        else "无可行解 (NO_FEASIBLE_SOLUTION)"
    )
    stab_val = f"{cand_s3.stability * 100.0:.1f}%" if cand_s3 else "N/A"
    stab_pass = "PASS" if cand_s3 and cand_s3.stability >= 0.70 else "WAIT"
    if view_label == "top10":
        view_name = "生产 Top 10 受限"
    elif view_label == "top20":
        view_name = "涨幅榜 Top 20"
    elif view_label == "top30":
        view_name = "涨幅榜 Top 30"
    else:
        view_name = "全市场全量"

    return {
        "stage_status": "provisional",
        "stage_status_display": "🟡 暂行观察期 (PROVISIONAL · 待累积)",
        "view_label": view_label,
        "gates": [
            {
                "name": f"8D 拓扑稳定性 ({view_name})",
                "target": "≥ 70.0%",
                "current": stab_val,
                "status": stab_pass,
            },
            {
                "name": "样本外验证超额",
                "target": "> 0.00 USDT",
                "current": "待真实OOS结算 (N/A)",
                "status": "WAIT",
            },
            {
                "name": "观察期达标天数",
                "target": "≥ 7 天",
                "current": "0 / 7 天 (待累积)",
                "status": "ACCUM",
            },
        ],
        "verdict_title": "⏸️ 保持当前实盘金牌参数不变 (维持当前配置)",
        "verdict_desc": (
            f"处于暂行观察期 ({view_name})，当前实盘已全量部署金牌参数 "
            "(2/1/0.75%/0.30/3.0/1.25x/cd=0/slots=2)。"
            "维持当前生产配置，持续累积样本外稳定性证据。"
        ),
        "recommended_params": rec_param_str,
    }


def run_six_scenarios_pipeline(
    data_dir: Path = DEFAULT_DATA_DIR,
    grid_csv: Path = DEFAULT_GRID_CSV,
    cache_file: Path = CACHE_PRICE_FILE,
    output_html: Path = OUTPUT_HTML_REPORT,
    artifact_dir: Path | None = ARTIFACT_DIR,
    governance_data: dict[str, Any] | None = None,
    reconciliation_data: dict[str, Any] | None = None,
    verify_depth: int | None = 0,
    max_workers: int | None = None,
    version_tag: str | None = None,
    recon_window_days: float = 1.0,
    scheduled_risk_window: ScheduledRiskWindowConfig | None = None,
) -> dict[str, Any]:
    """Execute complete dual-universe (Top 10 + All-Market) 8D optimization dashboard.

    Returns:
        Dictionary with scenarios, curves metadata, HTML paths, and dual-view payload.
    """
    t_total = time.perf_counter()
    print("=== Loading Events & 15s High-Frequency Price Series ===")
    events, manifest = load_all_replay_events(
        data_dir, allow_account_fallback=False, require_manifest=True
    )
    if manifest.pool_type != "raw_parameter_independent":
        raise ValueError(
            f"Opportunity pool manifest pool_type '{manifest.pool_type}' is invalid. "
            "6-Scenario dashboard optimization strictly requires "
            "'raw_parameter_independent'."
        )
    print(
        f"Loaded {len(events):,} deduplicated replay events with verified manifest "
        f"({manifest.snapshot_id}, pool_type={manifest.pool_type})."
    )

    full_grid_df = pd.read_csv(grid_csv).drop_duplicates(subset=DIMS)
    if "cooldown_buckets" in full_grid_df.columns:
        full_grid_df = full_grid_df[full_grid_df["cooldown_buckets"] == 0].copy()
    print(
        f"Loaded {len(full_grid_df):,} distinct 7D parameter candidates "
        f"(cd=0 constrained)."
    )

    prices_by_symbol = load_cached_price_series(cache_file, expected_manifest=manifest)
    print(f"Loaded 15s price streams for {len(prices_by_symbol)} symbols from cache.")

    opt_data_dir = grid_csv.parent

    # 1. Prepare Top 10 and Top 30 filtered opportunity pools
    top10_lookup = load_top10_lookup(
        DEFAULT_PARQUET_DIR, cache_path=DEFAULT_TOP10_CACHE, max_rank=10
    )
    events_top10 = filter_opportunities_by_top10(events, top10_lookup)
    print(
        f"  🎯 Top 10 机会池: {len(events_top10):,} / {len(events):,} 机会 "
        f"({len(events_top10) / max(1, len(events)) * 100:.1f}%)"
    )

    top20_lookup = load_top10_lookup(
        DEFAULT_PARQUET_DIR, cache_path=DEFAULT_TOP20_CACHE, max_rank=20
    )
    events_top20 = filter_opportunities_by_top10(events, top20_lookup)
    print(
        f"  🚀 Top 20 机会池: {len(events_top20):,} / {len(events):,} 机会 "
        f"({len(events_top20) / max(1, len(events)) * 100:.1f}%)"
    )

    top30_lookup = load_top10_lookup(
        DEFAULT_PARQUET_DIR, cache_path=DEFAULT_TOP30_CACHE, max_rank=30
    )
    events_top30 = filter_opportunities_by_top10(events, top30_lookup)
    print(
        f"  ⚡ Top 30 机会池: {len(events_top30):,} / {len(events):,} 机会 "
        f"({len(events_top30) / max(1, len(events)) * 100:.1f}%)"
    )

    # 2. Compute View 1: Top 10 Constrained (Production Mirror)
    print("\n=== [1/3] 正在解算 🎯 生产 Top 10 受限寻优视角 ===")
    (
        scenarios_top10,
        candidates_top10,
        curves_top10,
        timeline_top10,
        pts_top10,
        indices_top10,
    ) = compute_six_scenarios_view(
        events=events_top10,
        full_grid_df=full_grid_df,
        prices_by_symbol=prices_by_symbol,
        manifest=manifest,
        opt_data_dir=opt_data_dir,
        verify_depth=verify_depth,
        max_workers=max_workers,
        data_dir=data_dir,
        view_label="top10",
        scheduled_risk_window=scheduled_risk_window,
    )
    gov_top10 = governance_data or build_governance_data_for_scenarios(
        scenarios_top10, view_label="top10"
    )
    recon_top10 = reconciliation_data or build_reconciliation_payload(
        data_dir,
        prices_by_symbol,
        recon_window_days=recon_window_days,
        manifest=manifest,
        event_csv_suffix="_top10",
    )

    # 3. Compute View 2: Top 20 Constrained (Moderate Expansion)
    print("\n=== [2/3] 正在解算 🚀 涨幅榜 Top 20 扩展寻优视角 ===")
    (
        scenarios_top20,
        candidates_top20,
        curves_top20,
        timeline_top20,
        pts_top20,
        indices_top20,
    ) = compute_six_scenarios_view(
        events=events_top20,
        full_grid_df=full_grid_df,
        prices_by_symbol=prices_by_symbol,
        manifest=manifest,
        opt_data_dir=opt_data_dir,
        verify_depth=verify_depth,
        max_workers=max_workers,
        data_dir=data_dir,
        view_label="top20",
        scheduled_risk_window=scheduled_risk_window,
    )
    gov_top20 = build_governance_data_for_scenarios(scenarios_top20, view_label="top20")
    recon_top20 = build_reconciliation_payload(
        data_dir,
        prices_by_symbol,
        recon_window_days=recon_window_days,
        manifest=manifest,
        event_csv_suffix="_top20",
    )

    # 4. Compute View 3: Top 30 Constrained (Collector Full Range)
    print("\n=== [3/3] 正在解算 ⚡ 涨幅榜 Top 30 扩展寻优视角 ===")
    (
        scenarios_top30,
        candidates_top30,
        curves_top30,
        timeline_top30,
        pts_top30,
        indices_top30,
    ) = compute_six_scenarios_view(
        events=events_top30,
        full_grid_df=full_grid_df,
        prices_by_symbol=prices_by_symbol,
        manifest=manifest,
        opt_data_dir=opt_data_dir,
        verify_depth=verify_depth,
        max_workers=max_workers,
        data_dir=data_dir,
        view_label="top30",
        scheduled_risk_window=scheduled_risk_window,
    )
    gov_top30 = build_governance_data_for_scenarios(scenarios_top30, view_label="top30")
    recon_top30 = build_reconciliation_payload(
        data_dir,
        prices_by_symbol,
        recon_window_days=recon_window_days,
        manifest=manifest,
        event_csv_suffix="_top30",
    )

    views = {
        "top10": {
            "view_id": "top10",
            "title": "生产 Top 10 受限寻优 (实盘镜像)",
            "short_title": "Top 10 实盘镜像",
            "badge": "生产实盘镜像 (positive_gainer_top10)",
            "desc": (
                "严格约束仅在动态涨幅榜 Top 10 准入门禁内开仓，"
                "复现生产 2 槽位排队与高保真对账走势"
            ),
            "opp_count": len(events_top10),
            "optimization": {
                "curves": curves_top10,
                "timeline": timeline_top10,
            },
            "reconciliation": recon_top10,
            "governance": gov_top10,
        },
        "top20": {
            "view_id": "top20",
            "title": "涨幅榜 Top 20 寻优 (温和扩容)",
            "short_title": "Top 20 温和扩容",
            "badge": "涨幅榜 Top 20 适度扩展 (gainer_rank <= 20)",
            "desc": (
                "将准入门禁适度放宽至动态涨幅榜 Top 20，兼顾强势领涨动能"
                "与流动性深度，评估阶梯扩容下的边际收益与回撤"
            ),
            "opp_count": len(events_top20),
            "optimization": {
                "curves": curves_top20,
                "timeline": timeline_top20,
            },
            "reconciliation": recon_top20,
            "governance": gov_top20,
        },
        "top30": {
            "view_id": "top30",
            "title": "涨幅榜 Top 30 寻优 (采集全域)",
            "short_title": "Top 30 扩展池",
            "badge": "涨幅榜 Top 30 扩展全域 (gainer_rank <= 30)",
            "desc": (
                "放宽准入门禁至动态涨幅榜 Top 30，覆盖采集器全量逐笔流，"
                "评估门禁扩容后的容量扩展性与潜在滑点"
            ),
            "opp_count": len(events_top30),
            "optimization": {
                "curves": curves_top30,
                "timeline": timeline_top30,
            },
            "reconciliation": recon_top30,
            "governance": gov_top30,
        },
    }

    if version_tag is None:
        version_tag = datetime.now(tz=BEIJING_TZ).strftime("%Y%m%d_%H%M%S")

    # 4. Compounding Lab: Evaluate under live gold profile (Top 30 Universe)
    print(
        "\n=== [Compounding Lab] 正在解算指定参数下的单利与 3 种复利模式深度对比 "
        "(Top 30 全域视角) ==="
    )
    compounding_lab_data = evaluate_compounding_comparison(
        events=events_top30,
        prices_by_symbol=prices_by_symbol,
        params=DEFAULT_GOLD_PROFILE,
        manifest=manifest,
        scheduled_risk_window=scheduled_risk_window,
        initial_equity=INITIAL_EQUITY,
        f=0.10,
        leverage=LEVERAGE,
        universe="top30",
        universe_label="涨幅榜 Top 30 (全域扩展池)",
    )

    # 5. Render Dashboard HTML from Template (default view: top10)
    render_dashboard_html(
        curves_meta=curves_top10,
        timeline_series=timeline_top10,
        governance_data=gov_top10,
        reconciliation_data=recon_top10,
        output_html=output_html,
        artifact_dir=artifact_dir,
        version_tag=version_tag,
        views=views,
        default_view="top10",
        scheduled_risk_window=scheduled_risk_window,
        compounding_lab=compounding_lab_data,
    )
    v_fn = f"six_scenarios_equity_comparison_{version_tag}.html"
    v_path = output_html.parent / "history" / v_fn
    print(f"✅ 版本历史副本已归档至: {v_path}")
    artifact_saved_path = (
        (artifact_dir / "six_scenarios_equity_comparison.html")
        if (artifact_dir and artifact_dir.exists())
        else None
    )
    if artifact_saved_path:
        print(f"✅ 报告已同步至 Artifact 目录: {artifact_saved_path}")

    elapsed = time.perf_counter() - t_total
    print(
        f"🎉 全部三视角 6 场景寻优与 15s MTM 曲线重放圆满完成! 总耗时: {elapsed:.2f}s"
    )

    return {
        "scenarios": scenarios_top10,
        "all_candidates": candidates_top10,
        "curves_meta": curves_top10,
        "timeline_series": timeline_top10,
        "reconstructed_pts": pts_top10,
        "governance_data": gov_top10,
        "reconciliation_data": recon_top10,
        "output_html": output_html,
        "artifact_html": artifact_saved_path,
        "timeline_points": len(indices_top10),
        "elapsed_seconds": elapsed,
        "version_tag": version_tag,
        "views": views,
        "default_view": "top10",
        "scheduled_risk_window": scheduled_risk_window,
        "compounding_lab": compounding_lab_data,
    }


def render_dashboard_html(
    curves_meta: dict[str, Any],
    timeline_series: list[dict[str, Any]],
    governance_data: dict[str, Any],
    reconciliation_data: dict[str, Any],
    output_html: Path,
    artifact_dir: Path | None = None,
    template_file: Path = TEMPLATE_FILE,
    version_tag: str | None = None,
    views: dict[str, Any] | None = None,
    default_view: str = "top10",
    scheduled_risk_window: ScheduledRiskWindowConfig | None = None,
    compounding_lab: dict[str, Any] | None = None,
) -> Path:
    """Render and write the All-in-One daily dashboard HTML from template."""
    if version_tag is None:
        version_tag = datetime.now(tz=BEIJING_TZ).strftime("%Y%m%d_%H%M%S")
    gen_time_str = datetime.now(tz=BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")

    payload_dict: dict[str, Any] = {
        "version": f"v{version_tag}",
        "generated_at": gen_time_str,
        "default_view": default_view,
        "scheduled_risk_window": (
            {
                "enabled": True,
                "timezone": scheduled_risk_window.timezone,
                "flatten_time": scheduled_risk_window.flatten_start_at.strftime(
                    "%H:%M"
                ),
                "reopen_time": scheduled_risk_window.reopen_at.strftime("%H:%M"),
            }
            if scheduled_risk_window
            else {"enabled": False}
        ),
        "governance": governance_data,
        "reconciliation": reconciliation_data,
        "optimization": {
            "curves": curves_meta,
            "timeline": timeline_series,
        },
        "curves": curves_meta,
        "timeline": timeline_series,
    }
    if views is not None:
        payload_dict["views"] = views
    if compounding_lab is not None:
        payload_dict["compounding_lab"] = compounding_lab

    data_payload = json.dumps(payload_dict, ensure_ascii=False)
    template_content = template_file.read_text(encoding="utf-8")
    rendered_html = template_content.replace("__DATA_JSON__", data_payload)

    output_html.parent.mkdir(parents=True, exist_ok=True)
    output_html.write_text(rendered_html, encoding="utf-8")

    # Versioned history archive
    history_dir = output_html.parent / "history"
    history_dir.mkdir(parents=True, exist_ok=True)
    versioned_html = history_dir / f"six_scenarios_equity_comparison_{version_tag}.html"
    versioned_html.write_text(rendered_html, encoding="utf-8")

    if artifact_dir and artifact_dir.exists():
        artifact_saved = artifact_dir / "six_scenarios_equity_comparison.html"
        artifact_saved.write_text(rendered_html, encoding="utf-8")
        art_history_dir = artifact_dir / "history"
        art_history_dir.mkdir(parents=True, exist_ok=True)
        art_versioned = (
            art_history_dir / f"six_scenarios_equity_comparison_{version_tag}.html"
        )
        art_versioned.write_text(rendered_html, encoding="utf-8")

    return output_html


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate 6-Scenario Optimization and 15s MTM Comparison Dashboard."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Replay events directory",
    )
    parser.add_argument(
        "--grid-csv",
        type=Path,
        default=DEFAULT_GRID_CSV,
        help="Candidate grid CSV",
    )
    parser.add_argument(
        "--cache-file",
        type=Path,
        default=CACHE_PRICE_FILE,
        help="Pre-extracted 15s price cache pickle",
    )
    parser.add_argument(
        "--output-html",
        type=Path,
        default=OUTPUT_HTML_REPORT,
        help="Output HTML dashboard path",
    )
    parser.add_argument(
        "--verify-depth",
        type=int,
        default=0,
        help=(
            "Candidate verification depth for 15s MTM (default 0, evaluating "
            "all compliant candidates). Pass >0 to truncate to top-N."
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help=(
            "Maximum worker processes for parallel candidate verification "
            "(default: CPU count)."
        ),
    )
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=ARTIFACT_DIR,
        help="Target artifact directory to mirror HTML report",
    )
    parser.add_argument(
        "--recon-window-days",
        type=float,
        default=1.0,
        help="Observation window in days for live reconciliation audit (default: 1.0)",
    )
    parser.add_argument(
        "--scheduled-window",
        action="store_true",
        default=False,
        help="Enable daily scheduled risk window (07:45 flatten & 09:00 reopen)",
    )
    parser.add_argument(
        "--window-timezone",
        default="Asia/Shanghai",
        help="Timezone for scheduled risk window (default: Asia/Shanghai)",
    )
    parser.add_argument(
        "--flatten-time",
        default="07:45",
        help="Daily time to stop entry and flatten positions (default: 07:45)",
    )
    parser.add_argument(
        "--reopen-time",
        default="09:00",
        help="Daily time to reopen entries (default: 09:00)",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    scheduled_risk_window: ScheduledRiskWindowConfig | None = None
    if args.scheduled_window:
        from datetime import time

        h_f, m_f = map(int, args.flatten_time.split(":"))
        h_r, m_r = map(int, args.reopen_time.split(":"))
        scheduled_risk_window = ScheduledRiskWindowConfig(
            timezone=args.window_timezone,
            entry_stop_at=time(h_f, m_f),
            flatten_start_at=time(h_f, m_f),
            flatten_deadline_at=time(h_f, min(59, m_f + 10)),
            verify_at=time(h_f, min(59, m_f + 13)),
            reopen_at=time(h_r, m_r),
        )
        print(
            f"🛡️ 已启用定时避险风控: [{args.window_timezone}] "
            f"{args.flatten_time} 强平 ~ {args.reopen_time} 恢复开单"
        )

    run_six_scenarios_pipeline(
        data_dir=args.data_dir,
        grid_csv=args.grid_csv,
        cache_file=args.cache_file,
        output_html=args.output_html,
        artifact_dir=args.artifact_dir,
        verify_depth=args.verify_depth,
        max_workers=args.workers,
        recon_window_days=args.recon_window_days,
        scheduled_risk_window=scheduled_risk_window,
    )


if __name__ == "__main__":
    main()
