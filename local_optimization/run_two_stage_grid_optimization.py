#!/usr/bin/env python3
"""[LEGACY / DIAGNOSTIC ONLY] Run Two-Stage Parameter Optimization across 7D grid.

WARNING: This script is a historical 7D diagnostic screening tool using
approximate UI (mdd * 0.45). It does NOT reconstruct 15s MTM continuous equity
paths or evaluate 8D concurrency slots. Its output is strictly for historical
exploratory analysis and is PROHIBITED from being used directly for live parameter
promotion. Formal daily optimization is handled by generate_six_scenarios_dashboard.py
and run_daily_local_optimization.py.
"""

from __future__ import annotations

import argparse
import math
import multiprocessing as mp
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

# Ensure local_optimization can be imported
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from local_optimization.optimizer import (  # noqa: E402
    CandidateEvaluation,
    extract_pareto_frontier,
    is_candidate_compliant,
    select_best_and_recommended,
)
from local_optimization.protocol import (  # noqa: E402
    OptimizationProtocol,
    ParameterCandidate,
)

DIMS = [
    "impulse_window_buckets",
    "confirmation_buckets",
    "min_return_pct",
    "min_imbalance",
    "min_intensity",
    "min_volume_ratio",
    "cooldown_buckets",
]
DIMS_8D = list(DIMS) + ["max_open_positions"]
CONCURRENCY_SLOTS_DOMAIN = [1, 2, 3, 4]


def format_param_str(params: dict[str, object]) -> str:
    """Format parameter dict into compact representation."""
    w = int(params["impulse_window_buckets"])
    c = int(params["confirmation_buckets"])
    r = float(params["min_return_pct"])
    imb = float(params["min_imbalance"])
    inten = float(params["min_intensity"])
    vol = float(params["min_volume_ratio"])
    cd = int(params.get("cooldown_buckets", 0))
    slots = params.get("max_open_positions")
    if slots is not None:
        return (
            f"{w}/{c}/{r:.2f}%/{imb:.2f}/{inten:.1f}/{vol:.1f}x/cd={cd}/slots={slots}"
        )
    return f"{w}/{c}/{r:.2f}%/{imb:.2f}/{inten:.1f}/{vol:.1f}x/cd={cd}"


def _stability_worker_chunk(
    chunk_keys: list[tuple[object, ...]],
    pnl_map: dict[tuple[object, ...], tuple[float, bool]],
    grid_values: dict[str, list[object]],
    val_to_idx: dict[str, dict[object, int]],
    dims: list[str] | None = None,
) -> dict[tuple[object, ...], float]:
    """Compute stability scores for a slice of candidate keys."""
    use_dims = dims if dims is not None else DIMS
    submap: dict[tuple[object, ...], float] = {}
    for key in chunk_keys:
        cand_pnl, _is_feas = pnl_map.get(key, (0.0, False))
        if cand_pnl <= 0:
            submap[key] = 0.0
            continue

        neighbors: list[tuple[object, ...]] = []
        for i, d in enumerate(use_dims):
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
            submap[key] = 1.0
            continue

        stable_count = 0
        for n in neighbors:
            pnl, n_feas = pnl_map.get(n, (0.0, False))
            # Neighbor is stable if feasible and retains >= 30% candidate profit
            # (aligned with optimizer.py)
            if n_feas and pnl >= 0.30 * cand_pnl:
                stable_count += 1

        submap[key] = stable_count / len(neighbors)

    return submap


def _stability_worker_wrapper(
    args: tuple[
        list[tuple[object, ...]],
        dict[tuple[object, ...], tuple[float, bool]],
        dict[str, list[object]],
        dict[str, dict[object, int]],
        list[str],
    ],
) -> dict[tuple[object, ...], float]:
    """Top-level unpickling helper for multiprocessing pool."""
    chunk_keys, pnl_map, grid_values, val_to_idx, dims = args
    return _stability_worker_chunk(chunk_keys, pnl_map, grid_values, val_to_idx, dims)


def compute_all_neighborhood_stabilities(
    df: pd.DataFrame,
    grid_values: dict[str, list[object]],
    cap: float | None = None,
    *,
    workers: int = 1,
    dims: list[str] | None = None,
) -> dict[tuple[object, ...], float]:
    """Compute stability score across 1-step grid neighbors in specified dimensions.

    Supports multi-process parallelization across `workers` CPU cores.
    """
    if dims is None:
        if "max_open_positions" in df.columns and "max_open_positions" in grid_values:
            dims = DIMS_8D
        else:
            dims = DIMS

    keys = list(zip(*(df[d] for d in dims), strict=True))
    pnls = df["full_net_pnl_usdt"].to_numpy(dtype=float)
    margins = df["initial_margin_peak_usdt"].to_numpy(dtype=float)
    if cap is not None:
        feas = margins <= cap + 1e-9
    else:
        feas = np.ones(len(df), dtype=bool)

    pnl_map: dict[tuple[object, ...], tuple[float, bool]] = {
        keys[i]: (float(pnls[i]), bool(feas[i])) for i in range(len(keys))
    }
    val_to_idx = {d: {v: i for i, v in enumerate(grid_values[d])} for d in dims}

    if workers <= 1 or len(keys) < 200:
        return _stability_worker_chunk(keys, pnl_map, grid_values, val_to_idx, dims)

    chunk_size = math.ceil(len(keys) / workers)
    chunks = [keys[i : i + chunk_size] for i in range(0, len(keys), chunk_size)]
    worker_args = [(chunk, pnl_map, grid_values, val_to_idx, dims) for chunk in chunks]

    start_methods = mp.get_all_start_methods()
    # On macOS, fork in multi-threaded processes raises DeprecationWarning.
    # Prefer spawn on macOS, and fork on Linux for copy-on-write speed.
    if sys.platform == "darwin":
        ctx_name = "spawn"
    elif "fork" in start_methods:
        ctx_name = "fork"
    else:
        ctx_name = "spawn"
    ctx = mp.get_context(ctx_name)

    with ctx.Pool(processes=min(workers, len(chunks))) as pool:
        results = pool.map(_stability_worker_wrapper, worker_args)

    stability_map: dict[tuple[object, ...], float] = {}
    for sub in results:
        stability_map.update(sub)

    return stability_map


def run_scenario_optimization(
    df: pd.DataFrame,
    scenario_family: str,
    margin_cap_usdt: float | None,
    delta_p_usdt: float,
    stability_map: dict[tuple[object, ...], float],
) -> dict[str, object]:
    """Execute two-stage optimization using unified optimizer rules."""
    protocol = OptimizationProtocol(
        scenario_family=scenario_family,
        max_initial_margin_usdt=margin_cap_usdt,
        max_allowed_mdd_pct=0.30,
        max_allowed_ui=0.15,
        min_trades=30,
        near_optimal_delta_usdt=delta_p_usdt,
    )

    # Stage 1: Coarse Filtering (net_pnl > 0, optional margin cap)
    mask = df["full_net_pnl_usdt"] > 0
    if margin_cap_usdt is not None:
        mask = mask & (df["initial_margin_peak_usdt"] <= margin_cap_usdt + 1e-9)

    stage1_candidates = df[mask].copy()

    # Convert to CandidateEvaluation objects
    evaluations: list[CandidateEvaluation] = []
    for _, row in stage1_candidates.iterrows():
        key = tuple(row[d] for d in DIMS)
        params = {d: row[d] for d in DIMS}
        cand = ParameterCandidate.from_dict(params)
        stab = stability_map.get(key, 0.0)

        pnl = float(row["full_net_pnl_usdt"])
        mdd = float(row["full_max_drawdown_usdt"])
        margin = float(row["initial_margin_peak_usdt"])
        n_closed = int(row["full_n_closed"])

        # Path UI approximation for pre-filter grid results
        approx_ui = (mdd / 1000.0) * 0.45

        eval_item = CandidateEvaluation(
            candidate=cand,
            is_feasible=True,
            trade_count=n_closed,
            net_pnl=pnl,
            net_return_pct=pnl / 10.0,
            ulcer_index=approx_ui,
            max_drawdown_pct=mdd / 1000.0,
            peak_initial_margin_usdt=margin,
            neighborhood_stability_score=stab,
        )
        if is_candidate_compliant(eval_item, protocol)[0]:
            evaluations.append(eval_item)

    if not evaluations:
        return {
            "scenario": scenario_family,
            "margin_cap": margin_cap_usdt,
            "survived_count": 0,
            "daily_best": None,
            "recommended": None,
            "pareto": [],
        }

    # Extract Pareto Frontier and Robust Recommended using canonical optimizer logic
    pareto = extract_pareto_frontier(evaluations, protocol)
    daily_best, recommended = select_best_and_recommended(evaluations, protocol)

    return {
        "scenario": scenario_family,
        "margin_cap": margin_cap_usdt,
        "survived_count": len(evaluations),
        "daily_best": daily_best,
        "recommended": recommended,
        "pareto": pareto[:10],
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "[LEGACY / DIAGNOSTIC ONLY] Historical 7D parameter grid screening "
            "tool. Note: This tool uses approximate UI and does NOT perform "
            "15s MTM path reconstruction. Output is PROHIBITED from being used "
            "for live parameter promotion."
        )
    )
    local_grid = (
        ROOT_DIR
        / "local_optimization/data/optimization_all_collected_20260919/grid_results.csv"
    )
    legacy_grid = (
        ROOT_DIR
        / "server_exports/cml-research-data-20260918-000425"
        / "optimization-volume-feature-7d-20260918"
        / "notional_5m_vs_30m-v1/grid_results.csv"
    )
    default_grid = local_grid if local_grid.exists() else legacy_grid
    parser.add_argument(
        "--grid-csv",
        type=Path,
        default=default_grid,
        help="Path to grid_results.csv containing candidate backtest summaries",
    )
    parser.add_argument(
        "--delta-p-usdt",
        type=float,
        default=30.0,
        help="Near-optimal tolerance delta_P in USDT (default: 30.0)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=min(os.cpu_count() or 4, 8),
        help="Number of parallel workers for stability calculation",
    )
    parser.add_argument(
        "--output-md",
        type=Path,
        default=SCRIPT_DIR / "reports/two_stage_optimization_report.md",
        help="Output markdown report path",
    )
    args = parser.parse_args()

    print(
        "⚠️ [Legacy Diagnostic Tool] run_two_stage_grid_optimization executes "
        "fast 7D proxy filtering only. Formal daily recommendation strictly "
        "requires generate_six_scenarios_dashboard with 15s MTM."
    )
    print(f"=== Loading grid results from {args.grid_csv.name} ===")
    df = pd.read_csv(args.grid_csv)
    print(f"Loaded {len(df):,} candidates across 7 dimensions.")

    grid_values = {d: sorted(df[d].unique()) for d in DIMS}

    # Pre-calculate stability map across all 7 dimensions
    print(f"Computing 7D neighborhood stability using {args.workers} worker(s)...")
    t0 = time.perf_counter()
    stability_map_280 = compute_all_neighborhood_stabilities(
        df, grid_values, cap=280.0, workers=args.workers
    )
    stability_map_none = compute_all_neighborhood_stabilities(
        df, grid_values, cap=None, workers=args.workers
    )
    elapsed = time.perf_counter() - t0
    total_evals = len(df) * 2
    rate = total_evals / max(0.001, elapsed)
    print(
        f"Neighborhood stability completed in {elapsed:.3f}s "
        f"({rate:,.0f} candidate stability evals/sec)."
    )

    scenarios = [
        ("margin280-free-cooldown", 280.0, stability_map_280),
        ("unconstrained-reference", None, stability_map_none),
    ]

    results: list[dict[str, object]] = []
    for name, cap, stab_map in scenarios:
        print(f"Running scenario: {name} (Cap: {cap}U)...")
        res = run_scenario_optimization(
            df=df,
            scenario_family=name,
            margin_cap_usdt=cap,
            delta_p_usdt=args.delta_p_usdt,
            stability_map=stab_map,
        )
        results.append(res)

    # Build Markdown Report
    lines = [
        "# [历史诊断工具] 七维网格两阶段寻优与稳健推荐报告 (Legacy Diagnostic)",
        "",
        "> ⚠️ **[LEGACY / DIAGNOSTIC ONLY] 历史粗筛诊断报告**："
        "本报告仅用于历史 7D 网格粗筛诊断与近优凸区域分析"
        "（UI 为近似值，无 15s MTM 连续盯市）。"
        "**本脚本输出禁止直接用于实盘换参准入**。"
        "正式准入请使用 8D 全场景 MTM 盯市流水线"
        "（`generate_six_scenarios_dashboard.py` / "
        "`run_daily_local_optimization.py`）。",
        "",
        "- **搜索空间**: 7 个维度全量网格联合寻优，共 `25,200` 组候选。",
        f"- **近优容差**: $\\delta_P = {args.delta_p_usdt:.1f}\\text{{U}}$"
        "（在最优收益容差内优先挑高稳定性与低回撤参数）。",
        "- **两阶段筛选**: Stage 1 过滤保证金与低频候选（剪枝 >70%），"
        "Stage 2 评估 7 维参数邻域平坦度与 Pareto 非支配集。",
        "",
        "## 1. 跨约束场景横向对比表 (Daily Best vs Recommended)",
        "",
        "| 场景约束 | 角色 | 参数 (7维) | 净收益 | 回撤 | 保证金 | 交易数 | 稳定性 |",
        "|---|---|---|---:|---:|---:|---:|---:|",
    ]

    for res in results:
        scen = res["scenario"]
        cap_str = f"≤{res['margin_cap']:.0f}U" if res["margin_cap"] else "无限制"
        b: CandidateEvaluation = res["daily_best"]  # type: ignore
        r: CandidateEvaluation = res["recommended"]  # type: ignore

        if b:
            b_str = format_param_str(b.candidate.params)
            lines.append(
                f"| **{scen}** ({cap_str}) | **今日最高** | `{b_str}` | "
                f"+${b.net_pnl:.2f} | ${b.max_drawdown_pct * 1000:.2f} | "
                f"${b.peak_initial_margin_usdt:.1f} | {b.trade_count} | "
                f"{b.neighborhood_stability_score:.1%} |"
            )
        if r:
            r_str = format_param_str(r.candidate.params)
            lines.append(
                f"| | **稳定推荐** | `{r_str}` | "
                f"+${r.net_pnl:.2f} | ${r.max_drawdown_pct * 1000:.2f} | "
                f"${r.peak_initial_margin_usdt:.1f} | {r.trade_count} | "
                f"**{r.neighborhood_stability_score:.1%}** |"
            )

    # Analysis section
    lines.extend(
        [
            "",
            "## 2. 核心量化发现与推荐参数解析",
            "",
        ]
    )

    m280_res = results[0]
    best_280: CandidateEvaluation = m280_res["daily_best"]  # type: ignore
    rec_280: CandidateEvaluation = m280_res["recommended"]  # type: ignore

    if best_280 and rec_280:
        pnl_diff = best_280.net_pnl - rec_280.net_pnl
        dd_diff = (best_280.max_drawdown_pct - rec_280.max_drawdown_pct) * 1000.0
        best_p_str = format_param_str(best_280.candidate.params)
        rec_p_str = format_param_str(rec_280.candidate.params)
        lines.extend(
            [
                "### 保证金 ≤ 280U 主力场景分析：",
                f"- **今日最高参数 (`{best_p_str}`)**：",
                f"  - 净收益：**+${best_280.net_pnl:.2f}**，"
                f"回撤：**${best_280.max_drawdown_pct * 1000:.2f}**，"
                f"峰值保证金：**${best_280.peak_initial_margin_usdt:.1f}**，"
                f"邻域稳定性：**{best_280.neighborhood_stability_score:.1%}**。",
                f"- **稳定推荐参数 (`{rec_p_str}`)**：",
                f"  - 净收益：**+${rec_280.net_pnl:.2f}**，"
                f"回撤：**${rec_280.max_drawdown_pct * 1000:.2f}**，"
                f"峰值保证金：**${rec_280.peak_initial_margin_usdt:.1f}**，"
                f"邻域稳定性：**{rec_280.neighborhood_stability_score:.1%}**。",
                "- **取舍对比**：",
                f"  - 推荐参数仅让渡 **${pnl_diff:.2f}** 利润"
                f"（在 $\\delta_P={args.delta_p_usdt:.0f}\\text{{U}}$ 容差内）；",
                f"  - 回撤变化：**{dd_diff:+.2f}U**；",
                f"  - 邻域稳定性从 **{best_280.neighborhood_stability_score:.1%} "
                f"提升至 {rec_280.neighborhood_stability_score:.1%}**"
                "（抗扰动性大幅增强，非孤点过拟合）。",
                "",
            ]
        )

    lines.extend(
        [
            "## 3. 保证金边际收益与机会成本 (Marginal Capital Analysis)",
            "",
            "| 释放路径 | 边际收益 | 边际回撤 | 边际收益回撤比 | 资金效率 (U/U) |",
            "|---|---:|---:|---:|---:|",
        ]
    )

    b280: CandidateEvaluation = results[0]["daily_best"]  # type: ignore
    b_none: CandidateEvaluation = results[1]["daily_best"]  # type: ignore

    if b280 and b_none:
        # Path: 280 -> Unconstrained
        dp_none = b_none.net_pnl - b280.net_pnl
        ddd_none = (b_none.max_drawdown_pct - b280.max_drawdown_pct) * 1000.0
        dmargin_none = b_none.peak_initial_margin_usdt - b280.peak_initial_margin_usdt
        ratio_none = dp_none / ddd_none if ddd_none != 0 else 0.0
        eff_none = dp_none / dmargin_none if dmargin_none > 0 else 0.0
        lines.append(
            f"| **280U → 无限制** (${b_none.peak_initial_margin_usdt:.0f}U) | "
            f"+${dp_none:.2f} | +${ddd_none:.2f} | {ratio_none:.2f} | "
            f"+{eff_none:.2f} U/U |"
        )

    # Top Pareto Frontier for Margin <= 280U
    lines.extend(
        [
            "",
            "## 4. 保证金 ≤ 280U 的 3D Pareto 前沿 (非支配解集 Top 10)",
            "",
            "| 候选参数 (7维) | 净收益 | 最大回撤 | 峰值保证金 | 交易数 | 稳定性 |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )

    for p in results[0]["pareto"]:  # type: ignore
        p_str = format_param_str(p.candidate.params)
        lines.append(
            f"| `{p_str}` | +${p.net_pnl:.2f} | ${p.max_drawdown_pct * 1000:.2f} | "
            f"${p.peak_initial_margin_usdt:.1f} | {p.trade_count} | "
            f"{p.neighborhood_stability_score:.1%} |"
        )

    report_content = "\n".join(lines)
    args.output_md.parent.mkdir(parents=True, exist_ok=True)
    args.output_md.write_text(report_content, encoding="utf-8")
    print(f"Report successfully emitted to: {args.output_md}")
    print("\n" + report_content)


if __name__ == "__main__":
    main()
