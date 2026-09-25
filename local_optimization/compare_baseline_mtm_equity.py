#!/usr/bin/env python3
"""Compare legacy trade-level equity vs true 15s MTM equity for live baseline.

This script demonstrates how much hidden intra-trade drawdown was previously
invisible when calculating drawdown by summing closed trade PnL.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Ensure local_optimization can be imported
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from local_optimization.equity import evaluate_equity_curve  # noqa: E402
from local_optimization.mtm_engine import (  # noqa: E402
    evaluate_legacy_trade_level_curve,
    load_15s_price_series,
    load_trades_from_csv,
    reconstruct_mtm_equity,
)


def main() -> None:
    default_base = (
        ROOT_DIR
        / "server_exports/cml-research-data-20260918-000425"
        / "optimization-volume-feature-7d-20260918/notional_5m_vs_30m-v1"
    )
    parser = argparse.ArgumentParser(
        description="Reconstruct and compare MTM vs legacy equity for baseline trades."
    )
    parser.add_argument(
        "--baseline-csv",
        type=Path,
        default=default_base / "baseline_events.csv",
        help="Path to baseline_events.csv",
    )
    local_parquet = (
        ROOT_DIR / "local_optimization/data/all_data_parquet/environment=research"
    )
    server_parquet = (
        ROOT_DIR
        / "server_exports/cml-research-data-20260918-000425"
        / "parquet/environment=research"
    )
    default_parquet = local_parquet if local_parquet.exists() else server_parquet
    default_cache = ROOT_DIR / "local_optimization/data/cache_15s_price_series.pkl"
    default_output_dir = SCRIPT_DIR / "reports"

    parser = argparse.ArgumentParser(
        description="Reconstruct and compare MTM vs legacy equity for baseline trades."
    )
    parser.add_argument(
        "--baseline-csv",
        type=Path,
        default=default_base / "baseline_events.csv",
        help="Path to baseline_events.csv",
    )
    parser.add_argument(
        "--parquet-dir",
        type=Path,
        default=default_parquet,
        help="Root path of 15s market state parquet exports",
    )
    parser.add_argument(
        "--cache-file",
        type=Path,
        default=default_cache,
        help="Path to cached 15s price series pickle",
    )
    parser.add_argument(
        "--initial-equity",
        type=float,
        default=1000.0,
        help="Initial capital in USDT (default: 1000.0)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=default_output_dir,
        help="Directory to save comparison output",
    )
    args = parser.parse_args()

    print("=== Reconstructing Live Baseline 15s MTM Equity Curve ===")
    print(f"1. Loading trades from: {args.baseline_csv.name}")
    trades = load_trades_from_csv(args.baseline_csv)
    n_syms = len(set(t.symbol for t in trades))
    print(f"   Loaded {len(trades)} closed trades across {n_syms} symbols.")

    symbols = {t.symbol for t in trades}
    print(f"2. Loading 15s price streams for {len(symbols)} symbols...")
    t0 = time.time()
    price_series: dict[str, tuple[list[float], list[float]]] = {}
    if args.cache_file and args.cache_file.exists():
        try:
            from local_optimization.mtm_engine import load_cached_price_series

            cached = load_cached_price_series(args.cache_file)
            if symbols.issubset(set(cached.keys())):
                price_series = {s: cached[s] for s in symbols if s in cached}
                print(
                    f"   Loaded {len(price_series)} symbols directly from cache "
                    f"in {time.time() - t0:.3f}s."
                )
        except Exception as e:
            print(f"   Cache load skipped ({e}), falling back to parquet...")

    if not price_series:
        price_series = load_15s_price_series(args.parquet_dir, symbols)
        t1 = time.time()
        print(f"   Loaded {len(price_series)} symbol price streams in {t1 - t0:.2f}s.")

    # 1. Legacy trade-level curve
    print("3. Evaluating legacy trade-level curve (step-wise at exit)...")
    legacy_metrics = evaluate_legacy_trade_level_curve(
        trades, initial_equity=args.initial_equity
    )

    # 2. True 15s MTM curve
    print("4. Reconstructing continuous 15s mark-to-market equity points...")
    t2 = time.time()
    mtm_points = reconstruct_mtm_equity(
        trades=trades,
        price_series=price_series,
        initial_equity=args.initial_equity,
        grid_seconds=15,
    )
    mtm_metrics = evaluate_equity_curve(mtm_points, initial_equity=args.initial_equity)
    t3 = time.time()
    n_pts = len(mtm_points)
    print(f"   Reconstructed {n_pts:,} continuous 15s MTM points in {t3 - t2:.2f}s.")

    # Save MTM points
    args.output_dir.mkdir(parents=True, exist_ok=True)
    out_csv = args.output_dir / "baseline_15s_mtm_equity_series.csv"
    import csv

    with out_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "timestamp",
                "equity",
                "realized_pnl",
                "unrealized_pnl",
                "peak_initial_margin",
                "active_positions",
            ]
        )
        for p in mtm_points:
            writer.writerow(
                [
                    p.timestamp.isoformat(),
                    p.equity,
                    p.realized_pnl,
                    p.unrealized_pnl,
                    p.peak_initial_margin,
                    p.active_positions,
                ]
            )
    print(f"   Saved MTM curve to: {out_csv}")

    # Calculate hidden drawdown
    hidden_dd_usdt = mtm_metrics.max_drawdown_usdt - legacy_metrics.max_drawdown_usdt
    hidden_dd_pct = (
        mtm_metrics.max_drawdown_pct - legacy_metrics.max_drawdown_pct
    ) * 100.0

    print("\n" + "=" * 70)
    print(f"{'Metric':<30} | {'旧版 (逐笔累加)':<18} | {'新版 (15s 真实盯市)':<18}")
    print("-" * 70)

    def row(
        name: str,
        v1: float | object,
        v2: float | object,
        prefix: str = "",
        suffix: str = "",
        fmt: str = ".2f",
    ) -> None:
        if isinstance(v1, float) and isinstance(v2, float):
            s1 = f"{prefix}{v1:{fmt}}{suffix}"
            s2 = f"{prefix}{v2:{fmt}}{suffix}"
        else:
            s1 = f"{prefix}{v1}{suffix}"
            s2 = f"{prefix}{v2}{suffix}"
        print(f"{name:<28} | {s1:<18} | {s2:<18}")

    metrics_table = [
        (
            "初始本金 (USDT)",
            legacy_metrics.initial_equity,
            mtm_metrics.initial_equity,
            "$",
            "",
            ".2f",
        ),
        (
            "最终净收益 (USDT)",
            legacy_metrics.net_pnl,
            mtm_metrics.net_pnl,
            "$",
            "",
            ".2f",
        ),
        (
            "净收益率 (%)",
            legacy_metrics.net_return_pct,
            mtm_metrics.net_return_pct,
            "",
            "%",
            ".2f",
        ),
        (
            "最大回撤金额 (USDT)",
            legacy_metrics.max_drawdown_usdt,
            mtm_metrics.max_drawdown_usdt,
            "$",
            "",
            ".2f",
        ),
        (
            "最大回撤比例 (%)",
            legacy_metrics.max_drawdown_pct,
            mtm_metrics.max_drawdown_pct,
            "",
            "",
            ".2%",
        ),
        (
            "Ulcer Index (UI)",
            legacy_metrics.ulcer_index,
            mtm_metrics.ulcer_index,
            "",
            "",
            ".4f",
        ),
        (
            "CDaR 95%",
            legacy_metrics.cdar_95,
            mtm_metrics.cdar_95,
            "",
            "",
            ".4f",
        ),
        (
            "平均水下深度 (ADD)",
            legacy_metrics.average_drawdown,
            mtm_metrics.average_drawdown,
            "",
            "",
            ".4f",
        ),
        (
            "日内最大回撤 RMS",
            legacy_metrics.intraday_rms_drawdown,
            mtm_metrics.intraday_rms_drawdown,
            "",
            "",
            ".4f",
        ),
        (
            "水下时间占比 (%)",
            legacy_metrics.underwater_time_pct,
            mtm_metrics.underwater_time_pct,
            "",
            "%",
            ".1f",
        ),
    ]

    for name, v1, v2, pre, suf, fmt in metrics_table:
        row(name, v1, v2, prefix=pre, suffix=suf, fmt=fmt)

    row(
        "最长水下时长 (天)",
        legacy_metrics.max_underwater_seconds / 86400.0,
        mtm_metrics.max_underwater_seconds / 86400.0,
    )
    row(
        "期末未恢复 (Censored)",
        legacy_metrics.is_right_censored,
        mtm_metrics.is_right_censored,
    )
    print("=" * 70)
    print(
        f"🚨 真实盯市揭示出的隐性回撤: +${hidden_dd_usdt:.2f} ({hidden_dd_pct:+.2f}%)"
    )
    if mtm_metrics.ulcer_index > legacy_metrics.ulcer_index:
        ui_ratio = mtm_metrics.ulcer_index / max(1e-6, legacy_metrics.ulcer_index)
        print(f"🚨 真实 Ulcer Index 风险是旧版的 {ui_ratio:.2f} 倍！")
    print("=" * 70)


if __name__ == "__main__":
    main()
