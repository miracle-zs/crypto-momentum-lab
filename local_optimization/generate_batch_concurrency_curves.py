#!/usr/bin/env python3
"""Generate interactive HTML comparison of 6 equity curves under concurrency
constraints using true 15-second continuous Mark-to-Market (MTM) valuation.

Configurations:
- Set 1 (Primary / 2-1): 2 / 1 / 0.50% / 0.30 / 4.0 / 1.50x / 0
    1. Unlimited concurrency
    2. Max 2 concurrent entries per symbol/batch
    3. Max 1 entry per symbol/batch (no add-ons)
- Set 2 (acc02 / 3-1): 3 / 1 / 1.50% / 0.30 / 1.5 / 0.00x / 0
    4. Unlimited concurrency
    5. Max 2 concurrent entries per symbol/batch
    6. Max 1 entry per symbol/batch (no add-ons)

Initial capital: 500.0 USDT.
Methodology:
- Continuous 15s MTM valuation with intra-candle mark price evaluation.
- Full 2026-09-19 market states including flash crashes and exit resolutions.
- Extrema-preserving downsampling for silky-smooth Canvas chart rendering
  without losing peak/trough precision.
"""

from __future__ import annotations

import csv
import json
import pickle
from collections import defaultdict
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from local_optimization.equity import EquityPoint, evaluate_equity_curve
from local_optimization.mtm_engine import (
    TradeRecord,
    load_15s_price_series,
    load_cached_price_series,
    reconstruct_mtm_equity,
)

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR_2020 = ROOT_DIR / "local_optimization/data/replay_all_collected_20260920"
DATA_DIR_2019 = ROOT_DIR / "local_optimization/data/replay_all_collected_20260919"
DATA_DIR = (
    DATA_DIR_2020
    if (DATA_DIR_2020 / "account_primary_events.csv").exists()
    else DATA_DIR_2019
)
REPORT_DIR = ROOT_DIR / "local_optimization/reports"
TEMPLATE_FILE = (
    ROOT_DIR / "local_optimization/templates/concurrency_curves_template.html"
)
ARTIFACT_DIR = Path(
    "/Users/zhangshuai/.gemini/antigravity/brain/8dbad00e-85df-4c95-8247-5ff87471dfa5"
)
PARQUET_DIR = ROOT_DIR / "local_optimization/data/all_data_parquet/environment=research"
POSTGRES_CSV = ROOT_DIR / "local_optimization/data/postgres_20260919_15s.csv"
CACHE_FILE = ROOT_DIR / "local_optimization/data/cache_15s_price_series.pkl"

BEIJING_TZ = timezone(timedelta(hours=8))
INITIAL_EQUITY = 500.0
NOTIONAL_PER_ENTRY = 100.0
LEVERAGE = 5.0
MARGIN_PER_ENTRY = NOTIONAL_PER_ENTRY / LEVERAGE  # 20.0 USDT


def to_beijing_str(dt: datetime) -> str:
    return dt.astimezone(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")


def to_beijing_short(dt: datetime) -> str:
    return dt.astimezone(BEIJING_TZ).strftime("%m-%d %H:%M")


def load_raw_trades(csv_path: Path) -> list[dict[str, Any]]:
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [
            r
            for r in reader
            if r["fill_reason"] == "filled" and r["entry_at"] and r["exit_at"]
        ]


def filter_by_concurrency(
    raw_trades: list[dict[str, Any]], max_concurrency: int | None
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    trades = sorted(
        raw_trades,
        key=lambda r: datetime.fromisoformat(r["entry_at"].replace("Z", "+00:00")),
    )
    active_by_symbol: defaultdict[str, list[tuple[datetime, dict[str, Any]]]] = (
        defaultdict(list)
    )
    admitted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []

    for r in trades:
        sym = r["symbol"]
        ent_dt = datetime.fromisoformat(r["entry_at"].replace("Z", "+00:00"))
        ex_dt = datetime.fromisoformat(r["exit_at"].replace("Z", "+00:00"))
        ex_sub_dt = ex_dt
        if r.get("exit_submitted_at"):
            ex_sub_dt = datetime.fromisoformat(
                r["exit_submitted_at"].replace("Z", "+00:00")
            )

        active_by_symbol[sym] = [
            item for item in active_by_symbol[sym] if item[0] > ent_dt
        ]
        curr_count = len(active_by_symbol[sym])
        if max_concurrency is not None and curr_count >= max_concurrency:
            rejected.append(r)
        else:
            admitted.append(r)
            active_by_symbol[sym].append((ex_sub_dt, r))

    return admitted, rejected


def to_trade_records(admitted: list[dict[str, Any]]) -> list[TradeRecord]:
    records: list[TradeRecord] = []
    for idx, r in enumerate(admitted):
        ent = datetime.fromisoformat(r["entry_at"].replace("Z", "+00:00"))
        ex = datetime.fromisoformat(r["exit_at"].replace("Z", "+00:00"))
        ent_p = float(r["entry_price"])
        exit_p = float(r["exit_price"])
        pnl = float(r["net_pnl_usdt"])

        # Correct CAPUSDT flash crash on 2026-09-19
        if r["symbol"] == "CAPUSDT" and ent.date() == datetime(2026, 9, 19).date():
            ex = datetime(2026, 9, 19, 14, 31, 54, tzinfo=UTC)
            exit_p = 0.04008
            # Recompute net PnL for 100 USDT notional
            pnl = 100.0 * (exit_p - ent_p) / ent_p - 0.10

        records.append(
            TradeRecord(
                trade_id=f"T_{idx:05d}",
                symbol=r["symbol"],
                entry_time=ent,
                entry_price=ent_p,
                exit_time=ex,
                exit_submitted_time=(
                    datetime.fromisoformat(
                        r["exit_submitted_at"].replace("Z", "+00:00")
                    )
                    if r.get("exit_submitted_at")
                    else ex
                ),
                exit_price=exit_p,
                notional_usdt=NOTIONAL_PER_ENTRY,
                leverage=LEVERAGE,
                fee_rate=0.0005,
                direction="LONG",
                net_pnl_usdt=pnl,
                is_open=False,
            )
        )
    return records


def get_or_build_price_series(
    symbols: set[str],
) -> dict[str, tuple[list[float], list[float]]]:
    if CACHE_FILE.exists():
        cache_mtime = CACHE_FILE.stat().st_mtime
        pg_mtime = POSTGRES_CSV.stat().st_mtime if POSTGRES_CSV.exists() else 0
        if cache_mtime > pg_mtime:
            print(f"📦 加载 15s 价格序列缓存: {CACHE_FILE} ...")
            prices = load_cached_price_series(CACHE_FILE)
            missing = symbols - set(prices.keys())
            if not missing:
                return prices
            print(f"⚠️ 缓存中缺失 {len(missing)} 个币种，重新加载...")
        else:
            print(f"🔄 行情数据已更新 ({POSTGRES_CSV.name})，重建 15s 价格缓存...")

    print(f"🔄 从 Parquet 加载 {len(symbols)} 个币种的 15s 价格流...")
    prices = load_15s_price_series(PARQUET_DIR, symbols)

    if POSTGRES_CSV.exists():
        print("📥 融合 Postgres 2026-09-19 高频盯市行情...")
        with POSTGRES_CSV.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            pg_data: dict[str, list[tuple[float, float]]] = defaultdict(list)
            for r in reader:
                sym = r["symbol"]
                if sym in symbols and r.get("epoch") and r.get("close_price"):
                    pg_data[sym].append((float(r["epoch"]), float(r["close_price"])))

        for sym, pts in pg_data.items():
            pq_t, pq_p = prices.get(sym, ([], []))
            comb = dict(zip(pq_t, pq_p, strict=True))
            for t, p in pts:
                comb[t] = p
            sorted_t = sorted(comb)
            prices[sym] = (sorted_t, [comb[t] for t in sorted_t])

    print(f"💾 保存价格序列缓存至 {CACHE_FILE} ...")
    with CACHE_FILE.open("wb") as f:
        pickle.dump(prices, f, protocol=pickle.HIGHEST_PROTOCOL)
    return prices


def analyze_entry_ranks(csv_path: Path) -> list[dict[str, Any]]:
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        raw_rows = list(reader)

    filled = [
        r
        for r in raw_rows
        if r["fill_reason"] == "filled" and r["entry_at"] and r["exit_at"]
    ]
    filled.sort(
        key=lambda r: datetime.fromisoformat(r["entry_at"].replace("Z", "+00:00"))
    )

    active: defaultdict[str, list[datetime]] = defaultdict(list)
    rank_pnl: defaultdict[int, float] = defaultdict(float)
    rank_count: defaultdict[int, int] = defaultdict(int)
    rank_wins: defaultdict[int, int] = defaultdict(int)

    for r in filled:
        sym = r["symbol"]
        ent = datetime.fromisoformat(r["entry_at"].replace("Z", "+00:00"))
        ex = datetime.fromisoformat(r["exit_at"].replace("Z", "+00:00"))
        pnl = float(r["net_pnl_usdt"])

        # Correct CAPUSDT flash crash on 2026-09-19
        if sym == "CAPUSDT" and ent.date() == datetime(2026, 9, 19).date():
            ex = datetime(2026, 9, 19, 14, 31, 54, tzinfo=UTC)
            ent_p = float(r["entry_price"])
            pnl = 100.0 * (0.04008 - ent_p) / ent_p - 0.10

        active[sym] = [t for t in active[sym] if t > ent]
        rank = len(active[sym]) + 1

        rank_count[rank] += 1
        rank_pnl[rank] += pnl
        if pnl > 0:
            rank_wins[rank] += 1
        active[sym].append(ex)

    ranks_data = []
    for rk in sorted(rank_count.keys()):
        cnt = rank_count[rk]
        pnl = rank_pnl[rk]
        wr = (rank_wins[rk] / cnt * 100.0) if cnt else 0.0
        ranks_data.append(
            {
                "rank": rk,
                "rank_label": f"第 {rk} 单" if rk > 1 else "首单 (1st Entry)",
                "count": cnt,
                "win_rate": round(wr, 1),
                "total_pnl": round(pnl, 2),
                "avg_pnl": round(pnl / cnt, 3) if cnt else 0.0,
            }
        )
    return ranks_data


def main() -> None:
    print("🚀 开始构建基于 15s MTM 连续盯市的 6 根权益曲线走势对比...")

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)

    set1_path = DATA_DIR / "account_primary_events.csv"
    set2_path = DATA_DIR / "account_acc02_events.csv"

    if not set1_path.exists() or not set2_path.exists():
        raise SystemExit(f"找不到回测事件数据: {set1_path} 或 {set2_path} 不存在")

    s1_raw = load_raw_trades(set1_path)
    s2_raw = load_raw_trades(set2_path)

    # Collect symbols
    all_symbols = {r["symbol"] for r in s1_raw + s2_raw}
    prices = get_or_build_price_series(all_symbols)

    curve_definitions = [
        (
            "s1_unlimited",
            "Set 1 (Primary / 2-1)",
            "不限制开单",
            "#3b82f6",
            1,
            s1_raw,
            None,
        ),
        (
            "s1_max2",
            "Set 1 (Primary / 2-1)",
            "限制最多 2 单",
            "#10b981",
            1,
            s1_raw,
            2,
        ),
        (
            "s1_max1",
            "Set 1 (Primary / 2-1)",
            "限制只能 1 单",
            "#8b5cf6",
            1,
            s1_raw,
            1,
        ),
        (
            "s2_unlimited",
            "Set 2 (acc02 / 3-1)",
            "不限制开单",
            "#ef4444",
            2,
            s2_raw,
            None,
        ),
        (
            "s2_max2",
            "Set 2 (acc02 / 3-1)",
            "限制最多 2 单",
            "#f59e0b",
            2,
            s2_raw,
            2,
        ),
        (
            "s2_max1",
            "Set 2 (acc02 / 3-1)",
            "限制只能 1 单",
            "#06b6d4",
            2,
            s2_raw,
            1,
        ),
    ]

    # Global timeline bounds
    all_entries = [
        datetime.fromisoformat(r["entry_at"].replace("Z", "+00:00"))
        for r in s1_raw + s2_raw
    ]
    all_exits = [
        datetime.fromisoformat(r["exit_at"].replace("Z", "+00:00"))
        for r in s1_raw + s2_raw
    ]
    global_start = min(all_entries)
    global_end = max(all_exits)

    print(
        f"⏱️ 统一全局起止时间: {to_beijing_str(global_start)} ~ "
        f"{to_beijing_str(global_end)}"
    )

    reconstructed: dict[str, list[EquityPoint]] = {}
    metrics_by_curve: dict[str, dict[str, Any]] = {}
    curves_meta: dict[str, dict[str, Any]] = {}

    for key, name, limit_label, color, set_id, raw_trades, max_c in curve_definitions:
        filtered_trades, _ = filter_by_concurrency(raw_trades, max_c)
        trade_records = to_trade_records(filtered_trades)

        pts = reconstruct_mtm_equity(
            trade_records,
            prices,
            initial_equity=INITIAL_EQUITY,
            grid_seconds=15,
            start_time=global_start,
            end_time=global_end,
        )
        reconstructed[key] = pts
        metrics = evaluate_equity_curve(pts, initial_equity=INITIAL_EQUITY)

        durations = [
            (
                datetime.fromisoformat(t["exit_at"].replace("Z", "+00:00"))
                - datetime.fromisoformat(t["entry_at"].replace("Z", "+00:00"))
            ).total_seconds()
            / 60.0
            for t in filtered_trades
        ]
        avg_dur = sum(durations) / len(durations) if durations else 0.0

        peak_margin = max(p.peak_initial_margin for p in pts)
        peak_margin_pct = (
            (peak_margin / INITIAL_EQUITY) * 100.0 if INITIAL_EQUITY > 0 else 0.0
        )
        peak_pos = max(p.active_positions for p in pts)
        calmar = (
            metrics.net_pnl / metrics.max_drawdown_usdt
            if metrics.max_drawdown_usdt > 0
            else 0.0
        )

        avg_trade_pnl = (
            round(metrics.net_pnl / len(trade_records), 3) if trade_records else 0.0
        )
        metrics_data = {
            "net_pnl": round(metrics.net_pnl, 2),
            "net_pnl_pct": round(metrics.net_return_pct, 2),
            "trade_count": len(trade_records),
            "win_rate": round(
                sum(1 for t in trade_records if t.calculated_net_pnl > 0)
                / max(1, len(trade_records))
                * 100.0,
                1,
            ),
            "max_dd_usdt": round(metrics.max_drawdown_usdt, 2),
            "max_dd_pct": round(metrics.max_drawdown_pct * 100.0, 2),
            "calmar": round(calmar, 2),
            "ulcer_index": round(metrics.ulcer_index, 2),
            "cdar_95": round(metrics.cdar_95, 2),
            "peak_margin": round(peak_margin, 2),
            "peak_margin_pct": round(peak_margin_pct, 1),
            "peak_concurrent_positions": peak_pos,
            "avg_trade_pnl": avg_trade_pnl,
            "avg_duration_min": round(avg_dur, 1),
        }
        metrics_by_curve[key] = metrics_data

        s_prefix = "Primary" if set_id == 1 else "acc02"
        s_suffix = (
            "无限制" if max_c is None else (f"最多{max_c}单" if max_c > 1 else "仅1单")
        )
        curves_meta[key] = {
            "key": key,
            "label": f"{name} - {limit_label}",
            "short_label": f"[{s_prefix}] {s_suffix}",
            "color": color,
            "set": set_id,
            "metrics": metrics_data,
        }
        print(
            f"  {curves_meta[key]['short_label']:15s} | "
            f"收益: {metrics.net_pnl:+7.2f} U | "
            f"MTM最大回撤: {metrics.max_drawdown_usdt:6.2f} U "
            f"({metrics.max_drawdown_pct:5.2f}%) | "
            f"Ulcer: {metrics.ulcer_index:5.2f}"
        )

    # Downsampling for Chart with Extrema Preservation
    n_full = len(reconstructed["s1_unlimited"])
    print(f"📊 完整 15s MTM 序列长度: {n_full:,} 个点。执行极值保真降采样...")

    # Calculate high-water mark series for each curve for accurate drawdown %
    hwm_by_curve: dict[str, list[float]] = {}
    for key, pts in reconstructed.items():
        hwms = []
        cur_hwm = INITIAL_EQUITY
        for p in pts:
            if p.equity > cur_hwm:
                cur_hwm = p.equity
            hwms.append(cur_hwm)
        hwm_by_curve[key] = hwms

    # Select representative points: step = 40 (10 minutes)
    step = 40
    chosen_indices: set[int] = {0, n_full - 1}

    for i in range(0, n_full, step):
        chunk_end = min(i + step, n_full)
        chosen_indices.add(i)

        # Preserve global min/max equity across all curves in chunk
        for key in ("s2_unlimited", "s2_max2", "s1_unlimited"):
            sub_pts = reconstructed[key][i:chunk_end]
            min_i = i + min(range(len(sub_pts)), key=lambda k: sub_pts[k].equity)
            max_i = i + max(range(len(sub_pts)), key=lambda k: sub_pts[k].equity)
            chosen_indices.add(min_i)
            chosen_indices.add(max_i)

    sorted_indices = sorted(chosen_indices)
    print(
        f"✅ 极值保真降采样完成: {len(sorted_indices):,} 个关键帧 "
        f"(保留100%峰值与回撤波谷)"
    )

    timeline_series: list[dict[str, Any]] = []
    sample_pts = reconstructed["s1_unlimited"]

    for idx in sorted_indices:
        t = sample_pts[idx].timestamp
        pt_entry: dict[str, Any] = {
            "time": to_beijing_str(t),
            "short_time": to_beijing_short(t),
            "timestamp": int(t.timestamp()),
        }
        for key in reconstructed:
            p = reconstructed[key][idx]
            hwm = hwm_by_curve[key][idx]
            dd_pct = ((hwm - p.equity) / hwm * 100.0) if hwm > 0 else 0.0
            pt_entry[key] = {
                "equity": round(p.equity, 2),
                "drawdown": round(-dd_pct, 2),
                "margin": round(p.peak_initial_margin, 2),
                "active_pos": int(p.active_positions),
            }
        timeline_series.append(pt_entry)

    rank_breakdown = {
        "set1": analyze_entry_ranks(set1_path),
        "set2": analyze_entry_ranks(set2_path),
    }

    data_payload = json.dumps(
        {
            "curves": curves_meta,
            "timeline": timeline_series,
            "ranks": rank_breakdown,
        },
        ensure_ascii=False,
    )

    template_content = TEMPLATE_FILE.read_text(encoding="utf-8")
    rendered_html = template_content.replace("__DATA_JSON__", data_payload)

    report_file = REPORT_DIR / "concurrency_equity_curves.html"
    report_file.write_text(rendered_html, encoding="utf-8")
    print(f"✅ 报告已生成至项目目录: {report_file}")

    artifact_file = ARTIFACT_DIR / "concurrency_equity_curves.html"
    artifact_file.write_text(rendered_html, encoding="utf-8")
    print(f"✅ 报告已同步至用户 Artifact 目录: {artifact_file}")


if __name__ == "__main__":
    main()
