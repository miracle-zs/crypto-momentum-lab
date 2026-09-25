#!/usr/bin/env python3
"""Generate updated account replay event CSVs for the 4 unified live accounts.

Simulates the unified live gold profile (2/1/0.75%/0.30/3.0/1.25x/cd=0/slots=2)
across the authenticated RawOpportunity pool (2026-09-03 to 2026-09-23)
and writes standardized account replay event CSVs and manifest report.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import UTC, datetime, time
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from crypto_momentum_lab.live_rollout.scheduled_risk_window import (  # noqa: E402
    ScheduledRiskWindowConfig,
)
from local_optimization.mtm_engine import load_cached_price_series  # noqa: E402
from local_optimization.run_walk_forward_analysis import (  # noqa: E402
    load_all_replay_events,
)
from local_optimization.simulation_ledger import SimulationLedger  # noqa: E402

DATA_DIR = SCRIPT_DIR / "data/replay_all_collected_20260920"
CACHE_FILE = SCRIPT_DIR / "data/cache_15s_price_series.pkl"

UNIFIED_LIVE_PROFILE = {
    "impulse_window_buckets": 2,
    "confirmation_buckets": 1,
    "min_return_pct": 0.75,
    "min_imbalance": 0.30,
    "min_intensity": 3.0,
    "min_volume_ratio": 1.25,
    "cooldown_buckets": 0,
    "max_open_positions": 2,
}

ACCOUNT_NAMES = ["primary", "acc01", "acc02", "acc03"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    scheduled_risk_window: ScheduledRiskWindowConfig | None = None
    if args.scheduled_window:
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
            f"  🛡️ 已启用定时避险风控: [{args.window_timezone}] "
            f"{args.flatten_time} 强平 ~ {args.reopen_time} 恢复开单"
        )
    print("🚀 开始使用统一实盘金牌参数重构 4 账户离线回放事件流...")
    opps, manifest = load_all_replay_events(
        DATA_DIR, allow_account_fallback=False, require_manifest=True
    )
    print(
        f"  ✅ 已加载机会池: {len(opps):,} 条 "
        f"(水位 {manifest.watermark_start} ~ {manifest.watermark_end})"
    )

    prices = load_cached_price_series(CACHE_FILE, expected_manifest=manifest)
    print(f"  ✅ 已加载高频价格序列: {len(prices)} 币种")

    from local_optimization.opportunity import (
        filter_opportunities_by_top10,
        load_top10_lookup,
    )

    cache_top10 = SCRIPT_DIR / "data/cache_top10_lookup.pkl"
    cache_top20 = SCRIPT_DIR / "data/cache_top20_lookup.pkl"
    cache_top30 = SCRIPT_DIR / "data/cache_top30_lookup.pkl"
    parquet_dir = SCRIPT_DIR / "data/all_data_parquet/environment=research"
    top10_lookup = load_top10_lookup(parquet_dir, cache_path=cache_top10)
    opps_top10 = filter_opportunities_by_top10(opps, top10_lookup)
    print(
        f"  🎯 Top 10 门禁过滤: {len(opps_top10):,} / {len(opps):,} 机会 "
        f"({len(opps_top10) / max(1, len(opps)) * 100:.1f}%)"
    )

    top20_lookup = load_top10_lookup(parquet_dir, cache_path=cache_top20, max_rank=20)
    opps_top20 = filter_opportunities_by_top10(opps, top20_lookup)
    print(
        f"  🚀 Top 20 门禁过滤: {len(opps_top20):,} / {len(opps):,} 机会 "
        f"({len(opps_top20) / max(1, len(opps)) * 100:.1f}%)"
    )

    top30_lookup = load_top10_lookup(parquet_dir, cache_path=cache_top30, max_rank=30)
    opps_top30 = filter_opportunities_by_top10(opps, top30_lookup)
    print(
        f"  ⚡ Top 30 门禁过滤: {len(opps_top30):,} / {len(opps):,} 机会 "
        f"({len(opps_top30) / max(1, len(opps)) * 100:.1f}%)"
    )

    pools_to_run = [
        ("all_market", opps, ""),
        ("top10", opps_top10, "_top10"),
        ("top20", opps_top20, "_top20"),
        ("top30", opps_top30, "_top30"),
    ]

    report_summary: dict[str, Any] = {}

    for pool_name, pool_opps, suffix in pools_to_run:
        print(f"\n▶ 正在模拟 [{pool_name}] 回放事件流 ({len(pool_opps):,} 条候选)...")
        ledger = SimulationLedger(
            initial_cash=1000.0, leverage=5.0, notional_usdt=100.0
        )
        res, _ = ledger.simulate_window(
            opportunities=pool_opps,
            params=UNIFIED_LIVE_PROFILE,
            window_start=manifest.watermark_start,
            window_end=manifest.watermark_end,
            price_series=prices,
            max_concurrency=2,
            fast_eval=False,
            scheduled_risk_window=scheduled_risk_window,
        )

        admitted_execs = [e for e in res.executions if e.trade is not None]
        print(
            f"  ✅ [{pool_name}] 模拟完成: 产生 {len(admitted_execs):,} 笔准入成交 "
            f"(累计 PnL: {res.oos_pnl:+.2f}U, MDD: {res.mdd_pct:.2%})"
        )

        headers = [
            "symbol",
            "direction",
            "detected_at",
            "order_created_at",
            "impulse_return_pct",
            "aggressive_imbalance",
            "confirmation_min_imbalance",
            "notional_intensity",
            "volume_feature",
            "volume_ratio",
            "top10_proxy_allowed",
            "fill_reason",
            "entry_at",
            "entry_price",
            "exit_at",
            "exit_price",
            "exit_reason",
            "closed",
            "net_pnl_usdt",
            "net_return_pct",
        ]

        for acc in ACCOUNT_NAMES:
            out_csv = DATA_DIR / f"account_{acc}{suffix}_events.csv"
            with out_csv.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                for e in admitted_execs:
                    t = e.trade
                    det_iso = e.detected_at.isoformat() if e.detected_at else ""
                    ent_iso = e.entry_time.isoformat() if e.entry_time else ""
                    exit_iso = e.exit_time.isoformat() if e.exit_time else ""
                    is_closed = bool(e.exit_time)
                    pnl = t.calculated_net_pnl if t else 0.0
                    ret_pct = (pnl / 100.0) * 100.0
                    is_flattened = bool(
                        t
                        and t.exit_time
                        and scheduled_risk_window
                        and t.exit_time
                        == scheduled_risk_window.next_flatten_time(t.entry_time)
                    )
                    writer.writerow(
                        [
                            e.symbol,
                            "up" if e.direction == "LONG" else "down",
                            det_iso,
                            ent_iso,
                            0.75,
                            0.30,
                            0.30,
                            3.0,
                            "notional_5m_vs_30m",
                            1.25,
                            pool_name == "top10",
                            "orderflow_impulse_admitted",
                            ent_iso,
                            e.entry_price,
                            exit_iso,
                            e.exit_price or "",
                            "scheduled_risk_window_flatten"
                            if is_flattened
                            else ("candle_15m" if is_closed else ""),
                            is_closed,
                            round(pnl, 6),
                            round(ret_pct, 6),
                        ]
                    )
            print(
                f"  💾 已输出 {acc} [{pool_name}] 回放事件: "
                f"{out_csv.name} ({len(admitted_execs)} 行)"
            )

        report_summary[pool_name] = {
            "n_admitted_trades": len(admitted_execs),
            "total_net_pnl_usdt": res.oos_pnl,
            "max_drawdown_pct": res.mdd_pct,
        }

    report_path = DATA_DIR / "account_replay_report.json"
    report_dict = {
        "analysis": "dual-universe replay of deployed 4-account configurations",
        "source_watermark_start": manifest.watermark_start.isoformat(),
        "source_watermark_end": manifest.watermark_end.isoformat(),
        "created_at": datetime.now(UTC).isoformat(),
        "parameters": UNIFIED_LIVE_PROFILE,
        "views": report_summary,
        "n_admitted_trades": report_summary["top10"]["n_admitted_trades"],
        "total_net_pnl_usdt": report_summary["top10"]["total_net_pnl_usdt"],
        "max_drawdown_pct": report_summary["top10"]["max_drawdown_pct"],
    }
    with report_path.open("w", encoding="utf-8") as f:
        json.dump(report_dict, f, indent=2, ensure_ascii=False)
    print(f"\n📋 已更新回放报告: {report_path.name}")
    print("🎉 4 账户双视角离线回放事件流全部更新就绪!")


if __name__ == "__main__":
    main()
