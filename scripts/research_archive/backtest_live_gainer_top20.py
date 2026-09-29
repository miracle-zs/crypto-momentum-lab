#!/usr/bin/env python3
"""Replay live entries with a point-in-time gainer-rank top-20 filter."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

from backtest_live_exit_grace import (
    LiveEntry,
    build_curve,
    downsample_curve,
    load_live_entries,
    simulate_policy,
    summarize_policy,
)
from analyze_breakout_acceptance import format_time, load_states


def load_gainer_ranks(path: Path) -> dict[str, int]:
    ranks: dict[str, int] = {}
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            order_id = row.get("entry_order_id") or ""
            if not order_id:
                continue
            try:
                ranks[order_id] = int(row.get("gainer_rank") or 0)
            except ValueError:
                ranks[order_id] = 0
    return ranks


def add_filter_fields(
    trades: list[dict[str, Any]],
    ranks: dict[str, int],
    cohort: str,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for trade in trades:
        row = dict(trade)
        row["cohort"] = cohort
        row["gainer_rank"] = ranks.get(row["order_id"], 0)
        output.append(row)
    return output


def write_trade_csv(path: Path, trades: list[dict[str, Any]]) -> None:
    fields = [
        "cohort",
        "grace_bars",
        "entry_id",
        "order_id",
        "symbol",
        "gainer_rank",
        "entry_at",
        "entry_price",
        "quantity",
        "entry_notional_usdt",
        "entry_fee_usdt",
        "exit_at",
        "exit_price",
        "exit_reason",
        "closed",
        "exit_fee_usdt",
        "gross_pnl_usdt",
        "net_pnl_usdt",
        "marked_price",
        "marked_gross_pnl_usdt",
        "marked_net_pnl_usdt",
        "holding_minutes",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows({field: trade.get(field) for field in fields} for trade in trades)


def write_curve_csv(path: Path, curves: dict[str, list[dict[str, Any]]]) -> None:
    fields = [
        "cohort",
        "grace_bars",
        "at",
        "epoch",
        "equity_usdt",
        "realized_net_pnl_usdt",
        "unrealized_pnl_usdt",
        "margin_occupied_usdt",
        "gross_notional_occupied_usdt",
        "open_positions",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for key, curve in curves.items():
            cohort, grace_text = key.rsplit("_grace_", 1)
            for row in curve:
                writer.writerow(
                    {
                        "cohort": cohort,
                        "grace_bars": int(grace_text),
                        **{field: row.get(field) for field in fields[2:]},
                    }
                )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fills", type=Path, required=True)
    parser.add_argument("--orders", type=Path, required=True)
    parser.add_argument("--states", type=Path, required=True)
    parser.add_argument("--gainer-ranks", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-id", default="live-b1-long-100u-5x-v1")
    parser.add_argument("--initial-equity", type=float, default=1000.0)
    parser.add_argument("--leverage", type=float, default=5.0)
    parser.add_argument("--fee-rate", type=float, default=0.0005)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    entries = load_live_entries(args.fills, args.orders, run_id=args.run_id or None)
    ranks = load_gainer_ranks(args.gainer_ranks)
    missing = [entry.order_id for entry in entries if entry.order_id not in ranks]
    if missing:
        raise SystemExit(f"missing gainer ranks for {len(missing)} entries: {missing[:5]}")
    top20_entries = [
        entry for entry in entries if 1 <= ranks.get(entry.order_id, 0) <= 20
    ]
    states = load_states(args.states)

    cohorts: dict[str, list[LiveEntry]] = {
        "all_190": entries,
        "gainer_top20": top20_entries,
    }
    all_trades: list[dict[str, Any]] = []
    curves: dict[str, list[dict[str, Any]]] = {}
    summaries: dict[str, dict[str, Any]] = {}
    for cohort, cohort_entries in cohorts.items():
        for grace_bars in (0, 1, 8):
            trades = add_filter_fields(
                simulate_policy(
                    cohort_entries,
                    states,
                    grace_bars=grace_bars,
                    fee_rate=args.fee_rate,
                ),
                ranks,
                cohort,
            )
            curve = build_curve(
                trades,
                states,
                initial_equity=args.initial_equity,
                leverage=args.leverage,
            )
            key = f"{cohort}_grace_{grace_bars}"
            all_trades.extend(trades)
            curves[key] = curve
            summaries[key] = summarize_policy(
                trades,
                curve,
                initial_equity=args.initial_equity,
                leverage=args.leverage,
            )

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    write_trade_csv(output_dir / "live_gainer_top20_trades.csv", all_trades)
    write_curve_csv(output_dir / "live_gainer_top20_curves.csv", curves)
    report = {
        "run_id": args.run_id,
        "filter": {
            "definition": "server universe_entries.gainer_rank at latest active snapshot at or before each live order created_at",
            "top20_rank_inclusive": [1, 20],
            "all_entries": len(entries),
            "selected_entries": len(top20_entries),
            "excluded_entries": len(entries) - len(top20_entries),
            "selected_symbols": len({entry.symbol for entry in top20_entries}),
            "missing_rank_entries": len(missing),
        },
        "entry_cohort": {
            "first_entry_at": format_time(entries[0].entry_epoch) if entries else None,
            "last_entry_at": format_time(entries[-1].entry_epoch) if entries else None,
            "all_total_entry_notional_usdt": sum(
                entry.quantity * entry.entry_price for entry in entries
            ),
            "top20_total_entry_notional_usdt": sum(
                entry.quantity * entry.entry_price for entry in top20_entries
            ),
        },
        "assumptions": {
            "initial_equity_usdt": args.initial_equity,
            "leverage": args.leverage,
            "fee_rate": args.fee_rate,
            "decision_profit_pct": 0.001,
            "recovery_profit_pct": 0.0088,
            "exit_grace_bars": [0, 1, 8],
            "price_path": "exported runtime_market_states_15s; direct close uses 15m close; limit uses 15s high touch",
            "entry_policy": "actual primary live fills held fixed; top20 filter is applied point-in-time to the entry cohort",
        },
        "policies": summaries,
        "visual_curve_interval_seconds": 300,
        "visual_curves": {
            key: downsample_curve(curve) for key, curve in curves.items()
        },
    }
    (output_dir / "live_gainer_top20_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
