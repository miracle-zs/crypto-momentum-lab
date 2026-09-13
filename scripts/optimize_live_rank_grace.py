#!/usr/bin/env python3
"""Walk-forward grid search for live-primary rank and grace hyperparameters.

This search keeps actual live-primary BUY fills fixed and varies only the
point-in-time gainer rank cutoff and 15m grace length.  The +0.88% recovery
target and +0.10% direct-close threshold are held fixed in this first pass so
that the effect of the two requested hyperparameters is isolated.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from analyze_breakout_acceptance import format_time, parse_time
from backtest_live_exit_grace import load_live_entries
from backtest_live_top20_full_official import (
    DIRECT_PROFIT_PCT,
    RECOVERY_PROFIT_PCT,
    build_curve,
    load_candles,
    load_gainer_ranks,
    make_trade,
    replay,
    summarize,
)


METRIC_FIELDS = (
    "n_entries",
    "n_closed",
    "n_open_at_data_end",
    "total_net_pnl_usdt_closed",
    "open_marked_net_pnl_usdt",
    "marked_total_pnl_usdt",
    "entry_notional_usdt",
    "return_on_entry_notional_pct",
    "profit_factor_on_closed",
    "win_rate_on_closed",
    "realized_cashflow_max_drawdown_usdt",
)


def realized_cashflow_max_drawdown(trades: list[dict[str, Any]]) -> float:
    events: list[tuple[float, int, float]] = []
    for trade in trades:
        events.append((float(trade["entry_epoch"]), 0, -float(trade["entry_fee_usdt"])))
        if trade["closed"]:
            events.append(
                (
                    float(trade["exit_epoch"]),
                    1,
                    float(trade["gross_pnl_usdt"]) - float(trade["exit_fee_usdt"]),
                )
            )
    cumulative = 0.0
    peak = 0.0
    drawdown = 0.0
    for _timestamp, _priority, amount in sorted(events):
        cumulative += amount
        peak = max(peak, cumulative)
        drawdown = max(drawdown, peak - cumulative)
    return drawdown


def quick_metrics(trades: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [trade for trade in trades if trade["closed"]]
    open_trades = [trade for trade in trades if not trade["closed"]]
    values = [float(trade["net_pnl_usdt"]) for trade in closed]
    gains = sum(value for value in values if value > 0)
    losses = -sum(value for value in values if value < 0)
    closed_net = sum(values)
    open_marked = sum(float(trade["marked_net_pnl_usdt"]) for trade in open_trades)
    entry_notional = sum(float(trade["entry_notional_usdt"]) for trade in trades)
    marked_total = closed_net + open_marked
    return {
        "n_entries": len(trades),
        "n_closed": len(closed),
        "n_open_at_data_end": len(open_trades),
        "total_net_pnl_usdt_closed": closed_net,
        "open_marked_net_pnl_usdt": open_marked,
        "marked_total_pnl_usdt": marked_total,
        "entry_notional_usdt": entry_notional,
        "return_on_entry_notional_pct": (
            marked_total / entry_notional * 100.0 if entry_notional else None
        ),
        "profit_factor_on_closed": gains / losses if losses else None,
        "win_rate_on_closed": (
            sum(value > 0 for value in values) / len(values) if values else None
        ),
        "realized_cashflow_max_drawdown_usdt": realized_cashflow_max_drawdown(trades),
    }


def select_entries(
    entries: list[Any],
    ranks: dict[str, int | None],
    *,
    rank_max: int,
    start: float | None,
    end: float,
) -> list[Any]:
    return [
        entry
        for entry in entries
        if (start is None or entry.entry_epoch >= start)
        and entry.entry_epoch < end
        and 1 <= (ranks.get(entry.order_id) or 0) <= rank_max
    ]


def add_row(
    rows: list[dict[str, Any]],
    *,
    period: str,
    rank_max: int,
    grace_bars: int,
    metrics: dict[str, Any],
) -> None:
    rows.append(
        {
            "period": period,
            "rank_max": rank_max,
            "grace_bars": grace_bars,
            **metrics,
        }
    )


def top_rows(
    rows: list[dict[str, Any]],
    *,
    period: str,
    metric: str,
    limit: int = 10,
    min_closed: int = 30,
) -> list[dict[str, Any]]:
    candidates = [
        row
        for row in rows
        if row["period"] == period
        and int(row["n_closed"]) >= min_closed
        and row.get(metric) is not None
    ]
    return sorted(candidates, key=lambda row: float(row[metric]), reverse=True)[:limit]


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = ["period", "rank_max", "grace_bars", *METRIC_FIELDS]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows({field: row.get(field) for field in fields} for row in rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fills", type=Path, required=True)
    parser.add_argument("--orders", type=Path, required=True)
    parser.add_argument("--gainer-ranks", type=Path, required=True)
    parser.add_argument("--klines", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-id", default="live-b1-long-100u-5x-v1")
    parser.add_argument("--entry-cutoff", required=True)
    parser.add_argument("--train-end", default="2026-08-25T00:00:00+00:00")
    parser.add_argument("--validation-end", default="2026-08-29T00:00:00+00:00")
    parser.add_argument("--initial-equity", type=float, default=1000.0)
    parser.add_argument("--leverage", type=float, default=5.0)
    parser.add_argument("--fee-rate", type=float, default=0.0005)
    parser.add_argument("--rank-max", type=int, default=100)
    parser.add_argument("--max-grace-bars", type=int, default=12)
    parser.add_argument("--direct-profit-pct", type=float, default=DIRECT_PROFIT_PCT)
    parser.add_argument("--recovery-profit-pct", type=float, default=RECOVERY_PROFIT_PCT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.rank_max < 1 or args.rank_max > 100:
        raise SystemExit("--rank-max must be between 1 and 100")
    if args.max_grace_bars < 0:
        raise SystemExit("--max-grace-bars must not be negative")

    entry_cutoff = parse_time(args.entry_cutoff)
    train_end = parse_time(args.train_end)
    validation_end = parse_time(args.validation_end)
    if not (train_end < validation_end < entry_cutoff):
        raise SystemExit("expected train_end < validation_end < entry_cutoff")

    entries = [
        entry
        for entry in load_live_entries(args.fills, args.orders, run_id=args.run_id)
        if entry.entry_epoch <= entry_cutoff + 1e-6
    ]
    ranks = load_gainer_ranks(args.gainer_ranks)
    rank_unavailable = [
        entry for entry in entries if ranks.get(entry.order_id) is None
    ]
    candles_by_symbol = load_candles(args.klines, {entry.symbol for entry in entries})
    price_data_end = max(
        (candle.end for candles in candles_by_symbol.values() for candle in candles),
        default=entry_cutoff,
    )
    full_data_end = max(entry_cutoff, price_data_end)
    first_entry = entries[0].entry_epoch if entries else entry_cutoff

    periods = [
        ("train", first_entry, train_end, train_end),
        ("validation", train_end, validation_end, validation_end),
        # This is the data that would have been available before the final
        # holdout.  It lets us choose a candidate without looking at test.
        ("pretest", first_entry, validation_end, validation_end),
        ("test", validation_end, entry_cutoff, full_data_end),
        ("full", first_entry, entry_cutoff, full_data_end),
    ]
    rows: list[dict[str, Any]] = []
    period_trades: dict[tuple[str, int, int], list[dict[str, Any]]] = {}
    for period, period_start, period_end, replay_end in periods:
        for grace_bars in range(args.max_grace_bars + 1):
            # Replay each entry only once at the widest rank cutoff.  Smaller
            # cutoffs are nested subsets of this same trade set, so filtering
            # the finished trades is both exact and much faster than replaying
            # the same candles up to 100 times.
            selected = select_entries(
                entries,
                ranks,
                rank_max=args.rank_max,
                start=period_start,
                end=period_end,
            )
            base_trades = [
                make_trade(
                    entry,
                    ranks,
                    replay(
                        entry,
                        candles_by_symbol.get(entry.symbol, []),
                        grace_bars=grace_bars,
                        fee_rate=args.fee_rate,
                        data_end=replay_end,
                        direct_profit_pct=args.direct_profit_pct,
                        recovery_profit_pct=args.recovery_profit_pct,
                    ),
                    grace_bars,
                )
                for entry in selected
            ]
            for rank_max in range(1, args.rank_max + 1):
                trades = [
                    trade
                    for trade in base_trades
                    if 1 <= (trade["gainer_rank"] or 0) <= rank_max
                ]
                period_trades[(period, rank_max, grace_bars)] = trades
                add_row(
                    rows,
                    period=period,
                    rank_max=rank_max,
                    grace_bars=grace_bars,
                    metrics=quick_metrics(trades),
                )

    # Build exact mark-to-market summaries only for the top full-sample
    # candidates, while keeping the grid search inexpensive.
    full_rows = [row for row in rows if row["period"] == "full"]
    final_candidates: dict[str, list[dict[str, Any]]] = {}
    for metric, key in (
        ("marked_total_pnl_usdt", "top_full_net"),
        ("return_on_entry_notional_pct", "top_full_return"),
        ("profit_factor_on_closed", "top_full_pf"),
        ("realized_cashflow_max_drawdown_usdt", "lowest_full_realized_drawdown"),
    ):
        final_candidates[key] = sorted(
            [row for row in full_rows if row.get(metric) is not None and row["n_closed"] >= 30],
            key=lambda row: float(row[metric]),
            reverse=metric != "realized_cashflow_max_drawdown_usdt",
        )[:10]

    pretest_rows = [row for row in rows if row["period"] == "pretest"]
    for metric, key in (
        ("marked_total_pnl_usdt", "top_pretest_net"),
        ("return_on_entry_notional_pct", "top_pretest_return"),
        ("profit_factor_on_closed", "top_pretest_pf"),
    ):
        final_candidates[key] = sorted(
            [row for row in pretest_rows if row.get(metric) is not None and row["n_closed"] >= 100],
            key=lambda row: float(row[metric]),
            reverse=True,
        )[:10]

    # Evaluate candidates chosen only with pretest data on the untouched test
    # period.  This is the closest approximation here to a walk-forward check.
    test_by_pair = {
        (row["rank_max"], row["grace_bars"]): row
        for row in rows
        if row["period"] == "test"
    }
    pretest_holdout: list[dict[str, Any]] = []
    for key in ("top_pretest_net", "top_pretest_return", "top_pretest_pf"):
        for row in final_candidates[key][:5]:
            test_row = test_by_pair.get((row["rank_max"], row["grace_bars"]))
            if test_row is None:
                continue
            pretest_holdout.append(
                {
                    "selection": key,
                    "rank_max": row["rank_max"],
                    "grace_bars": row["grace_bars"],
                    "pretest_marked_total_pnl_usdt": row["marked_total_pnl_usdt"],
                    "pretest_return_on_entry_notional_pct": row["return_on_entry_notional_pct"],
                    "test_marked_total_pnl_usdt": test_row["marked_total_pnl_usdt"],
                    "test_return_on_entry_notional_pct": test_row["return_on_entry_notional_pct"],
                    "test_profit_factor_on_closed": test_row["profit_factor_on_closed"],
                    "test_n_closed": test_row["n_closed"],
                }
            )

    # A simple stability view: rank each parameter pair by validation and test
    # return on entry notional, then prefer the lowest average rank.  This is
    # descriptive, not a claim of statistical significance.
    combo_keys = {(row["rank_max"], row["grace_bars"]) for row in full_rows}
    by_period: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row["period"] in {"validation", "test"}:
            by_period[row["period"]].append(row)
    rank_maps: dict[str, dict[tuple[int, int], int]] = {}
    for period, period_rows in by_period.items():
        ranked = sorted(
            [row for row in period_rows if row.get("return_on_entry_notional_pct") is not None],
            key=lambda row: float(row["return_on_entry_notional_pct"]),
            reverse=True,
        )
        rank_maps[period] = {
            (row["rank_max"], row["grace_bars"]): index + 1
            for index, row in enumerate(ranked)
        }
    stability = []
    for rank_max, grace_bars in combo_keys:
        validation_rank = rank_maps.get("validation", {}).get((rank_max, grace_bars))
        test_rank = rank_maps.get("test", {}).get((rank_max, grace_bars))
        if validation_rank is None or test_rank is None:
            continue
        stability.append(
            {
                "rank_max": rank_max,
                "grace_bars": grace_bars,
                "validation_return_rank": validation_rank,
                "test_return_rank": test_rank,
                "average_return_rank": (validation_rank + test_rank) / 2.0,
            }
        )
    final_candidates["stable_validation_test_return"] = sorted(
        stability, key=lambda row: row["average_return_rank"]
    )[:10]

    exact_candidate_summaries: dict[str, Any] = {}
    candidate_pairs = {
        (row["rank_max"], row["grace_bars"])
        for key in ("top_full_net", "top_full_return", "top_full_pf", "stable_validation_test_return")
        for row in final_candidates[key]
        if "rank_max" in row and "grace_bars" in row
    }
    for rank_max, grace_bars in sorted(candidate_pairs):
        trades = period_trades[("full", rank_max, grace_bars)]
        curve = build_curve(
            trades,
            {symbol: candles_by_symbol[symbol] for symbol in {trade["symbol"] for trade in trades}},
            initial_equity=args.initial_equity,
            leverage=args.leverage,
            data_end=full_data_end,
        )
        exact_candidate_summaries[f"top{rank_max}_grace{grace_bars}"] = summarize(
            trades,
            curve,
            initial_equity=args.initial_equity,
        )

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(output_dir / "rank_grace_grid.csv", rows)
    report = {
        "run_id": args.run_id,
        "entry_cutoff_utc": format_time(entry_cutoff),
        "price_data_end_utc": format_time(price_data_end),
        "split": {
            "train_end_utc": format_time(train_end),
            "validation_end_utc": format_time(validation_end),
            "test_end_utc": format_time(entry_cutoff),
            "periods": "train: first entry to train_end; validation: train_end to validation_end; pretest: first entry to validation_end; test: validation_end to entry cutoff",
        },
        "entry_cohort": {
            "all_live_entries": len(entries),
            "rank_unavailable_entries": len(rank_unavailable),
            "first_entry_at_utc": format_time(entries[0].entry_epoch) if entries else None,
            "last_entry_at_utc": format_time(entries[-1].entry_epoch) if entries else None,
        },
        "grid": {
            "rank_max_values": [1, args.rank_max],
            "rank_max_evaluated": args.rank_max,
            "grace_bars_evaluated": list(range(args.max_grace_bars + 1)),
            "direct_profit_pct_fixed": args.direct_profit_pct,
            "recovery_profit_pct_fixed": args.recovery_profit_pct,
        },
        "method": {
            "entries": "actual live primary BUY fills held fixed",
            "rank": "latest activated universe snapshot at or before each entry order creation time",
            "exit": "official Binance 15m OHLC; first eligible bearish candle; recovery target uses 15m high",
            "selection_warning": "full-sample rankings are descriptive; validation/test rankings are the preferred evidence against in-sample overfit",
        },
        "final_candidates": final_candidates,
        "pretest_holdout": pretest_holdout,
        "exact_mark_to_market_summaries": exact_candidate_summaries,
    }
    (output_dir / "rank_grace_optimization_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
