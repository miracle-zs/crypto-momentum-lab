#!/usr/bin/env python3
"""Replay the four live-entry policies on the locally pulled server exports."""

from __future__ import annotations

import argparse
import csv
import gzip
import json
from collections import Counter
from pathlib import Path
from typing import Any

from analyze_breakout_acceptance import (
    format_time,
    load_states,
    parse_time,
)
from backtest_live_exit_grace import (
    LiveEntry,
    build_curve,
    downsample_curve,
    load_live_entries,
    simulate_policy,
    summarize_policy,
)
from backtest_live_gainer_top20 import (
    add_filter_fields,
    write_curve_csv,
    write_trade_csv,
)


def load_gainer_ranks(path: Path) -> dict[str, int]:
    ranks: dict[str, int] = {}
    opener = gzip.open if path.suffix == ".gz" else Path.open
    with opener(path, "rt", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            order_id = row.get("entry_order_id") or ""
            if not order_id:
                continue
            try:
                ranks[order_id] = int(row.get("gainer_rank") or 0)
            except ValueError:
                ranks[order_id] = 0
    return ranks


def coverage_reason(entry: LiveEntry, states: dict[str, Any]) -> str | None:
    series = states.get(entry.symbol)
    if series is None:
        return "no_state_for_symbol"
    if series.at_or_before(entry.entry_epoch) is None:
        return "state_starts_after_entry"
    return None


def prepend_start(
    curve: list[dict[str, Any]],
    *,
    start_epoch: float,
    initial_equity: float,
) -> list[dict[str, Any]]:
    baseline = {
        "epoch": start_epoch,
        "at": format_time(start_epoch),
        "equity_usdt": initial_equity,
        "realized_net_pnl_usdt": 0.0,
        "unrealized_pnl_usdt": 0.0,
        "margin_occupied_usdt": 0.0,
        "gross_notional_occupied_usdt": 0.0,
        "open_positions": 0,
    }
    return [baseline, *[row for row in curve if row["epoch"] > start_epoch]]


def censor_exits_after_data_end(
    trades: list[dict[str, Any]],
    states: dict[str, Any],
) -> None:
    """Do not turn a timeout beyond the last exported state into a fake exit."""
    for trade in trades:
        if not trade.get("closed") or trade.get("exit_epoch") is None:
            continue
        series = states.get(trade["symbol"])
        if series is None or not series.rows:
            continue
        data_end = series.rows[-1][0] + 15.0
        if float(trade["exit_epoch"]) <= data_end + 1e-6:
            continue
        marked_price = series.rows[-1][3]
        marked_gross = trade["quantity"] * (marked_price - trade["entry_price"])
        trade.update(
            {
                "exit_epoch": None,
                "exit_at": None,
                "exit_price": None,
                "exit_reason": "open_at_data_end",
                "closed": False,
                "exit_fee_usdt": 0.0,
                "gross_pnl_usdt": None,
                "net_pnl_usdt": None,
                "marked_price": marked_price,
                "marked_gross_pnl_usdt": marked_gross,
                "marked_net_pnl_usdt": marked_gross - trade["entry_fee_usdt"],
                "holding_minutes": None,
            }
        )


def state_gap_reason(
    trade: dict[str, Any],
    states: dict[str, Any],
    gap_index: dict[str, list[tuple[float, float, float]]],
    *,
    global_state_end: float,
    max_gap_seconds: float,
) -> str | None:
    series = states.get(trade["symbol"])
    if series is None or not series.rows:
        return "no_state_for_symbol"
    start = float(trade["entry_epoch"])
    if trade.get("closed") and trade.get("exit_epoch") is not None:
        end = float(trade["exit_epoch"])
    else:
        end = global_state_end
        if global_state_end - (series.rows[-1][0] + 15.0) > max_gap_seconds:
            return "symbol_ends_before_evaluation"
    for gap_start, gap_end, gap_seconds in gap_index.get(trade["symbol"], []):
        if gap_seconds > max_gap_seconds and gap_start < end and gap_end > start:
            return "crosses_state_gap"
    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fills", type=Path, required=True)
    parser.add_argument("--orders", type=Path, required=True)
    parser.add_argument("--states", type=Path, required=True)
    parser.add_argument("--gainer-ranks", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-id", default="live-b1-long-100u-5x-v1")
    parser.add_argument(
        "--start-at",
        default="2026-07-25T13:13:00+00:00",
        help="UTC analysis start; 2026-07-25 21:13 Beijing is 13:13 UTC",
    )
    parser.add_argument("--initial-equity", type=float, default=1000.0)
    parser.add_argument("--leverage", type=float, default=5.0)
    parser.add_argument("--fee-rate", type=float, default=0.0005)
    parser.add_argument(
        "--max-state-gap-seconds",
        type=float,
        default=900.0,
        help="Reject a trade whose replay interval crosses a longer state gap; 900s is one 15m candle.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start_epoch = parse_time(args.start_at)
    raw_entries = [
        entry
        for entry in load_live_entries(args.fills, args.orders, run_id=args.run_id or None)
        if entry.entry_epoch >= start_epoch
    ]
    ranks = load_gainer_ranks(args.gainer_ranks)
    states = load_states(args.states)

    missing_rank_ids = [entry.order_id for entry in raw_entries if entry.order_id not in ranks]
    coverage = Counter(
        reason or "replayable"
        for entry in raw_entries
        for reason in [coverage_reason(entry, states)]
    )
    replayable_entries = [
        entry for entry in raw_entries if coverage_reason(entry, states) is None
    ]
    raw_top20 = [entry for entry in raw_entries if 1 <= ranks.get(entry.order_id, 0) <= 20]
    top20_entries = [
        entry for entry in replayable_entries if 1 <= ranks.get(entry.order_id, 0) <= 20
    ]

    state_times = [row[0] for series in states.values() for row in series.rows]
    global_state_end = max(state_times) if state_times else start_epoch
    gap_index = {
        symbol: [
            (previous[0], current[0], current[0] - previous[0])
            for previous, current in zip(series.rows, series.rows[1:], strict=False)
        ]
        for symbol, series in states.items()
    }

    cohorts: dict[str, list[LiveEntry]] = {
        "all_live": replayable_entries,
        "gainer_top20": top20_entries,
    }
    all_trades: list[dict[str, Any]] = []
    curves: dict[str, list[dict[str, Any]]] = {}
    summaries: dict[str, dict[str, Any]] = {}
    strict_coverage: dict[str, Any] = {}
    for cohort, cohort_entries in cohorts.items():
        candidate_trades: dict[int, list[dict[str, Any]]] = {}
        valid_ids_by_grace: dict[int, set[str]] = {}
        candidate_reason_counts: dict[str, dict[str, int]] = {}
        for grace_bars in (1, 8):
            candidate = simulate_policy(
                cohort_entries,
                states,
                grace_bars=grace_bars,
                fee_rate=args.fee_rate,
            )
            censor_exits_after_data_end(candidate, states)
            reasons = Counter(
                state_gap_reason(
                    trade,
                    states,
                    gap_index,
                    global_state_end=global_state_end,
                    max_gap_seconds=args.max_state_gap_seconds,
                )
                or "valid"
                for trade in candidate
            )
            candidate_trades[grace_bars] = candidate
            valid_ids_by_grace[grace_bars] = {
                trade["order_id"]
                for trade in candidate
                if state_gap_reason(
                    trade,
                    states,
                    gap_index,
                    global_state_end=global_state_end,
                    max_gap_seconds=args.max_state_gap_seconds,
                )
                is None
            }
            candidate_reason_counts[str(grace_bars)] = dict(sorted(reasons.items()))

        common_valid_ids = valid_ids_by_grace[1] & valid_ids_by_grace[8]
        strict_coverage[cohort] = {
            "candidate_entries": len(cohort_entries),
            "valid_entries_grace_1": len(valid_ids_by_grace[1]),
            "valid_entries_grace_8": len(valid_ids_by_grace[8]),
            "common_entries_used_for_both_policies": len(common_valid_ids),
            "candidate_reason_counts_by_grace": candidate_reason_counts,
        }
        for grace_bars in (1, 8):
            trades = add_filter_fields(
                [
                    trade
                    for trade in candidate_trades[grace_bars]
                    if trade["order_id"] in common_valid_ids
                ],
                ranks,
                cohort,
            )
            curve = prepend_start(
                build_curve(
                    trades,
                    states,
                    initial_equity=args.initial_equity,
                    leverage=args.leverage,
                ),
                start_epoch=start_epoch,
                initial_equity=args.initial_equity,
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
        "analysis_window": {
            "start_at_utc": format_time(start_epoch),
            "start_at_beijing": "2026-07-25 21:13:00+08:00",
            "server_state_first_at_utc": format_time(min(state_times)) if state_times else None,
            "server_state_last_at_utc": format_time(max(state_times)) if state_times else None,
        },
        "source_data": {
            "entries_source": "server_exports/cml-gainer-top20-from-20260725/live_exchange_orders.csv.gz + live_account_fill_events.csv.gz",
            "gainer_rank_source": "server_exports/cml-gainer-top20-from-20260725/live_entry_gainer_rank.csv.gz",
            "state_source": "server-pulled legacy/current partitions plus locally pulled 2026-08-25 and 2026-08-28..31 snapshots, normalized and de-duplicated locally",
            "state_rows_loaded": sum(len(series.rows) for series in states.values()),
            "state_symbols_loaded": len(states),
        },
        "entry_cohort": {
            "raw_entries": len(raw_entries),
            "replayable_entries": len(replayable_entries),
            "excluded_entries": len(raw_entries) - len(replayable_entries),
            "coverage_reasons": dict(sorted(coverage.items())),
            "raw_top20_entries": len(raw_top20),
            "replayable_top20_entries": len(top20_entries),
            "strict_common_entries_all_live": strict_coverage["all_live"][
                "common_entries_used_for_both_policies"
            ],
            "strict_common_entries_top20": strict_coverage["gainer_top20"][
                "common_entries_used_for_both_policies"
            ],
            "missing_rank_order_ids": len(missing_rank_ids),
            "first_raw_entry_at_utc": format_time(raw_entries[0].entry_epoch) if raw_entries else None,
            "last_raw_entry_at_utc": format_time(raw_entries[-1].entry_epoch) if raw_entries else None,
            "first_replayable_entry_at_utc": format_time(replayable_entries[0].entry_epoch) if replayable_entries else None,
            "last_replayable_entry_at_utc": format_time(replayable_entries[-1].entry_epoch) if replayable_entries else None,
        },
        "filter": {
            "definition": "server universe_entries.gainer_rank at the latest active universe snapshot observed at or before each live order created_at",
            "top20_rank_inclusive": [1, 20],
            "point_in_time_no_lookahead": True,
        },
        "assumptions": {
            "initial_equity_usdt": args.initial_equity,
            "leverage": args.leverage,
            "fee_rate": args.fee_rate,
            "decision_profit_pct": 0.001,
            "recovery_profit_pct": 0.0088,
            "exit_grace_bars": [1, 8],
            "price_path": "normalized exported runtime_market_states_15s; direct close uses 15m close; recovery limit uses 15s high touch",
            "entry_policy": "actual primary live BUY fills held fixed; only the rank cohort and exit grace are counterfactual",
            "coverage_policy": "exclude an entry unless a valid OHLC state exists at or before its actual fill time; excluded entries are reported rather than treated as open forever",
            "max_state_gap_seconds": args.max_state_gap_seconds,
            "gap_policy": "use the common entry set valid under both grace policies; reject a trade interval crossing a state gap longer than one 15m candle or ending before the global evaluation timestamp",
        },
        "strict_coverage": strict_coverage,
        "policies": summaries,
        "visual_curve_interval_seconds": 1800,
        "visual_curves": {
            key: downsample_curve(curve, interval_seconds=1800)
            for key, curve in curves.items()
        },
    }
    (output_dir / "live_gainer_top20_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
