#!/usr/bin/env python3
"""Compare realized live performance across positive-gainer Top-N cutoffs.

The analysis is deliberately based on account-side fills.  Local exchange
orders provide the entry intent and the point-in-time rank map provides the
gainer rank at either order creation or the first real fill.  The output is a
filter-only counterfactual: actual fills, exits, and position sizing remain
fixed while only the rank cohort changes.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from analyze_live_trading_data import (
    aggregate_fills_by_order,
    as_float,
    iso,
    load_fills,
    load_intents,
    load_orders,
    pnl_stats,
    reconstruct_trades,
)

LOCAL_TZ = ZoneInfo("Asia/Shanghai")
TOP_COUNTS = (10, 20, 30, 50, 100)
BANDS = (
    ("1-10", 1, 10),
    ("11-20", 11, 20),
    ("21-30", 21, 30),
    ("31-50", 31, 50),
    ("51-100", 51, 100),
    ("unranked", None, None),
)


def read_csv_gz(path: Path) -> list[dict[str, str]]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def number(value: Any, default: int = 0) -> int:
    try:
        return int(str(value or "").strip())
    except ValueError:
        return default


def decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except ArithmeticError:
        return Decimal("0")


def float_value(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def bool_value(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes"}


def load_rank_map(path: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in read_csv_gz(path):
        parsed = dict(row)
        parsed["gainer_rank_num"] = number(row.get("gainer_rank"))
        parsed["loser_rank_num"] = number(row.get("loser_rank"))
        parsed["utc_day_return_num"] = float_value(row.get("utc_day_return"))
        parsed["is_target_bool"] = bool_value(row.get("is_target"))
        key = row.get("client_order_id") or row.get("observation_id") or ""
        if key:
            result[key] = parsed
    return result


def rank_status(row: dict[str, Any] | None) -> str:
    if row is None:
        return "no_rank_row"
    if row.get("gainer_rank_num", 0) > 0:
        return "positive_gainer"
    if row.get("loser_rank_num", 0) > 0:
        return "loser_only"
    if row.get("snapshot_id"):
        return "snapshot_unranked"
    return "no_snapshot"


def rank_band(rank: int) -> str:
    for label, lower, upper in BANDS[:-1]:
        assert lower is not None and upper is not None
        if lower <= rank <= upper:
            return label
    return "unranked"


def attach_rank_data(
    trades: list[dict[str, Any]],
    orders: list[dict[str, Any]],
    order_rank_map: dict[str, dict[str, Any]],
    fill_rank_map: dict[str, dict[str, Any]],
    *,
    anchor: str,
) -> None:
    exchange_to_order = {
        order["exchange_order_id"]: order
        for order in orders
        if order.get("exchange_order_id")
    }
    for index, trade in enumerate(trades):
        order = exchange_to_order.get(trade["entry_order_id"], {})
        client_order_id = order.get("client_order_id", "")
        rank_row = (
            fill_rank_map.get(client_order_id)
            if anchor == "fill"
            else order_rank_map.get(client_order_id)
        )
        if rank_row is None:
            rank_row = order_rank_map.get(client_order_id) or fill_rank_map.get(
                client_order_id
            )
        gainer_rank = number((rank_row or {}).get("gainer_rank_num"))
        loser_rank = number((rank_row or {}).get("loser_rank_num"))
        trade.update(
            {
                "trade_key": (
                    f"{trade['entry_order_id']}:{trade['exit_order_id']}:{index}"
                ),
                "entry_client_order_id": client_order_id,
                "entry_created_at": order.get("created_at"),
                "gainer_rank": gainer_rank,
                "loser_rank": loser_rank,
                "rank_band": rank_band(gainer_rank),
                "rank_status": rank_status(rank_row),
                "rank_snapshot_id": (rank_row or {}).get("snapshot_id"),
                "rank_snapshot_observed_at": (rank_row or {}).get(
                    "snapshot_observed_at"
                ),
                "rank_utc_day": (rank_row or {}).get("utc_day"),
                "rank_utc_day_return": (rank_row or {}).get(
                    "utc_day_return_num"
                ),
                "rank_is_target": (rank_row or {}).get("is_target_bool"),
                "entry_order_rank": number(
                    order_rank_map.get(client_order_id, {}).get("gainer_rank_num")
                ),
                "entry_fill_rank": number(
                    fill_rank_map.get(client_order_id, {}).get("gainer_rank_num")
                ),
            }
        )


def assign_time_split(trades: list[dict[str, Any]]) -> dict[str, Any]:
    rankable = [trade for trade in trades if trade["gainer_rank"] > 0]
    entry_times: dict[str, datetime] = {}
    for trade in rankable:
        entry_id = trade["entry_order_id"]
        opened_at = trade["opened_at"]
        if opened_at is not None:
            entry_times[entry_id] = min(
                opened_at,
                entry_times.get(entry_id, opened_at),
            )
    ordered_entries = sorted(entry_times, key=lambda item: entry_times[item])
    if len(ordered_entries) < 2:
        train_entry_ids = set(ordered_entries)
        cutoff = entry_times[ordered_entries[-1]] if ordered_entries else None
    else:
        cut = max(1, min(len(ordered_entries) - 1, int(len(ordered_entries) * 0.6)))
        train_entry_ids = set(ordered_entries[:cut])
        cutoff = entry_times[ordered_entries[cut]]
    for trade in trades:
        if trade["gainer_rank"] <= 0:
            trade["time_split"] = "unranked"
        elif trade["entry_order_id"] in train_entry_ids:
            trade["time_split"] = "train60"
        else:
            trade["time_split"] = "holdout40"
    return {
        "rankable_entry_orders": len(ordered_entries),
        "train_entry_orders": len(train_entry_ids),
        "holdout_entry_orders": len(ordered_entries) - len(train_entry_ids),
        "cutoff_first_holdout_entry_at": iso(cutoff),
        "first_rankable_entry_at": iso(entry_times[ordered_entries[0]])
        if ordered_entries
        else None,
        "last_rankable_entry_at": iso(entry_times[ordered_entries[-1]])
        if ordered_entries
        else None,
    }


def trade_stats(trades: list[dict[str, Any]]) -> dict[str, Any]:
    stats = pnl_stats(trades)
    net = sum((trade["net_pnl"] for trade in trades), start=Decimal("0"))
    notional = sum(
        (trade["entry_price"] * trade["quantity"] for trade in trades),
        start=Decimal("0"),
    )
    stats.update(
        {
            "closed_trade_segments": len(trades),
            "entry_orders": len({trade["entry_order_id"] for trade in trades}),
            "symbols": len({trade["symbol"] for trade in trades}),
            "weighted_net_return_pct": as_float(net / notional * 100)
            if notional
            else None,
        }
    )
    return stats


def topn_summary(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for top_count in TOP_COUNTS:
        cohort = [
            trade
            for trade in trades
            if 1 <= trade["gainer_rank"] <= top_count
        ]
        for split in ("all", "train60", "holdout40"):
            selected = (
                cohort
                if split == "all"
                else [trade for trade in cohort if trade["time_split"] == split]
            )
            rows.append(
                {
                    "cohort": f"top{top_count}",
                    "top_count": top_count,
                    "split": split,
                    **trade_stats(selected),
                }
            )
    return rows


def band_summary(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for label, _lower, _upper in BANDS:
        cohort = [trade for trade in trades if trade["rank_band"] == label]
        for split in ("all", "train60", "holdout40"):
            selected = (
                cohort
                if split == "all"
                else [trade for trade in cohort if trade["time_split"] == split]
            )
            rows.append(
                {
                    "rank_band": label,
                    "split": split,
                    **trade_stats(selected),
                }
            )
    return rows


def daily_summary(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for day in sorted(
        {
            trade["opened_at"].astimezone(LOCAL_TZ).date().isoformat()
            for trade in trades
            if trade["opened_at"] is not None
        }
    ):
        day_trades = [
            trade
            for trade in trades
            if trade["opened_at"].astimezone(LOCAL_TZ).date().isoformat() == day
        ]
        for cohort in ("all_closed", *[f"top{count}" for count in TOP_COUNTS]):
            selected = (
                day_trades
                if cohort == "all_closed"
                else [
                    trade
                    for trade in day_trades
                    if 1 <= trade["gainer_rank"] <= int(cohort[3:])
                ]
            )
            rows.append(
                {
                    "local_entry_day": day,
                    "cohort": cohort,
                    **trade_stats(selected),
                }
            )
    return rows


def signal_summary(path: Path) -> list[dict[str, Any]]:
    rows = read_csv_gz(path)
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        rank = number(row.get("gainer_rank"))
        grouped[rank_band(rank)].append(row)
    result: list[dict[str, Any]] = []
    for label, _lower, _upper in BANDS:
        selected = grouped.get(label, [])
        result.append(
            {
                "rank_band": label,
                "signals": len(selected),
                "symbols": len({row.get("symbol") for row in selected}),
                "positive_daily_return_signals": sum(
                    1 for row in selected if decimal(row.get("utc_day_return")) > 0
                ),
            }
        )
    return result


def rank_transition_summary(trades: list[dict[str, Any]]) -> dict[str, Any]:
    by_entry: dict[str, dict[str, Any]] = {}
    for trade in trades:
        by_entry.setdefault(
            trade["entry_order_id"],
            {
                "order_rank": trade["entry_order_rank"],
                "fill_rank": trade["entry_fill_rank"],
            },
        )
    rows = list(by_entry.values())
    return {
        "entry_orders": len(rows),
        "rank_changed_entry_orders": sum(
            row["order_rank"] != row["fill_rank"] for row in rows
        ),
        "entered_top10_at_fill_after_not_top10_at_order": sum(
            row["fill_rank"] > 0
            and row["fill_rank"] <= 10
            and not (0 < row["order_rank"] <= 10)
            for row in rows
        ),
        "left_top10_at_fill_after_top10_at_order": sum(
            0 < row["order_rank"] <= 10
            and not (0 < row["fill_rank"] <= 10)
            for row in rows
        ),
        "entered_top20_at_fill_after_not_top20_at_order": sum(
            row["fill_rank"] > 0
            and row["fill_rank"] <= 20
            and not (0 < row["order_rank"] <= 20)
            for row in rows
        ),
        "left_top20_at_fill_after_top20_at_order": sum(
            0 < row["order_rank"] <= 20
            and not (0 < row["fill_rank"] <= 20)
            for row in rows
        ),
    }


def serialise_trade(trade: dict[str, Any]) -> dict[str, Any]:
    fields = (
        "trade_key",
        "symbol",
        "entry_order_id",
        "entry_client_order_id",
        "exit_order_id",
        "quantity",
        "entry_price",
        "exit_price",
        "gross_model_pnl",
        "exchange_realized_pnl",
        "entry_fee",
        "exit_fee",
        "total_fees",
        "net_pnl",
        "return_pct",
        "opened_at",
        "closed_at",
        "hold_minutes",
        "entry_reason",
        "exit_reason",
        "gainer_rank",
        "loser_rank",
        "rank_band",
        "rank_status",
        "rank_snapshot_id",
        "rank_snapshot_observed_at",
        "rank_utc_day",
        "rank_utc_day_return",
        "rank_is_target",
        "entry_order_rank",
        "entry_fill_rank",
        "time_split",
    )
    output: dict[str, Any] = {}
    for field in fields:
        value = trade.get(field)
        if isinstance(value, datetime):
            output[field] = iso(value)
        elif isinstance(value, Decimal):
            output[field] = as_float(value)
        else:
            output[field] = value
    return output


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def descending_numeric(rows: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: float(row[field]) if row.get(field) not in (None, "") else 0.0,
        reverse=True,
    )


def build_analysis(input_dir: Path, output_dir: Path, anchor: str) -> dict[str, Any]:
    fills = load_fills(input_dir / "account_fill_events.csv.gz")
    orders = load_orders(input_dir / "exchange_orders.csv.gz")
    intents = load_intents(input_dir / "order_intents.csv.gz")
    event_summaries = read_csv_gz(
        input_dir / "exchange_order_event_summary.csv.gz"
    )
    order_rank_map = load_rank_map(input_dir / "live_order_gainer_rank.csv.gz")
    fill_rank_map = load_rank_map(
        input_dir / "live_entry_fill_gainer_rank.csv.gz"
    )

    order_aggs = aggregate_fills_by_order(fills)
    orders_by_exchange_id = {
        order["exchange_order_id"]: order
        for order in orders
        if order.get("exchange_order_id")
    }
    trades, matched_segments, unmatched_quantity = reconstruct_trades(
        order_aggs,
        orders_by_exchange_id,
        intents,
    )
    attach_rank_data(
        trades,
        orders,
        order_rank_map,
        fill_rank_map,
        anchor=anchor,
    )
    split_metadata = assign_time_split(trades)

    output_dir.mkdir(parents=True, exist_ok=True)
    serialised_trades = [serialise_trade(trade) for trade in trades]
    write_csv(output_dir / "live_trades_reconstructed.csv", serialised_trades)
    write_csv(
        output_dir / "trades_sorted_by_net_pnl.csv",
        descending_numeric(serialised_trades, "net_pnl"),
    )
    write_csv(
        output_dir / "trades_sorted_by_return_pct.csv",
        descending_numeric(serialised_trades, "return_pct"),
    )
    write_csv(output_dir / "topn_summary.csv", topn_summary(trades))
    write_csv(output_dir / "rank_band_summary.csv", band_summary(trades))
    write_csv(output_dir / "daily_summary.csv", daily_summary(trades))
    write_csv(
        output_dir / "signal_rank_summary.csv",
        signal_summary(input_dir / "live_signal_gainer_rank.csv.gz"),
    )

    rank_status_counts = Counter(trade["rank_status"] for trade in trades)
    rank_band_counts = Counter(trade["rank_band"] for trade in trades)
    fill_order_ids = {fill["order_id"] for fill in fills}
    linked_order_ids = {
        fill["order_id"] for fill in fills if fill["order_id"] in orders_by_exchange_id
    }
    entry_order_ids = {
        trade["entry_order_id"] for trade in trades
    }
    trade_times = [fill["trade_at"] for fill in fills if fill["trade_at"]]
    report: dict[str, Any] = {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "anchor": anchor,
        "method": {
            "actual_fills": "account_fill_events grouped by exchange order",
            "trade_pairing": (
                "FIFO long-lot matching of BUY account fills to SELL account fills"
            ),
            "rank_join": (
                "latest activated universe snapshot observed at or before "
                "first real fill"
                if anchor == "fill"
                else (
                    "latest activated universe snapshot observed at or before "
                    "exchange order creation"
                )
            ),
            "counterfactual": (
                "actual entry fills, exit fills, size, and holding periods fixed; "
                "only gainer-rank cohort filtered"
            ),
            "top_n_definition": (
                "positive UTC-day gainer rank in the inclusive range 1..N"
            ),
            "unranked_definition": (
                "no positive gainer_rank; loser-only and snapshot-unranked rows "
                "are kept separate from Top-N"
            ),
        },
        "source_data": {
            "input_dir": str(input_dir),
            "account_fill_rows": len(fills),
            "exchange_order_rows": len(orders),
            "order_intent_rows": len(intents),
            "exchange_order_event_summary_rows": len(event_summaries),
            "exchange_order_event_count": sum(
                number(row.get("event_count")) for row in event_summaries
            ),
            "order_rank_rows": len(order_rank_map),
            "fill_rank_rows": len(fill_rank_map),
            "first_account_fill_at_utc": iso(min(trade_times))
            if trade_times
            else None,
            "last_account_fill_at_utc": iso(max(trade_times))
            if trade_times
            else None,
            "account_labels": sorted(
                {f"{fill['environment']}/{fill['account_label']}" for fill in fills}
            ),
        },
        "reconstruction": {
            "filled_order_aggregates": len(order_aggs),
            "matched_trade_segments": matched_segments,
            "unique_closed_entry_orders": len(entry_order_ids),
            "unmatched_or_open_quantity": as_float(unmatched_quantity),
            "unique_fill_order_ids": len(fill_order_ids),
            "fill_order_link_coverage_pct": (
                100 * len(linked_order_ids) / len(fill_order_ids)
            )
            if fill_order_ids
            else None,
            "raw_fill_trade_id_duplicates": len(fills)
            - len({(fill["symbol"], fill["trade_id"]) for fill in fills}),
        },
        "order_event_audit": {
            "orders_with_filled_event": sum(
                bool(row.get("filled_at")) for row in event_summaries
            ),
            "orders_with_canceled_event": sum(
                bool(row.get("canceled_at")) for row in event_summaries
            ),
            "orders_with_rejected_event": sum(
                bool(row.get("rejected_at")) for row in event_summaries
            ),
            "unknown_pending_event_count": sum(
                number(row.get("unknown_pending_count"))
                for row in event_summaries
            ),
            "event_summary_cutoff_utc": "2026-09-01T04:33:16+00:00",
        },
        "rank_coverage": {
            "trade_segments": len(trades),
            "positive_gainer_rank_segments": sum(
                trade["gainer_rank"] > 0 for trade in trades
            ),
            "unranked_segments": sum(
                trade["gainer_rank"] <= 0 for trade in trades
            ),
            "by_status": dict(sorted(rank_status_counts.items())),
            "by_band": dict(sorted(rank_band_counts.items())),
            "order_vs_fill_rank": rank_transition_summary(trades),
        },
        "time_split": split_metadata,
        "topn": topn_summary(trades),
        "rank_bands": band_summary(trades),
        "all_closed": trade_stats(trades),
        "top100_positive_gainer": trade_stats(
            [trade for trade in trades if 1 <= trade["gainer_rank"] <= 100]
        ),
        "unranked": trade_stats(
            [trade for trade in trades if trade["gainer_rank"] <= 0]
        ),
    }
    (output_dir / "analysis.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--anchor", choices=("fill", "order"), default="fill")
    args = parser.parse_args()
    report = build_analysis(args.input_dir, args.output_dir, args.anchor)
    top10_net_pnl = next(
        row
        for row in report["topn"]
        if row["cohort"] == "top10" and row["split"] == "all"
    )["net_pnl_after_fees"]
    print(
        "live gainer Top-N analysis completed: "
        f"closed_segments={report['reconstruction']['matched_trade_segments']} "
        f"top100_net_pnl={report['top100_positive_gainer']['net_pnl_after_fees']:.6f} "
        f"top10_net_pnl={top10_net_pnl:.6f}"
    )


if __name__ == "__main__":
    main()
