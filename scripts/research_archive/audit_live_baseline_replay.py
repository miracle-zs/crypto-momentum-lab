#!/usr/bin/env python3
"""Audit why the fixed live-parameter replay differs from the live account."""

from __future__ import annotations

import argparse
import bisect
import csv
import gzip
import json
import math
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def number(value: str | None, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        result = float(value)
    except ValueError:
        return default
    return result if math.isfinite(result) else default


def truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y"}


def read_csv(path: Path) -> list[dict[str, str]]:
    opener = gzip.open if path.suffix == ".gz" else Path.open
    with opener(path, "rt", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def interpolate(points: list[tuple[float, float]], timestamp: float) -> float:
    if not points:
        return float("nan")
    if timestamp <= points[0][0]:
        return points[0][1]
    if timestamp >= points[-1][0]:
        return points[-1][1]
    times = [item[0] for item in points]
    right = bisect.bisect_right(times, timestamp)
    left = right - 1
    t0, v0 = points[left]
    t1, v1 = points[right]
    ratio = (timestamp - t0) / (t1 - t0) if t1 != t0 else 0.0
    return v0 + (v1 - v0) * ratio


def load_equity_points(path: Path) -> list[tuple[float, float]]:
    rows = read_csv(path)
    rows.sort(key=lambda row: parse_time(row["observed_at"]))
    return [
        (
            parse_time(row["observed_at"]).timestamp(),
            number(row.get("wallet_balance")) + number(row.get("unrealized_pnl")),
        )
        for row in rows
    ]


def load_baseline_pnl_points(path: Path) -> list[tuple[float, float]]:
    rows = read_csv(path)
    rows.sort(key=lambda row: parse_time(row["timestamp"]))
    return [
        (
            parse_time(row["timestamp"]).timestamp(),
            number(row.get("baseline_cumulative_pnl_usdt")),
        )
        for row in rows
    ]


def step_value(points: list[tuple[float, float]], timestamp: float) -> float:
    if not points:
        return 0.0
    index = bisect.bisect_right([item[0] for item in points], timestamp) - 1
    return points[index][1] if index >= 0 else 0.0


def within(timestamp: float, start: datetime, end: datetime) -> bool:
    return start.timestamp() <= timestamp <= end.timestamp()


def interval_stats(
    timestamps: list[float],
    values: list[float],
    *,
    start: datetime,
    end: datetime,
) -> dict[str, Any]:
    selected = [value for timestamp, value in zip(timestamps, values) if within(timestamp, start, end)]
    return {"count": len(selected), "sum": round(sum(selected), 8)}


def load_actual_activity(
    fill_path: Path,
    order_path: Path,
    signal_path: Path | None,
    *,
    run_id: str,
    config_hash: str | None,
    start: datetime,
    end: datetime,
    proxy_start: datetime,
) -> dict[str, Any]:
    orders = read_csv(order_path)
    orders_by_id = {
        row.get("exchange_order_id", ""): row
        for row in orders
        if row.get("exchange_order_id")
    }
    run_orders = [row for row in orders if row.get("run_id") == run_id]
    created_by_kind: dict[str, list[float]] = {"entry": [], "exit": []}
    for row in run_orders:
        try:
            timestamp = parse_time(row["created_at"]).timestamp()
        except (KeyError, ValueError):
            continue
        if not within(timestamp, start, end):
            continue
        kind = "exit" if truthy(row.get("reduce_only")) else "entry"
        created_by_kind[kind].append(timestamp)

    fill_rows: list[dict[str, Any]] = []
    for row in read_csv(fill_path):
        if row.get("environment") != "live" or row.get("account_label") != "primary":
            continue
        order = orders_by_id.get(row.get("order_id", ""))
        if order is None or order.get("run_id") != run_id:
            continue
        try:
            timestamp = parse_time(row["trade_at"])
        except (KeyError, ValueError):
            continue
        if not within(timestamp.timestamp(), start, end):
            continue
        if order.get("position_side", "").upper() != "LONG":
            continue
        kind = "exit" if truthy(order.get("reduce_only")) else "entry"
        fill_rows.append(
            {
                "timestamp": timestamp,
                "kind": kind,
                "realized_pnl": number(row.get("realized_pnl")),
                "fee": number(row.get("fee")),
                "symbol": row.get("symbol", ""),
                "order_id": row.get("order_id", ""),
            }
        )

    def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
        grouped = {
            name: [row for row in rows if row["kind"] == name]
            for name in ("entry", "exit")
        }
        return {
            name: {
                "fill_rows": len(items),
                "order_count": len({row["order_id"] for row in items}),
                "realized_pnl_usdt": round(sum(row["realized_pnl"] for row in items), 8),
                "fees_usdt": round(sum(row["fee"] for row in items), 8),
            }
            for name, items in grouped.items()
        }

    pre_proxy = [row for row in fill_rows if row["timestamp"] < proxy_start]
    post_proxy = [row for row in fill_rows if row["timestamp"] >= proxy_start]
    signals: dict[str, Any] = {"available": False}
    if signal_path is not None and signal_path.exists():
        signal_rows: list[dict[str, Any]] = []
        for row in read_csv(signal_path):
            if row.get("run_id") != run_id:
                continue
            if config_hash is not None and row.get("config_hash") != config_hash:
                continue
            try:
                timestamp = parse_time(row["detected_at"])
            except (KeyError, ValueError):
                continue
            if not within(timestamp.timestamp(), start, end):
                continue
            try:
                filter_context = json.loads(row.get("filter_context") or "{}")
            except json.JSONDecodeError:
                filter_context = {}
            try:
                candidate_context = json.loads(row.get("candidate_context") or "{}")
            except json.JSONDecodeError:
                candidate_context = {}
            candidates = candidate_context.get("candidates") or []
            signal_rows.append(
                {
                    "timestamp": timestamp.timestamp(),
                    "symbol": row.get("symbol", ""),
                    "entry_enabled": filter_context.get("entry_enabled"),
                    "pool_size": filter_context.get("entry_symbol_pool_size"),
                    "candidate_count": len(candidates),
                }
            )
        signals = {
            "available": True,
            "rows": len(signal_rows),
            "entry_enabled_rows": sum(row["entry_enabled"] is True for row in signal_rows),
            "candidate_rows": sum(row["candidate_count"] > 0 for row in signal_rows),
            "pool_size_counts": dict(Counter(str(row["pool_size"]) for row in signal_rows)),
            "before_proxy_start": sum(row["timestamp"] < proxy_start.timestamp() for row in signal_rows),
            "after_proxy_start": sum(row["timestamp"] >= proxy_start.timestamp() for row in signal_rows),
            "keys": {
                (row["symbol"], round(row["timestamp"], 3))
                for row in signal_rows
                if row["candidate_count"] > 0
            },
        }

    return {
        "orders_created": {
            "entry": sum(
                start.timestamp() <= timestamp < end.timestamp()
                for timestamp in created_by_kind["entry"]
            ),
            "exit": sum(
                start.timestamp() <= timestamp < end.timestamp()
                for timestamp in created_by_kind["exit"]
            ),
        },
        "fills": summarize(fill_rows),
        "fills_before_proxy_start": summarize(pre_proxy),
        "fills_after_proxy_start": summarize(post_proxy),
        "signals": signals,
    }


def load_replay_activity(
    event_path: Path,
    pnl_path: Path,
    *,
    start: datetime,
    end: datetime,
    proxy_start: datetime,
) -> dict[str, Any]:
    events = read_csv(event_path)
    selected = [row for row in events if row.get("symbol")]
    closed = [
        row
        for row in selected
        if truthy(row.get("closed")) and row.get("exit_at")
    ]

    def event_stats(rows: list[dict[str, str]]) -> dict[str, Any]:
        filled = [row for row in rows if row.get("entry_at")]
        closed_rows = [row for row in rows if truthy(row.get("closed")) and row.get("exit_at")]
        return {
            "selected_rows": len(rows),
            "filled_entries": len(filled),
            "closed_entries": len(closed_rows),
            "net_pnl_usdt": round(sum(number(row.get("net_pnl_usdt")) for row in closed_rows), 8),
            "first_entry_at": min((row["entry_at"] for row in filled), default=None),
            "last_entry_at": max((row["entry_at"] for row in filled), default=None),
        }

    before_proxy = []
    after_proxy = []
    for row in selected:
        entry = row.get("entry_at")
        if not entry:
            continue
        try:
            entry_time = parse_time(entry)
        except ValueError:
            continue
        (before_proxy if entry_time < proxy_start else after_proxy).append(row)

    pnl_points = load_baseline_pnl_points(pnl_path)
    return {
        "events": event_stats(selected),
        "events_before_proxy_start": event_stats(before_proxy),
        "events_after_proxy_start": event_stats(after_proxy),
        "pnl_series_start_usdt": round(step_value(pnl_points, start.timestamp()), 8),
        "pnl_series_at_proxy_start_usdt": round(step_value(pnl_points, proxy_start.timestamp()), 8),
        "pnl_series_end_usdt": round(step_value(pnl_points, end.timestamp()), 8),
        "matched_signal_keys": None,
        "_signal_keys": {
            (row.get("symbol", ""), round(parse_time(row["detected_at"]).timestamp(), 3))
            for row in selected
            if row.get("detected_at")
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--optimization-report", type=Path, required=True)
    parser.add_argument("--equity-series", type=Path, required=True)
    parser.add_argument("--baseline-events", type=Path, required=True)
    parser.add_argument("--balance-snapshots", type=Path, required=True)
    parser.add_argument("--fill-events", type=Path, required=True)
    parser.add_argument("--exchange-orders", type=Path, required=True)
    parser.add_argument("--live-signals", type=Path)
    parser.add_argument("--run-id", default="live-b1-long-100u-5x-v1")
    parser.add_argument("--config-hash")
    parser.add_argument("--assert-baseline-under-live", action="store_true")
    args = parser.parse_args()

    optimization = json.loads(args.optimization_report.read_text(encoding="utf-8"))
    start = parse_time(optimization["data_start"])
    end = parse_time(optimization["data_end"])
    proxy_start = parse_time(optimization["optimization_window"]["start"])
    live_points = load_equity_points(args.balance_snapshots)
    live_start = interpolate(live_points, start.timestamp())
    live_proxy = interpolate(live_points, proxy_start.timestamp())
    live_end = interpolate(live_points, end.timestamp())
    replay = load_replay_activity(
        args.baseline_events,
        args.equity_series,
        start=start,
        end=end,
        proxy_start=proxy_start,
    )
    activity = load_actual_activity(
        args.fill_events,
        args.exchange_orders,
        args.live_signals,
        run_id=args.run_id,
        config_hash=args.config_hash,
        start=start,
        end=end,
        proxy_start=proxy_start,
    )
    signal_keys = activity["signals"].pop("keys", set())
    replay_keys = replay.pop("_signal_keys")
    replay["matched_signal_keys"] = len(replay_keys & signal_keys) if signal_keys else None
    replay["selected_signal_key_count"] = len(replay_keys)
    result = {
        "window": {
            "start_utc": start.isoformat(),
            "proxy_start_utc": proxy_start.isoformat(),
            "end_utc": end.isoformat(),
            "duration_hours": round((end - start).total_seconds() / 3600.0, 4),
        },
        "live_equity": {
            "start_usdt": round(live_start, 8),
            "at_proxy_start_usdt": round(live_proxy, 8),
            "end_usdt": round(live_end, 8),
            "full_change_usdt": round(live_end - live_start, 8),
            "before_proxy_change_usdt": round(live_proxy - live_start, 8),
            "after_proxy_change_usdt": round(live_end - live_proxy, 8),
        },
        "replay": replay,
        "actual_activity": activity,
        "replay_vs_live": {
            "baseline_replay_full_pnl_usdt": replay["pnl_series_end_usdt"],
            "live_equity_change_usdt": round(live_end - live_start, 8),
            "live_minus_replay_usdt": round(
                (live_end - live_start) - replay["pnl_series_end_usdt"],
                8,
            ),
            "replay_absolute_end_equity_usdt": round(
                live_start + replay["pnl_series_end_usdt"],
                8,
            ),
        },
        "interpretation": {
            "proxy_start_is_first_full_utc_day": proxy_start.date().isoformat(),
            "replay_before_proxy_is_zero_by_construction": replay["events_before_proxy_start"]["selected_rows"] == 0,
            "why_not_exact_live_replay": [
                "the local replay disables the Top10 proxy until the first complete UTC day",
                "the local export does not contain the historical exact live Top10 membership snapshots",
                "live fills/exits are real exchange events while replay fills/exits are local 15s/15m approximations",
            ],
        },
    }
    if args.assert_baseline_under_live and not (
        result["replay_vs_live"]["baseline_replay_full_pnl_usdt"]
        < result["replay_vs_live"]["live_equity_change_usdt"]
    ):
        raise SystemExit("baseline replay is not below the live equity change")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=list))


if __name__ == "__main__":
    main()
