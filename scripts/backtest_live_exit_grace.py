#!/usr/bin/env python3
"""Counterfactual live-entry replay for 1 vs 8 candle grace bars.

The entry cohort is taken from actual primary live fills.  Only the live
``candle_15m`` grace period changes; all entries, entry prices, quantities, and
entry fees stay fixed.  Equity is marked to the exported 15s close path and
capital occupation is initial notional divided by the run's 5x leverage.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from analyze_breakout_acceptance import (
    SameExitConfig,
    Series,
    closed_candles,
    format_time,
    load_states,
    parse_time,
    simulate_same_exit,
)


@dataclass(frozen=True, slots=True)
class LiveEntry:
    entry_id: str
    order_id: str
    symbol: str
    entry_epoch: float
    entry_price: float
    quantity: float
    entry_fee: float


def as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_bool(value: Any) -> bool:
    return str(value).lower() in {"1", "true", "t", "yes"}


def load_live_entries(
    fills_path: Path,
    orders_path: Path,
    *,
    run_id: str | None,
) -> list[LiveEntry]:
    entry_orders: dict[str, dict[str, str]] = {}
    with gzip.open(orders_path, "rt", newline="") as handle:
        for row in csv.DictReader(handle):
            if run_id is not None and row.get("run_id") != run_id:
                continue
            if row.get("state") != "filled":
                continue
            if row.get("side") != "BUY" or as_bool(row.get("reduce_only")):
                continue
            order_id = row.get("exchange_order_id")
            if not order_id:
                continue
            entry_orders[order_id] = {
                "symbol": row.get("symbol") or "",
                "run_id": row.get("run_id") or "",
            }

    fills_by_order: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with gzip.open(fills_path, "rt", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("environment") != "live" or row.get("account_label") != "primary":
                continue
            order_id = row.get("order_id")
            if not order_id or order_id not in entry_orders:
                continue
            if row.get("side") != "BUY":
                continue
            fills_by_order[order_id].append(
                {
                    "symbol": row.get("symbol") or entry_orders[order_id]["symbol"],
                    "price": as_float(row.get("price")) or 0.0,
                    "quantity": as_float(row.get("quantity")) or 0.0,
                    "fee": as_float(row.get("fee")) or 0.0,
                    "trade_at": parse_time(row["trade_at"]),
                }
            )

    entries: list[LiveEntry] = []
    for order_id, fills in fills_by_order.items():
        quantity = sum(fill["quantity"] for fill in fills)
        notional = sum(fill["price"] * fill["quantity"] for fill in fills)
        if quantity <= 0 or notional <= 0:
            continue
        first = min(fills, key=lambda fill: fill["trade_at"])
        entries.append(
            LiveEntry(
                entry_id=f"entry-{order_id}",
                order_id=order_id,
                symbol=first["symbol"],
                entry_epoch=first["trade_at"],
                entry_price=notional / quantity,
                quantity=quantity,
                entry_fee=sum(fill["fee"] for fill in fills),
            )
        )
    return sorted(entries, key=lambda entry: entry.entry_epoch)


def simulate_policy(
    entries: list[LiveEntry],
    states: dict[str, Series],
    *,
    grace_bars: int,
    fee_rate: float,
) -> list[dict[str, Any]]:
    config = SameExitConfig(
        grace_bars=grace_bars,
        decision_profit_pct=0.001,
        recovery_profit_pct=0.0088,
        fee_rate=fee_rate,
        notional_usdt=100.0,
    )
    candle_cache = {
        symbol: closed_candles(states[symbol])
        for symbol in {entry.symbol for entry in entries}
        if symbol in states
    }
    trades: list[dict[str, Any]] = []
    for entry in entries:
        event = {
            "scenario": f"grace_{grace_bars}",
            "label": f"实盘入场，宽限{grace_bars}根",
            "entry_mode": "actual_fill",
            "signal_id": entry.entry_id,
            "symbol": entry.symbol,
            "entry_epoch": entry.entry_epoch,
            "entry_price": entry.entry_price,
        }
        series = states.get(entry.symbol)
        candles = candle_cache.get(entry.symbol)
        if series is None or candles is None:
            replay = {
                "closed": False,
                "exit_epoch": None,
                "exit_price": None,
                "exit_reason": "missing_states",
                "marked_price": None,
            }
        else:
            replay = simulate_same_exit(event, series, candles, config)

        exit_epoch = replay.get("exit_epoch")
        exit_price = replay.get("exit_price")
        closed = bool(replay.get("closed"))
        exit_fee = (
            entry.quantity * float(exit_price) * fee_rate
            if closed and exit_price is not None
            else 0.0
        )
        gross_pnl = (
            entry.quantity * (float(exit_price) - entry.entry_price)
            if closed and exit_price is not None
            else None
        )
        net_pnl = (
            float(gross_pnl) - entry.entry_fee - exit_fee
            if gross_pnl is not None
            else None
        )
        mark_price = replay.get("marked_price")
        if mark_price is None and series is not None and series.rows:
            mark_price = series.rows[-1][3]
        marked_gross_pnl = (
            entry.quantity * (float(mark_price) - entry.entry_price)
            if mark_price is not None
            else None
        )
        trades.append(
            {
                "grace_bars": grace_bars,
                "entry_id": entry.entry_id,
                "order_id": entry.order_id,
                "symbol": entry.symbol,
                "entry_epoch": entry.entry_epoch,
                "entry_at": format_time(entry.entry_epoch),
                "entry_price": entry.entry_price,
                "quantity": entry.quantity,
                "entry_notional_usdt": entry.quantity * entry.entry_price,
                "entry_fee_usdt": entry.entry_fee,
                "exit_epoch": exit_epoch,
                "exit_at": format_time(exit_epoch),
                "exit_price": exit_price,
                "exit_reason": replay.get("exit_reason"),
                "closed": closed,
                "exit_fee_usdt": exit_fee,
                "gross_pnl_usdt": gross_pnl,
                "net_pnl_usdt": net_pnl,
                "marked_price": mark_price,
                "marked_gross_pnl_usdt": marked_gross_pnl,
                "marked_net_pnl_usdt": (
                    float(marked_gross_pnl) - entry.entry_fee
                    if marked_gross_pnl is not None
                    else None
                ),
                "holding_minutes": (
                    (float(exit_epoch) - entry.entry_epoch) / 60.0
                    if closed and exit_epoch is not None
                    else None
                ),
            }
        )
    return trades


def overlap_count(trades: list[dict[str, Any]]) -> int:
    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        by_symbol[trade["symbol"]].append(trade)
    count = 0
    for symbol_trades in by_symbol.values():
        ordered = sorted(symbol_trades, key=lambda trade: trade["entry_epoch"])
        active_until: list[float | None] = []
        for trade in ordered:
            active_until = [
                value
                for value in active_until
                if value is None or value > trade["entry_epoch"]
            ]
            if active_until:
                count += 1
            active_until.append(trade["exit_epoch"])
    return count


def build_curve(
    trades: list[dict[str, Any]],
    states: dict[str, Series],
    *,
    initial_equity: float,
    leverage: float,
) -> list[dict[str, Any]]:
    if not trades:
        return []
    symbols = {trade["symbol"] for trade in trades}
    time_points: set[float] = {
        trade["entry_epoch"] for trade in trades
    }
    time_points.update(
        trade["exit_epoch"]
        for trade in trades
        if trade["exit_epoch"] is not None
    )
    for symbol in symbols:
        series = states.get(symbol)
        if series is not None:
            time_points.update(row[0] for row in series.rows)
    ordered_times = sorted(time_points)
    entry_events: dict[float, list[dict[str, Any]]] = defaultdict(list)
    exit_events: dict[float, list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        entry_events[trade["entry_epoch"]].append(trade)
        if trade["closed"] and trade["exit_epoch"] is not None:
            exit_events[trade["exit_epoch"]].append(trade)

    rows_by_symbol = {
        symbol: states[symbol].rows for symbol in symbols if symbol in states
    }
    index_by_symbol = {symbol: -1 for symbol in rows_by_symbol}
    mark_by_symbol: dict[str, float] = {}
    active: dict[str, dict[str, Any]] = {}
    realized_net = 0.0
    curve: list[dict[str, Any]] = []
    for timestamp in ordered_times:
        for symbol, rows in rows_by_symbol.items():
            index = index_by_symbol[symbol]
            while index + 1 < len(rows) and rows[index + 1][0] <= timestamp:
                index += 1
            index_by_symbol[symbol] = index
            if index >= 0:
                mark_by_symbol[symbol] = rows[index][3]

        for trade in exit_events.get(timestamp, ()):
            active.pop(trade["entry_id"], None)
            realized_net += (
                float(trade["gross_pnl_usdt"])
                - float(trade["exit_fee_usdt"])
            )
        for trade in entry_events.get(timestamp, ()):
            active[trade["entry_id"]] = trade
            realized_net -= float(trade["entry_fee_usdt"])

        unrealized = 0.0
        margin = 0.0
        gross_notional = 0.0
        for trade in active.values():
            mark = mark_by_symbol.get(trade["symbol"], trade["entry_price"])
            unrealized += trade["quantity"] * (mark - trade["entry_price"])
            margin += trade["entry_notional_usdt"] / leverage
            gross_notional += trade["quantity"] * mark
        curve.append(
            {
                "epoch": timestamp,
                "at": format_time(timestamp),
                "equity_usdt": initial_equity + realized_net + unrealized,
                "realized_net_pnl_usdt": realized_net,
                "unrealized_pnl_usdt": unrealized,
                "margin_occupied_usdt": margin,
                "gross_notional_occupied_usdt": gross_notional,
                "open_positions": len(active),
            }
        )
    return curve


def max_drawdown(values: list[float]) -> tuple[float, float]:
    peak = float("-inf")
    max_abs = 0.0
    max_pct = 0.0
    for value in values:
        peak = max(peak, value)
        drawdown = peak - value
        max_abs = max(max_abs, drawdown)
        if peak > 0:
            max_pct = max(max_pct, drawdown / peak)
    return max_abs, max_pct


def time_weighted_average(
    curve: list[dict[str, Any]], field: str
) -> float:
    if len(curve) < 2:
        return float(curve[0][field]) if curve else 0.0
    total = 0.0
    duration = 0.0
    for previous, current in zip(curve, curve[1:], strict=False):
        dt = current["epoch"] - previous["epoch"]
        if dt <= 0:
            continue
        total += (previous[field] + current[field]) * 0.5 * dt
        duration += dt
    return total / duration if duration else 0.0


def summarize_policy(
    trades: list[dict[str, Any]],
    curve: list[dict[str, Any]],
    *,
    initial_equity: float,
    leverage: float,
) -> dict[str, Any]:
    closed = [trade for trade in trades if trade["closed"]]
    open_trades = [trade for trade in trades if not trade["closed"]]
    net_returns = [
        trade["net_pnl_usdt"] / trade["entry_notional_usdt"] * 100.0
        for trade in closed
        if trade["entry_notional_usdt"] > 0
    ]
    equities = [row["equity_usdt"] for row in curve]
    dd_abs, dd_pct = max_drawdown(equities)
    final = curve[-1] if curve else {}
    final_open_marked = sum(
        trade["marked_net_pnl_usdt"] or 0.0 for trade in open_trades
    )
    winning_net = sum(
        max(float(trade["net_pnl_usdt"] or 0.0), 0.0) for trade in closed
    )
    losing_net = sum(
        max(-float(trade["net_pnl_usdt"] or 0.0), 0.0) for trade in closed
    )
    return {
        "grace_bars": trades[0]["grace_bars"] if trades else None,
        "initial_equity_usdt": initial_equity,
        "leverage": leverage,
        "n_entries": len(trades),
        "n_closed": len(closed),
        "n_open_at_data_end": len(open_trades),
        "closed_rate": len(closed) / len(trades) if trades else None,
        "avg_net_return_pct_on_closed": (
            sum(net_returns) / len(net_returns) if net_returns else None
        ),
        "median_net_return_pct_on_closed": (
            sorted(net_returns)[len(net_returns) // 2]
            if net_returns
            else None
        ),
        "win_rate_on_closed": (
            sum(trade["net_pnl_usdt"] > 0 for trade in closed) / len(closed)
            if closed
            else None
        ),
        "profit_factor_on_closed": (
            winning_net / losing_net if losing_net > 0 else None
        ),
        "total_gross_pnl_usdt_closed": sum(
            trade["gross_pnl_usdt"] or 0.0 for trade in closed
        ),
        "total_fees_usdt": sum(
            trade["entry_fee_usdt"] + trade["exit_fee_usdt"]
            for trade in trades
        ),
        "total_net_pnl_usdt_closed": sum(
            trade["net_pnl_usdt"] or 0.0 for trade in closed
        ),
        "open_marked_net_pnl_usdt": final_open_marked,
        "final_equity_usdt_marked": final.get("equity_usdt"),
        "marked_total_pnl_usdt": (
            final.get("equity_usdt", initial_equity) - initial_equity
            if final
            else None
        ),
        "max_drawdown_usdt": dd_abs,
        "max_drawdown_pct_of_equity": dd_pct,
        "max_margin_occupied_usdt": max(
            (row["margin_occupied_usdt"] for row in curve), default=0.0
        ),
        "avg_margin_occupied_usdt_time_weighted": time_weighted_average(
            curve, "margin_occupied_usdt"
        ),
        "max_gross_notional_occupied_usdt": max(
            (row["gross_notional_occupied_usdt"] for row in curve), default=0.0
        ),
        "avg_open_positions_time_weighted": time_weighted_average(
            curve, "open_positions"
        ),
        "max_open_positions": max(
            (row["open_positions"] for row in curve), default=0
        ),
        "same_symbol_overlap_entries_under_counterfactual": overlap_count(trades),
        "exit_reason_counts": dict(
            sorted(Counter(trade["exit_reason"] for trade in closed).items())
        ),
    }


def downsample_curve(curve: list[dict[str, Any]], interval_seconds: int = 300) -> list[dict[str, Any]]:
    if not curve:
        return []
    selected: list[dict[str, Any]] = []
    bucket = None
    for row in curve:
        current_bucket = math.floor(row["epoch"] / interval_seconds)
        if bucket is None or current_bucket != bucket:
            selected.append(row)
            bucket = current_bucket
        else:
            selected[-1] = row
    return selected


def write_trade_csv(path: Path, all_trades: dict[int, list[dict[str, Any]]]) -> None:
    fields = [
        "grace_bars",
        "entry_id",
        "order_id",
        "symbol",
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
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for trades in all_trades.values():
            for trade in trades:
                writer.writerow({field: trade.get(field) for field in fields})


def write_curve_csv(path: Path, curves: dict[int, list[dict[str, Any]]]) -> None:
    fields = [
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
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for grace, curve in curves.items():
            for row in curve:
                writer.writerow(
                    {"grace_bars": grace, **{field: row.get(field) for field in fields[1:]}}
                )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fills", type=Path, required=True)
    parser.add_argument("--orders", type=Path, required=True)
    parser.add_argument("--states", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-id", default="live-b1-long-100u-5x-v1")
    parser.add_argument("--initial-equity", type=float, default=1000.0)
    parser.add_argument("--leverage", type=float, default=5.0)
    parser.add_argument("--fee-rate", type=float, default=0.0005)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    entries = load_live_entries(
        args.fills,
        args.orders,
        run_id=args.run_id or None,
    )
    states = load_states(args.states)
    all_trades: dict[int, list[dict[str, Any]]] = {}
    curves: dict[int, list[dict[str, Any]]] = {}
    summaries: dict[str, dict[str, Any]] = {}
    for grace_bars in (0, 1, 8):
        trades = simulate_policy(
            entries,
            states,
            grace_bars=grace_bars,
            fee_rate=args.fee_rate,
        )
        curve = build_curve(
            trades,
            states,
            initial_equity=args.initial_equity,
            leverage=args.leverage,
        )
        all_trades[grace_bars] = trades
        curves[grace_bars] = curve
        summaries[str(grace_bars)] = summarize_policy(
            trades,
            curve,
            initial_equity=args.initial_equity,
            leverage=args.leverage,
        )

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    write_trade_csv(output_dir / "live_exit_grace_trades.csv", all_trades)
    write_curve_csv(output_dir / "live_exit_grace_curves.csv", curves)
    sampled_curves = {
        str(grace): downsample_curve(curve) for grace, curve in curves.items()
    }
    first_entry = entries[0].entry_epoch if entries else None
    last_entry = entries[-1].entry_epoch if entries else None
    report = {
        "run_id": args.run_id,
        "entry_cohort": {
            "n_entries": len(entries),
            "first_entry_at": format_time(first_entry),
            "last_entry_at": format_time(last_entry),
            "symbols": len({entry.symbol for entry in entries}),
            "total_entry_notional_usdt": sum(
                entry.quantity * entry.entry_price for entry in entries
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
            "entry_policy": "actual primary live fills held fixed; no counterfactual entry filtering",
        },
        "policies": summaries,
        "visual_curve_interval_seconds": 300,
        "visual_curves": sampled_curves,
    }
    (output_dir / "live_exit_grace_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
