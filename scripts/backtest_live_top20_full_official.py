#!/usr/bin/env python3
"""Replay full live-primary gainer-top20 long entries on official 15m OHLC.

The entry cohort is taken from actual ``live / primary`` BUY fills.  The
point-in-time gainer rank is matched to the latest activated universe snapshot
at or before each order's creation time.  Only the exit policy is
counterfactual:

* b0: close on the first eligible bearish 15m candle;
* b1/b8: if that candle is not already +0.10%, allow the next 1/8 candles to
  touch entry +0.88%, otherwise close at the final grace candle's close.

Official 15m highs are candle-level evidence of a recovery target touch.  The
exact intrabar fill time is not observable in this source, so recovery fills
are timestamped at the containing candle close for equity-curve ordering.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import gzip
import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from analyze_breakout_acceptance import format_time, parse_time
from backtest_live_exit_grace import LiveEntry, load_live_entries


BAR_SECONDS = 15 * 60
DIRECT_PROFIT_PCT = 0.001
RECOVERY_PROFIT_PCT = 0.0088


@dataclass(frozen=True, slots=True)
class OfficialCandle:
    symbol: str
    start: float
    end: float
    open: float
    high: float
    low: float
    close: float


def as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def load_candles(path: Path, symbols: set[str]) -> dict[str, list[OfficialCandle]]:
    grouped: dict[str, list[OfficialCandle]] = defaultdict(list)
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            symbol = (row.get("symbol") or "").strip().upper()
            if symbol not in symbols:
                continue
            values = [
                as_float(row.get(name))
                for name in ("open_price", "high_price", "low_price", "close_price")
            ]
            if any(value is None or value <= 0 for value in values):
                continue
            grouped[symbol].append(
                OfficialCandle(
                    symbol=symbol,
                    start=parse_time(row["candle_start"]),
                    end=parse_time(row["candle_end"]),
                    open=values[0] or 0.0,
                    high=values[1] or 0.0,
                    low=values[2] or 0.0,
                    close=values[3] or 0.0,
                )
            )
    for candles in grouped.values():
        candles.sort(key=lambda candle: candle.start)
    return grouped


def load_gainer_ranks(path: Path) -> dict[str, int | None]:
    ranks: dict[str, int | None] = {}
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            order_id = (row.get("exchange_order_id") or "").strip()
            if not order_id:
                continue
            rank_text = (row.get("gainer_rank") or "").strip()
            ranks[order_id] = int(rank_text) if rank_text else None
    return ranks


def mark_price(
    entry: LiveEntry,
    candles: list[OfficialCandle],
    data_end: float,
) -> tuple[float, str]:
    candidates = [
        candle
        for candle in candles
        if candle.end <= data_end + 1e-6 and candle.end > entry.entry_epoch
    ]
    if candidates:
        return candidates[-1].close, "official_15m_close"
    return entry.entry_price, "entry_price_no_post_entry_candle"


def replay(
    entry: LiveEntry,
    candles: list[OfficialCandle],
    *,
    grace_bars: int,
    fee_rate: float,
    data_end: float,
    direct_profit_pct: float = DIRECT_PROFIT_PCT,
    recovery_profit_pct: float = RECOVERY_PROFIT_PCT,
) -> dict[str, Any]:
    if not candles:
        return {
            "closed": False,
            "exit_epoch": None,
            "exit_price": None,
            "exit_reason": "missing_official_candles",
            "mark_price": entry.entry_price,
            "mark_source": "entry_price_no_candles",
        }

    first_eligible_start = math.floor(entry.entry_epoch / BAR_SECONDS) * BAR_SECONDS + BAR_SECONDS
    reverse: OfficialCandle | None = None
    for candle in candles:
        if candle.start < first_eligible_start:
            continue
        if candle.end > data_end + 1e-6:
            break
        if candle.close < candle.open:
            reverse = candle
            break

    if reverse is None:
        mark, mark_source = mark_price(entry, candles, data_end)
        return {
            "closed": False,
            "exit_epoch": None,
            "exit_price": None,
            "exit_reason": "no_reverse_candle_before_data_end",
            "mark_price": mark,
            "mark_source": mark_source,
        }

    if grace_bars == 0 or reverse.close >= entry.entry_price * (1.0 + direct_profit_pct):
        exit_price = reverse.close
        exit_fee = entry.quantity * exit_price * fee_rate
        gross = entry.quantity * (exit_price - entry.entry_price)
        return {
            "closed": True,
            "exit_epoch": reverse.end,
            "exit_price": exit_price,
            "exit_reason": "candle_15m_bearish",
            "exit_fee": exit_fee,
            "gross_pnl": gross,
            "net_pnl": gross - entry.entry_fee - exit_fee,
            "mark_price": exit_price,
            "mark_source": "exit_price",
        }

    deadline = reverse.end + BAR_SECONDS * grace_bars
    grace_candles = [
        candle
        for candle in candles
        if candle.start >= reverse.end - 1e-6
        and candle.end <= deadline + 1e-6
        and candle.end <= data_end + 1e-6
    ]
    if len(grace_candles) < grace_bars:
        mark, mark_source = mark_price(entry, candles, data_end)
        return {
            "closed": False,
            "exit_epoch": None,
            "exit_price": None,
            "exit_reason": f"insufficient_grace_data_{grace_bars}",
            "mark_price": mark,
            "mark_source": mark_source,
        }

    target_price = entry.entry_price * (1.0 + recovery_profit_pct)
    target_candle = next(
        (candle for candle in grace_candles if candle.high >= target_price),
        None,
    )
    if target_candle is not None:
        exit_epoch = target_candle.end
        exit_price = target_price
        exit_reason = f"candle_15m_bearish_grace_limit_{grace_bars}"
    else:
        exit_epoch = grace_candles[-1].end
        exit_price = grace_candles[-1].close
        exit_reason = f"candle_15m_grace_timeout_{grace_bars}"

    exit_fee = entry.quantity * exit_price * fee_rate
    gross = entry.quantity * (exit_price - entry.entry_price)
    return {
        "closed": True,
        "exit_epoch": exit_epoch,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "exit_fee": exit_fee,
        "gross_pnl": gross,
        "net_pnl": gross - entry.entry_fee - exit_fee,
        "mark_price": exit_price,
        "mark_source": "exit_price",
    }


def make_trade(
    entry: LiveEntry,
    ranks: dict[str, int | None],
    replayed: dict[str, Any],
    grace_bars: int,
) -> dict[str, Any]:
    closed = bool(replayed["closed"])
    mark_price = float(replayed["mark_price"])
    marked_gross = entry.quantity * (mark_price - entry.entry_price)
    return {
        "grace_bars": grace_bars,
        "order_id": entry.order_id,
        "symbol": entry.symbol,
        "gainer_rank": ranks.get(entry.order_id),
        "entry_epoch": entry.entry_epoch,
        "entry_at": format_time(entry.entry_epoch),
        "entry_price": entry.entry_price,
        "quantity": entry.quantity,
        "entry_notional_usdt": entry.entry_price * entry.quantity,
        "entry_fee_usdt": entry.entry_fee,
        "exit_epoch": replayed["exit_epoch"],
        "exit_at": format_time(replayed["exit_epoch"]),
        "exit_price": replayed["exit_price"],
        "exit_reason": replayed["exit_reason"],
        "closed": closed,
        "exit_fee_usdt": replayed.get("exit_fee", 0.0),
        "gross_pnl_usdt": replayed.get("gross_pnl"),
        "net_pnl_usdt": replayed.get("net_pnl"),
        "marked_price": mark_price,
        "mark_source": replayed["mark_source"],
        "marked_gross_pnl_usdt": marked_gross,
        "marked_net_pnl_usdt": marked_gross - entry.entry_fee,
        "holding_minutes": (
            (float(replayed["exit_epoch"]) - entry.entry_epoch) / 60.0
            if closed
            else None
        ),
    }


def build_curve(
    trades: list[dict[str, Any]],
    candles_by_symbol: dict[str, list[OfficialCandle]],
    *,
    initial_equity: float,
    leverage: float,
    data_end: float,
) -> list[dict[str, Any]]:
    if not trades:
        return []

    time_points: set[float] = {trade["entry_epoch"] for trade in trades}
    time_points.update(
        trade["exit_epoch"]
        for trade in trades
        if trade["closed"] and trade["exit_epoch"] is not None
    )
    for candles in candles_by_symbol.values():
        time_points.update(candle.end for candle in candles if candle.end <= data_end + 1e-6)
    ordered_times = sorted(time_points)
    entry_events: dict[float, list[dict[str, Any]]] = defaultdict(list)
    exit_events: dict[float, list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        entry_events[trade["entry_epoch"]].append(trade)
        if trade["closed"] and trade["exit_epoch"] is not None:
            exit_events[trade["exit_epoch"]].append(trade)

    ends_by_symbol = {
        symbol: [candle.end for candle in candles]
        for symbol, candles in candles_by_symbol.items()
    }
    closes_by_symbol = {
        symbol: [candle.close for candle in candles]
        for symbol, candles in candles_by_symbol.items()
    }
    active: dict[str, dict[str, Any]] = {}
    realized_net = 0.0
    curve: list[dict[str, Any]] = []
    for timestamp in ordered_times:
        for trade in exit_events.get(timestamp, ()):
            active.pop(trade["order_id"], None)
            realized_net += float(trade["gross_pnl_usdt"]) - float(trade["exit_fee_usdt"])
        for trade in entry_events.get(timestamp, ()):
            active[trade["order_id"]] = trade
            realized_net -= float(trade["entry_fee_usdt"])

        unrealized = 0.0
        margin = 0.0
        for trade in active.values():
            ends = ends_by_symbol.get(trade["symbol"], [])
            closes = closes_by_symbol.get(trade["symbol"], [])
            index = bisect.bisect_right(ends, timestamp) - 1
            mark = trade["entry_price"]
            if index >= 0 and ends[index] > trade["entry_epoch"]:
                mark = closes[index]
            unrealized += trade["quantity"] * (mark - trade["entry_price"])
            margin += trade["entry_notional_usdt"] / leverage
        curve.append(
            {
                "epoch": timestamp,
                "at": format_time(timestamp),
                "equity_usdt": initial_equity + realized_net + unrealized,
                "realized_net_pnl_usdt": realized_net,
                "unrealized_pnl_usdt": unrealized,
                "margin_occupied_usdt": margin,
                "open_positions": len(active),
            }
        )
    return curve


def max_drawdown(curve: list[dict[str, Any]]) -> tuple[float, float]:
    peak = float("-inf")
    max_abs = 0.0
    max_pct = 0.0
    for row in curve:
        equity = float(row["equity_usdt"])
        peak = max(peak, equity)
        drawdown = peak - equity
        max_abs = max(max_abs, drawdown)
        if peak > 0:
            max_pct = max(max_pct, drawdown / peak)
    return max_abs, max_pct


def summarize(
    trades: list[dict[str, Any]],
    curve: list[dict[str, Any]],
    *,
    initial_equity: float,
) -> dict[str, Any]:
    closed = [trade for trade in trades if trade["closed"]]
    open_trades = [trade for trade in trades if not trade["closed"]]
    values = [float(trade["net_pnl_usdt"]) for trade in closed]
    gains = sum(value for value in values if value > 0)
    losses = -sum(value for value in values if value < 0)
    dd_abs, dd_pct = max_drawdown(curve)
    closed_net = sum(values)
    open_marked = sum(float(trade["marked_net_pnl_usdt"]) for trade in open_trades)
    return {
        "n_entries": len(trades),
        "n_closed": len(closed),
        "n_open_at_data_end": len(open_trades),
        "closed_rate": len(closed) / len(trades) if trades else None,
        "total_net_pnl_usdt_closed": closed_net,
        "open_marked_net_pnl_usdt": open_marked,
        "marked_total_pnl_usdt": closed_net + open_marked,
        "final_equity_usdt_marked": initial_equity + closed_net + open_marked,
        "profit_factor_on_closed": gains / losses if losses > 0 else None,
        "win_rate_on_closed": (
            sum(value > 0 for value in values) / len(values) if values else None
        ),
        "max_drawdown_usdt": dd_abs,
        "max_drawdown_pct_of_equity": dd_pct,
        "exit_reason_counts": dict(
            sorted(Counter(trade["exit_reason"] for trade in closed).items())
        ),
        "mark_source_counts": dict(
            sorted(Counter(trade["mark_source"] for trade in open_trades).items())
        ),
    }


def write_trades(path: Path, trades_by_policy: dict[int, list[dict[str, Any]]]) -> None:
    fields = [
        "grace_bars",
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
        "mark_source",
        "marked_gross_pnl_usdt",
        "marked_net_pnl_usdt",
        "holding_minutes",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for trades in trades_by_policy.values():
            writer.writerows({field: trade.get(field) for field in fields} for trade in trades)


def write_curves(path: Path, curves: dict[int, list[dict[str, Any]]]) -> None:
    fields = [
        "grace_bars",
        "at",
        "epoch",
        "equity_usdt",
        "realized_net_pnl_usdt",
        "unrealized_pnl_usdt",
        "margin_occupied_usdt",
        "open_positions",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for grace_bars, curve in curves.items():
            for row in curve:
                writer.writerow(
                    {"grace_bars": grace_bars, **{field: row.get(field) for field in fields[1:]}}
                )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fills", type=Path, required=True)
    parser.add_argument("--orders", type=Path, required=True)
    parser.add_argument("--gainer-ranks", type=Path, required=True)
    parser.add_argument("--klines", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-id", default="live-b1-long-100u-5x-v1")
    parser.add_argument("--entry-cutoff", required=True)
    parser.add_argument("--rank-max", type=int, default=20)
    parser.add_argument("--initial-equity", type=float, default=1000.0)
    parser.add_argument("--leverage", type=float, default=5.0)
    parser.add_argument("--fee-rate", type=float, default=0.0005)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.rank_max < 1:
        raise SystemExit("--rank-max must be at least 1")
    entry_cutoff = parse_time(args.entry_cutoff)
    entries = [
        entry
        for entry in load_live_entries(args.fills, args.orders, run_id=args.run_id)
        if entry.entry_epoch <= entry_cutoff + 1e-6
    ]
    ranks = load_gainer_ranks(args.gainer_ranks)
    missing_rank_rows = [entry.order_id for entry in entries if entry.order_id not in ranks]
    selected_entries = [
        entry
        for entry in entries
        if 1 <= (ranks.get(entry.order_id) or 0) <= args.rank_max
    ]
    symbols = {entry.symbol for entry in selected_entries}
    candles_by_symbol = load_candles(args.klines, symbols)
    price_data_end = max(
        (candle.end for candles in candles_by_symbol.values() for candle in candles),
        default=entry_cutoff,
    )
    # The export cutoff can be later than the last completed official candle.
    # Keep those late entries in the cohort, but leave them open and mark them
    # at entry price when no post-entry candle is available.
    data_end = max(entry_cutoff, price_data_end)

    policies: dict[int, list[dict[str, Any]]] = {}
    curves: dict[int, list[dict[str, Any]]] = {}
    summaries: dict[str, dict[str, Any]] = {}
    for grace_bars in (0, 1, 8):
        trades = [
            make_trade(
                entry,
                ranks,
                replay(
                    entry,
                    candles_by_symbol.get(entry.symbol, []),
                    grace_bars=grace_bars,
                    fee_rate=args.fee_rate,
                    data_end=data_end,
                ),
                grace_bars,
            )
            for entry in selected_entries
        ]
        curve = build_curve(
            trades,
            candles_by_symbol,
            initial_equity=args.initial_equity,
            leverage=args.leverage,
            data_end=data_end,
        )
        policies[grace_bars] = trades
        curves[grace_bars] = curve
        summaries[str(grace_bars)] = summarize(
            trades,
            curve,
            initial_equity=args.initial_equity,
        )

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    write_trades(output_dir / f"live_top{args.rank_max}_full_trades.csv", policies)
    write_curves(output_dir / f"live_top{args.rank_max}_full_curves.csv", curves)
    report = {
        "run_id": args.run_id,
        "entry_cutoff_utc": format_time(entry_cutoff),
        "data_end_utc": format_time(data_end),
        "price_data_end_utc": format_time(price_data_end),
        "filter": {
            "entry_environment": "live",
            "account_label": "primary",
            "side": "BUY / long",
            "gainer_rank_inclusive": [1, args.rank_max],
            "rank_source": "latest activated universe snapshot at or before each exchange order creation time",
        },
        "entry_cohort": {
            "all_live_entries": len(entries),
            "selected_entries": len(selected_entries),
            "selected_symbols": len(symbols),
            "missing_rank_order_ids": len(missing_rank_rows),
            "first_entry_at_utc": format_time(entries[0].entry_epoch) if entries else None,
            "last_entry_at_utc": format_time(entries[-1].entry_epoch) if entries else None,
            "selected_entry_notional_usdt": sum(
                entry.entry_price * entry.quantity for entry in selected_entries
            ),
        },
        "data": {
            "fills_source": str(args.fills),
            "orders_source": str(args.orders),
            "rank_source": str(args.gainer_ranks),
            "klines_source": str(args.klines),
            "klines_symbols_loaded": len(candles_by_symbol),
            "klines_rows_loaded": sum(len(candles) for candles in candles_by_symbol.values()),
            "symbols_without_klines": sorted(symbols - candles_by_symbol.keys()),
        },
        "assumptions": {
            "initial_equity_usdt": args.initial_equity,
            "leverage": args.leverage,
            "fee_rate_each_side": args.fee_rate,
            "decision_profit_pct": DIRECT_PROFIT_PCT,
            "recovery_profit_pct": RECOVERY_PROFIT_PCT,
            "entry_policy": "actual live primary BUY fills held fixed",
            "direct_policy": "close at the first eligible completed bearish 15m candle close",
            "recovery_policy": "after a bearish candle below +0.10%, next N candles high touches +0.88%; otherwise close at Nth candle close",
            "target_touch": "official Binance 15m high; exact intrabar fill time is not observable",
            "open_policy": "entries without enough completed candles at data_end remain open and are marked, never forced closed",
        },
        "policies": summaries,
    }
    (output_dir / f"live_top{args.rank_max}_full_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
