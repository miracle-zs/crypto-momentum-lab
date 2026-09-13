#!/usr/bin/env python3
"""Backtest paper-account-05 gainer-long entries with a candle-grace exit.

The position snapshot contains the point-in-time rank label, so this script
keeps only ``side=long`` and ``rank_side=gainer`` positions in the acc10
comparison window.  Binance's official 15m OHLC feed supplies the missing
historical price path:

* the first completed bearish 15m candle after entry is the reversal;
* if its close is already at least +0.10% from entry, close immediately;
* otherwise allow the configured number of following completed 15m candles to
  touch entry +0.88%;
* if no high touches the target, close at the final grace candle's close.

The OHLC high is only candle-level evidence of a target touch.  It does not
claim the exact 15-second bid/ask fill timing, so the report calls those fills
``official_15m_high_touch`` rather than presenting them as order-book fills.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import gzip
import json
import math
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


DIRECT_PROFIT_PCT = 0.001
DEFAULT_FEE_RATE = 0.0004
BAR = timedelta(minutes=15)


def parse_dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


@dataclass(frozen=True, slots=True)
class Position:
    position_id: str
    symbol: str
    opened_at: datetime
    closed_at: datetime
    entry_price: float
    quantity: float
    entry_fee: float
    realized_pnl: float
    rank: int | None


@dataclass(frozen=True, slots=True)
class Candle:
    symbol: str
    start: datetime
    end: datetime
    open: float
    high: float
    low: float
    close: float


def load_positions(
    path: Path,
    *,
    start: datetime,
    end: datetime,
    rank_side: str,
) -> list[Position]:
    positions: list[Position] = []
    with path.open(encoding="utf-8", newline="") as handle:
        for fields in csv.reader(handle, delimiter="|"):
            if len(fields) < 20:
                continue
            # The ranked TSV is intentionally headerless.  Keep the field
            # positions explicit so an accidental header cannot be treated as
            # a trade.
            if fields[2] != "long":
                continue
            if rank_side == "gainer" and fields[15] != "gainer":
                continue
            if fields[3] != "closed" or not fields[5]:
                continue
            opened_at = parse_dt(fields[4])
            closed_at = parse_dt(fields[5])
            if opened_at < start or closed_at > end:
                continue
            rank_text = fields[16].strip()
            rank = int(rank_text) if rank_text else None
            positions.append(
                Position(
                    position_id=fields[0],
                    symbol=fields[1],
                    opened_at=opened_at,
                    closed_at=closed_at,
                    entry_price=float(fields[6]),
                    quantity=float(fields[8]),
                    entry_fee=float(fields[10]),
                    realized_pnl=float(fields[12]),
                    rank=rank,
                )
            )
    return sorted(positions, key=lambda row: (row.opened_at, row.position_id))


def load_candles(path: Path, symbols: set[str]) -> dict[str, list[Candle]]:
    grouped: dict[str, list[Candle]] = defaultdict(list)
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
            start = parse_dt(row["candle_start"])
            end = parse_dt(row["candle_end"])
            grouped[symbol].append(
                Candle(
                    symbol=symbol,
                    start=start,
                    end=end,
                    open=values[0] or 0.0,
                    high=values[1] or 0.0,
                    low=values[2] or 0.0,
                    close=values[3] or 0.0,
                )
            )
    for candles in grouped.values():
        candles.sort(key=lambda candle: candle.end)
    return grouped


def close_pnl(position: Position, exit_price: float, fee_rate: float) -> tuple[float, float, float]:
    gross = (exit_price - position.entry_price) * position.quantity
    exit_fee = abs(exit_price * position.quantity) * fee_rate
    return gross - position.entry_fee - exit_fee, gross, exit_fee


def replay(
    position: Position,
    candles: list[Candle],
    fee_rate: float,
    recovery_profit_pct: float,
    grace_bars: int,
) -> dict[str, Any]:
    # The candle containing the entry is observation-only in production.  The
    # first eligible candle is therefore the next 15m bucket, even when the
    # entry happened before the current bucket closed.
    first_eligible_epoch = (
        math.floor(position.opened_at.timestamp() / 900.0) + 1
    ) * 900.0
    first_eligible_start = datetime.fromtimestamp(first_eligible_epoch, UTC)
    ends = [candle.end for candle in candles]
    reverse: Candle | None = None
    for candle in candles:
        if candle.start < first_eligible_start:
            continue
        if candle.close < candle.open:
            reverse = candle
            break
    if reverse is None:
        return {
            "status": "no_reverse_candle",
            "pnl": None,
            "closed_at": None,
            "exit_price": None,
            "fill_kind": None,
            "reverse_candle_end": None,
            "deadline": None,
        }

    target_price = position.entry_price * (1 + recovery_profit_pct)
    direct_price = position.entry_price * (1 + DIRECT_PROFIT_PCT)
    if grace_bars == 0 or reverse.close >= direct_price:
        pnl, gross, exit_fee = close_pnl(position, reverse.close, fee_rate)
        return {
            "status": "closed",
            "pnl": pnl,
            "gross_pnl": gross,
            "exit_fee": exit_fee,
            "closed_at": reverse.end,
            "exit_price": reverse.close,
            "target_price": target_price,
            "fill_kind": (
                "direct_bearish_official_close"
                if grace_bars == 0
                else "immediate_marketable_official_close"
            ),
            "reverse_candle_end": reverse.end,
            "deadline": reverse.end,
        }

    deadline = reverse.end + BAR * grace_bars
    deadline_index = bisect.bisect_left(ends, deadline)
    if deadline_index >= len(candles) or candles[deadline_index].end != deadline:
        return {
            "status": "no_timeout_candle",
            "pnl": None,
            "closed_at": None,
            "exit_price": None,
            "fill_kind": None,
            "target_price": target_price,
            "reverse_candle_end": reverse.end,
            "deadline": deadline,
        }

    grace_candles = [
        candle
        for candle in candles
        if reverse.end < candle.end <= deadline
    ]
    if any(candle.high >= target_price for candle in grace_candles):
        exit_price = target_price
        fill_kind = "official_15m_high_touch"
    else:
        exit_price = candles[deadline_index].close
        fill_kind = "grace_timeout_official_close"
    pnl, gross, exit_fee = close_pnl(position, exit_price, fee_rate)
    return {
        "status": "closed",
        "pnl": pnl,
        "gross_pnl": gross,
        "exit_fee": exit_fee,
        "closed_at": deadline,
        "exit_price": exit_price,
        "target_price": target_price,
        "fill_kind": fill_kind,
        "reverse_candle_end": reverse.end,
        "deadline": deadline,
    }


def profit_factor(values: list[float]) -> float | None:
    gains = sum(value for value in values if value > 0)
    losses = -sum(value for value in values if value < 0)
    if losses == 0:
        return None if gains == 0 else float("inf")
    return gains / losses


def max_drawdown(events: list[tuple[datetime, float]]) -> float:
    by_time: dict[datetime, float] = defaultdict(float)
    for at, pnl in events:
        by_time[at] += pnl
    cumulative = 0.0
    peak = 0.0
    drawdown = 0.0
    for at in sorted(by_time):
        pnl = by_time[at]
        cumulative += pnl
        peak = max(peak, cumulative)
        drawdown = min(drawdown, cumulative - peak)
    return drawdown


def metrics(rows: list[dict[str, Any]], pnl_key: str, time_key: str) -> dict[str, Any]:
    closed = [
        row
        for row in rows
        if row.get(pnl_key) is not None and row.get(time_key) is not None
    ]
    values = [float(row[pnl_key]) for row in closed]
    wins = [value for value in values if value > 0]
    losses = [value for value in values if value < 0]
    return {
        "trades": len(closed),
        "net_pnl_usdt": sum(values),
        "profit_factor": profit_factor(values),
        "win_rate": len(wins) / len(values) if values else None,
        "expectancy_usdt": statistics.mean(values) if values else None,
        "median_pnl_usdt": statistics.median(values) if values else None,
        "average_win_usdt": statistics.mean(wins) if wins else None,
        "average_loss_usdt": statistics.mean(losses) if losses else None,
        "max_drawdown_usdt": max_drawdown(
            [(parse_dt(str(row[time_key])), float(row[pnl_key])) for row in closed]
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--positions-tsv", type=Path, required=True)
    parser.add_argument("--klines", type=Path, required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--fee-rate", type=float, default=DEFAULT_FEE_RATE)
    parser.add_argument("--recovery-profit-pct", type=float, default=0.0088)
    parser.add_argument("--grace-bars", type=int, default=1)
    parser.add_argument("--rank-side", choices=("gainer", "all"), default="gainer")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.grace_bars < 0:
        raise SystemExit("--grace-bars must not be negative")
    start = parse_dt(args.start)
    end = parse_dt(args.end)
    positions = load_positions(
        args.positions_tsv,
        start=start,
        end=end,
        rank_side=args.rank_side,
    )
    candles = load_candles(args.klines, {position.symbol for position in positions})

    rows: list[dict[str, Any]] = []
    for position in positions:
        result = replay(
            position,
            candles.get(position.symbol, []),
            args.fee_rate,
            args.recovery_profit_pct,
            args.grace_bars,
        )
        rows.append(
            {
                "position_id": position.position_id,
                "symbol": position.symbol,
                "rank": position.rank,
                "entry_at": position.opened_at.isoformat(),
                "entry_price": position.entry_price,
                "quantity": position.quantity,
                "entry_fee": position.entry_fee,
                "actual_closed_at": position.closed_at.isoformat(),
                "actual_pnl": position.realized_pnl,
                **{
                    key: value.isoformat() if isinstance(value, datetime) else value
                    for key, value in result.items()
                },
            }
        )

    replay_rows = [row for row in rows if row["pnl"] is not None]
    pair_rows = [row for row in replay_rows if row["actual_pnl"] is not None]
    delta_values = [float(row["pnl"]) - float(row["actual_pnl"]) for row in pair_rows]
    report = {
        "strategy": f"gainer_long_only_b{args.grace_bars}_recovery_target",
        "window": {
            "entry_and_actual_exit_filter_start_utc": start.isoformat(),
            "entry_and_actual_exit_filter_end_utc": end.isoformat(),
        },
        "entry_cohort": {
            "definition": (
                "paper-account-05 snapshot: side=long, rank_side=gainer; "
                "actual closed_at also inside the comparison window"
                if args.rank_side == "gainer"
                else "paper-account-05 snapshot: all side=long positions; actual closed_at also inside the comparison window"
            ),
            "rank_filter": args.rank_side,
            "positions": len(positions),
            "symbols": len({position.symbol for position in positions}),
            "first_entry_at_utc": positions[0].opened_at.isoformat() if positions else None,
            "last_entry_at_utc": positions[-1].opened_at.isoformat() if positions else None,
            "entry_notional_usdt": sum(position.entry_price * position.quantity for position in positions),
            "rank_min": min((position.rank for position in positions if position.rank is not None), default=None),
            "rank_max": max((position.rank for position in positions if position.rank is not None), default=None),
        },
        "data": {
            "klines_source": str(args.klines),
            "klines_symbols_loaded": len(candles),
            "klines_rows_loaded": sum(len(values) for values in candles.values()),
            "klines_first_at_utc": min(
                (candle.start for values in candles.values() for candle in values),
                default=None,
            ).isoformat()
            if candles
            else None,
            "klines_last_at_utc": max(
                (candle.end for values in candles.values() for candle in values),
                default=None,
            ).isoformat()
            if candles
            else None,
        },
        "assumptions": {
            "fee_rate_each_side": args.fee_rate,
            "decision_profit_pct": DIRECT_PROFIT_PCT,
            "recovery_profit_pct": args.recovery_profit_pct,
            "reversal": "first completed bearish official 15m candle after the entry candle",
            "grace": (
                "none; direct close on the first eligible bearish candle"
                if args.grace_bars == 0
                else f"the next {args.grace_bars} completed 15m candles"
            ),
            "target_touch": "official 15m high >= entry * (1 + recovery_profit_pct); exact intrabar bid timing is not observable here",
            "timeout": "official close of the one grace candle",
        },
        "coverage": {
            "replay_closed": len(replay_rows),
            "unresolved": len(rows) - len(replay_rows),
            "status_counts": dict(Counter(row["status"] for row in rows)),
            "symbols_without_klines": sorted(
                {row["symbol"] for row in rows if row["symbol"] not in candles}
            ),
        },
        "baseline_actual_b0": metrics(rows, "actual_pnl", "actual_closed_at"),
        f"counterfactual_b{args.grace_bars}_recovery_target": metrics(
            rows, "pnl", "closed_at"
        ),
        "paired_common_rows": {
            "trades": len(pair_rows),
            "net_pnl_delta_vs_actual_usdt": sum(delta_values),
            "mean_delta_usdt": statistics.mean(delta_values) if delta_values else None,
            "median_delta_usdt": statistics.median(delta_values) if delta_values else None,
            "counterfactual_better_rate": (
                sum(value > 0 for value in delta_values) / len(delta_values)
                if delta_values
                else None
            ),
        },
    }

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    trade_path = output_dir / "trades.csv"
    if rows:
        fields = list(rows[0])
        with trade_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
    (output_dir / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
