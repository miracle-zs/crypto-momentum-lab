"""Replay the orderflow 15m exit against one- through four-, eight-,
sixteen-, and ninety-six-bar grace exits.

This is deliberately a paired replay.  It keeps the entries from the server's
``paper-account-05-orderflow-candle15m-v1`` snapshot (B0), uses the server's
realized PnL for B0, and simulates B1/B2/B3/B4/B8/B16 from the same entries:

* first adverse completed 15m candle is the warning;
* a reduce-only limit is placed at the entry price, or at a favorable offset
  when ``--favorable-exit-pct`` is supplied;
* a quote touching that limit in the following 15 minutes fills B1;
* otherwise the remaining position is closed at an executable quote exactly at
  the timeout close (B1), the second following close (B2), the third following
  close (B3), the fourth following close (B4), the eighth following close
  (B8), the sixteenth following close (B16), or the ninety-sixth following
  close (B96).

The local runtime-state export is used for executable bid/ask touches.  At the
timeout, only a quote stamped exactly at that deadline is eligible for the
market exit.  When that quote is missing, the official 15m close at the
deadline is used as an explicitly marked fallback.  If the exact deadline
candle is missing too, the timeout remains unresolved instead of borrowing a
later candle.  This keeps a data gap from silently extending the holding period
into a later price move.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import gzip
import json
import math
import statistics
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

TARGET_RUN = "paper-account-05-orderflow-candle15m-v1"
DEFAULT_FEE_RATE = 0.0004
GRACE_BARS = (1, 2, 3, 4, 8, 16, 96)
GRACE_VARIANTS = tuple(f"b{bars}" for bars in GRACE_BARS)


def parse_dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
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
class Candle:
    symbol: str
    start: datetime
    end: datetime
    open: float
    high: float
    low: float
    close: float


@dataclass(frozen=True, slots=True)
class Quote:
    at: datetime
    bid: float | None
    ask: float | None


def read_csv(path: Path) -> Iterable[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        yield from csv.DictReader(handle)


def load_positions(path: Path) -> list[dict[str, Any]]:
    positions: list[dict[str, Any]] = []
    for row in read_csv(path):
        if row.get("run_id") != TARGET_RUN:
            continue
        opened_at = parse_dt(row["opened_at"])
        closed_at = parse_dt(row["closed_at"]) if row.get("closed_at") else None
        positions.append(
            {
                "position_id": row["position_id"],
                "symbol": row["symbol"],
                "side": row["side"],
                "opened_at": opened_at,
                "closed_at": closed_at,
                "entry_price": as_float(row.get("entry_price")) or 0.0,
                "quantity": as_float(row.get("quantity")) or 0.0,
                "entry_fee": as_float(row.get("entry_fee")) or 0.0,
                "b0_pnl": as_float(row.get("realized_pnl")),
                "b0_close_reason": row.get("close_reason") or "",
            }
        )
    return sorted(positions, key=lambda row: (row["opened_at"], row["position_id"]))


def load_entry_spreads_and_fee(
    path: Path,
) -> tuple[dict[str, float], float]:
    spreads: dict[str, list[float]] = defaultdict(list)
    fee_rates: list[float] = []
    for row in read_csv(path):
        if row.get("run_id") != TARGET_RUN:
            continue
        status = row.get("status")
        if status not in (None, "", "filled"):
            continue
        # The server API fill export uses ``spread``/``fee`` while the joined
        # research export uses ``fill_spread``/``fill_fee``.  Accept both so
        # the newest local snapshot can be replayed without a lossy rewrite.
        spread = as_float(row.get("spread"))
        if spread is None:
            spread = as_float(row.get("fill_spread"))
        if spread is not None and spread >= 0:
            spreads[row["symbol"]].append(spread)
        fee = as_float(row.get("fee"))
        if fee is None:
            fee = as_float(row.get("fill_fee"))
        notional = as_float(row.get("filled_notional"))
        if notional is None:
            notional = as_float(row.get("entry_notional"))
        if fee is not None and notional and notional > 0:
            fee_rates.append(fee / notional)
    medians = {
        symbol: statistics.median(values)
        for symbol, values in spreads.items()
        if values
    }
    return medians, statistics.median(fee_rates) if fee_rates else DEFAULT_FEE_RATE


def load_candles(
    path: Path,
    symbols: set[str],
    start: datetime,
    end: datetime,
) -> dict[str, list[Candle]]:
    grouped: dict[str, list[Candle]] = defaultdict(list)
    with gzip.open(path, "rt", newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            symbol = row.get("symbol")
            if symbol not in symbols:
                continue
            candle_start = parse_dt(row["candle_start"])
            candle_end = parse_dt(row["candle_end"])
            if candle_end < start or candle_start > end:
                continue
            values = [
                as_float(row.get(name))
                for name in ("open_price", "high_price", "low_price", "close_price")
            ]
            if any(value is None or value <= 0 for value in values):
                continue
            grouped[symbol].append(
                Candle(
                    symbol=symbol,
                    start=candle_start,
                    end=candle_end,
                    open=values[0] or 0.0,
                    high=values[1] or 0.0,
                    low=values[2] or 0.0,
                    close=values[3] or 0.0,
                )
            )
    for values in grouped.values():
        values.sort(key=lambda candle: candle.end)
    return grouped


def load_quotes(
    path: Path,
    symbols: set[str],
    start: datetime,
    end: datetime,
) -> dict[str, list[Quote]]:
    grouped: dict[str, list[Quote]] = defaultdict(list)
    with gzip.open(path, "rt", newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            symbol = row.get("symbol")
            if symbol not in symbols:
                continue
            at = parse_dt(row["bucket_end"])
            if at < start or at > end:
                continue
            bid = as_float(row.get("last_bid_price"))
            ask = as_float(row.get("last_ask_price"))
            if bid is None and ask is None:
                continue
            grouped[symbol].append(Quote(at=at, bid=bid, ask=ask))
    for values in grouped.values():
        values.sort(key=lambda quote: quote.at)
    return grouped


def find_first_adverse(
    candles: list[Candle],
    *,
    side: str,
    opened_at: datetime,
) -> Candle | None:
    ends = [candle.end for candle in candles]
    index = bisect.bisect_right(ends, opened_at)
    for candle in candles[index:]:
        if side == "long" and candle.close < candle.open:
            return candle
        if side == "short" and candle.close > candle.open:
            return candle
    return None


def executable_price(quote: Quote, side: str) -> float | None:
    return quote.bid if side == "long" else quote.ask


def close_pnl(
    *,
    side: str,
    entry_price: float,
    quantity: float,
    entry_fee: float,
    exit_price: float,
    fee_rate: float,
) -> tuple[float, float, float]:
    gross = (
        (exit_price - entry_price) * quantity
        if side == "long"
        else (entry_price - exit_price) * quantity
    )
    exit_fee = abs(exit_price * quantity) * fee_rate
    return gross - entry_fee - exit_fee, gross, exit_fee


def fallback_close(
    candles: list[Candle],
    deadline: datetime,
    *,
    side: str,
    spread: float,
) -> tuple[datetime, float] | None:
    for candle in candles:
        if candle.end < deadline:
            continue
        if candle.end > deadline:
            break
        half_spread = max(spread, 0.0) / 2
        price = (
            candle.close - half_spread
            if side == "long"
            else candle.close + half_spread
        )
        return candle.end, max(price, 0.0)
    return None


def replay_grace_variant(
    position: dict[str, Any],
    candles: list[Candle],
    quotes: list[Quote],
    *,
    spread: float,
    fee_rate: float,
    prefix: str,
    timeout_bars: int,
    favorable_exit_pct: float = 0.0,
) -> dict[str, Any]:
    if timeout_bars not in GRACE_BARS:
        raise ValueError(f"timeout_bars must be one of {GRACE_BARS}")
    if favorable_exit_pct < 0 or favorable_exit_pct >= 1:
        raise ValueError("favorable_exit_pct must be in [0, 1)")
    result = dict(position)
    reverse = find_first_adverse(
        candles,
        side=position["side"],
        opened_at=position["opened_at"],
    )
    if reverse is None:
        result.update(
            {
                f"{prefix}_status": "no_reverse_candle",
                f"{prefix}_pnl": None,
                f"{prefix}_closed_at": None,
                f"{prefix}_exit_price": None,
                f"{prefix}_fill_kind": None,
            }
        )
        return result

    side = position["side"]
    entry = position["entry_price"]
    quantity = position["quantity"]
    target_price = (
        entry * (1 + favorable_exit_pct)
        if side == "long"
        else entry * (1 - favorable_exit_pct)
    )
    if target_price <= 0:
        raise ValueError("favorable exit target must be positive")
    deadline = reverse.end + timedelta(minutes=15 * timeout_bars)
    times = [quote.at for quote in quotes]
    signal_index = bisect.bisect_left(times, reverse.end)
    signal_quote = quotes[signal_index] if signal_index < len(quotes) else None
    signal_price = executable_price(signal_quote, side) if signal_quote else None
    if signal_price is None:
        half_spread = max(spread, 0.0) / 2
        signal_price = (
            reverse.close - half_spread
            if side == "long"
            else reverse.close + half_spread
        )

    # A limit below the current bid (or above the current ask for a short) is
    # marketable.  It fills immediately at the executable quote rather than
    # pretending that the strategy gives away an existing profit.
    already_marketable = (
        signal_price >= target_price
        if side == "long"
        else signal_price <= target_price
    )
    if already_marketable:
        closed_at = signal_quote.at if signal_quote and signal_price else reverse.end
        pnl, gross, exit_fee = close_pnl(
            side=side,
            entry_price=entry,
            quantity=quantity,
            entry_fee=position["entry_fee"],
            exit_price=signal_price,
            fee_rate=fee_rate,
        )
        result.update(
            {
                f"{prefix}_status": "closed",
                f"{prefix}_pnl": pnl,
                f"{prefix}_gross_pnl": gross,
                f"{prefix}_exit_fee": exit_fee,
                f"{prefix}_closed_at": closed_at,
                f"{prefix}_exit_price": signal_price,
                f"{prefix}_target_price": target_price,
                f"{prefix}_fill_kind": "immediate_marketable",
                f"{prefix}_reverse_candle_end": reverse.end,
                f"{prefix}_quote_data": bool(signal_quote),
            }
        )
        return result

    # Resting favorable limit.  A quote touch is the minimum observable
    # evidence we can get from the archived 15-second bid/ask stream.
    for quote in quotes[signal_index + 1 :]:
        if quote.at > deadline:
            break
        touched = (
            quote.bid is not None and quote.bid >= target_price
            if side == "long"
            else quote.ask is not None and quote.ask <= target_price
        )
        if not touched:
            continue
        pnl, gross, exit_fee = close_pnl(
            side=side,
            entry_price=entry,
            quantity=quantity,
            entry_fee=position["entry_fee"],
            exit_price=target_price,
            fee_rate=fee_rate,
        )
        result.update(
            {
                f"{prefix}_status": "closed",
                f"{prefix}_pnl": pnl,
                f"{prefix}_gross_pnl": gross,
                f"{prefix}_exit_fee": exit_fee,
                f"{prefix}_closed_at": quote.at,
                f"{prefix}_exit_price": target_price,
                f"{prefix}_target_price": target_price,
                f"{prefix}_fill_kind": "limit_favorable_quote_touch",
                f"{prefix}_reverse_candle_end": reverse.end,
                f"{prefix}_quote_data": True,
            }
        )
        return result

    timeout_index = bisect.bisect_left(times, deadline)
    timeout_quote = (
        quotes[timeout_index]
        if timeout_index < len(quotes) and quotes[timeout_index].at == deadline
        else None
    )
    if timeout_quote is not None:
        timeout_price = executable_price(timeout_quote, side)
        if timeout_price is not None and timeout_price > 0:
            pnl, gross, exit_fee = close_pnl(
                side=side,
                entry_price=entry,
                quantity=quantity,
                entry_fee=position["entry_fee"],
                exit_price=timeout_price,
                fee_rate=fee_rate,
            )
            result.update(
                {
                    f"{prefix}_status": "closed",
                    f"{prefix}_pnl": pnl,
                    f"{prefix}_gross_pnl": gross,
                    f"{prefix}_exit_fee": exit_fee,
                    f"{prefix}_closed_at": timeout_quote.at,
                    f"{prefix}_exit_price": timeout_price,
                    f"{prefix}_target_price": target_price,
                    f"{prefix}_fill_kind": "timeout_market_quote",
                    f"{prefix}_reverse_candle_end": reverse.end,
                    f"{prefix}_quote_data": True,
                }
            )
            return result

    fallback = fallback_close(candles, deadline, side=side, spread=spread)
    if fallback is not None:
        closed_at, exit_price = fallback
        pnl, gross, exit_fee = close_pnl(
            side=side,
            entry_price=entry,
            quantity=quantity,
            entry_fee=position["entry_fee"],
            exit_price=exit_price,
            fee_rate=fee_rate,
        )
        result.update(
            {
                f"{prefix}_status": "closed",
                f"{prefix}_pnl": pnl,
                f"{prefix}_gross_pnl": gross,
                f"{prefix}_exit_fee": exit_fee,
                f"{prefix}_closed_at": closed_at,
                f"{prefix}_exit_price": exit_price,
                f"{prefix}_target_price": target_price,
                f"{prefix}_fill_kind": "timeout_official_close_fallback",
                f"{prefix}_reverse_candle_end": reverse.end,
                f"{prefix}_quote_data": False,
            }
        )
        return result

    result.update(
        {
            f"{prefix}_status": "no_timeout_mark",
            f"{prefix}_pnl": None,
            f"{prefix}_closed_at": None,
            f"{prefix}_exit_price": None,
            f"{prefix}_target_price": target_price,
            f"{prefix}_fill_kind": None,
            f"{prefix}_reverse_candle_end": reverse.end,
            f"{prefix}_quote_data": False,
        }
    )
    return result


def replay_b1(
    position: dict[str, Any],
    candles: list[Candle],
    quotes: list[Quote],
    *,
    spread: float,
    fee_rate: float,
    favorable_exit_pct: float = 0.0,
) -> dict[str, Any]:
    return replay_grace_variant(
        position,
        candles,
        quotes,
        spread=spread,
        fee_rate=fee_rate,
        favorable_exit_pct=favorable_exit_pct,
        prefix="b1",
        timeout_bars=1,
    )


def replay_b2(
    position: dict[str, Any],
    candles: list[Candle],
    quotes: list[Quote],
    *,
    spread: float,
    fee_rate: float,
    favorable_exit_pct: float = 0.0,
) -> dict[str, Any]:
    return replay_grace_variant(
        position,
        candles,
        quotes,
        spread=spread,
        fee_rate=fee_rate,
        favorable_exit_pct=favorable_exit_pct,
        prefix="b2",
        timeout_bars=2,
    )


def replay_b3(
    position: dict[str, Any],
    candles: list[Candle],
    quotes: list[Quote],
    *,
    spread: float,
    fee_rate: float,
    favorable_exit_pct: float = 0.0,
) -> dict[str, Any]:
    return replay_grace_variant(
        position,
        candles,
        quotes,
        spread=spread,
        fee_rate=fee_rate,
        favorable_exit_pct=favorable_exit_pct,
        prefix="b3",
        timeout_bars=3,
    )


def replay_b4(
    position: dict[str, Any],
    candles: list[Candle],
    quotes: list[Quote],
    *,
    spread: float,
    fee_rate: float,
    favorable_exit_pct: float = 0.0,
) -> dict[str, Any]:
    return replay_grace_variant(
        position,
        candles,
        quotes,
        spread=spread,
        fee_rate=fee_rate,
        favorable_exit_pct=favorable_exit_pct,
        prefix="b4",
        timeout_bars=4,
    )


def replay_b8(
    position: dict[str, Any],
    candles: list[Candle],
    quotes: list[Quote],
    *,
    spread: float,
    fee_rate: float,
    favorable_exit_pct: float = 0.0,
) -> dict[str, Any]:
    return replay_grace_variant(
        position,
        candles,
        quotes,
        spread=spread,
        fee_rate=fee_rate,
        favorable_exit_pct=favorable_exit_pct,
        prefix="b8",
        timeout_bars=8,
    )


def replay_b16(
    position: dict[str, Any],
    candles: list[Candle],
    quotes: list[Quote],
    *,
    spread: float,
    fee_rate: float,
    favorable_exit_pct: float = 0.0,
) -> dict[str, Any]:
    return replay_grace_variant(
        position,
        candles,
        quotes,
        spread=spread,
        fee_rate=fee_rate,
        favorable_exit_pct=favorable_exit_pct,
        prefix="b16",
        timeout_bars=16,
    )


def replay_b96(
    position: dict[str, Any],
    candles: list[Candle],
    quotes: list[Quote],
    *,
    spread: float,
    fee_rate: float,
    favorable_exit_pct: float = 0.0,
) -> dict[str, Any]:
    return replay_grace_variant(
        position,
        candles,
        quotes,
        spread=spread,
        fee_rate=fee_rate,
        favorable_exit_pct=favorable_exit_pct,
        prefix="b96",
        timeout_bars=96,
    )


def profit_factor(values: list[float]) -> float | None:
    gains = sum(value for value in values if value > 0)
    losses = -sum(value for value in values if value < 0)
    if losses == 0:
        return None if gains == 0 else float("inf")
    return gains / losses


def max_drawdown(values: list[tuple[datetime, float]]) -> float:
    cumulative = 0.0
    peak = 0.0
    drawdown = 0.0
    for _, pnl in sorted(values):
        cumulative += pnl
        peak = max(peak, cumulative)
        drawdown = min(drawdown, cumulative - peak)
    return drawdown


def metric_rows(
    rows: list[dict[str, Any]], pnl_key: str, time_key: str
) -> dict[str, Any]:
    closed = [
        row
        for row in rows
        if row.get(pnl_key) is not None and row.get(time_key) is not None
    ]
    pnls = [float(row[pnl_key]) for row in closed]
    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value < 0]
    return {
        "trades": len(closed),
        "net_pnl": sum(pnls),
        "profit_factor": profit_factor(pnls),
        "win_rate": len(wins) / len(pnls) if pnls else None,
        "expectancy": statistics.mean(pnls) if pnls else None,
        "median_pnl": statistics.median(pnls) if pnls else None,
        "average_win": statistics.mean(wins) if wins else None,
        "average_loss": statistics.mean(losses) if losses else None,
        "max_drawdown": max_drawdown(
            [(row[time_key], float(row[pnl_key])) for row in closed]
        ),
    }


def aggregate_events(
    rows: list[dict[str, Any]], pnl_key: str, time_key: str
) -> dict[datetime, float]:
    events: dict[datetime, float] = defaultdict(float)
    for row in rows:
        if row.get(pnl_key) is None or row.get(time_key) is None:
            continue
        events[row[time_key]] += float(row[pnl_key])
    return dict(events)


def build_series(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    event_keys = {
        "b0": ("b0_pnl", "closed_at"),
        **{
            variant: (f"{variant}_pnl", f"{variant}_closed_at")
            for variant in GRACE_VARIANTS
        },
    }
    events = {
        variant: aggregate_events(rows, pnl_key, time_key)
        for variant, (pnl_key, time_key) in event_keys.items()
    }
    timestamps = sorted(
        set().union(*(variant_events for variant_events in events.values()))
    )
    cumulative = dict.fromkeys(event_keys, 0.0)
    peaks = dict.fromkeys(event_keys, 0.0)
    series: list[dict[str, Any]] = []
    for timestamp in timestamps:
        point: dict[str, Any] = {"timestamp": timestamp.isoformat()}
        for variant in event_keys:
            step = events[variant].get(timestamp, 0.0)
            cumulative[variant] += step
            peaks[variant] = max(peaks[variant], cumulative[variant])
            point[f"{variant}_step_pnl"] = step
            point[f"{variant}_cumulative_pnl"] = cumulative[variant]
            point[f"{variant}_drawdown"] = cumulative[variant] - peaks[variant]
        for variant in GRACE_VARIANTS:
            point[f"{variant}_minus_b0"] = cumulative[variant] - cumulative["b0"]
        for left, right in zip(GRACE_VARIANTS, GRACE_VARIANTS[1:], strict=False):
            point[f"{right}_minus_{left}"] = cumulative[right] - cumulative[left]
        series.append(point)
    return series


def summarize(rows: list[dict[str, Any]], fee_rate: float) -> dict[str, Any]:
    common = [
        row
        for row in rows
        if all(
            row.get(f"{variant}_pnl") is not None
            for variant in ("b0", *GRACE_VARIANTS)
        )
    ]
    common_by_variant = {
        variant: [
            row
            for row in rows
            if row.get("b0_pnl") is not None
            and row.get(f"{variant}_pnl") is not None
        ]
        for variant in GRACE_VARIANTS
    }

    def pair_delta(variant: str, pair_rows: list[dict[str, Any]]) -> dict[str, Any]:
        deltas = [
            float(row[f"{variant}_pnl"]) - float(row["b0_pnl"])
            for row in pair_rows
        ]
        ordered = sorted(deltas)
        return {
            "net_pnl_delta": sum(deltas),
            "mean_pnl_delta": statistics.mean(deltas) if deltas else None,
            "median_pnl_delta": statistics.median(deltas) if deltas else None,
            "variant_better_rate": (
                sum(delta > 0 for delta in deltas) / len(deltas) if deltas else None
            ),
            "p10": ordered[max(0, math.floor((len(ordered) - 1) * 0.10))]
            if ordered
            else None,
            "p90": ordered[math.ceil((len(ordered) - 1) * 0.90)]
            if ordered
            else None,
        }

    def fill_counts(variant: str, pair_rows: list[dict[str, Any]]) -> dict[str, int]:
        counts: dict[str, int] = defaultdict(int)
        for row in pair_rows:
            counts[row.get(f"{variant}_fill_kind") or "unknown"] += 1
        return dict(sorted(counts.items()))

    side_metrics = {
        side: {
            "b0": metric_rows(
                [row for row in common if row["side"] == side],
                "b0_pnl",
                "closed_at",
            ),
            "b1": metric_rows(
                [row for row in common if row["side"] == side],
                "b1_pnl",
                "b1_closed_at",
            ),
            "b2": metric_rows(
                [row for row in common if row["side"] == side],
                "b2_pnl",
                "b2_closed_at",
            ),
            "b3": metric_rows(
                [row for row in common if row["side"] == side],
                "b3_pnl",
                "b3_closed_at",
            ),
            "b4": metric_rows(
                [row for row in common if row["side"] == side],
                "b4_pnl",
                "b4_closed_at",
            ),
            "b8": metric_rows(
                [row for row in common if row["side"] == side],
                "b8_pnl",
                "b8_closed_at",
            ),
            "b16": metric_rows(
                [row for row in common if row["side"] == side],
                "b16_pnl",
                "b16_closed_at",
            ),
            "b96": metric_rows(
                [row for row in common if row["side"] == side],
                "b96_pnl",
                "b96_closed_at",
            ),
        }
        for side in ("long", "short")
    }
    b1_b2_rows = [
        row
        for row in rows
        if row.get("b1_pnl") is not None and row.get("b2_pnl") is not None
    ]
    b1_b2_deltas = [
        float(row["b2_pnl"]) - float(row["b1_pnl"]) for row in b1_b2_rows
    ]
    b1_b3_rows = [
        row
        for row in rows
        if row.get("b1_pnl") is not None and row.get("b3_pnl") is not None
    ]
    b1_b3_deltas = [
        float(row["b3_pnl"]) - float(row["b1_pnl"]) for row in b1_b3_rows
    ]
    b2_b3_rows = [
        row
        for row in rows
        if row.get("b2_pnl") is not None and row.get("b3_pnl") is not None
    ]
    b2_b3_deltas = [
        float(row["b3_pnl"]) - float(row["b2_pnl"]) for row in b2_b3_rows
    ]
    b1_b4_rows = [
        row
        for row in rows
        if row.get("b1_pnl") is not None and row.get("b4_pnl") is not None
    ]
    b1_b4_deltas = [
        float(row["b4_pnl"]) - float(row["b1_pnl"]) for row in b1_b4_rows
    ]
    b2_b4_rows = [
        row
        for row in rows
        if row.get("b2_pnl") is not None and row.get("b4_pnl") is not None
    ]
    b2_b4_deltas = [
        float(row["b4_pnl"]) - float(row["b2_pnl"]) for row in b2_b4_rows
    ]
    b3_b4_rows = [
        row
        for row in rows
        if row.get("b3_pnl") is not None and row.get("b4_pnl") is not None
    ]
    b3_b4_deltas = [
        float(row["b4_pnl"]) - float(row["b3_pnl"]) for row in b3_b4_rows
    ]
    b1_b8_rows = [
        row
        for row in rows
        if row.get("b1_pnl") is not None and row.get("b8_pnl") is not None
    ]
    b1_b8_deltas = [
        float(row["b8_pnl"]) - float(row["b1_pnl"]) for row in b1_b8_rows
    ]
    b2_b8_rows = [
        row
        for row in rows
        if row.get("b2_pnl") is not None and row.get("b8_pnl") is not None
    ]
    b2_b8_deltas = [
        float(row["b8_pnl"]) - float(row["b2_pnl"]) for row in b2_b8_rows
    ]
    b3_b8_rows = [
        row
        for row in rows
        if row.get("b3_pnl") is not None and row.get("b8_pnl") is not None
    ]
    b3_b8_deltas = [
        float(row["b8_pnl"]) - float(row["b3_pnl"]) for row in b3_b8_rows
    ]
    b4_b8_rows = [
        row
        for row in rows
        if row.get("b4_pnl") is not None and row.get("b8_pnl") is not None
    ]
    b4_b8_deltas = [
        float(row["b8_pnl"]) - float(row["b4_pnl"]) for row in b4_b8_rows
    ]
    b1_b16_rows = [
        row
        for row in rows
        if row.get("b1_pnl") is not None and row.get("b16_pnl") is not None
    ]
    b1_b16_deltas = [
        float(row["b16_pnl"]) - float(row["b1_pnl"]) for row in b1_b16_rows
    ]
    b2_b16_rows = [
        row
        for row in rows
        if row.get("b2_pnl") is not None and row.get("b16_pnl") is not None
    ]
    b2_b16_deltas = [
        float(row["b16_pnl"]) - float(row["b2_pnl"]) for row in b2_b16_rows
    ]
    b3_b16_rows = [
        row
        for row in rows
        if row.get("b3_pnl") is not None and row.get("b16_pnl") is not None
    ]
    b3_b16_deltas = [
        float(row["b16_pnl"]) - float(row["b3_pnl"]) for row in b3_b16_rows
    ]
    b4_b16_rows = [
        row
        for row in rows
        if row.get("b4_pnl") is not None and row.get("b16_pnl") is not None
    ]
    b4_b16_deltas = [
        float(row["b16_pnl"]) - float(row["b4_pnl"]) for row in b4_b16_rows
    ]
    b8_b16_rows = [
        row
        for row in rows
        if row.get("b8_pnl") is not None and row.get("b16_pnl") is not None
    ]
    b8_b16_deltas = [
        float(row["b16_pnl"]) - float(row["b8_pnl"]) for row in b8_b16_rows
    ]
    b1_b96_rows = [
        row
        for row in rows
        if row.get("b1_pnl") is not None and row.get("b96_pnl") is not None
    ]
    b1_b96_deltas = [
        float(row["b96_pnl"]) - float(row["b1_pnl"]) for row in b1_b96_rows
    ]
    b8_b96_rows = [
        row
        for row in rows
        if row.get("b8_pnl") is not None and row.get("b96_pnl") is not None
    ]
    b8_b96_deltas = [
        float(row["b96_pnl"]) - float(row["b8_pnl"]) for row in b8_b96_rows
    ]
    overall = {
        "b0": metric_rows(rows, "b0_pnl", "closed_at"),
        "b1": metric_rows(rows, "b1_pnl", "b1_closed_at"),
        "b2": metric_rows(rows, "b2_pnl", "b2_closed_at"),
        "b3": metric_rows(rows, "b3_pnl", "b3_closed_at"),
        "b4": metric_rows(rows, "b4_pnl", "b4_closed_at"),
        "b8": metric_rows(rows, "b8_pnl", "b8_closed_at"),
        "b16": metric_rows(rows, "b16_pnl", "b16_closed_at"),
        "b96": metric_rows(rows, "b96_pnl", "b96_closed_at"),
    }
    return {
        "fee_rate": fee_rate,
        "position_rows": len(rows),
        "b0_closed_rows": sum(row.get("b0_pnl") is not None for row in rows),
        "b1_closed_rows": sum(row.get("b1_pnl") is not None for row in rows),
        "b2_closed_rows": sum(row.get("b2_pnl") is not None for row in rows),
        "b3_closed_rows": sum(row.get("b3_pnl") is not None for row in rows),
        "b4_closed_rows": sum(row.get("b4_pnl") is not None for row in rows),
        "b8_closed_rows": sum(row.get("b8_pnl") is not None for row in rows),
        "b16_closed_rows": sum(row.get("b16_pnl") is not None for row in rows),
        "b96_closed_rows": sum(row.get("b96_pnl") is not None for row in rows),
        "common_matured_rows": len(common),
        "b1_common_matured_rows": len(common_by_variant["b1"]),
        "b2_common_matured_rows": len(common_by_variant["b2"]),
        "b3_common_matured_rows": len(common_by_variant["b3"]),
        "b4_common_matured_rows": len(common_by_variant["b4"]),
        "b8_common_matured_rows": len(common_by_variant["b8"]),
        "b16_common_matured_rows": len(common_by_variant["b16"]),
        "b96_common_matured_rows": len(common_by_variant["b96"]),
        "b1_unresolved_rows": sum(row.get("b1_pnl") is None for row in rows),
        "b2_unresolved_rows": sum(row.get("b2_pnl") is None for row in rows),
        "b3_unresolved_rows": sum(row.get("b3_pnl") is None for row in rows),
        "b4_unresolved_rows": sum(row.get("b4_pnl") is None for row in rows),
        "b8_unresolved_rows": sum(row.get("b8_pnl") is None for row in rows),
        "b16_unresolved_rows": sum(row.get("b16_pnl") is None for row in rows),
        "b96_unresolved_rows": sum(row.get("b96_pnl") is None for row in rows),
        "b0": metric_rows(common, "b0_pnl", "closed_at"),
        "b1": metric_rows(common, "b1_pnl", "b1_closed_at"),
        "b2": metric_rows(common, "b2_pnl", "b2_closed_at"),
        "b3": metric_rows(common, "b3_pnl", "b3_closed_at"),
        "b4": metric_rows(common, "b4_pnl", "b4_closed_at"),
        "b8": metric_rows(common, "b8_pnl", "b8_closed_at"),
        "b16": metric_rows(common, "b16_pnl", "b16_closed_at"),
        "b96": metric_rows(common, "b96_pnl", "b96_closed_at"),
        # ``b0``/``b1``/``b2`` above are intentionally paired on the same
        # mature rows for the chart.  Keep the full-snapshot totals separate
        # so open/unresolved rows cannot be mistaken for zero PnL.
        "overall": overall,
        "paired_delta": pair_delta("b1", common_by_variant["b1"]),
        "paired_delta_b2": pair_delta("b2", common_by_variant["b2"]),
        "paired_delta_b3": pair_delta("b3", common_by_variant["b3"]),
        "paired_delta_b4": pair_delta("b4", common_by_variant["b4"]),
        "paired_delta_b8": pair_delta("b8", common_by_variant["b8"]),
        "paired_delta_b16": pair_delta("b16", common_by_variant["b16"]),
        "paired_delta_b96": pair_delta("b96", common_by_variant["b96"]),
        "b1_vs_b2": {
            "net_pnl_delta": sum(b1_b2_deltas),
            "mean_pnl_delta": (
                statistics.mean(b1_b2_deltas) if b1_b2_deltas else None
            ),
            "median_pnl_delta": (
                statistics.median(b1_b2_deltas) if b1_b2_deltas else None
            ),
            "b2_better_rate": (
                sum(delta > 0 for delta in b1_b2_deltas) / len(b1_b2_deltas)
                if b1_b2_deltas
                else None
            ),
        },
        "b1_vs_b3": {
            "net_pnl_delta": sum(b1_b3_deltas),
            "mean_pnl_delta": (
                statistics.mean(b1_b3_deltas) if b1_b3_deltas else None
            ),
            "median_pnl_delta": (
                statistics.median(b1_b3_deltas) if b1_b3_deltas else None
            ),
            "b3_better_rate": (
                sum(delta > 0 for delta in b1_b3_deltas) / len(b1_b3_deltas)
                if b1_b3_deltas
                else None
            ),
        },
        "b2_vs_b3": {
            "net_pnl_delta": sum(b2_b3_deltas),
            "mean_pnl_delta": (
                statistics.mean(b2_b3_deltas) if b2_b3_deltas else None
            ),
            "median_pnl_delta": (
                statistics.median(b2_b3_deltas) if b2_b3_deltas else None
            ),
            "b3_better_rate": (
                sum(delta > 0 for delta in b2_b3_deltas) / len(b2_b3_deltas)
                if b2_b3_deltas
                else None
            ),
        },
        "b1_vs_b4": {
            "net_pnl_delta": sum(b1_b4_deltas),
            "mean_pnl_delta": (
                statistics.mean(b1_b4_deltas) if b1_b4_deltas else None
            ),
            "median_pnl_delta": (
                statistics.median(b1_b4_deltas) if b1_b4_deltas else None
            ),
            "b4_better_rate": (
                sum(delta > 0 for delta in b1_b4_deltas) / len(b1_b4_deltas)
                if b1_b4_deltas
                else None
            ),
        },
        "b2_vs_b4": {
            "net_pnl_delta": sum(b2_b4_deltas),
            "mean_pnl_delta": (
                statistics.mean(b2_b4_deltas) if b2_b4_deltas else None
            ),
            "median_pnl_delta": (
                statistics.median(b2_b4_deltas) if b2_b4_deltas else None
            ),
            "b4_better_rate": (
                sum(delta > 0 for delta in b2_b4_deltas) / len(b2_b4_deltas)
                if b2_b4_deltas
                else None
            ),
        },
        "b3_vs_b4": {
            "net_pnl_delta": sum(b3_b4_deltas),
            "mean_pnl_delta": (
                statistics.mean(b3_b4_deltas) if b3_b4_deltas else None
            ),
            "median_pnl_delta": (
                statistics.median(b3_b4_deltas) if b3_b4_deltas else None
            ),
            "b4_better_rate": (
                sum(delta > 0 for delta in b3_b4_deltas) / len(b3_b4_deltas)
                if b3_b4_deltas
                else None
            ),
        },
        "b1_vs_b8": {
            "net_pnl_delta": sum(b1_b8_deltas),
            "mean_pnl_delta": (
                statistics.mean(b1_b8_deltas) if b1_b8_deltas else None
            ),
            "median_pnl_delta": (
                statistics.median(b1_b8_deltas) if b1_b8_deltas else None
            ),
            "b8_better_rate": (
                sum(delta > 0 for delta in b1_b8_deltas) / len(b1_b8_deltas)
                if b1_b8_deltas
                else None
            ),
        },
        "b2_vs_b8": {
            "net_pnl_delta": sum(b2_b8_deltas),
            "mean_pnl_delta": (
                statistics.mean(b2_b8_deltas) if b2_b8_deltas else None
            ),
            "median_pnl_delta": (
                statistics.median(b2_b8_deltas) if b2_b8_deltas else None
            ),
            "b8_better_rate": (
                sum(delta > 0 for delta in b2_b8_deltas) / len(b2_b8_deltas)
                if b2_b8_deltas
                else None
            ),
        },
        "b3_vs_b8": {
            "net_pnl_delta": sum(b3_b8_deltas),
            "mean_pnl_delta": (
                statistics.mean(b3_b8_deltas) if b3_b8_deltas else None
            ),
            "median_pnl_delta": (
                statistics.median(b3_b8_deltas) if b3_b8_deltas else None
            ),
            "b8_better_rate": (
                sum(delta > 0 for delta in b3_b8_deltas) / len(b3_b8_deltas)
                if b3_b8_deltas
                else None
            ),
        },
        "b4_vs_b8": {
            "net_pnl_delta": sum(b4_b8_deltas),
            "mean_pnl_delta": (
                statistics.mean(b4_b8_deltas) if b4_b8_deltas else None
            ),
            "median_pnl_delta": (
                statistics.median(b4_b8_deltas) if b4_b8_deltas else None
            ),
            "b8_better_rate": (
                sum(delta > 0 for delta in b4_b8_deltas) / len(b4_b8_deltas)
                if b4_b8_deltas
                else None
            ),
        },
        "b1_vs_b16": {
            "net_pnl_delta": sum(b1_b16_deltas),
            "mean_pnl_delta": (
                statistics.mean(b1_b16_deltas) if b1_b16_deltas else None
            ),
            "median_pnl_delta": (
                statistics.median(b1_b16_deltas) if b1_b16_deltas else None
            ),
            "b16_better_rate": (
                sum(delta > 0 for delta in b1_b16_deltas) / len(b1_b16_deltas)
                if b1_b16_deltas
                else None
            ),
        },
        "b2_vs_b16": {
            "net_pnl_delta": sum(b2_b16_deltas),
            "mean_pnl_delta": (
                statistics.mean(b2_b16_deltas) if b2_b16_deltas else None
            ),
            "median_pnl_delta": (
                statistics.median(b2_b16_deltas) if b2_b16_deltas else None
            ),
            "b16_better_rate": (
                sum(delta > 0 for delta in b2_b16_deltas) / len(b2_b16_deltas)
                if b2_b16_deltas
                else None
            ),
        },
        "b3_vs_b16": {
            "net_pnl_delta": sum(b3_b16_deltas),
            "mean_pnl_delta": (
                statistics.mean(b3_b16_deltas) if b3_b16_deltas else None
            ),
            "median_pnl_delta": (
                statistics.median(b3_b16_deltas) if b3_b16_deltas else None
            ),
            "b16_better_rate": (
                sum(delta > 0 for delta in b3_b16_deltas) / len(b3_b16_deltas)
                if b3_b16_deltas
                else None
            ),
        },
        "b4_vs_b16": {
            "net_pnl_delta": sum(b4_b16_deltas),
            "mean_pnl_delta": (
                statistics.mean(b4_b16_deltas) if b4_b16_deltas else None
            ),
            "median_pnl_delta": (
                statistics.median(b4_b16_deltas) if b4_b16_deltas else None
            ),
            "b16_better_rate": (
                sum(delta > 0 for delta in b4_b16_deltas) / len(b4_b16_deltas)
                if b4_b16_deltas
                else None
            ),
        },
        "b8_vs_b16": {
            "net_pnl_delta": sum(b8_b16_deltas),
            "mean_pnl_delta": (
                statistics.mean(b8_b16_deltas) if b8_b16_deltas else None
            ),
            "median_pnl_delta": (
                statistics.median(b8_b16_deltas) if b8_b16_deltas else None
            ),
            "b16_better_rate": (
                sum(delta > 0 for delta in b8_b16_deltas) / len(b8_b16_deltas)
                if b8_b16_deltas
                else None
            ),
        },
        "b1_vs_b96": {
            "net_pnl_delta": sum(b1_b96_deltas),
            "mean_pnl_delta": (
                statistics.mean(b1_b96_deltas) if b1_b96_deltas else None
            ),
            "median_pnl_delta": (
                statistics.median(b1_b96_deltas) if b1_b96_deltas else None
            ),
            "b96_better_rate": (
                sum(delta > 0 for delta in b1_b96_deltas) / len(b1_b96_deltas)
                if b1_b96_deltas
                else None
            ),
        },
        "b8_vs_b96": {
            "net_pnl_delta": sum(b8_b96_deltas),
            "mean_pnl_delta": (
                statistics.mean(b8_b96_deltas) if b8_b96_deltas else None
            ),
            "median_pnl_delta": (
                statistics.median(b8_b96_deltas) if b8_b96_deltas else None
            ),
            "b96_better_rate": (
                sum(delta > 0 for delta in b8_b96_deltas) / len(b8_b96_deltas)
                if b8_b96_deltas
                else None
            ),
        },
        "b1_fill_kind": fill_counts("b1", common_by_variant["b1"]),
        "b2_fill_kind": fill_counts("b2", common_by_variant["b2"]),
        "b3_fill_kind": fill_counts("b3", common_by_variant["b3"]),
        "b4_fill_kind": fill_counts("b4", common_by_variant["b4"]),
        "b8_fill_kind": fill_counts("b8", common_by_variant["b8"]),
        "b16_fill_kind": fill_counts("b16", common_by_variant["b16"]),
        "b96_fill_kind": fill_counts("b96", common_by_variant["b96"]),
        "side": side_metrics,
    }


def serializable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): serializable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [serializable(item) for item in value]
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--positions", type=Path, required=True)
    parser.add_argument("--fills", type=Path, required=True)
    parser.add_argument("--klines", type=Path, required=True)
    parser.add_argument("--states", type=Path, required=True)
    parser.add_argument("--snapshot-manifest", type=Path)
    parser.add_argument(
        "--side",
        choices=("all", "long", "short"),
        default="all",
        help="restrict the paired replay to one position side (default: all)",
    )
    parser.add_argument(
        "--favorable-exit-pct",
        type=float,
        default=0.0,
        help=(
            "gross favorable price offset for the grace limit, directionally "
            "from entry (e.g. 0.0058 for 0.58%%); default 0 keeps entry price"
        ),
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    positions = load_positions(args.positions)
    if args.side != "all":
        positions = [row for row in positions if row["side"] == args.side]
    if not positions:
        raise SystemExit(
            f"no {args.side} rows for {TARGET_RUN} in {args.positions}"
        )
    symbols = {row["symbol"] for row in positions}
    min_open = min(row["opened_at"] for row in positions)
    max_observed = max(
        [row["closed_at"] for row in positions if row["closed_at"] is not None]
        or [min_open]
    )
    start = min_open - timedelta(minutes=30)
    end = max_observed + timedelta(minutes=45)
    spreads, fee_rate = load_entry_spreads_and_fee(args.fills)
    candles = load_candles(args.klines, symbols, start, end)
    quotes = load_quotes(args.states, symbols, start, end)

    rows: list[dict[str, Any]] = []
    for position in positions:
        result = replay_b1(
            position,
            candles.get(position["symbol"], []),
            quotes.get(position["symbol"], []),
            spread=spreads.get(position["symbol"], 0.0),
            fee_rate=fee_rate,
            favorable_exit_pct=args.favorable_exit_pct,
        )
        result = replay_b2(
            result,
            candles.get(position["symbol"], []),
            quotes.get(position["symbol"], []),
            spread=spreads.get(position["symbol"], 0.0),
            fee_rate=fee_rate,
            favorable_exit_pct=args.favorable_exit_pct,
        )
        result = replay_b3(
            result,
            candles.get(position["symbol"], []),
            quotes.get(position["symbol"], []),
            spread=spreads.get(position["symbol"], 0.0),
            fee_rate=fee_rate,
            favorable_exit_pct=args.favorable_exit_pct,
        )
        result = replay_b4(
            result,
            candles.get(position["symbol"], []),
            quotes.get(position["symbol"], []),
            spread=spreads.get(position["symbol"], 0.0),
            fee_rate=fee_rate,
            favorable_exit_pct=args.favorable_exit_pct,
        )
        result = replay_b8(
            result,
            candles.get(position["symbol"], []),
            quotes.get(position["symbol"], []),
            spread=spreads.get(position["symbol"], 0.0),
            fee_rate=fee_rate,
            favorable_exit_pct=args.favorable_exit_pct,
        )
        result = replay_b16(
            result,
            candles.get(position["symbol"], []),
            quotes.get(position["symbol"], []),
            spread=spreads.get(position["symbol"], 0.0),
            fee_rate=fee_rate,
            favorable_exit_pct=args.favorable_exit_pct,
        )
        result = replay_b96(
            result,
            candles.get(position["symbol"], []),
            quotes.get(position["symbol"], []),
            spread=spreads.get(position["symbol"], 0.0),
            fee_rate=fee_rate,
            favorable_exit_pct=args.favorable_exit_pct,
        )
        result["delta_pnl"] = (
            result["b1_pnl"] - result["b0_pnl"]
            if result.get("b1_pnl") is not None and result.get("b0_pnl") is not None
            else None
        )
        result["b2_delta_pnl"] = (
            result["b2_pnl"] - result["b0_pnl"]
            if result.get("b2_pnl") is not None and result.get("b0_pnl") is not None
            else None
        )
        result["b3_delta_pnl"] = (
            result["b3_pnl"] - result["b0_pnl"]
            if result.get("b3_pnl") is not None and result.get("b0_pnl") is not None
            else None
        )
        result["b4_delta_pnl"] = (
            result["b4_pnl"] - result["b0_pnl"]
            if result.get("b4_pnl") is not None and result.get("b0_pnl") is not None
            else None
        )
        result["b8_delta_pnl"] = (
            result["b8_pnl"] - result["b0_pnl"]
            if result.get("b8_pnl") is not None and result.get("b0_pnl") is not None
            else None
        )
        result["b16_delta_pnl"] = (
            result["b16_pnl"] - result["b0_pnl"]
            if result.get("b16_pnl") is not None and result.get("b0_pnl") is not None
            else None
        )
        result["b96_delta_pnl"] = (
            result["b96_pnl"] - result["b0_pnl"]
            if result.get("b96_pnl") is not None and result.get("b0_pnl") is not None
            else None
        )
        rows.append(result)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    trade_path = args.output_dir / "trade_comparison.csv"
    fieldnames = [
        "position_id",
        "symbol",
        "side",
        "opened_at",
        "closed_at",
        "b0_pnl",
        "b0_close_reason",
        "b1_status",
        "b1_reverse_candle_end",
        "b1_closed_at",
        "b1_exit_price",
        "b1_target_price",
        "b1_pnl",
        "delta_pnl",
        "b1_fill_kind",
        "b1_quote_data",
        "b2_status",
        "b2_reverse_candle_end",
        "b2_closed_at",
        "b2_exit_price",
        "b2_target_price",
        "b2_pnl",
        "b2_delta_pnl",
        "b2_fill_kind",
        "b2_quote_data",
        "b3_status",
        "b3_reverse_candle_end",
        "b3_closed_at",
        "b3_exit_price",
        "b3_target_price",
        "b3_pnl",
        "b3_delta_pnl",
        "b3_fill_kind",
        "b3_quote_data",
        "b4_status",
        "b4_reverse_candle_end",
        "b4_closed_at",
        "b4_exit_price",
        "b4_target_price",
        "b4_pnl",
        "b4_delta_pnl",
        "b4_fill_kind",
        "b4_quote_data",
        "b8_status",
        "b8_reverse_candle_end",
        "b8_closed_at",
        "b8_exit_price",
        "b8_target_price",
        "b8_pnl",
        "b8_delta_pnl",
        "b8_fill_kind",
        "b8_quote_data",
        "b16_status",
        "b16_reverse_candle_end",
        "b16_closed_at",
        "b16_exit_price",
        "b16_target_price",
        "b16_pnl",
        "b16_delta_pnl",
        "b16_fill_kind",
        "b16_quote_data",
        "b96_status",
        "b96_reverse_candle_end",
        "b96_closed_at",
        "b96_exit_price",
        "b96_target_price",
        "b96_pnl",
        "b96_delta_pnl",
        "b96_fill_kind",
        "b96_quote_data",
    ]
    with trade_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    key: serializable(row.get(key))
                    for key in fieldnames
                }
            )

    # Keep the chart strictly paired. The server snapshot has open positions;
    # including exits visible only to a grace variant would make the comparison
    # look artificially favorable.
    common_rows = [
        row
        for row in rows
        if all(
            row.get(f"{variant}_pnl") is not None
            for variant in ("b0", *GRACE_VARIANTS)
        )
    ]
    series = build_series(common_rows)
    series_path = args.output_dir / "equity_series.csv"
    series_fields = list(series[0]) if series else [
        "timestamp",
        "b0_step_pnl",
        "b1_step_pnl",
        "b0_cumulative_pnl",
        "b1_cumulative_pnl",
        "b2_cumulative_pnl",
        "b3_cumulative_pnl",
        "b4_cumulative_pnl",
        "b8_cumulative_pnl",
        "b16_cumulative_pnl",
        "b96_cumulative_pnl",
        "b0_drawdown",
        "b1_drawdown",
        "b2_drawdown",
        "b3_drawdown",
        "b4_drawdown",
        "b8_drawdown",
        "b16_drawdown",
        "b96_drawdown",
        "b1_minus_b0",
        "b2_minus_b0",
        "b2_minus_b1",
        "b3_minus_b0",
        "b3_minus_b1",
        "b3_minus_b2",
        "b4_minus_b0",
        "b4_minus_b1",
        "b4_minus_b2",
        "b4_minus_b3",
        "b8_minus_b0",
        "b8_minus_b1",
        "b8_minus_b2",
        "b8_minus_b3",
        "b8_minus_b4",
        "b16_minus_b0",
        "b16_minus_b1",
        "b16_minus_b2",
        "b16_minus_b3",
        "b16_minus_b4",
        "b16_minus_b8",
        "b96_minus_b0",
        "b96_minus_b1",
        "b96_minus_b2",
        "b96_minus_b3",
        "b96_minus_b4",
        "b96_minus_b8",
        "b96_minus_b16",
    ]
    with series_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=series_fields)
        writer.writeheader()
        writer.writerows(series)

    manifest: dict[str, Any] = {}
    if args.snapshot_manifest and args.snapshot_manifest.exists():
        manifest = json.loads(args.snapshot_manifest.read_text(encoding="utf-8"))
    summary = summarize(rows, fee_rate)
    summary["favorable_exit_pct"] = args.favorable_exit_pct
    summary["favorable_exit_description"] = (
        f"directional entry-price offset of {args.favorable_exit_pct:.6%} "
        "before fees"
    )
    summary["data"] = {
        "target_run": TARGET_RUN,
        "side_filter": args.side,
        "snapshot_manifest": (
            str(args.snapshot_manifest) if args.snapshot_manifest else None
        ),
        "snapshot_at_utc": manifest.get("snapshot_at_utc"),
        "snapshot_server": manifest.get("source", {}).get("server"),
        "positions_path": str(args.positions),
        "klines_path": str(args.klines),
        "states_path": str(args.states),
        "symbols": len(symbols),
        "candles_loaded": sum(len(values) for values in candles.values()),
        "quotes_loaded": sum(len(values) for values in quotes.values()),
        "quote_symbols": len(quotes),
        "time_start": min_open.isoformat(),
        "time_end": max_observed.isoformat(),
        "timestamp_mode": (
            "server_recorded_opened_at; first adverse candle end strictly "
            "after opened_at"
        ),
        "limit_fill_model": (
            "15s executable bid/ask touch; no OHLC touch assumed when quote is missing"
        ),
        "timeout_model": (
            "B1 executable quote exactly at next 15m close; B2 exactly at "
            "second next 15m close; B3 exactly at third next 15m close; B4 "
            "exactly at fourth next 15m close; B8 exactly at eighth next 15m "
            "close; B16 exactly at sixteenth next 15m close; B96 exactly at "
            "ninety-sixth next 15m close; official close plus entry spread "
            "fallback when that quote is missing; unresolved when the exact "
            "deadline candle is missing"
        ),
    }
    summary["outputs"] = {
        "trade_comparison": str(trade_path),
        "equity_series": str(series_path),
    }
    (args.output_dir / "metrics.json").write_text(
        json.dumps(serializable(summary), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(serializable(summary), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
