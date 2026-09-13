#!/usr/bin/env python3
"""Analyze a volume-profile breakout-acceptance sequence from CML exports.

The server currently persists 15-second aggregates rather than every aggTrade
print or order-book level.  This script therefore reconstructs an approximate
volume profile from notional-weighted 15-second typical prices and labels the
sell-absorption step as a proxy.  All windows are causal relative to the
strategy signal; post-signal stages are deliberately reported as a delayed
confirmation/re-entry study rather than as a claim that the original entry
could have known the future.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


UTC = timezone.utc
State = tuple[
    float,
    float,
    float,
    float,
    float,
    float,
    float,
    int,
    bool,
    int,
    float,
]


def parse_time(value: str) -> float:
    normalized = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).timestamp()


def format_time(value: float | None) -> str | None:
    if value is None:
        return None
    return (
        datetime.fromtimestamp(value, tz=UTC)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_bool(value: Any) -> bool:
    return str(value).lower() in {"1", "true", "t", "yes"}


def safe_json(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


@dataclass
class Series:
    rows: list[State]
    times: list[float]

    def window(self, start: float, end: float) -> list[State]:
        left = self._left(start)
        right = self._left(end)
        return self.rows[left:right]

    def at_or_after(self, value: float) -> State | None:
        index = self._left(value)
        return self.rows[index] if index < len(self.rows) else None

    def at_or_before(self, value: float) -> State | None:
        index = self._left(value)
        if index < len(self.rows) and self.times[index] == value:
            return self.rows[index]
        index -= 1
        return self.rows[index] if index >= 0 else None

    def _left(self, value: float) -> int:
        lo = 0
        hi = len(self.times)
        while lo < hi:
            mid = (lo + hi) // 2
            if self.times[mid] < value:
                lo = mid + 1
            else:
                hi = mid
        return lo


@dataclass(frozen=True, slots=True)
class Candle15:
    start: float
    end: float
    open: float
    high: float
    low: float
    close: float
    state_count: int


def load_signals(path: Path) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    with gzip.open(path, "rt", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("account_label") != "primary":
                continue
            if row.get("strategy_name") != "orderflow_impulse":
                continue
            if row.get("signal_kind") != "strategy_signal":
                continue
            if row.get("side") != "long":
                continue

            features = safe_json(row.get("features"))
            candidate_context = safe_json(row.get("candidate_context"))
            candidate_id = row.get("candidate_id") or ""
            if not candidate_id:
                candidates = candidate_context.get("candidates")
                if isinstance(candidates, list) and candidates:
                    first_candidate = candidates[0]
                    if isinstance(first_candidate, dict):
                        candidate_id = str(first_candidate.get("candidate_id") or "")
            detected_at = parse_time(row["detected_at"])
            impulse_start = as_float(features.get("_impulse_start_epoch"))
            if impulse_start is None:
                raw_start = features.get("impulse_start")
                impulse_start = (
                    parse_time(str(raw_start))
                    if raw_start
                    else detected_at - 45.0
                )
            signals.append(
                {
                    "observation_id": row.get("observation_id"),
                    "signal_id": row.get("signal_id"),
                    "candidate_id": candidate_id,
                    "run_id": row.get("run_id"),
                    "symbol": row["symbol"],
                    "detected_at": detected_at,
                    "features": features,
                    "signal_price": as_float(features.get("impulse_end_price")),
                    "impulse_start": impulse_start,
                }
            )
    return signals


def load_states(path: Path) -> dict[str, Series]:
    grouped: dict[str, list[State]] = defaultdict(list)
    with gzip.open(path, "rt", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            high = as_float(row.get("high_price"))
            low = as_float(row.get("low_price"))
            close = as_float(row.get("close_price"))
            if high is None or low is None or close is None:
                continue
            open_price = as_float(row.get("open_price"))
            if open_price is None or open_price <= 0:
                open_price = close
            grouped[row["symbol"]].append(
                (
                    parse_time(row["bucket_start"]),
                    high,
                    low,
                    close,
                    as_float(row.get("trade_notional")) or 0.0,
                    as_float(row.get("aggressive_buy_notional")) or 0.0,
                    as_float(row.get("aggressive_sell_notional")) or 0.0,
                    int(float(row.get("trade_count") or 0)),
                    as_bool(row.get("data_complete")),
                    int(float(row.get("missing_agg_trade_count") or 0)),
                    open_price,
                )
            )

    result: dict[str, Series] = {}
    for symbol, rows in grouped.items():
        rows.sort(key=lambda item: item[0])
        result[symbol] = Series(rows=rows, times=[row[0] for row in rows])
    return result


def closed_candles(series: Series) -> list[Candle15]:
    """Aggregate the exported 15s OHLC states into closed 15m candles.

    The production exit consumes Binance's immutable 15m candle feed.  The
    export contains the same price path at 15s resolution, so this is a close
    replay approximation.  Buckets with no trade have no OHLC row in the
    series and are therefore carried by the last observed price only when a
    later traded bucket exists; this is sufficient for the liquid signal
    symbols in the two-day sample.
    """
    grouped: dict[float, list[State]] = defaultdict(list)
    for row in series.rows:
        start = math.floor(row[0] / 900.0) * 900.0
        grouped[start].append(row)

    candles: list[Candle15] = []
    for start in sorted(grouped):
        rows = grouped[start]
        first = rows[0]
        last = rows[-1]
        candles.append(
            Candle15(
                start=start,
                end=start + 900.0,
                open=first[10] if first[10] > 0 else first[3],
                high=max(row[1] for row in rows),
                low=min(row[2] for row in rows),
                close=last[3],
                state_count=len(rows),
            )
        )
    return candles


def representative_price(row: State) -> float:
    _time, high, low, close, *_ = row
    if high > 0 and low > 0 and close > 0:
        return (high + low + close) / 3.0
    return close


def typical_prices(rows: Iterable[State]) -> list[float]:
    return [
        representative_price(row)
        for row in rows
        if row[4] > 0 and representative_price(row) > 0
    ]


def profile(
    rows: Iterable[State],
    anchor: float,
    bin_pct: float,
    value_area_pct: float,
) -> dict[str, Any] | None:
    if anchor <= 0:
        return None
    base = 1.0 + bin_pct
    log_base = math.log(base)
    bins: dict[int, float] = defaultdict(float)
    usable_rows = 0
    for row in rows:
        notional = row[4]
        price = representative_price(row)
        if notional <= 0 or price <= 0:
            continue
        index = math.floor(math.log(price / anchor) / log_base)
        bins[index] += notional
        usable_rows += 1
    if not bins:
        return None

    total = sum(bins.values())
    poc_index = max(bins, key=lambda index: (bins[index], -abs(index)))
    lower = upper = poc_index
    value = bins[poc_index]
    target = total * value_area_pct
    while value < target:
        left_volume = bins.get(lower - 1, -1.0)
        right_volume = bins.get(upper + 1, -1.0)
        if left_volume < 0 and right_volume < 0:
            break
        if right_volume > left_volume:
            upper += 1
            value += right_volume
        else:
            lower -= 1
            value += left_volume

    def level(index: int) -> float:
        return anchor * (base**index)

    return {
        "anchor": anchor,
        "bin_pct": bin_pct,
        "total_notional": total,
        "usable_rows": usable_rows,
        "poc_index": poc_index,
        "poc_price": anchor * (base ** (poc_index + 0.5)),
        "val_index": lower,
        "val_price": level(lower),
        "vah_index": upper + 1,
        "vah_price": level(upper + 1),
        "value_area_notional": value,
        "value_area_pct_actual": value / total if total else 0.0,
    }


def flow_stats(rows: Iterable[State]) -> dict[str, float]:
    buy = sum(row[5] for row in rows)
    sell = sum(row[6] for row in rows)
    total = buy + sell
    return {
        "buy_notional": buy,
        "sell_notional": sell,
        "total_notional": total,
        "imbalance": (buy - sell) / total if total > 0 else math.nan,
        "sell_share": sell / total if total > 0 else math.nan,
    }


def median_prev_notional(rows: Iterable[State]) -> float:
    values = [row[4] for row in rows if row[4] > 0]
    return statistics.median(values) if values else 0.0


def price_from_features_or_series(
    signal: dict[str, Any], series: Series
) -> float | None:
    if signal["signal_price"] and signal["signal_price"] > 0:
        return signal["signal_price"]
    prior = series.window(signal["detected_at"] - 30.0, signal["detected_at"] + 1e-6)
    return prior[-1][3] if prior else None


def first_target_or_stop(
    rows: Iterable[State], entry: float, target_pct: float, stop_pct: float
) -> str:
    target = entry * (1.0 + target_pct)
    stop = entry * (1.0 - stop_pct)
    for row in rows:
        hit_target = row[1] >= target
        hit_stop = row[2] <= stop
        if hit_target and hit_stop:
            return "ambiguous_same_bucket"
        if hit_target:
            return "target_first"
        if hit_stop:
            return "stop_first"
    return "neither"


def forward_metrics(
    series: Series,
    start: float,
    entry: float,
    horizon_seconds: float,
) -> dict[str, Any]:
    rows = series.window(start, start + horizon_seconds)
    if not rows:
        return {
            "complete": False,
            "return_pct": None,
            "mfe_pct": None,
            "mae_pct": None,
            "path": None,
        }
    closes = [row[3] for row in rows]
    highs = [row[1] for row in rows]
    lows = [row[2] for row in rows]
    return {
        "complete": rows[-1][0] >= start + horizon_seconds - 15.0,
        "return_pct": (closes[-1] / entry - 1.0) * 100.0,
        "mfe_pct": (max(highs) / entry - 1.0) * 100.0,
        "mae_pct": (min(lows) / entry - 1.0) * 100.0,
        "path": first_target_or_stop(rows, entry, 0.003, 0.002),
    }


@dataclass(frozen=True, slots=True)
class SameExitConfig:
    """The live candle-exit parameters used for both entry cohorts."""

    grace_bars: int = 1
    decision_profit_pct: float = 0.001
    recovery_profit_pct: float = 0.0088
    fee_rate: float = 0.0005
    notional_usdt: float = 100.0


def mark_at_or_near(series: Series, timestamp: float) -> tuple[float, float] | None:
    """Return the closest exported mark around a wall-clock exit time."""
    after = series.at_or_after(timestamp)
    if after is not None and after[0] <= timestamp + 60.0:
        return after[0], after[3]
    before = series.at_or_before(timestamp)
    if before is not None:
        return before[0], before[3]
    return None


def make_same_exit_trade(
    event: dict[str, Any],
    *,
    exit_epoch: float | None,
    exit_price: float | None,
    exit_reason: str,
    config: SameExitConfig,
    marked_price: float | None = None,
) -> dict[str, Any]:
    entry_price = float(event["entry_price"])
    entry_epoch = float(event["entry_epoch"])
    trade: dict[str, Any] = {
        "scenario": event["scenario"],
        "label": event.get("label"),
        "entry_mode": event["entry_mode"],
        "signal_id": event.get("signal_id"),
        "symbol": event["symbol"],
        "entry_epoch": entry_epoch,
        "entry_at": format_time(entry_epoch),
        "entry_price": entry_price,
        "exit_epoch": exit_epoch,
        "exit_at": format_time(exit_epoch),
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "closed": exit_epoch is not None and exit_price is not None,
        "marked_price": marked_price,
        "marked_return_pct": (
            (marked_price / entry_price - 1.0) * 100.0
            if marked_price is not None and entry_price > 0
            else None
        ),
    }
    if exit_epoch is None or exit_price is None or exit_price <= 0:
        trade.update(
            {
                "gross_return_pct": None,
                "net_return_pct": None,
                "gross_pnl_usdt": None,
                "fee_usdt": None,
                "net_pnl_usdt": None,
                "holding_minutes": None,
            }
        )
        return trade

    ratio = exit_price / entry_price
    gross_return = ratio - 1.0
    entry_fee = config.notional_usdt * config.fee_rate
    exit_fee = config.notional_usdt * ratio * config.fee_rate
    gross_pnl = config.notional_usdt * gross_return
    fee = entry_fee + exit_fee
    net_pnl = gross_pnl - fee
    trade.update(
        {
            "gross_return_pct": gross_return * 100.0,
            "net_return_pct": net_pnl / config.notional_usdt * 100.0,
            "gross_pnl_usdt": gross_pnl,
            "fee_usdt": fee,
            "net_pnl_usdt": net_pnl,
            "holding_minutes": (exit_epoch - entry_epoch) / 60.0,
        }
    )
    return trade


def simulate_same_exit(
    event: dict[str, Any],
    series: Series,
    candles: list[Candle15],
    config: SameExitConfig,
) -> dict[str, Any]:
    """Replay the live ``candle_15m + grace`` exit for one long entry.

    A bearish completed 15m candle is the trigger.  If the close is already
    at least +0.10% from entry, the live path sends a direct close.  Otherwise
    it posts a +0.88% recovery limit for one 15m bar; a post-trigger high that
    reaches that level is filled at the limit, and an untouched order falls
    back to a market close at the one-bar timeout mark.
    """
    entry_epoch = float(event["entry_epoch"])
    entry_price = float(event["entry_price"])
    if entry_price <= 0:
        return make_same_exit_trade(
            event,
            exit_epoch=None,
            exit_price=None,
            exit_reason="invalid_entry_price",
            config=config,
        )

    data_end = series.rows[-1][0] + 15.0 if series.rows else entry_epoch
    first_eligible_start = math.floor(entry_epoch / 900.0) * 900.0 + 900.0
    for candle in candles:
        if candle.start < first_eligible_start or candle.end > data_end + 1e-6:
            continue
        if candle.close >= candle.open:
            continue

        if candle.close >= entry_price * (1.0 + config.decision_profit_pct):
            return make_same_exit_trade(
                event,
                exit_epoch=candle.end,
                exit_price=candle.close,
                exit_reason="candle_15m_bearish",
                config=config,
            )

        if config.grace_bars <= 0 or config.recovery_profit_pct <= 0:
            return make_same_exit_trade(
                event,
                exit_epoch=candle.end,
                exit_price=candle.close,
                exit_reason="candle_15m_bearish",
                config=config,
            )

        recovery_price = entry_price * (1.0 + config.recovery_profit_pct)
        timeout = candle.end + 900.0 * config.grace_bars
        for row in series.window(candle.end + 1e-6, timeout + 1e-6):
            if row[1] >= recovery_price:
                return make_same_exit_trade(
                    event,
                    exit_epoch=row[0],
                    exit_price=recovery_price,
                    exit_reason=f"candle_15m_bearish_grace_limit_{config.grace_bars}",
                    config=config,
                )

        mark = mark_at_or_near(series, timeout)
        if mark is not None:
            mark_time, mark_price = mark
            return make_same_exit_trade(
                event,
                exit_epoch=timeout,
                exit_price=mark_price,
                exit_reason=f"candle_15m_grace_timeout_{config.grace_bars}",
                config=config,
            )

    if series.rows:
        marked_price = series.rows[-1][3]
    else:
        marked_price = None
    return make_same_exit_trade(
        event,
        exit_epoch=None,
        exit_price=None,
        exit_reason="open_at_data_end",
        config=config,
        marked_price=marked_price,
    )


def build_entry_events(
    rows: list[dict[str, Any]],
    *,
    scenario: str,
    label: str,
    condition: str | None,
    entry_mode: str,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for row in rows:
        if row.get("status") != "ok":
            continue
        if condition is not None and not bool(row.get(condition)):
            continue
        if entry_mode == "signal":
            entry_epoch = row.get("signal_epoch")
            entry_price = row.get("signal_price")
        else:
            reclaim_at = row.get("reclaim_at")
            entry_epoch = parse_time(reclaim_at) if reclaim_at else None
            entry_price = row.get("reclaim_price")
        if entry_epoch is None or entry_price is None or entry_price <= 0:
            continue
        events.append(
            {
                "scenario": scenario,
                "label": label,
                "entry_mode": entry_mode,
                "signal_id": row.get("signal_id"),
                "symbol": row["symbol"],
                "entry_epoch": float(entry_epoch),
                "entry_price": float(entry_price),
            }
        )
    return sorted(events, key=lambda event: event["entry_epoch"])


def run_same_exit(
    events: list[dict[str, Any]],
    states: dict[str, Series],
    config: SameExitConfig,
    *,
    enforce_no_overlap: bool,
    candle_cache: dict[str, list[Candle15]] | None = None,
) -> dict[str, Any]:
    if candle_cache is None:
        candles_by_symbol = {
            symbol: closed_candles(series)
            for symbol, series in states.items()
            if any(event["symbol"] == symbol for event in events)
        }
    else:
        candles_by_symbol = candle_cache
    trades: list[dict[str, Any]] = []
    skipped_overlap = 0
    missing_states = 0
    active_until: dict[str, float] = {}
    for event in events:
        symbol = event["symbol"]
        if enforce_no_overlap and event["entry_epoch"] < active_until.get(
            symbol, float("-inf")
        ):
            skipped_overlap += 1
            continue
        series = states.get(symbol)
        candles = candles_by_symbol.get(symbol)
        if series is None or not series.rows or candles is None:
            missing_states += 1
            continue
        trade = simulate_same_exit(event, series, candles, config)
        trades.append(trade)
        if enforce_no_overlap:
            active_until[symbol] = (
                trade["exit_epoch"]
                if trade["closed"] and trade["exit_epoch"] is not None
                else float("inf")
            )
    return {
        "events": events,
        "trades": trades,
        "skipped_overlap": skipped_overlap,
        "missing_states": missing_states,
        "enforce_no_overlap": enforce_no_overlap,
    }


def summarize_same_exit_run(run: dict[str, Any]) -> dict[str, Any]:
    trades = run["trades"]
    closed = [trade for trade in trades if trade["closed"]]
    net_returns = [
        trade["net_return_pct"]
        for trade in closed
        if trade["net_return_pct"] is not None
    ]
    gross_returns = [
        trade["gross_return_pct"]
        for trade in closed
        if trade["gross_return_pct"] is not None
    ]
    holding = [
        trade["holding_minutes"]
        for trade in closed
        if trade["holding_minutes"] is not None
    ]
    reasons = Counter(trade["exit_reason"] for trade in closed)
    return {
        "scenario": (
            run["events"][0]["scenario"] if run["events"] else "empty"
        ),
        "label": run["events"][0]["label"] if run["events"] else "empty",
        "n_opportunities": len(run["events"]),
        "n_simulated_entries": len(trades),
        "n_skipped_overlap": run["skipped_overlap"],
        "n_missing_states": run["missing_states"],
        "n_closed": len(closed),
        "n_open_at_data_end": sum(not trade["closed"] for trade in trades),
        "closed_rate": len(closed) / len(trades) if trades else None,
        "avg_gross_return_pct": mean_or_none(gross_returns),
        "avg_net_return_pct": mean_or_none(net_returns),
        "median_net_return_pct": median_or_none(net_returns),
        "win_rate": (
            sum(value > 0 for value in net_returns) / len(net_returns)
            if net_returns
            else None
        ),
        "loss_rate": (
            sum(value <= 0 for value in net_returns) / len(net_returns)
            if net_returns
            else None
        ),
        "avg_holding_minutes": mean_or_none(holding),
        "total_gross_pnl_usdt_at_100": sum(
            trade["gross_pnl_usdt"] or 0.0 for trade in closed
        ),
        "total_fees_usdt_at_100": sum(
            trade["fee_usdt"] or 0.0 for trade in closed
        ),
        "total_net_pnl_usdt_at_100": sum(
            trade["net_pnl_usdt"] or 0.0 for trade in closed
        ),
        "exit_reason_counts": dict(sorted(reasons.items())),
    }


def write_same_exit_csv(path: Path, runs: list[dict[str, Any]]) -> None:
    fields = [
        "execution_mode",
        "scenario",
        "label",
        "entry_mode",
        "signal_id",
        "symbol",
        "entry_at",
        "entry_price",
        "exit_at",
        "exit_price",
        "exit_reason",
        "closed",
        "gross_return_pct",
        "net_return_pct",
        "gross_pnl_usdt",
        "fee_usdt",
        "net_pnl_usdt",
        "holding_minutes",
        "marked_price",
        "marked_return_pct",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for run in runs:
            mode = "non_overlapping" if run["enforce_no_overlap"] else "independent"
            for trade in run["trades"]:
                writer.writerow(
                    {
                        "execution_mode": mode,
                        **{
                            field: csv_value(trade.get(field))
                            for field in fields
                            if field != "execution_mode"
                        },
                    }
                )


def same_exit_delta(
    baseline: dict[str, Any], item: dict[str, Any]
) -> dict[str, Any]:
    def delta(key: str) -> float | None:
        left = item.get(key)
        right = baseline.get(key)
        if left is None or right is None:
            return None
        return left - right

    return {
        "scenario": item["scenario"],
        "label": item["label"],
        "delta_avg_net_return_pct": delta("avg_net_return_pct"),
        "delta_win_rate": delta("win_rate"),
        "delta_total_net_pnl_usdt_at_100": delta("total_net_pnl_usdt_at_100"),
    }


def analyze_signal(
    signal: dict[str, Any],
    series: Series | None,
    *,
    profile_minutes: int,
    early_profile_minutes: int,
    retest_start_minutes: int,
    retest_end_minutes: int,
    chain_end_minutes: int,
    bin_pct: float,
    value_area_pct: float,
    min_imbalance: float,
    touch_tolerance_pct: float,
    breach_tolerance_pct: float,
    absorption_before_buckets: int,
    absorption_after_buckets: int,
    absorption_sell_share: float,
    absorption_min_multiple: float,
    reclaim_buffer_pct: float,
) -> dict[str, Any]:
    t = signal["detected_at"]
    result: dict[str, Any] = {
        "signal_id": signal["signal_id"],
        "candidate_id": signal["candidate_id"],
        "symbol": signal["symbol"],
        "signal_at": format_time(t),
        "signal_epoch": t,
        "status": "ok",
        "profile_minutes": profile_minutes,
        "features_aggressive_imbalance": as_float(
            signal["features"].get("aggressive_imbalance")
        ),
        "features_breakout_distance_pct": as_float(
            signal["features"].get("breakout_distance_pct")
        ),
        "features_impulse_return_pct": as_float(
            signal["features"].get("impulse_return_pct")
        ),
    }
    if series is None:
        result["status"] = "missing_symbol_states"
        return result

    entry = price_from_features_or_series(signal, series)
    if entry is None or entry <= 0:
        result["status"] = "missing_signal_price"
        return result
    result["signal_price"] = entry

    profile_end = signal["impulse_start"]
    profile_start = profile_end - profile_minutes * 60.0
    previous_rows = series.window(profile_start, profile_end)
    prices = typical_prices(previous_rows)
    anchor = statistics.median(prices) if prices else 0.0
    previous = profile(previous_rows, anchor, bin_pct, value_area_pct)
    result["previous_profile_start"] = format_time(profile_start)
    result["previous_profile_end"] = format_time(profile_end)
    if previous is None:
        result["status"] = "missing_previous_profile"
        return result
    for key, value in previous.items():
        result[f"previous_{key}"] = value

    impulse_rows = series.window(signal["impulse_start"], t)
    impulse_flow = flow_stats(impulse_rows)
    result.update({f"impulse_{key}": value for key, value in impulse_flow.items()})
    feature_imbalance = result["features_aggressive_imbalance"]
    result["buy_imbalance"] = (
        impulse_flow["imbalance"] >= min_imbalance
        if not math.isnan(impulse_flow["imbalance"])
        else feature_imbalance is not None and feature_imbalance >= min_imbalance
    )
    result["breakout"] = entry >= previous["vah_price"]
    result["breakout_distance_from_vah_pct"] = (
        (entry / previous["vah_price"] - 1.0) * 100.0
        if previous["vah_price"]
        else None
    )

    early_rows = series.window(
        t, t + early_profile_minutes * 60.0
    )
    early_profile = profile(early_rows, anchor, bin_pct, value_area_pct)
    if early_profile:
        for key, value in early_profile.items():
            result[f"early_{key}"] = value
        result["poc_up"] = (
            early_profile["poc_index"] > previous["poc_index"]
        )
        result["value_up"] = (
            early_profile["vah_index"] > previous["vah_index"]
            and early_profile["val_index"] >= previous["val_index"]
        )
    else:
        result["poc_up"] = False
        result["value_up"] = False

    retest_start = t + retest_start_minutes * 60.0
    retest_end = t + retest_end_minutes * 60.0
    retest_rows = series.window(retest_start, retest_end)
    vah = previous["vah_price"]
    touch_rows = [
        row
        for row in retest_rows
        if row[2] <= vah * (1.0 + touch_tolerance_pct)
        and row[1] >= vah * (1.0 - touch_tolerance_pct)
    ]
    result["retest_observed"] = bool(touch_rows)
    result["retest_at"] = format_time(touch_rows[0][0]) if touch_rows else None
    if touch_rows:
        touch_time = touch_rows[0][0]
        touch_index = next(
            index for index, row in enumerate(retest_rows) if row[0] == touch_time
        )
        touch_window = retest_rows[
            max(0, touch_index - absorption_before_buckets) : min(
                len(retest_rows), touch_index + absorption_after_buckets
            )
        ]
        touch_low = min(row[2] for row in touch_window)
        touch_close = touch_rows[0][3]
        result["retest_low_pct_from_vah"] = (touch_low / vah - 1.0) * 100.0
        result["retest_hold"] = (
            touch_low >= vah * (1.0 - breach_tolerance_pct)
            and touch_close >= vah * (1.0 - breach_tolerance_pct)
        )
        absorption_flow = flow_stats(touch_window)
        result.update(
            {f"absorption_{key}": value for key, value in absorption_flow.items()}
        )
        baseline_notional = median_prev_notional(previous_rows)
        result["absorption_baseline_median_15s_notional"] = baseline_notional
        result["absorption_proxy"] = (
            result["retest_hold"]
            and absorption_flow["sell_share"] >= absorption_sell_share
            and (
                baseline_notional <= 0
                or absorption_flow["total_notional"]
                >= baseline_notional * absorption_min_multiple
            )
        )

        reclaim_rows = series.window(
            touch_time + 15.0, t + chain_end_minutes * 60.0
        )
        reclaim = None
        reclaim_level = touch_close * (1.0 + reclaim_buffer_pct)
        for row in reclaim_rows:
            if row[3] >= reclaim_level:
                reclaim = row
                break
        result["reclaim_at"] = format_time(reclaim[0]) if reclaim else None
        result["reclaim_price"] = reclaim[3] if reclaim else None
        result["reclaim"] = reclaim is not None
        if reclaim:
            reclaim_flow = flow_stats(
                series.window(reclaim[0] - 45.0, reclaim[0] + 15.0)
            )
            result.update(
                {f"reclaim_{key}": value for key, value in reclaim_flow.items()}
            )
            result["reclaim_buy_confirmed"] = reclaim_flow["imbalance"] >= 0.0
        else:
            result["reclaim_buy_confirmed"] = False
    else:
        result.update(
            {
                "retest_hold": False,
                "absorption_proxy": False,
                "reclaim": False,
                "reclaim_buy_confirmed": False,
                "retest_low_pct_from_vah": None,
                "retest_at": None,
                "reclaim_at": None,
                "reclaim_price": None,
            }
        )

    result["chain_complete"] = bool(
        series.rows and series.rows[-1][0] >= t + chain_end_minutes * 60.0 - 15.0
    )
    result["acceptance_chain"] = all(
        bool(result.get(key))
        for key in (
            "breakout",
            "buy_imbalance",
            "poc_up",
            "value_up",
            "retest_hold",
            "absorption_proxy",
            "reclaim",
        )
    ) and result["chain_complete"]

    for minutes in (5, 15, 30, 60):
        result[f"fwd_{minutes}m"] = forward_metrics(
            series, t, entry, minutes * 60.0
        )
    reclaim_price = result.get("reclaim_price")
    reclaim_at = result.get("reclaim_at")
    if reclaim_price and reclaim_at:
        reclaim_epoch = parse_time(reclaim_at)
        for minutes in (15, 30):
            result[f"reclaim_fwd_{minutes}m"] = forward_metrics(
                series, reclaim_epoch, reclaim_price, minutes * 60.0
            )
    else:
        result["reclaim_fwd_15m"] = {"complete": False}
        result["reclaim_fwd_30m"] = {"complete": False}
    return result


def load_actual_trades(
    fills_path: Path,
    orders_path: Path,
    intents_path: Path,
) -> dict[str, dict[str, Any]]:
    intents: dict[str, str] = {}
    with gzip.open(intents_path, "rt", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("intent_id") and row.get("candidate_id"):
                intents[row["intent_id"]] = row["candidate_id"]

    order_meta: dict[str, dict[str, Any]] = {}
    with gzip.open(orders_path, "rt", newline="") as handle:
        for row in csv.DictReader(handle):
            order_id = row.get("exchange_order_id")
            if not order_id:
                continue
            order_meta[order_id] = {
                "symbol": row.get("symbol"),
                "side": row.get("side"),
                "reduce_only": as_bool(row.get("reduce_only")),
                "candidate_id": intents.get(row.get("intent_id", "")),
                "created_at": parse_time(row["created_at"]),
            }

    fills_by_order: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with gzip.open(fills_path, "rt", newline="") as handle:
        for row in csv.DictReader(handle):
            order_id = row.get("order_id")
            if not order_id:
                continue
            fills_by_order[order_id].append(
                {
                    "symbol": row["symbol"],
                    "side": row["side"].upper(),
                    "price": as_float(row["price"]) or 0.0,
                    "quantity": as_float(row["quantity"]) or 0.0,
                    "realized_pnl": as_float(row["realized_pnl"]) or 0.0,
                    "fee": as_float(row["fee"]) or 0.0,
                    "trade_at": parse_time(row["trade_at"]),
                }
            )

    entries_by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    exit_fills: list[dict[str, Any]] = []
    for order_id, fills in fills_by_order.items():
        meta = order_meta.get(order_id, {})
        reduce_only = bool(meta.get("reduce_only"))
        for fill in fills:
            fill = {**fill, "order_id": order_id, "meta": meta}
            if fill["side"] == "BUY" and not reduce_only:
                candidate_id = meta.get("candidate_id")
                if candidate_id:
                    entries_by_candidate[candidate_id].append(fill)
            elif fill["side"] == "SELL" and reduce_only:
                exit_fills.append(fill)
    exit_fills.sort(key=lambda fill: fill["trade_at"])

    entry_groups = [
        (candidate_id, sorted(fills, key=lambda fill: fill["trade_at"]))
        for candidate_id, fills in entries_by_candidate.items()
    ]
    entry_groups.sort(key=lambda item: item[1][0]["trade_at"])
    entry_times_by_symbol: dict[str, list[float]] = defaultdict(list)
    for _candidate_id, fills in entry_groups:
        entry_times_by_symbol[fills[0]["symbol"]].append(fills[0]["trade_at"])

    trades: dict[str, dict[str, Any]] = {}
    for candidate_id, entry_fills in entry_groups:
        entry = entry_fills[0]
        symbol = entry["symbol"]
        entry_time = entry["trade_at"]
        entry_times = entry_times_by_symbol[symbol]
        next_times = [value for value in entry_times if value > entry_time]
        next_entry = min(next_times) if next_times else float("inf")
        related_exits = [
            fill
            for fill in exit_fills
            if fill["symbol"] == symbol
            and entry_time < fill["trade_at"] < next_entry
        ]
        existing = {
            "candidate_id": candidate_id,
            "symbol": symbol,
            "entry_time": entry_time,
            "entry_quantity": sum(fill["quantity"] for fill in entry_fills),
            "entry_notional": sum(
                fill["price"] * fill["quantity"] for fill in entry_fills
            ),
            "entry_fee": sum(fill["fee"] for fill in entry_fills),
            "exit_time": None,
            "exit_realized_pnl": 0.0,
            "exit_fee": 0.0,
            "exit_quantity": 0.0,
        }
        if related_exits:
            existing["exit_time"] = max(fill["trade_at"] for fill in related_exits)
            existing["exit_realized_pnl"] += sum(
                fill["realized_pnl"] for fill in related_exits
            )
            existing["exit_fee"] += sum(fill["fee"] for fill in related_exits)
            existing["exit_quantity"] += sum(
                fill["quantity"] for fill in related_exits
            )
        trades[candidate_id] = existing

    for trade in trades.values():
        trade["entry_price"] = (
            trade["entry_notional"] / trade["entry_quantity"]
            if trade["entry_quantity"]
            else None
        )
        trade["fee"] = trade["entry_fee"] + trade["exit_fee"]
        trade["net_pnl"] = trade["exit_realized_pnl"] - trade["fee"]
        trade["closed"] = bool(
            trade["exit_time"] is not None
            and trade["exit_quantity"] >= trade["entry_quantity"] * 0.999
        )
        trade["entry_at"] = format_time(trade["entry_time"])
        trade["exit_at"] = format_time(trade["exit_time"])
    return trades


def mean_or_none(values: list[float]) -> float | None:
    return statistics.mean(values) if values else None


def median_or_none(values: list[float]) -> float | None:
    return statistics.median(values) if values else None


def deduplicate_events(
    rows: list[dict[str, Any]], max_gap_seconds: float = 15.0 * 60.0
) -> list[dict[str, Any]]:
    """Keep the first signal in each short same-symbol event cluster."""
    selected: list[dict[str, Any]] = []
    last_by_symbol: dict[str, float] = {}
    for row in sorted(rows, key=lambda item: (item["symbol"], item["signal_epoch"])):
        last = last_by_symbol.get(row["symbol"])
        if last is None or row["signal_epoch"] - last > max_gap_seconds:
            selected.append(row)
            last_by_symbol[row["symbol"]] = row["signal_epoch"]
    return sorted(selected, key=lambda item: item["signal_epoch"])


def summarize(
    rows: list[dict[str, Any]],
    condition: str | None,
    actual_trades: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    selected = [
        row
        for row in rows
        if condition is None or bool(row.get(condition))
    ]
    fwd15 = [
        row["fwd_15m"]
        for row in selected
        if row.get("fwd_15m", {}).get("complete")
    ]
    fwd30 = [
        row["fwd_30m"]
        for row in selected
        if row.get("fwd_30m", {}).get("complete")
    ]
    reclaim_fwd15 = [
        row["reclaim_fwd_15m"]
        for row in selected
        if row.get("reclaim_fwd_15m", {}).get("complete")
    ]
    reclaim_fwd30 = [
        row["reclaim_fwd_30m"]
        for row in selected
        if row.get("reclaim_fwd_30m", {}).get("complete")
    ]
    actual = [
        actual_trades[row["candidate_id"]]
        for row in selected
        if row.get("candidate_id") in actual_trades
    ]

    def forward_summary(items: list[dict[str, Any]]) -> dict[str, Any]:
        paths = [item.get("path") for item in items]
        return {
            "n": len(items),
            "avg_return_pct": mean_or_none(
                [item["return_pct"] for item in items if item.get("return_pct") is not None]
            ),
            "median_return_pct": median_or_none(
                [item["return_pct"] for item in items if item.get("return_pct") is not None]
            ),
            "avg_mfe_pct": mean_or_none(
                [item["mfe_pct"] for item in items if item.get("mfe_pct") is not None]
            ),
            "avg_mae_pct": mean_or_none(
                [item["mae_pct"] for item in items if item.get("mae_pct") is not None]
            ),
            "target_first_rate": (
                sum(path == "target_first" for path in paths) / len(paths)
                if paths
                else None
            ),
            "stop_first_rate": (
                sum(path == "stop_first" for path in paths) / len(paths)
                if paths
                else None
            ),
        }

    return {
        "condition": condition or "all_signals",
        "n_signals": len(selected),
        "n_chain_complete": sum(bool(row.get("chain_complete")) for row in selected),
        "n_forward_15m": len(fwd15),
        "n_forward_30m": len(fwd30),
        "forward_15m": forward_summary(fwd15),
        "forward_30m": forward_summary(fwd30),
        "reclaim_forward_15m": forward_summary(reclaim_fwd15),
        "reclaim_forward_30m": forward_summary(reclaim_fwd30),
        "n_reclaim_entries": len(reclaim_fwd15),
        "n_actual_fills": len(actual),
        "n_actual_closed": sum(trade["closed"] for trade in actual),
        "actual_net_pnl": sum(trade["net_pnl"] for trade in actual),
        "actual_gross_realized_pnl": sum(
            trade["exit_realized_pnl"] for trade in actual
        ),
        "actual_fees": sum(trade["fee"] for trade in actual),
        "actual_win_rate": (
            sum(trade["net_pnl"] > 0 for trade in actual) / len(actual)
            if actual
            else None
        ),
    }


def csv_value(value: Any) -> Any:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return value


def write_signal_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "signal_id",
        "candidate_id",
        "symbol",
        "signal_at",
        "status",
        "signal_price",
        "previous_profile_start",
        "previous_profile_end",
        "previous_poc_price",
        "previous_val_price",
        "previous_vah_price",
        "breakout_distance_from_vah_pct",
        "features_aggressive_imbalance",
        "impulse_imbalance",
        "breakout",
        "buy_imbalance",
        "early_poc_price",
        "early_val_price",
        "early_vah_price",
        "poc_up",
        "value_up",
        "retest_observed",
        "retest_hold",
        "retest_at",
        "retest_low_pct_from_vah",
        "absorption_sell_share",
        "absorption_total_notional",
        "absorption_proxy",
        "reclaim",
        "reclaim_buy_confirmed",
        "reclaim_imbalance",
        "reclaim_at",
        "reclaim_price",
        "acceptance_chain",
        "strict_acceptance_chain",
        "stage_breakout",
        "stage_breakout_buy",
        "stage_breakout_buy_poc",
        "stage_breakout_buy_poc_value",
        "stage_retest_hold",
        "stage_absorption",
        "stage_acceptance_chain",
        "chain_complete",
        "actual_entry_at",
        "actual_entry_price",
        "actual_exit_at",
        "actual_net_pnl",
        "actual_closed",
    ]
    for minutes in (15, 30):
        fields.extend(
            [
                f"reclaim_fwd_{minutes}m_return_pct",
                f"reclaim_fwd_{minutes}m_mfe_pct",
                f"reclaim_fwd_{minutes}m_mae_pct",
                f"reclaim_fwd_{minutes}m_path",
                f"reclaim_fwd_{minutes}m_complete",
            ]
        )
    for minutes in (5, 15, 30, 60):
        fields.extend(
            [
                f"fwd_{minutes}m_return_pct",
                f"fwd_{minutes}m_mfe_pct",
                f"fwd_{minutes}m_mae_pct",
                f"fwd_{minutes}m_path",
                f"fwd_{minutes}m_complete",
            ]
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            output = dict(row)
            actual = output.pop("actual_trade", None) or {}
            output["actual_entry_at"] = actual.get("entry_at")
            output["actual_entry_price"] = actual.get("entry_price")
            output["actual_exit_at"] = actual.get("exit_at")
            output["actual_net_pnl"] = actual.get("net_pnl")
            output["actual_closed"] = actual.get("closed")
            for minutes in (5, 15, 30, 60):
                metrics = output.get(f"fwd_{minutes}m", {})
                for key in ("return_pct", "mfe_pct", "mae_pct", "path", "complete"):
                    output[f"fwd_{minutes}m_{key}"] = metrics.get(key)
            for minutes in (15, 30):
                metrics = output.get(f"reclaim_fwd_{minutes}m", {})
                for key in ("return_pct", "mfe_pct", "mae_pct", "path", "complete"):
                    output[f"reclaim_fwd_{minutes}m_{key}"] = metrics.get(key)
            writer.writerow({field: csv_value(output.get(field)) for field in fields})


def pct(value: float | None, digits: int = 2) -> str:
    return "n/a" if value is None else f"{value:.{digits}f}%"


def number(value: float | None, digits: int = 2) -> str:
    return "n/a" if value is None else f"{value:.{digits}f}"


def write_report(
    path: Path,
    report: dict[str, Any],
    rows: list[dict[str, Any]],
) -> None:
    funnel = report["funnel"]
    outcomes = report["sequential_outcomes"]
    same_exit = report["same_exit_comparison"]
    lines = [
        "# Breakout acceptance study",
        "",
        f"- Window: `{report['window']['start']}` to `{report['window']['end']}` UTC",
        f"- Entry signals analyzed: **{report['counts']['signals']}**",
        f"- Symbols: **{report['counts']['symbols']}**",
        f"- State rows: **{report['counts']['state_rows']}**",
        "",
        "## 结论摘要",
        "",
        "本报告把 `Previous VAH → 突破 → Buy Imbalance → POC 上移 → "
        "Value 上移 → 回踩 → Sell Absorption proxy → Reclaim` 作为一个可检验的事件链。",
        "其中 POC/Value 是由 15 秒成交额加权典型价重建的近似 profile；服务器没有逐笔成交价档，"
        "所以 Sell Absorption 不是盘口真吸收，而是“卖压占比高但价格守住 VAH”的可观测代理。",
        "",
        "`acceptance_chain` 在回踩和 reclaim 之后才成立，不能把它解释成原始信号时刻已经知道的过滤条件；"
        "它更接近延迟确认/重新入场条件。原始信号和链条各阶段的前瞻结果分开列出。",
        "",
        "## 事件链漏斗",
        "",
        "| 阶段 | 信号数 | 占全部入口信号 |",
        "|---|---:|---:|",
    ]
    for name, count in funnel.items():
        lines.append(f"| {name} | {count} | {count / report['counts']['signals']:.1%} |")
    lines.extend(
        [
            "",
            "## 前瞻表现（从原始信号价计算，未扣手续费/滑点）",
            "",
            "clean continuation 定义为 15 秒路径中先触及 +0.30%，再触及 -0.20% 之前；"
            "表中的 target/stop-first 是该路径标签。",
            "",
            "| 条件 | 信号数 | 15m完整样本 | 15m均值收益 | 15m MFE | 15m MAE | 先止盈率 | 先止损率 | 实际成交数 | 实盘净PnL |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for item in outcomes:
        fwd = item["forward_15m"]
        lines.append(
            "| {condition} | {n_signals} | {n_forward_15m} | {ret} | {mfe} | "
            "{mae} | {target} | {stop} | {fills} | {pnl} |".format(
                condition=item["condition"],
                n_signals=item["n_signals"],
                n_forward_15m=item["n_forward_15m"],
                ret=pct(fwd["avg_return_pct"]),
                mfe=pct(fwd["avg_mfe_pct"]),
                mae=pct(fwd["avg_mae_pct"]),
                target=(
                    "n/a"
                    if fwd["target_first_rate"] is None
                    else f"{fwd['target_first_rate']:.1%}"
                ),
                stop=(
                    "n/a"
                    if fwd["stop_first_rate"] is None
                    else f"{fwd['stop_first_rate']:.1%}"
                ),
                fills=item["n_actual_fills"],
                pnl=number(item["actual_net_pnl"]),
            )
        )
    outcome_by_condition = {
        item["condition"]: item for item in report["component_outcomes"]
    }
    lines.extend(
        [
            "",
            "## 延迟 reclaim 入场（无成本模拟）",
            "",
            "这里把 reclaim 收盘价当作确认后的入场价，只评估 reclaim 之后的路径；"
            "它不是当前实盘成交价，也不包含等待期间的机会成本。",
            "",
            "| 条件 | reclaim后15m样本 | 均值收益 | MFE | MAE | 先触及+0.30% | 先触及-0.20% |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for condition in ("reclaim", "acceptance_chain", "strict_acceptance_chain"):
        item = outcome_by_condition[condition]
        fwd = item["reclaim_forward_15m"]
        lines.append(
            "| {condition} | {n} | {ret} | {mfe} | {mae} | {target} | {stop} |".format(
                condition=condition,
                n=item["n_reclaim_entries"],
                ret=pct(fwd["avg_return_pct"]),
                mfe=pct(fwd["avg_mfe_pct"]),
                mae=pct(fwd["avg_mae_pct"]),
                target=(
                    "n/a"
                    if fwd["target_first_rate"] is None
                    else f"{fwd['target_first_rate']:.1%}"
                ),
                stop=(
                    "n/a"
                    if fwd["stop_first_rate"] is None
                    else f"{fwd['stop_first_rate']:.1%}"
                ),
            )
        )
    lines.extend(
        [
            "",
            "## 去重后的独立事件",
            "",
            "按同一交易对 15 分钟内只保留第一条信号，避免同一波行情重复触发造成样本膨胀。",
            "",
            "| 条件 | 去重事件数 | 原始信号价15m均值 | reclaim后15m均值 | reclaim后先止损率 |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for item in report["clustered_outcomes"]:
        fwd = item["forward_15m"]
        reclaim_fwd = item["reclaim_forward_15m"]
        lines.append(
            "| {condition} | {n} | {raw} | {reclaim} | {stop} |".format(
                condition=item["condition"],
                n=item["n_signals"],
                raw=pct(fwd["avg_return_pct"]),
                reclaim=pct(reclaim_fwd["avg_return_pct"]),
                stop=(
                    "n/a"
                    if reclaim_fwd["stop_first_rate"] is None
                    else f"{reclaim_fwd['stop_first_rate']:.1%}"
                ),
            )
        )
    lines.extend(
        [
            "",
            "## 同一退出条件对照（实盘 candle_15m）",
            "",
            "下面把所有入场方案放进同一套实盘退出：1 根 15m 阴线触发；若触发时相对入场价至少盈利 0.10%，直接平仓；否则挂入场价上方 0.88% 的回收限价，宽限 1 根 15m，未成交后市价平仓。统一按每笔 100 USDT、双边各 0.05% 手续费估算。主表按同一品种持仓未结束时跳过后续信号，避免重复计入同一波行情。",
            "",
            "这里的 `acceptance（事后筛选）` 是把完整链条回看成立后，仍按原始信号价入场；`acceptance 后 reclaim 入场` 才是等待回踩确认后再入场的可执行近似。两者都不能当作当前实盘已经执行过的真实成交。",
            "",
            "| 入场方案 | 机会数 | 模拟入场 | 重叠跳过 | 已平仓 | 平仓胜率 | 平均净收益/笔 | 累计净PnL（$100/笔） | 平均持仓 |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for item in same_exit["non_overlapping"]:
        lines.append(
            "| {label} | {opportunities} | {entries} | {skipped} | {closed} | {win} | {avg} | {pnl} | {holding} |".format(
                label=item["label"],
                opportunities=item["n_opportunities"],
                entries=item["n_simulated_entries"],
                skipped=item["n_skipped_overlap"],
                closed=item["n_closed"],
                win=(
                    "n/a"
                    if item["win_rate"] is None
                    else f"{item['win_rate']:.1%}"
                ),
                avg=pct(item["avg_net_return_pct"]),
                pnl=number(item["total_net_pnl_usdt_at_100"]),
                holding=(
                    "n/a"
                    if item["avg_holding_minutes"] is None
                    else f"{item['avg_holding_minutes']:.1f}m"
                ),
            )
        )
    lines.extend(
        [
            "",
            "### 相对原始 long 信号的变化",
            "",
            "| 筛选方案 | 平均净收益差 | 胜率差 | 累计净PnL差 |",
            "|---|---:|---:|---:|",
        ]
    )
    for item in same_exit["delta_vs_baseline_non_overlapping"]:
        lines.append(
            "| {label} | {avg} | {win} | {pnl} |".format(
                label=item["label"],
                avg=pct(item["delta_avg_net_return_pct"]),
                win=(
                    "n/a"
                    if item["delta_win_rate"] is None
                    else f"{item['delta_win_rate']:.1%}"
                ),
                pnl=number(item["delta_total_net_pnl_usdt_at_100"]),
            )
        )
    lines.extend(
        [
            "",
            "累计净PnL差会同时受到样本数变化影响；判断过滤是否有效，优先看平均净收益/笔、胜率和退出原因分布。允许样本重叠的独立计算结果保存在 JSON/CSV 中，用于敏感性核对。",
            "",
            "## 当前实盘基线",
            "",
            f"最近两天导出的 primary 实盘入口成交为 **{report['actual_baseline']['n_actual_fills']}** 笔，"
            f"其中已找到退出的为 **{report['actual_baseline']['n_actual_closed']}** 笔；"
            f"这些成交的已实现毛PnL为 **{number(report['actual_baseline']['actual_gross_realized_pnl'])} USDT**，"
            f"手续费为 **{number(report['actual_baseline']['actual_fees'])} USDT**，"
            f"成交口径净PnL为 **{number(report['actual_baseline']['actual_net_pnl'])} USDT**（未平仓样本仅计已发生手续费）。",
            "",
            "## 阈值与可执行含义",
            "",
            f"- Previous profile：突破前 {report['definition']['profile_minutes']} 分钟；Value Area {report['definition']['value_area_pct']:.0%}。",
            f"- Profile 分箱：相对价格 {report['definition']['bin_pct']:.2%}；主动买失衡阈值 {report['definition']['min_imbalance']:.2f}。",
            f"- 回踩：信号后 {report['definition']['retest_start_minutes']}–{report['definition']['retest_end_minutes']} 分钟内触及 VAH，允许触及误差 {report['definition']['touch_tolerance_pct']:.2%}，有效跌破容忍 {report['definition']['breach_tolerance_pct']:.2%}。",
            f"- Sell Absorption proxy：卖成交额占回踩窗口主动成交额至少 {report['definition']['absorption_sell_share']:.0%}，同时价格不有效跌破 VAH。",
            "",
            "详细逐信号字段见同目录的 `breakout_acceptance_signals.csv`；脚本为 `scripts/analyze_breakout_acceptance.py`。",
        ]
    )
    path.write_text("\n".join(lines) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--signals", type=Path, required=True)
    parser.add_argument("--states", type=Path, required=True)
    parser.add_argument("--fills", type=Path, required=True)
    parser.add_argument("--orders", type=Path, required=True)
    parser.add_argument("--intents", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--profile-minutes", type=int, default=60)
    parser.add_argument("--early-profile-minutes", type=int, default=5)
    parser.add_argument("--retest-start-minutes", type=int, default=5)
    parser.add_argument("--retest-end-minutes", type=int, default=20)
    parser.add_argument("--chain-end-minutes", type=int, default=30)
    parser.add_argument("--bin-pct", type=float, default=0.001)
    parser.add_argument("--value-area-pct", type=float, default=0.70)
    parser.add_argument("--min-imbalance", type=float, default=0.40)
    parser.add_argument("--touch-tolerance-pct", type=float, default=0.0015)
    parser.add_argument("--breach-tolerance-pct", type=float, default=0.0020)
    parser.add_argument("--absorption-before-buckets", type=int, default=4)
    parser.add_argument("--absorption-after-buckets", type=int, default=9)
    parser.add_argument("--absorption-sell-share", type=float, default=0.55)
    parser.add_argument("--absorption-min-multiple", type=float, default=1.0)
    parser.add_argument("--reclaim-buffer-pct", type=float, default=0.0005)
    parser.add_argument("--exit-grace-bars", type=int, default=1)
    parser.add_argument("--exit-decision-profit-pct", type=float, default=0.001)
    parser.add_argument("--exit-recovery-profit-pct", type=float, default=0.0088)
    parser.add_argument("--exit-fee-rate", type=float, default=0.0005)
    parser.add_argument("--exit-notional-usdt", type=float, default=100.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    signals = load_signals(args.signals)
    states = load_states(args.states)
    actual_trades = load_actual_trades(args.fills, args.orders, args.intents)
    analyzed: list[dict[str, Any]] = []
    for signal in signals:
        row = analyze_signal(
            signal,
            states.get(signal["symbol"]),
            profile_minutes=args.profile_minutes,
            early_profile_minutes=args.early_profile_minutes,
            retest_start_minutes=args.retest_start_minutes,
            retest_end_minutes=args.retest_end_minutes,
            chain_end_minutes=args.chain_end_minutes,
            bin_pct=args.bin_pct,
            value_area_pct=args.value_area_pct,
            min_imbalance=args.min_imbalance,
            touch_tolerance_pct=args.touch_tolerance_pct,
            breach_tolerance_pct=args.breach_tolerance_pct,
            absorption_before_buckets=args.absorption_before_buckets,
            absorption_after_buckets=args.absorption_after_buckets,
            absorption_sell_share=args.absorption_sell_share,
            absorption_min_multiple=args.absorption_min_multiple,
            reclaim_buffer_pct=args.reclaim_buffer_pct,
        )
        actual = actual_trades.get(row.get("candidate_id"))
        row["actual_trade"] = actual
        analyzed.append(row)

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    write_signal_csv(output_dir / "breakout_acceptance_signals.csv", analyzed)

    component_keys = [
        "breakout",
        "buy_imbalance",
        "poc_up",
        "value_up",
        "retest_hold",
        "absorption_proxy",
        "reclaim",
        "acceptance_chain",
        "strict_acceptance_chain",
    ]
    stage_definitions = [
        ("stage_breakout", "breakout", ("breakout",)),
        (
            "stage_breakout_buy",
            "breakout + buy imbalance",
            ("breakout", "buy_imbalance"),
        ),
        (
            "stage_breakout_buy_poc",
            "+ POC up",
            ("breakout", "buy_imbalance", "poc_up"),
        ),
        (
            "stage_breakout_buy_poc_value",
            "+ Value up",
            ("breakout", "buy_imbalance", "poc_up", "value_up"),
        ),
        (
            "stage_retest_hold",
            "+ retest hold",
            ("breakout", "buy_imbalance", "poc_up", "value_up", "retest_hold"),
        ),
        (
            "stage_absorption",
            "+ sell absorption proxy",
            (
                "breakout",
                "buy_imbalance",
                "poc_up",
                "value_up",
                "retest_hold",
                "absorption_proxy",
            ),
        ),
        (
            "stage_acceptance_chain",
            "acceptance_chain (+ reclaim)",
            (
                "breakout",
                "buy_imbalance",
                "poc_up",
                "value_up",
                "retest_hold",
                "absorption_proxy",
                "reclaim",
            ),
        ),
    ]
    for row in analyzed:
        for key, _label, requirements in stage_definitions:
            row[key] = all(bool(row.get(requirement)) for requirement in requirements)
        row["strict_acceptance_chain"] = bool(
            row.get("stage_acceptance_chain") and row.get("reclaim_buy_confirmed")
        )

    same_exit_config = SameExitConfig(
        grace_bars=args.exit_grace_bars,
        decision_profit_pct=args.exit_decision_profit_pct,
        recovery_profit_pct=args.exit_recovery_profit_pct,
        fee_rate=args.exit_fee_rate,
        notional_usdt=args.exit_notional_usdt,
    )
    same_exit_specs = [
        (
            "raw_signal",
            "原始 long 信号",
            None,
            "signal",
        ),
        (
            "breakout_stage",
            "完成 Previous VAH 突破",
            "stage_breakout",
            "signal",
        ),
        (
            "value_up_stage",
            "突破 + POC 上移 + Value 上移",
            "stage_breakout_buy_poc_value",
            "signal",
        ),
        (
            "acceptance_posthoc",
            "完整 acceptance（事后筛选，原信号入场）",
            "acceptance_chain",
            "signal",
        ),
        (
            "acceptance_reclaim",
            "完整 acceptance 后 reclaim 入场",
            "acceptance_chain",
            "reclaim",
        ),
        (
            "strict_reclaim",
            "严格 acceptance + reclaim 买方确认入场",
            "strict_acceptance_chain",
            "reclaim",
        ),
    ]
    same_exit_runs: list[dict[str, Any]] = []
    for scenario, label, condition, entry_mode in same_exit_specs:
        events = build_entry_events(
            analyzed,
            scenario=scenario,
            label=label,
            condition=condition,
            entry_mode=entry_mode,
        )
        for enforce_no_overlap in (False, True):
            run = run_same_exit(
                events,
                states,
                same_exit_config,
                enforce_no_overlap=enforce_no_overlap,
            )
            run["summary"] = summarize_same_exit_run(run)
            same_exit_runs.append(run)
    independent_same_exit = [
        run["summary"] for run in same_exit_runs if not run["enforce_no_overlap"]
    ]
    non_overlapping_same_exit = [
        run["summary"] for run in same_exit_runs if run["enforce_no_overlap"]
    ]
    baseline_same_exit = next(
        item
        for item in non_overlapping_same_exit
        if item["scenario"] == "raw_signal"
    )
    same_exit_comparison = {
        "config": {
            "exit_mode": "candle_15m",
            "candle_confirmation_count": 1,
            "grace_bars": same_exit_config.grace_bars,
            "decision_profit_pct": same_exit_config.decision_profit_pct,
            "recovery_profit_pct": same_exit_config.recovery_profit_pct,
            "fee_rate": same_exit_config.fee_rate,
            "notional_usdt_per_trade": same_exit_config.notional_usdt,
            "price_path_assumption": "15s high/close proxy; no order-book queue",
        },
        "baseline_scenario": "raw_signal",
        "independent": independent_same_exit,
        "non_overlapping": non_overlapping_same_exit,
        "delta_vs_baseline_non_overlapping": [
            same_exit_delta(baseline_same_exit, item)
            for item in non_overlapping_same_exit
            if item["scenario"] != "raw_signal"
        ],
    }
    write_same_exit_csv(
        output_dir / "same_exit_comparison_trades.csv", same_exit_runs
    )
    funnel = {
        label: sum(bool(row.get(key)) for row in analyzed)
        for key, label, _requirements in stage_definitions
    }
    funnel["strict chain (+ reclaim buy confirm)"] = sum(
        bool(row.get("strict_acceptance_chain")) for row in analyzed
    )
    component_outcomes = [
        summarize(analyzed, None, actual_trades),
        *[summarize(analyzed, key, actual_trades) for key in component_keys],
    ]
    sequential_outcomes = []
    for key, label, _requirements in stage_definitions:
        item = summarize(analyzed, key, actual_trades)
        item["condition"] = label
        sequential_outcomes.append(item)
    strict_item = summarize(analyzed, "strict_acceptance_chain", actual_trades)
    strict_item["condition"] = "strict chain (+ reclaim buy confirm)"
    sequential_outcomes.append(strict_item)
    clustered_rows = deduplicate_events(analyzed)
    clustered_outcomes = [
        summarize(clustered_rows, None, actual_trades),
    ]
    clustered_chain = summarize(clustered_rows, "acceptance_chain", actual_trades)
    clustered_chain["condition"] = "acceptance_chain"
    clustered_strict = summarize(
        clustered_rows, "strict_acceptance_chain", actual_trades
    )
    clustered_strict["condition"] = "strict_acceptance_chain"
    clustered_outcomes.extend([clustered_chain, clustered_strict])
    valid_rows = [row for row in analyzed if row.get("status") == "ok"]
    baseline = component_outcomes[0]
    epochs = [row["signal_epoch"] for row in analyzed]
    state_rows = sum(len(series.rows) for series in states.values())
    report = {
        "window": {
            "start": format_time(min(epochs)) if epochs else None,
            "end": format_time(max(epochs)) if epochs else None,
        },
        "counts": {
            "signals": len(analyzed),
            "valid_profile_rows": len(valid_rows),
            "symbols": len({row["symbol"] for row in analyzed}),
            "clustered_signals": len(clustered_rows),
            "state_symbols": len(states),
            "state_rows": state_rows,
        },
        "definition": {
            "profile_minutes": args.profile_minutes,
            "early_profile_minutes": args.early_profile_minutes,
            "retest_start_minutes": args.retest_start_minutes,
            "retest_end_minutes": args.retest_end_minutes,
            "chain_end_minutes": args.chain_end_minutes,
            "bin_pct": args.bin_pct,
            "value_area_pct": args.value_area_pct,
            "min_imbalance": args.min_imbalance,
            "touch_tolerance_pct": args.touch_tolerance_pct,
            "breach_tolerance_pct": args.breach_tolerance_pct,
            "absorption_before_buckets": args.absorption_before_buckets,
            "absorption_after_buckets": args.absorption_after_buckets,
            "absorption_sell_share": args.absorption_sell_share,
            "absorption_min_multiple": args.absorption_min_multiple,
            "reclaim_buffer_pct": args.reclaim_buffer_pct,
            "outcome_target_pct": 0.003,
            "outcome_stop_pct": 0.002,
        },
        "funnel": funnel,
        "component_outcomes": component_outcomes,
        "sequential_outcomes": sequential_outcomes,
        "outcomes": component_outcomes,
        "actual_baseline": baseline,
        "clustered_outcomes": clustered_outcomes,
        "same_exit_comparison": same_exit_comparison,
        "data_limitations": [
            "runtime_market_states_15s contains aggregates, not price-level aggTrades",
            "sell_absorption is a price-hold plus aggressive-sell-share proxy",
            "acceptance_chain uses post-signal information and is a delayed confirmation label",
            "recent signals may lack a complete 30-minute forward window",
            "actual trade net PnL includes fees; open positions have no exit PnL in the export window",
            "same-exit replay uses 15s OHLC states as a proxy for official 15m candles and limit fills",
        ],
    }
    (output_dir / "breakout_acceptance_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    )
    write_report(output_dir / "breakout_acceptance_report.md", report, analyzed)
    print(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
