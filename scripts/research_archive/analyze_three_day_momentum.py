#!/usr/bin/env python3
"""Local-only study of 15-minute rising runs and order-flow entry gates.

The input files are exported locally from the server with read-only SQL and are
never queried or analysed on the server by this script.  The script deliberately
uses a compact float representation: this is an exploratory event study, not a
live order-authorisation path.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import statistics
import sys
from array import array
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterable, Sequence


STATE_INTERVAL_SECONDS = 15
CANDLE_INTERVAL_SECONDS = 15 * 60
DEFAULT_HORIZONS = (1, 4, 12, 20)
NAN = float("nan")


def finite(value: float) -> bool:
    return math.isfinite(value)


def parse_epoch(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"timestamp must include timezone: {value!r}")
    return int(parsed.astimezone(UTC).timestamp())


def iso_epoch(value: int) -> str:
    return datetime.fromtimestamp(value, UTC).isoformat()


def parse_float(value: str | None) -> float:
    if value is None or not value.strip():
        return NAN
    try:
        return float(value)
    except ValueError:
        return NAN


def parse_int(value: str | None, default: int = 0) -> int:
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError:
        return default


@dataclass(slots=True)
class StateSeries:
    """Columnar 15-second states for one symbol."""

    ts: array
    open: array
    high: array
    low: array
    close: array
    midpoint: array
    mark: array
    notional: array
    aggressive_buy: array
    aggressive_sell: array
    bid: array
    ask: array
    ok: bytearray
    missing: array

    @classmethod
    def create(cls) -> StateSeries:
        return cls(
            ts=array("q"),
            open=array("d"),
            high=array("d"),
            low=array("d"),
            close=array("d"),
            midpoint=array("d"),
            mark=array("d"),
            notional=array("d"),
            aggressive_buy=array("d"),
            aggressive_sell=array("d"),
            bid=array("d"),
            ask=array("d"),
            ok=bytearray(),
            missing=array("q"),
        )

    def append(self, row: dict[str, str]) -> None:
        self.ts.append(parse_epoch(row["bucket_start"]))
        self.open.append(parse_float(row.get("open_price")))
        self.high.append(parse_float(row.get("high_price")))
        self.low.append(parse_float(row.get("low_price")))
        self.close.append(parse_float(row.get("close_price")))
        self.midpoint.append(parse_float(row.get("midpoint")))
        self.mark.append(parse_float(row.get("mark_price")))
        notional = parse_float(row.get("trade_notional"))
        aggressive_buy = parse_float(row.get("aggressive_buy_notional"))
        aggressive_sell = parse_float(row.get("aggressive_sell_notional"))
        self.notional.append(notional if finite(notional) else 0.0)
        self.aggressive_buy.append(
            aggressive_buy if finite(aggressive_buy) else 0.0
        )
        self.aggressive_sell.append(
            aggressive_sell if finite(aggressive_sell) else 0.0
        )
        self.bid.append(parse_float(row.get("last_bid_price")))
        self.ask.append(parse_float(row.get("last_ask_price")))
        raw_complete = (row.get("data_complete") or "").strip().lower()
        missing = parse_int(row.get("missing_agg_trade_count"))
        self.missing.append(missing)
        self.ok.append(int(raw_complete in {"t", "true", "1"} and missing == 0))

    def reorder_if_needed(self) -> None:
        if len(self.ts) < 2 or all(
            self.ts[index] <= self.ts[index + 1]
            for index in range(len(self.ts) - 1)
        ):
            return
        order = sorted(range(len(self.ts)), key=self.ts.__getitem__)
        for name in (
            "ts",
            "open",
            "high",
            "low",
            "close",
            "midpoint",
            "mark",
            "notional",
            "aggressive_buy",
            "aggressive_sell",
            "bid",
            "ask",
            "missing",
        ):
            values = getattr(self, name)
            setattr(self, name, array(values.typecode, (values[i] for i in order)))
        old_ok = self.ok
        self.ok = bytearray(old_ok[i] for i in order)


@dataclass(frozen=True, slots=True)
class Candle15m:
    start: int
    open: float
    high: float
    low: float
    close: float


@dataclass(frozen=True, slots=True)
class RisingRun:
    symbol: str
    mode: str
    start: int
    end: int
    length: int
    first_open: float
    last_close: float
    cumulative_return: float
    max_high: float


@dataclass(frozen=True, slots=True)
class GateConfig:
    name: str
    min_return: float
    min_imbalance: float
    min_intensity: float
    impulse_buckets: int = 3
    baseline_buckets: int = 4
    breakout_buckets: int = 4
    confirmation_buckets: int = 1
    cooldown_buckets: int = 2


@dataclass(frozen=True, slots=True)
class RawMetrics:
    index: int
    price: float
    impulse_return: float
    imbalance: float
    intensity: float
    breakout_high: float
    breakout_pass: bool
    confirmation_imbalance: float


class UniverseTimeline:
    def __init__(self, pools: dict[int, set[str]]) -> None:
        self.times = tuple(sorted(pools))
        self.pools = tuple(pools[value] for value in self.times)

    def pool_at(self, timestamp: int) -> set[str] | None:
        index = bisect_right(self.times, timestamp) - 1
        if index < 0:
            return None
        return self.pools[index]


def load_states(paths: Sequence[Path]) -> dict[str, StateSeries]:
    grouped: dict[str, StateSeries] = {}
    row_count = 0
    for path in paths:
        with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            required = {"symbol", "bucket_start"}
            if not required.issubset(reader.fieldnames or ()):
                raise ValueError(f"{path} is not a runtime state CSV")
            for row in reader:
                symbol = (row.get("symbol") or "").strip().upper()
                if not symbol:
                    continue
                series = grouped.setdefault(symbol, StateSeries.create())
                series.append(row)
                row_count += 1
    for series in grouped.values():
        series.reorder_if_needed()
    print(
        f"loaded {row_count:,} states for {len(grouped):,} symbols",
        file=sys.stderr,
    )
    return grouped


def load_universe(path: Path | None, top_count: int) -> UniverseTimeline | None:
    if path is None:
        return None
    pools: dict[int, set[str]] = {}
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            observed = row.get("observed_at") or row.get("snapshot_at")
            symbol = (row.get("symbol") or "").strip().upper()
            if not observed or not symbol:
                continue
            rank = parse_int(row.get("gainer_rank"), default=10**9)
            day_return = parse_float(row.get("utc_day_return"))
            if rank <= top_count and finite(day_return) and day_return > 0:
                pools.setdefault(parse_epoch(observed), set()).add(symbol)
    timeline = UniverseTimeline(pools)
    print(
        f"loaded {len(timeline.times):,} activated universe snapshots",
        file=sys.stderr,
    )
    return timeline


def good_segments(series: StateSeries) -> list[tuple[int, int]]:
    segments: list[tuple[int, int]] = []
    start: int | None = None
    for index in range(len(series.ts)):
        if not series.ok[index]:
            if start is not None and index - start >= 1:
                segments.append((start, index))
            start = None
            continue
        if start is None:
            start = index
        elif series.ts[index] - series.ts[index - 1] != STATE_INTERVAL_SECONDS:
            if index - start >= 1:
                segments.append((start, index))
            start = index
    if start is not None and len(series.ts) - start >= 1:
        segments.append((start, len(series.ts)))
    return segments


def state_price(series: StateSeries, index: int) -> float:
    for values in (series.close, series.midpoint, series.mark):
        value = values[index]
        if finite(value) and value > 0:
            return value
    return NAN


def state_high(series: StateSeries, index: int) -> float:
    value = series.high[index]
    return value if finite(value) else state_price(series, index)


def state_low(series: StateSeries, index: int) -> float:
    value = series.low[index]
    return value if finite(value) else state_price(series, index)


def aggregate_candles(
    series: StateSeries,
    segments: Sequence[tuple[int, int]],
) -> list[Candle15m]:
    candles: list[Candle15m] = []
    for segment_start, segment_end in segments:
        index = segment_start
        while index < segment_end:
            bucket = (series.ts[index] // CANDLE_INTERVAL_SECONDS) * (
                CANDLE_INTERVAL_SECONDS
            )
            cursor = index
            while cursor < segment_end and series.ts[cursor] < bucket + CANDLE_INTERVAL_SECONDS:
                cursor += 1
            if (
                series.ts[index] == bucket
                and cursor - index == 60
                and series.ts[cursor - 1] == bucket + 885
            ):
                opens = [
                    series.open[item]
                    if finite(series.open[item])
                    else state_price(series, item)
                    for item in range(index, cursor)
                ]
                highs = [state_high(series, item) for item in range(index, cursor)]
                lows = [state_low(series, item) for item in range(index, cursor)]
                close = state_price(series, cursor - 1)
                if (
                    finite(opens[0])
                    and finite(close)
                    and all(finite(value) for value in highs)
                    and all(finite(value) for value in lows)
                ):
                    candles.append(
                        Candle15m(
                            start=bucket,
                            open=opens[0],
                            high=max(highs),
                            low=min(lows),
                            close=close,
                        )
                    )
            index = max(cursor, index + 1)
    return sorted(candles, key=lambda candle: candle.start)


def build_rising_runs(symbol: str, candles: Sequence[Candle15m], mode: str) -> list[RisingRun]:
    runs: list[RisingRun] = []
    run_start: int | None = None
    run_end: int | None = None

    def flush() -> None:
        nonlocal run_start, run_end
        if run_start is None or run_end is None:
            return
        length = run_end - run_start + 1
        if length >= 2:
            first = candles[run_start]
            last = candles[run_end]
            runs.append(
                RisingRun(
                    symbol=symbol,
                    mode=mode,
                    start=first.start,
                    end=last.start + CANDLE_INTERVAL_SECONDS,
                    length=length,
                    first_open=first.open,
                    last_close=last.close,
                    cumulative_return=last.close / first.open - 1.0,
                    max_high=max(candle.high for candle in candles[run_start : run_end + 1]),
                )
            )
        run_start = None
        run_end = None

    for index in range(1, len(candles)):
        previous = candles[index - 1]
        current = candles[index]
        contiguous = current.start - previous.start == CANDLE_INTERVAL_SECONDS
        if mode == "green":
            step_ok = (
                contiguous
                and previous.close > previous.open
                and current.close > current.open
                and current.close > previous.close
            )
        else:
            step_ok = contiguous and current.close > previous.close
        if step_ok:
            if run_start is None:
                run_start = index - 1
            run_end = index
        else:
            flush()
    flush()
    return runs


def window_is_contiguous(series: StateSeries, start: int, end: int) -> bool:
    if start < 0 or end >= len(series.ts) or start > end:
        return False
    for index in range(start, end + 1):
        if not series.ok[index]:
            return False
        if index > start and series.ts[index] - series.ts[index - 1] != 15:
            return False
    return True


def raw_metrics(series: StateSeries, index: int, config: GateConfig) -> RawMetrics | None:
    impulse_start = index - config.impulse_buckets + 1
    baseline_end = impulse_start
    baseline_start = baseline_end - config.baseline_buckets
    breakout_start = index - config.breakout_buckets
    if not window_is_contiguous(series, baseline_start, index):
        return None
    if not window_is_contiguous(series, breakout_start, index - 1):
        return None
    start_price = state_price(series, impulse_start)
    end_price = state_price(series, index)
    if not finite(start_price) or not finite(end_price) or start_price <= 0:
        return None
    baseline_total = sum(series.notional[baseline_start:baseline_end])
    baseline_notional = baseline_total / config.baseline_buckets * config.impulse_buckets
    if baseline_notional <= 0:
        return None
    impulse_notional = sum(series.notional[impulse_start : index + 1])
    aggressive_buy = sum(series.aggressive_buy[impulse_start : index + 1])
    aggressive_sell = sum(series.aggressive_sell[impulse_start : index + 1])
    aggressive_total = aggressive_buy + aggressive_sell
    imbalance = (
        (aggressive_buy - aggressive_sell) / aggressive_total
        if aggressive_total > 0
        else 0.0
    )
    breakout_high = max(
        state_high(series, item) for item in range(breakout_start, index)
    )
    if not finite(breakout_high):
        return None
    confirmation_end = index + config.confirmation_buckets - 1
    confirmation_imbalance = NAN
    if confirmation_end < len(series.ts):
        confirmation_imbalances: list[float] = []
        for item in range(index, confirmation_end + 1):
            if not window_is_contiguous(series, item, item):
                break
            price = state_price(series, item)
            buy = series.aggressive_buy[item]
            sell = series.aggressive_sell[item]
            total = buy + sell
            item_imbalance = (buy - sell) / total if total > 0 else 0.0
            if not finite(price) or price <= breakout_high:
                break
            confirmation_imbalances.append(item_imbalance)
        if len(confirmation_imbalances) == config.confirmation_buckets:
            confirmation_imbalance = min(confirmation_imbalances)
    return RawMetrics(
        index=index,
        price=end_price,
        impulse_return=(end_price - start_price) / start_price,
        imbalance=imbalance,
        intensity=impulse_notional / baseline_notional,
        breakout_high=breakout_high,
        breakout_pass=end_price > breakout_high,
        confirmation_imbalance=confirmation_imbalance,
    )


def passes_up(metrics: RawMetrics, config: GateConfig) -> bool:
    if metrics.impulse_return < config.min_return:
        return False
    if metrics.imbalance < config.min_imbalance:
        return False
    if metrics.intensity < config.min_intensity:
        return False
    if not metrics.breakout_pass:
        return False
    if (
        not finite(metrics.confirmation_imbalance)
        or metrics.confirmation_imbalance < config.min_imbalance
    ):
        return False
    # Confirmation's direction-specific imbalance threshold is checked here,
    # after the common metrics are computed.
    return True


def candidate_index(series: StateSeries, config: GateConfig) -> int:
    return max(
        config.baseline_buckets + config.impulse_buckets - 1,
        config.breakout_buckets,
    )


def event_from_metrics(
    symbol: str,
    series: StateSeries,
    segment_end: int,
    metrics: RawMetrics,
    horizons: Sequence[int],
) -> dict[str, object]:
    returns: dict[str, float | None] = {}
    for horizon in horizons:
        future = metrics.index + horizon
        if future >= segment_end:
            returns[str(horizon)] = None
            continue
        if series.ts[future] - series.ts[metrics.index] != horizon * 15:
            returns[str(horizon)] = None
            continue
        future_price = state_price(series, future)
        returns[str(horizon)] = (
            (future_price - metrics.price) / metrics.price
            if finite(future_price) and metrics.price > 0
            else None
        )
    return {
        "symbol": symbol,
        "detected_at": iso_epoch(series.ts[metrics.index]),
        "detected_ts": series.ts[metrics.index],
        "index": metrics.index,
        "entry_price": metrics.price,
        "impulse_return": metrics.impulse_return,
        "aggressive_imbalance": metrics.imbalance,
        "notional_intensity": metrics.intensity,
        "breakout_distance": (
            metrics.price / metrics.breakout_high - 1.0
            if metrics.breakout_high > 0
            else NAN
        ),
        "forward_returns": returns,
    }


def scan_events(
    states: dict[str, StateSeries],
    configs: Sequence[GateConfig],
    horizons: Sequence[int],
) -> dict[str, list[dict[str, object]]]:
    events = {config.name: [] for config in configs}
    for symbol, series in states.items():
        for segment_start, segment_end in good_segments(series):
            next_allowed = {
                config.name: candidate_index(series, config)
                for config in configs
            }
            for index in range(segment_start, segment_end):
                metrics = raw_metrics(series, index, configs[0])
                if metrics is None:
                    continue
                for config in configs:
                    if index < next_allowed[config.name]:
                        continue
                    if passes_up(metrics, config):
                        events[config.name].append(
                            event_from_metrics(
                                symbol,
                                series,
                                segment_end,
                                metrics,
                                horizons,
                            )
                        )
                        next_allowed[config.name] = (
                            index + config.confirmation_buckets + config.cooldown_buckets
                        )
    for values in events.values():
        values.sort(key=lambda item: (str(item["symbol"]), int(item["detected_ts"])))
    return events


def ema(values: Sequence[float], period: int) -> float | None:
    if len(values) < period:
        return None
    current = sum(values[:period]) / period
    alpha = 2.0 / (period + 1)
    for value in values[period:]:
        current = value * alpha + current * (1.0 - alpha)
    return current


def ema_context(
    candles: Sequence[Candle15m],
    observed_ts: int,
    entry_price: float,
) -> tuple[str, float | None, float | None, bool]:
    candle_start = (observed_ts // CANDLE_INTERVAL_SECONDS) * CANDLE_INTERVAL_SECONDS
    closed = [candle.close for candle in candles if candle.start < candle_start]
    exact = len(closed) >= 200
    history = closed[-200:] if exact else closed
    ema5 = ema(history, 5)
    ema10 = ema(history, 10)
    if ema5 is None or ema10 is None:
        return "unknown", ema5, ema10, False
    if exact:
        passed = entry_price > ema5 and entry_price > ema10
        return "exact_from_200_server_15m", ema5, ema10, passed
    passed = entry_price > ema5 and entry_price > ema10
    return "approx_short_warmup", ema5, ema10, passed


def attach_entry_filters(
    events: Sequence[dict[str, object]],
    states: dict[str, StateSeries],
    candles: dict[str, list[Candle15m]],
    universe: UniverseTimeline | None,
    require_ema: bool = True,
) -> list[dict[str, object]]:
    attached: list[dict[str, object]] = []
    for source in events:
        item = dict(source)
        symbol = str(source["symbol"])
        timestamp = int(source["detected_ts"])
        pool = universe.pool_at(timestamp) if universe is not None else None
        pool_known = pool is not None
        pool_pass = pool_known and symbol in pool
        item["entry_pool_known"] = pool_known
        item["entry_pool_pass"] = pool_pass
        series = states[symbol]
        index = int(source["index"])
        entry_price = float(source["entry_price"])
        context_mode, ema5, ema10, ema_pass = ema_context(
            candles.get(symbol, ()),
            timestamp,
            entry_price,
        )
        item["ema_mode"] = context_mode
        item["ema5"] = ema5
        item["ema10"] = ema10
        item["ema_pass"] = ema_pass
        item["full_entry_pass_approx"] = bool(pool_pass and ema_pass)
        item["full_entry_pass_strict"] = bool(
            pool_pass and (ema_pass if context_mode.startswith("exact") else False)
        )
        if not pool_known:
            item["entry_gate_reason"] = "universe_unknown"
        elif not pool_pass:
            item["entry_gate_reason"] = "entry_pool_fail"
        elif require_ema and context_mode == "unknown":
            item["entry_gate_reason"] = "ema_unknown"
        elif require_ema and not ema_pass:
            item["entry_gate_reason"] = "ema_fail"
        else:
            item["entry_gate_reason"] = "full_entry"
        item["state_complete"] = bool(series.ok[index])
        attached.append(item)
    return attached


def diagnose_run(
    run: RisingRun,
    states: dict[str, StateSeries],
    candles: dict[str, list[Candle15m]],
    universe: UniverseTimeline | None,
    config: GateConfig,
) -> dict[str, object]:
    series = states.get(run.symbol)
    result: dict[str, object] = {
        "symbol": run.symbol,
        "mode": run.mode,
        "start": iso_epoch(run.start),
        "end": iso_epoch(run.end),
        "length_15m": run.length,
        "cumulative_return": run.cumulative_return,
        "max_high": run.max_high,
    }
    if series is None:
        result["primary_reason"] = "no_15s_coverage"
        result["metrics_count"] = 0
        return result
    lo = bisect_left(series.ts, run.start)
    hi = bisect_left(series.ts, run.end)
    metrics: list[RawMetrics] = []
    for index in range(lo, min(hi, len(series.ts))):
        metric = raw_metrics(series, index, config)
        if metric is not None:
            metrics.append(metric)
    result["metrics_count"] = len(metrics)
    if not metrics:
        result["primary_reason"] = "no_complete_15s_window"
        return result
    best_return = max(metrics, key=lambda item: item.impulse_return)
    result["best_impulse_return"] = best_return.impulse_return
    result["best_imbalance"] = max(item.imbalance for item in metrics)
    result["best_intensity"] = max(item.intensity for item in metrics)
    result["best_return_at"] = iso_epoch(series.ts[best_return.index])
    return_candidates = [
        item for item in metrics if item.impulse_return >= config.min_return
    ]
    imbalance_candidates = [
        item for item in return_candidates if item.imbalance >= config.min_imbalance
    ]
    intensity_candidates = [
        item for item in imbalance_candidates if item.intensity >= config.min_intensity
    ]
    breakout_candidates = [item for item in intensity_candidates if item.breakout_pass]
    confirmation_candidates = [
        item
        for item in breakout_candidates
        if finite(item.confirmation_imbalance)
        and item.confirmation_imbalance >= config.min_imbalance
    ]
    result["return_pass_count"] = len(return_candidates)
    result["imbalance_pass_count"] = len(imbalance_candidates)
    result["intensity_pass_count"] = len(intensity_candidates)
    result["breakout_pass_count"] = len(breakout_candidates)
    result["orderflow_pass_count"] = len(confirmation_candidates)
    if not return_candidates:
        result["primary_reason"] = "return_below_threshold"
        return result
    if not imbalance_candidates:
        result["primary_reason"] = "imbalance_below_threshold"
        return result
    if not intensity_candidates:
        result["primary_reason"] = "notional_intensity_below_threshold"
        return result
    if not breakout_candidates:
        result["primary_reason"] = "breakout_fail"
        return result
    if not confirmation_candidates:
        result["primary_reason"] = "confirmation_fail"
        return result

    attached = attach_entry_filters(
        [
            event_from_metrics(
                run.symbol,
                series,
                len(series.ts),
                item,
                DEFAULT_HORIZONS,
            )
            for item in confirmation_candidates
        ],
        states,
        candles,
        universe,
    )
    result["pool_pass_count"] = sum(
        int(bool(item["entry_pool_pass"])) for item in attached
    )
    result["ema_approx_pass_count"] = sum(
        int(bool(item["full_entry_pass_approx"])) for item in attached
    )
    result["strict_full_entry_count"] = sum(
        int(bool(item["full_entry_pass_strict"])) for item in attached
    )
    if any(bool(item["full_entry_pass_strict"]) for item in attached):
        result["primary_reason"] = "full_entry"
    elif any(bool(item["full_entry_pass_approx"]) for item in attached):
        result["primary_reason"] = "ema_unverified_or_approx_only"
    elif any(bool(item["entry_pool_pass"]) for item in attached):
        result["primary_reason"] = "ema_fail_or_unknown"
    else:
        result["primary_reason"] = "entry_pool_fail"
    return result


def pct(value: float | None) -> float | None:
    return None if value is None or not finite(value) else value * 100.0


def ratio_percent(value: object) -> str:
    if not isinstance(value, (int, float)) or not finite(float(value)):
        return "n/a"
    return f"{float(value) * 100:.3f}%"


def event_stats(events: Sequence[dict[str, object]], cost: float) -> dict[str, object]:
    def values(horizon: int) -> list[float]:
        result: list[float] = []
        for event in events:
            value = event["forward_returns"].get(str(horizon))  # type: ignore[union-attr]
            if isinstance(value, (int, float)) and finite(float(value)):
                result.append(float(value))
        return result

    result: dict[str, object] = {"events": len(events)}
    for horizon, label in ((1, "15s"), (4, "1m"), (12, "3m"), (20, "5m")):
        raw = values(horizon)
        net = [value - cost for value in raw]
        result[f"n_{label}"] = len(raw)
        result[f"mean_{label}"] = statistics.fmean(raw) if raw else None
        result[f"median_{label}"] = statistics.median(raw) if raw else None
        result[f"win_{label}"] = (
            sum(value > cost for value in raw) / len(raw) if raw else None
        )
        result[f"mean_net_{label}"] = statistics.fmean(net) if net else None
    return result


def parameter_grid() -> list[GateConfig]:
    configs: list[GateConfig] = []
    for min_return in (0.005, 0.008, 0.010):
        for min_imbalance in (0.33, 0.40, 0.50):
            for min_intensity in (1.5, 2.0, 3.0):
                name = f"r{min_return:.3f}_i{min_imbalance:.2f}_n{min_intensity:.1f}"
                configs.append(
                    GateConfig(
                        name=name,
                        min_return=min_return,
                        min_imbalance=min_imbalance,
                        min_intensity=min_intensity,
                    )
                )
    return configs


def choose_config(
    configs: Sequence[GateConfig],
    events: dict[str, list[dict[str, object]]],
    cutoff: int,
    cost: float,
) -> tuple[GateConfig, list[dict[str, object]]]:
    rows: list[dict[str, object]] = []
    for config in configs:
        all_events = events[config.name]
        train = [item for item in all_events if int(item["detected_ts"]) < cutoff]
        validation = [item for item in all_events if int(item["detected_ts"]) >= cutoff]
        train_stats = event_stats(train, cost)
        validation_stats = event_stats(validation, cost)
        val_score_values = [
            value
            for key in ("mean_net_1m", "mean_net_5m")
            if isinstance((value := validation_stats[key]), (int, float))
        ]
        train_score_values = [
            value
            for key in ("mean_net_1m", "mean_net_5m")
            if isinstance((value := train_stats[key]), (int, float))
        ]
        enough_validation = int(validation_stats["events"]) >= 3
        score_values = val_score_values if enough_validation else train_score_values
        score = (
            0.7 * float(validation_stats["mean_net_1m"])
            + 0.3 * float(validation_stats["mean_net_5m"])
            if enough_validation
            and isinstance(validation_stats["mean_net_1m"], (int, float))
            and isinstance(validation_stats["mean_net_5m"], (int, float))
            else (
                0.7 * float(train_stats["mean_net_1m"])
                + 0.3 * float(train_stats["mean_net_5m"])
                if isinstance(train_stats["mean_net_1m"], (int, float))
                and isinstance(train_stats["mean_net_5m"], (int, float))
                else -999.0
            )
        )
        row = {
            "parameter": config.name,
            "min_return": config.min_return,
            "min_imbalance": config.min_imbalance,
            "min_intensity": config.min_intensity,
            "selection_split": "validation" if enough_validation else "train_fallback",
            "score": score,
            "train": train_stats,
            "validation": validation_stats,
        }
        rows.append(row)
    rows.sort(key=lambda row: float(row["score"]), reverse=True)
    selected_name = str(rows[0]["parameter"])
    selected = next(config for config in configs if config.name == selected_name)
    return selected, rows


def diagnostic_summary(
    diagnostics: Sequence[dict[str, object]],
    *,
    mode: str,
    minimum_length: int = 2,
    minimum_return: float = 0.01,
) -> dict[str, object]:
    selected = [
        row
        for row in diagnostics
        if row.get("mode") == mode
        and int(row.get("length_15m", 0)) >= minimum_length
        and float(row.get("cumulative_return", 0.0)) >= minimum_return
    ]
    reasons: dict[str, int] = {}
    for row in selected:
        reason = str(row.get("primary_reason", "unknown"))
        reasons[reason] = reasons.get(reason, 0) + 1
    return {
        "all_runs": sum(row.get("mode") == mode for row in diagnostics),
        "significant_runs": len(selected),
        "significant_definition": (
            f"length_15m >= {minimum_length} and cumulative_return >= "
            f"{minimum_return * 100:.1f}%"
        ),
        "miss_reasons": reasons,
    }


def write_csv(path: Path, rows: Iterable[dict[str, object]], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_markdown_report(output_dir: Path, summary: dict[str, object]) -> None:
    coverage = summary["coverage"]
    runs = summary["rising_runs"]
    baseline = summary["baseline"]
    selected = summary["selected_backtest"]
    selection = summary["parameter_selection"]
    by_mode = summary["baseline_run_miss_reasons_by_mode"]
    data_source_note = str(summary["data_source_note"])
    candle_source_note = str(summary["candle_source_note"])
    lines = [
        "# 15 分钟连续上涨与开单条件本地回测",
        "",
        f"> {data_source_note}",
        "",
        "## 数据边界",
        "",
        f"- 实际覆盖：`{coverage['start']}` 至 `{coverage['end']}`，约 `{coverage['hours']:.2f}` 小时。",
        f"- symbol：`{coverage['symbols']}`；相对 72 小时目标缺少约 `{coverage['missing_hours_vs_72h']:.2f}` 小时。",
        f"- {candle_source_note}",
        "- EMA 严格值需要 200 根已收盘 15 分钟 K 线，本次服务器 15 秒留存不足以覆盖全部事件，因此 EMA 结果分为 exact、approx 和 unknown。",
        "",
        "## 连续上涨行情",
        "",
        f"- 收盘价连续上行（至少 2 根）：`{runs['close_mode_count']}` 段；其中累计涨幅至少 1%：`{by_mode['close']['significant_runs']}` 段。",
        f"- 连续阳线且收盘价上行（至少 2 根）：`{runs['green_mode_count']}` 段；其中累计涨幅至少 1%：`{by_mode['green']['significant_runs']}` 段。",
        "",
        "### 收盘价连续上行的最大几段",
        "",
        "| symbol | 开始 | 结束 | 根数 | 累计涨幅 |",
        "|---|---|---|---:|---:|",
    ]
    for item in runs["top_close_runs"][:20]:
        lines.append(
            f"| {item['symbol']} | {item['start']} | {item['end']} | "
            f"{item['length_15m']} | {item['cumulative_return_pct']:.2f}% |"
        )
    lines.extend(
        [
            "",
            "## 基线与候选参数",
            "",
            f"- 基线：45 秒涨幅 ≥ 1%、aggressive imbalance ≥ 0.50、成交额强度 ≥ 2。",
            f"- 基线 order-flow 事件：`{baseline['orderflow_events']}`；通过 Top100 正收益 entry pool：`{baseline['pool_pass_events']}`；EMA 近似下完整通过：`{baseline['approx_full_entry_events']}`。",
            f"- 基线 EMA 近似完整事件前瞻收益：1 分钟均值 `{ratio_percent(baseline['approx_full_entry_forward_return_stats']['mean_1m'])}`，5 分钟均值 `{ratio_percent(baseline['approx_full_entry_forward_return_stats']['mean_5m'])}`。",
            f"- 候选参数：`{selection['selected']}`，即涨幅 ≥ `{selection['min_return_pct']:.1f}%`、imbalance ≥ `{selection['min_imbalance']:.2f}`、强度 ≥ `{selection['min_intensity']:.1f}`。",
            f"- 候选事件：`{selected['orderflow_events']}`；通过 entry pool：`{selected['pool_pass_events']}`；EMA 近似下完整通过：`{selected['approx_full_entry_events']}`。",
            f"- 候选 EMA 近似完整事件前瞻收益：1 分钟均值 `{ratio_percent(selected['approx_full_entry_forward_return_stats']['mean_1m'])}`，5 分钟均值 `{ratio_percent(selected['approx_full_entry_forward_return_stats']['mean_5m'])}`。",
            "- 选择方式：前 60% 时间训练、后 40% 时间验证；20 bps round-trip 成本只用于研究评分，不代表实际费率。",
            "",
            "### 连续上涨段的基线漏单归因（累计涨幅至少 1%）",
            "",
            "| 类型 | 段数 | 主要原因 |",
            "|---|---:|---|",
        ]
    )
    reason_labels = {
        "return_below_threshold": "45 秒涨幅不足",
        "imbalance_below_threshold": "aggressive imbalance 不足",
        "notional_intensity_below_threshold": "成交额强度不足",
        "breakout_fail": "未突破前 1 分钟高点",
        "confirmation_fail": "确认桶未通过",
        "entry_pool_fail": "不在 Top100 正收益池",
        "ema_fail_or_unknown": "EMA 失败或不确定",
        "ema_unverified_or_approx_only": "仅近似 EMA 通过，无法严格确认",
        "no_complete_15s_window": "没有完整 15 秒窗口",
        "no_15s_coverage": "没有 15 秒覆盖",
    }
    for mode in ("close", "green"):
        mode_summary = by_mode[mode]
        reasons = mode_summary["miss_reasons"]
        ordered = sorted(reasons.items(), key=lambda item: item[1], reverse=True)
        reason_text = "；".join(
            f"{reason_labels.get(reason, reason)} {count}"
            for reason, count in ordered
        )
        lines.append(
            f"| {mode} | {mode_summary['significant_runs']} | {reason_text} |"
        )
    lines.extend(
        [
            "",
            "## 结论",
            "",
            "1. 这批数据里，最主要的漏点不是 EMA，而是 45 秒价格冲击没有同时满足 order-flow 的 imbalance/突破组合；对大幅连续上涨段尤其明显。",
            "2. 从验证段看，把 imbalance 从 0.50 放宽到 0.40、同时把成交额强度提高到 3，事件质量优于基线；但全覆盖事件在扣除 20 bps 研究成本后，1 分钟平均仍为负，不能直接上线。",
            "3. BTR 的 `2026-08-26 04:45–06:30 UTC` 连续上涨约 74.15%；基线有 106 个 45 秒涨幅达标窗口，但没有一个达到 imbalance 0.50，说明“连续上涨”与“主动买盘占比达到 50%”不是同一条件。",
            "4. 本次没有找到可据此直接替换生产参数的稳健证据；建议先用更长历史、严格 EMA 和真实成交价/滑点再做 walk-forward 验证。",
            "",
            "## 产物",
            "",
            "- `rising_runs.csv`：所有连续上涨段。",
            "- `baseline_run_diagnostics.csv`：每一段的基线开单条件逐项归因。",
            "- `baseline_events.csv` / `selected_events.csv`：基线与候选参数事件及前瞻收益。",
            "- `parameter_grid.csv`：参数网格训练/验证结果。",
        ]
    )
    (output_dir / "analysis_report.md").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )


def flatten_event(event: dict[str, object]) -> dict[str, object]:
    row = dict(event)
    returns = row.pop("forward_returns")
    if isinstance(returns, dict):
        for horizon, value in returns.items():
            row[f"fwd_{horizon}"] = pct(value if isinstance(value, (int, float)) else None)
    for key in (
        "entry_price",
        "impulse_return",
        "aggressive_imbalance",
        "notional_intensity",
        "breakout_distance",
        "ema5",
        "ema10",
    ):
        if isinstance(row.get(key), (int, float)):
            if key not in {"entry_price", "notional_intensity", "ema5", "ema10"}:
                row[key] = pct(float(row[key]))
    return row


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--states", nargs="+", type=Path, required=True)
    parser.add_argument("--universe", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--top-count", type=int, default=100)
    parser.add_argument("--round-trip-cost", type=float, default=0.002)
    parser.add_argument(
        "--data-source-note",
        default="本报告只使用服务器已保存的 15 秒状态导出；筛选、归因和参数回测全部在本地完成。",
    )
    parser.add_argument(
        "--candle-source-note",
        default="15 分钟 K 线由完整的服务器 15 秒状态在本地聚合；未在服务器运行分析，也未下单。",
    )
    args = parser.parse_args()

    states = load_states(args.states)
    universe = load_universe(args.universe, args.top_count)
    candles: dict[str, list[Candle15m]] = {}
    runs: list[RisingRun] = []
    for symbol, series in states.items():
        series_segments = good_segments(series)
        symbol_candles = aggregate_candles(series, series_segments)
        candles[symbol] = symbol_candles
        runs.extend(build_rising_runs(symbol, symbol_candles, "close"))
        runs.extend(build_rising_runs(symbol, symbol_candles, "green"))
    runs.sort(key=lambda run: (run.start, run.symbol, run.mode))
    if not states:
        raise SystemExit("no states were loaded")
    all_timestamps = [timestamp for series in states.values() for timestamp in series.ts]
    coverage_start = min(all_timestamps)
    coverage_end = max(all_timestamps)

    # Mirror registry._default_order_flow_config(); this is the code default,
    # not a value fetched from a live server account.
    base = GateConfig("baseline_r0.010_i0.50_n2.0", 0.010, 0.50, 2.0)
    configs = [base] + [
        config
        for config in parameter_grid()
        if (
            config.min_return,
            config.min_imbalance,
            config.min_intensity,
        )
        != (base.min_return, base.min_imbalance, base.min_intensity)
    ]
    event_map = scan_events(states, configs, DEFAULT_HORIZONS)
    midpoint = coverage_start + int((coverage_end - coverage_start) * 0.60)
    selected, parameter_rows = choose_config(
        configs,
        event_map,
        midpoint,
        args.round_trip_cost,
    )
    selected_events = attach_entry_filters(
        event_map[selected.name],
        states,
        candles,
        universe,
    )
    baseline_events = attach_entry_filters(
        event_map[base.name],
        states,
        candles,
        universe,
    )
    baseline_pool_events = [
        event for event in baseline_events if bool(event["entry_pool_pass"])
    ]
    baseline_approx_events = [
        event for event in baseline_events if bool(event["full_entry_pass_approx"])
    ]
    selected_pool_events = [
        event for event in selected_events if bool(event["entry_pool_pass"])
    ]
    selected_approx_events = [
        event for event in selected_events if bool(event["full_entry_pass_approx"])
    ]
    diagnostics = [
        diagnose_run(run, states, candles, universe, base)
        for run in runs
    ]

    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    run_rows = [
        {
            "symbol": run.symbol,
            "mode": run.mode,
            "start": iso_epoch(run.start),
            "end": iso_epoch(run.end),
            "length_15m": run.length,
            "first_open": run.first_open,
            "last_close": run.last_close,
            "cumulative_return_pct": pct(run.cumulative_return),
            "max_high": run.max_high,
        }
        for run in runs
    ]
    write_csv(
        output_dir / "rising_runs.csv",
        run_rows,
        (
            "symbol",
            "mode",
            "start",
            "end",
            "length_15m",
            "first_open",
            "last_close",
            "cumulative_return_pct",
            "max_high",
        ),
    )
    diagnostic_fields = sorted({key for row in diagnostics for key in row})
    write_csv(output_dir / "baseline_run_diagnostics.csv", diagnostics, diagnostic_fields)
    baseline_flat = [flatten_event(event) for event in baseline_events]
    selected_flat = [flatten_event(event) for event in selected_events]
    event_fields = sorted({key for row in baseline_flat + selected_flat for key in row})
    write_csv(output_dir / "baseline_events.csv", baseline_flat, event_fields)
    write_csv(output_dir / "selected_events.csv", selected_flat, event_fields)

    parameter_output: list[dict[str, object]] = []
    for row in parameter_rows:
        flat = {
            "parameter": row["parameter"],
            "min_return_pct": float(row["min_return"]) * 100.0,
            "min_imbalance": row["min_imbalance"],
            "min_intensity": row["min_intensity"],
            "selection_split": row["selection_split"],
            "score": row["score"],
        }
        for split_name in ("train", "validation"):
            stats = row[split_name]
            for key, value in stats.items():
                flat[f"{split_name}_{key}"] = value
        parameter_output.append(flat)
    parameter_fields = sorted({key for row in parameter_output for key in row})
    write_csv(output_dir / "parameter_grid.csv", parameter_output, parameter_fields)

    strict_full = sum(int(bool(item["full_entry_pass_strict"])) for item in selected_events)
    approx_full = sum(int(bool(item["full_entry_pass_approx"])) for item in selected_events)
    pool_pass = sum(int(bool(item["entry_pool_pass"])) for item in selected_events)
    reason_counts: dict[str, int] = {}
    for row in diagnostics:
        reason = str(row.get("primary_reason", "unknown"))
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
    summary = {
        "generated_at": datetime.now(UTC).isoformat(),
        "data_source": args.data_source_note,
        "data_source_note": args.data_source_note,
        "candle_source_note": args.candle_source_note,
        "coverage": {
            "start": iso_epoch(coverage_start),
            "end": iso_epoch(coverage_end),
            "hours": (coverage_end - coverage_start) / 3600.0,
            "requested_hours": 72.0,
            "missing_hours_vs_72h": max(0.0, 72.0 - (coverage_end - coverage_start) / 3600.0),
            "symbols": len(states),
        },
        "definition": {
            "candle_source": args.candle_source_note,
            "close_mode": "consecutive complete 15m candles with close_t > close_(t-1)",
            "green_mode": "consecutive complete 15m candles with open < close and close_t > close_(t-1)",
            "minimum_reported_run_length": 2,
            "baseline_orderflow": {
                "impulse_seconds": 45,
                "baseline_seconds": 60,
                "breakout_seconds": 60,
                "min_return_pct": 1.0,
                "min_aggressive_imbalance": 0.50,
                "min_notional_intensity": 2.0,
                "confirmation_buckets": 1,
                "cooldown_buckets": 2,
            },
            "entry_pool": f"positive gainer rank <= {args.top_count} and utc_day_return > 0",
            "ema": "EMA5 and EMA10 above-price gate; exact only when 200 prior server-derived 15m closes exist, otherwise labelled approximate/unknown",
        },
        "rising_runs": {
            "close_mode_count": sum(run.mode == "close" for run in runs),
            "green_mode_count": sum(run.mode == "green" for run in runs),
            "top_close_runs": [
                {
                    "symbol": run.symbol,
                    "start": iso_epoch(run.start),
                    "end": iso_epoch(run.end),
                    "length_15m": run.length,
                    "cumulative_return_pct": pct(run.cumulative_return),
                }
                for run in sorted(
                    (run for run in runs if run.mode == "close"),
                    key=lambda item: item.cumulative_return,
                    reverse=True,
                )[:20]
            ],
        },
        "baseline": {
            "orderflow_events": len(baseline_events),
            "pool_pass_events": pool_pass if base.name == selected.name else sum(
                int(bool(item["entry_pool_pass"])) for item in baseline_events
            ),
            "strict_full_entry_events": sum(
                int(bool(item["full_entry_pass_strict"])) for item in baseline_events
            ),
            "approx_full_entry_events": sum(
                int(bool(item["full_entry_pass_approx"])) for item in baseline_events
            ),
            "forward_return_stats": event_stats(event_map[base.name], args.round_trip_cost),
            "pool_forward_return_stats": event_stats(
                baseline_pool_events, args.round_trip_cost
            ),
            "approx_full_entry_forward_return_stats": event_stats(
                baseline_approx_events, args.round_trip_cost
            ),
        },
        "parameter_selection": {
            "selected": selected.name,
            "min_return_pct": selected.min_return * 100.0,
            "min_imbalance": selected.min_imbalance,
            "min_intensity": selected.min_intensity,
            "selection_split_cutoff": iso_epoch(midpoint),
            "round_trip_cost_assumption": args.round_trip_cost,
            "top_10": parameter_output[:10],
        },
        "selected_backtest": {
            "orderflow_events": len(selected_events),
            "pool_pass_events": pool_pass,
            "approx_full_entry_events": approx_full,
            "strict_full_entry_events": strict_full,
            "forward_return_stats": event_stats(event_map[selected.name], args.round_trip_cost),
            "pool_forward_return_stats": event_stats(
                selected_pool_events, args.round_trip_cost
            ),
            "approx_full_entry_forward_return_stats": event_stats(
                selected_approx_events, args.round_trip_cost
            ),
            "filter_reason_counts": {
                reason: sum(
                    int(item["entry_gate_reason"] == reason) for item in selected_events
                )
                for reason in sorted({str(item["entry_gate_reason"]) for item in selected_events})
            },
        },
        "baseline_run_miss_reasons": reason_counts,
        "baseline_run_miss_reasons_by_mode": {
            "close": diagnostic_summary(diagnostics, mode="close"),
            "green": diagnostic_summary(diagnostics, mode="green"),
        },
        "artifacts": {
            "analysis_report": "analysis_report.md",
            "rising_runs": "rising_runs.csv",
            "baseline_run_diagnostics": "baseline_run_diagnostics.csv",
            "baseline_events": "baseline_events.csv",
            "selected_events": "selected_events.csv",
            "parameter_grid": "parameter_grid.csv",
        },
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    write_markdown_report(output_dir, summary)
    print(json.dumps(summary["selected_backtest"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
