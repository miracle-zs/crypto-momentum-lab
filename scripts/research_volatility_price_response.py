#!/usr/bin/env python3
# ruff: noqa: E501,E731
"""Research causal volatility and price-response features on 15s states.

The study intentionally keeps the existing event detector and execution
replay fixed.  It measures whether two families of *additional* information
are useful:

1. a same-horizon impulse return divided by pre-impulse historical volatility;
2. whether aggressive buying is producing efficient upward price progress.

This is a research report, not a production strategy change.  The candidate
universe is the relaxed live long-only event universe after the lowest values
of the current six entry filters and the causal local Top-10 proxy.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import multiprocessing as mp
import os
import statistics
import sys
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import optimize_local_live_constrained as optimizer  # noqa: E402
import optimize_volume_feature_joint_fast as fast  # noqa: E402
from analyze_breakout_acceptance import SameExitConfig  # noqa: E402
from optimize_research_orderflow import (  # noqa: E402
    BUCKET,
    Split,
    build_pool,
    load_states,
    split_contiguous_states,
)

from crypto_momentum_lab.domain.market.models import MarketState15s  # noqa: E402
from crypto_momentum_lab.strategies.order_flow_impulse.event_study import (  # noqa: E402
    OrderFlowDirection,
)

HORIZONS = (1, 4, 8, 16, 32)
IMPULSE_WINDOWS = (2, 3, 4)
CONFIRMATIONS = (1, 2, 3)
VOL_LOOKBACK_BUCKETS = 240  # 60 minutes of history at 15 seconds per bucket.
VOL_MIN_SAMPLES = 32
FEE_RATE = 0.0005
_POOL_CONTEXT: dict[str, Any] | None = None


@dataclass(slots=True)
class FeatureRecord:
    event: Any
    impulse_window: int
    confirmation: int
    confirmation_min: Decimal | None
    top10_allowed: bool
    historical_vol: float | None
    shock_z: float | None
    close_location: float | None
    pullback_z: float | None
    price_efficiency: float | None
    buy_pressure_persistence: float | None
    future_returns_pct: dict[int, float | None]
    simulation: optimizer.SimulatedEvent
    entry_excluded: bool

    @property
    def trade(self) -> dict[str, Any] | None:
        return self.simulation.trade

    @property
    def detected_epoch(self) -> float:
        return self.event.detected_at.timestamp()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-root",
        type=Path,
        default=Path("server_exports/cml-research-data-20260908-170354/parquet"),
    )
    parser.add_argument(
        "--live-signals",
        type=Path,
        default=Path(
            "server_exports/cml-live-current-latest-20260905/"
            "live_strategy_signals.csv.gz"
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(
            "server_exports/cml-research-data-20260908-170354/"
            "volatility-price-response-study-20260909"
        ),
    )
    parser.add_argument("--environment", default="research")
    parser.add_argument("--top-count", type=int, default=10)
    parser.add_argument("--fee-rate", type=float, default=FEE_RATE)
    parser.add_argument(
        "--workers",
        type=int,
        default=min(6, max(1, (os.cpu_count() or 2) - 1)),
        help="parallel event-pool workers; fork is used when available",
    )
    parser.add_argument("--exclude-entry-hours", default="08:00-10:00")
    parser.add_argument(
        "--exclude-entry-timezone",
        choices=("UTC", "Asia/Shanghai"),
        default="Asia/Shanghai",
    )
    return parser.parse_args()


def state_value(state: MarketState15s) -> float | None:
    price = optimizer.state_price(state)
    return None if price is None or price <= 0 else float(price)


def contiguous_return_volatility(
    states: list[MarketState15s],
    *,
    start_index: int,
    horizon_buckets: int,
) -> float | None:
    """Stddev of same-horizon log returns strictly before the impulse."""

    timestamps = [state.bucket_start.timestamp() for state in states]
    sample_start = max(horizon_buckets, start_index - VOL_LOOKBACK_BUCKETS)
    returns: list[float] = []
    expected_seconds = BUCKET.total_seconds() * horizon_buckets
    for end_index in range(sample_start, start_index):
        if (
            timestamps[end_index] - timestamps[end_index - horizon_buckets]
            != expected_seconds
        ):
            continue
        first = state_value(states[end_index - horizon_buckets])
        last = state_value(states[end_index])
        if first is None or last is None:
            continue
        returns.append(math.log(last / first))
    if len(returns) < VOL_MIN_SAMPLES:
        return None
    scale = statistics.pstdev(returns)
    return scale if scale > 0 else None


def close_location(state: MarketState15s) -> float | None:
    price = state_value(state)
    high = None if state.high_price is None else float(state.high_price)
    low = None if state.low_price is None else float(state.low_price)
    if price is None or high is None or low is None or high <= low:
        return None
    return max(0.0, min(1.0, (price - low) / (high - low)))


def build_feature_record(
    event: Any,
    *,
    impulse_window: int,
    confirmation: int,
    states_by_symbol: dict[str, list[MarketState15s]],
    state_index_by_key: dict[tuple[str, datetime], int],
) -> tuple[
    float | None,
    float | None,
    float | None,
    float | None,
    float | None,
    float | None,
]:
    states = states_by_symbol[event.symbol]
    detection_index = state_index_by_key[(event.symbol, event.detected_at)]
    # The detector's confirmation can end after the impulse.  Use the
    # event's recorded impulse_start rather than counting backward from the
    # detection time, otherwise confirmation buckets leak into shock_z.
    impulse_start_index = state_index_by_key[(event.symbol, event.impulse_start)]
    if impulse_start_index < 0:
        return None, None, None, None, None, None

    impulse_start_price = state_value(states[impulse_start_index])
    impulse_end_price = state_value(states[impulse_start_index + impulse_window - 1])
    detection_price = state_value(states[detection_index])
    if (
        impulse_start_price is None
        or impulse_end_price is None
        or detection_price is None
    ):
        return None, None, None, None, None, None
    current_log_return = math.log(impulse_end_price / impulse_start_price)
    if event.direction is OrderFlowDirection.DOWN:
        current_log_return = -current_log_return
    historical_vol = contiguous_return_volatility(
        states,
        start_index=impulse_start_index,
        horizon_buckets=impulse_window,
    )
    shock_z = None if historical_vol is None else current_log_return / historical_vol

    detection_state = states[detection_index]
    location = close_location(detection_state)
    prior_start = max(0, detection_index - 7)
    prior_states = states[prior_start : detection_index + 1]
    highs = [
        float(state.high_price)
        for state in prior_states
        if state.high_price is not None and state.high_price > 0
    ]
    pullback_z = None
    if historical_vol is not None and highs and detection_price > 0:
        pullback_z = max(0.0, max(highs) / detection_price - 1.0) / historical_vol

    efficiency = None
    if shock_z is not None and event.notional_intensity > 0:
        efficiency = shock_z / float(event.notional_intensity)

    pressure_states = states[impulse_start_index : detection_index + 1]
    if confirmation > 1:
        confirmation_start = detection_index - confirmation + 1
        pressure_states = states[
            min(impulse_start_index, confirmation_start) : detection_index + 1
        ]
    if pressure_states:
        positive = sum(
            state.aggressive_buy_notional > state.aggressive_sell_notional
            for state in pressure_states
        )
        persistence = positive / len(pressure_states)
    else:
        persistence = None
    return historical_vol, shock_z, location, pullback_z, efficiency, persistence


def parse_split_window(
    proxy: optimizer.ProxyTop10,
    full_end: datetime,
) -> tuple[Split, ...]:
    optimization_start = proxy.first_valid_at
    duration = full_end - optimization_start
    train_end = optimization_start + duration * 0.60
    validation_end = optimization_start + duration * 0.80
    return (
        Split("train", optimization_start, train_end),
        Split("validation", train_end, validation_end),
        Split("holdout", validation_end, full_end),
    )


def numeric(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, 8)


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def metric_summary(
    records: list[FeatureRecord],
    *,
    horizon: int,
    predicate: Callable[[FeatureRecord], bool],
    start: datetime | None = None,
    end: datetime | None = None,
    fee_rate: float,
) -> dict[str, float | int | None]:
    fee_pct = fee_rate * 2.0 * 100.0
    values: list[float] = []
    for record in records:
        if not predicate(record):
            continue
        if start is not None and not (start <= record.event.detected_at < end):
            continue
        value = record.future_returns_pct.get(horizon)
        if value is None:
            continue
        values.append(value - fee_pct)
    gains = sum(value for value in values if value > 0)
    losses = -sum(value for value in values if value < 0)
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in values:
        cumulative += value
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    return {
        "n": len(values),
        "net_forward_pnl_usdt_at_100u": numeric(sum(values)),
        "mean_net_forward_pct": numeric(statistics.fmean(values) if values else None),
        "median_net_forward_pct": numeric(
            statistics.median(values) if values else None
        ),
        "win_rate_pct": numeric(
            sum(value > 0 for value in values) / len(values) * 100.0 if values else None
        ),
        "profit_factor": numeric(gains / losses if losses else None),
        "max_drawdown_usdt_at_100u": numeric(max_drawdown),
    }


def live_trade_summary(
    records: list[FeatureRecord],
    *,
    predicate: Callable[[FeatureRecord], bool],
) -> dict[str, float | int | None]:
    selected = [
        record for record in records if predicate(record) and not record.entry_excluded
    ]
    closed = [
        record.trade
        for record in selected
        if record.trade is not None and record.trade.get("closed")
    ]
    closed = [trade for trade in closed if trade is not None]
    pnls = [float(trade["net_pnl_usdt"]) for trade in closed]
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for pnl in pnls:
        cumulative += pnl
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    margin = fast.initial_margin_peak_rows(
        [
            fast.FastRow(
                observation=record.simulation.observation,
                simulation=record.simulation,
                volume_ratio=None,
                excluded_entry=record.entry_excluded,
                symbol=record.event.symbol,
                detected_epoch=record.detected_epoch,
                impulse_return=record.event.impulse_return_pct,
                imbalance=optimizer.directional_imbalance(record.event),
                confirmation_min=record.confirmation_min,
                intensity=record.event.notional_intensity,
            )
            for record in selected
        ]
    )
    return {
        "selected": len(selected),
        "closed": len(closed),
        "net_pnl_usdt": numeric(sum(pnls)),
        "mean_net_return_pct": numeric(
            statistics.fmean(float(trade["net_return_pct"]) for trade in closed)
            if closed
            else None
        ),
        "win_rate_pct": numeric(
            sum(pnl > 0 for pnl in pnls) / len(pnls) * 100.0 if pnls else None
        ),
        "max_drawdown_usdt": numeric(max_drawdown),
        "natural_initial_margin_peak_usdt": margin,
    }


def quantile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("cannot calculate a quantile of an empty sequence")
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def bucket_summary(
    records: list[FeatureRecord],
    *,
    feature_name: str,
    values: list[tuple[str, Callable[[FeatureRecord], bool]]],
    splits: tuple[Split, ...],
    fee_rate: float,
) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for label, predicate in values:
        row: dict[str, object] = {"feature": feature_name, "bucket": label}
        for horizon in HORIZONS:
            full = metric_summary(
                records,
                horizon=horizon,
                predicate=predicate,
                fee_rate=fee_rate,
            )
            row[f"h{horizon}_n"] = full["n"]
            row[f"h{horizon}_mean_net_pct"] = full["mean_net_forward_pct"]
            row[f"h{horizon}_win_rate_pct"] = full["win_rate_pct"]
            row[f"h{horizon}_pnl"] = full["net_forward_pnl_usdt_at_100u"]
        for split in splits:
            row[f"{split.name}_h8"] = metric_summary(
                records,
                horizon=8,
                predicate=predicate,
                start=split.start,
                end=split.end,
                fee_rate=fee_rate,
            )["mean_net_forward_pct"]
        live = live_trade_summary(records, predicate=predicate)
        row.update({f"live_{key}": value for key, value in live.items()})
        output.append(row)
    return output


def equal_width_buckets(
    records: list[FeatureRecord],
    *,
    feature_name: str,
    getter: Callable[[FeatureRecord], float | None],
    edges: tuple[float, ...],
) -> list[tuple[str, Callable[[FeatureRecord], bool]]]:
    buckets: list[tuple[str, Callable[[FeatureRecord], bool]]] = []
    for index in range(len(edges) - 1):
        left, right = edges[index], edges[index + 1]
        label = f"{left:g}–{right:g}"
        buckets.append(
            (
                label,
                lambda record, left=left, right=right: (
                    getter(record) is not None and left <= getter(record) < right
                ),
            )
        )
    last = edges[-1]
    buckets.append(
        (
            f">={last:g}",
            lambda record, last=last: (
                getter(record) is not None and getter(record) >= last
            ),
        )
    )
    return buckets


def quantile_buckets(
    records: list[FeatureRecord],
    *,
    getter: Callable[[FeatureRecord], float | None],
) -> list[tuple[str, Callable[[FeatureRecord], bool]]]:
    values = [value for record in records if (value := getter(record)) is not None]
    edges = [quantile(values, fraction) for fraction in (0.0, 0.2, 0.4, 0.6, 0.8, 1.0)]
    buckets: list[tuple[str, Callable[[FeatureRecord], bool]]] = []
    for index in range(5):
        left, right = edges[index], edges[index + 1]
        label = f"Q{index + 1} [{left:.4g},{right:.4g}]"
        if index == 4:
            predicate = lambda record, left=left, right=right, getter=getter: (
                getter(record) is not None and left <= getter(record) <= right
            )
        else:
            predicate = lambda record, left=left, right=right, getter=getter: (
                getter(record) is not None and left <= getter(record) < right
            )
        buckets.append((label, predicate))
    return buckets


def select_baseline(
    records: list[FeatureRecord],
    *,
    feature_predicate: Callable[[FeatureRecord], bool] | None = None,
) -> list[FeatureRecord]:
    """Apply the current six-dimensional baseline to one event pool."""

    selected: list[FeatureRecord] = []
    last_selected: dict[str, float] = {}
    for record in records:
        event = record.event
        if record.impulse_window != 3 or record.confirmation != 1:
            continue
        if event.direction is not OrderFlowDirection.UP:
            continue
        if record.entry_excluded:
            continue
        if event.impulse_return_pct < Decimal("0.01"):
            continue
        if optimizer.directional_imbalance(event) < Decimal("0.40"):
            continue
        if record.confirmation_min is None or record.confirmation_min < Decimal("0.40"):
            continue
        if event.notional_intensity < Decimal("2.0"):
            continue
        if feature_predicate is not None and not feature_predicate(record):
            continue
        previous = last_selected.get(event.symbol)
        if previous is not None and record.detected_epoch <= previous:
            continue
        selected.append(record)
        last_selected[event.symbol] = record.detected_epoch
    return selected


def process_event_pool(task: tuple[int, int]) -> tuple[str, int, list[FeatureRecord], int]:
    """Build and replay one parameter pool in a forked worker.

    The large immutable state/index structures are placed in a module-level
    context immediately before the pool is forked.  This keeps the hot event
    detection and replay work parallel without repeatedly serializing the
    full million-row state set for every task.
    """

    if _POOL_CONTEXT is None:
        raise RuntimeError("event-pool worker context has not been initialized")
    impulse_window, confirmation = task
    context = _POOL_CONTEXT
    segments = context["segments"]
    states_by_symbol = context["states_by_symbol"]
    state_by_key = context["state_by_key"]
    state_index_by_key = context["state_index_by_key"]
    proxy = context["proxy"]
    state_times_by_symbol = context["state_times_by_symbol"]
    series_by_symbol = context["series_by_symbol"]
    candles_by_symbol = context["candles_by_symbol"]
    candle_starts_by_symbol = context["candle_starts_by_symbol"]
    exit_config = context["exit_config"]
    full_end = context["full_end"]
    exclusion = context["exclusion"]

    events = build_pool(
        segments,
        impulse_window_buckets=impulse_window,
        confirmation_buckets=confirmation,
        horizons=HORIZONS,
    )
    records: list[FeatureRecord] = []
    skipped = 0
    for event in events:
        confirmation_min = optimizer.confirmation_minimum(
            event,
            confirmation_buckets=confirmation,
            state_by_key=state_by_key,
        )
        top10_allowed = proxy.allows(event.symbol, event.detected_at)
        if (
            event.direction is not OrderFlowDirection.UP
            or not top10_allowed
            or event.impulse_return_pct < Decimal("0.005")
            or optimizer.directional_imbalance(event) < Decimal("0.30")
            or confirmation_min is None
            or confirmation_min < Decimal("0.30")
            or event.notional_intensity < Decimal("1.5")
        ):
            skipped += 1
            continue
        (
            historical_vol,
            shock_z,
            location,
            pullback_z,
            efficiency,
            persistence,
        ) = build_feature_record(
            event,
            impulse_window=impulse_window,
            confirmation=confirmation,
            states_by_symbol=states_by_symbol,
            state_index_by_key=state_index_by_key,
        )
        observation = optimizer.EventObservation(
            event=event,
            confirmation_min_imbalance=confirmation_min,
            top10_proxy_allowed=top10_allowed,
            notional_5m_vs_30m=None,
        )
        simulation = fast.fast_simulate_live_limit_event(
            observation,
            states_by_symbol=states_by_symbol,
            state_times_by_symbol=state_times_by_symbol,
            series_by_symbol=series_by_symbol,
            candles_by_symbol=candles_by_symbol,
            candle_starts_by_symbol=candle_starts_by_symbol,
            state_by_key=state_by_key,
            exit_config=exit_config,
            data_end=full_end,
        )
        records.append(
            FeatureRecord(
                event=event,
                impulse_window=impulse_window,
                confirmation=confirmation,
                confirmation_min=confirmation_min,
                top10_allowed=top10_allowed,
                historical_vol=historical_vol,
                shock_z=shock_z,
                close_location=location,
                pullback_z=pullback_z,
                price_efficiency=efficiency,
                buy_pressure_persistence=persistence,
                future_returns_pct={
                    horizon: (
                        None
                        if event.forward_returns.get(horizon) is None
                        else float(event.forward_returns[horizon]) * 100.0
                    )
                    for horizon in HORIZONS
                },
                simulation=simulation,
                entry_excluded=exclusion.excludes(simulation),
            )
        )
    return f"{impulse_window}/{confirmation}", len(events), records, skipped


def main() -> None:
    args = parse_args()
    if args.top_count != 10:
        raise SystemExit("--top-count is fixed at 10")
    parsed_window = optimizer.parse_entry_time_window(args.exclude_entry_hours)
    if parsed_window is None:
        raise SystemExit("an entry-time exclusion window is required")
    offsets = {"UTC": 0, "Asia/Shanghai": 8}
    exclusion = optimizer.EntryTimeExclusion(
        start_minute=parsed_window[0],
        end_minute=parsed_window[1],
        timezone_label=args.exclude_entry_timezone,
        offset_hours=offsets[args.exclude_entry_timezone],
    )

    print(
        json.dumps(
            {"phase": "start", "pid": os.getpid(), "requested_workers": args.workers}
        ),
        flush=True,
    )
    states, load_stats = load_states(args.input_root, environment=args.environment)
    if not states:
        raise SystemExit("no usable states")
    states_by_symbol: defaultdict[str, list[MarketState15s]] = defaultdict(list)
    state_by_key: dict[tuple[str, datetime], MarketState15s] = {}
    state_index_by_key: dict[tuple[str, datetime], int] = {}
    for state in states:
        states_by_symbol[state.symbol].append(state)
        state_by_key[(state.symbol, state.bucket_start)] = state
    for symbol, symbol_states in states_by_symbol.items():
        symbol_states.sort(key=lambda item: item.bucket_start)
        for index, state in enumerate(symbol_states):
            state_index_by_key[(symbol, state.bucket_start)] = index

    minimum_buckets = max(max(IMPULSE_WINDOWS) + 4, 8) + max(CONFIRMATIONS)
    segments = split_contiguous_states(states, minimum_buckets=minimum_buckets)
    if not segments:
        raise SystemExit("no contiguous segments")
    full_start = min(segment[0].bucket_start for segment in segments)
    full_end = max(segment[-1].bucket_end for segment in segments)
    proxy = optimizer.build_top10_proxy(states, top_count=args.top_count)
    splits = parse_split_window(proxy, full_end)
    optimization_start = splits[0].start

    series_by_symbol = {
        symbol: optimizer.as_series(symbol_states)
        for symbol, symbol_states in states_by_symbol.items()
    }
    candles_by_symbol = {
        symbol: optimizer.complete_candles(symbol_states)
        for symbol, symbol_states in states_by_symbol.items()
    }
    state_times_by_symbol = {
        symbol: [state.bucket_start.timestamp() for state in symbol_states]
        for symbol, symbol_states in states_by_symbol.items()
    }
    candle_starts_by_symbol = {
        symbol: [candle.start for candle in candles]
        for symbol, candles in candles_by_symbol.items()
    }
    exit_config = SameExitConfig(
        grace_bars=int(optimizer.LIVE_FIXED_SETTINGS["candle_grace_bars"]),
        decision_profit_pct=float(
            optimizer.LIVE_FIXED_SETTINGS["candle_grace_decision_profit_pct"]
        ),
        recovery_profit_pct=float(
            optimizer.LIVE_FIXED_SETTINGS["candle_grace_profit_pct"]
        ),
        fee_rate=args.fee_rate,
        notional_usdt=float(optimizer.LIVE_FIXED_SETTINGS["entry_notional_usdt"]),
    )

    global _POOL_CONTEXT
    _POOL_CONTEXT = {
        "segments": segments,
        "states_by_symbol": states_by_symbol,
        "state_by_key": state_by_key,
        "state_index_by_key": state_index_by_key,
        "proxy": proxy,
        "state_times_by_symbol": state_times_by_symbol,
        "series_by_symbol": series_by_symbol,
        "candles_by_symbol": candles_by_symbol,
        "candle_starts_by_symbol": candle_starts_by_symbol,
        "exit_config": exit_config,
        "full_end": full_end,
        "exclusion": exclusion,
    }
    pool_tasks = [
        (impulse_window, confirmation)
        for impulse_window in IMPULSE_WINDOWS
        for confirmation in CONFIRMATIONS
    ]
    requested_workers = max(1, args.workers)
    workers = min(requested_workers, len(pool_tasks))
    if workers > 1:
        try:
            multiprocessing_context = mp.get_context("fork")
        except ValueError:
            multiprocessing_context = None
            workers = 1
    else:
        multiprocessing_context = None

    if multiprocessing_context is None:
        pool_results = [process_event_pool(task) for task in pool_tasks]
    else:
        with multiprocessing_context.Pool(processes=workers) as pool:
            pool_results = pool.map(process_event_pool, pool_tasks)

    records = []
    raw_pool_counts = {}
    skipped = 0
    for pool_name, raw_events, pool_records, pool_skipped in pool_results:
        raw_pool_counts[pool_name] = raw_events
        records.extend(pool_records)
        skipped += pool_skipped
        print(
            json.dumps(
                {
                    "phase": "pool_ready",
                    "pool": pool_name,
                    "raw_events": raw_events,
                    "pool_records": len(pool_records),
                    "records": len(records),
                    "skipped": pool_skipped,
                    "workers": workers,
                }
            ),
            flush=True,
        )

    records.sort(key=lambda record: (record.detected_epoch, record.event.symbol))
    all_predicate = lambda _record: True
    bucket_rows: list[dict[str, object]] = []
    feature_getters: dict[str, Callable[[FeatureRecord], float | None]] = {
        "shock_z": lambda record: record.shock_z,
        "close_location": lambda record: record.close_location,
        "pullback_z": lambda record: record.pullback_z,
        "price_efficiency": lambda record: record.price_efficiency,
        "buy_pressure_persistence": lambda record: record.buy_pressure_persistence,
    }
    fixed_edges = {
        "shock_z": (0.0, 0.5, 1.0, 2.0, 3.0),
        "close_location": (0.0, 0.25, 0.50, 0.75, 1.0),
        "pullback_z": (0.0, 0.25, 0.50, 1.0, 2.0),
        "price_efficiency": (0.0, 0.25, 0.50, 1.0, 2.0),
        "buy_pressure_persistence": (0.0, 0.25, 0.50, 0.75, 1.0),
    }
    for feature_name, getter in feature_getters.items():
        bucket_rows.extend(
            bucket_summary(
                records,
                feature_name=feature_name,
                values=equal_width_buckets(
                    records,
                    feature_name=feature_name,
                    getter=getter,
                    edges=fixed_edges[feature_name],
                ),
                splits=splits,
                fee_rate=args.fee_rate,
            )
        )
        bucket_rows.extend(
            bucket_summary(
                records,
                feature_name=f"{feature_name}_quantile",
                values=quantile_buckets(records, getter=getter),
                splits=splits,
                fee_rate=args.fee_rate,
            )
        )

    high_intensity = lambda record: record.event.notional_intensity >= Decimal("2.0")
    shock_low = lambda record: record.shock_z is not None and record.shock_z < 1.0
    location_low = lambda record: (
        record.close_location is not None and record.close_location <= 0.50
    )
    pullback_high = lambda record: (
        record.pullback_z is not None and record.pullback_z >= 0.50
    )
    high_values = [
        record.price_efficiency
        for record in records
        if high_intensity(record) and record.price_efficiency is not None
    ]
    efficiency_q25 = quantile(high_values, 0.25) if high_values else math.nan
    efficiency_low = lambda record: (
        record.price_efficiency is not None
        and record.price_efficiency <= efficiency_q25
    )
    interaction_predicates: list[tuple[str, Callable[[FeatureRecord], bool]]] = [
        ("all candidate events", all_predicate),
        ("high intensity", high_intensity),
        (
            "high intensity + shock_z<1",
            lambda record: high_intensity(record) and shock_low(record),
        ),
        (
            "high intensity + close_location<=0.50",
            lambda record: high_intensity(record) and location_low(record),
        ),
        (
            "high intensity + pullback_z>=0.50",
            lambda record: high_intensity(record) and pullback_high(record),
        ),
        (
            f"high intensity + efficiency<=Q25({efficiency_q25:.4g})",
            lambda record: high_intensity(record) and efficiency_low(record),
        ),
    ]
    interaction_rows = bucket_summary(
        records,
        feature_name="high_intensity_interactions",
        values=interaction_predicates,
        splits=splits,
        fee_rate=args.fee_rate,
    )

    baseline_records = [
        record
        for record in records
        if record.impulse_window == 3 and record.confirmation == 1
    ]
    baseline_filters: list[tuple[str, Callable[[FeatureRecord], bool]]] = [
        ("baseline six-dimensional", lambda _record: True),
        (
            "baseline + shock_z>=1",
            lambda record: record.shock_z is not None and record.shock_z >= 1.0,
        ),
        (
            "baseline + shock_z>=2",
            lambda record: record.shock_z is not None and record.shock_z >= 2.0,
        ),
        (
            "baseline + close_location>=0.75",
            lambda record: (
                record.close_location is not None and record.close_location >= 0.75
            ),
        ),
        (
            "baseline + pullback_z<0.50",
            lambda record: record.pullback_z is not None and record.pullback_z < 0.50,
        ),
        (
            "baseline + efficiency>=Q75",
            lambda record: (
                record.price_efficiency is not None
                and record.price_efficiency
                >= (
                    quantile(
                        [
                            r.price_efficiency
                            for r in baseline_records
                            if r.price_efficiency is not None
                        ],
                        0.75,
                    )
                    if any(r.price_efficiency is not None for r in baseline_records)
                    else math.inf
                )
            ),
        ),
        (
            "baseline + high intensity + shock_z<1",
            lambda record: high_intensity(record) and shock_low(record),
        ),
        (
            "baseline + high intensity + close_location<=0.50",
            lambda record: high_intensity(record) and location_low(record),
        ),
        (
            "baseline + high intensity + pullback_z>=0.50",
            lambda record: high_intensity(record) and pullback_high(record),
        ),
        (
            "baseline + high intensity + efficiency<=Q25",
            lambda record: high_intensity(record) and efficiency_low(record),
        ),
    ]
    baseline_rows: list[dict[str, object]] = []
    for label, feature_predicate in baseline_filters:
        selected = select_baseline(
            baseline_records, feature_predicate=feature_predicate
        )
        for horizon in HORIZONS:
            outcome = metric_summary(
                selected,
                horizon=horizon,
                predicate=all_predicate,
                fee_rate=args.fee_rate,
            )
            baseline_rows.append(
                {
                    "filter": label,
                    "horizon_buckets": horizon,
                    **outcome,
                }
            )
        live = live_trade_summary(selected, predicate=all_predicate)
        for row in baseline_rows[-len(HORIZONS) :]:
            row.update({f"live_{key}": value for key, value in live.items()})

    event_rows: list[dict[str, object]] = []
    for record in records:
        trade = record.trade or {}
        event_rows.append(
            {
                "symbol": record.event.symbol,
                "detected_at": record.event.detected_at.isoformat(),
                "impulse_window": record.impulse_window,
                "confirmation": record.confirmation,
                "impulse_return_pct": float(record.event.impulse_return_pct) * 100.0,
                "aggressive_imbalance": float(
                    optimizer.directional_imbalance(record.event)
                ),
                "confirmation_min_imbalance": (
                    None
                    if record.confirmation_min is None
                    else float(record.confirmation_min)
                ),
                "notional_intensity": float(record.event.notional_intensity),
                "historical_vol_log_return": record.historical_vol,
                "shock_z": record.shock_z,
                "close_location": record.close_location,
                "pullback_z": record.pullback_z,
                "price_efficiency": record.price_efficiency,
                "buy_pressure_persistence": record.buy_pressure_persistence,
                "entry_excluded": record.entry_excluded,
                "fill_reason": record.simulation.fill_reason,
                "entry_at": trade.get("entry_at"),
                "exit_at": trade.get("exit_at"),
                "net_pnl_usdt": trade.get("net_pnl_usdt"),
                "net_return_pct": trade.get("net_return_pct"),
                **{
                    f"forward_return_h{horizon}_pct": record.future_returns_pct[horizon]
                    for horizon in HORIZONS
                },
            }
        )

    output = args.output_dir
    output.mkdir(parents=True, exist_ok=True)
    write_csv(output / "event_feature_records.csv", event_rows)
    write_csv(output / "feature_buckets.csv", bucket_rows)
    write_csv(output / "interaction_results.csv", interaction_rows)
    write_csv(output / "baseline_filter_results.csv", baseline_rows)

    summary: dict[str, object] = {
        "analysis": "causal volatility-normalized impulse and price-response research",
        "data_start": full_start.isoformat(),
        "data_end": full_end.isoformat(),
        "optimization_window": {
            "start": optimization_start.isoformat(),
            "end": full_end.isoformat(),
        },
        "splits": {
            split.name: {"start": split.start.isoformat(), "end": split.end.isoformat()}
            for split in splits
        },
        "load": load_stats,
        "symbols": len({state.symbol for state in states}),
        "usable_states": len(states),
        "raw_pool_counts": raw_pool_counts,
        "candidate_records": len(records),
        "skipped_events": skipped,
        "workers": workers,
        "pool_rows_are_overlapping": True,
        "volatility_definition": {
            "current_return": "directional log return over the same impulse window",
            "history": "population standard deviation of same-window log returns strictly before the impulse",
            "lookback_buckets": VOL_LOOKBACK_BUCKETS,
            "lookback_minutes": 60,
            "minimum_samples": VOL_MIN_SAMPLES,
            "shock_z": "current directional log return / historical same-window volatility",
        },
        "price_response_definitions": {
            "close_location": "(detection close - detection low) / (detection high - detection low)",
            "pullback_z": "max high over prior 8 buckets through detection / detection close - 1, divided by historical volatility",
            "price_efficiency": "shock_z / notional_intensity; low means more notional acceleration per unit of normalized price progress",
            "buy_pressure_persistence": "share of impulse and confirmation buckets where aggressive buy notional exceeds aggressive sell notional",
        },
        "entry_time_exclusion": {
            "window": exclusion.window_text,
            "timezone": exclusion.timezone_label,
            "interval": "[start, end)",
            "applies_to": "actual filled entry_at for live replay",
        },
        "interaction_efficiency_q25_high_intensity": efficiency_q25,
        "feature_bucket_rows": bucket_rows,
        "interaction_rows": interaction_rows,
        "baseline_filter_rows": baseline_rows,
        "sources": {
            "event_detector": str(
                Path(
                    "src/crypto_momentum_lab/strategies/order_flow_impulse/event_study.py"
                )
            ),
            "market_state_model": str(
                Path("src/crypto_momentum_lab/domain/market/models.py")
            ),
            "execution_replay": str(Path("scripts/optimize_local_live_constrained.py")),
        },
    }
    (output / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )

    lines = [
        "# 波动率标准化与价格响应研究",
        "",
        "本报告使用现有15秒状态和原有事件检测、限价成交、15分钟退出回放，研究两个新增信息维度。没有修改实盘参数。",
        "",
        f"- 数据窗口：`{full_start.isoformat()}` 至 `{full_end.isoformat()}`；有效窗口从 `{optimization_start.isoformat()}` 开始。",
        f"- 可用状态：`{len(states):,}` 条，币种 `{len({state.symbol for state in states})}` 个。",
        f"- 候选事件：`{len(records):,}` 个；先应用现有事件池的最低候选门槛和因果 Top10 代理。",
        "- 候选记录按 9 个 `(impulse, confirmation)` 参数池分别统计；同一币种/时刻可能在多个池重复出现，不能把行数当作独立样本数。",
        f"- 波动率历史窗：触发前 `{VOL_LOOKBACK_BUCKETS * 15 / 60:g}` 分钟，至少 `{VOL_MIN_SAMPLES}` 个同长度收益样本。",
        f"- 事件池回放使用 `{workers}` 个 fork worker；数据加载和最终汇总仍在主进程完成。",
        f"- 实际成交回放排除：`{exclusion.window_text}`（{exclusion.timezone_label}，左闭右开）。",
        "",
        "## 指标定义",
        "",
        "- `shock_z`：当前冲击窗口的方向性 log return ÷ 触发前、同样窗口长度的历史 log return 波动率。",
        "- `close_location`：检测15秒桶的 `(close-low)/(high-low)`，越高表示收盘更接近该桶高点。",
        "- `pullback_z`：检测前8个15秒桶内最高价到检测收盘价的回落幅度，再除以历史波动率。",
        "- `price_efficiency`：`shock_z / notional_intensity`；低值表示成交额加速较大但标准化价格推进较小。",
        "- `buy_pressure_persistence`：冲击及确认桶中主动买入额大于主动卖出额的桶占比。",
        "",
        "所有前向收益均从事件检测价计算，并扣除双边手续费；实际交易结果另按限价成交和退出回放计算。",
        "",
        "## 高买入强度与价格停滞/回落",
        "",
        "| 条件 | H4均值净收益 | H8均值净收益 | H16均值净收益 | H8样本数 | 实际回放PnL | 实际回放回撤 |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in interaction_rows:
        lines.append(
            f"| {row['bucket']} | {row['h4_mean_net_pct'] or 0:+.4f}% | {row['h8_mean_net_pct'] or 0:+.4f}% | {row['h16_mean_net_pct'] or 0:+.4f}% | {row['h8_n']} | {row['live_net_pnl_usdt'] or 0:+.2f}U | {row['live_max_drawdown_usdt'] or 0:.2f}U |"
        )
    lines += [
        "",
        "## 基线加特征筛选",
        "",
        "以下结果固定现有六维基线 `3/1/1.00%/0.40/2.0/cooldown=0`，只观察新增条件，不能代替完整联合寻优。",
        "",
        "| 条件 | H8均值净收益 | H8样本数 | 实际回放PnL | 实际回放平仓数 | 实际回放回撤 |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for label in [label for label, _ in baseline_filters]:
        rows = [
            row
            for row in baseline_rows
            if row["filter"] == label and row["horizon_buckets"] == 8
        ]
        if not rows:
            continue
        row = rows[0]
        lines.append(
            f"| {label} | {row['mean_net_forward_pct'] or 0:+.4f}% | {row['n']} | {row['live_net_pnl_usdt'] or 0:+.2f}U | {row['live_closed']} | {row['live_max_drawdown_usdt'] or 0:.2f}U |"
        )
    lines += [
        "",
        "## 如何使用结果",
        "",
        "- 先看分位区间是否在 train、validation、holdout 三段方向一致，再看实际限价成交回放；单个全量最优区间不能直接用于实盘。",
        "- `shock_z` 解决的是固定涨幅跨币种不可比的问题，但它可能与 `impulse` 和 `min_return` 仍有信息重叠。只有在控制原有参数后仍然改善，才算新增价值。",
        "- 价格响应指标的重点不是追求更高的收盘位置，而是识别“高强度买入没有转化为价格推进”的情形；如果该交互在多个时间段稳定变差，才适合成为过滤条件。",
        "- 盘口OFI尚未包含在本研究中；这里的买卖压力仍来自成交数据。",
        "",
        f"原始明细：`{output / 'event_feature_records.csv'}`、`{output / 'feature_buckets.csv'}`、`{output / 'interaction_results.csv'}`、`{output / 'baseline_filter_results.csv'}`。",
    ]
    (output / "research_report.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "phase": "complete",
                "output_dir": str(output),
                "candidate_records": len(records),
                "raw_pool_counts": raw_pool_counts,
                "interaction_rows": len(interaction_rows),
                "baseline_rows": len(baseline_rows),
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
