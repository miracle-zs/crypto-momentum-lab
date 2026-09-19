#!/usr/bin/env python3
# ruff: noqa: E501
"""Exhaustive seven-dimensional replay for one causal volume feature.

The original local optimizer replays each event and then walks the complete
event pool again for every grid row.  That is correct but unnecessarily slow
for the three volume-feature variants.  This runner keeps the same event
detector, entry fill, exit replay, split metrics, and margin calculation, but
uses binary search for event replay and caches the static threshold filters.

One invocation searches all seven dimensions for exactly one volume feature.
Run separate invocations concurrently for the independent feature variants.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import json
import math
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import optimize_local_live_constrained as optimizer  # noqa: E402
from analyze_breakout_acceptance import (  # noqa: E402
    SameExitConfig,
    make_same_exit_trade,
    mark_at_or_near,
)
from optimize_research_orderflow import (  # noqa: E402
    BUCKET,
    Split,
    build_pool,
    load_states,
    split_contiguous_states,
)  # noqa: E402

from crypto_momentum_lab.domain.market.models import MarketState15s  # noqa: E402
from crypto_momentum_lab.strategies.order_flow_impulse.event_study import (  # noqa: E402
    OrderFlowDirection,
)

FEATURE_WINDOWS: dict[str, tuple[int, int]] = {
    "notional_1m_vs_5m": (4, 20),
    "notional_1m_vs_15m": (4, 60),
    "notional_5m_vs_30m": (20, 120),
}

IMPULSE_WINDOWS = (2, 3, 4)
CONFIRMATIONS = (1, 2, 3)
MIN_RETURNS = tuple(
    Decimal(str(value)) / 100 for value in (0.50, 0.75, 1.00, 1.25, 1.50)
)
MIN_IMBALANCES = tuple(Decimal(str(value)) for value in (0.30, 0.40, 0.50, 0.60))
MIN_INTENSITIES = tuple(Decimal(str(value)) for value in (1.5, 2.0, 3.0, 4.0))
VOLUME_THRESHOLDS = tuple(
    Decimal(str(value)) for value in (0.0, 1.0, 1.25, 1.5, 2.0, 3.0, 4.0)
)
COOLDOWNS = (0, 4, 8, 16, 32)

PROFILE_SPECS: dict[str, dict[str, object]] = {
    "A": {
        "cap": None,
        "cooldown": None,
        "drawdown_weight": 0.0,
        "label": "无保证金上限",
    },
    "B": {
        "cap": 350.0,
        "cooldown": None,
        "drawdown_weight": 0.0,
        "label": "保证金≤350U，自由 cooldown",
    },
    "C": {
        "cap": 350.0,
        "cooldown": 0,
        "drawdown_weight": 0.0,
        "label": "保证金≤350U，cooldown=0",
    },
    "D": {
        "cap": 280.0,
        "cooldown": None,
        "drawdown_weight": 0.0,
        "label": "保证金≤280U，自由 cooldown",
    },
    "E": {
        "cap": 280.0,
        "cooldown": 0,
        "drawdown_weight": 0.0,
        "label": "保证金≤280U，cooldown=0",
    },
    "F": {
        "cap": 280.0,
        "cooldown": 0,
        "drawdown_weight": 0.10,
        "label": "保证金≤280U，回撤惩罚，cooldown=0",
    },
    "G": {
        "cap": 280.0,
        "cooldown": None,
        "drawdown_weight": 0.10,
        "label": "保证金≤280U，回撤惩罚，自由 cooldown",
    },
}

BASELINE = {
    "impulse_window_buckets": 3,
    "confirmation_buckets": 1,
    "min_return_pct": 1.00,
    "min_imbalance": 0.40,
    "min_intensity": 2.0,
    "min_volume_ratio": 0.0,
    "cooldown_buckets": 0,
}


@dataclass(slots=True)
class FastRow:
    """One statically eligible event and its one-time market replay."""

    observation: optimizer.EventObservation
    simulation: optimizer.SimulatedEvent
    volume_ratio: Decimal | None
    excluded_entry: bool
    symbol: str
    detected_epoch: float
    impulse_return: Decimal
    imbalance: Decimal
    confirmation_min: Decimal | None
    intensity: Decimal

    @property
    def trade(self) -> dict[str, Any] | None:
        return self.simulation.trade


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--live-signals", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--volume-feature", choices=tuple(FEATURE_WINDOWS), required=True
    )
    parser.add_argument("--environment", default="research")
    parser.add_argument("--top-count", type=int, default=10)
    parser.add_argument("--fee-rate", type=float, default=0.0005)
    parser.add_argument("--min-validation-trades", type=int, default=10)
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="number of forked workers for the grid evaluation phase",
    )
    parser.add_argument(
        "--selection-scope",
        choices=("validation", "full"),
        default="full",
        help="period used by the main best-row selector; A-G profiles always use full",
    )
    parser.add_argument(
        "--drawdown-weight",
        type=float,
        default=0.0,
        help="score weight for the main best-row selector",
    )
    parser.add_argument(
        "--max-initial-margin-usdt",
        type=float,
        default=None,
        help="optional margin cap for the main best-row selector",
    )
    parser.add_argument(
        "--exclude-entry-hours",
        default="08:00-10:00",
        metavar="HH:MM-HH:MM",
    )
    parser.add_argument(
        "--exclude-entry-timezone",
        choices=("UTC", "Asia/Shanghai"),
        default="Asia/Shanghai",
    )
    return parser.parse_args()


def build_volume_ratio_lookup(
    states_by_symbol: dict[str, list[MarketState15s]],
    *,
    recent_buckets: int,
    baseline_buckets: int,
) -> dict[tuple[str, datetime], Decimal]:
    """Build a causal ratio using the same endpoint-contiguity rule as CML."""

    lookup: dict[tuple[str, datetime], Decimal] = {}
    total_buckets = recent_buckets + baseline_buckets
    expected_span = BUCKET * (total_buckets - 1)
    for symbol, symbol_states in states_by_symbol.items():
        ordered = sorted(symbol_states, key=lambda item: item.bucket_start)
        prefix: list[Decimal] = [Decimal("0")]
        for state in ordered:
            prefix.append(prefix[-1] + state.trade_notional)
        for index, state in enumerate(ordered):
            first_index = index - total_buckets + 1
            if first_index < 0:
                continue
            if state.bucket_start - ordered[first_index].bucket_start != expected_span:
                continue
            recent_start = index + 1 - recent_buckets
            baseline_start = first_index
            baseline_end = recent_start
            recent_total = prefix[index + 1] - prefix[recent_start]
            baseline_total = prefix[baseline_end] - prefix[baseline_start]
            if baseline_total <= 0:
                continue
            lookup[(symbol, state.bucket_start)] = (
                recent_total
                * Decimal(baseline_buckets)
                / (baseline_total * Decimal(recent_buckets))
            )
    return lookup


def fast_simulate_live_limit_event(
    observation: optimizer.EventObservation,
    *,
    states_by_symbol: dict[str, list[MarketState15s]],
    state_times_by_symbol: dict[str, list[float]],
    series_by_symbol: dict[str, optimizer.Series],
    candles_by_symbol: dict[str, list[optimizer.Candle15]],
    candle_starts_by_symbol: dict[str, list[float]],
    state_by_key: dict[tuple[str, datetime], MarketState15s],
    exit_config: SameExitConfig,
    data_end: datetime,
) -> optimizer.SimulatedEvent:
    """Replay one event without repeatedly scanning from the symbol's origin."""

    event = observation.event
    entry_price, order_created_at = optimizer.event_entry_price(event, state_by_key)
    if entry_price is None or entry_price <= 0:
        return optimizer.SimulatedEvent(observation, None, "missing_entry_price")

    symbol_states = states_by_symbol.get(event.symbol, [])
    state_times = state_times_by_symbol.get(event.symbol, [])
    order_epoch = order_created_at.timestamp()
    deadline_epoch = order_epoch + int(
        optimizer.LIVE_FIXED_SETTINGS["entry_limit_ttl_seconds"]
    )
    left = bisect.bisect_left(state_times, order_epoch)
    right = bisect.bisect_left(state_times, deadline_epoch)
    fill_state: MarketState15s | None = None
    for state in symbol_states[left:right]:
        low = state.low_price or optimizer.state_price(state)
        if low is not None and low <= entry_price:
            fill_state = state
            break
    if fill_state is None:
        return optimizer.SimulatedEvent(
            observation, None, "limit_not_filled_before_ttl"
        )

    entry_epoch = fill_state.bucket_end.timestamp()
    event_row = {
        "scenario": "live_fixed_settings",
        "label": "live-fixed-settings",
        "entry_mode": "limit",
        "signal_id": f"local-{event.symbol}-{event.detected_at.isoformat()}",
        "symbol": event.symbol,
        "entry_epoch": entry_epoch,
        "entry_price": float(entry_price),
    }
    series = series_by_symbol.get(event.symbol)
    candles = candles_by_symbol.get(event.symbol, [])
    if series is None or not series.rows or not candles:
        return optimizer.SimulatedEvent(observation, None, "missing_exit_path")

    # This is the body of analyze_breakout_acceptance.simulate_same_exit, with
    # binary-search entry points for both candles and recovery-bar states.
    entry_price_float = float(entry_price)
    series_data_end = series.rows[-1][0] + 15.0
    first_eligible_start = math.floor(entry_epoch / 900.0) * 900.0 + 900.0
    candle_starts = candle_starts_by_symbol[event.symbol]
    candle_left = bisect.bisect_left(candle_starts, first_eligible_start)
    for candle in candles[candle_left:]:
        if candle.end > series_data_end + 1e-6:
            continue
        if candle.close >= candle.open:
            continue

        if candle.close >= entry_price_float * (1.0 + exit_config.decision_profit_pct):
            trade = make_same_exit_trade(
                event_row,
                exit_epoch=candle.end,
                exit_price=candle.close,
                exit_reason="candle_15m_bearish",
                config=exit_config,
            )
            break

        if exit_config.grace_bars <= 0 or exit_config.recovery_profit_pct <= 0:
            trade = make_same_exit_trade(
                event_row,
                exit_epoch=candle.end,
                exit_price=candle.close,
                exit_reason="candle_15m_bearish",
                config=exit_config,
            )
            break

        recovery_price = entry_price_float * (1.0 + exit_config.recovery_profit_pct)
        timeout = candle.end + 900.0 * exit_config.grace_bars
        series_left = bisect.bisect_left(series.times, candle.end + 1e-6)
        series_right = bisect.bisect_left(series.times, timeout + 1e-6)
        for row in series.rows[series_left:series_right]:
            if row[1] >= recovery_price:
                trade = make_same_exit_trade(
                    event_row,
                    exit_epoch=row[0],
                    exit_price=recovery_price,
                    exit_reason=f"candle_15m_bearish_grace_limit_{exit_config.grace_bars}",
                    config=exit_config,
                )
                break
        else:
            mark = mark_at_or_near(series, timeout)
            if mark is not None:
                _mark_time, mark_price = mark
                trade = make_same_exit_trade(
                    event_row,
                    exit_epoch=timeout,
                    exit_price=mark_price,
                    exit_reason=f"candle_15m_grace_timeout_{exit_config.grace_bars}",
                    config=exit_config,
                )
                break
            continue
        break
    else:
        trade = make_same_exit_trade(
            event_row,
            exit_epoch=None,
            exit_price=None,
            exit_reason="open_at_data_end",
            config=exit_config,
            marked_price=series.rows[-1][3],
        )

    if not trade["closed"]:
        return optimizer.SimulatedEvent(observation, trade, "open_at_data_end")
    if (
        trade.get("exit_epoch") is not None
        and float(trade["exit_epoch"]) > data_end.timestamp()
    ):
        return optimizer.SimulatedEvent(observation, None, "exit_after_data_end")
    return optimizer.SimulatedEvent(observation, trade, "filled")


def metric(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, 8)


def metrics_from_values(
    values: list[float],
    *,
    n_selected: int,
    n_open: int,
    n_limit_unfilled: int,
) -> dict[str, float | int | None]:
    returns = [value / 100.0 * 100.0 for value in values]
    gains = sum(value for value in values if value > 0)
    losses = -sum(value for value in values if value < 0)
    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0
    for pnl in values:
        cumulative += pnl
        peak = max(peak, cumulative)
        max_dd = max(max_dd, peak - cumulative)
    return {
        "n_selected": n_selected,
        "n_closed": len(values),
        "n_open_at_data_end": n_open,
        "n_limit_unfilled": n_limit_unfilled,
        "net_pnl_usdt": metric(sum(values)),
        "mean_net_return_pct": metric(statistics.fmean(returns) if returns else None),
        "median_net_return_pct": metric(
            statistics.median(returns) if returns else None
        ),
        "win_rate_pct": metric(
            sum(value > 0 for value in returns) / len(returns) * 100.0
            if returns
            else None
        ),
        "profit_factor": metric(gains / losses if losses else None),
        "max_drawdown_usdt": metric(max_dd),
    }


def metrics_for_rows(
    selected: list[FastRow],
    *,
    splits: tuple[Split, ...],
    optimization_start: datetime,
    full_end: datetime,
) -> dict[str, dict[str, float | int | None]]:
    """Calculate all split metrics in one pass, matching the old semantics."""

    bounds = [(split.start.timestamp(), split.end.timestamp()) for split in splits]
    bounds.append((optimization_start.timestamp(), full_end.timestamp()))
    values: list[list[float]] = [[] for _ in bounds]
    open_counts = [0 for _ in bounds]
    limit_unfilled = sum(
        row.simulation.fill_reason == "limit_not_filled_before_ttl" for row in selected
    )
    for row in selected:
        trade = row.trade
        if trade is None:
            continue
        entry_epoch = float(trade["entry_epoch"])
        closed = bool(trade["closed"])
        exit_epoch = float(trade["exit_epoch"]) if closed else None
        pnl = float(trade["net_pnl_usdt"]) if closed else None
        for index, (start_epoch, end_epoch) in enumerate(bounds):
            if not (start_epoch <= entry_epoch < end_epoch):
                continue
            if not closed:
                open_counts[index] += 1
            elif exit_epoch is not None and exit_epoch < end_epoch:
                assert pnl is not None
                values[index].append(pnl)

    names = [split.name for split in splits] + ["full"]
    return {
        name: metrics_from_values(
            values[index],
            n_selected=len(selected),
            n_open=open_counts[index],
            n_limit_unfilled=limit_unfilled,
        )
        for index, name in enumerate(names)
    }


def initial_margin_peak_rows(selected: list[FastRow]) -> float:
    margin_per_entry = float(
        optimizer.LIVE_FIXED_SETTINGS["entry_notional_usdt"]
    ) / float(optimizer.LIVE_FIXED_SETTINGS["entry_leverage"])
    events: list[tuple[float, int]] = []
    for row in selected:
        trade = row.trade
        if trade is None:
            continue
        events.append((float(trade["entry_epoch"]), 1))
        if trade.get("closed") and trade.get("exit_epoch") is not None:
            events.append((float(trade["exit_epoch"]), -1))
    current = 0
    peak = 0
    for _timestamp, delta in sorted(events, key=lambda item: (item[0], item[1])):
        current += delta
        peak = max(peak, current)
    return round(peak * margin_per_entry, 8)


def select_rows(
    rows: list[FastRow],
    *,
    min_return: Decimal,
    min_imbalance: Decimal,
    min_intensity: Decimal,
    min_volume_ratio: Decimal,
    cooldown_buckets: int,
) -> list[FastRow]:
    selected: list[FastRow] = []
    last_selected: dict[str, float] = {}
    cooldown_seconds = BUCKET.total_seconds() * cooldown_buckets
    for row in rows:
        if row.excluded_entry:
            continue
        if row.impulse_return < min_return:
            continue
        if row.imbalance < min_imbalance:
            continue
        if row.confirmation_min is None or row.confirmation_min < min_imbalance:
            continue
        if row.intensity < min_intensity:
            continue
        if min_volume_ratio > 0 and (
            row.volume_ratio is None or row.volume_ratio < min_volume_ratio
        ):
            continue
        previous = last_selected.get(row.symbol)
        if previous is not None and row.detected_epoch <= previous + cooldown_seconds:
            continue
        selected.append(row)
        last_selected[row.symbol] = row.detected_epoch
    return selected


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def row_config(row: dict[str, object]) -> dict[str, object]:
    return {
        "impulse_window_buckets": int(row["impulse_window_buckets"]),
        "confirmation_buckets": int(row["confirmation_buckets"]),
        "min_return_pct": float(row["min_return_pct"]),
        "min_imbalance": float(row["min_imbalance"]),
        "min_intensity": float(row["min_intensity"]),
        "min_volume_ratio": float(row["min_volume_ratio"]),
        "cooldown_buckets": int(row["cooldown_buckets"]),
    }


def row_number(row: dict[str, object], key: str) -> float:
    value = row.get(key)
    return float(value) if value is not None else -math.inf


def choose_row(
    rows: list[dict[str, object]],
    *,
    cap: float | None,
    cooldown: int | None,
    drawdown_weight: float,
    min_validation_trades: int,
) -> dict[str, object]:
    eligible: list[tuple[tuple[float, float, float, int], dict[str, object]]] = []
    for row in rows:
        if int(row["validation_n_closed"]) < min_validation_trades:
            continue
        if cap is not None and float(row["initial_margin_peak_usdt"]) > cap + 1e-9:
            continue
        if cooldown is not None and int(row["cooldown_buckets"]) != cooldown:
            continue
        pnl = row_number(row, "full_net_pnl_usdt")
        drawdown = row_number(row, "full_max_drawdown_usdt")
        score = pnl - drawdown_weight * drawdown
        key = (score, pnl, -drawdown, int(row["full_n_closed"]))
        eligible.append((key, row))
    if not eligible:
        raise SystemExit("profile has no eligible candidate rows")
    eligible.sort(key=lambda item: item[0], reverse=True)
    return eligible[0][1]


def report_for_rows(
    selected: list[FastRow],
    *,
    config: dict[str, object],
    splits: tuple[Split, ...],
    optimization_start: datetime,
    full_end: datetime,
    cap: float | None,
    drawdown_weight: float,
) -> dict[str, object]:
    metrics = metrics_for_rows(
        selected,
        splits=splits,
        optimization_start=optimization_start,
        full_end=full_end,
    )
    full = metrics["full"]
    pnl = full["net_pnl_usdt"]
    drawdown = full["max_drawdown_usdt"]
    score = (
        None
        if pnl is None or drawdown is None
        else round(float(pnl) - drawdown_weight * float(drawdown), 8)
    )
    peak = initial_margin_peak_rows(selected)
    return {
        "config": config,
        "natural_initial_margin_peak_usdt": peak,
        "margin_constraint_feasible": cap is None or peak <= cap + 1e-9,
        "selection_scope": "full",
        "selection_score": score,
        "metrics": metrics,
    }


def event_csv_row(row: FastRow, volume_feature: str) -> dict[str, object]:
    event = row.observation.event
    trade = row.trade or {}
    return {
        "symbol": event.symbol,
        "direction": event.direction.value,
        "detected_at": event.detected_at.isoformat(),
        "order_created_at": (event.detected_at + BUCKET).isoformat(),
        "impulse_return_pct": float(event.impulse_return_pct) * 100.0,
        "aggressive_imbalance": float(optimizer.directional_imbalance(event)),
        "confirmation_min_imbalance": (
            None if row.confirmation_min is None else float(row.confirmation_min)
        ),
        "notional_intensity": float(event.notional_intensity),
        "volume_feature": volume_feature,
        "volume_ratio": None if row.volume_ratio is None else float(row.volume_ratio),
        "top10_proxy_allowed": row.observation.top10_proxy_allowed,
        "fill_reason": row.simulation.fill_reason,
        "entry_at": trade.get("entry_at"),
        "entry_price": trade.get("entry_price"),
        "exit_at": trade.get("exit_at"),
        "exit_price": trade.get("exit_price"),
        "exit_reason": trade.get("exit_reason"),
        "closed": trade.get("closed"),
        "net_pnl_usdt": trade.get("net_pnl_usdt"),
        "net_return_pct": trade.get("net_return_pct"),
    }


def profile_equity_series(
    named_rows: dict[str, list[FastRow]],
    *,
    start: datetime,
    end: datetime,
) -> list[dict[str, object]]:
    deltas: defaultdict[datetime, dict[str, float]] = defaultdict(dict)
    for name, rows in named_rows.items():
        for row in rows:
            trade = row.trade
            if trade is None or not trade["closed"]:
                continue
            exit_at = datetime.fromtimestamp(float(trade["exit_epoch"]), tz=UTC)
            if start <= exit_at < end:
                deltas[exit_at][name] = deltas[exit_at].get(name, 0.0) + float(
                    trade["net_pnl_usdt"]
                )
    names = tuple(named_rows)
    cumulative = {name: 0.0 for name in names}
    output: list[dict[str, object]] = [
        {
            "timestamp": start.isoformat(),
            **{f"{name}_cumulative_pnl_usdt": 0.0 for name in names},
        }
    ]
    for timestamp in sorted(deltas):
        row: dict[str, object] = {"timestamp": timestamp.isoformat()}
        for name in names:
            cumulative[name] += deltas[timestamp].get(name, 0.0)
            row[f"{name}_cumulative_pnl_usdt"] = round(cumulative[name], 8)
        output.append(row)
    if output[-1]["timestamp"] != end.isoformat():
        output.append(
            {
                "timestamp": end.isoformat(),
                **{
                    f"{name}_cumulative_pnl_usdt": round(cumulative[name], 8)
                    for name in names
                },
            }
        )
    return output


_GRID_CONTEXT: dict[str, Any] | None = None


def candidate_score(
    pnl: float | None,
    drawdown: float | None,
    *,
    margin_feasible: bool,
    validation_n_closed: int,
    min_validation_trades: int,
    drawdown_weight: float,
) -> float | None:
    if (
        not margin_feasible
        or validation_n_closed < min_validation_trades
        or pnl is None
        or drawdown is None
    ):
        return None
    return round(float(pnl) - drawdown_weight * float(drawdown), 8)


def evaluate_grid_specs(
    specs: list[tuple[tuple[int, int], Decimal, Decimal, Decimal, Decimal, int]],
) -> list[dict[str, object]]:
    """Evaluate a shard of the complete grid in a forked worker.

    The prepared event/replay data is inherited copy-on-write on macOS/Linux,
    so workers only receive the small candidate-spec list.  Keeping specs in
    base-threshold order also makes the per-worker static-filter cache useful.
    """

    if _GRID_CONTEXT is None:
        raise RuntimeError("grid worker context was not initialized")
    context = _GRID_CONTEXT
    pools: dict[tuple[int, int], list[FastRow]] = context["pools"]
    splits: tuple[Split, ...] = context["splits"]
    optimization_start: datetime = context["optimization_start"]
    full_end: datetime = context["full_end"]
    selection_scope: str = context["selection_scope"]
    drawdown_weight: float = context["drawdown_weight"]
    max_initial_margin: float | None = context["max_initial_margin"]
    min_validation_trades: int = context["min_validation_trades"]

    static_cache: dict[
        tuple[tuple[int, int], Decimal, Decimal, Decimal], list[FastRow]
    ] = {}
    output: list[dict[str, object]] = []
    for (
        pool_key,
        min_return,
        min_imbalance,
        min_intensity,
        min_volume_ratio,
        cooldown,
    ) in specs:
        cache_key = (pool_key, min_return, min_imbalance, min_intensity)
        base_rows = static_cache.get(cache_key)
        if base_rows is None:
            base_rows = [
                row
                for row in pools[pool_key]
                if row.impulse_return >= min_return
                and row.imbalance >= min_imbalance
                and row.confirmation_min is not None
                and row.confirmation_min >= min_imbalance
                and row.intensity >= min_intensity
                and not row.excluded_entry
            ]
            static_cache[cache_key] = base_rows
        if min_volume_ratio == 0:
            ratio_rows = base_rows
        else:
            ratio_rows = [
                row
                for row in base_rows
                if row.volume_ratio is not None and row.volume_ratio >= min_volume_ratio
            ]
        selected = select_rows(
            ratio_rows,
            min_return=min_return,
            min_imbalance=min_imbalance,
            min_intensity=min_intensity,
            min_volume_ratio=min_volume_ratio,
            cooldown_buckets=cooldown,
        )
        metrics = metrics_for_rows(
            selected,
            splits=splits,
            optimization_start=optimization_start,
            full_end=full_end,
        )
        margin_peak = initial_margin_peak_rows(selected)
        margin_feasible = (
            max_initial_margin is None or margin_peak <= max_initial_margin + 1e-9
        )
        validation = metrics["validation"]
        selection_metrics = metrics[selection_scope]

        row: dict[str, object] = {
            "impulse_window_buckets": pool_key[0],
            "confirmation_buckets": pool_key[1],
            "min_return_pct": float(min_return) * 100.0,
            "min_imbalance": float(min_imbalance),
            "min_intensity": float(min_intensity),
            "volume_feature": context["volume_feature"],
            "min_volume_ratio": float(min_volume_ratio),
            f"min_{context['volume_feature']}": float(min_volume_ratio),
            "cooldown_buckets": cooldown,
            "n_selected_full": len(selected),
            "initial_margin_peak_usdt": margin_peak,
            "margin_constraint_feasible": margin_feasible,
            "validation_score": candidate_score(
                validation["net_pnl_usdt"],
                validation["max_drawdown_usdt"],
                margin_feasible=margin_feasible,
                validation_n_closed=int(validation["n_closed"]),
                min_validation_trades=min_validation_trades,
                drawdown_weight=drawdown_weight,
            ),
            "selection_score": candidate_score(
                selection_metrics["net_pnl_usdt"],
                selection_metrics["max_drawdown_usdt"],
                margin_feasible=margin_feasible,
                validation_n_closed=int(validation["n_closed"]),
                min_validation_trades=min_validation_trades,
                drawdown_weight=drawdown_weight,
            ),
            "selection_scope": selection_scope,
            "selection_n_closed": selection_metrics["n_closed"],
            "selection_net_pnl_usdt": selection_metrics["net_pnl_usdt"],
            "selection_max_drawdown_usdt": selection_metrics["max_drawdown_usdt"],
        }
        for split_name, split_metrics in metrics.items():
            for metric_name, value in split_metrics.items():
                row[f"{split_name}_{metric_name}"] = value
        output.append(row)
    return output


def main() -> None:
    args = parse_args()
    if args.top_count != 10:
        raise SystemExit("--top-count is fixed at the live value 10")
    if args.min_validation_trades < 1:
        raise SystemExit("--min-validation-trades must be positive")
    if args.drawdown_weight < 0:
        raise SystemExit("--drawdown-weight must not be negative")
    if args.max_initial_margin_usdt is not None and args.max_initial_margin_usdt <= 0:
        raise SystemExit("--max-initial-margin-usdt must be positive")

    parsed_window = optimizer.parse_entry_time_window(args.exclude_entry_hours)
    exclusion = None
    if parsed_window is not None:
        offsets = {"UTC": 0, "Asia/Shanghai": 8}
        exclusion = optimizer.EntryTimeExclusion(
            start_minute=parsed_window[0],
            end_minute=parsed_window[1],
            timezone_label=args.exclude_entry_timezone,
            offset_hours=offsets[args.exclude_entry_timezone],
        )

    recent_buckets, baseline_buckets = FEATURE_WINDOWS[args.volume_feature]
    print(
        json.dumps(
            {
                "phase": "start",
                "feature": args.volume_feature,
                "windows": {
                    "recent_buckets": recent_buckets,
                    "baseline_buckets": baseline_buckets,
                },
                "pid": __import__("os").getpid(),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )

    states, load_stats = load_states(args.input_root, environment=args.environment)
    if not states:
        raise SystemExit("no usable local research states found")
    states_by_symbol: defaultdict[str, list[MarketState15s]] = defaultdict(list)
    state_by_key: dict[tuple[str, datetime], MarketState15s] = {}
    for state in states:
        states_by_symbol[state.symbol].append(state)
        state_by_key[(state.symbol, state.bucket_start)] = state
    for symbol_states in states_by_symbol.values():
        symbol_states.sort(key=lambda item: item.bucket_start)

    minimum_buckets = max(max(IMPULSE_WINDOWS) + 4, 8) + max(CONFIRMATIONS)
    segments = split_contiguous_states(states, minimum_buckets=minimum_buckets)
    if not segments:
        raise SystemExit("no contiguous local research state segments found")
    full_start = min(segment[0].bucket_start for segment in segments)
    full_end = max(segment[-1].bucket_end for segment in segments)
    proxy = optimizer.build_true_top10_proxy_from_parquet(
        args.input_root,
        top_count=args.top_count,
        environment=args.environment,
    )
    if proxy is None:
        proxy = optimizer.build_top10_proxy(states, top_count=args.top_count)
    optimization_start = proxy.first_valid_at
    if optimization_start >= full_end:
        raise SystemExit("local data has no valid full UTC-day Top10 proxy window")
    duration = full_end - optimization_start
    train_end = optimization_start + duration * 0.60
    validation_end = optimization_start + duration * 0.80
    splits = (
        Split("train", optimization_start, train_end),
        Split("validation", train_end, validation_end),
        Split("holdout", validation_end, full_end),
    )

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
    volume_lookup = build_volume_ratio_lookup(
        states_by_symbol,
        recent_buckets=recent_buckets,
        baseline_buckets=baseline_buckets,
    )

    pools: dict[tuple[int, int], list[FastRow]] = {}
    raw_pool_counts: dict[str, int] = {}
    skipped_static = 0
    for impulse_window in IMPULSE_WINDOWS:
        for confirmation in CONFIRMATIONS:
            events = build_pool(
                segments,
                impulse_window_buckets=impulse_window,
                confirmation_buckets=confirmation,
                horizons=(1,),
            )
            key = (impulse_window, confirmation)
            raw_pool_counts[f"impulse_{impulse_window}_confirmation_{confirmation}"] = (
                len(events)
            )
            rows: list[FastRow] = []
            for event in events:
                confirmation_min = optimizer.confirmation_minimum(
                    event,
                    confirmation_buckets=confirmation,
                    state_by_key=state_by_key,
                )
                top10_allowed = proxy.allows(event.symbol, event.detected_at)
                imbalance = optimizer.directional_imbalance(event)
                ratio = volume_lookup.get((event.symbol, event.detected_at))
                # These are the lowest values in the complete grid.  Events
                # failing them can never enter any candidate and need not be
                # replayed, while preserving exact results for every row.
                if (
                    event.direction is not OrderFlowDirection.UP
                    or not top10_allowed
                    or event.impulse_return_pct < MIN_RETURNS[0]
                    or imbalance < MIN_IMBALANCES[0]
                    or confirmation_min is None
                    or confirmation_min < MIN_IMBALANCES[0]
                    or event.notional_intensity < MIN_INTENSITIES[0]
                ):
                    skipped_static += 1
                    continue
                observation = optimizer.EventObservation(
                    event=event,
                    confirmation_min_imbalance=confirmation_min,
                    top10_proxy_allowed=top10_allowed,
                    notional_5m_vs_30m=ratio,
                )
                simulation = fast_simulate_live_limit_event(
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
                entry_excluded = (
                    False if exclusion is None else exclusion.excludes(simulation)
                )
                rows.append(
                    FastRow(
                        observation=observation,
                        simulation=simulation,
                        volume_ratio=ratio,
                        excluded_entry=entry_excluded,
                        symbol=event.symbol,
                        detected_epoch=event.detected_at.timestamp(),
                        impulse_return=event.impulse_return_pct,
                        imbalance=imbalance,
                        confirmation_min=confirmation_min,
                        intensity=event.notional_intensity,
                    )
                )
            rows.sort(key=lambda row: (row.detected_epoch, row.symbol))
            pools[key] = rows
            print(
                json.dumps(
                    {
                        "phase": "pool_ready",
                        "feature": args.volume_feature,
                        "pool": f"{impulse_window}/{confirmation}",
                        "raw_events": len(events),
                        "replayed_static_eligible": len(rows),
                        "skipped_static_total": skipped_static,
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )

    all_rows = [row for rows in pools.values() for row in rows]
    grid_specs: list[
        tuple[tuple[int, int], Decimal, Decimal, Decimal, Decimal, int]
    ] = []
    for impulse_window in IMPULSE_WINDOWS:
        for confirmation in CONFIRMATIONS:
            for min_return in MIN_RETURNS:
                for min_imbalance in MIN_IMBALANCES:
                    for min_intensity in MIN_INTENSITIES:
                        for min_volume_ratio in VOLUME_THRESHOLDS:
                            for cooldown in COOLDOWNS:
                                grid_specs.append(
                                    (
                                        (impulse_window, confirmation),
                                        min_return,
                                        min_imbalance,
                                        min_intensity,
                                        min_volume_ratio,
                                        cooldown,
                                    )
                                )

    global _GRID_CONTEXT
    _GRID_CONTEXT = {
        "pools": pools,
        "splits": splits,
        "optimization_start": optimization_start,
        "full_end": full_end,
        "selection_scope": args.selection_scope,
        "drawdown_weight": args.drawdown_weight,
        "max_initial_margin": args.max_initial_margin_usdt,
        "min_validation_trades": args.min_validation_trades,
        "volume_feature": args.volume_feature,
    }
    if args.workers < 1:
        raise SystemExit("--workers must be positive")
    worker_count = min(args.workers, len(grid_specs))
    print(
        json.dumps(
            {
                "phase": "grid_start",
                "feature": args.volume_feature,
                "candidate_count": len(grid_specs),
                "workers": worker_count,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    if worker_count == 1:
        grid_rows = evaluate_grid_specs(grid_specs)
    else:
        import multiprocessing as mp

        if "fork" not in mp.get_all_start_methods():
            raise SystemExit("multiple grid workers require a fork-capable platform")
        chunk_size = math.ceil(len(grid_specs) / worker_count)
        spec_chunks = [
            grid_specs[index : index + chunk_size]
            for index in range(0, len(grid_specs), chunk_size)
        ]
        fork_context = mp.get_context("fork")
        with fork_context.Pool(processes=worker_count) as pool:
            parts = pool.map(evaluate_grid_specs, spec_chunks)
        grid_rows = [row for part in parts for row in part]
    base_combos = (
        len(pools) * len(MIN_RETURNS) * len(MIN_IMBALANCES) * len(MIN_INTENSITIES)
    )

    def ranking_key(row: dict[str, object]) -> tuple[bool, float, float, float, int]:
        score = row.get("selection_score")
        return (
            score is not None,
            row_number(row, "selection_score"),
            row_number(row, "selection_net_pnl_usdt"),
            -row_number(row, "selection_max_drawdown_usdt"),
            int(row["selection_n_closed"]),
        )

    ranked = sorted(grid_rows, key=ranking_key, reverse=True)
    feasible_ranked = [row for row in ranked if row["selection_score"] is not None]
    if not feasible_ranked:
        raise SystemExit("parameter grid produced no feasible scored rows")
    best_row = feasible_ranked[0]

    def selected_for_config(config: dict[str, object]) -> list[FastRow]:
        return select_rows(
            pools[
                (
                    int(config["impulse_window_buckets"]),
                    int(config["confirmation_buckets"]),
                )
            ],
            min_return=Decimal(str(float(config["min_return_pct"]) / 100.0)),
            min_imbalance=Decimal(str(config["min_imbalance"])),
            min_intensity=Decimal(str(config["min_intensity"])),
            min_volume_ratio=Decimal(str(config["min_volume_ratio"])),
            cooldown_buckets=int(config["cooldown_buckets"]),
        )

    baseline_selected = selected_for_config(BASELINE)
    baseline_report = report_for_rows(
        baseline_selected,
        config=BASELINE,
        splits=splits,
        optimization_start=optimization_start,
        full_end=full_end,
        cap=None,
        drawdown_weight=0.0,
    )
    profile_rows: dict[str, dict[str, object]] = {}
    profile_reports: dict[str, dict[str, object]] = {}
    profile_selected: dict[str, list[FastRow]] = {}
    for profile_name, spec in PROFILE_SPECS.items():
        chosen = choose_row(
            grid_rows,
            cap=None if spec["cap"] is None else float(spec["cap"]),
            cooldown=None if spec["cooldown"] is None else int(spec["cooldown"]),
            drawdown_weight=float(spec["drawdown_weight"]),
            min_validation_trades=args.min_validation_trades,
        )
        config = row_config(chosen)
        selected = selected_for_config(config)
        profile_rows[profile_name] = chosen
        profile_selected[profile_name] = selected
        profile_reports[profile_name] = report_for_rows(
            selected,
            config=config,
            splits=splits,
            optimization_start=optimization_start,
            full_end=full_end,
            cap=None if spec["cap"] is None else float(spec["cap"]),
            drawdown_weight=float(spec["drawdown_weight"]),
        )

    best_config = row_config(best_row)
    best_selected = selected_for_config(best_config)
    best_report = report_for_rows(
        best_selected,
        config=best_config,
        splits=splits,
        optimization_start=optimization_start,
        full_end=full_end,
        cap=args.max_initial_margin_usdt,
        drawdown_weight=args.drawdown_weight,
    )

    output = args.output_dir
    output.mkdir(parents=True, exist_ok=True)
    write_csv(output / "grid_results.csv", grid_rows)
    write_csv(output / "top_candidates.csv", ranked[:100])
    write_csv(
        output / "best_candidate_events.csv",
        [event_csv_row(row, args.volume_feature) for row in best_selected],
    )
    write_csv(
        output / "baseline_events.csv",
        [event_csv_row(row, args.volume_feature) for row in baseline_selected],
    )
    for profile_name, selected in profile_selected.items():
        write_csv(
            output / f"profile_{profile_name}_events.csv",
            [event_csv_row(row, args.volume_feature) for row in selected],
        )
    write_csv(
        output / "equity_series.csv",
        profile_equity_series(
            {"baseline": baseline_selected, "best": best_selected},
            start=full_start,
            end=full_end,
        ),
    )
    write_csv(
        output / "profile_equity_series.csv",
        profile_equity_series(
            {"baseline": baseline_selected, **profile_selected},
            start=full_start,
            end=full_end,
        ),
    )

    manifest: dict[str, object] = {
        "analysis": "complete seven-dimensional joint optimization for a causal volume feature",
        "evaluation_mode": "fast_cached_exact_replay",
        "volume_feature": args.volume_feature,
        "volume_windows": {
            "recent_buckets": recent_buckets,
            "baseline_buckets": baseline_buckets,
            "bucket_seconds": 15,
            "ratio_definition": "average recent notional / average immediately preceding baseline notional",
        },
        "selection_scope": args.selection_scope,
        "selection_objective": (
            f"{args.selection_scope}_net_pnl_usdt_minus_{args.drawdown_weight:g}_times_"
            f"{args.selection_scope}_max_drawdown_usdt"
        ),
        "drawdown_weight": args.drawdown_weight,
        "source_root": str(args.input_root),
        "environment": args.environment,
        "load": load_stats,
        "contiguous_segments": len(segments),
        "symbols": len({state.symbol for state in states}),
        "data_start": full_start.isoformat(),
        "data_end": full_end.isoformat(),
        "optimization_window": {
            "start": optimization_start.isoformat(),
            "end": full_end.isoformat(),
            "first_partial_utc_day": proxy.first_partial_utc_day.isoformat(),
        },
        "splits": {
            split.name: {"start": split.start.isoformat(), "end": split.end.isoformat()}
            for split in splits
        },
        "fixed_live_settings": optimizer.LIVE_FIXED_SETTINGS,
        "entry_time_exclusion": {
            "window": exclusion.window_text if exclusion else "none",
            "timezone": exclusion.timezone_label if exclusion else "UTC",
            "offset_hours": exclusion.offset_hours if exclusion else 0,
            "interval": "[start, end)" if exclusion else "none",
            "applies_to": "filled entry_at",
        },
        "optimized_parameters": [
            "impulse_window_buckets",
            "confirmation_buckets",
            "min_return_pct",
            "min_imbalance",
            "min_intensity",
            f"min_{args.volume_feature}",
            "cooldown_buckets",
        ],
        "parameter_grid": {
            "impulse_window_buckets": list(IMPULSE_WINDOWS),
            "confirmation_buckets": list(CONFIRMATIONS),
            "min_return_pct": [float(value) * 100.0 for value in MIN_RETURNS],
            "min_imbalance": [float(value) for value in MIN_IMBALANCES],
            "min_intensity": [float(value) for value in MIN_INTENSITIES],
            "min_volume_ratio": [float(value) for value in VOLUME_THRESHOLDS],
            "cooldown_buckets": list(COOLDOWNS),
            "candidate_count": len(grid_rows),
        },
        "top10_proxy": {
            "mode": "local_available_symbols_positive_utc_day_return",
            "source_symbol_count": proxy.source_symbol_count,
            "first_valid_at": proxy.first_valid_at.isoformat(),
            "exact_live_universe_snapshots_available": False,
        },
        "local_live_signal_metadata": optimizer.read_live_signal_metadata(
            args.live_signals
        ),
        "event_pools": raw_pool_counts,
        "replayed_static_eligible_events": len(all_rows),
        "skipped_static_events": skipped_static,
        "baseline": baseline_report,
        "best_validation": best_report,
        "profiles": {
            name: {
                "label": PROFILE_SPECS[name]["label"],
                **profile_reports[name],
            }
            for name in PROFILE_SPECS
        },
        "top_candidates": ranked[:20],
        "assumptions": {
            "entry_fill": "effective live long LIMIT touched by a later local 15s low before the 900s TTL; fill at the limit price",
            "exit_replay": "complete local 15s bars aggregated to 15m; first eligible bearish candle, direct +0.10% close or +0.88% recovery limit with B8",
            "capital_model": "independent 100U positions in Hedge Mode; margin is a candidate-level natural peak, not a post-selection entry throttle",
            "funding": "not modeled from the local market-state export",
            "optimization": "all seven dimensions are jointly enumerated; volume threshold is not a post-hoc filter",
        },
    }
    (output / "optimization_report.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )

    best_full = best_report["metrics"]["full"]
    markdown_lines = [
        "# 七维联合寻优：" + args.volume_feature,
        "",
        f"- 模式：`{manifest['evaluation_mode']}`；完整搜索 7 个维度，共 `{len(grid_rows):,}` 组。",
        f"- 特征：最近 `{recent_buckets * 15 // 60:g}` 分钟平均成交额 / 前 `{baseline_buckets * 15 // 60:g}` 分钟平均成交额。",
        (
            f"- 过滤：实际成交 `entry_at` 的 `{exclusion.window_text}`（{exclusion.timezone_label}，左闭右开）。"
            if exclusion
            else "- 过滤：无入场时间排除（全天 24 小时常驻运行）。"
        ),
        f"- 数据窗口：`{full_start.isoformat()}` 至 `{full_end.isoformat()}`；有效寻优起点 `{optimization_start.isoformat()}`。",
        f"- 可用状态：`{len(states):,}` 条，币种 `{len({state.symbol for state in states})}` 个，连续片段 `{len(segments):,}` 个。",
        "",
        "## 主选择结果",
        "",
        f"- 参数：`{json.dumps(best_config, ensure_ascii=False)}`。",
        f"- 全窗口净 PnL：`{best_full['net_pnl_usdt']}`U；最大回撤：`{best_full['max_drawdown_usdt']}`U；自然峰值保证金：`{best_report['natural_initial_margin_peak_usdt']}`U。",
        "",
        "## A–G 约束结果",
        "",
        "| 组别 | 约束 | 参数 | 全量 PnL | 全量最大回撤 | 峰值保证金 |",
        "|---|---|---|---:|---:|---:|",
    ]
    for name, spec in PROFILE_SPECS.items():
        report = profile_reports[name]
        full = report["metrics"]["full"]
        config = report["config"]
        markdown_lines.append(
            f"| {name} | {spec['label']} | `{config['impulse_window_buckets']}/{config['confirmation_buckets']}/{config['min_return_pct']:.2f}%/{config['min_imbalance']:.2f}/{config['min_intensity']:.1f}/{config['min_volume_ratio']:.2f}x/{config['cooldown_buckets']}` | {float(full['net_pnl_usdt'] or 0):+.2f}U | {float(full['max_drawdown_usdt'] or 0):.2f}U | {report['natural_initial_margin_peak_usdt']:.2f}U |"
        )
    baseline_full = baseline_report["metrics"]["full"]
    markdown_lines += [
        "",
        "## 基线",
        "",
        f"- 基线：`{json.dumps(BASELINE, ensure_ascii=False)}`；全量净 PnL `{baseline_full['net_pnl_usdt']}`U，最大回撤 `{baseline_full['max_drawdown_usdt']}`U，峰值保证金 `{baseline_report['natural_initial_margin_peak_usdt']}`U。",
        "",
        "## 说明",
        "",
        "- 这次的特征阈值与其余六个参数在同一个网格中联合选择，不是先选六维参数再事后套过滤器。",
        "- `notional_5m_vs_30m` 的既有七维结果仍以原目录为准；本目录只新增本特征的独立全网格结果。",
        "- 本地研究仍使用本地币种集合的因果 Top10 代理，不能等同于完整历史实盘候选宇宙。",
        "",
        "Artifacts: `optimization_report.json`, `optimization_report.md`, `grid_results.csv`, `top_candidates.csv`, `profile_*_events.csv`, `profile_equity_series.csv`.",
    ]
    (output / "optimization_report.md").write_text(
        "\n".join(markdown_lines) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "phase": "complete",
                "feature": args.volume_feature,
                "output_dir": str(output),
                "grid_rows": len(grid_rows),
                "replayed_static_eligible_events": len(all_rows),
                "base_combos": base_combos,
                "best_config": best_config,
                "best_full": best_full,
                "profiles": {
                    name: {
                        "config": profile_reports[name]["config"],
                        "full_pnl": profile_reports[name]["metrics"]["full"][
                            "net_pnl_usdt"
                        ],
                        "full_drawdown": profile_reports[name]["metrics"]["full"][
                            "max_drawdown_usdt"
                        ],
                        "margin": profile_reports[name][
                            "natural_initial_margin_peak_usdt"
                        ],
                    }
                    for name in PROFILE_SPECS
                },
            },
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
