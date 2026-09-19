#!/usr/bin/env python3
"""Optimize only the live order-flow entry parameters on local exports.

The live execution and exit settings are intentionally constants in this
script.  The six searched dimensions are the order-flow impulse window,
confirmation window, minimum directional return, minimum imbalance, minimum
notional intensity, and symbol cooldown.

This is a local research replay, not an exchange/account simulator.  It uses
the production event detector, a causal Top-10 proxy built from the symbols in
the local research export, the live limit-entry rule, and a replay of the live
15-minute bearish-candle + B8 exit.  The exported research data does not
contain the full historical universe-ranking snapshots, so the proxy is
explicitly reported instead of being presented as the exact live universe.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import gzip
import itertools
import json
import math
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

# Sibling analysis scripts are intentionally reusable, but are not installed
# as a package.  This keeps the command runnable from the repository root.
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from analyze_breakout_acceptance import (  # noqa: E402
    Candle15,
    SameExitConfig,
    Series,
    simulate_same_exit,
)
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
    OrderFlowImpulseEvent,
)

LIVE_FIXED_SETTINGS: dict[str, object] = {
    "strategy": "orderflow_impulse",
    "market_environment": "research",
    "entry_long_only": True,
    "entry_positive_gainer_top_count": 10,
    "entry_price_above_ema5": False,
    "entry_price_above_ema10": False,
    "entry_order_type": "limit",
    "entry_limit_ttl_seconds": 900,
    "hedge_mode": True,
    "entry_notional_usdt": 100.0,
    "entry_leverage": 5,
    "exit_mode": "candle_15m",
    "take_profit_pct": 0.02,
    "stop_loss_pct": 0.01,
    "candle_confirmation_count": 1,
    "candle_grace_bars": 8,
    "candle_grace_decision_profit_pct": 0.001,
    "candle_grace_profit_pct": 0.0088,
    "fee_rate_per_side": 0.0005,
}
LIVE_CONFIG_HASH = "5c79ca3444961fa71e1066ee769224d33bba28e3041d64ced7de2f617b8e6084"

OPTIMIZED_PARAMETERS = (
    "impulse_window_buckets",
    "confirmation_buckets",
    "min_return_pct",
    "min_imbalance",
    "min_intensity",
    "min_notional_5m_vs_30m",
    "cooldown_buckets",
)

VOLUME_RATIO_RECENT_BUCKETS = 20
VOLUME_RATIO_BASELINE_BUCKETS = 120
VOLUME_RATIO_TOTAL_BUCKETS = (
    VOLUME_RATIO_RECENT_BUCKETS + VOLUME_RATIO_BASELINE_BUCKETS
)


@dataclass(frozen=True, slots=True)
class ProxyTop10:
    """Causal local-universe Top-N membership snapshots."""

    times: tuple[datetime, ...]
    members: tuple[frozenset[str], ...]
    first_valid_at: datetime
    source_symbol_count: int
    first_partial_utc_day: date

    def allows(self, symbol: str, observed_at: datetime) -> bool:
        index = bisect.bisect_right(self.times, observed_at) - 1
        if index < 0 or observed_at < self.first_valid_at:
            return False
        # Do not use stale rankings older than 5 minutes
        if (observed_at - self.times[index]) > timedelta(minutes=5):
            return False
        return symbol in self.members[index]


@dataclass(frozen=True, slots=True)
class EventObservation:
    event: OrderFlowImpulseEvent
    confirmation_min_imbalance: Decimal | None
    top10_proxy_allowed: bool
    notional_5m_vs_30m: Decimal | None = None


@dataclass(frozen=True, slots=True)
class SimulatedEvent:
    observation: EventObservation
    trade: dict[str, Any] | None
    fill_reason: str


@dataclass(frozen=True, slots=True)
class EntryTimeExclusion:
    """Exclude filled entries inside a local-time half-open clock window."""

    start_minute: int
    end_minute: int
    timezone_label: str
    offset_hours: int

    def excludes(self, simulation: SimulatedEvent) -> bool:
        trade = simulation.trade
        if trade is None or trade.get("entry_epoch") is None:
            return False
        entry_at = datetime.fromtimestamp(float(trade["entry_epoch"]), tz=UTC)
        local_at = entry_at + timedelta(hours=self.offset_hours)
        local_minute = local_at.hour * 60 + local_at.minute
        return self.start_minute <= local_minute < self.end_minute

    @property
    def window_text(self) -> str:
        def clock(value: int) -> str:
            return f"{value // 60:02d}:{value % 60:02d}"

        return f"{clock(self.start_minute)}–{clock(self.end_minute)}"


@dataclass(frozen=True, slots=True)
class NoEntryTimeExclusion:
    """Null exclusion policy when no entry time window is excluded."""

    window_text: str = "none"
    timezone_label: str = "UTC"
    offset_hours: int = 0

    def excludes(self, simulation: Any) -> bool:
        return False


def parse_entry_time_window(value: str | None) -> tuple[int, int] | None:
    if value is None:
        return None
    raw = value.strip()
    if not raw or raw.lower() in {"none", "off", "disable", "disabled", "false"}:
        return None
    parts = raw.split("-", 1)
    if len(parts) != 2:
        raise SystemExit("--exclude-entry-hours must use HH:MM-HH:MM or 'none'")

    def parse_clock(raw: str) -> int:
        clock_parts = raw.strip().split(":", 1)
        if len(clock_parts) != 2:
            raise SystemExit("--exclude-entry-hours must use HH:MM-HH:MM")
        try:
            hour = int(clock_parts[0])
            minute = int(clock_parts[1])
        except ValueError as exc:
            raise SystemExit("--exclude-entry-hours must use HH:MM-HH:MM") from exc
        if not 0 <= hour <= 23 or not 0 <= minute <= 59:
            raise SystemExit("--exclude-entry-hours contains an invalid clock time")
        return hour * 60 + minute

    start, end = (parse_clock(part) for part in parts)
    if start >= end:
        raise SystemExit("--exclude-entry-hours must have start time before end time")
    return start, end


def state_price(state: MarketState15s) -> Decimal | None:
    if state.close_price is not None:
        return state.close_price
    if state.midpoint is not None:
        return state.midpoint
    return state.mark_price


def as_series(states: list[MarketState15s]) -> Series:
    rows: list[
        tuple[float, float, float, float, float, float, float, int, bool, int, float]
    ] = []
    for state in sorted(states, key=lambda item: item.bucket_start):
        price = state_price(state)
        if price is None or price <= 0:
            continue
        high = state.high_price if state.high_price is not None else price
        low = state.low_price if state.low_price is not None else price
        open_price = state.open_price if state.open_price is not None else price
        rows.append(
            (
                state.bucket_start.timestamp(),
                float(high),
                float(low),
                float(price),
                float(state.trade_notional),
                float(state.aggressive_buy_notional),
                float(state.aggressive_sell_notional),
                state.trade_count,
                state.data_complete,
                state.missing_agg_trade_count,
                float(open_price),
            )
        )
    return Series(rows=rows, times=[row[0] for row in rows])


def complete_candles(states: list[MarketState15s]) -> list[Candle15]:
    """Aggregate only complete 60-bucket 15m bars from the local states."""

    grouped: defaultdict[float, dict[float, MarketState15s]] = defaultdict(dict)
    for state in states:
        grouped[math.floor(state.bucket_start.timestamp() / 900.0) * 900.0][
            state.bucket_start.timestamp()
        ] = state

    candles: list[Candle15] = []
    for start, by_time in sorted(grouped.items()):
        expected = [start + 15.0 * index for index in range(60)]
        if any(timestamp not in by_time for timestamp in expected):
            continue
        ordered = [by_time[timestamp] for timestamp in expected]
        prices = [state_price(state) for state in ordered]
        if any(price is None or price <= 0 for price in prices):
            continue
        usable_prices = [price for price in prices if price is not None]
        first = ordered[0]
        open_price = first.open_price or usable_prices[0]
        highs = [state.high_price or state_price(state) for state in ordered]
        lows = [state.low_price or state_price(state) for state in ordered]
        if any(value is None for value in highs + lows):
            continue
        candles.append(
            Candle15(
                start=start,
                end=start + 900.0,
                open=float(open_price),
                high=max(float(value) for value in highs if value is not None),
                low=min(float(value) for value in lows if value is not None),
                close=float(usable_prices[-1]),
                state_count=len(ordered),
            )
        )
    return candles


def build_top10_proxy(
    states: list[MarketState15s],
    *,
    top_count: int,
) -> ProxyTop10:
    """Build point-in-time positive-gainer ranks from local symbols only.

    The first local UTC day is partial.  Membership on that day is not used
    for the optimizer; the full comparison chart may still start earlier and
    simply shows zero research PnL until the proxy becomes valid.
    """

    by_time: defaultdict[datetime, list[MarketState15s]] = defaultdict(list)
    for state in states:
        by_time[state.bucket_start].append(state)
    times = sorted(by_time)
    if not times:
        raise ValueError("cannot build Top10 proxy without states")

    first_partial_day = times[0].date()
    first_valid_at = datetime.combine(
        first_partial_day + timedelta(days=1),
        datetime.min.time(),
        tzinfo=UTC,
    )
    latest: dict[str, Decimal] = {}
    daily_open: dict[str, Decimal] = {}
    current_day: date | None = None
    snapshot_times: list[datetime] = []
    snapshot_members: list[frozenset[str]] = []

    for timestamp in times:
        day = timestamp.date()
        if current_day != day:
            current_day = day
            latest = {}
            daily_open = {}
        for state in by_time[timestamp]:
            price = state_price(state)
            if price is None or price <= 0:
                continue
            latest[state.symbol] = price
            daily_open.setdefault(state.symbol, price)
        ranked = sorted(
            (
                (price / daily_open[symbol] - Decimal("1"), symbol)
                for symbol, price in latest.items()
                if symbol in daily_open and price > daily_open[symbol]
            ),
            key=lambda item: (-item[0], item[1]),
        )
        snapshot_times.append(timestamp)
        snapshot_members.append(
            frozenset(symbol for _return, symbol in ranked[:top_count])
        )

    return ProxyTop10(
        times=tuple(snapshot_times),
        members=tuple(snapshot_members),
        first_valid_at=first_valid_at,
        source_symbol_count=len({state.symbol for state in states}),
        first_partial_utc_day=first_partial_day,
    )


def build_true_top10_proxy_from_parquet(
    parquet_root: Path,
    *,
    top_count: int = 10,
    environment: str = "research",
) -> ProxyTop10 | None:
    """Build exact Top-N universe proxy from live-recorded gainer_rank in Parquet.

    Returns None if no parquet files contain the gainer_rank column or
    no valid ranks exist.
    """
    import pyarrow.parquet as pq

    files = sorted(parquet_root.rglob("*.parquet"))
    if not files:
        return None

    by_time: defaultdict[datetime, set[str]] = defaultdict(set)
    found_any = False
    for path in files:
        try:
            pf = pq.ParquetFile(path)
            schema_names = set(pf.schema.names)
            if "gainer_rank" not in schema_names or "bucket_start" not in schema_names:
                continue
            cols = ["symbol", "bucket_start", "gainer_rank", "environment"]
            table = pf.read(columns=cols)
            df = table.to_pandas()
            if environment and "environment" in df.columns:
                df = df[df["environment"].astype(str) == environment]
            # Ensure every timestamp observed in parquet has an entry even if no symbol is in top_count
            for t in df["bucket_start"].dropna().unique():
                by_time.setdefault(t, set())

            valid = df[df["gainer_rank"].notna() & (df["gainer_rank"] <= top_count)]
            if len(valid) > 0:
                found_any = True
                s_list = list(valid["symbol"])
                t_list = list(valid["bucket_start"])
                for s, t in zip(s_list, t_list, strict=False):
                    by_time[t].add(str(s))
        except Exception:
            continue

    if not found_any or not by_time:
        return None

    times = sorted(by_time)
    members = tuple(frozenset(by_time[t]) for t in times)
    first_day = times[0].date()
    return ProxyTop10(
        times=tuple(times),
        members=members,
        first_valid_at=times[0],
        source_symbol_count=len(set().union(*members)),
        first_partial_utc_day=first_day,
    )



def confirmation_minimum(
    event: OrderFlowImpulseEvent,
    *,
    confirmation_buckets: int,
    state_by_key: dict[tuple[str, datetime], MarketState15s],
) -> Decimal | None:
    start = event.detected_at - BUCKET * (confirmation_buckets - 1)
    values: list[Decimal] = []
    for offset in range(confirmation_buckets):
        state = state_by_key.get((event.symbol, start + BUCKET * offset))
        if state is None:
            return None
        total = state.aggressive_buy_notional + state.aggressive_sell_notional
        imbalance = (
            Decimal("0")
            if total == 0
            else (state.aggressive_buy_notional - state.aggressive_sell_notional)
            / total
        )
        values.append(
            imbalance if event.direction is OrderFlowDirection.UP else -imbalance
        )
    return min(values) if values else None


def build_notional_volume_ratio_lookup(
    states_by_symbol: dict[str, list[MarketState15s]],
) -> dict[tuple[str, datetime], Decimal]:
    """Build the causal 5m-vs-30m notional-volume ratio for each state.

    The recent window includes the detected 15-second bucket and the prior
    19 buckets.  The baseline is the immediately preceding 120 buckets.  A
    value is emitted only when all 140 buckets are consecutive, so a collector
    gap cannot turn into a false volume spike.
    """

    lookup: dict[tuple[str, datetime], Decimal] = {}
    expected_span = BUCKET * (VOLUME_RATIO_TOTAL_BUCKETS - 1)
    for symbol, symbol_states in states_by_symbol.items():
        ordered = sorted(symbol_states, key=lambda item: item.bucket_start)
        prefix: list[Decimal] = [Decimal("0")]
        for state in ordered:
            prefix.append(prefix[-1] + state.trade_notional)
        for index, state in enumerate(ordered):
            first_index = index - VOLUME_RATIO_TOTAL_BUCKETS + 1
            if first_index < 0:
                continue
            if state.bucket_start - ordered[first_index].bucket_start != expected_span:
                continue
            recent_start = index + 1 - VOLUME_RATIO_RECENT_BUCKETS
            baseline_start = first_index
            baseline_end = recent_start
            recent_total = prefix[index + 1] - prefix[recent_start]
            baseline_total = prefix[baseline_end] - prefix[baseline_start]
            if baseline_total <= 0:
                continue
            lookup[(symbol, state.bucket_start)] = (
                recent_total * Decimal(VOLUME_RATIO_BASELINE_BUCKETS)
                / (baseline_total * Decimal(VOLUME_RATIO_RECENT_BUCKETS))
            )
    return lookup


def event_entry_price(
    event: OrderFlowImpulseEvent,
    state_by_key: dict[tuple[str, datetime], MarketState15s],
) -> tuple[Decimal | None, datetime]:
    """Apply the live long LIMIT price rule at signal creation.

    The production event is detected on the source state bucket; the signal
    and order are created at that state's bucket end.
    """

    entry_at = event.detected_at + BUCKET
    state = state_by_key.get((event.symbol, event.detected_at))
    if state is None:
        return None, entry_at
    close = state.close_price or state.midpoint or state.mark_price
    ask = state.last_ask_price
    if close is None or close <= 0:
        return None, entry_at
    if ask is not None and ask > 0:
        return min(ask, close), entry_at
    return close, entry_at


def simulate_live_limit_event(
    observation: EventObservation,
    *,
    states_by_symbol: dict[str, list[MarketState15s]],
    series_by_symbol: dict[str, Series],
    candles_by_symbol: dict[str, list[Candle15]],
    state_by_key: dict[tuple[str, datetime], MarketState15s],
    exit_config: SameExitConfig,
    data_end: datetime,
) -> SimulatedEvent:
    event = observation.event
    entry_price, order_created_at = event_entry_price(event, state_by_key)
    if entry_price is None or entry_price <= 0:
        return SimulatedEvent(observation, None, "missing_entry_price")
    deadline = order_created_at + timedelta(
        seconds=int(LIVE_FIXED_SETTINGS["entry_limit_ttl_seconds"])
    )
    fill_state: MarketState15s | None = None
    for state in states_by_symbol.get(event.symbol, ()):
        if state.bucket_start < order_created_at:
            continue
        if state.bucket_start >= deadline:
            break
        low = state.low_price or state_price(state)
        if low is not None and low <= entry_price:
            fill_state = state
            break
    if fill_state is None:
        return SimulatedEvent(observation, None, "limit_not_filled_before_ttl")

    entry_at = fill_state.bucket_end
    event_row = {
        "scenario": "live_fixed_settings",
        "label": "live-fixed-settings",
        "entry_mode": "limit",
        "signal_id": f"local-{event.symbol}-{event.detected_at.isoformat()}",
        "symbol": event.symbol,
        "entry_epoch": entry_at.timestamp(),
        "entry_price": float(entry_price),
    }
    series = series_by_symbol.get(event.symbol)
    candles = candles_by_symbol.get(event.symbol)
    if series is None or not series.rows or not candles:
        return SimulatedEvent(observation, None, "missing_exit_path")
    trade = simulate_same_exit(event_row, series, candles, exit_config)
    if not trade["closed"]:
        # Open positions are kept as a marked observation for the final curve,
        # but excluded from train/validation/holdout selection scores.
        return SimulatedEvent(observation, trade, "open_at_data_end")
    if (
        trade.get("exit_epoch") is not None
        and trade["exit_epoch"] > data_end.timestamp()
    ):
        return SimulatedEvent(observation, None, "exit_after_data_end")
    return SimulatedEvent(observation, trade, "filled")


def directional_imbalance(event: OrderFlowImpulseEvent) -> Decimal:
    if event.direction is OrderFlowDirection.UP:
        return event.aggressive_imbalance
    return -event.aggressive_imbalance


def select_observations(
    observations: list[EventObservation],
    *,
    min_return: Decimal,
    min_imbalance: Decimal,
    min_intensity: Decimal,
    cooldown_buckets: int,
    min_notional_5m_vs_30m: Decimal = Decimal("0"),
    simulated: dict[int, SimulatedEvent] | None = None,
    entry_time_exclusion: EntryTimeExclusion | None = None,
) -> list[EventObservation]:
    if entry_time_exclusion is not None and simulated is None:
        raise ValueError("simulated events are required for entry-time exclusion")
    selected: list[EventObservation] = []
    last_selected: dict[str, datetime] = {}
    cooldown = BUCKET * cooldown_buckets
    for observation in observations:
        event = observation.event
        if event.direction is not OrderFlowDirection.UP:
            continue
        if (
            entry_time_exclusion is not None
            and simulated is not None
            and entry_time_exclusion.excludes(simulated[id(observation)])
        ):
            continue
        if not observation.top10_proxy_allowed:
            continue
        if event.impulse_return_pct < min_return:
            continue
        if directional_imbalance(event) < min_imbalance:
            continue
        if (
            observation.confirmation_min_imbalance is None
            or observation.confirmation_min_imbalance < min_imbalance
        ):
            continue
        if event.notional_intensity < min_intensity:
            continue
        if min_notional_5m_vs_30m > 0 and (
            observation.notional_5m_vs_30m is None
            or observation.notional_5m_vs_30m < min_notional_5m_vs_30m
        ):
            continue
        previous = last_selected.get(event.symbol)
        if previous is not None and event.detected_at <= previous + cooldown:
            continue
        selected.append(observation)
        last_selected[event.symbol] = event.detected_at
    return selected


def initial_margin_peak(
    selected: list[EventObservation],
    simulated: dict[int, SimulatedEvent],
) -> float:
    """Return the natural peak initial margin of a parameter-set replay.

    Every selected observation is replayed.  No entry is rejected when the
    account gets crowded; the peak is used only to decide whether the whole
    parameter set is feasible under an ex-ante margin constraint.
    """
    margin_per_entry = float(LIVE_FIXED_SETTINGS["entry_notional_usdt"]) / float(
        LIVE_FIXED_SETTINGS["entry_leverage"]
    )
    events: list[tuple[float, int]] = []
    for observation in selected:
        trade = simulated[id(observation)].trade
        if trade is None:
            continue
        entry_epoch = float(trade["entry_epoch"])
        events.append((entry_epoch, 1))
        if trade.get("closed") and trade.get("exit_epoch") is not None:
            events.append((float(trade["exit_epoch"]), -1))
    current_entries = 0
    peak_entries = 0
    # Process exits before entries at the same instant.  This models a
    # position being released before a simultaneous replacement entry.
    for _timestamp, delta in sorted(events, key=lambda item: (item[0], item[1])):
        current_entries += delta
        peak_entries = max(peak_entries, current_entries)
    return round(peak_entries * margin_per_entry, 8)


def metric(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, 8)


def trade_net_return_pct(trade: dict[str, Any]) -> float:
    return (
        float(trade["net_pnl_usdt"])
        / float(LIVE_FIXED_SETTINGS["entry_notional_usdt"])
        * 100.0
    )


def metrics_for_split(
    selected: list[EventObservation],
    simulated: dict[int, SimulatedEvent],
    *,
    split: Split,
) -> dict[str, float | int | None]:
    closed: list[dict[str, Any]] = []
    n_limit_unfilled = 0
    n_open = 0
    for observation in selected:
        simulation = simulated[id(observation)]
        trade = simulation.trade
        if trade is None:
            if simulation.fill_reason == "limit_not_filled_before_ttl":
                n_limit_unfilled += 1
            continue
        entry_at = datetime.fromtimestamp(float(trade["entry_epoch"]), tz=UTC)
        if not (split.start <= entry_at < split.end):
            continue
        if not trade["closed"]:
            n_open += 1
            continue
        exit_at = datetime.fromtimestamp(float(trade["exit_epoch"]), tz=UTC)
        if exit_at >= split.end:
            continue
        closed.append(trade)

    returns = [trade_net_return_pct(trade) for trade in closed]
    pnls = [float(trade["net_pnl_usdt"]) for trade in closed]
    gains = sum(value for value in pnls if value > 0)
    losses = -sum(value for value in pnls if value < 0)
    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0
    for pnl in pnls:
        cumulative += pnl
        peak = max(peak, cumulative)
        max_dd = max(max_dd, peak - cumulative)
    return {
        "n_selected": len(selected),
        "n_closed": len(closed),
        "n_open_at_data_end": n_open,
        "n_limit_unfilled": n_limit_unfilled,
        "net_pnl_usdt": metric(sum(pnls)),
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


def all_metrics(
    selected: list[EventObservation],
    simulated: dict[int, SimulatedEvent],
    *,
    splits: tuple[Split, ...],
) -> dict[str, dict[str, float | int | None]]:
    return {
        split.name: metrics_for_split(selected, simulated, split=split)
        for split in splits
    }


def build_pnl_series(
    named_selected: dict[str, list[EventObservation]],
    simulated: dict[int, SimulatedEvent],
    *,
    start: datetime,
    end: datetime,
) -> list[dict[str, object]]:
    deltas: defaultdict[datetime, dict[str, float]] = defaultdict(dict)
    for name, selected in named_selected.items():
        for observation in selected:
            trade = simulated[id(observation)].trade
            if trade is None or not trade["closed"]:
                continue
            exit_at = datetime.fromtimestamp(float(trade["exit_epoch"]), tz=UTC)
            if not (start <= exit_at < end):
                continue
            deltas[exit_at][name] = deltas[exit_at].get(name, 0.0) + float(
                trade["net_pnl_usdt"]
            )
    names = tuple(named_selected)
    cumulative = {name: 0.0 for name in names}
    rows: list[dict[str, object]] = [
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
        rows.append(row)
    if rows[-1]["timestamp"] != end.isoformat():
        rows.append(
            {
                "timestamp": end.isoformat(),
                **{
                    f"{name}_cumulative_pnl_usdt": round(cumulative[name], 8)
                    for name in names
                },
            }
        )
    return rows


def event_csv_row(
    observation: EventObservation,
    simulation: SimulatedEvent,
) -> dict[str, object]:
    event = observation.event
    trade = simulation.trade or {}
    return {
        "symbol": event.symbol,
        "direction": event.direction.value,
        "detected_at": event.detected_at.isoformat(),
        "order_created_at": (event.detected_at + BUCKET).isoformat(),
        "impulse_return_pct": float(event.impulse_return_pct) * 100.0,
        "aggressive_imbalance": float(directional_imbalance(event)),
        "confirmation_min_imbalance": (
            None
            if observation.confirmation_min_imbalance is None
            else float(observation.confirmation_min_imbalance)
        ),
        "notional_intensity": float(event.notional_intensity),
        "notional_5m_vs_30m": (
            None
            if observation.notional_5m_vs_30m is None
            else float(observation.notional_5m_vs_30m)
        ),
        "top10_proxy_allowed": observation.top10_proxy_allowed,
        "fill_reason": simulation.fill_reason,
        "entry_at": (None if trade.get("entry_at") is None else trade.get("entry_at")),
        "entry_price": trade.get("entry_price"),
        "exit_at": trade.get("exit_at"),
        "exit_price": trade.get("exit_price"),
        "exit_reason": trade.get("exit_reason"),
        "closed": trade.get("closed"),
        "net_pnl_usdt": trade.get("net_pnl_usdt"),
        "net_return_pct": trade.get("net_return_pct"),
    }


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def read_live_signal_metadata(path: Path | None) -> dict[str, object]:
    if path is None or not path.exists():
        return {"available": False}
    counts: defaultdict[str, int] = defaultdict(int)
    first: str | None = None
    last: str | None = None
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("config_hash") != LIVE_CONFIG_HASH:
                continue
            try:
                context = json.loads(row.get("filter_context") or "{}")
            except json.JSONDecodeError:
                context = {}
            pool = context.get("entry_symbol_pool_size")
            counts[str(pool)] += 1
            timestamp = row.get("detected_at")
            if timestamp:
                first = timestamp if first is None or timestamp < first else first
                last = timestamp if last is None or timestamp > last else last
    return {
        "available": True,
        "config_hash": LIVE_CONFIG_HASH,
        "pool_size_counts": dict(counts),
        "first_signal_at": first,
        "last_signal_at": last,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-root",
        type=Path,
        default=Path("server_exports/cml-research-data-20260905/parquet"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(
            "server_exports/cml-research-data-20260905/"
            "optimization-live-constrained-20260905"
        ),
    )
    parser.add_argument(
        "--live-signals",
        type=Path,
        default=Path(
            "server_exports/cml-live-current-20260905/live_strategy_signals.csv.gz"
        ),
    )
    parser.add_argument("--environment", default="research")
    parser.add_argument("--top-count", type=int, default=10)
    parser.add_argument("--fee-rate", type=float, default=0.0005)
    parser.add_argument("--min-validation-trades", type=int, default=10)
    parser.add_argument(
        "--selection-scope",
        choices=("validation", "full"),
        default="validation",
        help=(
            "period used to select the winning parameter set; validation keeps "
            "the original walk-forward objective, full selects on the complete "
            "optimization window"
        ),
    )
    parser.add_argument(
        "--drawdown-weight",
        type=float,
        default=0.10,
        help=(
            "penalty weight in the selected period's PnL - weight * max drawdown; "
            "0.10 means 1U PnL is worth 10U drawdown"
        ),
    )
    parser.add_argument(
        "--max-initial-margin-usdt",
        type=float,
        default=None,
        help="optional ex-ante cap on each parameter set's natural peak initial margin",
    )
    parser.add_argument(
        "--fixed-cooldown-buckets",
        type=int,
        default=None,
        help="optionally hold cooldown_buckets fixed while searching",
    )
    parser.add_argument(
        "--exclude-entry-hours",
        default=None,
        metavar="HH:MM-HH:MM",
        help=(
            "exclude filled entries in this half-open local-time window, "
            "for example 08:00-10:00"
        ),
    )
    parser.add_argument(
        "--exclude-entry-timezone",
        choices=("UTC", "Asia/Shanghai"),
        default="Asia/Shanghai",
        help="timezone for --exclude-entry-hours (default: Asia/Shanghai)",
    )
    return parser.parse_args()


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
    if args.fixed_cooldown_buckets is not None and args.fixed_cooldown_buckets < 0:
        raise SystemExit("--fixed-cooldown-buckets must not be negative")
    parsed_entry_window = parse_entry_time_window(args.exclude_entry_hours)
    entry_time_exclusion = None
    if parsed_entry_window is not None:
        timezone_offsets = {"UTC": 0, "Asia/Shanghai": 8}
        entry_time_exclusion = EntryTimeExclusion(
            start_minute=parsed_entry_window[0],
            end_minute=parsed_entry_window[1],
            timezone_label=args.exclude_entry_timezone,
            offset_hours=timezone_offsets[args.exclude_entry_timezone],
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

    impulse_windows = (2, 3, 4)
    confirmations = (1, 2, 3)
    min_returns = tuple(
        Decimal(str(value)) / 100 for value in (0.50, 0.75, 1.00, 1.25, 1.50)
    )
    min_imbalances = tuple(Decimal(str(value)) for value in (0.30, 0.40, 0.50, 0.60))
    min_intensities = tuple(Decimal(str(value)) for value in (1.5, 2.0, 3.0, 4.0))
    min_notional_5m_vs_30m = tuple(
        Decimal(str(value)) for value in (0.0, 1.0, 1.25, 1.5, 2.0, 3.0, 4.0)
    )
    cooldowns = (
        (args.fixed_cooldown_buckets,)
        if args.fixed_cooldown_buckets is not None
        else (0, 4, 8, 16, 32)
    )
    horizons = (1,)

    minimum_buckets = max(max(impulse_windows) + 4, 8) + max(confirmations)
    segments = split_contiguous_states(states, minimum_buckets=minimum_buckets)
    if not segments:
        raise SystemExit("no contiguous local research state segments found")
    full_start = min(segment[0].bucket_start for segment in segments)
    full_end = max(segment[-1].bucket_end for segment in segments)
    proxy = build_top10_proxy(states, top_count=args.top_count)
    optimization_start = proxy.first_valid_at
    if optimization_start >= full_end:
        raise SystemExit("local data has no valid full UTC-day Top10 proxy window")
    optimization_duration = full_end - optimization_start
    train_end = optimization_start + optimization_duration * 0.60
    validation_end = optimization_start + optimization_duration * 0.80
    splits = (
        Split("train", optimization_start, train_end),
        Split("validation", train_end, validation_end),
        Split("holdout", validation_end, full_end),
    )

    series_by_symbol = {
        symbol: as_series(symbol_states)
        for symbol, symbol_states in states_by_symbol.items()
    }
    candles_by_symbol = {
        symbol: complete_candles(symbol_states)
        for symbol, symbol_states in states_by_symbol.items()
    }
    exit_config = SameExitConfig(
        grace_bars=int(LIVE_FIXED_SETTINGS["candle_grace_bars"]),
        decision_profit_pct=float(
            LIVE_FIXED_SETTINGS["candle_grace_decision_profit_pct"]
        ),
        recovery_profit_pct=float(LIVE_FIXED_SETTINGS["candle_grace_profit_pct"]),
        fee_rate=args.fee_rate,
        notional_usdt=float(LIVE_FIXED_SETTINGS["entry_notional_usdt"]),
    )
    notional_volume_ratios = build_notional_volume_ratio_lookup(states_by_symbol)

    pools: dict[tuple[int, int], list[EventObservation]] = {}
    simulated: dict[int, SimulatedEvent] = {}
    for impulse_window, confirmation in itertools.product(
        impulse_windows, confirmations
    ):
        events = build_pool(
            segments,
            impulse_window_buckets=impulse_window,
            confirmation_buckets=confirmation,
            horizons=horizons,
        )
        observations: list[EventObservation] = []
        for event in events:
            confirmation_min = confirmation_minimum(
                event,
                confirmation_buckets=confirmation,
                state_by_key=state_by_key,
            )
            observation = EventObservation(
                event=event,
                confirmation_min_imbalance=confirmation_min,
                top10_proxy_allowed=proxy.allows(
                    event.symbol,
                    event.detected_at,
                ),
                notional_5m_vs_30m=notional_volume_ratios.get(
                    (event.symbol, event.detected_at)
                ),
            )
            observations.append(observation)
            simulated[id(observation)] = simulate_live_limit_event(
                observation,
                states_by_symbol=states_by_symbol,
                series_by_symbol=series_by_symbol,
                candles_by_symbol=candles_by_symbol,
                state_by_key=state_by_key,
                exit_config=exit_config,
                data_end=full_end,
            )
        pools[(impulse_window, confirmation)] = observations

    grid_rows: list[dict[str, object]] = []
    for (impulse_window, confirmation), observations in pools.items():
        for (
            min_return,
            min_imbalance,
            min_intensity,
            min_notional_ratio,
            cooldown,
        ) in itertools.product(
            min_returns,
            min_imbalances,
            min_intensities,
            min_notional_5m_vs_30m,
            cooldowns,
        ):
            selected = select_observations(
                observations,
                min_return=min_return,
                min_imbalance=min_imbalance,
                min_intensity=min_intensity,
                cooldown_buckets=cooldown,
                min_notional_5m_vs_30m=min_notional_ratio,
                simulated=simulated,
                entry_time_exclusion=entry_time_exclusion,
            )
            margin_peak = initial_margin_peak(selected, simulated)
            margin_feasible = (
                args.max_initial_margin_usdt is None
                or margin_peak <= args.max_initial_margin_usdt + 1e-9
            )
            metrics = all_metrics(selected, simulated, splits=splits)
            full_metrics = metrics_for_split(
                selected,
                simulated,
                split=Split("full", optimization_start, full_end),
            )
            validation = metrics["validation"]
            selection_metrics = (
                full_metrics if args.selection_scope == "full" else validation
            )
            validation_n = int(validation["n_closed"] or 0)
            validation_pnl = validation["net_pnl_usdt"]
            validation_max_drawdown = validation["max_drawdown_usdt"]
            selection_pnl = selection_metrics["net_pnl_usdt"]
            selection_max_drawdown = selection_metrics["max_drawdown_usdt"]

            def score_for(
                pnl: float | None,
                max_drawdown: float | None,
            ) -> float | None:
                if (
                    not margin_feasible
                    or validation_n < args.min_validation_trades
                    or pnl is None
                    or max_drawdown is None
                ):
                    return None
                return round(
                    float(pnl) - args.drawdown_weight * float(max_drawdown),
                    8,
                )

            validation_score = score_for(validation_pnl, validation_max_drawdown)
            selection_score = score_for(selection_pnl, selection_max_drawdown)
            row: dict[str, object] = {
                "impulse_window_buckets": impulse_window,
                "confirmation_buckets": confirmation,
                "min_return_pct": float(min_return) * 100.0,
                "min_imbalance": float(min_imbalance),
                "min_intensity": float(min_intensity),
                "min_notional_5m_vs_30m": float(min_notional_ratio),
                "cooldown_buckets": cooldown,
                "n_selected_full": len(selected),
                "initial_margin_peak_usdt": margin_peak,
                "margin_constraint_feasible": margin_feasible,
                "validation_score": validation_score,
                "selection_score": selection_score,
                "selection_scope": args.selection_scope,
                "selection_n_closed": selection_metrics["n_closed"],
                "selection_net_pnl_usdt": selection_pnl,
                "selection_max_drawdown_usdt": selection_max_drawdown,
            }
            for split_name, split_metrics in {
                **metrics,
                "full": full_metrics,
            }.items():
                for key, value in split_metrics.items():
                    row[f"{split_name}_{key}"] = value
            grid_rows.append(row)

    ranked = sorted(
        grid_rows,
        key=lambda row: (
            row["selection_score"] is not None,
            (
                float(row["selection_score"])
                if row["selection_score"] is not None
                else -float("inf")
            ),
            (
                float(row["selection_net_pnl_usdt"])
                if row["selection_net_pnl_usdt"] is not None
                else -float("inf")
            ),
            (
                -float(row["selection_max_drawdown_usdt"])
                if row["selection_max_drawdown_usdt"] is not None
                else -float("inf")
            ),
            int(row["selection_n_closed"]),
        ),
        reverse=True,
    )
    feasible_ranked = [row for row in ranked if row["selection_score"] is not None]
    if not feasible_ranked:
        raise SystemExit("parameter grid produced no feasible scored rows")
    best_row = feasible_ranked[0]

    def config_from_row(row: dict[str, object]) -> dict[str, object]:
        return {
            "impulse_window_buckets": int(row["impulse_window_buckets"]),
            "confirmation_buckets": int(row["confirmation_buckets"]),
            "min_return_pct": float(row["min_return_pct"]),
            "min_imbalance": float(row["min_imbalance"]),
            "min_intensity": float(row["min_intensity"]),
            "min_notional_5m_vs_30m": float(row["min_notional_5m_vs_30m"]),
            "cooldown_buckets": int(row["cooldown_buckets"]),
        }

    baseline = {
        "impulse_window_buckets": 3,
        "confirmation_buckets": 1,
        "min_return_pct": 1.00,
        "min_imbalance": 0.40,
        "min_intensity": 2.0,
        "min_notional_5m_vs_30m": 0.0,
        "cooldown_buckets": 0,
    }
    best_config = config_from_row(best_row)

    def observations_for_config(config: dict[str, object]) -> list[EventObservation]:
        return select_observations(
            pools[
                (
                    int(config["impulse_window_buckets"]),
                    int(config["confirmation_buckets"]),
                )
            ],
            min_return=Decimal(str(float(config["min_return_pct"]) / 100.0)),
            min_imbalance=Decimal(str(config["min_imbalance"])),
            min_intensity=Decimal(str(config["min_intensity"])),
            cooldown_buckets=int(config["cooldown_buckets"]),
            min_notional_5m_vs_30m=Decimal(
                str(config.get("min_notional_5m_vs_30m", 0.0))
            ),
            simulated=simulated,
            entry_time_exclusion=entry_time_exclusion,
        )

    named_selected = {
        "baseline": observations_for_config(baseline),
        "best_validation": observations_for_config(best_config),
    }
    named_metrics = {
        name: all_metrics(selected, simulated, splits=splits)
        for name, selected in named_selected.items()
    }

    output = args.output_dir
    output.mkdir(parents=True, exist_ok=True)
    write_csv(output / "grid_results.csv", grid_rows)
    write_csv(output / "top_candidates.csv", ranked[:50])
    write_csv(
        output / "best_candidate_events.csv",
        [
            event_csv_row(observation, simulated[id(observation)])
            for observation in named_selected["best_validation"]
        ],
    )
    write_csv(
        output / "baseline_events.csv",
        [
            event_csv_row(observation, simulated[id(observation)])
            for observation in named_selected["baseline"]
        ],
    )
    write_csv(
        output / "equity_series.csv",
        build_pnl_series(
            named_selected,
            simulated,
            start=full_start,
            end=full_end,
        ),
    )

    def report_for(name: str, config: dict[str, object]) -> dict[str, object]:
        margin_peak = initial_margin_peak(named_selected[name], simulated)
        margin_feasible = (
            args.max_initial_margin_usdt is None
            or margin_peak <= args.max_initial_margin_usdt + 1e-9
        )
        validation_report = named_metrics[name]["validation"]
        full_report = metrics_for_split(
            named_selected[name],
            simulated,
            split=Split("full", optimization_start, full_end),
        )
        selection_report = (
            full_report if args.selection_scope == "full" else validation_report
        )
        selection_pnl = selection_report["net_pnl_usdt"]
        selection_max_drawdown = selection_report["max_drawdown_usdt"]
        selection_score = (
            None
            if selection_pnl is None or selection_max_drawdown is None
            else round(
                float(selection_pnl)
                - args.drawdown_weight * float(selection_max_drawdown),
                8,
            )
        )
        return {
            "config": config,
            "natural_initial_margin_peak_usdt": margin_peak,
            "margin_constraint_feasible": margin_feasible,
            "selection_scope": args.selection_scope,
            "selection_score": selection_score,
            "metrics": {
                **named_metrics[name],
                "full": full_report,
            },
        }

    full_best = named_metrics["best_validation"]["holdout"]
    full_baseline = named_metrics["baseline"]["holdout"]
    optimized_parameters = [
        parameter
        for parameter in OPTIMIZED_PARAMETERS
        if args.fixed_cooldown_buckets is None or parameter != "cooldown_buckets"
    ]
    manifest: dict[str, object] = {
        "analysis": "live-constrained local order-flow parameter optimization",
        "selection_scope": args.selection_scope,
        "selection_objective": (
            f"{args.selection_scope}_net_pnl_usdt_minus_"
            f"{args.drawdown_weight:g}_times_{args.selection_scope}_max_drawdown_usdt"
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
            split.name: {
                "start": split.start.isoformat(),
                "end": split.end.isoformat(),
            }
            for split in splits
        },
        "fixed_live_settings": LIVE_FIXED_SETTINGS,
        "margin_constraint": {
            "mode": (
                "parameter_set_natural_peak"
                if args.max_initial_margin_usdt is not None
                else "none"
            ),
            "max_initial_margin_usdt": args.max_initial_margin_usdt,
            "initial_margin_per_entry_usdt": float(
                LIVE_FIXED_SETTINGS["entry_notional_usdt"]
            )
            / float(LIVE_FIXED_SETTINGS["entry_leverage"]),
            "max_full_entries_at_once": (
                None
                if args.max_initial_margin_usdt is None
                else math.floor(
                    args.max_initial_margin_usdt
                    / (
                        float(LIVE_FIXED_SETTINGS["entry_notional_usdt"])
                        / float(LIVE_FIXED_SETTINGS["entry_leverage"])
                    )
                )
            ),
        },
        "fixed_cooldown_buckets": args.fixed_cooldown_buckets,
        "entry_time_exclusion": (
            None
            if entry_time_exclusion is None
            else {
                "window": entry_time_exclusion.window_text,
                "timezone": entry_time_exclusion.timezone_label,
                "offset_hours": entry_time_exclusion.offset_hours,
                "interval": "[start, end)",
                "applies_to": "filled entry_at",
            }
        ),
        "optimized_parameters": optimized_parameters,
        "parameter_grid": {
            "impulse_window_buckets": list(impulse_windows),
            "confirmation_buckets": list(confirmations),
            "min_return_pct": [float(value) * 100.0 for value in min_returns],
            "min_imbalance": [float(value) for value in min_imbalances],
            "min_intensity": [float(value) for value in min_intensities],
            "min_notional_5m_vs_30m": [
                float(value) for value in min_notional_5m_vs_30m
            ],
            "cooldown_buckets": list(cooldowns),
        },
        "top10_proxy": {
            "mode": "local_available_symbols_positive_utc_day_return",
            "source_symbol_count": proxy.source_symbol_count,
            "first_valid_at": proxy.first_valid_at.isoformat(),
            "exact_live_universe_snapshots_available": False,
            "reason": (
                "the local research export contains 15s states but not the "
                "full historical activated-universe ranking snapshots"
            ),
        },
        "local_live_signal_metadata": read_live_signal_metadata(args.live_signals),
        "assumptions": {
            "entry_fill": (
                "effective live long LIMIT touched by a later local 15s low "
                "before the 900s TTL; fill at the limit price"
            ),
            "exit_replay": (
                "complete local 15s bars aggregated to 15m; first eligible "
                "bearish candle, direct +0.10% close or +0.88% recovery "
                "limit with B8"
            ),
            "capital_model": (
                "independent 100U positions in Hedge Mode; "
                + (
                    "parameter sets whose unthrottled natural initial-margin peak exceeds "
                    f"{args.max_initial_margin_usdt:.2f}U are excluded; no entries are "
                    "rejected after a parameter set is selected"
                    if args.max_initial_margin_usdt is not None
                    else "no artificial max-position cap because live risk caps were unlimited"
                )
            ),
            "leverage_effect": (
                "5x recorded and margin per 100U entry is 20U; PnL is not "
                "multiplied by leverage"
            ),
            "funding": "not modeled from the local market-state export",
            "selection_score": (
                f"single objective: {args.selection_scope} absolute net PnL in USDT minus "
                f"{args.drawdown_weight:g} times {args.selection_scope} max drawdown in USDT; "
                "the score is maximized and requires at least the configured "
                "minimum closed trades"
            ),
        },
        "event_pools": {
            f"impulse_{impulse}_confirmation_{confirmation}": len(observations)
            for (impulse, confirmation), observations in pools.items()
        },
        "baseline": report_for("baseline", baseline),
        "best_validation": report_for("best_validation", best_config),
        "top_candidates": ranked[:20],
        "holdout_summary": {
            "best_validation": full_best,
            "baseline": full_baseline,
        },
    }
    (output / "optimization_report.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )

    best_validation = manifest["best_validation"]["metrics"]["validation"]
    holdout = manifest["best_validation"]["metrics"]["holdout"]
    baseline_validation = manifest["baseline"]["metrics"]["validation"]
    baseline_holdout = manifest["baseline"]["metrics"]["holdout"]
    local_symbol_count = len({state.symbol for state in states})
    both_positive = sum(
        bool(row["margin_constraint_feasible"])
        and float(row["validation_mean_net_return_pct"] or 0) > 0
        and float(row["holdout_mean_net_return_pct"] or 0) > 0
        for row in grid_rows
    )
    markdown = (
        "\n".join(
            [
                "# 实盘固定设置下的本地寻优",
                "",
                (
                    f"- 原始本地窗口：`{full_start.isoformat()}` 至 "
                    f"`{full_end.isoformat()}`"
                ),
                (
                    f"- 寻优有效窗口：`{optimization_start.isoformat()}` 至 "
                    f"`{full_end.isoformat()}`；首个 UTC 日为部分日，"
                    "因此 Top10 代理从下一 UTC 日起生效"
                ),
                (
                    f"- 数据：{len(states):,} 条可用 15 秒状态、"
                    f"{local_symbol_count} 个本地采集币种、{len(segments):,} 个连续片段"
                ),
                "",
                "## 固定项",
                "",
                f"- `{json.dumps(LIVE_FIXED_SETTINGS, ensure_ascii=False)}`",
                f"- 只寻优：`{', '.join(optimized_parameters)}`",
                (
                    f"- `cooldown_buckets` 固定为 `{args.fixed_cooldown_buckets}`"
                    if args.fixed_cooldown_buckets is not None
                    else ""
                ),
                (
                    "- 当前实盘基线：`3/1/1.00%/0.40/2/0`；这是本次网格中的 "
                    "一个明确基线，不再使用旧脚本的任意基线。"
                ),
                (
                    "- 开仓时段过滤："
                    + (
                        f"按实际成交 `entry_at` 排除 {entry_time_exclusion.window_text} "
                        f"（{entry_time_exclusion.timezone_label}，左闭右开）；"
                        "被排除的入场不参与收益、回撤、保证金峰值或 cooldown。"
                        if entry_time_exclusion is not None
                        else "本次未排除任何开仓时段。"
                    )
                ),
                (
                    "- 保证金约束："
                    + (
                        f"初始保证金上限 {args.max_initial_margin_usdt:.2f}U；"
                        f"按每笔约 {float(LIVE_FIXED_SETTINGS['entry_notional_usdt']) / float(LIVE_FIXED_SETTINGS['entry_leverage']):.2f}U 计算；"
                        "每组参数完整回放，只有自然峰值超过上限的参数组被排除，"
                        "不会在达到上限时拒绝后续入场。"
                        if args.max_initial_margin_usdt is not None
                        else "本次运行未设置初始保证金上限。"
                    )
                ),
                "",
                "## 结果",
                "",
                (
                    f"- 选择目标：单目标分数 = {args.selection_scope}绝对净 PnL − "
                    f"{args.drawdown_weight:g} × {args.selection_scope}最大回撤；分数越大越好。"
                    "这使 PnL 接近时较小回撤会连续改善分数。"
                ),
                f"- 单目标分数最优参数：`{json.dumps(best_config, ensure_ascii=False)}`",
                (
                    f"- 验证集：{best_validation['n_closed']} 笔已平仓，净 PnL "
                    f"{float(best_validation['net_pnl_usdt'] or 0):+.2f}U，平均净收益 "
                    f"{float(best_validation['mean_net_return_pct'] or 0):+.4f}%，"
                    f"PF {best_validation['profit_factor']}"
                ),
                (
                    f"- 留出集：{holdout['n_closed']} 笔已平仓，净 PnL "
                    f"{float(holdout['net_pnl_usdt'] or 0):+.2f}U，平均净收益 "
                    f"{float(holdout['mean_net_return_pct'] or 0):+.4f}%，"
                    f"PF {holdout['profit_factor']}"
                ),
                (
                    f"- 同一固定设置下的实盘基线：验证集 "
                    f"{baseline_validation['n_closed']} 笔、"
                    f"{float(baseline_validation['net_pnl_usdt'] or 0):+.2f}U；留出集 "
                    f"{baseline_holdout['n_closed']} 笔、"
                    f"{float(baseline_holdout['net_pnl_usdt'] or 0):+.2f}U"
                ),
                (
                    f"- 网格：{len(grid_rows):,} 组；验证集和留出集同时为正："
                    f"{both_positive} 组"
                ),
                "",
                "## 边界",
                "",
                (
                    "- 本地文件没有完整历史 Top10 排名快照；本报告使用 "
                    f"{local_symbol_count} 个本地采集币种的因果正收益 Top10 代理，"
                    "首个部分 UTC 日不参与寻优。因此这是一份严格锁定已知实盘设置的 "
                    "研究结果，但不能宣称等同于完整实盘回放。"
                ),
                (
                    "- 保证金约束是候选参数组级别的事前可行性约束："
                    "候选组在不删减任何信号的完整回放中必须满足自然初始保证金峰值上限。"
                ),
                (
                    "- 研究曲线使用本地 15 秒状态推导 15 分钟 K 线，并假设 LIMIT 在 "
                    "TTL 内被触及；实际成交延迟、未成交订单、资金费、账户余额和当时 "
                    "已有仓位仍可能不同。"
                ),
                (
                    "- 实盘账户权益基准应以本地 `account_balance_"
                    "snapshots.csv.gz` 为准；比较图会保留真实的 "
                    "`218.11U → 279.93U` 曲线，不用研究曲线反推实盘。"
                ),
                "",
                (
                    "Artifacts: `optimization_report.json`, `optimization_report.md`, "
                    "`grid_results.csv`, `top_candidates.csv`, `baseline_events.csv`, "
                    "`best_candidate_events.csv`, `equity_series.csv`."
                ),
            ]
        )
        + "\n"
    )
    (output / "optimization_report.md").write_text(markdown, encoding="utf-8")

    print(
        json.dumps(
            {
                "output_dir": str(output),
                "states": len(states),
                "symbols": len({state.symbol for state in states}),
                "segments": len(segments),
                "grid_rows": len(grid_rows),
                "optimization_window": {
                    "start": optimization_start.isoformat(),
                    "end": full_end.isoformat(),
                },
                "best_config": best_config,
                "best_natural_initial_margin_peak_usdt": manifest["best_validation"][
                    "natural_initial_margin_peak_usdt"
                ],
                "best_margin_constraint_feasible": manifest["best_validation"][
                    "margin_constraint_feasible"
                ],
                "best_validation": best_validation,
                "best_holdout": holdout,
                "max_initial_margin_usdt": args.max_initial_margin_usdt,
                "entry_time_exclusion": (
                    None
                    if entry_time_exclusion is None
                    else {
                        "window": entry_time_exclusion.window_text,
                        "timezone": entry_time_exclusion.timezone_label,
                        "interval": "[start, end)",
                    }
                ),
                "baseline_validation": baseline_validation,
                "baseline_holdout": baseline_holdout,
            },
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        )
    )


if __name__ == "__main__":
    main()
