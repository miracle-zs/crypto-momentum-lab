#!/usr/bin/env python3
"""Local-only backtest of additional long momentum signal filters.

The base gate is the current no-EMA exploratory gate: 45-second return >=
0.8%, aggressive imbalance >= 0.40, notional intensity >= 1.5x, and the
positive Top-100 gainer pool.  All filters use information available at the
decision bucket.  Exits are replayed with the common candle_15m policy from
``compare_parameter_equity.py``.

This is an event study.  The default curve allows different symbols to
overlap, while a second curve applies a simple one-active-position-per-symbol
guard.  Neither mode is a capital-constrained exchange simulator.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
from bisect import bisect_right
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from analyze_three_day_momentum import (  # noqa: E402
    CANDLE_INTERVAL_SECONDS,
    GateConfig,
    RawMetrics,
    StateSeries,
    aggregate_candles,
    build_rising_runs,
    candidate_index,
    event_from_metrics,
    finite,
    good_segments,
    iso_epoch,
    load_states,
    load_universe,
    raw_metrics,
    state_price,
    passes_up,
)
from compare_parameter_equity import (  # noqa: E402
    load_market_data,
    resolve_exit,
)


ROUND_TRIP_COST = 0.002
INITIAL_EQUITY = 1_000.0
NOTIONAL = 100.0


@dataclass(slots=True)
class PreparedEvent:
    """A no-EMA, Top-100 event plus entry-visible path features."""

    symbol: str
    detected_ts: int
    entry_price: float
    impulse_return: float
    imbalance: float
    intensity: float
    breakout_distance: float
    positive_bucket_count: int
    bucket_returns: tuple[float, ...]
    bucket_imbalances: tuple[float, ...]
    last_bucket_return: float
    last_bucket_imbalance: float
    prior_bucket_imbalance_mean: float
    imbalance_decay: float
    efficiency: float
    spread_bps: float | None
    run_key: str | None = None
    exit_ms: int | None = None
    exit_price: float | None = None
    exit_reason: str | None = None

    def row(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "detected_ts": self.detected_ts,
            "detected_at": iso_epoch(self.detected_ts),
            "entry_price": self.entry_price,
            "impulse_return_pct": self.impulse_return * 100.0,
            "aggressive_imbalance": self.imbalance,
            "notional_intensity": self.intensity,
            "breakout_distance_pct": self.breakout_distance * 100.0,
            "positive_bucket_count": self.positive_bucket_count,
            "bucket_returns_pct": ";".join(
                f"{value * 100:.6f}" for value in self.bucket_returns
            ),
            "bucket_imbalances": ";".join(
                f"{value:.6f}" for value in self.bucket_imbalances
            ),
            "last_bucket_return_pct": self.last_bucket_return * 100.0,
            "last_bucket_imbalance": self.last_bucket_imbalance,
            "prior_bucket_imbalance_mean": self.prior_bucket_imbalance_mean,
            "imbalance_decay": self.imbalance_decay,
            "price_flow_efficiency": self.efficiency,
            "spread_bps": self.spread_bps,
            "run_key": self.run_key,
            "exit_ms": self.exit_ms,
            "exit_price": self.exit_price,
            "exit_reason": self.exit_reason,
        }


def percentile(values: Iterable[float], fraction: float) -> float | None:
    ordered = sorted(value for value in values if finite(value))
    if not ordered:
        return None
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * min(1.0, max(0.0, fraction))
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def parse_utc_ms(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"timestamp must include a timezone: {value!r}")
    return int(parsed.astimezone(UTC).timestamp() * 1000)


def bucket_return(series: StateSeries, index: int) -> float:
    close = state_price(series, index)
    opening = series.open[index]
    if not finite(opening) or opening <= 0:
        if index > 0:
            previous = state_price(series, index - 1)
            opening = previous
    if not finite(close) or close <= 0 or not finite(opening) or opening <= 0:
        return float("nan")
    return close / opening - 1.0


def bucket_imbalance(series: StateSeries, index: int) -> float:
    buy = series.aggressive_buy[index]
    sell = series.aggressive_sell[index]
    total = buy + sell
    return (buy - sell) / total if total > 0 else 0.0


def prepare_event(
    symbol: str,
    series: StateSeries,
    segment_end: int,
    metrics: RawMetrics,
    config: GateConfig,
) -> PreparedEvent:
    start = metrics.index - config.impulse_buckets + 1
    returns = tuple(bucket_return(series, item) for item in range(start, metrics.index + 1))
    imbalances = tuple(
        bucket_imbalance(series, item)
        for item in range(start, metrics.index + 1)
    )
    finite_returns = tuple(value for value in returns if finite(value))
    positive_count = sum(value > 0 for value in finite_returns)
    last_return = returns[-1] if finite(returns[-1]) else float("nan")
    last_imbalance = imbalances[-1]
    previous = imbalances[:-1]
    prior_mean = statistics.fmean(previous) if previous else float("nan")
    decay = last_imbalance - prior_mean if finite(prior_mean) else float("nan")
    efficiency = metrics.impulse_return / metrics.intensity if metrics.intensity > 0 else float("nan")
    spread_bps: float | None = None
    bid = series.bid[metrics.index]
    ask = series.ask[metrics.index]
    if finite(bid) and finite(ask) and bid > 0 and ask >= bid:
        midpoint = (bid + ask) / 2.0
        if midpoint > 0:
            spread_bps = (ask - bid) / midpoint * 10_000.0
    breakout_distance = (
        metrics.price / metrics.breakout_high - 1.0
        if metrics.breakout_high > 0
        else float("nan")
    )
    return PreparedEvent(
        symbol=symbol,
        detected_ts=int(series.ts[metrics.index]),
        entry_price=metrics.price,
        impulse_return=metrics.impulse_return,
        imbalance=metrics.imbalance,
        intensity=metrics.intensity,
        breakout_distance=breakout_distance,
        positive_bucket_count=positive_count,
        bucket_returns=returns,
        bucket_imbalances=imbalances,
        last_bucket_return=last_return,
        last_bucket_imbalance=last_imbalance,
        prior_bucket_imbalance_mean=prior_mean,
        imbalance_decay=decay,
        efficiency=efficiency,
        spread_bps=spread_bps,
    )


def collect_events(
    states: dict[str, StateSeries],
    universe: object,
    config: GateConfig,
) -> tuple[list[PreparedEvent], int]:
    """Collect the base gate before additional filters, then apply Top-100."""

    events: list[PreparedEvent] = []
    raw_count = 0
    for symbol, series in states.items():
        for segment_start, segment_end in good_segments(series):
            next_allowed = candidate_index(series, config)
            for index in range(segment_start, segment_end):
                if index < next_allowed:
                    continue
                metrics = raw_metrics(series, index, config)
                if metrics is None or not passes_up(metrics, config):
                    continue
                raw_count += 1
                event = prepare_event(symbol, series, segment_end, metrics, config)
                pool = universe.pool_at(event.detected_ts) if universe is not None else None
                if pool is not None and symbol in pool:
                    events.append(event)
                next_allowed = index + config.confirmation_buckets + config.cooldown_buckets
    events.sort(key=lambda item: (item.detected_ts, item.symbol))
    return events, raw_count


def run_key_for_timestamp(
    event: PreparedEvent,
    runs_by_symbol: dict[str, list[tuple[int, int, str]]],
) -> str | None:
    runs = runs_by_symbol.get(event.symbol, ())
    starts = [item[0] for item in runs]
    index = bisect_right(starts, event.detected_ts) - 1
    if index < 0:
        return None
    start, end, key = runs[index]
    return key if start <= event.detected_ts < end else None


def attach_runs(
    events: list[PreparedEvent],
    states: dict[str, StateSeries],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    all_run_rows: list[dict[str, object]] = []
    runs_by_symbol: dict[str, list[tuple[int, int, str]]] = defaultdict(list)
    for symbol, series in states.items():
        candles = aggregate_candles(series, good_segments(series))
        for index, run in enumerate(build_rising_runs(symbol, candles, "close")):
            if run.length < 2 or run.cumulative_return < 0.01:
                continue
            key = f"{symbol}:{run.start}"
            runs_by_symbol[symbol].append((run.start, run.end, key))
            all_run_rows.append(
                {
                    "run_key": key,
                    "symbol": symbol,
                    "start": iso_epoch(run.start),
                    "end": iso_epoch(run.end),
                    "length_15m": run.length,
                    "cumulative_return_pct": run.cumulative_return * 100.0,
                }
            )
    for values in runs_by_symbol.values():
        values.sort()
    for event in events:
        event.run_key = run_key_for_timestamp(event, runs_by_symbol)
    all_run_rows.sort(key=lambda item: (str(item["start"]), str(item["symbol"])))
    return all_run_rows, all_run_rows


def fit_thresholds(events: list[PreparedEvent], cutoff: int) -> dict[str, float | None]:
    train = [event for event in events if event.detected_ts < cutoff]
    return {
        "train_imbalance_p90": percentile((event.imbalance for event in train), 0.90),
        "train_imbalance_p95": percentile((event.imbalance for event in train), 0.95),
        "train_efficiency_p40": percentile((event.efficiency for event in train), 0.40),
        "train_spread_bps_p90_known": percentile(
            (event.spread_bps for event in train if event.spread_bps is not None),
            0.90,
        ),
    }


def variant_masks(
    events: list[PreparedEvent],
    thresholds: dict[str, float | None],
) -> dict[str, set[int]]:
    """Return event indexes that pass each filter family.

    The spread rule keeps unknown spread values for this study because the
    Binance aggTrades-reconstructed 24-hour segment has no quote fields.  The
    report explicitly labels this as a soft/partial quality test.
    """

    p90_imbalance = thresholds["train_imbalance_p90"]
    p95_imbalance = thresholds["train_imbalance_p95"]
    p40_efficiency = thresholds["train_efficiency_p40"]
    p90_spread = thresholds["train_spread_bps_p90_known"]

    def persistence(event: PreparedEvent) -> bool:
        return (
            event.positive_bucket_count >= 2
            and finite(event.last_bucket_return)
            and event.last_bucket_return > 0
        )

    def persistence_with_imbalance(event: PreparedEvent) -> bool:
        return persistence(event) and event.last_bucket_imbalance >= 0.0

    def no_imbalance_decay(event: PreparedEvent) -> bool:
        return (
            persistence(event)
            and finite(event.imbalance_decay)
            and event.imbalance_decay >= -0.10
        )

    def absorption(event: PreparedEvent) -> bool:
        if p90_imbalance is None or p40_efficiency is None:
            return True
        return not (
            event.imbalance >= p90_imbalance
            and event.efficiency <= p40_efficiency
        )

    def imbalance_cap(event: PreparedEvent) -> bool:
        return p95_imbalance is None or event.imbalance <= p95_imbalance

    def spread_quality(event: PreparedEvent) -> bool:
        return (
            p90_spread is None
            or event.spread_bps is None
            or event.spread_bps <= p90_spread
        )

    masks: dict[str, set[int]] = {
        "control": set(range(len(events))),
        "persistence_2of3_last_positive": {
            index for index, event in enumerate(events) if persistence(event)
        },
        "persistence_plus_last_imbalance_nonnegative": {
            index
            for index, event in enumerate(events)
            if persistence_with_imbalance(event)
        },
        "persistence_no_imbalance_decay": {
            index for index, event in enumerate(events) if no_imbalance_decay(event)
        },
        "soft_spread_quality": {
            index for index, event in enumerate(events) if spread_quality(event)
        },
        "absorption_conditional": {
            index for index, event in enumerate(events) if absorption(event)
        },
        "imbalance_p95_cap_shadow": {
            index for index, event in enumerate(events) if imbalance_cap(event)
        },
    }
    masks["persistence_plus_spread"] = masks[
        "persistence_2of3_last_positive"
    ] & masks["soft_spread_quality"]
    masks["persistence_plus_absorption"] = masks[
        "persistence_2of3_last_positive"
    ] & masks["absorption_conditional"]
    masks["persistence_spread_absorption"] = (
        masks["persistence_2of3_last_positive"]
        & masks["soft_spread_quality"]
        & masks["absorption_conditional"]
    )
    return masks


def attach_exits(
    events: list[PreparedEvent],
    candles: dict[str, list[dict[str, float | int]]],
    marks: dict[str, object],
    end_ms: int,
) -> int:
    unresolved = 0
    for event in events:
        exit_data = resolve_exit(
            {
                "symbol": event.symbol,
                "detected_ts": str(event.detected_ts),
                "entry_price": str(event.entry_price),
            },
            candles=candles,
            marks=marks,  # type: ignore[arg-type]
            end_ms=end_ms,
        )
        if exit_data is None:
            unresolved += 1
            continue
        event.exit_ms, event.exit_price, event.exit_reason = exit_data
    return unresolved


def equity_metrics(
    events: list[PreparedEvent],
    indexes: set[int],
    *,
    one_active_per_symbol: bool,
    start_ms: int,
    end_ms: int,
) -> dict[str, object]:
    selected = [
        events[index]
        for index in sorted(indexes)
        if events[index].exit_ms is not None
        and events[index].exit_price is not None
        and int(events[index].exit_ms) <= end_ms
    ]
    selected.sort(key=lambda event: (event.detected_ts, event.symbol))
    if one_active_per_symbol:
        active_until: dict[str, int] = {}
        deduped: list[PreparedEvent] = []
        for event in selected:
            exit_ms = int(event.exit_ms or 0)
            if event.detected_ts * 1_000 < active_until.get(event.symbol, -1):
                continue
            deduped.append(event)
            active_until[event.symbol] = exit_ms
        selected = deduped

    grouped_pnl: defaultdict[int, float] = defaultdict(float)
    exit_reasons: Counter[str] = Counter()
    net_returns: list[float] = []
    gross_returns: list[float] = []
    for event in selected:
        assert event.exit_price is not None
        gross = event.exit_price / event.entry_price - 1.0
        net = gross - ROUND_TRIP_COST
        grouped_pnl[int(event.exit_ms or end_ms)] += NOTIONAL * net
        gross_returns.append(gross)
        net_returns.append(net)
        exit_reasons[str(event.exit_reason)] += 1

    points: list[tuple[int, float]] = [(start_ms, INITIAL_EQUITY)]
    equity = INITIAL_EQUITY
    for timestamp in sorted(grouped_pnl):
        equity += grouped_pnl[timestamp]
        points.append((timestamp, equity))
    if points[-1][0] < end_ms:
        points.append((end_ms, equity))
    peak = points[0][1]
    max_dd = 0.0
    max_dd_pct = 0.0
    for _, value in points:
        peak = max(peak, value)
        max_dd = min(max_dd, value - peak)
        max_dd_pct = min(max_dd_pct, (value - peak) / peak if peak else 0.0)
    positive = sum(value for value in net_returns if value > 0)
    negative = sum(value for value in net_returns if value < 0)
    return {
        "trades": len(net_returns),
        "wins": sum(value > 0 for value in net_returns),
        "losses": sum(value <= 0 for value in net_returns),
        "win_rate_pct": (
            sum(value > 0 for value in net_returns) / len(net_returns) * 100.0
            if net_returns
            else None
        ),
        "mean_gross_pct": statistics.fmean(gross_returns) * 100.0 if gross_returns else None,
        "mean_net_pct": statistics.fmean(net_returns) * 100.0 if net_returns else None,
        "median_net_pct": statistics.median(net_returns) * 100.0 if net_returns else None,
        "profit_factor": positive / abs(negative) if negative else None,
        "final_equity": equity,
        "total_return_pct": (equity / INITIAL_EQUITY - 1.0) * 100.0,
        "max_drawdown_usdt": max_dd,
        "max_drawdown_pct": max_dd_pct * 100.0,
        "exit_reasons": dict(exit_reasons),
    }


def coverage_metrics(
    events: list[PreparedEvent],
    indexes: set[int],
    run_keys: set[str],
    top10_keys: set[str],
    top1_keys: set[str],
) -> dict[str, object]:
    selected = [events[index] for index in indexes]
    covered = {event.run_key for event in selected if event.run_key is not None}
    covered.discard(None)
    top10_covered = covered & top10_keys
    top1_covered = covered & top1_keys
    return {
        "events": len(selected),
        "covered_runs": len(covered),
        "run_coverage_pct": len(covered) / len(run_keys) * 100.0 if run_keys else None,
        "top10_covered": len(top10_covered),
        "top10_total": len(top10_keys),
        "top10_coverage_pct": len(top10_covered) / len(top10_keys) * 100.0 if top10_keys else None,
        "top1_covered": len(top1_covered),
        "top1_total": len(top1_keys),
        "top1_coverage_pct": len(top1_covered) / len(top1_keys) * 100.0 if top1_keys else None,
    }


def write_csv(path: Path, rows: Iterable[dict[str, object]]) -> None:
    rows = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def render_report(
    output_dir: Path,
    *,
    config: GateConfig,
    coverage: dict[str, object],
    thresholds: dict[str, float | None],
    results: list[dict[str, object]],
    quality_counts: dict[str, object],
) -> None:
    recommended = next(
        (
            row
            for row in results
            if row["variant"] == "persistence_plus_spread"
            and row["overlap_mode"] == "one_active_per_symbol"
        ),
        None,
    )
    lines = [
        "# 追加低质量信号过滤：本地回测",
        "",
        "> 全部计算在本地完成；没有连接服务器、没有修改实盘。",
        "",
        "## 回测口径",
        "",
        f"- 覆盖：`{coverage['start']}` 至 `{coverage['end']}`，约 `{coverage['hours']:.2f}` 小时，`{coverage['symbols']}` 个 symbol。",
        f"- 基线：45 秒涨幅 ≥ `{config.min_return * 100:.1f}%`、imbalance ≥ `{config.min_imbalance:.2f}`、成交额强度 ≥ `{config.min_intensity:.1f}x`、Top100 正收益池、无 EMA5/EMA10。",
        "- 退出：第一根反向完整 `candle_15m` 收线，最长 24 小时；每笔名义金额 100 USDT，初始权益 1,000 USDT，往返成本 20 bps。",
        "- 事件级模式允许不同 symbol 重叠；第二种模式只限制同一 symbol 同时一个仓位，仍不是完整资金约束撮合。",
        "",
        "## 训练段拟合的观察阈值",
        "",
        f"- imbalance P90：`{thresholds['train_imbalance_p90']}`；P95：`{thresholds['train_imbalance_p95']}`。",
        f"- 量价效率 P40：`{thresholds['train_efficiency_p40']}`。",
        f"- 已知报价 spread P90：`{thresholds['train_spread_bps_p90_known']}` bps。",
        "- 吸收过滤只在 imbalance 极高且量价效率偏低时剔除；spread 过滤对未知报价暂时放行，因为 aggTrades 重建的 24 小时没有 bid/ask。",
        "",
        "## 结果",
        "",
        "| 过滤组合 | 模式 | 信号数 | 覆盖上涨段 | Top10 | Top1 | 交易数 | 收益 | 最大回撤 | 胜率 | PF |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in results:
        lines.append(
            "| {variant} | {mode} | {events} | {run_cov:.1f}% | {top10:.1f}% | {top1:.1f}% | {trades} | {ret:+.2f}% | {dd:.2f}% | {win:.1f}% | {pf} |".format(
                variant=row["variant"],
                mode="重叠" if row["overlap_mode"] == "overlap" else "同 symbol 不重叠",
                events=row["events"],
                run_cov=float(row["run_coverage_pct"] or 0.0),
                top10=float(row["top10_coverage_pct"] or 0.0),
                top1=float(row["top1_coverage_pct"] or 0.0),
                trades=row["trades"],
                ret=float(row["total_return_pct"] or 0.0),
                dd=float(row["max_drawdown_pct"] or 0.0),
                win=float(row["win_rate_pct"] or 0.0),
                pf=(
                    f"{float(row['profit_factor']):.2f}"
                    if row["profit_factor"] is not None
                    else "n/a"
                ),
            )
        )
    lines.extend(
        [
            "",
            "## 解释与建议",
            "",
            "- `persistence_2of3_last_positive` 是本次最重要的结构过滤：要求 3 个 15 秒桶中至少 2 个上涨，且最后一桶仍为正。",
            "- `soft_spread_quality` 只能作为部分验证；因为缺口重建数据没有报价，不能把当前 spread 结果直接当作完整历史证据。",
            "- `absorption_conditional` 和固定 imbalance 上限只作为观察组，避免误删真正的高强度长尾行情。",
            "- 采纳标准应同时满足：验证段收益/回撤改善、Top10/Top1 长尾覆盖没有明显下降、亏损笔数减少。",
        ]
    )
    if recommended is not None:
        lines.extend(
            [
                "",
                f"当前建议观察组合：`persistence_plus_spread` + 同 symbol 不重叠；对应验证结果见 CSV/JSON，尚未写入实盘。",
            ]
        )
    lines.extend(
        [
            "",
            "## 报价质量统计",
            "",
            f"- 基线事件：`{quality_counts['base_events']}`；其中有 spread：`{quality_counts['known_spread_events']}`；未知：`{quality_counts['unknown_spread_events']}`。",
            f"- 已知 spread 超过训练 P90：`{quality_counts['known_spread_over_p90']}`。",
        ]
    )
    (output_dir / "signal_filter_backtest.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def main() -> None:
    global ROUND_TRIP_COST

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--states", nargs="+", type=Path, required=True)
    parser.add_argument("--universe", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--top-count", type=int, default=100)
    parser.add_argument("--min-return", type=float, default=0.008)
    parser.add_argument("--min-imbalance", type=float, default=0.40)
    parser.add_argument("--min-intensity", type=float, default=1.5)
    parser.add_argument("--cooldown-buckets", type=int, default=2)
    parser.add_argument("--round-trip-cost", type=float, default=ROUND_TRIP_COST)
    args = parser.parse_args()

    ROUND_TRIP_COST = args.round_trip_cost

    states = load_states(args.states)
    universe = load_universe(args.universe, args.top_count)
    if not states:
        raise SystemExit("no local states loaded")
    all_timestamps = [timestamp for series in states.values() for timestamp in series.ts]
    start_ms = min(all_timestamps) * 1_000
    end_ms = (max(all_timestamps) + 15) * 1_000
    coverage = {
        "start": iso_epoch(start_ms // 1_000),
        "end": iso_epoch((end_ms // 1_000) - 15),
        "hours": (end_ms - start_ms) / 3_600_000.0,
        "symbols": len(states),
    }
    midpoint = start_ms + int((end_ms - start_ms) * 0.60)

    config = GateConfig(
        f"control_r{args.min_return:.3f}_i{args.min_imbalance:.2f}_n{args.min_intensity:.1f}",
        args.min_return,
        args.min_imbalance,
        args.min_intensity,
        cooldown_buckets=args.cooldown_buckets,
    )
    events, raw_count = collect_events(states, universe, config)
    run_rows, _ = attach_runs(events, states)
    candles, marks = load_market_data(args.states, start_ms=start_ms, end_ms=end_ms)
    unresolved = attach_exits(events, candles, marks, end_ms)
    thresholds = fit_thresholds(events, midpoint // 1_000)
    masks = variant_masks(events, thresholds)

    sorted_runs = sorted(
        run_rows,
        key=lambda row: float(row["cumulative_return_pct"]),
        reverse=True,
    )
    run_keys = {str(row["run_key"]) for row in run_rows}
    top10_keys = {
        str(row["run_key"])
        for row in sorted_runs[: max(1, math.ceil(len(sorted_runs) * 0.10))]
    }
    top1_keys = {
        str(row["run_key"])
        for row in sorted_runs[: max(1, math.ceil(len(sorted_runs) * 0.01))]
    }

    results: list[dict[str, object]] = []
    variant_order = [
        "control",
        "persistence_2of3_last_positive",
        "persistence_plus_last_imbalance_nonnegative",
        "persistence_no_imbalance_decay",
        "soft_spread_quality",
        "absorption_conditional",
        "imbalance_p95_cap_shadow",
        "persistence_plus_spread",
        "persistence_plus_absorption",
        "persistence_spread_absorption",
    ]
    for variant in variant_order:
        selected_indexes = masks[variant]
        coverage_all = coverage_metrics(
            events,
            selected_indexes,
            run_keys,
            top10_keys,
            top1_keys,
        )
        for overlap_mode, one_active in (
            ("overlap", False),
            ("one_active_per_symbol", True),
        ):
            equity = equity_metrics(
                events,
                selected_indexes,
                one_active_per_symbol=one_active,
                start_ms=start_ms,
                end_ms=end_ms,
            )
            train_indexes = {
                index
                for index in selected_indexes
                if events[index].detected_ts < midpoint // 1_000
            }
            validation_indexes = selected_indexes - train_indexes
            train_equity = equity_metrics(
                events,
                train_indexes,
                one_active_per_symbol=one_active,
                start_ms=start_ms,
                end_ms=midpoint,
            )
            validation_equity = equity_metrics(
                events,
                validation_indexes,
                one_active_per_symbol=one_active,
                start_ms=midpoint,
                end_ms=end_ms,
            )
            row: dict[str, object] = {
                "variant": variant,
                "overlap_mode": overlap_mode,
                **coverage_all,
                **equity,
                "train_events": len(train_indexes),
                "validation_events": len(validation_indexes),
                "train_return_pct": train_equity["total_return_pct"],
                "train_max_drawdown_pct": train_equity["max_drawdown_pct"],
                "validation_return_pct": validation_equity["total_return_pct"],
                "validation_max_drawdown_pct": validation_equity["max_drawdown_pct"],
                "validation_win_rate_pct": validation_equity["win_rate_pct"],
                "validation_profit_factor": validation_equity["profit_factor"],
                "unresolved": unresolved,
            }
            results.append(row)

    quality_counts = {
        "base_events": len(events),
        "known_spread_events": sum(event.spread_bps is not None for event in events),
        "unknown_spread_events": sum(event.spread_bps is None for event in events),
        "known_spread_over_p90": sum(
            event.spread_bps is not None
            and thresholds["train_spread_bps_p90_known"] is not None
            and event.spread_bps > float(thresholds["train_spread_bps_p90_known"])
            for event in events
        ),
    }
    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(output_dir / "events.csv", [event.row() for event in events])
    write_csv(output_dir / "rising_runs.csv", run_rows)
    write_csv(output_dir / "results.csv", results)
    summary = {
        "generated_at": datetime.now(UTC).isoformat(),
        "local_only": True,
        "raw_orderflow_events": raw_count,
        "top100_events": len(events),
        "unresolved_exits": unresolved,
        "coverage": coverage,
        "split": {
            "cutoff": iso_epoch(midpoint // 1_000),
            "train_fraction": 0.60,
        },
        "base_config": {
            "impulse_seconds": 45,
            "baseline_seconds": 60,
            "breakout_seconds": 60,
            "min_return_pct": config.min_return * 100.0,
            "min_imbalance": config.min_imbalance,
            "min_intensity": config.min_intensity,
            "confirmation_buckets": config.confirmation_buckets,
            "cooldown_buckets": config.cooldown_buckets,
            "top_count": args.top_count,
            "ema": False,
        },
        "thresholds_fit_on_train": thresholds,
        "rising_runs": {
            "significant_count": len(run_rows),
            "top10_count": len(top10_keys),
            "top1_count": len(top1_keys),
        },
        "quality_counts": quality_counts,
        "results": results,
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    render_report(
        output_dir,
        config=config,
        coverage=coverage,
        thresholds=thresholds,
        results=results,
        quality_counts=quality_counts,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
