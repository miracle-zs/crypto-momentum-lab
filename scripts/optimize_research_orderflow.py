#!/usr/bin/env python3
"""Explore order-flow impulse thresholds on the local research Parquet data.

This is an event-study optimizer, not a broker simulator.  It reuses the
production order-flow event definition, keeps only contiguous 15-second state
segments, applies a conservative round-trip cost, and ranks parameter sets on
the validation split.  The holdout split is reported but never used for the
selection of the best candidate.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import math
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow.parquet as parquet

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.strategies.order_flow_impulse.event_study import (
    OrderFlowDirection,
    OrderFlowImpulseConfig,
    OrderFlowImpulseEvent,
    find_order_flow_impulses,
)


BUCKET = timedelta(seconds=15)
DEFAULT_HORIZONS = (4, 12, 20, 40)
STATE_COLUMNS = (
    "schema_version",
    "exchange",
    "environment",
    "symbol",
    "bucket_start",
    "bucket_end",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "trade_count",
    "trade_notional",
    "aggressive_buy_notional",
    "aggressive_sell_notional",
    "last_bid_price",
    "last_ask_price",
    "spread",
    "midpoint",
    "liquidation_count",
    "liquidation_notional",
    "mark_price",
    "closed_kline_count",
    "source_event_count",
    "first_received_at",
    "last_received_at",
    "data_complete",
    "missing_agg_trade_count",
)


@dataclass(frozen=True, slots=True)
class Split:
    name: str
    start: datetime
    end: datetime


def parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp must include a timezone: {value!r}")
    return parsed.astimezone(UTC)


def parse_decimal(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    return Decimal(str(value))


def parse_csv_values(value: str, converter: Any) -> tuple[Any, ...]:
    result = tuple(converter(item.strip()) for item in value.split(",") if item.strip())
    if not result:
        raise ValueError("parameter list must not be empty")
    return result


def state_from_row(row: dict[str, object]) -> MarketState15s:
    return MarketState15s(
        schema_version=int(row["schema_version"] or 1),
        exchange=str(row["exchange"]),
        environment=str(row["environment"]),
        symbol=str(row["symbol"]),
        bucket_start=row["bucket_start"],
        bucket_end=row["bucket_end"],
        open_price=parse_decimal(row["open_price"]),
        high_price=parse_decimal(row["high_price"]),
        low_price=parse_decimal(row["low_price"]),
        close_price=parse_decimal(row["close_price"]),
        trade_count=int(row["trade_count"] or 0),
        trade_notional=parse_decimal(row["trade_notional"]) or Decimal("0"),
        aggressive_buy_notional=(
            parse_decimal(row["aggressive_buy_notional"]) or Decimal("0")
        ),
        aggressive_sell_notional=(
            parse_decimal(row["aggressive_sell_notional"]) or Decimal("0")
        ),
        last_bid_price=parse_decimal(row["last_bid_price"]),
        last_ask_price=parse_decimal(row["last_ask_price"]),
        spread=parse_decimal(row["spread"]),
        midpoint=parse_decimal(row["midpoint"]),
        liquidation_count=int(row["liquidation_count"] or 0),
        liquidation_notional=(
            parse_decimal(row["liquidation_notional"]) or Decimal("0")
        ),
        mark_price=parse_decimal(row["mark_price"]),
        closed_kline_count=int(row["closed_kline_count"] or 0),
        source_event_count=int(row["source_event_count"] or 0),
        first_received_at=row["first_received_at"],
        last_received_at=row["last_received_at"],
        data_complete=bool(row["data_complete"]),
        missing_agg_trade_count=int(row["missing_agg_trade_count"] or 0),
    )


def load_states(root: Path, *, environment: str) -> tuple[list[MarketState15s], dict[str, int]]:
    states_by_key: dict[tuple[str, datetime], MarketState15s] = {}
    file_count = 0
    source_rows = 0
    skipped_incomplete = 0
    for path in sorted(root.rglob("*.parquet")):
        file_count += 1
        parquet_file = parquet.ParquetFile(path)
        table = parquet_file.read(columns=list(STATE_COLUMNS))
        for raw_row in table.to_pylist():
            source_rows += 1
            if str(raw_row["environment"]) != environment:
                continue
            if not bool(raw_row["data_complete"]) or int(
                raw_row["missing_agg_trade_count"] or 0
            ) != 0:
                skipped_incomplete += 1
                continue
            state = state_from_row(raw_row)
            if state.close_price is None and state.midpoint is None and state.mark_price is None:
                skipped_incomplete += 1
                continue
            states_by_key.setdefault((state.symbol, state.bucket_start), state)
    states = sorted(states_by_key.values(), key=lambda item: (item.bucket_start, item.symbol))
    return states, {
        "parquet_files": file_count,
        "source_rows": source_rows,
        "usable_states": len(states),
        "skipped_incomplete_or_unpriced": skipped_incomplete,
    }


def split_contiguous_states(
    states: list[MarketState15s],
    *,
    minimum_buckets: int,
) -> list[tuple[MarketState15s, ...]]:
    by_symbol: defaultdict[str, list[MarketState15s]] = defaultdict(list)
    for state in states:
        by_symbol[state.symbol].append(state)

    segments: list[tuple[MarketState15s, ...]] = []
    for symbol_states in by_symbol.values():
        ordered = sorted(symbol_states, key=lambda item: item.bucket_start)
        current: list[MarketState15s] = []
        previous: datetime | None = None
        for state in ordered:
            if previous is not None and state.bucket_start - previous != BUCKET:
                if len(current) >= minimum_buckets:
                    segments.append(tuple(current))
                current = []
            current.append(state)
            previous = state.bucket_start
        if len(current) >= minimum_buckets:
            segments.append(tuple(current))
    return sorted(segments, key=lambda item: (item[0].bucket_start, item[0].symbol))


def build_pool(
    segments: list[tuple[MarketState15s, ...]],
    *,
    impulse_window_buckets: int,
    confirmation_buckets: int,
    horizons: tuple[int, ...],
) -> list[OrderFlowImpulseEvent]:
    relaxed = OrderFlowImpulseConfig(
        impulse_window_buckets=impulse_window_buckets,
        baseline_window_buckets=4,
        breakout_window_buckets=4,
        min_return_pct=Decimal("0.0000001"),
        min_aggressive_imbalance=Decimal("0"),
        min_notional_intensity=Decimal("0.0000001"),
        confirmation_buckets=confirmation_buckets,
        cooldown_buckets=0,
        forward_horizon_buckets=horizons,
    )
    events: list[OrderFlowImpulseEvent] = []
    for segment in segments:
        events.extend(find_order_flow_impulses(segment, relaxed))
    return sorted(events, key=lambda item: (item.detected_at, item.symbol))


def directional_imbalance(event: OrderFlowImpulseEvent) -> Decimal:
    if event.direction is OrderFlowDirection.UP:
        return event.aggressive_imbalance
    return -event.aggressive_imbalance


def select_events(
    events: list[OrderFlowImpulseEvent],
    *,
    min_return: Decimal,
    min_imbalance: Decimal,
    min_intensity: Decimal,
    cooldown_buckets: int,
) -> list[OrderFlowImpulseEvent]:
    selected: list[OrderFlowImpulseEvent] = []
    last_selected: dict[str, datetime] = {}
    cooldown = BUCKET * cooldown_buckets
    for event in events:
        if event.impulse_return_pct < min_return:
            continue
        if directional_imbalance(event) < min_imbalance:
            continue
        if event.notional_intensity < min_intensity:
            continue
        previous = last_selected.get(event.symbol)
        if previous is not None and event.detected_at <= previous + cooldown:
            continue
        selected.append(event)
        last_selected[event.symbol] = event.detected_at
    return selected


def metric_value(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, 8)


def event_metrics(
    events: list[OrderFlowImpulseEvent],
    *,
    horizon: int,
    split: Split,
    round_trip_cost: float,
    notional: float,
) -> dict[str, float | int | None]:
    eligible: list[OrderFlowImpulseEvent] = []
    for event in events:
        exit_at = event.detected_at + BUCKET * horizon
        if not (split.start <= event.detected_at < split.end):
            continue
        if exit_at >= split.end:
            continue
        if event.forward_returns.get(horizon) is None:
            continue
        eligible.append(event)

    net_returns = [
        float(event.forward_returns[horizon]) - round_trip_cost for event in eligible
    ]
    gross_returns = [float(event.forward_returns[horizon]) for event in eligible]
    gains = sum(value for value in net_returns if value > 0)
    losses = -sum(value for value in net_returns if value < 0)
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in net_returns:
        cumulative += value * notional
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)

    by_direction: dict[str, list[float]] = {"up": [], "down": []}
    for event, value in zip(eligible, net_returns, strict=True):
        by_direction[event.direction.value].append(value)
    return {
        "n_labeled": len(eligible),
        "net_pnl_usdt": metric_value(sum(net_returns) * notional),
        "gross_pnl_usdt": metric_value(sum(gross_returns) * notional),
        "mean_net_return_pct": metric_value(
            statistics.fmean(net_returns) * 100 if net_returns else None
        ),
        "median_net_return_pct": metric_value(
            statistics.median(net_returns) * 100 if net_returns else None
        ),
        "win_rate_pct": metric_value(
            sum(value > 0 for value in net_returns) / len(net_returns) * 100
            if net_returns
            else None
        ),
        "profit_factor": metric_value(gains / losses if losses else None),
        "max_drawdown_usdt": metric_value(max_drawdown),
        "up_n": len(by_direction["up"]),
        "down_n": len(by_direction["down"]),
        "up_mean_net_return_pct": metric_value(
            statistics.fmean(by_direction["up"]) * 100
            if by_direction["up"]
            else None
        ),
        "down_mean_net_return_pct": metric_value(
            statistics.fmean(by_direction["down"]) * 100
            if by_direction["down"]
            else None
        ),
    }


def build_equity_series(
    named_events: dict[str, list[OrderFlowImpulseEvent]],
    *,
    horizon: int,
    start: datetime,
    end: datetime,
    round_trip_cost: float,
    notional: float,
) -> list[dict[str, object]]:
    deltas: defaultdict[datetime, dict[str, float]] = defaultdict(dict)
    names = tuple(named_events)
    for name, events in named_events.items():
        for event in events:
            exit_at = event.detected_at + BUCKET * horizon
            if not (start <= event.detected_at < end and exit_at < end):
                continue
            forward = event.forward_returns.get(horizon)
            if forward is None:
                continue
            deltas[exit_at][name] = deltas[exit_at].get(name, 0.0) + (
                float(forward) - round_trip_cost
            ) * notional

    cumulative = {name: 0.0 for name in names}
    rows: list[dict[str, object]] = []
    for timestamp in sorted(deltas):
        row: dict[str, object] = {"timestamp": timestamp.isoformat()}
        for name in names:
            cumulative[name] += deltas[timestamp].get(name, 0.0)
            row[f"{name}_cumulative_pnl_usdt"] = round(cumulative[name], 8)
        rows.append(row)
    return rows


def event_row(event: OrderFlowImpulseEvent, *, horizon: int) -> dict[str, object]:
    row: dict[str, object] = {
        "symbol": event.symbol,
        "direction": event.direction.value,
        "detected_at": event.detected_at.isoformat(),
        "impulse_return_pct": float(event.impulse_return_pct) * 100,
        "aggressive_imbalance": float(event.aggressive_imbalance),
        "directional_imbalance": float(directional_imbalance(event)),
        "notional_intensity": float(event.notional_intensity),
        "breakout_distance_pct": float(event.breakout_distance_pct) * 100,
        "impulse_trade_count": event.impulse_trade_count,
        "impulse_trade_notional": float(event.impulse_trade_notional),
        "liquidation_count": event.liquidation_count,
    }
    for key, value in sorted(event.forward_returns.items()):
        row[f"forward_{key}_buckets_return_pct"] = (
            None if value is None else float(value) * 100
        )
    row["primary_forward_return_pct"] = (
        None
        if event.forward_returns.get(horizon) is None
        else float(event.forward_returns[horizon]) * 100
    )
    return row


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = list(rows[0])
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


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
            "optimization-orderflow-20260905"
        ),
    )
    parser.add_argument("--environment", default="research")
    parser.add_argument("--primary-horizon", type=int, default=20)
    parser.add_argument("--round-trip-cost-pct", type=float, default=0.12)
    parser.add_argument("--notional", type=float, default=100.0)
    parser.add_argument("--impulse-windows", default="3,4,6")
    parser.add_argument("--confirmation-buckets", default="1,2,3")
    parser.add_argument("--min-return-pct", default="0.05,0.10,0.15,0.25")
    parser.add_argument("--min-imbalance", default="0.10,0.20,0.30,0.40,0.50")
    parser.add_argument("--min-intensity", default="1.0,1.5,2.0,3.0")
    parser.add_argument("--cooldown-buckets", default="0,4,8,16,32")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.primary_horizon <= 0:
        raise SystemExit("--primary-horizon must be positive")
    round_trip_cost = args.round_trip_cost_pct / 100.0
    horizons = tuple(sorted(set(DEFAULT_HORIZONS + (args.primary_horizon,))))
    impulse_windows = parse_csv_values(args.impulse_windows, int)
    confirmations = parse_csv_values(args.confirmation_buckets, int)
    min_returns = parse_csv_values(args.min_return_pct, lambda value: Decimal(value) / 100)
    min_imbalances = parse_csv_values(args.min_imbalance, Decimal)
    min_intensities = parse_csv_values(args.min_intensity, Decimal)
    cooldowns = parse_csv_values(args.cooldown_buckets, int)

    states, load_stats = load_states(args.input_root, environment=args.environment)
    if not states:
        raise SystemExit("no usable states found")
    minimum_buckets = max(max(impulse_windows) + 4, 8) + max(confirmations)
    segments = split_contiguous_states(states, minimum_buckets=minimum_buckets)
    if not segments:
        raise SystemExit("no contiguous state segments found")
    data_start = min(segment[0].bucket_start for segment in segments)
    data_end = max(segment[-1].bucket_end for segment in segments)
    duration = data_end - data_start
    train_end = data_start + duration * 0.60
    validation_end = data_start + duration * 0.80
    splits = (
        Split("train", data_start, train_end),
        Split("validation", train_end, validation_end),
        Split("holdout", validation_end, data_end),
    )

    pools: dict[tuple[int, int], list[OrderFlowImpulseEvent]] = {}
    for impulse_window, confirmation in itertools.product(impulse_windows, confirmations):
        pools[(impulse_window, confirmation)] = build_pool(
            segments,
            impulse_window_buckets=impulse_window,
            confirmation_buckets=confirmation,
            horizons=horizons,
        )

    grid_rows: list[dict[str, object]] = []
    for (impulse_window, confirmation), events in pools.items():
        for min_return, min_imbalance, min_intensity, cooldown in itertools.product(
            min_returns, min_imbalances, min_intensities, cooldowns
        ):
            selected = select_events(
                events,
                min_return=min_return,
                min_imbalance=min_imbalance,
                min_intensity=min_intensity,
                cooldown_buckets=cooldown,
            )
            row: dict[str, object] = {
                "impulse_window_buckets": impulse_window,
                "confirmation_buckets": confirmation,
                "min_return_pct": float(min_return) * 100,
                "min_imbalance": float(min_imbalance),
                "min_intensity": float(min_intensity),
                "cooldown_buckets": cooldown,
                "n_selected_full": len(selected),
            }
            for split in splits:
                metrics = event_metrics(
                    selected,
                    horizon=args.primary_horizon,
                    split=split,
                    round_trip_cost=round_trip_cost,
                    notional=args.notional,
                )
                for key, value in metrics.items():
                    row[f"{split.name}_{key}"] = value
            validation_n = int(row["validation_n_labeled"])
            validation_mean = row["validation_mean_net_return_pct"]
            row["validation_score"] = (
                None
                if validation_n < 5 or validation_mean is None
                else round(float(validation_mean) * math.sqrt(validation_n), 8)
            )
            grid_rows.append(row)

    ranked = sorted(
        grid_rows,
        key=lambda row: (
            row["validation_score"] is not None,
            float(row["validation_score"] or -float("inf")),
            int(row["validation_n_labeled"]),
        ),
        reverse=True,
    )
    best_row = ranked[0] if ranked and ranked[0]["validation_score"] is not None else ranked[0]

    def config_from_row(row: dict[str, object]) -> dict[str, object]:
        return {
            "impulse_window_buckets": int(row["impulse_window_buckets"]),
            "confirmation_buckets": int(row["confirmation_buckets"]),
            "min_return_pct": float(row["min_return_pct"]),
            "min_imbalance": float(row["min_imbalance"]),
            "min_intensity": float(row["min_intensity"]),
            "cooldown_buckets": int(row["cooldown_buckets"]),
        }

    baseline = {
        "impulse_window_buckets": 3,
        "confirmation_buckets": 1,
        "min_return_pct": 0.10,
        "min_imbalance": 0.40,
        "min_intensity": 2.0,
        "cooldown_buckets": 8,
    }

    def selected_for_config(config: dict[str, object]) -> list[OrderFlowImpulseEvent]:
        pool = pools[(int(config["impulse_window_buckets"]), int(config["confirmation_buckets"]))]
        return select_events(
            pool,
            min_return=Decimal(str(float(config["min_return_pct"]) / 100)),
            min_imbalance=Decimal(str(config["min_imbalance"])),
            min_intensity=Decimal(str(config["min_intensity"])),
            cooldown_buckets=int(config["cooldown_buckets"]),
        )

    best_config = config_from_row(best_row)
    named_configs = {"baseline": baseline, "best_validation": best_config}
    named_events = {
        name: selected_for_config(config) for name, config in named_configs.items()
    }

    output = args.output_dir
    output.mkdir(parents=True, exist_ok=True)
    write_csv(output / "grid_results.csv", grid_rows)
    write_csv(output / "top_candidates.csv", ranked[:50])
    write_csv(
        output / "best_candidate_events.csv",
        [event_row(event, horizon=args.primary_horizon) for event in named_events["best_validation"]],
    )
    write_csv(
        output / "equity_series.csv",
        build_equity_series(
            named_events,
            horizon=args.primary_horizon,
            start=data_start,
            end=data_end,
            round_trip_cost=round_trip_cost,
            notional=args.notional,
        ),
    )

    def split_report(config: dict[str, object], events: list[OrderFlowImpulseEvent]) -> dict[str, object]:
        report: dict[str, object] = {}
        for split in splits:
            report[split.name] = event_metrics(
                events,
                horizon=args.primary_horizon,
                split=split,
                round_trip_cost=round_trip_cost,
                notional=args.notional,
            )
        report["all_horizons"] = {
            str(horizon): event_metrics(
                events,
                horizon=horizon,
                split=Split("full", data_start, data_end),
                round_trip_cost=round_trip_cost,
                notional=args.notional,
            )
            for horizon in horizons
        }
        return {"config": config, "metrics": report}

    manifest = {
        "analysis": "exploratory order-flow impulse event study",
        "source_root": str(args.input_root),
        "environment": args.environment,
        "load": load_stats,
        "contiguous_segments": len(segments),
        "symbols": len({state.symbol for state in states}),
        "data_start": data_start.isoformat(),
        "data_end": data_end.isoformat(),
        "splits": {split.name: {"start": split.start.isoformat(), "end": split.end.isoformat()} for split in splits},
        "assumptions": {
            "base_interval_seconds": 15,
            "baseline_window_buckets": 4,
            "breakout_window_buckets": 4,
            "primary_horizon_buckets": args.primary_horizon,
            "primary_horizon_minutes": args.primary_horizon * 15 / 60,
            "round_trip_cost_pct": args.round_trip_cost_pct,
            "fixed_notional_usdt": args.notional,
            "selection_score": "validation mean net return percent * sqrt(validation labeled events)",
            "gap_policy": "drop incomplete rows and split symbols at any non-15-second gap",
            "capital_model": "event study; overlapping symbols and trades are not capital constrained",
        },
        "event_pools": {
            f"impulse_{impulse}_confirmation_{confirmation}": len(events)
            for (impulse, confirmation), events in pools.items()
        },
        "baseline": split_report(baseline, named_events["baseline"]),
        "best_validation": split_report(best_config, named_events["best_validation"]),
        "top_candidates": ranked[:20],
    }
    (output / "optimization_report.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )

    best_validation = manifest["best_validation"]["metrics"]["validation"]
    holdout = manifest["best_validation"]["metrics"]["holdout"]
    report_lines = [
        "# Order-flow impulse exploratory optimization",
        "",
        f"- Data: `{data_start.isoformat()}` to `{data_end.isoformat()}`",
        f"- States: {len(states):,}; symbols: {manifest['symbols']}; contiguous segments: {len(segments):,}",
        f"- Primary horizon: {args.primary_horizon * 15 / 60:g} minutes; round-trip cost: {args.round_trip_cost_pct:.3g}%",
        "- Ranking: validation mean net return × √(validation labeled events); holdout was not used for selection.",
        "",
        "## Best validation candidate",
        "",
        f"- Parameters: `{json.dumps(best_config, ensure_ascii=False)}`",
        f"- Validation: {best_validation['n_labeled']} events, net PnL {best_validation['net_pnl_usdt']} USDT, mean net return {best_validation['mean_net_return_pct']}%, max drawdown {best_validation['max_drawdown_usdt']} USDT",
        f"- Holdout: {holdout['n_labeled']} events, net PnL {holdout['net_pnl_usdt']} USDT, mean net return {holdout['mean_net_return_pct']}%, max drawdown {holdout['max_drawdown_usdt']} USDT",
        "",
        "## Caveat",
        "",
        "This is a short-sample event study. It does not model order fills, capital limits, funding, or symbol-level overlap, and the current result must not be promoted directly to live trading.",
        "",
        "Artifacts: `grid_results.csv`, `top_candidates.csv`, `best_candidate_events.csv`, `equity_series.csv`, `optimization_report.json`.",
    ]
    (output / "optimization_report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    print(json.dumps({
        "output_dir": str(output),
        "states": len(states),
        "symbols": manifest["symbols"],
        "segments": len(segments),
        "grid_rows": len(grid_rows),
        "best_config": best_config,
        "validation": best_validation,
        "holdout": holdout,
    }, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
