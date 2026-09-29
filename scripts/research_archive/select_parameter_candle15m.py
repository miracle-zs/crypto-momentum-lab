#!/usr/bin/env python3
"""Select order-flow entry gates with one common candle_15m exit rule.

This is a local-only research replay.  It reuses the three-day event scanner,
keeps only approximate full-entry events, and ranks the parameter grid by
validation mean net return after the common first-adverse-15m-candle exit.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from array import array
from pathlib import Path

from analyze_three_day_momentum import (
    GateConfig,
    aggregate_candles,
    attach_entry_filters,
    good_segments,
    load_states,
    load_universe,
    parameter_grid,
    scan_events,
    state_price,
)
from compare_parameter_equity import CANDLE_INTERVAL_MS, build_curve


def _configs() -> list[GateConfig]:
    baseline = GateConfig("baseline_r0.010_i0.50_n2.0", 0.010, 0.50, 2.0)
    return [baseline] + [
        config
        for config in parameter_grid()
        if (
            config.min_return,
            config.min_imbalance,
            config.min_intensity,
        )
        != (baseline.min_return, baseline.min_imbalance, baseline.min_intensity)
    ]


def _write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _flatten_event(row: dict[str, object]) -> dict[str, object]:
    flattened = dict(row)
    returns = flattened.pop("forward_returns", {})
    if isinstance(returns, dict):
        for horizon, value in returns.items():
            flattened[f"fwd_{horizon}"] = value
    return flattened


def _common_exit_data(states: dict[str, object]) -> tuple[
    dict[str, list[dict[str, float | int]]],
    dict[str, tuple[array, array]],
]:
    """Convert the already-loaded state series into exit-replay inputs."""

    candles: dict[str, list[dict[str, float | int]]] = {}
    marks: dict[str, tuple[array, array]] = {}
    for symbol, raw_series in states.items():
        series = raw_series
        timestamp_values = array("q")
        price_values = array("d")
        for index, timestamp in enumerate(series.ts):  # type: ignore[attr-defined]
            price = state_price(series, index)  # type: ignore[arg-type]
            if not math.isfinite(price) or price <= 0:
                continue
            timestamp_values.append(int(timestamp) * 1_000)
            price_values.append(price)
        marks[symbol] = (timestamp_values, price_values)
        candles[symbol] = [
            {
                "start": candle.start * 1_000,
                "end": (candle.start * 1_000) + CANDLE_INTERVAL_MS,
                "open": candle.open,
                "close": candle.close,
            }
            for candle in aggregate_candles(series, good_segments(series))  # type: ignore[arg-type]
        ]
    return candles, marks


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--states", nargs="+", type=Path, required=True)
    parser.add_argument("--universe", type=Path, required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--top-count", type=int, default=100)
    parser.add_argument("--initial-equity", type=float, default=1000.0)
    parser.add_argument("--notional", type=float, default=100.0)
    parser.add_argument("--round-trip-cost", type=float, default=0.002)
    parser.add_argument("--validation-min-trades", type=int, default=3)
    args = parser.parse_args()

    start_ms = _parse_ms(args.start)
    end_ms = _parse_ms(args.end)
    if end_ms <= start_ms:
        raise ValueError("end must be after start")

    states = load_states(args.states)
    universe = load_universe(args.universe, args.top_count)
    candles_for_filters = {
        symbol: aggregate_candles(series, good_segments(series))
        for symbol, series in states.items()
    }
    candles, marks = _common_exit_data(states)
    configs = _configs()
    event_map = scan_events(states, configs, (1, 4, 12, 20))
    full_entry_events = {
        config.name: [
            row
            for row in attach_entry_filters(
                event_map[config.name],
                states,
                candles_for_filters,
                universe,
            )
            if bool(row["full_entry_pass_approx"])
        ]
        for config in configs
    }
    cutoff_ms = start_ms + int((end_ms - start_ms) * 0.60)
    rows: list[dict[str, object]] = []
    for config in configs:
        events = full_entry_events[config.name]
        train = [row for row in events if int(row["detected_ts"]) * 1000 < cutoff_ms]
        validation = [
            row for row in events if int(row["detected_ts"]) * 1000 >= cutoff_ms
        ]
        train_curve = build_curve(
            train,
            candles=candles,
            marks=marks,
            initial_equity=args.initial_equity,
            notional=args.notional,
            cost=args.round_trip_cost,
            start_ms=start_ms,
            end_ms=end_ms,
        )
        validation_curve = build_curve(
            validation,
            candles=candles,
            marks=marks,
            initial_equity=args.initial_equity,
            notional=args.notional,
            cost=args.round_trip_cost,
            start_ms=cutoff_ms,
            end_ms=end_ms,
        )
        if (
            validation_curve["trades"] >= args.validation_min_trades
            and isinstance(validation_curve["mean_net_pct"], (int, float))
        ):
            selected_curve = validation_curve
            selection_split = "validation"
        else:
            selected_curve = train_curve
            selection_split = "train_fallback"
        rows.append(
            {
                "parameter": config.name,
                "min_return_pct": config.min_return * 100,
                "min_imbalance": config.min_imbalance,
                "min_intensity": config.min_intensity,
                "all_events": len(events),
                "train_trades": train_curve["trades"],
                "validation_trades": validation_curve["trades"],
                "train_mean_net_pct": train_curve["mean_net_pct"],
                "validation_mean_net_pct": validation_curve["mean_net_pct"],
                "train_total_return_pct": train_curve["total_return_pct"],
                "validation_total_return_pct": validation_curve["total_return_pct"],
                "selection_split": selection_split,
                "score_mean_net_pct": selected_curve["mean_net_pct"],
            }
        )
    rows.sort(
        key=lambda row: float(row["score_mean_net_pct"])
        if isinstance(row["score_mean_net_pct"], (int, float))
        else -999.0,
        reverse=True,
    )
    selected = rows[0]
    output_dir: Path = args.output_dir
    _write_rows(output_dir / "candle15m_parameter_grid.csv", rows)
    baseline_name = configs[0].name
    _write_rows(
        output_dir / "baseline_events.csv",
        [_flatten_event(row) for row in full_entry_events[baseline_name]],
    )
    _write_rows(
        output_dir / "selected_events.csv",
        [_flatten_event(row) for row in full_entry_events[selected["parameter"]]],
    )
    payload = {
        "coverage_start": args.start,
        "coverage_end": args.end,
        "cutoff": _iso_ms(cutoff_ms),
        "entry_filter": "full_entry_pass_approx",
        "exit_policy": {
            "mode": "candle_15m",
            "rule": "first complete adverse 15m candle; long close < open",
            "max_holding_hours": 24,
            "round_trip_cost_bps": args.round_trip_cost * 10_000,
            "sample_end_mark": "last available 15s close",
        },
        "selected": selected,
        "ranked": rows,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "candle15m_selection.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def _parse_ms(value: str) -> int:
    from datetime import UTC, datetime

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"timestamp must include a timezone: {value!r}")
    return int(parsed.astimezone(UTC).timestamp() * 1000)


def _iso_ms(value: int) -> str:
    from datetime import UTC, datetime

    return datetime.fromtimestamp(value / 1000, UTC).isoformat()


if __name__ == "__main__":
    main()
