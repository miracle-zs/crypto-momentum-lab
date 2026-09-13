#!/usr/bin/env python3
"""Extend the previously selected A-G configurations over newer local data.

This is intentionally a replay, not a second optimization pass.  The
parameter values are read from the prior optimization reports, then each
configuration is evaluated over the complete local Parquet window.  Keeping
the parameters frozen makes the added period a genuine forward extension of
the comparison that was already shown.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import optimize_local_live_constrained as optimizer  # noqa: E402
from analyze_breakout_acceptance import SameExitConfig  # noqa: E402
from optimize_local_live_constrained import (  # noqa: E402
    LIVE_CONFIG_HASH,
    LIVE_FIXED_SETTINGS,
    EventObservation,
    SimulatedEvent,
    all_metrics,
    as_series,
    build_pnl_series,
    build_top10_proxy,
    complete_candles,
    confirmation_minimum,
    event_csv_row,
    initial_margin_peak,
    metrics_for_split,
    select_observations,
    simulate_live_limit_event,
    state_price,
    write_csv,
)
from optimize_research_orderflow import (  # noqa: E402
    BUCKET,
    Split,
    build_pool,
    load_states,
    split_contiguous_states,
)

STRATEGIES = {
    "A": {
        "key": "unconstrained",
        "source": "optimization-live-pnl-20260905",
        "label": "寻优 A（无保证金上限）",
        "objective": "validation_net_pnl_usdt",
        "default_drawdown_weight": 0.0,
    },
    "B": {
        "key": "margin350",
        "source": "optimization-live-pnl-margin350-20260905",
        "label": "寻优 B（保证金≤350U，自由cooldown）",
        "objective": "validation_net_pnl_usdt",
        "default_drawdown_weight": 0.0,
    },
    "C": {
        "key": "margin350cd0",
        "source": "optimization-live-pnl-margin350-cd0-20260905",
        "label": "寻优 C（保证金≤350U，固定cooldown=0）",
        "objective": "validation_net_pnl_usdt",
        "default_drawdown_weight": 0.0,
    },
    "D": {
        "key": "margin280",
        "source": "optimization-live-pnl-margin280-20260905",
        "label": "寻优 D（保证金≤280U，自由cooldown）",
        "objective": "validation_net_pnl_usdt",
        "default_drawdown_weight": 0.0,
    },
    "E": {
        "key": "margin280cd0",
        "source": "optimization-live-pnl-margin280-cd0-20260905",
        "label": "寻优 E（保证金≤280U，固定cooldown=0）",
        "objective": "validation_net_pnl_usdt",
        "default_drawdown_weight": 0.0,
    },
    "F": {
        "key": "drawdown_cd0",
        "source": "optimization-live-pnl-dd-margin280-cd0-20260905",
        "label": "寻优 F（保证金≤280U，固定cooldown=0；PnL优先、回撤次优）",
        "objective": "validation_net_pnl_usdt_minus_0.1_times_validation_max_drawdown_usdt",
        "default_drawdown_weight": 0.10,
    },
    "G": {
        "key": "drawdown_free",
        "source": "optimization-live-pnl-dd-margin280-free-20260905",
        "label": "寻优 G（保证金≤280U，自由cooldown；PnL优先、回撤次优）",
        "objective": "validation_net_pnl_usdt_minus_0.1_times_validation_max_drawdown_usdt",
        "default_drawdown_weight": 0.10,
    },
}

BASELINE_CONFIG = {
    "impulse_window_buckets": 3,
    "confirmation_buckets": 1,
    "min_return_pct": 1.00,
    "min_imbalance": 0.40,
    "min_intensity": 2.0,
    "min_notional_5m_vs_30m": 0.0,
    "cooldown_buckets": 0,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-root",
        type=Path,
        default=Path("server_exports/cml-research-data-20260905/parquet"),
    )
    parser.add_argument(
        "--source-root",
        type=Path,
        default=Path("server_exports/cml-research-data-20260905"),
    )
    parser.add_argument(
        "--source-template",
        default=None,
        help=(
            "optional directory template under --source-root for prior A-G "
            "reports; {label} is replaced with A through G"
        ),
    )
    parser.add_argument(
        "--live-signals",
        type=Path,
        default=Path(
            "server_exports/cml-live-current-latest-20260905/live_strategy_signals.csv.gz"
        ),
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        required=True,
        help="directory containing one replay-A ... replay-G report directory",
    )
    parser.add_argument("--environment", default="research")
    parser.add_argument("--top-count", type=int, default=10)
    parser.add_argument("--fee-rate", type=float, default=0.0005)
    return parser.parse_args()


def source_report(
    args: argparse.Namespace,
    strategy_name: str,
    profile: dict[str, object],
) -> dict[str, object]:
    source_name = str(profile["source"])
    if args.source_template is not None:
        source_name = args.source_template.format(label=strategy_name)
    profile["_resolved_source_name"] = source_name
    path = args.source_root / source_name / "optimization_report.json"
    return json.loads(path.read_text(encoding="utf-8"))


def unique_pairs() -> set[tuple[int, int]]:
    pairs = {(3, 1)}
    for profile in STRATEGIES.values():
        pairs.add((
            int(profile["config"]["impulse_window_buckets"]),
            int(profile["config"]["confirmation_buckets"]),
        ))
    return pairs


def prepare_replay(args: argparse.Namespace) -> tuple[
    dict[str, object],
    dict[tuple[int, int], list[EventObservation]],
    dict[int, SimulatedEvent],
    tuple[Split, ...],
    datetime,
    datetime,
    datetime,
]:
    states, load_stats = load_states(args.input_root, environment=args.environment)
    if not states:
        raise SystemExit("no usable local research states found")

    states_by_symbol: defaultdict[str, list[object]] = defaultdict(list)
    state_by_key: dict[tuple[str, datetime], object] = {}
    for state in states:
        states_by_symbol[state.symbol].append(state)
        state_by_key[(state.symbol, state.bucket_start)] = state
    for symbol_states in states_by_symbol.values():
        symbol_states.sort(key=lambda item: item.bucket_start)

    minimum_buckets = max(4 + 4, 8) + 3
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
        decision_profit_pct=float(LIVE_FIXED_SETTINGS["candle_grace_decision_profit_pct"]),
        recovery_profit_pct=float(LIVE_FIXED_SETTINGS["candle_grace_profit_pct"]),
        fee_rate=args.fee_rate,
        notional_usdt=float(LIVE_FIXED_SETTINGS["entry_notional_usdt"]),
    )
    notional_volume_ratios = optimizer.build_notional_volume_ratio_lookup(
        states_by_symbol
    )

    pools: dict[tuple[int, int], list[EventObservation]] = {}
    simulated: dict[int, SimulatedEvent] = {}
    for impulse_window, confirmation in unique_pairs():
        events = build_pool(
            segments,
            impulse_window_buckets=impulse_window,
            confirmation_buckets=confirmation,
            horizons=(1,),
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
                top10_proxy_allowed=proxy.allows(event.symbol, event.detected_at),
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

    metadata = {
        "load": load_stats,
        "states": states,
        "states_by_symbol": states_by_symbol,
        "segments": segments,
        "proxy": proxy,
        "series_by_symbol": series_by_symbol,
        "candles_by_symbol": candles_by_symbol,
        "pools": pools,
        "symbols": len({state.symbol for state in states}),
        "live_signal_metadata": optimizer.read_live_signal_metadata(args.live_signals),
    }
    return metadata, pools, simulated, splits, full_start, full_end, optimization_start


def selected_for_config(
    config: dict[str, object],
    pools: dict[tuple[int, int], list[EventObservation]],
) -> list[EventObservation]:
    return select_observations(
        pools[(
            int(config["impulse_window_buckets"]),
            int(config["confirmation_buckets"]),
        )],
        min_return=optimizer.Decimal(str(float(config["min_return_pct"]) / 100.0)),
        min_imbalance=optimizer.Decimal(str(config["min_imbalance"])),
        min_intensity=optimizer.Decimal(str(config["min_intensity"])),
        cooldown_buckets=int(config["cooldown_buckets"]),
        min_notional_5m_vs_30m=optimizer.Decimal(
            str(config.get("min_notional_5m_vs_30m", 0.0))
        ),
    )


def report_for(
    *,
    config: dict[str, object],
    selected: list[EventObservation],
    simulated: dict[int, SimulatedEvent],
    splits: tuple[Split, ...],
    optimization_start: datetime,
    full_end: datetime,
    cap: float | None,
    drawdown_weight: float,
) -> dict[str, object]:
    split_metrics = all_metrics(selected, simulated, splits=splits)
    validation = split_metrics["validation"]
    pnl = validation.get("net_pnl_usdt")
    drawdown = validation.get("max_drawdown_usdt")
    score = None
    if pnl is not None and drawdown is not None:
        score = round(float(pnl) - drawdown_weight * float(drawdown), 8)
    peak = initial_margin_peak(selected, simulated)
    feasible = cap is None or peak <= cap + 1e-9
    return {
        "config": config,
        "natural_initial_margin_peak_usdt": peak,
        "margin_constraint_feasible": feasible,
        "selection_score": score,
        "metrics": {
            **split_metrics,
            "full": metrics_for_split(
                selected,
                simulated,
                split=Split("full", optimization_start, full_end),
            ),
        },
    }


def build_manifest(
    *,
    args: argparse.Namespace,
    profile: dict[str, object],
    prior: dict[str, object],
    metadata: dict[str, object],
    pools: dict[tuple[int, int], list[EventObservation]],
    simulated: dict[int, SimulatedEvent],
    splits: tuple[Split, ...],
    full_start: datetime,
    full_end: datetime,
    optimization_start: datetime,
) -> tuple[dict[str, object], dict[str, list[EventObservation]]]:
    config = dict(prior["best_validation"]["config"])
    cap_value = prior.get("margin_constraint", {}).get("max_initial_margin_usdt")
    cap = None if cap_value is None else float(cap_value)
    objective = str(profile["objective"])
    weight = float(prior.get("drawdown_weight", profile["default_drawdown_weight"]))
    if objective == "validation_net_pnl_usdt":
        weight = 0.0

    named_selected = {
        "baseline": selected_for_config(BASELINE_CONFIG, pools),
        "best_validation": selected_for_config(config, pools),
    }
    best = report_for(
        config=config,
        selected=named_selected["best_validation"],
        simulated=simulated,
        splits=splits,
        optimization_start=optimization_start,
        full_end=full_end,
        cap=cap,
        drawdown_weight=weight,
    )
    baseline = report_for(
        config=BASELINE_CONFIG,
        selected=named_selected["baseline"],
        simulated=simulated,
        splits=splits,
        optimization_start=optimization_start,
        full_end=full_end,
        cap=cap,
        drawdown_weight=weight,
    )
    proxy = metadata["proxy"]
    manifest: dict[str, object] = {
        "analysis": "frozen A-G local live replay over latest research export",
        "evaluation_mode": "frozen_parameters_extended_replay",
        "source_optimization_report": str(
            args.source_root
            / str(profile.get("_resolved_source_name", profile["source"]))
            / "optimization_report.json"
        ),
        "selection_objective": objective,
        "drawdown_weight": weight,
        "source_root": str(args.input_root),
        "environment": args.environment,
        "load": metadata["load"],
        "contiguous_segments": len(metadata["segments"]),
        "symbols": metadata["symbols"],
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
        "fixed_live_settings": LIVE_FIXED_SETTINGS,
        "margin_constraint": {
            "mode": "parameter_set_natural_peak" if cap is not None else "none",
            "max_initial_margin_usdt": cap,
            "initial_margin_per_entry_usdt": 20.0,
            "max_full_entries_at_once": None if cap is None else math.floor(cap / 20.0),
        },
        "fixed_cooldown_buckets": prior.get("fixed_cooldown_buckets"),
        "optimized_parameters": prior.get("optimized_parameters", []),
        "top10_proxy": prior.get("top10_proxy", {
            "mode": "local_available_symbols_positive_utc_day_return",
            "source_symbol_count": proxy.source_symbol_count,
            "first_valid_at": proxy.first_valid_at.isoformat(),
            "exact_live_universe_snapshots_available": False,
        }),
        "local_live_signal_metadata": metadata["live_signal_metadata"],
        "assumptions": {
            "replay_mode": (
                "parameters are frozen from the prior report; no parameter search was "
                "performed on the newly appended period"
            ),
            "entry_fill": (
                "effective live long LIMIT touched by a later local 15s low before the "
                "900s TTL; fill at the limit price"
            ),
            "exit_replay": (
                "complete local 15s bars aggregated to 15m; first eligible bearish "
                "candle, direct +0.10% close or +0.88% recovery limit with B8"
            ),
            "capital_model": (
                "independent 100U positions in Hedge Mode; natural peak cap is a "
                "parameter-set filter, with no dynamic rejection after selection"
            ),
            "leverage_effect": "5x recorded; 100U entry uses 20U initial margin and PnL is not multiplied",
            "funding": "not modeled from the local market-state export",
        },
        "event_pools": {
            f"impulse_{impulse}_confirmation_{confirmation}": len(observations)
            for (impulse, confirmation), observations in pools.items()
        },
        "baseline": baseline,
        "best_validation": best,
        "top_candidates": [],
        "holdout_summary": {
            "best_validation": best["metrics"]["holdout"],
            "baseline": baseline["metrics"]["holdout"],
        },
    }
    return manifest, named_selected


def write_strategy_output(
    *,
    output_dir: Path,
    manifest: dict[str, object],
    named_selected: dict[str, list[EventObservation]],
    simulated: dict[int, SimulatedEvent],
    full_start: datetime,
    full_end: datetime,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(
        output_dir / "best_candidate_events.csv",
        [
            event_csv_row(observation, simulated[id(observation)])
            for observation in named_selected["best_validation"]
        ],
    )
    write_csv(
        output_dir / "baseline_events.csv",
        [
            event_csv_row(observation, simulated[id(observation)])
            for observation in named_selected["baseline"]
        ],
    )
    write_csv(
        output_dir / "equity_series.csv",
        build_pnl_series(
            named_selected,
            simulated,
            start=full_start,
            end=full_end,
        ),
    )
    (output_dir / "optimization_report.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    config = manifest["best_validation"]["config"]
    metrics = manifest["best_validation"]["metrics"]
    lines = [
        "# A-G 最新数据扩展回放（冻结参数）",
        "",
        f"- 来源参数报告：`{manifest['source_optimization_report']}`",
        f"- 最新本地数据：`{manifest['data_start']}` 至 `{manifest['data_end']}`",
        f"- 参数：`{json.dumps(config, ensure_ascii=False)}`",
        f"- 验证集绝对净 PnL：{metrics['validation']['net_pnl_usdt']}",
        f"- 验证集最大回撤：{metrics['validation']['max_drawdown_usdt']}",
        f"- 留出集绝对净 PnL：{metrics['holdout']['net_pnl_usdt']}",
        f"- 全窗口绝对净 PnL：{metrics['full']['net_pnl_usdt']}",
        "",
        "本目录用于把既有参数延伸到服务器最新采集数据；它没有在新增数据上重新寻优。",
    ]
    (output_dir / "optimization_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    if args.top_count != 10:
        raise SystemExit("--top-count is fixed at the live value 10")

    for name, profile in STRATEGIES.items():
        prior = source_report(args, name, profile)
        profile["config"] = dict(prior["best_validation"]["config"])

    metadata, pools, simulated, splits, full_start, full_end, optimization_start = prepare_replay(args)
    args.output_root.mkdir(parents=True, exist_ok=True)
    summary: dict[str, object] = {
        "evaluation_mode": "frozen_parameters_extended_replay",
        "data_start": full_start.isoformat(),
        "data_end": full_end.isoformat(),
        "strategies": {},
    }
    for name, profile in STRATEGIES.items():
        prior = source_report(args, name, profile)
        manifest, named_selected = build_manifest(
            args=args,
            profile=profile,
            prior=prior,
            metadata=metadata,
            pools=pools,
            simulated=simulated,
            splits=splits,
            full_start=full_start,
            full_end=full_end,
            optimization_start=optimization_start,
        )
        output_dir = args.output_root / f"replay-{name}"
        write_strategy_output(
            output_dir=output_dir,
            manifest=manifest,
            named_selected=named_selected,
            simulated=simulated,
            full_start=full_start,
            full_end=full_end,
        )
        summary["strategies"][name] = {
            "output_dir": str(output_dir),
            "config": manifest["best_validation"]["config"],
            "metrics": manifest["best_validation"]["metrics"],
            "natural_initial_margin_peak_usdt": manifest["best_validation"][
                "natural_initial_margin_peak_usdt"
            ],
        }
    (args.output_root / "replay_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
