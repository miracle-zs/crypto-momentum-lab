#!/usr/bin/env python3
"""Replay the four deployed account configurations on the latest local data.

This deliberately reuses the fast event detector and limit/exit replay from
``optimize_volume_feature_joint_fast``.  It prepares only the two impulse /
confirmation pools needed by the live accounts and does not evaluate the
25,200-candidate optimization grid again.
"""

from __future__ import annotations

import argparse
import json
import math
import multiprocessing as mp
import sys
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import optimize_local_live_constrained as optimizer  # noqa: E402
from analyze_breakout_acceptance import SameExitConfig  # noqa: E402
from optimize_research_orderflow import (  # noqa: E402
    Split,
    build_pool,
    load_states,
    split_contiguous_states,
)
from optimize_volume_feature_joint_fast import (  # noqa: E402
    FEATURE_WINDOWS,
    FastRow,
    build_volume_ratio_lookup,
    event_csv_row,
    fast_simulate_live_limit_event,
    initial_margin_peak_rows,
    metrics_for_rows,
    select_rows,
    write_csv,
)

from crypto_momentum_lab.domain.market.models import MarketState15s  # noqa: E402
from crypto_momentum_lab.strategies.order_flow_impulse.event_study import (  # noqa: E402
    OrderFlowDirection,
)

ACCOUNT_CONFIGS: dict[str, dict[str, Any]] = {
    "primary": {
        "title": "实盘 Primary",
        "impulse_window_buckets": 4,
        "confirmation_buckets": 1,
        "min_return_pct": 0.50,
        "min_imbalance": 0.30,
        "min_intensity": 1.5,
        "min_volume_ratio": 1.50,
        "cooldown_buckets": 0,
    },
    "acc01": {
        "title": "实盘 acc01 / account-2",
        "impulse_window_buckets": 4,
        "confirmation_buckets": 1,
        "min_return_pct": 0.50,
        "min_imbalance": 0.30,
        "min_intensity": 1.5,
        "min_volume_ratio": 1.50,
        "cooldown_buckets": 0,
    },
    "acc02": {
        "title": "实盘 acc02 / account-3",
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.50,
        "min_imbalance": 0.30,
        "min_intensity": 4.0,
        "min_volume_ratio": 1.50,
        "cooldown_buckets": 0,
    },
    "acc03": {
        "title": "实盘 acc03 / account-4",
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.50,
        "min_imbalance": 0.30,
        "min_intensity": 4.0,
        "min_volume_ratio": 1.50,
        "cooldown_buckets": 0,
    },
}
VOLUME_FEATURE = "notional_5m_vs_30m"
MIN_RETURN = Decimal("0.005")
MIN_IMBALANCE = Decimal("0.30")
MIN_INTENSITY = Decimal("1.5")

_SIMULATION_CONTEXT: dict[str, Any] | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--environment", default="research")
    parser.add_argument("--top-count", type=int, default=10)
    parser.add_argument("--fee-rate", type=float, default=0.0005)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--exclude-entry-hours", default="08:00-10:00")
    parser.add_argument(
        "--exclude-entry-timezone",
        choices=("UTC", "Asia/Shanghai"),
        default="Asia/Shanghai",
    )
    return parser.parse_args()


def config_text(config: dict[str, Any]) -> str:
    return " / ".join(
        (
            str(int(config["impulse_window_buckets"])),
            str(int(config["confirmation_buckets"])),
            f"{float(config['min_return_pct']):.2f}%",
            f"{float(config['min_imbalance']):.2f}",
            f"{float(config['min_intensity']):.1f}",
            f"{float(config['min_volume_ratio']):.2f}x",
            str(int(config["cooldown_buckets"])),
        )
    )


def simulate_task(
    task: tuple[tuple[int, int], Any, Decimal | None, bool, Decimal | None],
) -> tuple[tuple[int, int], Any, Any]:
    if _SIMULATION_CONTEXT is None:
        raise RuntimeError("simulation context was not initialized")
    pool_key, event, confirmation_min, top10_allowed, volume_ratio = task
    observation = optimizer.EventObservation(
        event=event,
        confirmation_min_imbalance=confirmation_min,
        top10_proxy_allowed=top10_allowed,
        notional_5m_vs_30m=volume_ratio,
    )
    simulation = fast_simulate_live_limit_event(
        observation,
        states_by_symbol=_SIMULATION_CONTEXT["states_by_symbol"],
        state_times_by_symbol=_SIMULATION_CONTEXT["state_times_by_symbol"],
        series_by_symbol=_SIMULATION_CONTEXT["series_by_symbol"],
        candles_by_symbol=_SIMULATION_CONTEXT["candles_by_symbol"],
        candle_starts_by_symbol=_SIMULATION_CONTEXT["candle_starts_by_symbol"],
        state_by_key=_SIMULATION_CONTEXT["state_by_key"],
        exit_config=_SIMULATION_CONTEXT["exit_config"],
        data_end=_SIMULATION_CONTEXT["full_end"],
    )
    return pool_key, observation, simulation


def fast_row(observation: Any, simulation: Any) -> FastRow:
    event = observation.event
    return FastRow(
        observation=observation,
        simulation=simulation,
        volume_ratio=observation.notional_5m_vs_30m,
        excluded_entry=bool(_SIMULATION_CONTEXT["entry_exclusion"].excludes(simulation)),
        symbol=event.symbol,
        detected_epoch=event.detected_at.timestamp(),
        impulse_return=event.impulse_return_pct,
        imbalance=optimizer.directional_imbalance(event),
        confirmation_min=observation.confirmation_min_imbalance,
        intensity=event.notional_intensity,
    )


def build_tasks(
    segments: list[tuple[MarketState15s, ...]],
    *,
    state_by_key: dict[tuple[str, datetime], MarketState15s],
    proxy: Any,
    volume_lookup: dict[tuple[str, datetime], Decimal],
) -> list[tuple[tuple[int, int], Any, Decimal | None, bool, Decimal | None]]:
    tasks: list[tuple[tuple[int, int], Any, Decimal | None, bool, Decimal | None]] = []
    pool_keys = sorted(
        {
            (
                int(config["impulse_window_buckets"]),
                int(config["confirmation_buckets"]),
            )
            for config in ACCOUNT_CONFIGS.values()
        }
    )
    for impulse_window, confirmation in pool_keys:
        events = build_pool(
            segments,
            impulse_window_buckets=impulse_window,
            confirmation_buckets=confirmation,
            horizons=(1,),
        )
        pool_tasks = 0
        for event in events:
            confirmation_min = optimizer.confirmation_minimum(
                event,
                confirmation_buckets=confirmation,
                state_by_key=state_by_key,
            )
            top10_allowed = proxy.allows(event.symbol, event.detected_at)
            imbalance = optimizer.directional_imbalance(event)
            volume_ratio = volume_lookup.get((event.symbol, event.detected_at))
            if (
                event.direction is not OrderFlowDirection.UP
                or not top10_allowed
                or event.impulse_return_pct < MIN_RETURN
                or imbalance < MIN_IMBALANCE
                or confirmation_min is None
                or confirmation_min < MIN_IMBALANCE
                or event.notional_intensity < MIN_INTENSITY
            ):
                continue
            tasks.append(
                (
                    (impulse_window, confirmation),
                    event,
                    confirmation_min,
                    top10_allowed,
                    volume_ratio,
                )
            )
            pool_tasks += 1
        print(
            json.dumps(
                {
                    "phase": "pool_ready",
                    "pool": f"{impulse_window}/{confirmation}",
                    "raw_events": len(events),
                    "replay_tasks": pool_tasks,
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
    return tasks


def main() -> None:
    args = parse_args()
    if args.top_count != 10:
        raise SystemExit("--top-count is fixed at the live value 10")
    if args.workers < 1:
        raise SystemExit("--workers must be positive")
    parsed_window = optimizer.parse_entry_time_window(args.exclude_entry_hours)
    if parsed_window is None:
        raise SystemExit("an entry-time exclusion window is required")
    offsets = {"UTC": 0, "Asia/Shanghai": 8}
    entry_exclusion = optimizer.EntryTimeExclusion(
        start_minute=parsed_window[0],
        end_minute=parsed_window[1],
        timezone_label=args.exclude_entry_timezone,
        offset_hours=offsets[args.exclude_entry_timezone],
    )

    print(json.dumps({"phase": "load_start"}, ensure_ascii=False), flush=True)
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

    minimum_buckets = 11
    segments = split_contiguous_states(states, minimum_buckets=minimum_buckets)
    if not segments:
        raise SystemExit("no contiguous local research state segments found")
    full_start = min(segment[0].bucket_start for segment in segments)
    full_end = max(segment[-1].bucket_end for segment in segments)
    proxy = optimizer.build_top10_proxy(states, top_count=args.top_count)
    optimization_start = proxy.first_valid_at
    if optimization_start >= full_end:
        raise SystemExit("local data has no valid Top10 proxy window")
    duration = full_end - optimization_start
    splits = (
        Split("train", optimization_start, optimization_start + duration * 0.60),
        Split(
            "validation",
            optimization_start + duration * 0.60,
            optimization_start + duration * 0.80,
        ),
        Split("holdout", optimization_start + duration * 0.80, full_end),
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
        recovery_profit_pct=float(optimizer.LIVE_FIXED_SETTINGS["candle_grace_profit_pct"]),
        fee_rate=args.fee_rate,
        notional_usdt=float(optimizer.LIVE_FIXED_SETTINGS["entry_notional_usdt"]),
    )
    recent_buckets, baseline_buckets = FEATURE_WINDOWS[VOLUME_FEATURE]
    volume_lookup = build_volume_ratio_lookup(
        dict(states_by_symbol),
        recent_buckets=recent_buckets,
        baseline_buckets=baseline_buckets,
    )

    global _SIMULATION_CONTEXT
    _SIMULATION_CONTEXT = {
        "states_by_symbol": states_by_symbol,
        "state_times_by_symbol": state_times_by_symbol,
        "series_by_symbol": series_by_symbol,
        "candles_by_symbol": candles_by_symbol,
        "candle_starts_by_symbol": candle_starts_by_symbol,
        "state_by_key": state_by_key,
        "exit_config": exit_config,
        "full_end": full_end,
        "entry_exclusion": entry_exclusion,
    }
    tasks = build_tasks(
        segments,
        state_by_key=state_by_key,
        proxy=proxy,
        volume_lookup=volume_lookup,
    )
    print(
        json.dumps(
            {
                "phase": "replay_start",
                "tasks": len(tasks),
                "workers": min(args.workers, len(tasks) or 1),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )

    results: list[tuple[tuple[int, int], Any, Any]] = []
    worker_count = min(args.workers, len(tasks) or 1)
    if worker_count == 1:
        results = [simulate_task(task) for task in tasks]
    else:
        if "fork" not in mp.get_all_start_methods():
            raise SystemExit("account replay requires a fork-capable platform")
        context = mp.get_context("fork")
        chunk_size = max(1, math.ceil(len(tasks) / (worker_count * 8)))
        with context.Pool(processes=worker_count) as pool:
            for index, result in enumerate(
                pool.imap(simulate_task, tasks, chunksize=chunk_size), 1
            ):
                results.append(result)
                if index % 500 == 0 or index == len(tasks):
                    print(
                        json.dumps(
                            {
                                "phase": "replay_progress",
                                "completed": index,
                                "total": len(tasks),
                            },
                            ensure_ascii=False,
                        ),
                        flush=True,
                    )

    rows_by_pool: dict[tuple[int, int], list[FastRow]] = defaultdict(list)
    for pool_key, observation, simulation in results:
        rows_by_pool[pool_key].append(fast_row(observation, simulation))
    for rows in rows_by_pool.values():
        rows.sort(key=lambda row: (row.detected_epoch, row.symbol))

    output = args.output_dir
    output.mkdir(parents=True, exist_ok=True)
    account_reports: dict[str, Any] = {}
    for name, config in ACCOUNT_CONFIGS.items():
        pool = rows_by_pool[
            (int(config["impulse_window_buckets"]), int(config["confirmation_buckets"]))
        ]
        selected = select_rows(
            pool,
            min_return=Decimal(str(float(config["min_return_pct"]) / 100.0)),
            min_imbalance=Decimal(str(config["min_imbalance"])),
            min_intensity=Decimal(str(config["min_intensity"])),
            min_volume_ratio=Decimal(str(config["min_volume_ratio"])),
            cooldown_buckets=int(config["cooldown_buckets"]),
        )
        event_path = output / f"account_{name}_events.csv"
        write_csv(event_path, [event_csv_row(row, VOLUME_FEATURE) for row in selected])
        metrics = metrics_for_rows(
            selected,
            splits=splits,
            optimization_start=optimization_start,
            full_end=full_end,
        )
        account_reports[name] = {
            "title": config["title"],
            "config": config,
            "config_text": config_text(config),
            "natural_initial_margin_peak_usdt": initial_margin_peak_rows(selected),
            "metrics": metrics,
            "event_file": event_path.name,
        }

    manifest = {
        "analysis": "fast exact replay of deployed account configurations",
        "source_root": str(args.input_root),
        "environment": args.environment,
        "load": load_stats,
        "data_start": full_start.isoformat(),
        "data_end": full_end.isoformat(),
        "optimization_window": {
            "start": optimization_start.isoformat(),
            "end": full_end.isoformat(),
            "first_partial_utc_day": proxy.first_partial_utc_day.isoformat(),
        },
        "volume_feature": VOLUME_FEATURE,
        "volume_windows": {
            "recent_buckets": recent_buckets,
            "baseline_buckets": baseline_buckets,
            "bucket_seconds": 15,
        },
        "entry_time_exclusion": {
            "window": entry_exclusion.window_text,
            "timezone": entry_exclusion.timezone_label,
            "interval": "[start, end)",
        },
        "accounts": account_reports,
    }
    (output / "account_replay_report.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "phase": "complete",
                "output": str(output),
                "accounts": {
                    name: {
                        "config": item["config_text"],
                        "full_pnl": item["metrics"]["full"]["net_pnl_usdt"],
                        "full_dd": item["metrics"]["full"]["max_drawdown_usdt"],
                        "margin": item["natural_initial_margin_peak_usdt"],
                        "closed": item["metrics"]["full"]["n_closed"],
                    }
                    for name, item in account_reports.items()
                },
            },
            ensure_ascii=False,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
