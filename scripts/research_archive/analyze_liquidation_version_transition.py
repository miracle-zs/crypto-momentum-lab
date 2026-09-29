from __future__ import annotations

import argparse
import bisect
import csv
import gzip
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import replay_liquidation_entry_variants as replay

BASELINE_RUN_ID = "paper-account-03-liquidation-v1"
THRESHOLD_CHANGE_COMMIT = "d14fceea04c5520f3160e3233f22041c4a45b4c9"
THRESHOLD_CHANGE_AT = datetime(2026, 8, 3, 9, 4, 57, tzinfo=UTC)
OLD_IMBALANCE_THRESHOLD = 0.50
NEW_IMBALANCE_THRESHOLD = 0.33


def parse_seconds(value: str) -> int:
    return replay.parse_utc_seconds(value)


def iso(value: int) -> str:
    rendered = replay.iso_utc(value)
    if rendered is None:
        raise AssertionError("timestamp must not be None")
    return rendered


def signal_key(row: dict[str, str]) -> tuple[str, str, int]:
    return row["symbol"], row["side"], parse_seconds(row["source_state_at"])


def read_signal_history(path: Path) -> dict[str, object]:
    keys_by_run: dict[str, set[tuple[str, str, int]]] = defaultdict(set)
    config_rows: dict[str, list[tuple[int, float]]] = defaultdict(list)
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["strategy_name"] != "liquidation_cascade":
                continue
            key = signal_key(row)
            keys_by_run[row["run_id"]].add(key)
            if row["run_id"] != BASELINE_RUN_ID:
                continue
            features = json.loads(row["features"])
            imbalance = abs(float(features["aggressive_imbalance"]))
            config_rows[row["config_hash"]].append((key[2], imbalance))

    baseline = keys_by_run[BASELINE_RUN_ID]
    run_comparisons = {}
    for run_id, keys in sorted(keys_by_run.items()):
        run_comparisons[run_id] = {
            "signal_count": len(keys),
            "missing_vs_baseline": len(baseline - keys),
            "extra_vs_baseline": len(keys - baseline),
        }

    configs = []
    for config_hash, rows in config_rows.items():
        rows.sort()
        configs.append(
            {
                "config_hash": config_hash,
                "signal_count": len(rows),
                "first_signal_at": iso(rows[0][0]),
                "last_signal_at": iso(rows[-1][0]),
                "minimum_abs_aggressive_imbalance": min(
                    imbalance for _, imbalance in rows
                ),
                "below_old_threshold_count": sum(
                    imbalance < OLD_IMBALANCE_THRESHOLD
                    for _, imbalance in rows
                ),
            }
        )
    configs.sort(key=lambda row: str(row["first_signal_at"]))
    return {
        "run_comparisons": run_comparisons,
        "baseline_config_periods": configs,
    }


def read_extra_events(path: Path) -> list[tuple[str, str, int]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [
        (row["symbol"], row["side"], parse_seconds(row["detected_at"]))
        for row in payload["extra_examples"]
    ]


def read_gap_details(path: Path) -> dict[tuple[str, str, int], dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        (
            str(row["symbol"]),
            str(row["side"]),
            parse_seconds(str(row["detected_at"])),
        ): row
        for row in payload["extra_signal_details"]
    }


def old_threshold_passes(
    event: replay.CascadeEvent,
    confirmation_imbalance: float,
) -> bool:
    if event.direction == "up":
        return (
            event.aggressive_imbalance >= OLD_IMBALANCE_THRESHOLD
            and confirmation_imbalance >= OLD_IMBALANCE_THRESHOLD
        )
    return (
        event.aggressive_imbalance <= -OLD_IMBALANCE_THRESHOLD
        and confirmation_imbalance <= -OLD_IMBALANCE_THRESHOLD
    )


def event_features(
    sorted_states: Path,
    target_keys: set[tuple[str, str, int]],
) -> dict[tuple[str, str, int], dict[str, object]]:
    target_symbols = {symbol for symbol, _, _ in target_keys}
    output: dict[tuple[str, str, int], dict[str, object]] = {}
    for states in replay.iter_symbol_states(sorted_states):
        if states.symbol not in target_symbols:
            continue
        events = replay.detect_events(states)[replay.BASELINE_DETECTION]
        for event in events:
            side = "long" if event.direction == "up" else "short"
            key = states.symbol, side, event.detected_at
            if key not in target_keys:
                continue
            confirmation = replay._single_bucket_imbalance(states, event.index)
            output[key] = {
                "cluster_aggressive_imbalance": event.aggressive_imbalance,
                "confirmation_aggressive_imbalance": confirmation,
                "passes_old_50pct_threshold": old_threshold_passes(
                    event,
                    confirmation,
                ),
            }
    return output


def historical_replication_events(
    states: replay.SymbolStates,
    events: list[replay.CascadeEvent],
    run_started_at: int,
    memberships: replay.MembershipTimeline,
) -> set[tuple[str, str, int]]:
    strategy_times = [states.at[index] for index in states.strategy_rows]
    start_strategy_index = bisect.bisect_left(strategy_times, run_started_at)
    if start_strategy_index >= len(states.strategy_rows):
        return set()
    segment_first: dict[int, int] = {}
    for strategy_index, raw_index in enumerate(states.strategy_rows):
        segment_first.setdefault(states.segment[raw_index], strategy_index)
    eligible = []
    warmup_prior_buckets = max(
        replay.BASELINE_DETECTION.breakout_window,
        replay.BASELINE_DETECTION.liquidation_window - 1,
    )
    change_at = int(THRESHOLD_CHANGE_AT.timestamp())
    for event in events:
        if event.detected_at < run_started_at:
            continue
        effective_start = max(
            start_strategy_index,
            segment_first[event.segment],
        )
        if event.strategy_index - effective_start < warmup_prior_buckets:
            continue
        if (
            event.detected_at < change_at
            and not old_threshold_passes(
                event,
                event.confirmation_imbalance,
            )
        ):
            continue
        eligible.append(event)
    baseline = next(
        candidate
        for candidate in replay.ALL_CANDIDATES
        if candidate.candidate_id == replay.BASELINE_CANDIDATE_ID
    )
    return {
        (
            states.symbol,
            "long" if event.direction == "up" else "short",
            event.detected_at,
        )
        for event in replay.gate_events(eligible, baseline)
        if memberships.contains(states.symbol, event.detected_at)
    }


def recompute_replications(
    args: argparse.Namespace,
    gap_details: dict[tuple[str, str, int], dict[str, object]],
) -> dict[str, object]:
    memberships = replay.read_membership_timeline(
        args.snapshots,
        args.memberships,
    )
    run_started_at = replay.read_run_start(args.runs, BASELINE_RUN_ID)
    current: set[tuple[str, str, int]] = set()
    historical: set[tuple[str, str, int]] = set()
    for states in replay.iter_symbol_states(args.sorted_states):
        events = replay.detect_events(states)[replay.BASELINE_DETECTION]
        current.update(
            replay.runtime_replication_events(
                states,
                events,
                run_started_at,
                memberships,
            )
        )
        historical.update(
            historical_replication_events(
                states,
                events,
                run_started_at,
                memberships,
            )
        )
    actual = replay.read_actual_signals(args.signals, BASELINE_RUN_ID)
    known_gap_keys = {
        key
        for key, row in gap_details.items()
        if bool(row.get("inside_gap_over_120_seconds"))
    }
    return {
        "fixed_33pct_from_account_start": replay.signal_replication_payload(
            actual,
            current,
        ),
        "historical_50pct_then_33pct": replay.signal_replication_payload(
            actual,
            historical,
        ),
        "historical_excluding_known_daemon_gaps": (
            replay.signal_replication_payload(
                actual,
                historical - known_gap_keys,
            )
        ),
    }


def analyze(args: argparse.Namespace) -> dict[str, object]:
    extras = read_extra_events(args.replication)
    gap_details = read_gap_details(args.daemon_gaps)
    features = event_features(args.sorted_states, set(extras))
    change_at = int(THRESHOLD_CHANGE_AT.timestamp())
    details = []
    for key in extras:
        symbol, side, detected_at = key
        event = features.get(key)
        heartbeat = gap_details.get(key, {})
        before_change = detected_at < change_at
        passes_old = (
            None if event is None else bool(event["passes_old_50pct_threshold"])
        )
        threshold_explained = before_change and passes_old is False
        gap_explained = bool(heartbeat.get("inside_gap_over_120_seconds"))
        runtime_eligibility_explained = event is None
        details.append(
            {
                "symbol": symbol,
                "side": side,
                "detected_at": iso(detected_at),
                "before_threshold_change": before_change,
                **({} if event is None else event),
                "explained_by_historical_threshold": threshold_explained,
                "inside_daemon_gap_over_120_seconds": gap_explained,
                "explained_by_runtime_close_requirement": (
                    runtime_eligibility_explained
                ),
                "remaining_unexplained": (
                    not threshold_explained
                    and not gap_explained
                    and not runtime_eligibility_explained
                ),
            }
        )

    return {
        "historical_change": {
            "commit": THRESHOLD_CHANGE_COMMIT,
            "commit_at": THRESHOLD_CHANGE_AT.isoformat(),
            "old_min_aggressive_imbalance": OLD_IMBALANCE_THRESHOLD,
            "new_min_aggressive_imbalance": NEW_IMBALANCE_THRESHOLD,
        },
        "signal_history": read_signal_history(args.signals),
        "replication_recomputed": recompute_replications(args, gap_details),
        "extra_signal_summary": {
            "count": len(details),
            "before_change_count": sum(
                bool(row["before_threshold_change"]) for row in details
            ),
            "explained_by_historical_threshold_count": sum(
                bool(row["explained_by_historical_threshold"])
                for row in details
            ),
            "inside_daemon_gap_count": sum(
                bool(row["inside_daemon_gap_over_120_seconds"])
                for row in details
            ),
            "explained_by_runtime_close_requirement_count": sum(
                bool(row["explained_by_runtime_close_requirement"])
                for row in details
            ),
            "remaining_unexplained_count": sum(
                bool(row["remaining_unexplained"]) for row in details
            ),
            "missing_event_feature_count": sum(
                key not in features for key in extras
            ),
        },
        "extra_signal_details": details,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sorted-states", type=Path, required=True)
    parser.add_argument("--signals", type=Path, required=True)
    parser.add_argument("--runs", type=Path, required=True)
    parser.add_argument("--snapshots", type=Path, required=True)
    parser.add_argument("--memberships", type=Path, required=True)
    parser.add_argument("--replication", type=Path, required=True)
    parser.add_argument("--daemon-gaps", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    payload = analyze(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(payload["extra_signal_summary"], ensure_ascii=False, indent=2))
    print(json.dumps(payload["signal_history"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
