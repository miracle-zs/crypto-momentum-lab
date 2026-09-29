from __future__ import annotations

import argparse
import bisect
import csv
import gzip
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

RUN_ID = "paper-account-03-liquidation-v1"


def parse_seconds(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return int(parsed.timestamp())


def iso(value: int | None) -> str | None:
    return None if value is None else datetime.fromtimestamp(value, UTC).isoformat()


def read_heartbeats(path: Path) -> list[int]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        return sorted(
            {
                parse_seconds(row["observed_at"])
                for row in csv.DictReader(handle)
                if row["run_id"] == RUN_ID
            }
        )


def read_actual_signals(path: Path) -> list[tuple[str, str, int]]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        return [
            (
                row["symbol"],
                row["side"],
                parse_seconds(row["source_state_at"]),
            )
            for row in csv.DictReader(handle)
            if row["run_id"] == RUN_ID
        ]


def read_extra_signals(path: Path) -> list[tuple[str, str, int]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [
        (row["symbol"], row["side"], parse_seconds(row["detected_at"]))
        for row in payload["extra_examples"]
    ]


def event_heartbeat(
    event: tuple[str, str, int],
    heartbeats: list[int],
) -> dict[str, object]:
    symbol, side, event_at = event
    position = bisect.bisect_right(heartbeats, event_at)
    previous_at = heartbeats[position - 1] if position > 0 else None
    next_at = heartbeats[position] if position < len(heartbeats) else None
    previous_distance = event_at - previous_at if previous_at is not None else None
    next_distance = next_at - event_at if next_at is not None else None
    distances = [
        value for value in (previous_distance, next_distance) if value is not None
    ]
    nearest = min(distances) if distances else None
    surrounding_gap = (
        next_at - previous_at
        if previous_at is not None and next_at is not None
        else None
    )
    return {
        "symbol": symbol,
        "side": side,
        "detected_at": iso(event_at),
        "previous_heartbeat_at": iso(previous_at),
        "next_heartbeat_at": iso(next_at),
        "previous_distance_seconds": previous_distance,
        "next_distance_seconds": next_distance,
        "nearest_distance_seconds": nearest,
        "surrounding_gap_seconds": surrounding_gap,
        "within_90_seconds": nearest is not None and nearest <= 90,
        "inside_gap_over_120_seconds": surrounding_gap is not None
        and surrounding_gap > 120,
    }


def summarize(records: list[dict[str, object]]) -> dict[str, object]:
    distances = sorted(
        int(value)
        for row in records
        if (value := row["nearest_distance_seconds"]) is not None
    )

    def percentile(fraction: float) -> int | None:
        if not distances:
            return None
        return distances[round((len(distances) - 1) * fraction)]

    by_day = Counter(str(row["detected_at"])[:10] for row in records)
    return {
        "count": len(records),
        "within_90_seconds_count": sum(
            bool(row["within_90_seconds"]) for row in records
        ),
        "inside_gap_over_120_seconds_count": sum(
            bool(row["inside_gap_over_120_seconds"]) for row in records
        ),
        "nearest_heartbeat_p50_seconds": percentile(0.50),
        "nearest_heartbeat_p90_seconds": percentile(0.90),
        "nearest_heartbeat_p99_seconds": percentile(0.99),
        "nearest_heartbeat_max_seconds": max(distances, default=None),
        "by_day": dict(sorted(by_day.items())),
    }


def heartbeat_gaps(heartbeats: list[int]) -> list[dict[str, object]]:
    return [
        {
            "previous_at": iso(previous),
            "next_at": iso(current),
            "gap_seconds": current - previous,
        }
        for previous, current in zip(heartbeats, heartbeats[1:], strict=False)
        if current - previous > 120
    ]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--equity", type=Path, required=True)
    parser.add_argument("--signals", type=Path, required=True)
    parser.add_argument("--replication", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    heartbeats = read_heartbeats(args.equity)
    actual = [
        event_heartbeat(event, heartbeats)
        for event in read_actual_signals(args.signals)
    ]
    extra = [
        event_heartbeat(event, heartbeats)
        for event in read_extra_signals(args.replication)
    ]
    gaps = heartbeat_gaps(heartbeats)
    payload = {
        "run_id": RUN_ID,
        "heartbeat_count": len(heartbeats),
        "first_heartbeat_at": iso(heartbeats[0]) if heartbeats else None,
        "last_heartbeat_at": iso(heartbeats[-1]) if heartbeats else None,
        "gap_over_120_seconds_count": len(gaps),
        "largest_gaps": sorted(
            gaps,
            key=lambda row: int(row["gap_seconds"]),
            reverse=True,
        )[:30],
        "actual_signal_summary": summarize(actual),
        "extra_signal_summary": summarize(extra),
        "extra_signal_details": extra,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
