"""Build per-variant capital-usage series for the paired orderflow replay."""

from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime
from pathlib import Path

VARIANTS = ("b0", "b1", "b2", "b3", "b4", "b8", "b16", "b96")


def parse_dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def is_filled(value: str | None) -> bool:
    return value not in (None, "")


def read_positions(path: Path, side: str) -> dict[str, tuple[str, float, datetime]]:
    positions: dict[str, tuple[str, float, datetime]] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if side != "all" and row.get("side") != side:
                continue
            notional = float(row.get("entry_notional") or 0.0)
            if notional <= 0:
                continue
            positions[row["position_id"]] = (
                row.get("side", ""),
                notional,
                parse_dt(row["opened_at"]),
            )
    return positions


def read_trades(
    path: Path,
    positions: dict[str, tuple[str, float, datetime]],
    common_only: bool,
) -> list[dict[str, object]]:
    trades: list[dict[str, object]] = []
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            position_id = row.get("position_id", "")
            position = positions.get(position_id)
            if position is None:
                continue
            if common_only and not all(
                is_filled(row.get(f"{variant}_pnl")) for variant in VARIANTS
            ):
                continue
            trades.append(
                {
                    "opened_at": position[2],
                    "notional": position[1],
                    **{
                        variant: (
                            parse_dt(row["closed_at"])
                            if variant == "b0" and is_filled(row.get("closed_at"))
                            else (
                                parse_dt(row[f"{variant}_closed_at"])
                                if is_filled(row.get(f"{variant}_closed_at"))
                                else None
                            )
                        )
                        for variant in VARIANTS
                    },
                }
            )
    return trades


def read_timestamps(path: Path) -> list[datetime]:
    with path.open(newline="", encoding="utf-8") as handle:
        return [parse_dt(row["timestamp"]) for row in csv.DictReader(handle)]


def build_series(
    trades: list[dict[str, object]], timestamps: list[datetime]
) -> list[dict[str, object]]:
    series: list[dict[str, object]] = []
    for timestamp in timestamps:
        point: dict[str, object] = {"timestamp": timestamp.isoformat()}
        for variant in VARIANTS:
            capital = sum(
                float(trade["notional"])
                for trade in trades
                if trade["opened_at"] <= timestamp
                and (trade[variant] is None or timestamp < trade[variant])
            )
            point[f"{variant}_capital_used"] = round(capital, 6)
        series.append(point)
    return series


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--positions", type=Path, required=True)
    parser.add_argument("--trades", type=Path, required=True)
    parser.add_argument("--equity-series", type=Path, required=True)
    parser.add_argument("--side", choices=("all", "long", "short"), default="all")
    parser.add_argument(
        "--common-only",
        action="store_true",
        help="exclude variants that are not yet mature at the replay horizon",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    positions = read_positions(args.positions, args.side)
    trades = read_trades(args.trades, positions, args.common_only)
    timestamps = read_timestamps(args.equity_series)
    series = build_series(trades, timestamps)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        fields = ["timestamp", *(f"{variant}_capital_used" for variant in VARIANTS)]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(series)
    print(
        {
            "output": str(args.output),
            "side": args.side,
            "common_trades": len(trades),
            "points": len(series),
            "peak_capital": {
                variant: max(
                    (float(row[f"{variant}_capital_used"]) for row in series),
                    default=0.0,
                )
                for variant in VARIANTS
            },
        }
    )


if __name__ == "__main__":
    main()
