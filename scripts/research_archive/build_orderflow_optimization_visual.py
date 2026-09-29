#!/usr/bin/env python3
"""Build the compact inline visualization for an optimization report."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


def number(value: Any) -> float | None:
    if value in (None, "", "None", "null"):
        return None
    return float(value)


def sample_rows(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if len(rows) <= limit:
        return rows
    indexes = sorted({round(index * (len(rows) - 1) / (limit - 1)) for index in range(limit)})
    return [rows[index] for index in indexes]


def build_data(report_path: Path, grid_path: Path, equity_path: Path) -> dict[str, Any]:
    report = json.loads(report_path.read_text(encoding="utf-8"))
    top = report["top_candidates"][:20]

    equity: list[dict[str, Any]] = []
    with equity_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            equity.append(
                {
                    "timestamp": row["timestamp"],
                    "baseline_cumulative_pnl_usdt": number(row["baseline_cumulative_pnl_usdt"]),
                    "best_validation_cumulative_pnl_usdt": number(row["best_validation_cumulative_pnl_usdt"]),
                }
            )

    best = report["best_validation"]["config"]
    fixed = {
        "impulse_window_buckets": int(best["impulse_window_buckets"]),
        "confirmation_buckets": int(best["confirmation_buckets"]),
        "min_intensity": float(best["min_intensity"]),
        "cooldown_buckets": int(best["cooldown_buckets"]),
    }
    cells: list[dict[str, float | None]] = []
    with grid_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if int(row["impulse_window_buckets"]) != fixed["impulse_window_buckets"]:
                continue
            if int(row["confirmation_buckets"]) != fixed["confirmation_buckets"]:
                continue
            if float(row["min_intensity"]) != fixed["min_intensity"]:
                continue
            if int(row["cooldown_buckets"]) != fixed["cooldown_buckets"]:
                continue
            cells.append(
                {
                    "x": float(row["min_return_pct"]),
                    "y": float(row["min_imbalance"]),
                    "value": number(row["validation_mean_net_return_pct"]),
                }
            )
    x_values = sorted({cell["x"] for cell in cells})
    y_values = sorted({cell["y"] for cell in cells})
    return {
        "top_candidates": top,
        "equity": sample_rows(equity, 240),
        "heatmap": {
            "x_values": x_values,
            "y_values": y_values,
            "cells": cells,
            "fixed": fixed,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--grid", type=Path, required=True)
    parser.add_argument("--equity", type=Path, required=True)
    parser.add_argument("--template", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    data = build_data(args.report, args.grid, args.equity)
    template = args.template.read_text(encoding="utf-8")
    rendered = template.replace(
        "__DATA__",
        json.dumps(data, ensure_ascii=False, separators=(",", ":")),
        1,
    )
    if "__DATA__" in rendered:
        raise SystemExit("unexpanded visualization placeholder")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8")
    print(args.output)


if __name__ == "__main__":
    main()
