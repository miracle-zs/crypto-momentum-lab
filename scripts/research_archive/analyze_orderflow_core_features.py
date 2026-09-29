from __future__ import annotations

import argparse
import json
import statistics
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from analyze_orderflow_15m_false_signals import (
    Trade,
    add_signal_features,
    file_fingerprint,
    load_trades,
    metrics,
    quantile,
    split_trades,
)

CORE_FEATURES = (
    "impulse_return_abs",
    "notional_intensity",
    "breakout_distance_abs",
    "aggressive_imbalance_abs",
    "impulse_trade_notional_log10",
    "impulse_trade_count_log10",
    "notional_per_trade_log10",
    "price_impact_per_million_log10",
)

SCOPES = ("all", "long", "short")
QUANTILE_FRACTIONS = (0.2, 0.4, 0.6, 0.7, 0.8, 0.9)


def scoped(trades: list[Trade], scope: str) -> list[Trade]:
    if scope == "all":
        return trades
    return [trade for trade in trades if trade.side == scope]


def average_rank(values: list[float]) -> list[float]:
    indexed = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    cursor = 0
    while cursor < len(indexed):
        end = cursor + 1
        while end < len(indexed) and indexed[end][1] == indexed[cursor][1]:
            end += 1
        rank = (cursor + 1 + end) / 2.0
        for original_index, _ in indexed[cursor:end]:
            ranks[original_index] = rank
        cursor = end
    return ranks


def spearman(trades: list[Trade], feature: str) -> float | None:
    pairs = [
        (trade.features[feature], trade.pnl)
        for trade in trades
        if feature in trade.features and trade.pnl is not None
    ]
    if len(pairs) < 2:
        return None
    values = [pair[0] for pair in pairs]
    pnls = [pair[1] for pair in pairs]
    if len(set(values)) < 2 or len(set(pnls)) < 2:
        return None
    return statistics.correlation(average_rank(values), average_rank(pnls))


def feature_metrics(
    trades: list[Trade],
    feature: str,
    predicate: Callable[[float], bool],
) -> dict[str, Any]:
    eligible = [trade for trade in trades if feature in trade.features]
    accepted = [
        trade for trade in eligible if predicate(trade.features[feature])
    ]
    result = metrics(accepted)
    result["eligible_count"] = len(eligible)
    result["retention"] = len(accepted) / len(eligible) if eligible else None
    return result


def evaluate_filter(
    splits: dict[str, list[Trade]],
    feature: str,
    predicate: Callable[[float], bool],
) -> dict[str, Any]:
    return {
        split_name: {
            scope: feature_metrics(scoped(rows, scope), feature, predicate)
            for scope in SCOPES
        }
        for split_name, rows in splits.items()
    }


def baseline_metrics(
    splits: dict[str, list[Trade]],
    feature: str,
) -> dict[str, Any]:
    return evaluate_filter(splits, feature, lambda _value: True)


def quintile_filters(cuts: dict[float, float]) -> list[dict[str, Any]]:
    q20 = cuts[0.2]
    q40 = cuts[0.4]
    q60 = cuts[0.6]
    q80 = cuts[0.8]
    return [
        {
            "label": "Q1",
            "lower": None,
            "upper": q20,
            "predicate": lambda value, upper=q20: value <= upper,
        },
        {
            "label": "Q2",
            "lower": q20,
            "upper": q40,
            "predicate": lambda value, lower=q20, upper=q40: (
                lower < value <= upper
            ),
        },
        {
            "label": "Q3",
            "lower": q40,
            "upper": q60,
            "predicate": lambda value, lower=q40, upper=q60: (
                lower < value <= upper
            ),
        },
        {
            "label": "Q4",
            "lower": q60,
            "upper": q80,
            "predicate": lambda value, lower=q60, upper=q80: (
                lower < value <= upper
            ),
        },
        {
            "label": "Q5",
            "lower": q80,
            "upper": None,
            "predicate": lambda value, lower=q80: value > lower,
        },
    ]


def audit_feature(
    feature: str,
    splits: dict[str, list[Trade]],
) -> dict[str, Any]:
    train_values = [
        trade.features[feature]
        for trade in splits["train"]
        if feature in trade.features
    ]
    if len(train_values) < 80:
        raise ValueError(f"insufficient train coverage for {feature}")
    cuts = {
        fraction: quantile(train_values, fraction)
        for fraction in QUANTILE_FRACTIONS
    }
    filters: dict[str, Any] = {}
    for fraction in (0.6, 0.7, 0.8, 0.9):
        threshold = cuts[fraction]
        filters[f"at_most_q{int(fraction * 100)}"] = {
            "threshold": threshold,
            "results": evaluate_filter(
                splits,
                feature,
                lambda value, upper=threshold: value <= upper,
            ),
        }
    filters["middle_q20_q80"] = {
        "lower": cuts[0.2],
        "upper": cuts[0.8],
        "results": evaluate_filter(
            splits,
            feature,
            lambda value, lower=cuts[0.2], upper=cuts[0.8]: (
                lower <= value <= upper
            ),
        ),
    }
    filters["above_q80"] = {
        "threshold": cuts[0.8],
        "results": evaluate_filter(
            splits,
            feature,
            lambda value, lower=cuts[0.8]: value > lower,
        ),
    }
    quintiles = []
    for definition in quintile_filters(cuts):
        quintiles.append(
            {
                "label": definition["label"],
                "lower": definition["lower"],
                "upper": definition["upper"],
                "results": evaluate_filter(
                    splits,
                    feature,
                    definition["predicate"],
                ),
            }
        )
    return {
        "train_quantiles": {
            f"q{int(fraction * 100)}": value
            for fraction, value in cuts.items()
        },
        "baselines": baseline_metrics(splits, feature),
        "spearman": {
            split_name: {
                scope: spearman(scoped(rows, scope), feature)
                for scope in SCOPES
            }
            for split_name, rows in splits.items()
        },
        "quintiles": quintiles,
        "filters": filters,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Focused local audit of orderflow signal features."
    )
    parser.add_argument(
        "--positions",
        type=Path,
        default=Path("/tmp/paper_positions-complete.csv"),
    )
    parser.add_argument(
        "--signals",
        type=Path,
        default=Path("/tmp/strategy_signals-complete.csv"),
    )
    parser.add_argument(
        "--fills",
        type=Path,
        default=Path("/tmp/paper_fills-complete.csv"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(
            "data/server-paper-accounts-20260807T160821Z/analysis/"
            "orderflow_core_feature_audit_20260809.json"
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    trades = load_trades(args.positions, args.signals, args.fills)
    for trade in trades:
        add_signal_features(trade)
    splits, split_meta = split_trades(trades)
    output = {
        "generated_at": datetime.now(UTC).isoformat(),
        "method": {
            "thresholds": (
                "all cut points are learned from the first-half train segment"
            ),
            "split": (
                "50% train / 25% validation / 25% holdout by entry time; "
                "labels crossing the first two boundaries are purged"
            ),
            "scope": (
                "the same all-direction train thresholds are evaluated on all, "
                "long, and short cohorts"
            ),
            "warning": (
                "the six-day snapshot has been inspected before; validation and "
                "holdout are stability checks, not untouched future evidence"
            ),
        },
        "inputs": [
            file_fingerprint(args.positions),
            file_fingerprint(args.signals),
            file_fingerprint(args.fills),
        ],
        "counts": {
            "all_positions": len(trades),
            "closed_positions": len(splits["full"]),
            "open_positions": len(trades) - len(splits["full"]),
        },
        "split": split_meta,
        "features": {
            feature: audit_feature(feature, splits)
            for feature in CORE_FEATURES
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(output, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(output, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
