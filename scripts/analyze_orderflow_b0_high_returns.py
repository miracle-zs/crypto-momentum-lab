"""Profile the high-return tail of the B0 orderflow paper account.

The input is the server-side position/signal/fill join produced by
``export_b0_closed_trades.sql``.  The analysis deliberately separates fields
known at entry from post-entry outcomes, and freezes all univariate cut points
on the first chronological half before evaluating later periods.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
from collections import Counter, defaultdict, deque
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

LOCAL_TZ = timezone(timedelta(hours=8))
TOP_FRACTION = 0.10

RAW_NUMERIC_FIELDS = (
    "duration_minutes",
    "entry_price",
    "exit_price",
    "entry_notional",
    "entry_fee",
    "exit_fee",
    "realized_pnl",
    "return_pct",
    "impulse_return_pct",
    "breakout_distance_pct",
    "baseline_notional",
    "impulse_trade_notional",
    "notional_intensity",
    "impulse_trade_count",
    "aggressive_imbalance",
    "aggressive_buy_notional",
    "aggressive_sell_notional",
    "liquidation_count",
    "liquidation_notional",
    "signal_midpoint",
    "signal_spread",
    "fill_reference_midpoint",
    "fill_spread",
    "fill_cost_bps",
    "fill_price",
    "fill_fee",
)

ENTRY_FEATURES = (
    "impulse_return_abs",
    "breakout_distance_abs",
    "notional_intensity",
    "aggressive_imbalance_abs",
    "directional_flow_share",
    "breakout_fraction_of_impulse",
    "impulse_strength_product",
    "baseline_notional_log10",
    "impulse_trade_notional_log10",
    "impulse_trade_count_log10",
    "notional_per_trade_log10",
    "price_impact_per_million_log10",
    "signal_spread_bps",
    "fill_cost_bps",
    "liquidation_count",
    "liquidation_notional_log10",
    "same_symbol_gap_minutes",
    "same_side_gap_minutes",
    "opposite_side_gap_minutes",
    "prior_market_signals_5m",
    "prior_market_signals_15m",
    "prior_symbol_signals_15m",
    "prior_same_side_symbol_signals_15m",
    "open_symbol_positions",
    "open_same_side_positions",
    "open_opposite_side_positions",
)

DERIVED_OUTPUT_FIELDS = (
    "rank",
    "return_pct_percent",
    "tail_group",
    "opened_at_cn",
    "closed_at_cn",
    "local_date",
    "local_hour",
    "local_session",
    "weekpart",
    *ENTRY_FEATURES,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--history", type=Path, required=True)
    parser.add_argument("--overview", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def parse_dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def finite_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def safe_log10(value: float | None) -> float | None:
    if value is None or value <= 0:
        return None
    return math.log10(value)


def quantile(values: list[float], fraction: float) -> float | None:
    ordered = sorted(value for value in values if math.isfinite(value))
    if not ordered:
        return None
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def fingerprint(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    stat = path.stat()
    return {
        "path": str(path),
        "size": stat.st_size,
        "sha256": digest.hexdigest(),
    }


def local_session(hour: int) -> str:
    if hour < 6:
        return "cn_00_06"
    if hour < 12:
        return "cn_06_12"
    if hour < 18:
        return "cn_12_18"
    return "cn_18_24"


def load_trades(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        source_fields = list(reader.fieldnames or [])
        rows = [dict(row) for row in reader]
    if not rows:
        raise ValueError("input contains no trades")
    for row in rows:
        row["opened_at_dt"] = parse_dt(row["opened_at"])
        row["closed_at_dt"] = parse_dt(row["closed_at"])
        for field in RAW_NUMERIC_FIELDS:
            row[field] = finite_float(row.get(field))
        if row["return_pct"] is None or row["realized_pnl"] is None:
            raise ValueError(f"closed trade lacks return: {row['position_id']}")
        add_static_features(row)
    add_sequence_features(rows)
    rows.sort(
        key=lambda row: (
            -row["return_pct"],
            row["closed_at_dt"],
            row["position_id"],
        )
    )
    top_1_count = max(1, math.ceil(len(rows) * 0.01))
    top_5_count = max(1, math.ceil(len(rows) * 0.05))
    top_10_count = max(1, math.ceil(len(rows) * TOP_FRACTION))
    bottom_10_start = len(rows) - top_10_count + 1
    for rank, row in enumerate(rows, start=1):
        row["rank"] = rank
        row["return_pct_percent"] = row["return_pct"] * 100
        if rank <= top_1_count:
            row["tail_group"] = "top_1pct"
        elif rank <= top_5_count:
            row["tail_group"] = "top_1_to_5pct"
        elif rank <= top_10_count:
            row["tail_group"] = "top_5_to_10pct"
        elif rank >= bottom_10_start:
            row["tail_group"] = "bottom_10pct"
        else:
            row["tail_group"] = "middle_80pct"
    return rows, source_fields


def add_static_features(row: dict[str, Any]) -> None:
    opened_local = row["opened_at_dt"].astimezone(LOCAL_TZ)
    closed_local = row["closed_at_dt"].astimezone(LOCAL_TZ)
    row["opened_at_cn"] = opened_local.isoformat()
    row["closed_at_cn"] = closed_local.isoformat()
    row["local_date"] = opened_local.date().isoformat()
    row["local_hour"] = opened_local.hour
    row["local_session"] = local_session(opened_local.hour)
    row["weekpart"] = "weekend" if opened_local.weekday() >= 5 else "weekday"

    impulse_return = abs(row["impulse_return_pct"] or 0.0)
    breakout = abs(row["breakout_distance_pct"] or 0.0)
    imbalance = abs(row["aggressive_imbalance"] or 0.0)
    notional = row["impulse_trade_notional"] or 0.0
    count = row["impulse_trade_count"] or 0.0
    intensity = row["notional_intensity"] or 0.0
    row["impulse_return_abs"] = impulse_return
    row["breakout_distance_abs"] = breakout
    row["aggressive_imbalance_abs"] = imbalance
    row["directional_flow_share"] = (1.0 + imbalance) / 2.0
    row["breakout_fraction_of_impulse"] = (
        breakout / impulse_return if impulse_return > 0 else None
    )
    row["impulse_strength_product"] = impulse_return * imbalance * intensity
    row["baseline_notional_log10"] = safe_log10(row["baseline_notional"])
    row["impulse_trade_notional_log10"] = safe_log10(notional)
    row["impulse_trade_count_log10"] = safe_log10(count)
    notional_per_trade = notional / count if count > 0 else None
    row["notional_per_trade_log10"] = safe_log10(notional_per_trade)
    impact = impulse_return / notional * 1_000_000 if notional > 0 else None
    row["price_impact_per_million_log10"] = safe_log10(impact)
    midpoint = row["signal_midpoint"]
    spread = row["signal_spread"]
    row["signal_spread_bps"] = (
        spread / midpoint * 10_000
        if spread is not None and midpoint is not None and midpoint > 0
        else None
    )
    row["liquidation_notional_log10"] = safe_log10(row["liquidation_notional"])


def add_sequence_features(rows: list[dict[str, Any]]) -> None:
    chronological = sorted(
        rows, key=lambda row: (row["opened_at_dt"], row["position_id"])
    )
    last_symbol: dict[str, datetime] = {}
    last_symbol_side: dict[tuple[str, str], datetime] = {}
    recent_market: deque[tuple[datetime, str, str]] = deque()
    recent_by_symbol: dict[str, deque[tuple[datetime, str]]] = defaultdict(deque)
    prior_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    cap_minutes = 24 * 60.0
    for row in chronological:
        now = row["opened_at_dt"]
        symbol = row["symbol"]
        side = row["side"]
        opposite = "short" if side == "long" else "long"

        def gap(previous: datetime | None, observed_at: datetime = now) -> float:
            if previous is None:
                return cap_minutes
            return min(
                cap_minutes, (observed_at - previous).total_seconds() / 60
            )

        row["same_symbol_gap_minutes"] = gap(last_symbol.get(symbol))
        row["same_side_gap_minutes"] = gap(last_symbol_side.get((symbol, side)))
        row["opposite_side_gap_minutes"] = gap(
            last_symbol_side.get((symbol, opposite))
        )

        while recent_market and recent_market[0][0] < now - timedelta(minutes=15):
            recent_market.popleft()
        recent_symbol = recent_by_symbol[symbol]
        while recent_symbol and recent_symbol[0][0] < now - timedelta(minutes=15):
            recent_symbol.popleft()
        row["prior_market_signals_5m"] = float(
            sum(item[0] >= now - timedelta(minutes=5) for item in recent_market)
        )
        row["prior_market_signals_15m"] = float(len(recent_market))
        row["prior_symbol_signals_15m"] = float(len(recent_symbol))
        row["prior_same_side_symbol_signals_15m"] = float(
            sum(item_side == side for _, item_side in recent_symbol)
        )
        still_open = [
            prior
            for prior in prior_by_symbol[symbol]
            if prior["closed_at_dt"] > now
        ]
        row["open_symbol_positions"] = float(len(still_open))
        row["open_same_side_positions"] = float(
            sum(prior["side"] == side for prior in still_open)
        )
        row["open_opposite_side_positions"] = float(
            sum(prior["side"] != side for prior in still_open)
        )

        last_symbol[symbol] = now
        last_symbol_side[(symbol, side)] = now
        recent_market.append((now, symbol, side))
        recent_symbol.append((now, side))
        prior_by_symbol[symbol].append(row)


def write_sorted_csv(
    rows: list[dict[str, Any]], source_fields: list[str], output: Path
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    fields = [*DERIVED_OUTPUT_FIELDS, *source_fields]
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def profit_factor(pnls: list[float]) -> float | None:
    gains = sum(value for value in pnls if value > 0)
    losses = -sum(value for value in pnls if value < 0)
    if losses == 0:
        return None
    return gains / losses


def max_drawdown(rows: list[dict[str, Any]]) -> float:
    ordered = sorted(
        rows, key=lambda row: (row["closed_at_dt"], row["position_id"])
    )
    equity = 0.0
    peak = 0.0
    worst = 0.0
    for row in ordered:
        equity += row["realized_pnl"]
        peak = max(peak, equity)
        worst = max(worst, peak - equity)
    return worst


def metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "count": 0,
            "net_pnl": 0.0,
            "mean_return": None,
            "median_return": None,
            "win_rate": None,
            "profit_factor": None,
            "max_drawdown": None,
        }
    pnls = [row["realized_pnl"] for row in rows]
    returns = [row["return_pct"] for row in rows]
    return {
        "count": len(rows),
        "net_pnl": sum(pnls),
        "mean_return": statistics.mean(returns),
        "median_return": statistics.median(returns),
        "win_rate": sum(value > 0 for value in pnls) / len(rows),
        "profit_factor": profit_factor(pnls),
        "max_drawdown": max_drawdown(rows),
    }


def top_ids(rows: list[dict[str, Any]], fraction: float = TOP_FRACTION) -> set[str]:
    ordered = sorted(
        rows,
        key=lambda row: (
            -row["return_pct"],
            row["closed_at_dt"],
            row["position_id"],
        ),
    )
    count = max(1, math.ceil(len(ordered) * fraction))
    return {row["position_id"] for row in ordered[:count]}


def auc(values: list[tuple[float, int]]) -> float | None:
    positives = sum(label for _, label in values)
    negatives = len(values) - positives
    if positives == 0 or negatives == 0:
        return None
    ordered = sorted(values, key=lambda item: item[0])
    rank_sum = 0.0
    cursor = 0
    while cursor < len(ordered):
        end = cursor + 1
        while end < len(ordered) and ordered[end][0] == ordered[cursor][0]:
            end += 1
        average_rank = ((cursor + 1) + end) / 2.0
        rank_sum += average_rank * sum(label for _, label in ordered[cursor:end])
        cursor = end
    return (rank_sum - positives * (positives + 1) / 2) / (
        positives * negatives
    )


def average_ranks(values: list[float]) -> list[float]:
    indexed = sorted(enumerate(values), key=lambda item: item[1])
    result = [0.0] * len(values)
    cursor = 0
    while cursor < len(indexed):
        end = cursor + 1
        while end < len(indexed) and indexed[end][1] == indexed[cursor][1]:
            end += 1
        average = ((cursor + 1) + end) / 2.0
        for original, _ in indexed[cursor:end]:
            result[original] = average
        cursor = end
    return result


def spearman(pairs: list[tuple[float, float]]) -> float | None:
    if len(pairs) < 2:
        return None
    features = [item[0] for item in pairs]
    returns = [item[1] for item in pairs]
    if len(set(features)) < 2 or len(set(returns)) < 2:
        return None
    return statistics.correlation(average_ranks(features), average_ranks(returns))


def chronological_splits(
    rows: list[dict[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    ordered = sorted(
        rows, key=lambda row: (row["opened_at_dt"], row["position_id"])
    )
    first_boundary = ordered[len(ordered) // 2]["opened_at_dt"]
    second_boundary = ordered[len(ordered) * 3 // 4]["opened_at_dt"]
    train = [
        row
        for row in ordered
        if row["opened_at_dt"] < first_boundary
        and row["closed_at_dt"] <= first_boundary
    ]
    validation = [
        row
        for row in ordered
        if first_boundary <= row["opened_at_dt"] < second_boundary
        and row["closed_at_dt"] <= second_boundary
    ]
    holdout = [row for row in ordered if row["opened_at_dt"] >= second_boundary]
    retained = {row["position_id"] for row in train + validation + holdout}
    return (
        {
            "full": ordered,
            "train": train,
            "validation": validation,
            "holdout": holdout,
        },
        {
            "first_boundary": first_boundary.isoformat(),
            "second_boundary": second_boundary.isoformat(),
            "purged_cross_boundary_trades": len(rows) - len(retained),
            "counts": {
                "train": len(train),
                "validation": len(validation),
                "holdout": len(holdout),
            },
        },
    )


def feature_summary(
    rows: list[dict[str, Any]], feature: str
) -> dict[str, Any]:
    high = top_ids(rows)
    values = [
        (row[feature], 1 if row["position_id"] in high else 0, row["return_pct"])
        for row in rows
        if row.get(feature) is not None
    ]
    high_values = [value for value, label, _ in values if label]
    rest_values = [value for value, label, _ in values if not label]
    return {
        "coverage": len(values),
        "top_count": len(high_values),
        "top_median": statistics.median(high_values) if high_values else None,
        "rest_median": statistics.median(rest_values) if rest_values else None,
        "auc_top10": auc([(value, label) for value, label, _ in values]),
        "spearman_return": spearman(
            [(value, return_value) for value, _, return_value in values]
        ),
    }


def audit_features(
    splits: dict[str, list[dict[str, Any]]]
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for feature in ENTRY_FEATURES:
        row: dict[str, Any] = {"feature": feature}
        for split_name, split_rows in splits.items():
            summary = feature_summary(split_rows, feature)
            for key, value in summary.items():
                row[f"{split_name}_{key}"] = value
        output.append(row)
    output.sort(
        key=lambda row: abs((row.get("full_auc_top10") or 0.5) - 0.5),
        reverse=True,
    )
    return output


def write_csv(rows: list[dict[str, Any]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({key for row in rows for key in row})
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def evaluate_rule(
    rows: list[dict[str, Any]],
    *,
    feature: str,
    operation: str,
    threshold: float,
) -> dict[str, Any]:
    eligible = [row for row in rows if row.get(feature) is not None]
    high = top_ids(eligible)
    if operation == "ge":
        accepted = [row for row in eligible if row[feature] >= threshold]
    else:
        accepted = [row for row in eligible if row[feature] <= threshold]
    accepted_high = sum(row["position_id"] in high for row in accepted)
    base_rate = len(high) / len(eligible) if eligible else None
    accepted_rate = accepted_high / len(accepted) if accepted else None
    row_metrics = metrics(accepted)
    row_metrics.update(
        {
            "eligible_count": len(eligible),
            "retention": len(accepted) / len(eligible) if eligible else None,
            "top10_rate": accepted_rate,
            "top10_lift": (
                accepted_rate / base_rate
                if accepted_rate is not None and base_rate
                else None
            ),
        }
    )
    return row_metrics


def frozen_univariate_rules(
    splits: dict[str, list[dict[str, Any]]], feature_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    train = splits["train"]
    output: list[dict[str, Any]] = []
    by_feature = {row["feature"]: row for row in feature_rows}
    for feature in ENTRY_FEATURES:
        values = [row[feature] for row in train if row.get(feature) is not None]
        if len(values) < 80 or min(values) == max(values):
            continue
        train_auc = by_feature[feature].get("train_auc_top10")
        if train_auc is None:
            continue
        operation = "ge" if train_auc >= 0.5 else "le"
        threshold = quantile(values, 0.8 if operation == "ge" else 0.2)
        if threshold is None:
            continue
        result: dict[str, Any] = {
            "feature": feature,
            "operation": operation,
            "threshold": threshold,
            "selection_note": (
                "direction selected on train top-decile AUC; threshold is the "
                "pre-specified train outer quintile"
            ),
        }
        for split_name, split_rows in splits.items():
            evaluated = evaluate_rule(
                split_rows,
                feature=feature,
                operation=operation,
                threshold=threshold,
            )
            for key, value in evaluated.items():
                result[f"{split_name}_{key}"] = value
        output.append(result)
    output.sort(
        key=lambda row: min(
            row.get("validation_top10_lift") or 0.0,
            row.get("holdout_top10_lift") or 0.0,
        ),
        reverse=True,
    )
    return output


def category_rows(
    rows: list[dict[str, Any]], category: str
) -> list[dict[str, Any]]:
    high = top_ids(rows)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row[category])].append(row)
    output = []
    base_rate = len(high) / len(rows)
    for value, members in grouped.items():
        high_count = sum(row["position_id"] in high for row in members)
        values = metrics(members)
        values.update(
            {
                "category": category,
                "value": value,
                "high_count": high_count,
                "high_rate": high_count / len(members),
                "high_lift": (high_count / len(members)) / base_rate,
            }
        )
        output.append(values)
    return sorted(output, key=lambda row: row["net_pnl"], reverse=True)


def tail_and_concentration(rows: list[dict[str, Any]]) -> dict[str, Any]:
    positive = [row for row in rows if row["realized_pnl"] > 0]
    positive_sum = sum(row["realized_pnl"] for row in positive)
    total = sum(row["realized_pnl"] for row in rows)
    deheaded = {}
    for count in (1, 2, 5, 10, 22, 110, 220):
        removed = rows[:count]
        deheaded[str(count)] = {
            "net_pnl_after_removal": total
            - sum(row["realized_pnl"] for row in removed),
            "removed_pnl": sum(row["realized_pnl"] for row in removed),
        }

    top_1_count = max(1, math.ceil(len(rows) * 0.01))
    top_1 = rows[:top_1_count]
    top_count = max(1, math.ceil(len(rows) * TOP_FRACTION))
    top = rows[:top_count]
    symbol_pnl: dict[str, float] = defaultdict(float)
    symbol_counts: Counter[str] = Counter()
    for row in rows:
        symbol_pnl[row["symbol"]] += row["realized_pnl"]
        symbol_counts[row["symbol"]] += 1
    top_symbol = max(symbol_pnl, key=symbol_pnl.get)
    top_symbols = Counter(row["symbol"] for row in top)
    episodes = Counter(
        (row["symbol"], row["side"], row["closed_at"]) for row in top
    )
    top_1_episodes = Counter(
        (row["symbol"], row["side"], row["closed_at"]) for row in top_1
    )
    all_episode_rows: list[dict[str, Any]] = []
    all_episode_groups: dict[
        tuple[str, str, str], list[dict[str, Any]]
    ] = defaultdict(list)
    for row in rows:
        all_episode_groups[(row["symbol"], row["side"], row["closed_at"])].append(
            row
        )
    for episode, members in all_episode_groups.items():
        all_episode_rows.append(
            {
                "episode": episode,
                "side": episode[1],
                "maximum_member_return": max(
                    member["return_pct"] for member in members
                ),
            }
        )
    all_episode_rows.sort(
        key=lambda item: (-item["maximum_member_return"], item["episode"])
    )
    episode_top_1_count = max(1, math.ceil(len(all_episode_rows) * 0.01))
    episode_top_1 = all_episode_rows[:episode_top_1_count]
    chronological = sorted(
        rows, key=lambda row: (row["opened_at_dt"], row["position_id"])
    )
    midpoint = chronological[len(chronological) // 2]["opened_at_dt"]
    top_1_first = [row for row in top_1 if row["opened_at_dt"] < midpoint]
    top_1_second = [row for row in top_1 if row["opened_at_dt"] >= midpoint]
    positive_symbol_pnl: dict[str, float] = defaultdict(float)
    for row in positive:
        positive_symbol_pnl[row["symbol"]] += row["realized_pnl"]
    hhi = (
        sum((value / positive_sum) ** 2 for value in positive_symbol_pnl.values())
        if positive_sum > 0
        else None
    )
    return {
        "total_net_pnl": total,
        "gross_positive_pnl": positive_sum,
        "gross_negative_pnl": sum(
            row["realized_pnl"] for row in rows if row["realized_pnl"] < 0
        ),
        "top_1_trade": {
            "position_id": rows[0]["position_id"],
            "symbol": rows[0]["symbol"],
            "side": rows[0]["side"],
            "return_pct": rows[0]["return_pct"],
            "realized_pnl": rows[0]["realized_pnl"],
        },
        "top_1pct": {
            "count": len(top_1),
            "cutoff_return_pct": top_1[-1]["return_pct"],
            "pnl": sum(row["realized_pnl"] for row in top_1),
            "long_count": sum(row["side"] == "long" for row in top_1),
            "long_share": sum(row["side"] == "long" for row in top_1)
            / len(top_1),
            "unique_symbols": len({row["symbol"] for row in top_1}),
            "unique_exit_episodes": len(top_1_episodes),
            "long_exit_episode_count": sum(
                episode[1] == "long" for episode in top_1_episodes
            ),
            "first_half_count": len(top_1_first),
            "first_half_long_count": sum(
                row["side"] == "long" for row in top_1_first
            ),
            "second_half_count": len(top_1_second),
            "second_half_long_count": sum(
                row["side"] == "long" for row in top_1_second
            ),
        },
        "episode_ranked_top_1pct": {
            "episode_count": len(all_episode_rows),
            "top_count": len(episode_top_1),
            "cutoff_maximum_member_return": episode_top_1[-1][
                "maximum_member_return"
            ],
            "long_count": sum(item["side"] == "long" for item in episode_top_1),
            "top_10_long_count": sum(
                item["side"] == "long" for item in episode_top_1[:10]
            ),
        },
        "top_10pct": {
            "count": len(top),
            "cutoff_return_pct": top[-1]["return_pct"],
            "pnl": sum(row["realized_pnl"] for row in top),
            "share_of_gross_positive_pnl": (
                sum(row["realized_pnl"] for row in top) / positive_sum
                if positive_sum > 0
                else None
            ),
            "long_share": sum(row["side"] == "long" for row in top) / len(top),
            "unique_symbols": len(top_symbols),
            "unique_exit_episodes": len(episodes),
            "largest_exit_episode_count": max(episodes.values()),
            "top_symbols_by_count": top_symbols.most_common(12),
        },
        "positive_pnl_symbol_hhi": hhi,
        "deheaded": deheaded,
        "best_net_pnl_symbol": {
            "symbol": top_symbol,
            "net_pnl": symbol_pnl[top_symbol],
            "trade_count": symbol_counts[top_symbol],
            "portfolio_net_after_removal": total - symbol_pnl[top_symbol],
        },
        "top_symbols_by_net_pnl": sorted(
            (
                {
                    "symbol": symbol,
                    "net_pnl": pnl,
                    "trade_count": symbol_counts[symbol],
                }
                for symbol, pnl in symbol_pnl.items()
            ),
            key=lambda item: item["net_pnl"],
            reverse=True,
        )[:15],
    }


def exploratory_composite(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Evaluate one post-hoc composite suggested by the descriptive audit."""

    splits, _ = chronological_splits(rows)
    full_top_10 = top_ids(rows, 0.10)
    full_top_1 = top_ids(rows, 0.01)
    output: dict[str, Any] = {
        "rule": "side=long AND local_session!=cn_00_06",
        "status": "post_hoc_exploratory_not_preregistered",
        "warning": "must be evaluated in a new forward sample before deployment",
    }
    for split_name, split_rows in splits.items():
        accepted = [
            row
            for row in split_rows
            if row["side"] == "long" and row["local_session"] != "cn_00_06"
        ]
        values = metrics(accepted)
        values["retention"] = len(accepted) / len(split_rows)
        if split_name == "full":
            values["top10_recall"] = (
                sum(row["position_id"] in full_top_10 for row in accepted)
                / len(full_top_10)
            )
            values["top1_recall"] = (
                sum(row["position_id"] in full_top_1 for row in accepted)
                / len(full_top_1)
            )
            ranked = sorted(
                accepted,
                key=lambda row: (
                    -row["return_pct"],
                    row["closed_at_dt"],
                    row["position_id"],
                ),
            )
            values["deheaded"] = {
                str(count): sum(row["realized_pnl"] for row in ranked[count:])
                for count in (1, 5, 10)
            }
            best_symbol = max(
                {row["symbol"] for row in accepted},
                key=lambda symbol: sum(
                    row["realized_pnl"]
                    for row in accepted
                    if row["symbol"] == symbol
                ),
            )
            values["best_symbol"] = best_symbol
            values["net_pnl_without_best_symbol"] = sum(
                row["realized_pnl"]
                for row in accepted
                if row["symbol"] != best_symbol
            )
        output[split_name] = values
    return output


def duration_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    high = top_ids(rows)
    top = [row["duration_minutes"] for row in rows if row["position_id"] in high]
    rest = [row["duration_minutes"] for row in rows if row["position_id"] not in high]
    winning = [row["duration_minutes"] for row in rows if row["realized_pnl"] > 0]
    losing = [row["duration_minutes"] for row in rows if row["realized_pnl"] <= 0]
    return {
        "warning": "duration and exit reason are post-entry outcomes, not filters",
        "top10_median_minutes": statistics.median(top),
        "rest_median_minutes": statistics.median(rest),
        "winning_median_minutes": statistics.median(winning),
        "losing_median_minutes": statistics.median(losing),
        "top10_duration_q25": quantile(top, 0.25),
        "top10_duration_q75": quantile(top, 0.75),
        "auc_duration_for_top10": auc(
            [
                (row["duration_minutes"], 1 if row["position_id"] in high else 0)
                for row in rows
            ]
        ),
    }


def main() -> None:
    args = parse_args()
    rows, source_fields = load_trades(args.input)
    history = json.loads(args.history.read_text(encoding="utf-8"))
    overview = json.loads(args.overview.read_text(encoding="utf-8"))
    history_ids = {row["position_id"] for row in history["closed_trades"]}
    input_ids = {row["position_id"] for row in rows}
    if len(rows) != len(input_ids):
        raise ValueError("duplicate position_id in joined input")
    if history_ids != input_ids:
        raise ValueError(
            "joined input does not match frozen API history: "
            f"api_only={len(history_ids - input_ids)} "
            f"input_only={len(input_ids - history_ids)}"
        )

    output_dir = args.output_dir
    analysis_dir = output_dir / "analysis"
    sorted_path = analysis_dir / "trades_sorted_by_net_return.csv"
    feature_path = analysis_dir / "entry_feature_comparison.csv"
    rule_path = analysis_dir / "frozen_univariate_rule_checks.csv"
    category_path = analysis_dir / "category_comparison.csv"
    summary_path = analysis_dir / "summary.json"
    manifest_path = output_dir / "manifest.json"

    write_sorted_csv(rows, source_fields, sorted_path)
    splits, split_meta = chronological_splits(rows)
    feature_rows = audit_features(splits)
    write_csv(feature_rows, feature_path)
    rules = frozen_univariate_rules(splits, feature_rows)
    write_csv(rules, rule_path)

    category_output = []
    for split_name, split_rows in splits.items():
        for category in ("side", "local_session", "weekpart", "local_date"):
            for category_row in category_rows(split_rows, category):
                category_output.append({"split": split_name, **category_row})
    write_csv(category_output, category_path)

    top_count = max(1, math.ceil(len(rows) * TOP_FRACTION))
    summary = {
        "snapshot_at_utc": overview["generated_at"],
        "run_id": rows[0]["run_id"],
        "sample": {
            "closed_trade_count": len(rows),
            "first_opened_at": min(row["opened_at_dt"] for row in rows).isoformat(),
            "last_closed_at": max(row["closed_at_dt"] for row in rows).isoformat(),
            "api_history_count": history["closed_trade_count"],
            "api_db_position_id_exact_match": history_ids == input_ids,
        },
        "definition": {
            "return": "realized_pnl / entry_notional; includes entry and exit fees",
            "high_return": (
                f"top {TOP_FRACTION:.0%} by net return, exactly {top_count} trades"
            ),
            "entry_feature_boundary": (
                "signal/fill fields and sequence state observable no later than entry"
            ),
            "post_entry_fields": ["duration_minutes", "close_reason", "exit_price"],
        },
        "all_metrics": metrics(rows),
        "tail_and_concentration": tail_and_concentration(rows),
        "duration": duration_summary(rows),
        "exploratory_composite": exploratory_composite(rows),
        "split": split_meta,
        "split_metrics": {
            name: metrics(split_rows) for name, split_rows in splits.items()
        },
        "side_metrics": {
            name: {
                side: metrics([row for row in split_rows if row["side"] == side])
                for side in ("long", "short")
            }
            for name, split_rows in splits.items()
        },
        "strongest_full_sample_entry_associations": feature_rows[:10],
        "frozen_rule_checks_ranked_by_later_stability": rules[:10],
        "limitations": [
            (
                "Only 12 calendar days are observed; validation and holdout are "
                "temporal stability checks, not independent live evidence."
            ),
            (
                "The strategy can open several positions in one symbol/trend, "
                "so rows are not fully independent."
            ),
            (
                "Univariate rule direction is selected on train across many "
                "features; later checks reduce but do not eliminate "
                "multiple-testing risk."
            ),
            "No post-entry duration or exit-path field is used as an entry rule.",
        ],
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    manifest = {
        "generated_at": datetime.now(UTC).isoformat(),
        "command": (
            "python scripts/analyze_orderflow_b0_high_returns.py "
            f"--input {args.input} --history {args.history} "
            f"--overview {args.overview} --output-dir {args.output_dir}"
        ),
        "inputs": [
            fingerprint(args.input),
            fingerprint(args.history),
            fingerprint(args.overview),
        ],
        "outputs": [
            fingerprint(sorted_path),
            fingerprint(feature_path),
            fingerprint(rule_path),
            fingerprint(category_path),
            fingerprint(summary_path),
        ],
    }
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "closed_trades": len(rows),
                "net_pnl": summary["all_metrics"]["net_pnl"],
                "top_10pct_cutoff": summary["tail_and_concentration"]["top_10pct"][
                    "cutoff_return_pct"
                ],
                "output": str(summary_path),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
