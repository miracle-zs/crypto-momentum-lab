from __future__ import annotations

import argparse
import bisect
import csv
import hashlib
import json
import math
import random
import statistics
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

TARGET_RUN = "paper-account-05-orderflow-candle15m-v1"
LOCAL_TZ = timezone(timedelta(hours=8))
GAP_CAP_MINUTES = 24 * 60.0


@dataclass(frozen=True)
class Bar:
    end: datetime
    high: float
    low: float
    close: float


@dataclass
class Trade:
    position_id: str
    signal_id: str
    symbol: str
    side: str
    opened_at: datetime
    closed_at: datetime | None
    entry_price: float
    pnl: float | None
    features: dict[str, float]
    categories: dict[str, str]


def parse_dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def safe_log10(value: float | None) -> float | None:
    if value is None or value <= 0:
        return None
    return math.log10(value)


def file_fingerprint(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    stat = path.stat()
    return {
        "path": str(path),
        "size": stat.st_size,
        "mtime": datetime.fromtimestamp(stat.st_mtime, tz=UTC).isoformat(),
        "sha256": digest.hexdigest(),
    }


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def numeric_features(payload: str) -> dict[str, float]:
    raw = json.loads(payload)
    output: dict[str, float] = {}
    for key, value in raw.items():
        number = as_float(value)
        if number is not None:
            output[key] = number
    return output


def load_spreads(path: Path) -> dict[tuple[str, datetime], float]:
    grouped: dict[tuple[str, datetime], list[float]] = defaultdict(list)
    for row in read_csv(path):
        if row.get("run_id") != TARGET_RUN or row.get("status") != "filled":
            continue
        spread = as_float(row.get("spread"))
        if spread is None:
            continue
        grouped[(row["symbol"], parse_dt(row["filled_at"]))].append(spread)
    return {key: statistics.median(values) for key, values in grouped.items()}


def load_trades(
    positions_path: Path,
    signals_path: Path,
    fills_path: Path,
) -> list[Trade]:
    signals = {
        row["signal_id"]: numeric_features(row["features"])
        for row in read_csv(signals_path)
        if row.get("run_id") == TARGET_RUN
    }
    spreads = load_spreads(fills_path)
    trades: list[Trade] = []
    for row in read_csv(positions_path):
        if row.get("run_id") != TARGET_RUN:
            continue
        opened_at = parse_dt(row["opened_at"])
        closed_at = parse_dt(row["closed_at"]) if row.get("closed_at") else None
        pnl = as_float(row.get("realized_pnl")) if closed_at else None
        entry_price = float(row["entry_price"])
        features = dict(signals[row["signal_id"]])
        spread = spreads.get((row["symbol"], opened_at))
        if spread is not None and entry_price > 0:
            features["entry_spread_bps"] = spread / entry_price * 10_000
        trades.append(
            Trade(
                position_id=row["position_id"],
                signal_id=row["signal_id"],
                symbol=row["symbol"],
                side=row["side"],
                opened_at=opened_at,
                closed_at=closed_at,
                entry_price=entry_price,
                pnl=pnl,
                features=features,
                categories={},
            )
        )
    trades.sort(key=lambda trade: (trade.opened_at, trade.position_id))
    return trades


def add_signal_features(trade: Trade) -> None:
    values = trade.features
    impulse_return = abs(values.get("impulse_return_pct", 0.0))
    imbalance = abs(values.get("aggressive_imbalance", 0.0))
    breakout = abs(values.get("breakout_distance_pct", 0.0))
    notional = values.get("impulse_trade_notional", 0.0)
    trade_count = values.get("impulse_trade_count", 0.0)
    baseline = values.get("baseline_notional", 0.0)
    intensity = values.get("notional_intensity", 0.0)

    values["impulse_return_abs"] = impulse_return
    values["aggressive_imbalance_abs"] = imbalance
    values["breakout_distance_abs"] = breakout
    values["directional_flow_share"] = (1.0 + imbalance) / 2.0
    if impulse_return > 0:
        values["breakout_fraction_of_impulse"] = breakout / impulse_return
    if trade_count > 0:
        values["notional_per_trade"] = notional / trade_count
    if notional > 0:
        values["price_impact_per_million"] = impulse_return / notional * 1_000_000
    values["impulse_strength_product"] = impulse_return * imbalance * intensity

    log_values = {
        "baseline_notional_log10": safe_log10(baseline),
        "impulse_trade_notional_log10": safe_log10(notional),
        "impulse_trade_count_log10": safe_log10(trade_count),
        "notional_per_trade_log10": safe_log10(values.get("notional_per_trade")),
        "price_impact_per_million_log10": safe_log10(
            values.get("price_impact_per_million")
        ),
    }
    values.update(
        {key: value for key, value in log_values.items() if value is not None}
    )

    local = trade.opened_at.astimezone(LOCAL_TZ)
    trade.categories["side"] = trade.side
    trade.categories["utc_session"] = f"utc_{trade.opened_at.hour // 8 * 8:02d}"
    trade.categories["local_session"] = f"cn_{local.hour // 6 * 6:02d}"
    trade.categories["weekpart"] = "weekend" if local.weekday() >= 5 else "weekday"
    values["local_hour"] = float(local.hour)


def add_sequence_features(trades: list[Trade]) -> None:
    last_symbol: dict[str, datetime] = {}
    last_symbol_side: dict[tuple[str, str], datetime] = {}
    recent_market: deque[tuple[datetime, str]] = deque()
    recent_symbol: dict[str, deque[tuple[datetime, str]]] = defaultdict(deque)
    prior_by_symbol: dict[str, list[Trade]] = defaultdict(list)

    for trade in trades:
        now = trade.opened_at
        symbol = trade.symbol
        side = trade.side
        opposite = "short" if side == "long" else "long"
        previous_symbol = last_symbol.get(symbol)
        previous_same = last_symbol_side.get((symbol, side))
        previous_opposite = last_symbol_side.get((symbol, opposite))

        trade.features["same_symbol_gap_minutes"] = min(
            GAP_CAP_MINUTES,
            (now - previous_symbol).total_seconds() / 60
            if previous_symbol
            else GAP_CAP_MINUTES,
        )
        trade.features["same_side_gap_minutes"] = min(
            GAP_CAP_MINUTES,
            (now - previous_same).total_seconds() / 60
            if previous_same
            else GAP_CAP_MINUTES,
        )
        trade.features["opposite_side_gap_minutes"] = min(
            GAP_CAP_MINUTES,
            (now - previous_opposite).total_seconds() / 60
            if previous_opposite
            else GAP_CAP_MINUTES,
        )

        while recent_market and recent_market[0][0] < now - timedelta(minutes=15):
            recent_market.popleft()
        symbol_queue = recent_symbol[symbol]
        while symbol_queue and symbol_queue[0][0] < now - timedelta(minutes=15):
            symbol_queue.popleft()

        market_5m = [
            item for item in recent_market if item[0] >= now - timedelta(minutes=5)
        ]
        same_market_5m = sum(item_side == side for _, item_side in market_5m)
        opposite_market_5m = len(market_5m) - same_market_5m
        trade.features["prior_market_signals_5m"] = float(len(market_5m))
        trade.features["prior_market_signals_15m"] = float(len(recent_market))
        trade.features["prior_same_direction_market_5m"] = float(same_market_5m)
        trade.features["prior_opposite_direction_market_5m"] = float(opposite_market_5m)
        if market_5m:
            trade.features["market_direction_breadth_5m"] = (
                same_market_5m - opposite_market_5m
            ) / len(market_5m)
        else:
            trade.features["market_direction_breadth_5m"] = 0.0

        trade.features["prior_symbol_signals_15m"] = float(len(symbol_queue))
        trade.features["prior_same_side_symbol_signals_15m"] = float(
            sum(item_side == side for _, item_side in symbol_queue)
        )
        trade.features["prior_opposite_symbol_signals_15m"] = float(
            sum(item_side != side for _, item_side in symbol_queue)
        )

        earlier = prior_by_symbol[symbol]
        open_symbol = [
            item for item in earlier if item.closed_at is None or item.closed_at > now
        ]
        trade.features["open_symbol_positions"] = float(len(open_symbol))
        trade.features["open_same_side_positions"] = float(
            sum(item.side == side for item in open_symbol)
        )
        trade.features["open_opposite_side_positions"] = float(
            sum(item.side != side for item in open_symbol)
        )

        last_symbol[symbol] = now
        last_symbol_side[(symbol, side)] = now
        recent_market.append((now, side))
        symbol_queue.append((now, side))
        earlier.append(trade)


def load_bars(path: Path) -> tuple[list[datetime], list[Bar]]:
    rows = json.loads(path.read_text(encoding="utf-8"))
    bars = [
        Bar(
            end=parse_dt(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
        )
        for row in rows
        if isinstance(row, list) and len(row) >= 5
    ]
    bars.sort(key=lambda bar: bar.end)
    return [bar.end for bar in bars], bars


def add_bar_features(trades: list[Trade], bars_dir: Path) -> dict[str, Any]:
    cache: dict[str, tuple[list[datetime], list[Bar]] | None] = {}
    available = 0
    stale = 0
    for trade in trades:
        if trade.symbol not in cache:
            path = bars_dir / f"{trade.symbol}.json"
            cache[trade.symbol] = load_bars(path) if path.exists() else None
        loaded = cache[trade.symbol]
        if loaded is None:
            continue
        ends, bars = loaded
        index = bisect.bisect_right(ends, trade.opened_at)
        completed = bars[:index]
        if not completed:
            continue
        bar_age_minutes = (trade.opened_at - completed[-1].end).total_seconds() / 60
        trade.features["last_completed_bar_age_minutes"] = bar_age_minutes
        if bar_age_minutes > 5.0:
            stale += 1
            continue
        available += 1
        direction = 1.0 if trade.side == "long" else -1.0
        last_close = completed[-1].close
        if last_close > 0:
            trade.features["entry_extension_from_last_5m_close"] = direction * (
                trade.entry_price / last_close - 1.0
            )
        for bars_count, label in ((1, "5m"), (3, "15m"), (6, "30m"), (12, "60m")):
            if len(completed) >= bars_count + 1:
                previous = completed[-bars_count - 1].close
                if previous > 0:
                    raw_return = last_close / previous - 1.0
                    trade.features[f"past_return_{label}"] = raw_return
                    trade.features[f"past_return_align_{label}"] = (
                        direction * raw_return
                    )
            if len(completed) < bars_count:
                continue
            window = completed[-bars_count:]
            high = max(bar.high for bar in window)
            low = min(bar.low for bar in window)
            if last_close > 0:
                trade.features[f"range_{label}"] = (high - low) / last_close
            changes = [
                abs(window[index].close - window[index - 1].close)
                for index in range(1, len(window))
            ]
            movement = window[-1].close - window[0].close
            denominator = sum(changes)
            if denominator > 0:
                trade.features[f"trend_efficiency_align_{label}"] = (
                    direction * movement / denominator
                )
            if high > low:
                location = (trade.entry_price - low) / (high - low)
                trade.features[f"directional_range_location_{label}"] = (
                    location if trade.side == "long" else 1.0 - location
                )
    return {
        "trade_rows_with_fresh_bar": available,
        "trade_rows_with_stale_last_bar": stale,
        "trade_rows_total": len(trades),
        "symbols_loaded": sum(value is not None for value in cache.values()),
        "symbols_total": len(cache),
    }


def profit_factor(pnls: list[float]) -> float | None:
    gains = sum(value for value in pnls if value > 0)
    losses = -sum(value for value in pnls if value < 0)
    if losses == 0:
        return None
    return gains / losses


def metrics(trades: list[Trade]) -> dict[str, Any]:
    pnls = [trade.pnl for trade in trades if trade.pnl is not None]
    if not pnls:
        return {
            "count": 0,
            "net_pnl": 0.0,
            "profit_factor": None,
            "win_rate": None,
        }
    running = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in pnls:
        running += value
        peak = max(peak, running)
        max_drawdown = min(max_drawdown, running - peak)
    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value < 0]
    average_win = statistics.mean(wins) if wins else None
    average_loss = statistics.mean(losses) if losses else None
    return {
        "count": len(pnls),
        "net_pnl": sum(pnls),
        "profit_factor": profit_factor(pnls),
        "win_rate": len(wins) / len(pnls),
        "mean_pnl": statistics.mean(pnls),
        "median_pnl": statistics.median(pnls),
        "average_win": average_win,
        "average_loss": average_loss,
        "payoff_ratio": (
            average_win / abs(average_loss)
            if average_win is not None and average_loss not in (None, 0)
            else None
        ),
        "max_sequence_drawdown": max_drawdown,
    }


def quantile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def rule_description(rule: list[dict[str, Any]]) -> str:
    parts = []
    for predicate in rule:
        feature = predicate["feature"]
        operation = predicate["op"]
        if operation == "eq":
            parts.append(f"{feature}={predicate['value']}")
        elif operation == "le":
            parts.append(f"{feature}<={predicate['value']:.8g}")
        elif operation == "ge":
            parts.append(f"{feature}>={predicate['value']:.8g}")
        else:
            parts.append(
                f"{predicate['lower']:.8g}<={feature}<={predicate['upper']:.8g}"
            )
    return " AND ".join(parts)


def predicate_matches(trade: Trade, predicate: dict[str, Any]) -> bool:
    operation = predicate["op"]
    feature = predicate["feature"]
    if operation == "eq":
        return trade.categories.get(feature) == predicate["value"]
    value = trade.features.get(feature)
    if value is None:
        return False
    if operation == "le":
        return value <= predicate["value"]
    if operation == "ge":
        return value >= predicate["value"]
    return predicate["lower"] <= value <= predicate["upper"]


def rule_matches(trade: Trade, rule: list[dict[str, Any]]) -> bool:
    return all(predicate_matches(trade, predicate) for predicate in rule)


def evaluate_rule(
    rule: list[dict[str, Any]],
    splits: dict[str, list[Trade]],
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "description": rule_description(rule),
        "rule": rule,
        "splits": {},
    }
    for name, rows in splits.items():
        accepted = [trade for trade in rows if rule_matches(trade, rule)]
        row_metrics = metrics(accepted)
        row_metrics["retention"] = len(accepted) / len(rows) if rows else None
        result["splits"][name] = row_metrics
    return result


SEARCH_FEATURES = (
    "impulse_return_abs",
    "aggressive_imbalance_abs",
    "notional_intensity",
    "breakout_distance_abs",
    "breakout_fraction_of_impulse",
    "impulse_strength_product",
    "baseline_notional_log10",
    "impulse_trade_notional_log10",
    "impulse_trade_count_log10",
    "notional_per_trade_log10",
    "price_impact_per_million_log10",
    "entry_spread_bps",
    "same_symbol_gap_minutes",
    "same_side_gap_minutes",
    "opposite_side_gap_minutes",
    "prior_market_signals_5m",
    "prior_market_signals_15m",
    "prior_same_direction_market_5m",
    "prior_opposite_direction_market_5m",
    "market_direction_breadth_5m",
    "prior_symbol_signals_15m",
    "prior_same_side_symbol_signals_15m",
    "prior_opposite_symbol_signals_15m",
    "open_symbol_positions",
    "open_same_side_positions",
    "open_opposite_side_positions",
    "entry_extension_from_last_5m_close",
    "past_return_align_5m",
    "past_return_align_15m",
    "past_return_align_30m",
    "past_return_align_60m",
    "range_15m",
    "range_30m",
    "range_60m",
    "trend_efficiency_align_15m",
    "trend_efficiency_align_30m",
    "trend_efficiency_align_60m",
    "directional_range_location_15m",
    "directional_range_location_30m",
    "directional_range_location_60m",
)


def generate_single_rules(train: list[Trade]) -> list[list[dict[str, Any]]]:
    rules: list[list[dict[str, Any]]] = []
    for feature, values in (
        (
            feature,
            [trade.features[feature] for trade in train if feature in trade.features],
        )
        for feature in SEARCH_FEATURES
    ):
        if len(values) < 80 or min(values) == max(values):
            continue
        cuts = {
            fraction: quantile(values, fraction)
            for fraction in (0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)
        }
        for fraction in (0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8):
            rules.append([{"feature": feature, "op": "le", "value": cuts[fraction]}])
            rules.append([{"feature": feature, "op": "ge", "value": cuts[fraction]}])
        for lower_fraction, upper_fraction in (
            (0.1, 0.9),
            (0.2, 0.8),
            (0.3, 0.7),
            (0.2, 0.6),
            (0.3, 0.6),
            (0.4, 0.7),
            (0.4, 0.8),
        ):
            lower = cuts[lower_fraction]
            upper = cuts[upper_fraction]
            if lower < upper:
                rules.append(
                    [
                        {
                            "feature": feature,
                            "op": "between",
                            "lower": lower,
                            "upper": upper,
                        }
                    ]
                )
    for side in ("long", "short"):
        rules.append([{"feature": "side", "op": "eq", "value": side}])
    for session in ("utc_00", "utc_08", "utc_16"):
        rules.append([{"feature": "utc_session", "op": "eq", "value": session}])
    for weekpart in ("weekday", "weekend"):
        rules.append([{"feature": "weekpart", "op": "eq", "value": weekpart}])
    return rules


def comparable_pf(value: float | None) -> float:
    return value if value is not None else 100.0


def train_score(candidate: dict[str, Any], baseline: dict[str, Any]) -> float:
    row = candidate["splits"]["train"]
    return (
        comparable_pf(row["profit_factor"])
        - comparable_pf(baseline["profit_factor"])
        + 3.0 * (row["win_rate"] - baseline["win_rate"])
    ) * math.sqrt(row["retention"] or 0.0)


def candidate_score(
    candidate: dict[str, Any],
    baselines: dict[str, dict[str, Any]],
) -> float:
    train = candidate["splits"]["train"]
    valid = candidate["splits"]["validation"]
    pf_lift = min(
        comparable_pf(train["profit_factor"])
        - comparable_pf(baselines["train"]["profit_factor"]),
        comparable_pf(valid["profit_factor"])
        - comparable_pf(baselines["validation"]["profit_factor"]),
    )
    win_lift = min(
        train["win_rate"] - baselines["train"]["win_rate"],
        valid["win_rate"] - baselines["validation"]["win_rate"],
    )
    return (
        pf_lift
        + 3.0 * win_lift
        + 0.1 * math.sqrt(min(train["retention"], valid["retention"]))
    )


def eligible(
    candidate: dict[str, Any],
    baselines: dict[str, dict[str, Any]],
) -> bool:
    train = candidate["splits"]["train"]
    valid = candidate["splits"]["validation"]
    if train["count"] < 80 or valid["count"] < 40:
        return False
    if train["retention"] < 0.2 or valid["retention"] < 0.2:
        return False
    if train["net_pnl"] <= 0 or valid["net_pnl"] <= 0:
        return False
    train_pf_lift = comparable_pf(train["profit_factor"]) - comparable_pf(
        baselines["train"]["profit_factor"]
    )
    valid_pf_lift = comparable_pf(valid["profit_factor"]) - comparable_pf(
        baselines["validation"]["profit_factor"]
    )
    train_win_lift = train["win_rate"] - baselines["train"]["win_rate"]
    valid_win_lift = valid["win_rate"] - baselines["validation"]["win_rate"]
    improves_pf = train_pf_lift >= 0.05 and valid_pf_lift >= 0.05
    improves_win = train_win_lift >= 0.02 and valid_win_lift >= 0.02
    return improves_pf or improves_win


def pair_rules(
    singles: list[dict[str, Any]],
    baselines: dict[str, dict[str, Any]],
    splits: dict[str, list[Trade]],
) -> list[dict[str, Any]]:
    ranked = sorted(
        singles,
        key=lambda candidate: train_score(candidate, baselines["train"]),
        reverse=True,
    )
    per_feature: dict[str, int] = defaultdict(int)
    selected: list[dict[str, Any]] = []
    for candidate in ranked:
        feature = candidate["rule"][0]["feature"]
        if per_feature[feature] >= 2:
            continue
        if candidate["splits"]["train"]["count"] < 100:
            continue
        selected.append(candidate)
        per_feature[feature] += 1
        if len(selected) >= 28:
            break

    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    for left_index, left in enumerate(selected):
        for right in selected[left_index + 1 :]:
            left_feature = left["rule"][0]["feature"]
            right_feature = right["rule"][0]["feature"]
            if left_feature == right_feature:
                continue
            rule = [left["rule"][0], right["rule"][0]]
            key = json.dumps(rule, sort_keys=True)
            if key in seen:
                continue
            seen.add(key)
            candidate = evaluate_rule(rule, splits)
            if eligible(candidate, baselines):
                candidate["selection_score"] = candidate_score(candidate, baselines)
                output.append(candidate)
    return sorted(output, key=lambda row: row["selection_score"], reverse=True)


def named_diagnostic_rules(train: list[Trade]) -> dict[str, list[dict[str, Any]]]:
    rules: dict[str, list[dict[str, Any]]] = {
        "long_only": [{"feature": "side", "op": "eq", "value": "long"}],
        "short_only": [{"feature": "side", "op": "eq", "value": "short"}],
        "no_existing_symbol_position": [
            {"feature": "open_symbol_positions", "op": "le", "value": 0.0}
        ],
        "no_existing_same_side_position": [
            {"feature": "open_same_side_positions", "op": "le", "value": 0.0}
        ],
        "no_prior_symbol_signal_15m": [
            {"feature": "prior_symbol_signals_15m", "op": "le", "value": 0.0}
        ],
        "same_symbol_gap_at_least_15m": [
            {"feature": "same_symbol_gap_minutes", "op": "ge", "value": 15.0}
        ],
        "same_symbol_gap_at_least_30m": [
            {"feature": "same_symbol_gap_minutes", "op": "ge", "value": 30.0}
        ],
        "same_side_gap_at_least_30m": [
            {"feature": "same_side_gap_minutes", "op": "ge", "value": 30.0}
        ],
        "aligned_30m_return_positive": [
            {"feature": "past_return_align_30m", "op": "ge", "value": 0.0}
        ],
        "aligned_60m_return_positive": [
            {"feature": "past_return_align_60m", "op": "ge", "value": 0.0}
        ],
        "aligned_60m_trend_efficiency_positive": [
            {
                "feature": "trend_efficiency_align_60m",
                "op": "ge",
                "value": 0.0,
            }
        ],
        "long_and_aligned_60m_return_positive": [
            {"feature": "side", "op": "eq", "value": "long"},
            {"feature": "past_return_align_60m", "op": "ge", "value": 0.0},
        ],
    }
    for feature in (
        "impulse_return_abs",
        "notional_intensity",
        "breakout_distance_abs",
        "aggressive_imbalance_abs",
    ):
        values = [
            trade.features[feature] for trade in train if feature in trade.features
        ]
        if len(values) < 80:
            continue
        lower = quantile(values, 0.2)
        upper = quantile(values, 0.8)
        middle_rule = {
            "feature": feature,
            "op": "between",
            "lower": lower,
            "upper": upper,
        }
        rules[f"{feature}_train_q20_to_q80"] = [middle_rule]
        rules[f"{feature}_at_most_train_q80"] = [
            {"feature": feature, "op": "le", "value": upper}
        ]
        rules[f"{feature}_at_least_train_q20"] = [
            {"feature": feature, "op": "ge", "value": lower}
        ]
        rules[f"long_and_{feature}_train_q20_to_q80"] = [
            {"feature": "side", "op": "eq", "value": "long"},
            middle_rule,
        ]
        rules[f"long_and_{feature}_at_most_train_q80"] = [
            {"feature": "side", "op": "eq", "value": "long"},
            {"feature": feature, "op": "le", "value": upper},
        ]
        rules[f"long_and_{feature}_at_least_train_q20"] = [
            {"feature": "side", "op": "eq", "value": "long"},
            {"feature": feature, "op": "ge", "value": lower},
        ]
        rules[f"short_and_{feature}_train_q20_to_q80"] = [
            {"feature": "side", "op": "eq", "value": "short"},
            middle_rule,
        ]
        if feature == "impulse_return_abs":
            for fraction in (0.6, 0.7, 0.8, 0.9):
                threshold = quantile(values, fraction)
                suffix = int(fraction * 100)
                rules[f"impulse_return_abs_at_most_train_q{suffix}"] = [
                    {"feature": feature, "op": "le", "value": threshold}
                ]
                rules[f"long_and_impulse_return_abs_at_most_train_q{suffix}"] = [
                    {"feature": "side", "op": "eq", "value": "long"},
                    {"feature": feature, "op": "le", "value": threshold},
                ]
    rules["short_and_aligned_60m_return_positive"] = [
        {"feature": "side", "op": "eq", "value": "short"},
        {"feature": "past_return_align_60m", "op": "ge", "value": 0.0},
    ]
    return rules


def split_trades(trades: list[Trade]) -> tuple[dict[str, list[Trade]], dict[str, Any]]:
    closed = [
        trade
        for trade in trades
        if trade.closed_at is not None and trade.pnl is not None
    ]
    closed.sort(key=lambda trade: (trade.opened_at, trade.position_id))
    first_boundary = closed[len(closed) // 2].opened_at
    second_boundary = closed[len(closed) * 3 // 4].opened_at
    train = [
        trade
        for trade in closed
        if trade.opened_at < first_boundary and trade.closed_at <= first_boundary
    ]
    validation = [
        trade
        for trade in closed
        if first_boundary <= trade.opened_at < second_boundary
        and trade.closed_at <= second_boundary
    ]
    holdout = [trade for trade in closed if trade.opened_at >= second_boundary]
    retained_ids = {trade.position_id for trade in train + validation + holdout}
    return (
        {
            "train": train,
            "validation": validation,
            "holdout": holdout,
            "full": closed,
        },
        {
            "first_boundary": first_boundary.isoformat(),
            "second_boundary": second_boundary.isoformat(),
            "purged_cross_boundary_trades": len(closed) - len(retained_ids),
            "selection_uses_holdout": False,
        },
    )


def auc_for_feature(trades: list[Trade], feature: str) -> dict[str, Any] | None:
    values = [
        (trade.features[feature], 1 if trade.pnl and trade.pnl > 0 else 0)
        for trade in trades
        if feature in trade.features and trade.pnl is not None
    ]
    positives = sum(label for _, label in values)
    negatives = len(values) - positives
    if len(values) < 80 or positives == 0 or negatives == 0:
        return None
    values.sort(key=lambda item: item[0])
    rank_sum = 0.0
    index = 0
    while index < len(values):
        end = index + 1
        while end < len(values) and values[end][0] == values[index][0]:
            end += 1
        average_rank = ((index + 1) + end) / 2.0
        rank_sum += average_rank * sum(label for _, label in values[index:end])
        index = end
    auc = (rank_sum - positives * (positives + 1) / 2) / (positives * negatives)
    winning = [value for value, label in values if label == 1]
    losing = [value for value, label in values if label == 0]
    return {
        "feature": feature,
        "count": len(values),
        "auc_for_win": auc,
        "distance_from_random": abs(auc - 0.5),
        "winner_median": statistics.median(winning),
        "loser_median": statistics.median(losing),
    }


def removal_robustness(trades: list[Trade]) -> dict[str, Any]:
    pnls = sorted(
        (trade.pnl for trade in trades if trade.pnl and trade.pnl > 0), reverse=True
    )
    by_symbol: dict[str, float] = defaultdict(float)
    for trade in trades:
        if trade.pnl is not None:
            by_symbol[trade.symbol] += trade.pnl
    winning_symbols = sorted(
        (value for value in by_symbol.values() if value > 0), reverse=True
    )
    net = sum(trade.pnl or 0.0 for trade in trades)
    return {
        "net_without_top_1_trade": net - sum(pnls[:1]),
        "net_without_top_3_trades": net - sum(pnls[:3]),
        "net_without_top_5_trades": net - sum(pnls[:5]),
        "net_without_top_1_symbol": net - sum(winning_symbols[:1]),
        "net_without_top_3_symbols": net - sum(winning_symbols[:3]),
        "net_without_top_5_symbols": net - sum(winning_symbols[:5]),
        "symbol_count": len(by_symbol),
    }


def daily_metrics(trades: list[Trade]) -> dict[str, Any]:
    grouped: dict[str, list[Trade]] = defaultdict(list)
    for trade in trades:
        day = trade.opened_at.astimezone(LOCAL_TZ).date().isoformat()
        grouped[day].append(trade)
    return {day: metrics(rows) for day, rows in sorted(grouped.items())}


def percentile(values: list[float], fraction: float) -> float | None:
    return quantile(values, fraction) if values else None


def cluster_bootstrap(
    trades: list[Trade],
    *,
    iterations: int = 1_000,
    seed: int = 20260809,
) -> dict[str, Any]:
    by_symbol: dict[str, list[float]] = defaultdict(list)
    for trade in trades:
        if trade.pnl is not None:
            by_symbol[trade.symbol].append(trade.pnl)
    symbols = sorted(by_symbol)
    if not symbols:
        return {}
    rng = random.Random(seed)
    means: list[float] = []
    profit_factors: list[float] = []
    for _ in range(iterations):
        sampled: list[float] = []
        for _symbol in symbols:
            sampled.extend(by_symbol[rng.choice(symbols)])
        means.append(statistics.mean(sampled))
        pf = profit_factor(sampled)
        if pf is not None:
            profit_factors.append(pf)
    return {
        "iterations": iterations,
        "mean_pnl_ci95": [percentile(means, 0.025), percentile(means, 0.975)],
        "profit_factor_ci95": [
            percentile(profit_factors, 0.025),
            percentile(profit_factors, 0.975),
        ],
        "positive_mean_probability": sum(value > 0 for value in means) / len(means),
    }


def enrich_candidate(candidate: dict[str, Any], full: list[Trade]) -> None:
    accepted = [trade for trade in full if rule_matches(trade, candidate["rule"])]
    rejected = [trade for trade in full if not rule_matches(trade, candidate["rule"])]
    candidate["accepted_robustness"] = removal_robustness(accepted)
    candidate["accepted_daily"] = daily_metrics(accepted)
    candidate["accepted_symbol_cluster_bootstrap"] = cluster_bootstrap(accepted)
    candidate["rejected_metrics"] = metrics(rejected)


def write_candidate_csv(path: Path, candidates: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["rank", "description", "selection_score"]
    for split in ("train", "validation", "holdout", "full"):
        fields.extend(
            [
                f"{split}_count",
                f"{split}_retention",
                f"{split}_net_pnl",
                f"{split}_profit_factor",
                f"{split}_win_rate",
            ]
        )
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for rank, candidate in enumerate(candidates, start=1):
            row: dict[str, Any] = {
                "rank": rank,
                "description": candidate["description"],
                "selection_score": candidate.get("selection_score"),
            }
            for split in ("train", "validation", "holdout", "full"):
                split_metrics = candidate["splits"][split]
                for key in (
                    "count",
                    "retention",
                    "net_pnl",
                    "profit_factor",
                    "win_rate",
                ):
                    row[f"{split}_{key}"] = split_metrics.get(key)
            writer.writerow(row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Mine pre-entry filters for orderflow 15m candle exits."
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
        "--bars-dir",
        type=Path,
        default=Path(
            "data/server-paper-accounts-20260807T160821Z/analysis/"
            "klines-5m-ohlc-20260808"
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(
            "data/server-paper-accounts-20260807T160821Z/analysis/"
            "orderflow_15m_false_signal_filters_20260809.json"
        ),
    )
    parser.add_argument(
        "--candidate-csv",
        type=Path,
        default=Path(
            "data/server-paper-accounts-20260807T160821Z/analysis/"
            "orderflow_15m_false_signal_candidates_20260809.csv"
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    trades = load_trades(args.positions, args.signals, args.fills)
    for trade in trades:
        add_signal_features(trade)
    add_sequence_features(trades)
    bar_coverage = add_bar_features(trades, args.bars_dir)
    splits, split_meta = split_trades(trades)
    baselines = {name: metrics(rows) for name, rows in splits.items()}

    single_rules = generate_single_rules(splits["train"])
    singles = [evaluate_rule(rule, splits) for rule in single_rules]
    eligible_singles = []
    for candidate in singles:
        if eligible(candidate, baselines):
            candidate["selection_score"] = candidate_score(candidate, baselines)
            eligible_singles.append(candidate)
    eligible_singles.sort(key=lambda row: row["selection_score"], reverse=True)
    pairs = pair_rules(singles, baselines, splits)
    selected = sorted(
        eligible_singles + pairs,
        key=lambda row: row["selection_score"],
        reverse=True,
    )[:20]
    for candidate in selected:
        enrich_candidate(candidate, splits["full"])

    diagnostics = {}
    for name, rule in named_diagnostic_rules(splits["train"]).items():
        candidate = evaluate_rule(rule, splits)
        enrich_candidate(candidate, splits["full"])
        diagnostics[name] = candidate

    contrasts = [
        result
        for feature in SEARCH_FEATURES
        if (result := auc_for_feature(splits["train"], feature)) is not None
    ]
    contrasts.sort(key=lambda row: row["distance_from_random"], reverse=True)

    output = {
        "generated_at": datetime.now(UTC).isoformat(),
        "target_run": TARGET_RUN,
        "method": {
            "selection": (
                "quantile cut points are learned on the first half; candidates "
                "must improve train and validation; holdout is not used for selection"
            ),
            "split": (
                "50% train / 25% validation / 25% holdout by entry time; trades "
                "whose labels cross train/validation boundaries are purged"
            ),
            "minimum_support": "80 train, 40 validation, at least 20% retention",
            "costs": (
                "realized paper PnL already includes entry/exit fees; candle exit "
                "price remains the paper account's official-close execution model"
            ),
            "warning": (
                "the six-day server snapshot has been examined previously, so the "
                "holdout is pseudo-out-of-sample, not a truly untouched future sample"
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
            "single_rules_evaluated": len(singles),
            "eligible_single_rules": len(eligible_singles),
            "eligible_pair_rules": len(pairs),
        },
        "bar_coverage": bar_coverage,
        "split": split_meta,
        "baselines": baselines,
        "train_feature_win_contrasts": contrasts[:20],
        "named_diagnostics": diagnostics,
        "top_candidates": selected,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(output, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_candidate_csv(args.candidate_csv, selected)
    print(json.dumps(output, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
