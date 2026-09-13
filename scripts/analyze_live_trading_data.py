"""Mine a read-only Binance live-account export.

The exporter contains exchange fills, local exchange-order state, live order
intents, a USDT balance series, and a small set of operational summaries.  This
script deliberately keeps the analysis dependency-free so the result can be
re-run on another machine from the raw ``.csv.gz`` files alone.
"""

# The generated Chinese Markdown lines intentionally keep readable sentences.
# Code style is still checked for all other rules.
# ruff: noqa: E501

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
from collections import Counter, defaultdict, deque
from collections.abc import Iterable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from statistics import mean, median
from typing import Any
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("Asia/Shanghai")
ZERO = Decimal("0")
FEATURE_NAMES = (
    "impulse_return_pct",
    "notional_intensity",
    "aggressive_imbalance",
    "breakout_distance_pct",
    "impulse_trade_notional",
    "baseline_notional",
    "impulse_trade_count",
    "liquidation_count",
    "liquidation_notional",
)


def read_csv_gz(path: Path) -> list[dict[str, str]]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def dec(value: Any) -> Decimal:
    if value is None or value == "":
        return ZERO
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return ZERO


def bool_value(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes"}


def as_float(value: Decimal | int | float | None) -> float | None:
    if value is None:
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def parse_json(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def load_fills(path: Path) -> list[dict[str, Any]]:
    fills: list[dict[str, Any]] = []
    for row in read_csv_gz(path):
        payload = parse_json(row.get("raw_payload"))
        fills.append(
            {
                "environment": row.get("environment", ""),
                "account_label": row.get("account_label", ""),
                "symbol": row.get("symbol", ""),
                "trade_id": row.get("trade_id", ""),
                "order_id": row.get("order_id", ""),
                "side": row.get("side", "").upper(),
                "position_side": str(
                    payload.get("positionSide") or row.get("position_side") or "LONG"
                ).upper(),
                "price": dec(row.get("price")),
                "quantity": dec(row.get("quantity")),
                "realized_pnl": dec(row.get("realized_pnl")),
                "fee": dec(row.get("fee")),
                "fee_asset": row.get("fee_asset", ""),
                "trade_at": parse_dt(row.get("trade_at")),
                "maker": bool(payload.get("maker")),
            }
        )
    return fills


def load_orders(path: Path) -> list[dict[str, Any]]:
    orders: list[dict[str, Any]] = []
    for row in read_csv_gz(path):
        orders.append(
            {
                **row,
                "exchange_order_id": row.get("exchange_order_id", ""),
                "quantity": dec(row.get("quantity")),
                "price": dec(row.get("price")),
                "reduce_only": bool_value(row.get("reduce_only")),
                "created_at": parse_dt(row.get("created_at")),
                "updated_at": parse_dt(row.get("updated_at")),
            }
        )
    return orders


def load_intents(path: Path) -> dict[str, dict[str, Any]]:
    intents: dict[str, dict[str, Any]] = {}
    for row in read_csv_gz(path):
        details = parse_json(row.get("details"))
        features = details.get("features")
        intents[row.get("intent_id", "")] = {
            **row,
            "details": details,
            "features": features if isinstance(features, dict) else {},
            "reason": details.get("reason") or "unknown",
            "entry_type": details.get("entry_type") or row.get("entry_type"),
            "reduce_only": bool(details.get("reduce_only")),
            "created_at": parse_dt(details.get("created_at") or row.get("approved_at")),
        }
    return intents


def load_event_summaries(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in read_csv_gz(path):
        parsed = dict(row)
        for key in (
            "first_event_at",
            "last_event_at",
            "submitting_at",
            "acknowledged_at",
            "partially_filled_at",
            "filled_at",
            "canceled_at",
            "rejected_at",
        ):
            parsed[key] = parse_dt(row.get(key))
        parsed["event_count"] = int(row.get("event_count") or 0)
        parsed["unknown_pending_count"] = int(row.get("unknown_pending_count") or 0)
        rows.append(parsed)
    return rows


def load_balance(path: Path) -> list[dict[str, Any]]:
    return [
        {
            **row,
            "wallet_balance": dec(row.get("wallet_balance")),
            "available_balance": dec(row.get("available_balance")),
            "unrealized_pnl": dec(row.get("unrealized_pnl")),
            "observed_at": parse_dt(row.get("observed_at")),
        }
        for row in read_csv_gz(path)
    ]


def load_positions(path: Path) -> list[dict[str, Any]]:
    return [
        {
            **row,
            "position_amt": dec(row.get("position_amt")),
            "entry_price": dec(row.get("entry_price")),
            "mark_price": dec(row.get("mark_price")),
            "unrealized_pnl": dec(row.get("unrealized_pnl")),
            "notional": dec(row.get("notional")),
            "observed_at": parse_dt(row.get("observed_at")),
        }
        for row in read_csv_gz(path)
    ]


def load_config(path: Path) -> list[dict[str, Any]]:
    return [
        {
            **row,
            "multi_assets_mode": bool_value(row.get("multi_assets_mode")),
            "hedge_mode": bool_value(row.get("hedge_mode")),
            "can_trade": bool_value(row.get("can_trade")),
            "observed_at": parse_dt(row.get("observed_at")),
        }
        for row in read_csv_gz(path)
    ]


def load_reconciliation(path: Path) -> dict[str, Any]:
    rows = read_csv_gz(path)
    if not rows:
        return {}
    row = rows[0]
    return {
        "reconciliation_runs": int(row.get("reconciliation_runs") or 0),
        "total_mismatch_count": int(row.get("total_mismatch_count") or 0),
        "max_mismatch_count": int(row.get("max_mismatch_count") or 0),
        "mismatched_runs": int(row.get("mismatched_runs") or 0),
        "first_observed_at": row.get("first_observed_at"),
        "last_observed_at": row.get("last_observed_at"),
    }


def load_quality(path: Path) -> Counter[str]:
    return Counter(row.get("category") or "unknown" for row in read_csv_gz(path))


def aggregate_fills_by_order(fills: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for fill in fills:
        grouped[(fill["symbol"], fill["order_id"])].append(fill)

    result: list[dict[str, Any]] = []
    for (symbol, order_id), items in grouped.items():
        quantity = sum((item["quantity"] for item in items), start=ZERO)
        notional = sum(
            (item["price"] * item["quantity"] for item in items),
            start=ZERO,
        )
        trade_times = [item["trade_at"] for item in items if item["trade_at"]]
        side_counts = Counter(item["side"] for item in items)
        position_counts = Counter(item["position_side"] for item in items)
        result.append(
            {
                "key": (symbol, order_id),
                "symbol": symbol,
                "order_id": order_id,
                "side": side_counts.most_common(1)[0][0] if side_counts else "",
                "position_side": (
                    position_counts.most_common(1)[0][0]
                    if position_counts
                    else "LONG"
                ),
                "quantity": quantity,
                "notional": notional,
                "avg_price": notional / quantity if quantity else ZERO,
                "realized_pnl": sum(
                    (item["realized_pnl"] for item in items), start=ZERO
                ),
                "fee": sum((item["fee"] for item in items), start=ZERO),
                "fee_assets": sorted({item["fee_asset"] for item in items}),
                "fill_count": len(items),
                "first_trade_at": min(trade_times) if trade_times else None,
                "last_trade_at": max(trade_times) if trade_times else None,
            }
        )
    return sorted(
        result,
        key=lambda item: (item["first_trade_at"] or datetime.min.replace(tzinfo=UTC)),
    )


def pct(value: Decimal, denominator: Decimal) -> float | None:
    if denominator == ZERO:
        return None
    return as_float(value / denominator * Decimal("100"))


def percentile(values: list[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def max_drawdown(pnls: Iterable[Decimal]) -> Decimal:
    equity = ZERO
    peak = ZERO
    drawdown = ZERO
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return drawdown


def pnl_stats(trades: list[dict[str, Any]]) -> dict[str, Any]:
    pnls = [trade["net_pnl"] for trade in trades]
    gross_realized = sum(
        (trade["exchange_realized_pnl"] for trade in trades), start=ZERO
    )
    model_gross = sum((trade["gross_model_pnl"] for trade in trades), start=ZERO)
    fees = sum((trade["total_fees"] for trade in trades), start=ZERO)
    net = sum(pnls, start=ZERO)
    positive = sum((pnl for pnl in pnls if pnl > ZERO), start=ZERO)
    negative = sum((pnl for pnl in pnls if pnl < ZERO), start=ZERO)
    notional = sum(
        (trade["entry_price"] * trade["quantity"] for trade in trades),
        start=ZERO,
    )
    returns = [as_float(trade["return_pct"]) for trade in trades]
    returns = [value for value in returns if value is not None]
    hold_minutes = [float(trade["hold_minutes"]) for trade in trades]
    top_positive = sorted((pnl for pnl in pnls if pnl > ZERO), reverse=True)
    return {
        "trades": len(trades),
        "wins": sum(1 for pnl in pnls if pnl > ZERO),
        "losses": sum(1 for pnl in pnls if pnl < ZERO),
        "win_rate_pct": pct(
            Decimal(sum(1 for pnl in pnls if pnl > ZERO)),
            Decimal(len(pnls)),
        ),
        "gross_realized_pnl": as_float(gross_realized),
        "model_gross_pnl": as_float(model_gross),
        "fees": as_float(fees),
        "net_pnl_after_fees": as_float(net),
        "profit_factor": as_float(positive / (-negative)) if negative else None,
        "expectancy_per_trade": as_float(net / Decimal(len(pnls))) if pnls else None,
        "average_net_pnl": as_float(Decimal(str(mean([float(x) for x in pnls]))))
        if pnls
        else None,
        "median_net_pnl": as_float(Decimal(str(median([float(x) for x in pnls]))))
        if pnls
        else None,
        "max_drawdown": as_float(max_drawdown(pnls)),
        "average_return_pct": mean(returns) if returns else None,
        "median_return_pct": median(returns) if returns else None,
        "average_hold_minutes": mean(hold_minutes) if hold_minutes else None,
        "median_hold_minutes": median(hold_minutes) if hold_minutes else None,
        "total_entry_notional": as_float(notional),
        "top_1_positive_share_pct": pct(top_positive[0], positive) if positive else None,
        "top_5_positive_share_pct": pct(sum(top_positive[:5], start=ZERO), positive)
        if positive
        else None,
    }


def rank_values(values: list[float]) -> list[float]:
    ordered = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    index = 0
    while index < len(ordered):
        end = index
        while end + 1 < len(ordered) and ordered[end + 1][1] == ordered[index][1]:
            end += 1
        rank = (index + end + 2) / 2
        for position in range(index, end + 1):
            ranks[ordered[position][0]] = rank
        index = end + 1
    return ranks


def spearman(pairs: list[tuple[float, float]]) -> float | None:
    if len(pairs) < 3:
        return None
    x = rank_values([pair[0] for pair in pairs])
    y = rank_values([pair[1] for pair in pairs])
    x_mean = mean(x)
    y_mean = mean(y)
    numerator = sum((a - x_mean) * (b - y_mean) for a, b in zip(x, y, strict=True))
    x_denominator = math.sqrt(sum((a - x_mean) ** 2 for a in x))
    y_denominator = math.sqrt(sum((b - y_mean) ** 2 for b in y))
    if x_denominator == 0 or y_denominator == 0:
        return None
    return numerator / (x_denominator * y_denominator)


def feature_mining(trades: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ordered_trades = sorted(trades, key=lambda item: item["opened_at"] or datetime.min.replace(tzinfo=UTC))
    split_index = max(1, int(len(ordered_trades) * 0.6)) if ordered_trades else 0
    summary_rows: list[dict[str, Any]] = []
    bucket_rows: list[dict[str, Any]] = []
    for feature in FEATURE_NAMES:
        eligible = [
            trade
            for trade in ordered_trades
            if trade["features"].get(feature) not in (None, "")
        ]
        pairs = [(float(dec(trade["features"][feature])), float(trade["net_pnl"])) for trade in eligible]
        train = eligible[:split_index]
        test = eligible[split_index:]
        train_pairs = [(float(dec(t["features"][feature])), float(t["net_pnl"])) for t in train]
        test_pairs = [(float(dec(t["features"][feature])), float(t["net_pnl"])) for t in test]
        summary_rows.append(
            {
                "feature": feature,
                "sample_count": len(eligible),
                "full_spearman": spearman(pairs),
                "train_spearman": spearman(train_pairs),
                "holdout_spearman": spearman(test_pairs),
            }
        )
        if not eligible:
            continue
        ranked = sorted(eligible, key=lambda item: float(dec(item["features"][feature])))
        for bucket in range(4):
            start = len(ranked) * bucket // 4
            end = len(ranked) * (bucket + 1) // 4
            selected = ranked[start:end]
            if not selected:
                continue
            stats = pnl_stats(selected)
            bucket_rows.append(
                {
                    "feature": feature,
                    "bucket": f"Q{bucket + 1}",
                    "min_value": float(dec(selected[0]["features"][feature])),
                    "max_value": float(dec(selected[-1]["features"][feature])),
                    **stats,
                }
            )
    summary_rows.sort(
        key=lambda row: abs(row["full_spearman"] or 0),
        reverse=True,
    )
    return summary_rows, bucket_rows


def grouped_stats(
    trades: list[dict[str, Any]],
    key_function: Any,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        grouped[str(key_function(trade))].append(trade)
    rows: list[dict[str, Any]] = []
    for key, items in sorted(grouped.items()):
        rows.append({"group": key, **pnl_stats(items)})
    return rows


def latency_metrics(
    event_rows: list[dict[str, Any]],
    orders_by_client: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    values: dict[str, list[float]] = defaultdict(list)
    by_category: dict[str, dict[str, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )
    unknown = 0
    canceled_after_partial = 0
    for event in event_rows:
        submitting = event.get("submitting_at")
        for label, field in (
            ("acknowledgement", "acknowledged_at"),
            ("partial_fill", "partially_filled_at"),
            ("final_fill", "filled_at"),
            ("cancel", "canceled_at"),
            ("reject", "rejected_at"),
        ):
            target = event.get(field)
            if submitting and target:
                milliseconds = (target - submitting).total_seconds() * 1000
                values[label].append(milliseconds)
                order = orders_by_client.get(event.get("client_order_id", ""))
                category = "exit" if order and order["reduce_only"] else "entry"
                by_category[category][label].append(milliseconds)
        if event.get("unknown_pending_count", 0) > 0:
            unknown += 1
        if event.get("partially_filled_at") and event.get("canceled_at"):
            canceled_after_partial += 1

    def summarize(series: list[float]) -> dict[str, float | None]:
        return {
            "count": len(series),
            "median_ms": median(series) if series else None,
            "p95_ms": percentile(series, 0.95),
            "max_ms": max(series) if series else None,
        }

    return {
        "overall": {label: summarize(series) for label, series in values.items()},
        "by_category": {
            category: {label: summarize(series) for label, series in fields.items()}
            for category, fields in by_category.items()
        },
        "orders_with_unknown_pending_reconciliation": unknown,
        "orders_canceled_after_partial_fill": canceled_after_partial,
    }


def reconstruct_trades(
    order_aggs: list[dict[str, Any]],
    orders_by_exchange_id: dict[str, dict[str, Any]],
    intents: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], int, Decimal]:
    for aggregate in order_aggs:
        local_order = orders_by_exchange_id.get(aggregate["order_id"])
        aggregate["local_order"] = local_order
        intent = intents.get(local_order.get("intent_id", "")) if local_order else None
        aggregate["intent"] = intent

    lots_by_symbol: dict[str, deque[dict[str, Any]]] = defaultdict(deque)
    trades: list[dict[str, Any]] = []
    unmatched_exit_quantity = ZERO
    for aggregate in order_aggs:
        if aggregate["position_side"] not in {"LONG", "BOTH"}:
            continue
        local_order = aggregate.get("local_order") or {}
        intent = aggregate.get("intent") or {}
        if aggregate["side"] == "BUY":
            lots_by_symbol[aggregate["symbol"]].append(
                {
                    "remaining_quantity": aggregate["quantity"],
                    "quantity": aggregate["quantity"],
                    "entry_price": aggregate["avg_price"],
                    "entry_fee": aggregate["fee"],
                    "opened_at": aggregate["first_trade_at"],
                    "entry_order_id": aggregate["order_id"],
                    "entry_reason": intent.get("reason", "unknown"),
                    "features": dict(intent.get("features") or {}),
                }
            )
            continue
        if aggregate["side"] != "SELL":
            continue
        remaining = aggregate["quantity"]
        lots = lots_by_symbol[aggregate["symbol"]]
        while remaining > ZERO and lots:
            lot = lots[0]
            matched = min(remaining, lot["remaining_quantity"])
            entry_fee = lot["entry_fee"] * matched / lot["quantity"]
            exit_fee = aggregate["fee"] * matched / aggregate["quantity"] if aggregate["quantity"] else ZERO
            exchange_realized = aggregate["realized_pnl"] * matched / aggregate["quantity"] if aggregate["quantity"] else ZERO
            gross_model = (aggregate["avg_price"] - lot["entry_price"]) * matched
            total_fees = entry_fee + exit_fee
            net_pnl = exchange_realized - total_fees
            opened_at = lot["opened_at"]
            closed_at = aggregate["last_trade_at"]
            hold_minutes = (
                (closed_at - opened_at).total_seconds() / 60
                if opened_at and closed_at
                else 0.0
            )
            exit_intent = aggregate.get("intent") or {}
            trades.append(
                {
                    "symbol": aggregate["symbol"],
                    "entry_order_id": lot["entry_order_id"],
                    "exit_order_id": aggregate["order_id"],
                    "quantity": matched,
                    "entry_price": lot["entry_price"],
                    "exit_price": aggregate["avg_price"],
                    "gross_model_pnl": gross_model,
                    "exchange_realized_pnl": exchange_realized,
                    "entry_fee": entry_fee,
                    "exit_fee": exit_fee,
                    "total_fees": total_fees,
                    "net_pnl": net_pnl,
                    "return_pct": net_pnl / (lot["entry_price"] * matched) * 100
                    if lot["entry_price"] and matched
                    else ZERO,
                    "opened_at": opened_at,
                    "closed_at": closed_at,
                    "hold_minutes": hold_minutes,
                    "entry_reason": lot["entry_reason"],
                    "exit_reason": exit_intent.get("reason", "unknown"),
                    "features": lot["features"],
                }
            )
            remaining -= matched
            lot["remaining_quantity"] -= matched
            if lot["remaining_quantity"] <= ZERO:
                lots.popleft()
        if remaining > ZERO:
            unmatched_exit_quantity += remaining
    open_quantity = sum(
        (lot["remaining_quantity"] for lots in lots_by_symbol.values() for lot in lots),
        start=ZERO,
    )
    trades.sort(key=lambda item: item["closed_at"] or datetime.min.replace(tzinfo=UTC))
    return trades, len(trades), unmatched_exit_quantity + open_quantity


def order_metrics(
    orders: list[dict[str, Any]],
    fills_by_order: list[dict[str, Any]],
) -> dict[str, Any]:
    state_counts = Counter(row.get("state", "unknown") for row in orders)
    by_category: dict[str, Counter[str]] = defaultdict(Counter)
    for order in orders:
        category = "exit" if order["reduce_only"] else "entry"
        by_category[category][order.get("state", "unknown")] += 1
    fill_aggregates = [row for row in fills_by_order if row["quantity"] > ZERO]
    fragment_count = sum(row["fill_count"] for row in fill_aggregates)
    linked = sum(1 for row in fill_aggregates if row.get("local_order"))
    return {
        "total_orders": len(orders),
        "state_counts": dict(state_counts),
        "by_category": {key: dict(value) for key, value in by_category.items()},
        "filled_order_count": state_counts.get("filled", 0),
        "fill_fragment_count": fragment_count,
        "average_fragments_per_filled_order": fragment_count / linked if linked else None,
        "filled_order_link_coverage_pct": linked / len(fill_aggregates) * 100
        if fill_aggregates
        else None,
    }


def coverage_metrics(
    fills: list[dict[str, Any]],
    orders: list[dict[str, Any]],
    intents: dict[str, dict[str, Any]],
    balances: list[dict[str, Any]],
    positions: list[dict[str, Any]],
    config: list[dict[str, Any]],
) -> dict[str, Any]:
    fill_times = [row["trade_at"] for row in fills if row["trade_at"]]
    balance_times = [row["observed_at"] for row in balances if row["observed_at"]]
    position_times = [row["observed_at"] for row in positions if row["observed_at"]]
    latest_balance = max(balances, key=lambda row: row["observed_at"] or datetime.min.replace(tzinfo=UTC), default=None)
    latest_config = max(config, key=lambda row: row["observed_at"] or datetime.min.replace(tzinfo=UTC), default=None)
    nonzero_positions = [row for row in positions if row["position_amt"] != ZERO]
    return {
        "fills": {
            "rows": len(fills),
            "unique_trade_ids": len({(row["symbol"], row["trade_id"]) for row in fills}),
            "unique_order_ids": len({(row["symbol"], row["order_id"]) for row in fills}),
            "symbols": len({row["symbol"] for row in fills}),
            "first_trade_at": iso(min(fill_times)) if fill_times else None,
            "last_trade_at": iso(max(fill_times)) if fill_times else None,
        },
        "orders": {
            "rows": len(orders),
            "unique_client_order_ids": len({row.get("client_order_id") for row in orders}),
            "symbols": len({row.get("symbol") for row in orders}),
        },
        "intents": {
            "rows": len(intents),
            "entry_intents": sum(1 for row in intents.values() if not row["reduce_only"]),
            "exit_intents": sum(1 for row in intents.values() if row["reduce_only"]),
        },
        "usdt_balance": {
            "rows": len(balances),
            "first_observed_at": iso(min(balance_times)) if balance_times else None,
            "last_observed_at": iso(max(balance_times)) if balance_times else None,
            "latest_wallet_balance": as_float(latest_balance["wallet_balance"])
            if latest_balance
            else None,
            "latest_available_balance": as_float(latest_balance["available_balance"])
            if latest_balance
            else None,
            "latest_unrealized_pnl": as_float(latest_balance["unrealized_pnl"])
            if latest_balance
            else None,
            "nonzero_wallet_samples": sum(
                1 for row in balances if row["wallet_balance"] != ZERO
            ),
        },
        "latest_positions": {
            "rows": len(positions),
            "nonzero_rows": len(nonzero_positions),
            "symbols": len({row["symbol"] for row in nonzero_positions}),
            "gross_notional": as_float(
                sum((row["notional"].copy_abs() for row in nonzero_positions), start=ZERO)
            ),
            "unrealized_pnl": as_float(
                sum((row["unrealized_pnl"] for row in nonzero_positions), start=ZERO)
            ),
            "latest_observed_at": iso(max(position_times)) if position_times else None,
        },
        "latest_account_config": {
            key: latest_config[key] if latest_config else None
            for key in ("multi_assets_mode", "hedge_mode", "can_trade", "fee_tier")
        },
    }


def quality_metrics(
    fills: list[dict[str, Any]],
    orders: list[dict[str, Any]],
    order_aggs: list[dict[str, Any]],
    reconciliation: dict[str, Any],
    quality_categories: Counter[str],
) -> dict[str, Any]:
    duplicate_trade_ids = len(fills) - len({(row["symbol"], row["trade_id"]) for row in fills})
    invalid_quantities = sum(1 for row in fills if row["quantity"] <= ZERO)
    invalid_prices = sum(1 for row in fills if row["price"] <= ZERO)
    order_ids = {row.get("exchange_order_id") for row in orders if row.get("exchange_order_id")}
    missing_order_links = sum(1 for row in order_aggs if row["order_id"] not in order_ids)
    return {
        "duplicate_trade_id_rows": duplicate_trade_ids,
        "nonpositive_fill_quantities": invalid_quantities,
        "nonpositive_fill_prices": invalid_prices,
        "fill_orders_missing_local_exchange_order": missing_order_links,
        "reconciliation": reconciliation,
        "market_data_quality_categories": dict(quality_categories.most_common()),
    }


def serialise_trade(trade: dict[str, Any]) -> dict[str, Any]:
    output = {
        "symbol": trade["symbol"],
        "entry_order_id": trade["entry_order_id"],
        "exit_order_id": trade["exit_order_id"],
        "quantity": as_float(trade["quantity"]),
        "entry_price": as_float(trade["entry_price"]),
        "exit_price": as_float(trade["exit_price"]),
        "gross_model_pnl": as_float(trade["gross_model_pnl"]),
        "exchange_realized_pnl": as_float(trade["exchange_realized_pnl"]),
        "entry_fee": as_float(trade["entry_fee"]),
        "exit_fee": as_float(trade["exit_fee"]),
        "total_fees": as_float(trade["total_fees"]),
        "net_pnl": as_float(trade["net_pnl"]),
        "return_pct": as_float(trade["return_pct"]),
        "opened_at": iso(trade["opened_at"]),
        "closed_at": iso(trade["closed_at"]),
        "hold_minutes": trade["hold_minutes"],
        "entry_reason": trade["entry_reason"],
        "exit_reason": trade["exit_reason"],
    }
    for feature in FEATURE_NAMES:
        value = trade["features"].get(feature)
        output[feature] = as_float(dec(value)) if value not in (None, "") else None
    return output


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_report(
    path: Path,
    report: dict[str, Any],
    output_dir: Path,
) -> None:
    coverage = report["coverage"]
    overall = report["overall_performance"]
    execution = report["execution"]
    quality = report["data_quality"]
    top_symbols = report["by_symbol"][:5]
    top_features = report["feature_mining"][:5]
    lines = [
        "# 实盘交易数据挖掘报告",
        "",
        f"生成时间：{report['generated_at']}",
        "",
        "## 数据范围",
        "",
        (
            f"账户 `{coverage['fills'].get('account_label', 'live/primary')}`；"
            f"成交 {coverage['fills']['rows']:,} 条，覆盖 "
            f"{coverage['fills']['first_trade_at']} 至 {coverage['fills']['last_trade_at']}。"
        ),
        (
            f"本地还保存了 {coverage['usdt_balance']['rows']:,} 条 USDT 余额快照、"
            f"{coverage['latest_positions']['rows']:,} 行拉回时点持仓快照、"
            f"{coverage['orders']['rows']:,} 条实盘订单和 "
            f"{coverage['intents']['rows']:,} 条订单意图。"
        ),
        "",
        "## 结论摘要",
        "",
        (
            f"- 交易级净已实现 PnL（已扣成交手续费）："
            f"**{overall['net_pnl_after_fees']:.6f} USDT**；"
            f"胜率 {overall['win_rate_pct']:.2f}%、"
            f"Profit Factor {overall['profit_factor']:.3f}。"
            if overall.get("profit_factor") is not None
            else (
                f"- 交易级净已实现 PnL（已扣成交手续费）："
                f"**{overall['net_pnl_after_fees']:.6f} USDT**；"
                f"胜率 {overall['win_rate_pct']:.2f}%。"
            )
        ),
        (
            f"- 序列最大回撤约 **{overall['max_drawdown']:.6f} USDT**；"
            f"前 1 笔盈利占全部盈利尾部 {overall['top_1_positive_share_pct']:.2f}%，"
            f"前 5 笔占 {overall['top_5_positive_share_pct']:.2f}%。"
        ),
        (
            f"- 实盘订单填充率按订单状态计为 "
            f"{execution['filled_order_count']}/{execution['total_orders']}；"
            f"实际成交被拆成 {execution['fill_fragment_count']:,} 个 fill 片段。"
        ),
        (
            f"- Reconciliation 共 {quality['reconciliation'].get('reconciliation_runs', 0):,} 次，"
            f"不一致次数 {quality['reconciliation'].get('total_mismatch_count', 0):,}，"
            f"发生不一致的运行 {quality['reconciliation'].get('mismatched_runs', 0):,} 次。"
        ),
        "",
        "## 执行与风险观察",
        "",
        (
            f"- 入口订单按状态：{execution['by_category'].get('entry', {})}；"
            f"退出订单按状态：{execution['by_category'].get('exit', {})}。"
        ),
        (
            f"- 最终成交延迟中位数："
            f"{execution['latency'].get('overall', {}).get('final_fill', {}).get('median_ms')} ms；"
            f"P95：{execution['latency'].get('overall', {}).get('final_fill', {}).get('p95_ms')} ms。"
        ),
        (
            f"- 未知待 reconciliation 订单 {execution['latency'].get('orders_with_unknown_pending_reconciliation', 0)} 个；"
            f"部分成交后取消 {execution['latency'].get('orders_canceled_after_partial_fill', 0)} 个。"
        ),
        (
            f"- 拉回快照中的账户配置：Hedge Mode={coverage['latest_account_config'].get('hedge_mode')}，"
            f"Multi-Assets={coverage['latest_account_config'].get('multi_assets_mode')}，"
            f"Can Trade={coverage['latest_account_config'].get('can_trade')}。"
        ),
        "",
        "## 按品种净 PnL（前五）",
        "",
        "| 品种 | 交易数 | 净 PnL | 胜率 | Profit Factor |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in top_symbols:
        lines.append(
            f"| {row['group']} | {row['trades']} | {row['net_pnl_after_fees']:.6f} | "
            f"{row['win_rate_pct']:.2f}% | "
            f"{row['profit_factor']:.3f} |"
            if row.get("profit_factor") is not None
            else (
                f"| {row['group']} | {row['trades']} | {row['net_pnl_after_fees']:.6f} | "
                f"{row['win_rate_pct']:.2f}% | — |"
            )
        )
    lines.extend(
        [
            "",
            "## 特征挖掘（探索性）",
            "",
            "以下相关性和分位数只用于提出候选假设，不代表因果关系；样本来自同一套实盘规则，不能直接当作新参数上线依据。",
            "",
            "| 特征 | 样本 | 全样本 Spearman | 前 60% | 后 40% |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for row in top_features:
        lines.append(
            f"| {row['feature']} | {row['sample_count']} | "
            f"{row['full_spearman'] if row['full_spearman'] is not None else '—'} | "
            f"{row['train_spearman'] if row['train_spearman'] is not None else '—'} | "
            f"{row['holdout_spearman'] if row['holdout_spearman'] is not None else '—'} |"
        )
    lines.extend(
        [
            "",
            "## 产物",
            "",
            f"- 原始拉回数据：`{output_dir.parent / 'raw'}`",
            f"- 交易级配对：`{output_dir / 'trades.csv'}`",
            f"- 按日统计：`{output_dir / 'daily_summary.csv'}`",
            f"- 按品种统计：`{output_dir / 'symbol_summary.csv'}`",
            f"- 特征分箱：`{output_dir / 'feature_bins.csv'}`",
            f"- 机器可读汇总：`{output_dir / 'analysis.json'}`",
            "",
            "## 解读边界",
            "",
            "本次数据覆盖约四天，且实盘策略为仅多头 B1 变体；样本量足以做执行与行为审计，但不足以证明长期优势。下一步应保持规则冻结，继续积累至少 30 天或 300 笔成熟交易，再做前向验证。",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_report(input_dir: Path, output_dir: Path) -> dict[str, Any]:
    fills = load_fills(input_dir / "account_fill_events.csv.gz")
    orders = load_orders(input_dir / "exchange_orders.csv.gz")
    intents = load_intents(input_dir / "order_intents.csv.gz")
    event_rows = load_event_summaries(input_dir / "exchange_order_event_summary.csv.gz")
    balances = load_balance(input_dir / "account_balance_usdt.csv.gz")
    positions = load_positions(input_dir / "latest_positions.csv.gz")
    config = load_config(input_dir / "account_config_public.csv.gz")
    reconciliation = load_reconciliation(input_dir / "reconciliation_summary.csv.gz")
    quality_categories = load_quality(input_dir / "market_data_quality_public.csv.gz")

    order_aggs = aggregate_fills_by_order(fills)
    orders_by_exchange_id = {
        str(order["exchange_order_id"]): order
        for order in orders
        if order.get("exchange_order_id")
    }
    trades, matched_count, unmatched_quantity = reconstruct_trades(
        order_aggs,
        orders_by_exchange_id,
        intents,
    )
    for aggregate in order_aggs:
        aggregate.setdefault("local_order", None)
    order_summary = order_metrics(orders, order_aggs)
    order_summary["latency"] = latency_metrics(
        event_rows,
        {order["client_order_id"]: order for order in orders},
    )
    overall = pnl_stats(trades)
    feature_summary, feature_bins = feature_mining(trades)
    coverage = coverage_metrics(
        fills,
        orders,
        intents,
        balances,
        positions,
        config,
    )
    coverage["fills"]["account_label"] = (
        f"{fills[0]['environment']}/{fills[0]['account_label']}" if fills else "unknown"
    )
    quality = quality_metrics(
        fills,
        orders,
        order_aggs,
        reconciliation,
        quality_categories,
    )

    report: dict[str, Any] = {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "coverage": coverage,
        "overall_performance": overall,
        "execution": order_summary,
        "data_quality": quality,
        "reconstruction": {
            "matched_trade_segments": matched_count,
            "unmatched_or_open_quantity": as_float(unmatched_quantity),
            "filled_order_aggregates": len(order_aggs),
        },
        "by_symbol": sorted(
            grouped_stats(trades, lambda item: item["symbol"]),
            key=lambda row: row["net_pnl_after_fees"],
            reverse=True,
        ),
        "by_exit_reason": grouped_stats(trades, lambda item: item["exit_reason"]),
        "by_local_day": grouped_stats(
            trades,
            lambda item: item["closed_at"].astimezone(LOCAL_TZ).date().isoformat(),
        ),
        "by_local_session": grouped_stats(
            trades,
            lambda item: (
                f"CN_{item['closed_at'].astimezone(LOCAL_TZ).hour // 6 * 6:02d}"
            ),
        ),
        "feature_mining": feature_summary,
    }
    write_csv(output_dir / "trades.csv", [serialise_trade(trade) for trade in trades])
    write_csv(output_dir / "symbol_summary.csv", report["by_symbol"])
    write_csv(output_dir / "daily_summary.csv", report["by_local_day"])
    write_csv(output_dir / "exit_reason_summary.csv", report["by_exit_reason"])
    write_csv(output_dir / "session_summary.csv", report["by_local_session"])
    write_csv(output_dir / "feature_summary.csv", feature_summary)
    write_csv(output_dir / "feature_bins.csv", feature_bins)
    (output_dir / "analysis.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    write_report(output_dir / "report.md", report, output_dir)
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    report = build_report(args.input_dir, args.output_dir)
    overall = report["overall_performance"]
    print(
        "live trading analysis completed: "
        f"trades={overall['trades']} "
        f"net_pnl={overall['net_pnl_after_fees']:.6f} "
        f"max_drawdown={overall['max_drawdown']:.6f}"
    )


if __name__ == "__main__":
    main()
