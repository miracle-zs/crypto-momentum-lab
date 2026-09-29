#!/usr/bin/env python3
"""Analyze a signal-price limit-entry counterfactual from server exports.

The exported runtime market state is a 15-second aggregate.  This script
therefore reports two observable proxies instead of pretending to know an
exchange queue-level fill:

* quote_touch: the post-signal best ask (long) / bid (short) touched the limit;
* trade_touch: the post-signal OHLC low/high touched the limit.  This is an
  upper bound because a trade touch does not prove that our order was ahead of
  the queue.

An order whose limit is already marketable at the signal quote is classified
as immediate_marketable and is valued at the signal best ask/bid, not at the
limit price.  That is the normal behavior of a non-post-only marketable limit.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import html
import json
import math
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Any, Iterable


getcontext().prec = 40
UTC = timezone.utc


def parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value is None or value == "":
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def open_text(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", newline="")
    return path.open("r", encoding="utf-8", newline="")


def load_csv(path: Path) -> list[dict[str, str]]:
    with open_text(path) as handle:
        return list(csv.DictReader(handle))


def truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def iso(value: datetime | None) -> str:
    return value.isoformat() if value else ""


def as_number(value: Decimal | None) -> str:
    if value is None:
        return ""
    return format(value, "f")


def pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:.2f}%"


def safe_float(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def load_states(rows: Iterable[dict[str, str]]) -> dict[str, list[dict[str, Any]]]:
    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("environment") not in {"research", ""}:
            continue
        start = parse_dt(row.get("bucket_start"))
        end = parse_dt(row.get("bucket_end"))
        if not start or not end:
            continue
        by_symbol[row.get("symbol", "")].append(
            {
                "bucket_start": start,
                "bucket_end": end,
                "last_bid": decimal(row.get("last_bid_price")),
                "last_ask": decimal(row.get("last_ask_price")),
                "low": decimal(row.get("low_price")),
                "high": decimal(row.get("high_price")),
                "data_complete": truthy(row.get("data_complete")),
                "trade_count": int(row.get("trade_count") or 0),
            }
        )
    for states in by_symbol.values():
        states.sort(key=lambda item: item["bucket_start"])
    return by_symbol


def merge_states(*state_maps: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    """Merge state exports by symbol and bucket, preserving one row per bucket."""
    merged: dict[str, dict[datetime, dict[str, Any]]] = defaultdict(dict)
    for state_map in state_maps:
        for symbol, states in state_map.items():
            for state in states:
                merged[symbol][state["bucket_start"]] = state
    output = {symbol: sorted(bucket_map.values(), key=lambda item: item["bucket_start"]) for symbol, bucket_map in merged.items()}
    return output


def load_orders(
    intents: Iterable[dict[str, str]],
    orders: Iterable[dict[str, str]],
    fills: Iterable[dict[str, str]],
) -> tuple[dict[str, list[dict[str, str]]], dict[str, list[dict[str, str]]], dict[str, list[dict[str, str]]]]:
    intents_by_candidate: dict[str, list[dict[str, str]]] = defaultdict(list)
    orders_by_intent: dict[str, list[dict[str, str]]] = defaultdict(list)
    fills_by_order: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in intents:
        candidate_id = row.get("candidate_id", "")
        if candidate_id:
            intents_by_candidate[candidate_id].append(row)
    for row in orders:
        intent_id = row.get("intent_id", "")
        if intent_id:
            orders_by_intent[intent_id].append(row)
    for row in fills:
        order_id = row.get("order_id", "")
        if order_id:
            fills_by_order[order_id].append(row)
    return intents_by_candidate, orders_by_intent, fills_by_order


def load_runtime_events(rows: Iterable[dict[str, str]]) -> tuple[dict[str, dict[str, datetime]], dict[str, dict[str, datetime]]]:
    """Index durable runtime phase times by exchange order and candidate."""
    by_order: dict[str, dict[str, datetime]] = defaultdict(dict)
    by_candidate: dict[str, dict[str, datetime]] = defaultdict(dict)
    for row in rows:
        occurred_at = parse_dt(row.get("occurred_at"))
        if occurred_at is None:
            continue
        details = json_object(row.get("details"))
        event_type = row.get("event_type", "")
        order_id = str(details.get("exchange_order_id") or details.get("order_id") or "")
        candidate_id = str(details.get("candidate_id") or "")
        if order_id:
            previous = by_order[order_id].get(event_type)
            if previous is None or occurred_at < previous:
                by_order[order_id][event_type] = occurred_at
        if candidate_id:
            previous = by_candidate[candidate_id].get(event_type)
            if previous is None or occurred_at < previous:
                by_candidate[candidate_id][event_type] = occurred_at
    return by_order, by_candidate


def actual_for_candidate(
    candidate_id: str,
    intents_by_candidate: dict[str, list[dict[str, str]]],
    orders_by_intent: dict[str, list[dict[str, str]]],
    fills_by_order: dict[str, list[dict[str, str]]],
    runtime_by_order: dict[str, dict[str, datetime]],
    runtime_by_candidate: dict[str, dict[str, datetime]],
) -> dict[str, Any]:
    intent_rows = intents_by_candidate.get(candidate_id, [])
    order_rows: list[dict[str, str]] = []
    for intent in intent_rows:
        order_rows.extend(orders_by_intent.get(intent.get("intent_id", ""), []))
    order_rows.sort(key=lambda row: parse_dt(row.get("created_at")) or datetime.max.replace(tzinfo=UTC))
    order_ids = {row.get("exchange_order_id", "") for row in order_rows}
    fill_rows: list[dict[str, str]] = []
    for order_id in order_ids:
        fill_rows.extend(fills_by_order.get(order_id, []))

    # A candidate is an entry candidate here.  Only fills on the order's side
    # are included; SELL fills are exits for the long-only live run.
    entry_fills: list[dict[str, str]] = []
    expected_side = next((row.get("side") for row in order_rows if row.get("side")), "BUY")
    for fill in fill_rows:
        if fill.get("side", "").upper() == expected_side.upper():
            entry_fills.append(fill)

    qty = sum((decimal(row.get("quantity")) or Decimal(0) for row in entry_fills), Decimal(0))
    notional = sum(
        ((decimal(row.get("price")) or Decimal(0)) * (decimal(row.get("quantity")) or Decimal(0)) for row in entry_fills),
        Decimal(0),
    )
    fee = sum((decimal(row.get("fee")) or Decimal(0) for row in entry_fills), Decimal(0))
    vwap = notional / qty if qty else None
    fill_times = sorted(filter(None, (parse_dt(row.get("trade_at")) for row in entry_fills)))
    exchange_filled_times = sorted(
        runtime_by_order.get(order_id, {}).get("exchange_filled")
        for order_id in order_ids
        if runtime_by_order.get(order_id, {}).get("exchange_filled") is not None
    )
    candidate_runtime = runtime_by_candidate.get(candidate_id, {})
    states = Counter(row.get("state", "") for row in order_rows)
    final_state = order_rows[-1].get("state", "") if order_rows else ""
    return {
        "has_intent": bool(intent_rows),
        "has_order": bool(order_rows),
        "order_count": len(order_rows),
        "order_ids": sorted(order_ids - {""}),
        "order_type": order_rows[-1].get("order_type", "") if order_rows else "",
        "order_state": final_state,
        "order_states": dict(states),
        "order_created_at": order_rows[0].get("created_at", "") if order_rows else "",
        "actual_fill_count": len(entry_fills),
        "actual_fill_qty": qty,
        "actual_fill_vwap": vwap,
        "actual_fill_fee": fee,
        "actual_first_fill_at": fill_times[0] if fill_times else None,
        "actual_last_fill_at": fill_times[-1] if fill_times else None,
        "actual_exchange_filled_at": exchange_filled_times[0] if exchange_filled_times else (fill_times[0] if fill_times else None),
        "actual_candidate_accepted_at": candidate_runtime.get("candidate_accepted"),
        "actual_intent_saved_at": candidate_runtime.get("intent_saved"),
    }


def future_state_window(
    states_by_symbol: dict[str, list[dict[str, Any]]],
    symbol: str,
    signal_at: datetime,
    expires_at: datetime,
) -> list[dict[str, Any]]:
    # A state whose bucket ends at the signal was used to produce the signal;
    # the order cannot have participated in it.  Keep only later buckets.
    return [
        state
        for state in states_by_symbol.get(symbol, [])
        if state["bucket_end"] > signal_at and state["bucket_start"] < expires_at
    ]


def state_at(states_by_symbol: dict[str, list[dict[str, Any]]], symbol: str, observed_at: datetime) -> dict[str, Any] | None:
    """Return the 15-second aggregate containing observed_at, if available."""
    states = states_by_symbol.get(symbol, [])
    for state in states:
        if state["bucket_start"] <= observed_at < state["bucket_end"]:
            return state
    return None


def evaluate_at_actual_execution(
    side: str,
    limit_price: Decimal | None,
    execution_at: datetime | None,
    expires_at: datetime,
    states_by_symbol: dict[str, list[dict[str, Any]]],
    symbol: str,
) -> dict[str, Any]:
    """Evaluate the same signal-price limit after the observed live delay."""
    result: dict[str, Any] = {
        "limit_price": limit_price,
        "execution_quote": None,
        "execution_at": execution_at,
        "expired_before_exchange": False,
        "classification": "not_actual_filled",
        "immediate_marketable": False,
        "quote_touch": False,
        "trade_touch": False,
        "touch_at": None,
        "touch_source": "",
        "proxy_price": None,
        "min_observed_quote": None,
        "min_or_max_trade": None,
    }
    if execution_at is None or limit_price is None:
        return result
    if execution_at >= expires_at:
        result["expired_before_exchange"] = True
        result["classification"] = "expired_before_exchange"
        return result
    current_state = state_at(states_by_symbol, symbol, execution_at)
    is_long = side.lower() in {"long", "buy"}
    current_quote = None
    if current_state is not None:
        current_quote = current_state["last_ask"] if is_long else current_state["last_bid"]
    result["execution_quote"] = current_quote
    window = future_state_window(states_by_symbol, symbol, execution_at, expires_at)
    evaluated = evaluate_limit(side, limit_price, current_quote, window)
    result.update(evaluated)
    result["execution_at"] = execution_at
    return result


def evaluate_limit(
    side: str,
    limit_price: Decimal | None,
    signal_quote: Decimal | None,
    states: list[dict[str, Any]],
) -> dict[str, Any]:
    is_long = side.lower() in {"long", "buy"}
    result: dict[str, Any] = {
        "limit_price": limit_price,
        "immediate_marketable": False,
        "quote_touch": False,
        "trade_touch": False,
        "classification": "missing_limit_or_quote",
        "touch_at": None,
        "touch_source": "",
        "proxy_price": None,
        "min_observed_quote": None,
        "min_or_max_trade": None,
    }
    if limit_price is None:
        return result

    if signal_quote is not None and ((is_long and limit_price >= signal_quote) or (not is_long and limit_price <= signal_quote)):
        result.update(
            immediate_marketable=True,
            quote_touch=True,
            classification="immediate_marketable",
            touch_at=None,
            touch_source="signal_best_ask" if is_long else "signal_best_bid",
            proxy_price=signal_quote,
        )
        # Still collect post-signal extrema for diagnostics.

    quote_values: list[Decimal] = []
    trade_values: list[Decimal] = []
    quote_touch_state: dict[str, Any] | None = None
    trade_touch_state: dict[str, Any] | None = None
    for state in states:
        quote = state["last_ask"] if is_long else state["last_bid"]
        trade = state["low"] if is_long else state["high"]
        if quote is not None:
            quote_values.append(quote)
            touched = quote <= limit_price if is_long else quote >= limit_price
            if touched and quote_touch_state is None:
                quote_touch_state = state
        if trade is not None:
            trade_values.append(trade)
            touched = trade <= limit_price if is_long else trade >= limit_price
            if touched and trade_touch_state is None:
                trade_touch_state = state

    if quote_values:
        result["min_observed_quote"] = min(quote_values) if is_long else max(quote_values)
    if trade_values:
        result["min_or_max_trade"] = min(trade_values) if is_long else max(trade_values)
    if quote_touch_state is not None:
        result["quote_touch"] = True
        if not result["immediate_marketable"]:
            result.update(
                classification="quote_touch",
                touch_at=quote_touch_state["bucket_end"],
                touch_source="15s_last_ask" if is_long else "15s_last_bid",
                proxy_price=limit_price,
            )
    if trade_touch_state is not None:
        result["trade_touch"] = True
        if not result["immediate_marketable"] and not result["quote_touch"]:
            result.update(
                classification="trade_touch_only",
                touch_at=trade_touch_state["bucket_end"],
                touch_source="15s_low" if is_long else "15s_high",
                proxy_price=limit_price,
            )
    if result["immediate_marketable"]:
        return result
    if result["quote_touch"]:
        return result
    if result["trade_touch"]:
        return result
    if not states:
        result["classification"] = "no_post_signal_state"
    else:
        result["classification"] = "no_observed_touch"
    return result


def comparison_delta(side: str, actual: Decimal | None, hypothetical: Decimal | None, qty: Decimal) -> tuple[Decimal | None, Decimal | None]:
    if actual is None or hypothetical is None or not qty:
        return None, None
    is_long = side.lower() in {"long", "buy"}
    # Positive means the hypothetical entry improves gross PnL for the same
    # quantity: lower entry for a long, higher entry for a short.
    price_delta = actual - hypothetical if is_long else hypothetical - actual
    return price_delta, price_delta * qty


def build_signal_rows(
    signal_rows: Iterable[dict[str, str]],
    states_by_symbol: dict[str, list[dict[str, Any]]],
    intents_by_candidate: dict[str, list[dict[str, str]]],
    orders_by_intent: dict[str, list[dict[str, str]]],
    fills_by_order: dict[str, list[dict[str, str]]],
    runtime_by_order: dict[str, dict[str, datetime]],
    runtime_by_candidate: dict[str, dict[str, datetime]],
    execution_states_by_symbol: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for signal in signal_rows:
        if signal.get("signal_kind") != "strategy_signal":
            continue
        market = json_object(signal.get("market_context"))
        reference = json_object(signal.get("reference_prices"))
        signal_features = json_object(signal.get("features"))
        context = json_object(signal.get("candidate_context"))
        candidates = context.get("candidates") or []
        if not isinstance(candidates, list):
            candidates = []
        for candidate in candidates:
            if not isinstance(candidate, dict) or truthy(candidate.get("reduce_only")):
                continue
            candidate_id = str(candidate.get("candidate_id") or signal.get("candidate_id") or "")
            signal_id = signal.get("signal_id", "")
            if not candidate_id or (signal_id, candidate_id) in seen:
                continue
            seen.add((signal_id, candidate_id))
            features = candidate.get("features") if isinstance(candidate.get("features"), dict) else signal_features
            signal_at = parse_dt(candidate.get("created_at")) or parse_dt(signal.get("detected_at"))
            expires_at = parse_dt(candidate.get("expires_at"))
            if signal_at is None:
                continue
            if expires_at is None:
                expires_at = signal_at + timedelta(seconds=60)
            side = str(candidate.get("side") or signal.get("side") or "long")
            symbol = signal.get("symbol", "")
            signal_close = decimal(market.get("close_price")) or decimal(features.get("impulse_end_price"))
            signal_mid = decimal(reference.get("midpoint")) or decimal(market.get("midpoint"))
            signal_ask_or_bid = decimal(market.get("last_ask_price")) if side.lower() in {"long", "buy"} else decimal(market.get("last_bid_price"))
            window = future_state_window(states_by_symbol, symbol, signal_at, expires_at)
            actual = actual_for_candidate(
                candidate_id,
                intents_by_candidate,
                orders_by_intent,
                fills_by_order,
                runtime_by_order,
                runtime_by_candidate,
            )
            close_eval = evaluate_limit(side, signal_close, signal_ask_or_bid, window)
            mid_eval = evaluate_limit(side, signal_mid, signal_ask_or_bid, window)
            quote_eval = evaluate_limit(side, signal_ask_or_bid, signal_ask_or_bid, window)
            model_evals = {"close": close_eval, "midpoint": mid_eval, "signal_quote": quote_eval}

            row: dict[str, Any] = {
                "signal_id": signal_id,
                "candidate_id": candidate_id,
                "symbol": symbol,
                "side": side,
                "signal_at": signal_at,
                "expires_at": expires_at,
                "signal_price": signal_close,
                "signal_midpoint": signal_mid,
                "signal_best_ask_or_bid": signal_ask_or_bid,
                "quote_volume_24h": decimal(signal.get("quote_volume_24h")),
                "quote_volume_24h_age_ms": decimal(signal.get("quote_volume_24h_age_ms")),
                "state_count": len(window),
                "state_quote_count": sum(1 for state in window if (state["last_ask"] if side.lower() in {"long", "buy"} else state["last_bid"]) is not None),
                "state_complete_count": sum(1 for state in window if state["data_complete"]),
                **actual,
            }
            actual_execution_at = actual["actual_exchange_filled_at"]
            row["actual_exchange_latency_seconds"] = (
                Decimal(str((actual_execution_at - signal_at).total_seconds()))
                if actual_execution_at is not None
                else None
            )
            for model_name, evaluation in model_evals.items():
                prefix = f"{model_name}_"
                for key in ("limit_price", "immediate_marketable", "quote_touch", "trade_touch", "classification", "touch_at", "touch_source", "proxy_price", "min_observed_quote", "min_or_max_trade"):
                    row[prefix + key] = evaluation.get(key)
                strict_fill = evaluation["classification"] in {"immediate_marketable", "quote_touch"}
                row[prefix + "strict_quote_fill_proxy"] = strict_fill
                row[prefix + "upper_bound_fill_proxy"] = evaluation["trade_touch"] or evaluation["quote_touch"] or evaluation["immediate_marketable"]
                row[prefix + "actual_price_delta"] = None
                row[prefix + "gross_pnl_delta"] = None
                row[prefix + "entry_improvement_bps"] = None
                row[prefix + "hypothetical_filled_price"] = evaluation["proxy_price"] if strict_fill else None
                if strict_fill and actual["actual_fill_vwap"] is not None:
                    price_delta, pnl_delta = comparison_delta(side, actual["actual_fill_vwap"], evaluation["proxy_price"], actual["actual_fill_qty"])
                    row[prefix + "actual_price_delta"] = price_delta
                    row[prefix + "gross_pnl_delta"] = pnl_delta
                    if actual["actual_fill_vwap"]:
                        row[prefix + "entry_improvement_bps"] = (price_delta / actual["actual_fill_vwap"]) * Decimal(10000) if price_delta is not None else None

                delayed = evaluate_at_actual_execution(
                    side,
                    evaluation.get("limit_price"),
                    actual_execution_at,
                    expires_at,
                    execution_states_by_symbol,
                    symbol,
                )
                delayed_prefix = f"latency_{model_name}_"
                for key in (
                    "limit_price",
                    "execution_quote",
                    "execution_at",
                    "expired_before_exchange",
                    "immediate_marketable",
                    "quote_touch",
                    "trade_touch",
                    "classification",
                    "touch_at",
                    "touch_source",
                    "proxy_price",
                    "min_observed_quote",
                    "min_or_max_trade",
                ):
                    row[delayed_prefix + key] = delayed.get(key)
                delayed_strict = delayed["classification"] in {"immediate_marketable", "quote_touch"}
                row[delayed_prefix + "strict_quote_fill_proxy"] = delayed_strict
                row[delayed_prefix + "upper_bound_fill_proxy"] = delayed["trade_touch"] or delayed["quote_touch"] or delayed["immediate_marketable"]
                row[delayed_prefix + "hypothetical_filled_price"] = delayed["proxy_price"] if delayed_strict else None
                row[delayed_prefix + "gross_pnl_delta"] = None
                row[delayed_prefix + "entry_improvement_bps"] = None
                if delayed_strict and actual["actual_fill_vwap"] is not None:
                    price_delta, pnl_delta = comparison_delta(side, actual["actual_fill_vwap"], delayed["proxy_price"], actual["actual_fill_qty"])
                    row[delayed_prefix + "gross_pnl_delta"] = pnl_delta
                    if price_delta is not None and actual["actual_fill_vwap"]:
                        row[delayed_prefix + "entry_improvement_bps"] = (price_delta / actual["actual_fill_vwap"]) * Decimal(10000)
            output.append(row)
    output.sort(key=lambda row: row["signal_at"])
    return output


def csv_value(value: Any) -> str:
    if isinstance(value, datetime):
        return iso(value)
    if isinstance(value, Decimal):
        return as_number(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def write_signal_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("\n", encoding="utf-8")
        return
    fields = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: csv_value(row.get(field)) for field in fields})


def model_summary(rows: list[dict[str, Any]], model: str) -> dict[str, Any]:
    total = len(rows)
    attempted = sum(1 for row in rows if row["has_order"])
    actual_filled = sum(1 for row in rows if row["actual_fill_qty"])
    strict = sum(1 for row in rows if row[f"{model}_strict_quote_fill_proxy"])
    upper = sum(1 for row in rows if row[f"{model}_upper_bound_fill_proxy"])
    immediate = sum(1 for row in rows if row[f"{model}_immediate_marketable"])
    quote_touch = sum(1 for row in rows if row[f"{model}_quote_touch"])
    trade_touch = sum(1 for row in rows if row[f"{model}_trade_touch"])
    no_touch = sum(1 for row in rows if row[f"{model}_classification"] in {"no_observed_touch", "no_post_signal_state"})
    actual_filled_and_hypo_filled = [
        row[f"{model}_gross_pnl_delta"] for row in rows if row[f"{model}_gross_pnl_delta"] is not None
    ]
    positive = [value for value in actual_filled_and_hypo_filled if value > 0]
    negative = [value for value in actual_filled_and_hypo_filled if value < 0]
    deltas_median = statistics.median(actual_filled_and_hypo_filled) if actual_filled_and_hypo_filled else None
    latency_rows = [row for row in rows if row.get("actual_fill_qty")]
    latency_evaluable = [
        row for row in latency_rows if row.get(f"latency_{model}_classification") not in {None, "not_actual_filled"}
    ]
    latency_strict = [row for row in latency_rows if row.get(f"latency_{model}_strict_quote_fill_proxy")]
    latency_expired = [row for row in latency_rows if row.get(f"latency_{model}_classification") == "expired_before_exchange"]
    latency_deltas = [row[f"latency_{model}_gross_pnl_delta"] for row in latency_rows if row.get(f"latency_{model}_gross_pnl_delta") is not None]
    return {
        "model": model,
        "definition": {
            "close": "signal market_context.close_price, fallback candidate impulse_end_price",
            "midpoint": "signal midpoint",
            "signal_quote": "signal best ask for long / best bid for short",
        }[model],
        "candidate_count": total,
        "with_exchange_order_count": attempted,
        "actual_filled_count": actual_filled,
        "actual_filled_rate_all_candidates": actual_filled / total if total else None,
        "immediate_marketable_count": immediate,
        "immediate_marketable_rate": immediate / total if total else None,
        "quote_touch_count_including_immediate": quote_touch,
        "quote_touch_rate": quote_touch / total if total else None,
        "strict_quote_fill_proxy_count": strict,
        "strict_quote_fill_proxy_rate": strict / total if total else None,
        "trade_touch_count_including_quote": trade_touch,
        "upper_bound_fill_proxy_count": upper,
        "upper_bound_fill_proxy_rate": upper / total if total else None,
        "no_observed_touch_count": no_touch,
        "no_observed_touch_rate": no_touch / total if total else None,
        "actual_filled_and_hypothetical_strict_filled_count": len(actual_filled_and_hypo_filled),
        "actual_filled_and_hypothetical_strict_filled_positive_count": len(positive),
        "actual_filled_and_hypothetical_strict_filled_negative_count": len(negative),
        "same_quantity_gross_pnl_delta_usdt_sum": sum(actual_filled_and_hypo_filled, Decimal(0)),
        "same_quantity_gross_pnl_delta_usdt_avg": (sum(actual_filled_and_hypo_filled, Decimal(0)) / len(actual_filled_and_hypo_filled)) if actual_filled_and_hypo_filled else None,
        "same_quantity_gross_pnl_delta_usdt_median": deltas_median,
        "actual_latency_sample_count": len(latency_rows),
        "actual_latency_evaluable_count": len(latency_evaluable),
        "actual_latency_expired_before_exchange_count": len(latency_expired),
        "actual_latency_strict_quote_fill_proxy_count": len(latency_strict),
        "actual_latency_strict_quote_fill_proxy_rate": len(latency_strict) / len(latency_rows) if latency_rows else None,
        "actual_latency_gross_pnl_delta_usdt_sum": sum(latency_deltas, Decimal(0)),
        "actual_latency_gross_pnl_delta_usdt_avg": (sum(latency_deltas, Decimal(0)) / len(latency_deltas)) if latency_deltas else None,
    }


def round_summary(value: Any) -> Any:
    if isinstance(value, Decimal):
        return as_number(value)
    if isinstance(value, datetime):
        return iso(value)
    if isinstance(value, dict):
        return {key: round_summary(item) for key, item in value.items()}
    if isinstance(value, list):
        return [round_summary(item) for item in value]
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def write_svg(path: Path, summaries: list[dict[str, Any]]) -> None:
    width, height = 1100, 560
    left, right, top, bottom = 90, 40, 70, 120
    chart_w, chart_h = width - left - right, height - top - bottom
    colors = {"actual": "#475569", "immediate": "#2563eb", "quote": "#059669", "upper": "#f59e0b"}
    labels = {"actual": "实盘实际已成交", "immediate": "立即可成交", "quote": "15秒盘口触价", "upper": "价格触价上限"}
    model_labels = {"close": "信号收盘价限价", "midpoint": "信号中间价限价", "signal_quote": "信号卖一限价"}
    series = ["actual", "immediate", "quote", "upper"]
    max_value = 1.0
    groups = len(summaries)
    bar_w = 34
    group_w = chart_w / max(groups, 1)
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#f8fafc"/>',
        '<style>text{font-family:-apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei",sans-serif;fill:#0f172a} .small{font-size:14px} .title{font-size:22px;font-weight:700} .axis{stroke:#94a3b8;stroke-width:1}</style>',
        '<text x="55" y="36" class="title">限价入场成交率代理对比（信号后 60 秒）</text>',
        f'<line x1="{left}" y1="{top + chart_h}" x2="{width-right}" y2="{top + chart_h}" class="axis"/>',
    ]
    for tick in range(0, 6):
        value = tick / 5
        y = top + chart_h * (1 - value)
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{width-right}" y2="{y:.1f}" stroke="#e2e8f0"/>')
        parts.append(f'<text x="{left-12}" y="{y+5:.1f}" text-anchor="end" class="small">{value:.0%}</text>')
    for group_index, summary in enumerate(summaries):
        model = summary["model"]
        x0 = left + group_w * (group_index + 0.5) - (len(series) * bar_w) / 2
        values = {
            "actual": summary["actual_filled_rate_all_candidates"] or 0,
            "immediate": summary["immediate_marketable_rate"] or 0,
            "quote": summary["strict_quote_fill_proxy_rate"] or 0,
            "upper": summary["upper_bound_fill_proxy_rate"] or 0,
        }
        for series_index, series_name in enumerate(series):
            value = max(0.0, min(max_value, values[series_name]))
            x = x0 + series_index * bar_w
            bar_h = chart_h * value
            y = top + chart_h - bar_h
            parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="28" height="{bar_h:.1f}" rx="3" fill="{colors[series_name]}"/>')
            parts.append(f'<text x="{x+14:.1f}" y="{max(y-6, top+12):.1f}" text-anchor="middle" class="small">{value:.0%}</text>')
        label_x = left + group_w * (group_index + 0.5)
        parts.append(f'<text x="{label_x:.1f}" y="{top+chart_h+30}" text-anchor="middle" class="small">{html.escape(model_labels[model])}</text>')
    legend_x = left
    legend_y = height - 35
    for index, series_name in enumerate(series):
        x = legend_x + index * 225
        parts.append(f'<rect x="{x}" y="{legend_y-12}" width="14" height="14" fill="{colors[series_name]}"/>')
        parts.append(f'<text x="{x+21}" y="{legend_y}" class="small">{html.escape(labels[series_name])}</text>')
    parts.append("</svg>")
    path.write_text("\n".join(parts), encoding="utf-8")


def write_report(path: Path, rows: list[dict[str, Any]], summaries: list[dict[str, Any]], data_dir: Path) -> None:
    first = min((row["signal_at"] for row in rows), default=None)
    last = max((row["signal_at"] for row in rows), default=None)
    state_count = sum(row["state_count"] for row in rows)
    quote_count = sum(row["state_quote_count"] for row in rows)
    complete_count = sum(row["state_complete_count"] for row in rows)
    actual_filled = sum(1 for row in rows if row["actual_fill_qty"])
    latency_values = sorted(
        float(row["actual_exchange_latency_seconds"])
        for row in rows
        if row.get("actual_exchange_latency_seconds") is not None
    )
    latency_p50 = statistics.median(latency_values) if latency_values else None
    latency_p90 = latency_values[max(0, math.ceil(len(latency_values) * 0.90) - 1)] if latency_values else None
    latency_max = max(latency_values) if latency_values else None
    lines = [
        "# 实盘信号价格限价入场反事实分析",
        "",
        f"- 数据目录：`{data_dir}`",
        f"- 信号时间（UTC）：{iso(first)} 至 {iso(last)}",
        f"- 可分析入场候选：{len(rows)} 条；实盘实际有成交：{actual_filled} 条",
        f"- 信号后 60 秒状态窗口：共 {state_count} 个 15 秒状态，其中含盘口报价 {quote_count} 个，完整状态 {complete_count} 个",
        "",
        "## 沿用实盘下单延迟的校正",
        "",
        f"实盘已成交样本的信号到交易所成交事件延迟：p50 {latency_p50:.3f} 秒、p90 {latency_p90:.3f} 秒、最大 {latency_max:.3f} 秒；其中超过候选 60 秒有效期的样本 {sum(1 for value in latency_values if value >= 60):} 条。这里使用 `exchange_filled` 运行事件作为实际下单/成交时刻代理。",
        "",
        "下表的 `延迟校正盘口代理` 只在实盘实际已成交样本上统计：把限价价格保持为信号价格，但把挂单时刻推迟到观察到的交易所成交事件；若已经超过 60 秒有效期，记为 `expired_before_exchange`，不再算作可成交。",
        "延迟校正入口价差总额同样只对延迟后仍有严格盘口触价的样本计算；它是同数量、同后续路径下的入口毛利估算，不是把整套策略重跑后的最终权益差。",
        "",
        "| 模型 | 实盘已成交样本 | 超过有效期 | 延迟校正后 15 秒盘口触价代理 | 延迟校正入口价差总额（USDT） |",
        "|---|---:|---:|---:|---:|",
    ]
    for summary in summaries:
        lines.append(
            f"| {summary['definition']} | {summary['actual_latency_sample_count']} | {summary['actual_latency_expired_before_exchange_count']} | "
            f"{pct(summary['actual_latency_strict_quote_fill_proxy_rate'])} | "
            f"{csv_value(summary['actual_latency_gross_pnl_delta_usdt_sum']) or 'n/a'} |"
        )
    lines += [
        "",
        "## 结论口径",
        "",
        "表中的“实盘实际成交率”是现有市价执行链在 435 个候选中的覆盖率（138/435），不是限价单已经验证过的成交率；另外 297 个候选没有形成实盘交易所订单，不能直接当成限价单未成交。",
        "",
        "主模型是把买入限价设为信号 `close_price`（缺失时用策略 `impulse_end_price`）。如果该价格在信号瞬间已经高于等于卖一，普通限价单会立即吃单，不能把成交价当成限价价位；报告按信号卖一作为立即成交代理价。",
        "",
        "`15秒盘口触价` 是信号后的 15 秒状态中，卖一曾经小于等于限价（多头）的比例；它比仅看 K 线更严格，但仍不能证明排队成交。`价格触价上限` 是 15 秒 low 曾经触及限价的比例，只能作为成交率上界。",
        "",
        "因此，本报告可以严谨回答“在现有数据可观测范围内，有多少信号具备成交条件”，不能回答交易所队列级的精确成交率。服务器没有保存历史逐笔 bookTicker/depth，且即便有 best bid/ask，也还缺少下单时的队列位置和逐笔成交消耗。",
        "",
        "## 各限价定义的结果",
        "",
        "| 模型 | 候选数 | 实盘实际成交率 | 立即可成交 | 15秒盘口触价代理 | 价格触价上限 | 未观察到触价 | 同时有实盘/代理成交的入场价差总额（USDT） |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for summary in summaries:
        lines.append(
            f"| {summary['definition']} | {summary['candidate_count']} | {pct(summary['actual_filled_rate_all_candidates'])} | "
            f"{pct(summary['immediate_marketable_rate'])} | {pct(summary['strict_quote_fill_proxy_rate'])} | "
            f"{pct(summary['upper_bound_fill_proxy_rate'])} | {pct(summary['no_observed_touch_rate'])} | "
            f"{csv_value(summary['same_quantity_gross_pnl_delta_usdt_sum']) or 'n/a'} |"
        )
    lines += [
        "",
        "其中入场价差总额只对“实盘已成交且限价代理也有严格盘口触价”的同一批信号计算，正数表示限价代理的入场价更有利、按实盘成交数量估算可改善的毛利；不含手续费、未成交后的后续持仓路径变化，也不等于最终权益差额。",
        "重要：这里的 88 条正差异、30 条负差异不是同一时刻把市价单替换成限价单的公平 A/B。它比较的是信号时刻的限价代理和日志中实际较晚的市价成交；其中 30 条负差异里有 19 条信号时已具备立即成交条件，若两种订单同一时刻发送且都有足够深度，普通可成交限价单通常与市价单吃到同一侧盘口，负差异主要来自实际市价单后来成交时价格已经下跌。",
        "",
        "## 如何解读",
        "",
        "1. `信号收盘价限价` 是最贴近问题描述的主结果。若立即可成交比例很高，挂限价并不会自动带来更低的成交价，只是给成交设置价格上限；若价格快速上冲，可能变成未成交。",
        "2. 盘口触价但没有成交的情况无法从当前数据区分，因为没有逐笔报价和队列消耗记录；所以实际成交率应落在“严格盘口触价代理”和“价格触价上限”之间，且还要扣除撤单/过期/部分成交等因素。",
        "3. 对实盘已成交样本，入场价差估算只说明“同样数量、同样后续退出”的入口改善，不代表策略改成限价后最终收益一定增加；漏掉的交易会改变持仓、退出和后续信号。",
        "4. 如果要得到真正的成交率，应从现在开始持久化信号后的逐笔 `bookTicker`，最好同时记录盘口深度增量、下单发送/交易所确认时间、订单簿队列估计和逐笔成交；再用真实限价单或撮合回放校准。",
        "",
        "## 产物",
        "",
        "- `limit_entry_signal_analysis.csv`：逐信号、逐模型的触价、实盘成交和入场价差；",
        "- `limit_entry_summary.json`：机器可读汇总；",
        "- `limit_entry_fill_rates.svg`：成交率代理对比图。",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--actual-state-file", type=Path)
    args = parser.parse_args()
    data_dir: Path = args.data_dir
    output_dir: Path = args.output_dir or data_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    signals = load_csv(data_dir / "live_strategy_signals.csv.gz")
    intents = load_csv(data_dir / "order_intents.csv.gz")
    orders = load_csv(data_dir / "exchange_orders.csv.gz")
    fills = load_csv(data_dir / "account_fill_events.csv.gz")
    states = load_csv(data_dir / "runtime_market_states_15s.csv.gz")
    actual_state_path = args.actual_state_file or (data_dir / "runtime_market_states_actual_fill_windows.csv.gz")
    actual_states = load_csv(actual_state_path) if actual_state_path.exists() else []
    runtime_events = load_csv(data_dir / "strategy_runtime_events.csv.gz")
    states_by_symbol = load_states(states)
    execution_states_by_symbol = merge_states(states_by_symbol, load_states(actual_states))
    intents_by_candidate, orders_by_intent, fills_by_order = load_orders(intents, orders, fills)
    runtime_by_order, runtime_by_candidate = load_runtime_events(runtime_events)
    rows = build_signal_rows(
        signals,
        states_by_symbol,
        intents_by_candidate,
        orders_by_intent,
        fills_by_order,
        runtime_by_order,
        runtime_by_candidate,
        execution_states_by_symbol,
    )

    models = ["close", "midpoint", "signal_quote"]
    summaries = [model_summary(rows, model) for model in models]
    summary = {
        "generated_at": datetime.now(UTC),
        "data_dir": str(data_dir),
        "source_row_counts": {
            "live_strategy_signals": len(signals),
            "order_intents": len(intents),
            "exchange_orders": len(orders),
            "account_fill_events": len(fills),
            "runtime_market_states_15s": len(states),
            "runtime_market_states_actual_fill_windows": len(actual_states),
            "strategy_runtime_events": len(runtime_events),
        },
        "candidate_rows": len(rows),
        "state_window": {
            "total_states": sum(row["state_count"] for row in rows),
            "states_with_quote": sum(row["state_quote_count"] for row in rows),
            "complete_states": sum(row["state_complete_count"] for row in rows),
            "signals_without_post_signal_state": sum(1 for row in rows if row["state_count"] == 0),
        },
        "models": summaries,
        "classification_counts": {
            model: dict(Counter(row[f"{model}_classification"] for row in rows)) for model in models
        },
    }
    summary = round_summary(summary)
    write_signal_csv(output_dir / "limit_entry_signal_analysis.csv", rows)
    (output_dir / "limit_entry_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    write_report(output_dir / "limit_entry_analysis.md", rows, summaries, data_dir)
    write_svg(output_dir / "limit_entry_fill_rates.svg", summaries)

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
