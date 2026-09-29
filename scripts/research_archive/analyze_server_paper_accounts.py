from __future__ import annotations

import asyncio
import itertools
import json
import math
import os
import statistics
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


ACCOUNT_ORDER = [
    "paper-account-01-compression-original-fixed-v1",
    "paper-account-02-compression-original-candle15m-v1",
    "paper-account-02-orderflow-v1",
    "paper-account-05-orderflow-candle15m-v1",
    "paper-account-07-orderflow-candle45m-v1",
    "paper-account-03-liquidation-v1",
    "paper-account-06-liquidation-candle15m-v1",
    "paper-account-08-liquidation-candle2confirm-v1",
]

ACCOUNT_LABELS = {
    ACCOUNT_ORDER[0]: "01 压缩突破｜fixed",
    ACCOUNT_ORDER[1]: "02 压缩突破｜15m 反向收线",
    ACCOUNT_ORDER[2]: "02 订单流｜fixed",
    ACCOUNT_ORDER[3]: "05 订单流｜15m 反向收线",
    ACCOUNT_ORDER[4]: "07 订单流｜45m 后反向收线",
    ACCOUNT_ORDER[5]: "03 清算级联｜fixed",
    ACCOUNT_ORDER[6]: "06 清算级联｜15m 反向收线",
    ACCOUNT_ORDER[7]: "08 清算级联｜连续 2 根反向收线",
}

REFERENCE_RUNS = {
    "compression_breakout": ACCOUNT_ORDER[0],
    "orderflow_impulse": ACCOUNT_ORDER[2],
    "liquidation_cascade": ACCOUNT_ORDER[5],
}

VARIANT_RUNS = {
    "compression_breakout": ACCOUNT_ORDER[:2],
    "orderflow_impulse": ACCOUNT_ORDER[2:5],
    "liquidation_cascade": ACCOUNT_ORDER[5:],
}

LOCAL_TZ = ZoneInfo("Asia/Shanghai")


def float_value(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def iso(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def parse_json(value: object) -> object:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def flatten_numeric(value: object, prefix: str = "") -> dict[str, float]:
    value = parse_json(value)
    if isinstance(value, dict):
        flattened: dict[str, float] = {}
        for key, child in value.items():
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            flattened.update(flatten_numeric(child, child_prefix))
        return flattened
    if isinstance(value, (list, tuple)):
        return {}
    number = float_value(value)
    return {prefix: number} if prefix and number is not None else {}


def median_or_none(values: list[float]) -> float | None:
    return statistics.median(values) if values else None


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * fraction
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    weight = index - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def profit_factor(values: list[float]) -> float | None:
    gains = sum(value for value in values if value > 0)
    losses = -sum(value for value in values if value < 0)
    if losses == 0:
        return None if gains == 0 else float("inf")
    return gains / losses


def rank_values(values: list[float]) -> list[float]:
    order = sorted(range(len(values)), key=lambda index: values[index])
    ranks = [0.0] * len(values)
    cursor = 0
    while cursor < len(order):
        end = cursor + 1
        while end < len(order) and values[order[end]] == values[order[cursor]]:
            end += 1
        rank = (cursor + end - 1) / 2 + 1
        for position in order[cursor:end]:
            ranks[position] = rank
        cursor = end
    return ranks


def spearman_correlation(x_values: list[float], y_values: list[float]) -> float | None:
    if len(x_values) < 3 or len(x_values) != len(y_values):
        return None
    x_ranks = rank_values(x_values)
    y_ranks = rank_values(y_values)
    x_mean = statistics.mean(x_ranks)
    y_mean = statistics.mean(y_ranks)
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_ranks, y_ranks))
    x_scale = math.sqrt(sum((x - x_mean) ** 2 for x in x_ranks))
    y_scale = math.sqrt(sum((y - y_mean) ** 2 for y in y_ranks))
    if x_scale == 0 or y_scale == 0:
        return None
    return numerator / (x_scale * y_scale)


def max_drawdown(points: list[dict[str, object]]) -> dict[str, object]:
    ordered = sorted(points, key=lambda point: point["observed_at"])
    peak_value = None
    peak_at = None
    worst = {"amount": 0.0, "pct": 0.0, "from": None, "to": None, "duration_minutes": 0.0}
    for point in ordered:
        equity = point["equity"]
        observed_at = point["observed_at"]
        if peak_value is None or equity > peak_value:
            peak_value = equity
            peak_at = observed_at
        if peak_value is None or peak_value <= 0:
            continue
        drawdown_amount = equity - peak_value
        drawdown_pct = drawdown_amount / peak_value
        if drawdown_pct < worst["pct"]:
            worst = {
                "amount": drawdown_amount,
                "pct": drawdown_pct,
                "from": iso(peak_at),
                "to": iso(observed_at),
                "duration_minutes": (observed_at - peak_at).total_seconds() / 60,
            }
    return worst


def exposure_peak(records: list[dict[str, object]]) -> dict[str, object]:
    events: list[tuple[datetime, int, float]] = []
    for record in records:
        opened_at = record["opened_at"]
        entry_notional = record["entry_notional"] or 0.0
        events.append((opened_at, 1, entry_notional))
        if record["closed_at"] is not None:
            events.append((record["closed_at"], -1, -entry_notional))
    events.sort(key=lambda event: (event[0], event[1]))
    open_count = 0
    notional = 0.0
    peak_count = 0
    peak_notional = 0.0
    peak_at = None
    for observed_at, count_delta, notional_delta in events:
        open_count += count_delta
        notional += notional_delta
        if open_count > peak_count or notional > peak_notional:
            peak_count = max(peak_count, open_count)
            if notional >= peak_notional:
                peak_notional = notional
                peak_at = observed_at
    return {
        "peak_open_positions": peak_count,
        "peak_entry_notional": peak_notional,
        "peak_at": iso(peak_at),
        "peak_notional_to_initial_balance": peak_notional / 1000 if peak_notional else 0,
    }


def basic_trade_stats(records: list[dict[str, object]]) -> dict[str, object]:
    closed = [record for record in records if record["closed"]]
    pnls = [record["pnl"] for record in closed if record["pnl"] is not None]
    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value < 0]
    durations = [record["duration_minutes"] for record in closed if record["duration_minutes"] is not None]
    fees = sum(record["entry_fee"] + record["exit_fee"] for record in records)
    gross = sum(record["gross_pnl"] for record in closed if record["gross_pnl"] is not None)
    return {
        "trades": len(records),
        "closed_trades": len(closed),
        "open_trades": len(records) - len(closed),
        "net_realized_pnl": sum(pnls),
        "gross_realized_pnl": gross,
        "total_fees": fees,
        "fee_to_gross_pct": fees / gross * 100 if gross > 0 else None,
        "win_rate": len(wins) / len(pnls) if pnls else None,
        "profit_factor": profit_factor(pnls),
        "expectancy": statistics.mean(pnls) if pnls else None,
        "median_pnl": median_or_none(pnls),
        "average_win": statistics.mean(wins) if wins else None,
        "average_loss": statistics.mean(losses) if losses else None,
        "p90_loss": percentile(losses, 0.1),
        "p90_win": percentile(wins, 0.9),
        "average_duration_minutes": statistics.mean(durations) if durations else None,
        "median_duration_minutes": median_or_none(durations),
        "max_duration_minutes": max(durations) if durations else None,
        "close_reasons": dict(Counter(record["close_reason"] or "open" for record in records)),
        "side": grouped_trade_stats(records, "side"),
        "top_wins": sorted_trade_rows(closed, reverse=True),
        "worst_losses": sorted_trade_rows(closed, reverse=False),
        "tail_concentration": tail_concentration(pnls),
    }


def grouped_trade_stats(records: list[dict[str, object]], field: str) -> dict[str, object]:
    groups: dict[str, list[dict[str, object]]] = defaultdict(list)
    for record in records:
        groups[str(record[field])].append(record)
    result = {}
    for key, group in sorted(groups.items()):
        stats = basic_trade_stats_without_groups(group)
        result[key] = stats
    return result


def basic_trade_stats_without_groups(records: list[dict[str, object]]) -> dict[str, object]:
    closed = [record for record in records if record["closed"] and record["pnl"] is not None]
    pnls = [record["pnl"] for record in closed]
    return {
        "trades": len(records),
        "closed_trades": len(closed),
        "net_pnl": sum(pnls),
        "win_rate": sum(value > 0 for value in pnls) / len(pnls) if pnls else None,
        "profit_factor": profit_factor(pnls),
        "average_pnl": statistics.mean(pnls) if pnls else None,
        "median_pnl": median_or_none(pnls),
    }


def sorted_trade_rows(records: list[dict[str, object]], reverse: bool) -> list[dict[str, object]]:
    rows = sorted(records, key=lambda record: record["pnl"] or 0.0, reverse=reverse)
    output = []
    for record in rows[:10]:
        output.append(
            {
                "symbol": record["symbol"],
                "side": record["side"],
                "pnl": record["pnl"],
                "return_pct": record["return_pct"],
                "gross_pnl": record["gross_pnl"],
                "duration_minutes": record["duration_minutes"],
                "close_reason": record["close_reason"],
                "opened_at": iso(record["opened_at"]),
                "closed_at": iso(record["closed_at"]),
                "signal_source_at": iso(record["signal_source_at"]),
            }
        )
    return output


def tail_concentration(pnls: list[float]) -> dict[str, object]:
    positive = sorted((value for value in pnls if value > 0), reverse=True)
    positive_total = sum(positive)
    return {
        "positive_pnl": positive_total,
        "top_1_share": sum(positive[:1]) / positive_total if positive_total else None,
        "top_3_share": sum(positive[:3]) / positive_total if positive_total else None,
        "top_5_share": sum(positive[:5]) / positive_total if positive_total else None,
        "net_without_top_1": sum(pnls) - sum(positive[:1]),
        "net_without_top_3": sum(pnls) - sum(positive[:3]),
        "net_without_top_5": sum(pnls) - sum(positive[:5]),
    }


def quantile_feature_bins(rows: list[tuple[float, float]]) -> list[dict[str, object]]:
    ordered = sorted(rows, key=lambda item: item[0])
    buckets: dict[int, list[tuple[float, float]]] = defaultdict(list)
    for index, pair in enumerate(ordered):
        buckets[min(4, index * 5 // len(ordered))].append(pair)
    output = []
    for bucket, values in sorted(buckets.items()):
        pnls = [pnl for _, pnl in values]
        output.append(
            {
                "bucket": bucket + 1,
                "count": len(values),
                "feature_min": min(value for value, _ in values),
                "feature_max": max(value for value, _ in values),
                "mean_pnl": statistics.mean(pnls),
                "median_pnl": median_or_none(pnls),
                "win_rate": sum(pnl > 0 for pnl in pnls) / len(pnls),
                "profit_factor": profit_factor(pnls),
            }
        )
    return output


def feature_mining(records: list[dict[str, object]]) -> list[dict[str, object]]:
    values_by_feature: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for record in records:
        if not record["closed"] or record["pnl"] is None:
            continue
        for feature, value in flatten_numeric(record["features"]).items():
            values_by_feature[feature].append((value, record["pnl"]))
    mined = []
    for feature, rows in values_by_feature.items():
        if len(rows) < 20:
            continue
        feature_values = [value for value, _ in rows]
        pnls = [pnl for _, pnl in rows]
        mined.append(
            {
                "feature": feature,
                "count": len(rows),
                "spearman_pnl": spearman_correlation(feature_values, pnls),
                "bins": quantile_feature_bins(rows),
            }
        )
    mined.sort(key=lambda row: abs(row["spearman_pnl"] or 0), reverse=True)
    return mined[:20]


def local_time_buckets(records: list[dict[str, object]]) -> dict[str, object]:
    groups: dict[str, list[dict[str, object]]] = defaultdict(list)
    for record in records:
        local = record["opened_at"].astimezone(LOCAL_TZ)
        groups[f"{local.hour:02d}:00"].append(record)
    return {key: basic_trade_stats_without_groups(value) for key, value in sorted(groups.items())}


def symbol_buckets(records: list[dict[str, object]]) -> list[dict[str, object]]:
    groups: dict[str, list[dict[str, object]]] = defaultdict(list)
    for record in records:
        groups[record["symbol"]].append(record)
    rows = []
    for symbol, group in groups.items():
        stats = basic_trade_stats_without_groups(group)
        rows.append({"symbol": symbol, **stats})
    rows.sort(key=lambda row: row["net_pnl"])
    return rows


def build_record(
    position: dict[str, object],
    signal_by_id: dict[str, dict[str, object]],
    fill_by_id: dict[str, dict[str, object]],
) -> dict[str, object]:
    signal = signal_by_id.get(position["signal_id"], {})
    fill = fill_by_id.get(position["entry_fill_id"], {})
    closed = position["status"] == "closed" or position["closed_at"] is not None
    realized = float_value(position["realized_pnl"])
    unrealized = float_value(position["unrealized_pnl"]) or 0.0
    pnl = realized if closed else unrealized
    entry_fee = float_value(position["entry_fee"]) or 0.0
    exit_fee = float_value(position["exit_fee"]) or 0.0
    gross_pnl = (pnl + entry_fee + exit_fee) if closed and pnl is not None else None
    opened_at = position["opened_at"]
    closed_at = position["closed_at"]
    duration_minutes = (
        (closed_at - opened_at).total_seconds() / 60
        if closed_at is not None
        else None
    )
    signal_source_at = signal.get("source_state_at") or signal.get("detected_at")
    return {
        "position_id": position["position_id"],
        "run_id": position["run_id"],
        "strategy_name": signal.get("strategy_name"),
        "signal_id": position["signal_id"],
        "symbol": position["symbol"],
        "side": position["side"],
        "status": position["status"],
        "closed": closed,
        "opened_at": opened_at,
        "closed_at": closed_at,
        "signal_source_at": signal_source_at,
        "entry_price": float_value(position["entry_price"]),
        "exit_price": float_value(position["exit_price"]),
        "entry_notional": float_value(position["entry_notional"]) or 0.0,
        "entry_fee": entry_fee,
        "exit_fee": exit_fee,
        "fill_spread": float_value(fill.get("spread")),
        "fill_cost_bps": float_value(fill.get("cost_bps")),
        "pnl": pnl,
        "gross_pnl": gross_pnl,
        "realized_pnl": realized,
        "unrealized_pnl": unrealized,
        "return_pct": float_value(position["return_pct"]),
        "duration_minutes": duration_minutes,
        "close_reason": position["close_reason"],
        "features": signal.get("features") or {},
    }


def pair_comparisons(records_by_run: dict[str, list[dict[str, object]]]) -> list[dict[str, object]]:
    by_key: dict[str, dict[tuple[object, ...], dict[str, object]]] = {}
    for run_id, records in records_by_run.items():
        for record in records:
            key = (
                record["strategy_name"],
                record["symbol"],
                record["side"],
                iso(record["signal_source_at"]),
            )
            by_key.setdefault(run_id, {})[key] = record
    output = []
    for strategy, run_ids in VARIANT_RUNS.items():
        for base_run, variant_run in itertools.combinations(run_ids, 2):
            base_map = by_key.get(base_run, {})
            variant_map = by_key.get(variant_run, {})
            common = sorted(set(base_map).intersection(variant_map), key=str)
            deltas = []
            both_closed = 0
            variant_better = 0
            for key in common:
                base = base_map[key]
                variant = variant_map[key]
                if base["closed"] and variant["closed"]:
                    both_closed += 1
                base_pnl = base["pnl"] or 0.0
                variant_pnl = variant["pnl"] or 0.0
                delta = variant_pnl - base_pnl
                deltas.append(delta)
                variant_better += delta > 0
            output.append(
                {
                    "strategy": strategy,
                    "base_run_id": base_run,
                    "base_label": ACCOUNT_LABELS.get(base_run, base_run),
                    "variant_run_id": variant_run,
                    "variant_label": ACCOUNT_LABELS.get(variant_run, variant_run),
                    "common_entries": len(common),
                    "both_closed": both_closed,
                    "total_pnl_delta": sum(deltas),
                    "mean_pnl_delta": statistics.mean(deltas) if deltas else None,
                    "median_pnl_delta": median_or_none(deltas),
                    "variant_better_rate": variant_better / len(deltas) if deltas else None,
                    "delta_p10": percentile(deltas, 0.1),
                    "delta_p90": percentile(deltas, 0.9),
                }
            )
    return output


def serialize_run(row: dict[str, object]) -> dict[str, object]:
    return {
        "run_id": row["run_id"],
        "label": ACCOUNT_LABELS.get(row["run_id"], row["run_id"]),
        "strategy_name": row["strategy_name"],
        "strategy_version": row["strategy_version"],
        "config_hash": row["config_hash"],
        "code_commit": row["code_commit"],
        "run_mode": row["run_mode"],
        "created_at": iso(row["created_at"]),
        "signal_count_db": row["signal_count"],
        "candidate_count_db": row["candidate_count"],
        "fill_count_db": row["fill_count"],
        "pending_candidate_count_db": row["pending_candidate_count"],
        "execution_config": row["execution_config"],
    }


async def fetch_all() -> dict[str, list[dict[str, object]]]:
    database_url = os.environ["CML_DATABASE_URL"]
    engine = create_async_engine(database_url, pool_pre_ping=True)
    queries = {
        "runs": "select * from strategy_runs order by run_id",
        "signals": "select signal_id, run_id, strategy_name, symbol, side, detected_at, source_state_at, features, reference_prices from strategy_signals",
        "fills": "select fill_id, candidate_id, signal_id, run_id, symbol, side, status, target_fill_at, filled_at, requested_notional, filled_notional, quantity, reference_midpoint, spread, fill_price, fee, total_cost, cost_bps, reason from paper_fills",
        "positions": "select position_id, run_id, entry_fill_id, signal_id, symbol, side, status, opened_at, closed_at, entry_price, exit_price, quantity, entry_notional, entry_fee, exit_fee, last_mark_price, unrealized_pnl, realized_pnl, return_pct, close_reason, updated_at from paper_positions",
        "equity": "select run_id, observed_at, balance, equity, realized_pnl, unrealized_pnl, total_fees, open_position_count from paper_equity_snapshots",
        "runtime_events": "select run_id, event_type, count(*) as event_count, min(occurred_at) as first_at, max(occurred_at) as last_at from strategy_runtime_events group by run_id, event_type order by run_id, event_type",
    }
    result: dict[str, list[dict[str, object]]] = {}
    async with engine.connect() as connection:
        for name, query in queries.items():
            rows = await connection.execute(text(query))
            result[name] = [dict(row._mapping) for row in rows]
    await engine.dispose()
    return result


async def main() -> None:
    tables = await fetch_all()
    runs = {row["run_id"]: row for row in tables["runs"]}
    signals = {row["signal_id"]: row for row in tables["signals"]}
    fills = {row["fill_id"]: row for row in tables["fills"]}
    records_by_run: dict[str, list[dict[str, object]]] = defaultdict(list)
    for position in tables["positions"]:
        records_by_run[position["run_id"]].append(build_record(position, signals, fills))
    equity_by_run: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in tables["equity"]:
        equity_by_run[row["run_id"]].append(
            {
                "observed_at": row["observed_at"],
                "balance": float_value(row["balance"]) or 0.0,
                "equity": float_value(row["equity"]) or 0.0,
                "realized_pnl": float_value(row["realized_pnl"]) or 0.0,
                "unrealized_pnl": float_value(row["unrealized_pnl"]) or 0.0,
                "total_fees": float_value(row["total_fees"]) or 0.0,
                "open_position_count": row["open_position_count"],
            }
        )

    account_metrics = []
    for run_id in ACCOUNT_ORDER:
        records = records_by_run.get(run_id, [])
        equity = sorted(equity_by_run.get(run_id, []), key=lambda row: row["observed_at"])
        latest = equity[-1] if equity else None
        metrics = basic_trade_stats(records)
        metrics.update(exposure_peak(records))
        metrics.update(
            {
                "run_id": run_id,
                "label": ACCOUNT_LABELS[run_id],
                "strategy_name": runs.get(run_id, {}).get("strategy_name"),
                "created_at": iso(runs.get(run_id, {}).get("created_at")),
                "config_hash": runs.get(run_id, {}).get("config_hash"),
                "code_commit": runs.get(run_id, {}).get("code_commit"),
                "latest_observed_at": iso(latest["observed_at"]) if latest else None,
                "latest_equity": latest["equity"] if latest else None,
                "latest_balance": latest["balance"] if latest else None,
                "latest_unrealized_pnl": latest["unrealized_pnl"] if latest else None,
                "latest_open_position_count": latest["open_position_count"] if latest else None,
                "equity_rows": len(equity),
                "max_drawdown": max_drawdown(equity),
                "local_hour": local_time_buckets(records),
                "symbol_metrics": symbol_buckets(records),
                "feature_mining": feature_mining(records),
            }
        )
        account_metrics.append(metrics)

    strategy_analysis = []
    for strategy, reference_run in REFERENCE_RUNS.items():
        reference_records = records_by_run.get(reference_run, [])
        variants = [metric for metric in account_metrics if metric["run_id"] in VARIANT_RUNS[strategy]]
        strategy_analysis.append(
            {
                "strategy": strategy,
                "reference_run_id": reference_run,
                "reference_label": ACCOUNT_LABELS[reference_run],
                "reference_trade_stats": next(
                    metric for metric in account_metrics if metric["run_id"] == reference_run
                ),
                "variant_runs": variants,
                "reference_time_buckets": local_time_buckets(reference_records),
                "reference_symbol_metrics": symbol_buckets(reference_records),
                "reference_feature_mining": feature_mining(reference_records),
            }
        )

    counts = {
        "strategy_runs": len(tables["runs"]),
        "strategy_signals": len(tables["signals"]),
        "paper_fills": len(tables["fills"]),
        "paper_positions": len(tables["positions"]),
        "paper_equity_snapshots": len(tables["equity"]),
        "runtime_event_rollups": len(tables["runtime_events"]),
    }
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "timezone": "Asia/Shanghai",
        "account_order": ACCOUNT_ORDER,
        "counts": counts,
        "runs": [serialize_run(runs[run_id]) for run_id in ACCOUNT_ORDER if run_id in runs],
        "account_metrics": account_metrics,
        "strategy_analysis": strategy_analysis,
        "pair_comparisons": pair_comparisons(records_by_run),
        "runtime_event_rollups": [
            {
                "run_id": row["run_id"],
                "event_type": row["event_type"],
                "event_count": row["event_count"],
                "first_at": iso(row["first_at"]),
                "last_at": iso(row["last_at"]),
            }
            for row in tables["runtime_events"]
        ],
    }
    print(json.dumps(output, ensure_ascii=False, separators=(",", ":"), default=str))


if __name__ == "__main__":
    asyncio.run(main())
