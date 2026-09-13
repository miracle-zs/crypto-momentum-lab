from __future__ import annotations

import asyncio
import csv
import json
import math
import os
import random
import statistics
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
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

LABELS = {
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
UTC = timezone.utc
FIVE_MINUTES = timedelta(minutes=5)
DEFAULT_FEE_RATE = 0.0004
DEFAULT_MAX_HOLD_MINUTES = 24 * 60


@dataclass(frozen=True, slots=True)
class Bar:
    start: datetime
    end: datetime
    open: float
    close: float


@dataclass(frozen=True, slots=True)
class Rule:
    rule_id: str
    interval_minutes: int
    confirmation_count: int = 1
    minimum_hold_minutes: int = 0
    minimum_body_pct: float = 0.0
    max_hold_minutes: int = DEFAULT_MAX_HOLD_MINUTES
    confirmation_window: int = 0
    require_close_beyond_entry: bool = False
    favorable_activation_pct: float = 0.0
    trailing_drawdown_pct: float = 0.0


RULES = [
    Rule("first_5m", 5),
    Rule("first_15m", 15),
    Rule("first_30m", 30),
    Rule("first_60m", 60),
    Rule("two_15m_confirm", 15, confirmation_count=2),
    Rule("first_15m_after_15m", 15, minimum_hold_minutes=15),
    Rule("first_15m_after_30m", 15, minimum_hold_minutes=30),
    Rule("first_15m_after_45m", 15, minimum_hold_minutes=45),
    Rule("first_15m_after_60m", 15, minimum_hold_minutes=60),
    Rule("first_15m_after_75m", 15, minimum_hold_minutes=75),
    Rule("first_15m_after_90m", 15, minimum_hold_minutes=90),
    Rule("first_15m_after_120m", 15, minimum_hold_minutes=120),
    Rule("first_15m_after_180m", 15, minimum_hold_minutes=180),
    Rule("two_15m_after_45m", 15, confirmation_count=2, minimum_hold_minutes=45),
    Rule("two_15m_after_60m", 15, confirmation_count=2, minimum_hold_minutes=60),
    Rule("first_30m_after_30m", 30, minimum_hold_minutes=30),
    Rule("first_30m_after_60m", 30, minimum_hold_minutes=60),
    Rule("first_60m_after_60m", 60, minimum_hold_minutes=60),
    Rule("first_15m_body_0.10pct", 15, minimum_body_pct=0.001),
    Rule("first_15m_body_0.25pct", 15, minimum_body_pct=0.0025),
    Rule("first_15m_body_0.50pct", 15, minimum_body_pct=0.005),
    Rule("first_15m_body_0.50pct_after_45m", 15, minimum_hold_minutes=45, minimum_body_pct=0.005),
    Rule("first_15m_body_0.50pct_after_60m", 15, minimum_hold_minutes=60, minimum_body_pct=0.005),
    Rule("three_15m_confirm", 15, confirmation_count=3),
    Rule("two_of_three_15m", 15, confirmation_count=2, confirmation_window=3),
    Rule("first_15m_cross_entry", 15, require_close_beyond_entry=True),
    Rule("first_15m_after_60m_cross_entry", 15, minimum_hold_minutes=60, require_close_beyond_entry=True),
    Rule("first_15m_profit_lock_1pct", 15, favorable_activation_pct=0.01),
    Rule("first_15m_after_60m_profit_lock_1pct", 15, minimum_hold_minutes=60, favorable_activation_pct=0.01),
    Rule("first_15m_trailing_0.5pct", 15, favorable_activation_pct=0.01, trailing_drawdown_pct=0.005),
    Rule("first_15m_after_60m_trailing_0.5pct", 15, minimum_hold_minutes=60, favorable_activation_pct=0.01, trailing_drawdown_pct=0.005),
]


def parse_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    text_value = str(value).replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text_value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def parse_json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def as_float(value: Any, default: float | None = None) -> float | None:
    if value is None or isinstance(value, bool):
        return default
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def floor_time(value: datetime, minutes: int) -> datetime:
    value = value.astimezone(UTC)
    epoch_minutes = int(value.timestamp() // 60)
    return datetime.fromtimestamp(
        (epoch_minutes - epoch_minutes % minutes) * 60,
        tz=UTC,
    )


def parse_execution_config(value: Any) -> dict[str, Any]:
    parsed = parse_json(value)
    return parsed if isinstance(parsed, dict) else {}


async def fetch_db() -> dict[str, Any]:
    local_dir = os.environ.get("CANDLE_REPLAY_LOCAL_DIR")
    if local_dir:
        base = Path(local_dir)

        def read_rows(env_name: str, default_name: str) -> list[dict[str, Any]]:
            path = Path(os.environ.get(env_name, str(base / default_name)))
            with path.open(newline="", encoding="utf-8") as handle:
                return list(csv.DictReader(handle))

        fills = read_rows("CANDLE_REPLAY_LOCAL_FILLS", "paper_fills.csv")
        fills = [
            row for row in fills
            if not row.get("status") or str(row.get("status")).lower() == "filled"
        ]
        return {
            "runs": read_rows("CANDLE_REPLAY_LOCAL_RUNS", "strategy_runs.csv"),
            "positions": read_rows(
                "CANDLE_REPLAY_LOCAL_POSITIONS", "paper_positions.csv"
            ),
            "fills": fills,
            "equity": read_rows(
                "CANDLE_REPLAY_LOCAL_EQUITY", "paper_equity_snapshots.csv"
            ),
            "signals": read_rows(
                "CANDLE_REPLAY_LOCAL_SIGNALS", "strategy_signals.csv"
            ),
        }

    database_url = os.environ["CML_DATABASE_URL"]
    engine = create_async_engine(database_url, pool_pre_ping=True)
    queries = {
        "runs": "select run_id, strategy_name, execution_config, created_at from strategy_runs",
        "positions": (
            "select position_id, run_id, signal_id, symbol, side, status, opened_at, "
            "closed_at, entry_price, exit_price, quantity, entry_notional, entry_fee, "
            "exit_fee, realized_pnl, close_reason, updated_at from paper_positions"
        ),
        "fills": "select run_id, symbol, spread, filled_at from paper_fills where status = 'filled'",
        "equity": "select run_id, observed_at, equity, balance, realized_pnl, unrealized_pnl, total_fees, open_position_count from paper_equity_snapshots",
        "signals": "select signal_id, run_id, symbol, side, source_state_at, features from strategy_signals",
    }
    tables: dict[str, list[dict[str, Any]]] = {}
    async with engine.connect() as connection:
        for name, query in queries.items():
            result = await connection.execute(text(query))
            tables[name] = [dict(row._mapping) for row in result]
    await engine.dispose()
    return tables


async def fetch_5m_bars(
    symbols: list[str],
    start: datetime,
    end: datetime,
    cache_dir: Path,
) -> tuple[dict[str, list[Bar]], dict[str, str]]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    errors: dict[str, str] = {}
    semaphore = asyncio.Semaphore(8)
    limits = httpx.Limits(max_connections=12, max_keepalive_connections=12)
    timeout = httpx.Timeout(20.0, connect=10.0)

    async with httpx.AsyncClient(
        base_url="https://fapi.binance.com",
        timeout=timeout,
        limits=limits,
        trust_env=False,
    ) as client:

        async def one(symbol: str) -> tuple[str, list[Bar]]:
            cache_path = cache_dir / f"{symbol}.json"
            if cache_path.exists():
                try:
                    cached = json.loads(cache_path.read_text())
                    bars = [
                        Bar(parse_dt(row[0]), parse_dt(row[1]), float(row[2]), float(row[3]))
                        for row in cached
                    ]
                    if bars and bars[0].start <= start and bars[-1].end >= end:
                        return symbol, bars
                except (OSError, ValueError, TypeError, json.JSONDecodeError):
                    pass

            rows: dict[int, list[Any]] = {}
            cursor = floor_time(start, 5)
            request_end = floor_time(end + FIVE_MINUTES, 5)
            async with semaphore:
                for _ in range(6):
                    if cursor >= request_end:
                        break
                    params = {
                        "symbol": symbol,
                        "interval": "5m",
                        "startTime": int(cursor.timestamp() * 1000),
                        "endTime": int(request_end.timestamp() * 1000) - 1,
                        "limit": 1500,
                    }
                    last_error: Exception | None = None
                    for attempt in range(4):
                        try:
                            response = await client.get("/fapi/v1/klines", params=params)
                            response.raise_for_status()
                            page = response.json()
                            if not isinstance(page, list):
                                raise ValueError("Binance kline page is not a list")
                            for row in page:
                                if not isinstance(row, list) or len(row) < 7:
                                    continue
                                rows[int(row[0])] = row
                            last_error = None
                            break
                        except Exception as error:  # noqa: BLE001
                            last_error = error
                            await asyncio.sleep(0.5 * (attempt + 1))
                    if last_error is not None:
                        raise last_error
                    if not rows:
                        break
                    next_cursor = datetime.fromtimestamp(
                        (max(rows) + 300_000) / 1000,
                        tz=UTC,
                    )
                    if next_cursor <= cursor:
                        break
                    cursor = next_cursor
                    if len(page) < 1500:
                        break
            bars = []
            for timestamp, row in sorted(rows.items()):
                bar_start = datetime.fromtimestamp(timestamp / 1000, tz=UTC)
                bar_end = bar_start + FIVE_MINUTES
                if start - FIVE_MINUTES <= bar_start < request_end:
                    bars.append(Bar(bar_start, bar_end, float(row[1]), float(row[4])))
            cache_path.write_text(
                json.dumps([[b.start.isoformat(), b.end.isoformat(), b.open, b.close] for b in bars])
            )
            return symbol, bars

        tasks = [asyncio.create_task(one(symbol)) for symbol in symbols]
        output: dict[str, list[Bar]] = {}
        for index, task in enumerate(asyncio.as_completed(tasks), start=1):
            try:
                symbol, bars = await task
                output[symbol] = bars
                print(f"downloaded {index}/{len(symbols)} {symbol} ({len(bars)} 5m bars)", file=sys.stderr)
            except Exception as error:  # noqa: BLE001
                message = f"{type(error).__name__}: {error}"
                errors[f"task_{index}"] = message
                print(f"kline download failed {index}/{len(symbols)}: {message}", file=sys.stderr)
    return output, errors


def aggregate_bars(raw: list[Bar], interval_minutes: int) -> list[Bar]:
    if interval_minutes == 5:
        return raw
    expected = interval_minutes // 5
    groups: dict[datetime, list[Bar]] = defaultdict(list)
    for bar in raw:
        bucket = floor_time(bar.start, interval_minutes)
        groups[bucket].append(bar)
    output: list[Bar] = []
    for bucket, group in sorted(groups.items()):
        group.sort(key=lambda bar: bar.start)
        if len(group) != expected:
            continue
        if any(
            current.start != previous.start + FIVE_MINUTES
            for previous, current in zip(group, group[1:], strict=False)
        ):
            continue
        output.append(Bar(bucket, bucket + timedelta(minutes=interval_minutes), group[0].open, group[-1].close))
    return output


def executable_price(close: float, side: str, spread: float) -> float:
    half_spread = max(0.0, spread) / 2
    if side == "long":
        return max(0.0, close - half_spread)
    return close + half_spread


def find_bar_price(bars: list[Bar], at: datetime) -> float | None:
    eligible = [bar for bar in bars if bar.end <= at]
    return eligible[-1].close if eligible else None


def find_candle_exit(
    bars: list[Bar],
    *,
    opened_at: datetime,
    side: str,
    entry_price: float,
    rule: Rule,
    cutoff: datetime,
) -> tuple[datetime, float, str] | None:
    max_at = min(cutoff, opened_at + timedelta(minutes=rule.max_hold_minutes))
    minimum_end = opened_at + timedelta(minutes=rule.minimum_hold_minutes)
    eligible = [
        bar
        for bar in bars
        if opened_at < bar.end <= max_at and bar.end >= minimum_end
    ]
    count = rule.confirmation_count
    window = rule.confirmation_window or count
    for index in range(window - 1, len(eligible)):
        window_bars = eligible[index - window + 1 : index + 1]
        confirmed = window_bars[-count:]
        if any(
            current.start != previous.start + timedelta(minutes=rule.interval_minutes)
            for previous, current in zip(window_bars, window_bars[1:], strict=False)
        ):
            continue
        if any(
            abs(bar.close - bar.open) / bar.open < rule.minimum_body_pct
            for bar in confirmed
            if bar.open > 0
        ):
            continue
        if side == "long":
            reverse_flags = [bar.close < bar.open for bar in window_bars]
            reverse = all(reverse_flags) if rule.confirmation_window == 0 else (
                reverse_flags[-1] and sum(reverse_flags) >= rule.confirmation_count
            )
            beyond_entry = not rule.require_close_beyond_entry or window_bars[-1].close < entry_price
            activated = (
                rule.favorable_activation_pct <= 0
                or any(
                    bar.close >= entry_price * (1 + rule.favorable_activation_pct)
                    for bar in eligible[:index]
                )
            )
            if rule.trailing_drawdown_pct > 0:
                favorable = [bar.close for bar in eligible[:index]]
                extreme = max(favorable, default=entry_price)
                trailing = (
                    extreme >= entry_price * (1 + rule.favorable_activation_pct)
                    and window_bars[-1].close <= extreme * (1 - rule.trailing_drawdown_pct)
                )
            else:
                trailing = True
            if reverse and beyond_entry and activated and trailing:
                return window_bars[-1].end, window_bars[-1].close, f"{rule.rule_id}_bearish"
        if side == "short":
            reverse_flags = [bar.close > bar.open for bar in window_bars]
            reverse = all(reverse_flags) if rule.confirmation_window == 0 else (
                reverse_flags[-1] and sum(reverse_flags) >= rule.confirmation_count
            )
            beyond_entry = not rule.require_close_beyond_entry or window_bars[-1].close > entry_price
            activated = (
                rule.favorable_activation_pct <= 0
                or any(
                    bar.close <= entry_price * (1 - rule.favorable_activation_pct)
                    for bar in eligible[:index]
                )
            )
            if rule.trailing_drawdown_pct > 0:
                favorable = [bar.close for bar in eligible[:index]]
                extreme = min(favorable, default=entry_price)
                trailing = (
                    extreme <= entry_price * (1 - rule.favorable_activation_pct)
                    and window_bars[-1].close >= extreme * (1 + rule.trailing_drawdown_pct)
                )
            else:
                trailing = True
            if reverse and beyond_entry and activated and trailing:
                return window_bars[-1].end, window_bars[-1].close, f"{rule.rule_id}_bullish"

    if max_at < cutoff:
        mark = find_bar_price(bars, max_at)
        if mark is not None:
            return max_at, mark, f"{rule.rule_id}_max_holding"
    return None


def base_trade(position: dict[str, Any], spread: float) -> dict[str, Any]:
    return {
        "position_id": str(position["position_id"]),
        "run_id": str(position["run_id"]),
        "symbol": str(position["symbol"]),
        "side": str(position["side"]),
        "opened_at": parse_dt(position["opened_at"]),
        "entry_price": as_float(position["entry_price"], 0.0) or 0.0,
        "quantity": as_float(position["quantity"], 0.0) or 0.0,
        "entry_notional": as_float(position["entry_notional"], 0.0) or 0.0,
        "entry_fee": as_float(position["entry_fee"], 0.0) or 0.0,
        "spread": spread,
    }


def pnl_for(trade: dict[str, Any], exit_price: float, fee_rate: float, closed: bool) -> float:
    if trade["side"] == "long":
        gross = (exit_price - trade["entry_price"]) * trade["quantity"]
    else:
        gross = (trade["entry_price"] - exit_price) * trade["quantity"]
    exit_fee = abs(exit_price * trade["quantity"]) * fee_rate if closed else 0.0
    return gross - trade["entry_fee"] - exit_fee


def simulate_trade(
    trade: dict[str, Any],
    bars: list[Bar],
    rule: Rule,
    cutoff: datetime,
    fee_rate: float,
) -> dict[str, Any]:
    outcome = find_candle_exit(
        bars,
        opened_at=trade["opened_at"],
        side=trade["side"],
        entry_price=trade["entry_price"],
        rule=rule,
        cutoff=cutoff,
    )
    if outcome is not None:
        closed_at, raw_price, reason = outcome
        closed = True
    else:
        mark = find_bar_price(bars, cutoff)
        if mark is None:
            return {
                **trade,
                "closed": False,
                "closed_at": None,
                "exit_price": None,
                "pnl": None,
                "close_reason": "no_mark",
                "duration_minutes": None,
            }
        closed_at, raw_price, reason, closed = cutoff, mark, "open", False
    exit_price = executable_price(raw_price, trade["side"], trade["spread"])
    pnl = pnl_for(trade, exit_price, fee_rate, closed)
    return {
        **trade,
        "closed": closed,
        "closed_at": closed_at,
        "exit_price": exit_price,
        "raw_exit_price": raw_price,
        "pnl": pnl,
        "close_reason": reason,
        "duration_minutes": (closed_at - trade["opened_at"]).total_seconds() / 60,
    }


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower, upper = math.floor(position), math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def profit_factor(values: list[float]) -> float | None:
    gains = sum(value for value in values if value > 0)
    losses = -sum(value for value in values if value < 0)
    if losses == 0:
        return None if gains == 0 else float("inf")
    return gains / losses


def max_drawdown(trades: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(
        [trade for trade in trades if trade.get("pnl") is not None],
        key=lambda trade: trade["closed_at"] or trade["opened_at"],
    )
    cumulative = 0.0
    peak = 0.0
    peak_at = None
    worst = {"amount": 0.0, "from": None, "to": None}
    for trade in ordered:
        cumulative += trade["pnl"]
        observed_at = trade["closed_at"] or trade["opened_at"]
        if cumulative > peak:
            peak, peak_at = cumulative, observed_at
        drawdown = cumulative - peak
        if drawdown < worst["amount"]:
            worst = {"amount": drawdown, "from": iso(peak_at), "to": iso(observed_at)}
    return worst


def symbol_robustness(trades: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [trade for trade in trades if trade["closed"] and trade["pnl"] is not None]
    by_symbol: dict[str, list[float]] = defaultdict(list)
    for trade in closed:
        by_symbol[trade["symbol"]].append(trade["pnl"])
    symbol_net = sorted(
        ((symbol, sum(values), len(values)) for symbol, values in by_symbol.items()),
        key=lambda item: item[1],
    )
    positive_symbols = sorted(symbol_net, key=lambda item: item[1], reverse=True)
    top_symbols = {item[0] for item in positive_symbols[:5]}
    top_trade_pnls = sorted((trade["pnl"] for trade in closed if trade["pnl"] > 0), reverse=True)
    all_net = sum(trade["pnl"] for trade in closed)
    net_without_top_symbols = sum(
        trade["pnl"] for trade in closed if trade["symbol"] not in top_symbols
    )
    net_without_top_trades = all_net - sum(top_trade_pnls[:5])
    means = [sum(values) / len(values) for values in by_symbol.values() if values]
    rng = random.Random(20260808)
    bootstrap = []
    if means:
        for _ in range(2000):
            bootstrap.append(statistics.mean(rng.choices(means, k=len(means))))
    return {
        "symbol_count": len(by_symbol),
        "worst_symbols": [
            {"symbol": symbol, "net_pnl": net, "trades": count}
            for symbol, net, count in symbol_net[:8]
        ],
        "best_symbols": [
            {"symbol": symbol, "net_pnl": net, "trades": count}
            for symbol, net, count in positive_symbols[:8]
        ],
        "top_5_positive_symbol_share": (
            sum(item[1] for item in positive_symbols[:5])
            / sum(item[1] for item in positive_symbols if item[1] > 0)
            if any(item[1] > 0 for item in positive_symbols)
            else 0.0
        ),
        "net_without_top_5_symbols": net_without_top_symbols,
        "net_without_top_5_trades": net_without_top_trades,
        "symbol_mean_pnl_bootstrap_95": [
            percentile(bootstrap, 0.025), percentile(bootstrap, 0.975)
        ],
    }


def subset_stats(
    trades: list[dict[str, Any]],
    *,
    include_side: bool = True,
) -> dict[str, Any]:
    closed = [trade for trade in trades if trade["closed"] and trade["pnl"] is not None]
    pnls = [trade["pnl"] for trade in closed]
    wins = [pnl for pnl in pnls if pnl > 0]
    losses = [pnl for pnl in pnls if pnl < 0]
    result = {
        "trades": len(trades),
        "closed_trades": len(closed),
        "open_trades": len(trades) - len(closed),
        "net_pnl_marked": sum(trade["pnl"] for trade in trades if trade["pnl"] is not None),
        "net_pnl_closed": sum(pnls),
        "profit_factor": profit_factor(pnls),
        "win_rate": len(wins) / len(pnls) if pnls else None,
        "expectancy": statistics.mean(pnls) if pnls else None,
        "median_pnl": statistics.median(pnls) if pnls else None,
        "average_duration_minutes": statistics.mean(
            [trade["duration_minutes"] for trade in closed if trade["duration_minutes"] is not None]
        )
        if closed
        else None,
        "close_reasons": dict(Counter(trade["close_reason"] for trade in trades)),
    }
    if include_side:
        result["side"] = {
            side: subset_stats(
                [trade for trade in trades if trade["side"] == side],
                include_side=False,
            )
            for side in ("long", "short")
            if any(trade["side"] == side for trade in trades)
        }
    return result


def daily_stats(trades: list[dict[str, Any]]) -> dict[str, Any]:
    by_day: dict[str, list[float]] = defaultdict(list)
    for trade in trades:
        if trade["closed"] and trade["pnl"] is not None and trade["closed_at"] is not None:
            day = trade["closed_at"].astimezone(LOCAL_TZ).date().isoformat()
            by_day[day].append(trade["pnl"])
    result = {
        day: {"trades": len(values), "net_pnl": sum(values), "pf": profit_factor(values)}
        for day, values in sorted(by_day.items())
    }
    return result


def split_stats(trades: list[dict[str, Any]], split_at: datetime) -> dict[str, Any]:
    train = [trade for trade in trades if trade["opened_at"] < split_at]
    validation = [trade for trade in trades if trade["opened_at"] >= split_at]
    return {
        "split_at": iso(split_at),
        "train": subset_stats(train),
        "validation": subset_stats(validation),
    }


def rule_metrics(
    trades: list[dict[str, Any]],
    split_at: datetime,
) -> dict[str, Any]:
    closed = [trade for trade in trades if trade["closed"] and trade["pnl"] is not None]
    positive = sorted((trade["pnl"] for trade in closed if trade["pnl"] > 0), reverse=True)
    net = sum(trade["pnl"] for trade in closed)
    return {
        **subset_stats(trades),
        "max_drawdown": max_drawdown(trades),
        "symbol_robustness": symbol_robustness(trades),
        "daily": daily_stats(trades),
        "positive_days": sum(1 for values in daily_stats(trades).values() if values["net_pnl"] > 0),
        "total_positive_pnl": sum(positive),
        "top_1_positive_share": positive[0] / sum(positive) if positive and sum(positive) else 0.0,
        "top_3_positive_share": sum(positive[:3]) / sum(positive) if positive and sum(positive) else 0.0,
        "top_5_positive_share": sum(positive[:5]) / sum(positive) if positive and sum(positive) else 0.0,
        "net_without_top_2_trades": net - sum(positive[:2]),
        "split": split_stats(trades, split_at),
    }


def pair_delta(base: list[dict[str, Any]], variant: list[dict[str, Any]]) -> dict[str, Any]:
    base_by_id = {trade["position_id"]: trade for trade in base}
    variant_by_id = {trade["position_id"]: trade for trade in variant}
    deltas = []
    for position_id in sorted(base_by_id.keys() & variant_by_id.keys()):
        left, right = base_by_id[position_id], variant_by_id[position_id]
        if left["pnl"] is None or right["pnl"] is None:
            continue
        deltas.append(right["pnl"] - left["pnl"])
    return {
        "common_entries": len(base_by_id.keys() & variant_by_id.keys()),
        "paired_with_mark": len(deltas),
        "total_pnl_delta": sum(deltas),
        "mean_pnl_delta": statistics.mean(deltas) if deltas else None,
        "median_pnl_delta": statistics.median(deltas) if deltas else None,
        "variant_better_rate": sum(delta > 0 for delta in deltas) / len(deltas) if deltas else None,
        "delta_p10": percentile(deltas, 0.1),
        "delta_p90": percentile(deltas, 0.9),
    }


def actual_summary(positions: list[dict[str, Any]], equity: list[dict[str, Any]]) -> dict[str, Any]:
    by_run: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for position in positions:
        by_run[str(position["run_id"])].append(position)
    equity_by_run: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in equity:
        equity_by_run[str(row["run_id"])].append(row)
    output = {}
    for run_id in ACCOUNT_ORDER:
        rows = by_run.get(run_id, [])
        closed = [row for row in rows if row.get("status") == "closed" and row.get("realized_pnl") is not None]
        pnls = [as_float(row["realized_pnl"], 0.0) or 0.0 for row in closed]
        wins = [pnl for pnl in pnls if pnl > 0]
        run_equity = sorted(equity_by_run.get(run_id, []), key=lambda row: parse_dt(row["observed_at"]))
        output[run_id] = {
            "label": LABELS[run_id],
            "trades": len(rows),
            "closed_trades": len(closed),
            "open_trades": len(rows) - len(closed),
            "net_realized_pnl": sum(pnls),
            "profit_factor": profit_factor(pnls),
            "win_rate": len(wins) / len(pnls) if pnls else None,
            "latest_observed_at": iso(run_equity[-1]["observed_at"]) if run_equity else None,
            "latest_equity": as_float(run_equity[-1]["equity"]) if run_equity else None,
        }
    return output


async def main() -> None:
    tables = await fetch_db()
    positions = tables["positions"]
    equity = tables["equity"]
    cutoff_override = os.environ.get("CANDLE_REPLAY_CUTOFF")
    cutoff = (
        parse_dt(cutoff_override)
        if cutoff_override
        else max(parse_dt(row["observed_at"]) for row in equity)
    )
    reference_positions = [
        row for row in positions
        if (
            str(row["run_id"]) in REFERENCE_RUNS.values()
            and row.get("opened_at") is not None
            and parse_dt(row["opened_at"]) <= cutoff
        )
    ]
    if not reference_positions:
        raise RuntimeError("no reference positions found")
    frozen_positions = [
        row
        for row in positions
        if row.get("opened_at") is not None and parse_dt(row["opened_at"]) <= cutoff
    ]
    frozen_signals = [
        row
        for row in tables["signals"]
        if row.get("source_state_at") is not None
        and parse_dt(row["source_state_at"]) <= cutoff
    ]
    frozen_equity = [
        row
        for row in equity
        if parse_dt(row["observed_at"]) <= cutoff
    ]
    minimum_open = min(parse_dt(row["opened_at"]) for row in reference_positions)
    spread_values: dict[str, list[float]] = defaultdict(list)
    for row in tables["fills"]:
        if row.get("filled_at") is not None and parse_dt(row["filled_at"]) > cutoff:
            continue
        spread = as_float(row.get("spread"))
        if spread is not None and spread >= 0:
            spread_values[str(row["symbol"])].append(spread)
    spreads = {
        symbol: statistics.median(values) if values else 0.0
        for symbol, values in spread_values.items()
    }
    run_configs = {
        str(row["run_id"]): parse_execution_config(row.get("execution_config"))
        for row in tables["runs"]
    }
    fee_rate = DEFAULT_FEE_RATE
    for config in run_configs.values():
        fills = config.get("fills", {}) if isinstance(config, dict) else {}
        fee_rate = as_float(fills.get("taker_fee_rate"), fee_rate) or fee_rate
        break

    trades_by_strategy: dict[str, list[dict[str, Any]]] = {}
    for strategy, run_id in REFERENCE_RUNS.items():
        strategy_positions = [row for row in reference_positions if str(row["run_id"]) == run_id]
        trades_by_strategy[strategy] = [
            base_trade(row, spreads.get(str(row["symbol"]), 0.0))
            for row in strategy_positions
        ]

    symbols = sorted({trade["symbol"] for trades in trades_by_strategy.values() for trade in trades})
    cache_dir = Path(os.environ.get("CANDLE_REPLAY_CACHE", "/tmp/candle-replay-klines-20260808"))
    raw_bars, download_errors = await fetch_5m_bars(
        symbols,
        floor_time(minimum_open, 5),
        cutoff,
        cache_dir,
    )
    split_at = minimum_open + (cutoff - minimum_open) * 0.6
    output: dict[str, Any] = {
        "generated_at": datetime.now(UTC).isoformat(),
        "cutoff": iso(cutoff),
        "minimum_opened_at": iso(minimum_open),
        "split_at": iso(split_at),
        "timezone": "Asia/Shanghai",
        "fee_rate": fee_rate,
        "counts": {
            "strategy_runs": len(tables["runs"]),
            "strategy_signals": len(frozen_signals),
            "paper_positions": len(frozen_positions),
            "paper_equity_snapshots": len(frozen_equity),
            "reference_positions": sum(len(trades) for trades in trades_by_strategy.values()),
            "symbols_requested": len(symbols),
            "symbols_loaded": len(raw_bars),
        },
        "actual_summary": actual_summary(positions, equity),
        "download_errors": download_errors,
        "strategies": {},
    }

    aggregated_by_symbol = {
        symbol: {
            interval: aggregate_bars(raw_bars.get(symbol, []), interval)
            for interval in (5, 15, 30, 60)
        }
        for symbol in raw_bars
    }

    for strategy, base_trades in trades_by_strategy.items():
        strategy_minimum_open = min(trade["opened_at"] for trade in base_trades)
        strategy_split_at = strategy_minimum_open + (cutoff - strategy_minimum_open) * 0.6
        rule_outputs: dict[str, Any] = {}
        simulated_by_rule: dict[str, list[dict[str, Any]]] = {}
        for rule in RULES:
            simulated = []
            for trade in base_trades:
                bars = aggregated_by_symbol.get(trade["symbol"], {}).get(rule.interval_minutes, [])
                simulated.append(simulate_trade(trade, bars, rule, cutoff, fee_rate))
            simulated_by_rule[rule.rule_id] = simulated
            rule_outputs[rule.rule_id] = {
                "rule": {
                    "interval_minutes": rule.interval_minutes,
                    "confirmation_count": rule.confirmation_count,
                    "confirmation_window": rule.confirmation_window,
                    "minimum_hold_minutes": rule.minimum_hold_minutes,
                    "minimum_body_pct": rule.minimum_body_pct,
                    "max_hold_minutes": rule.max_hold_minutes,
                    "require_close_beyond_entry": rule.require_close_beyond_entry,
                    "favorable_activation_pct": rule.favorable_activation_pct,
                    "trailing_drawdown_pct": rule.trailing_drawdown_pct,
                },
                "metrics": rule_metrics(simulated, strategy_split_at),
                "directional_metrics": {
                    side: rule_metrics(
                        [trade for trade in simulated if trade["side"] == side],
                        strategy_split_at,
                    )
                    for side in ("long", "short")
                },
            }
        pair_outputs = {}
        base = simulated_by_rule["first_15m"]
        for rule_id, simulated in simulated_by_rule.items():
            if rule_id != "first_15m":
                pair_outputs[f"first_15m_vs_{rule_id}"] = pair_delta(base, simulated)
        output["strategies"][strategy] = {
            "reference_run_id": REFERENCE_RUNS[strategy],
            "reference_label": LABELS[REFERENCE_RUNS[strategy]],
            "reference_entries": len(base_trades),
            "rules": rule_outputs,
            "pair_deltas_vs_first_15m": pair_outputs,
        }

    print(json.dumps(output, ensure_ascii=False, separators=(",", ":"), default=str))


if __name__ == "__main__":
    asyncio.run(main())
