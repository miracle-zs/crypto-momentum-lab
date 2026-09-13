from __future__ import annotations

import asyncio
import csv
import json
import os
import statistics
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx


TARGET_RUNS = {
    "paper-account-02-orderflow-v1": "02 订单流｜fixed",
    "paper-account-05-orderflow-candle15m-v1": "05 订单流｜15m 反向收线",
    "paper-account-07-orderflow-candle45m-v1": "07 订单流｜45m 后反向收线",
}

UTC = timezone.utc
LOCAL_TZ = ZoneInfo("Asia/Shanghai")
FIVE_MINUTES = timedelta(minutes=5)
FIFTEEN_MINUTES = timedelta(minutes=15)
FEE_RATE = 0.0004


@dataclass(frozen=True, slots=True)
class Bar:
    start: datetime
    end: datetime
    high: float
    low: float
    close: float


def parse_dt(value: Any) -> datetime:
    text = str(value).replace("Z", "+00:00").replace(" ", "T")
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def floor_time(value: datetime, minutes: int) -> datetime:
    value = value.astimezone(UTC)
    epoch_minutes = int(value.timestamp() // 60)
    return datetime.fromtimestamp(
        (epoch_minutes - epoch_minutes % minutes) * 60,
        tz=UTC,
    )


def ceil_time(value: datetime, minutes: int) -> datetime:
    floored = floor_time(value, minutes)
    return floored if floored == value else floored + timedelta(minutes=minutes)


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


async def fetch_bars(
    symbols: list[str],
    start: datetime,
    end: datetime,
    cache_dir: Path,
    preloaded: dict[str, list[Bar]] | None = None,
) -> tuple[dict[str, list[Bar]], dict[str, str]]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    preloaded = preloaded or {}
    errors: dict[str, str] = {}
    semaphore = asyncio.Semaphore(2)
    limits = httpx.Limits(max_connections=4, max_keepalive_connections=4)
    timeout = httpx.Timeout(30.0, connect=15.0)

    async with httpx.AsyncClient(
        base_url="https://fapi.binance.com",
        timeout=timeout,
        limits=limits,
        trust_env=False,
    ) as client:

        async def one(symbol: str) -> tuple[str, list[Bar]]:
            if symbol in preloaded:
                return symbol, preloaded[symbol]
            cache_path = cache_dir / f"{symbol}.json"
            if cache_path.exists():
                try:
                    cached = json.loads(cache_path.read_text())
                    bars = [
                        Bar(
                            parse_dt(row[0]),
                            parse_dt(row[1]),
                            float(row[2]),
                            float(row[3]),
                            float(row[4]),
                        )
                        for row in cached
                        if isinstance(row, list) and len(row) >= 5
                    ]
                    if bars and bars[0].start <= start and bars[-1].end >= end:
                        return symbol, bars
                except (OSError, ValueError, TypeError, json.JSONDecodeError):
                    pass

            rows: dict[int, list[Any]] = {}
            cursor = floor_time(start, 5)
            request_end = floor_time(end + FIVE_MINUTES, 5)
            async with semaphore:
                while cursor < request_end:
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
                                if isinstance(row, list) and len(row) >= 7:
                                    rows[int(row[0])] = row
                            last_error = None
                            break
                        except Exception as error:  # noqa: BLE001
                            last_error = error
                            status_code = getattr(getattr(error, "response", None), "status_code", None)
                            delay = 12.0 * (attempt + 1) if status_code in (418, 429) else 0.5 * (attempt + 1)
                            await asyncio.sleep(delay)
                    if last_error is not None:
                        raise last_error
                    if not page:
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

            bars = [
                Bar(
                    datetime.fromtimestamp(timestamp / 1000, tz=UTC),
                    datetime.fromtimestamp(timestamp / 1000, tz=UTC) + FIVE_MINUTES,
                    float(row[2]),
                    float(row[3]),
                    float(row[4]),
                )
                for timestamp, row in sorted(rows.items())
                if start - FIVE_MINUTES <= datetime.fromtimestamp(timestamp / 1000, tz=UTC) < request_end
            ]
            cache_path.write_text(
                json.dumps(
                    [[bar.start.isoformat(), bar.end.isoformat(), bar.high, bar.low, bar.close] for bar in bars]
                )
            )
            return symbol, bars

        tasks = [asyncio.create_task(one(symbol)) for symbol in symbols]
        output: dict[str, list[Bar]] = {}
        for index, task in enumerate(asyncio.as_completed(tasks), start=1):
            try:
                symbol, bars = await task
                output[symbol] = bars
                print(f"loaded {index}/{len(symbols)} {symbol} ({len(bars)} 5m bars)", file=sys.stderr)
            except Exception as error:  # noqa: BLE001
                errors[f"task_{index}"] = f"{type(error).__name__}: {error}"
    return output, errors


def load_api_pages(raw_dir: Path) -> dict[str, list[Bar]]:
    grouped: dict[str, dict[int, list[Any]]] = defaultdict(dict)
    for path in raw_dir.glob("*.json") if raw_dir.exists() else []:
        symbol = path.name.rsplit("-", 1)[0]
        try:
            page = json.loads(path.read_text())
            if not isinstance(page, list):
                continue
            for row in page:
                if isinstance(row, list) and len(row) >= 7:
                    grouped[symbol][int(row[0])] = row
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            continue
    result: dict[str, list[Bar]] = {}
    for symbol, rows in grouped.items():
        result[symbol] = [
            Bar(
                datetime.fromtimestamp(timestamp / 1000, tz=UTC),
                datetime.fromtimestamp(timestamp / 1000, tz=UTC) + FIVE_MINUTES,
                float(row[2]),
                float(row[3]),
                float(row[4]),
            )
            for timestamp, row in sorted(rows.items())
        ]
    return result


def load_server_ohlc(path: Path) -> dict[str, list[Bar]]:
    grouped: dict[str, list[Bar]] = defaultdict(list)
    if not path.exists():
        return grouped
    for row in read_csv(path):
        start = parse_dt(row["bar_start"])
        grouped[row["symbol"]].append(
            Bar(
                start,
                start + FIVE_MINUTES,
                as_float(row["high"]),
                as_float(row["low"]),
                as_float(row["close"]),
            )
        )
    for bars in grouped.values():
        bars.sort(key=lambda bar: bar.start)
    return grouped


def executable_price(price: float, side: str, spread: float) -> float:
    half_spread = max(0.0, spread) / 2
    return price - half_spread if side == "long" else price + half_spread


def pnl_for(position: dict[str, Any], exit_price: float, closed: bool) -> float:
    entry = as_float(position["entry_price"])
    quantity = as_float(position["quantity"])
    entry_fee = as_float(position["entry_fee"])
    gross = (exit_price - entry) * quantity if position["side"] == "long" else (entry - exit_price) * quantity
    exit_fee = abs(exit_price * quantity) * FEE_RATE if closed else 0.0
    return gross - entry_fee - exit_fee


def visible_structure_stop(
    position: dict[str, Any],
    bars: list[Bar],
) -> tuple[float, dict[str, Any]]:
    opened_at = parse_dt(position["opened_at"])
    entry = as_float(position["entry_price"])
    current_bucket = floor_time(opened_at, 15)
    previous_bucket = current_bucket - FIFTEEN_MINUTES
    previous = [
        bar for bar in bars
        if previous_bucket <= bar.start < current_bucket and bar.end <= opened_at
    ]
    current_visible = [
        bar for bar in bars
        if current_bucket <= bar.start < opened_at and bar.end <= opened_at
    ]
    if position["side"] == "long":
        candidates = [entry] + [bar.low for bar in previous + current_visible]
        stop = min(candidates)
    else:
        candidates = [entry] + [bar.high for bar in previous + current_visible]
        stop = max(candidates)
    return stop, {
        "current_15m_start": current_bucket.isoformat(),
        "previous_15m_start": previous_bucket.isoformat(),
        "current_visible_5m_bars": len(current_visible),
        "previous_15m_5m_bars": len(previous),
    }


def first_stop_hit(
    position: dict[str, Any],
    bars: list[Bar],
    stop: float,
    horizon: datetime,
) -> datetime | None:
    opened_at = parse_dt(position["opened_at"])
    first_full_bar = ceil_time(opened_at, 5)
    for bar in bars:
        if bar.start < first_full_bar or bar.start >= horizon:
            continue
        if position["side"] == "long" and bar.low <= stop:
            return bar.start
        if position["side"] == "short" and bar.high >= stop:
            return bar.start
    return None


def mark_at_cutoff(position: dict[str, Any], bars: list[Bar], cutoff: datetime, spread: float) -> float:
    eligible = [bar for bar in bars if bar.end <= cutoff]
    if not eligible:
        return pnl_for(position, as_float(position["entry_price"]), False)
    mark = executable_price(eligible[-1].close, position["side"], spread)
    return pnl_for(position, mark, False)


def latest_equity(rows: list[dict[str, Any]], run_id: str, cutoff: datetime) -> dict[str, Any]:
    eligible = [row for row in rows if row["run_id"] == run_id and parse_dt(row["observed_at"]) <= cutoff]
    return max(eligible, key=lambda row: parse_dt(row["observed_at"]))


async def main() -> None:
    positions_path = Path("/tmp/paper_positions-complete.csv")
    fills_path = Path("/tmp/paper_fills-complete.csv")
    equity_path = Path("/tmp/paper_equity_snapshots-complete.csv")
    cache_dir = Path("data/server-paper-accounts-20260807T160821Z/analysis/klines-5m-ohlc-20260808")
    positions_path = Path(os.environ.get("STRUCTURAL_STOP_POSITIONS", positions_path))
    fills_path = Path(os.environ.get("STRUCTURAL_STOP_FILLS", fills_path))
    equity_path = Path(os.environ.get("STRUCTURAL_STOP_EQUITY", equity_path))
    cache_dir = Path(os.environ.get("STRUCTURAL_STOP_CACHE", cache_dir))
    raw_api_dir = Path(os.environ.get("STRUCTURAL_STOP_RAW_API", "/tmp/orderflow-missing-ohlc-20260808"))
    server_ohlc_path = Path(os.environ.get("STRUCTURAL_STOP_SERVER_OHLC", "/tmp/research_5m_ohlc.csv"))

    positions = [
        row for row in read_csv(positions_path)
        if row.get("run_id") in TARGET_RUNS
    ]
    fills = read_csv(fills_path)
    equity = read_csv(equity_path)
    cutoff = max(parse_dt(row["observed_at"]) for row in equity)
    positions = [row for row in positions if parse_dt(row["opened_at"]) <= cutoff]
    symbols = sorted({row["symbol"] for row in positions})
    spread_values: dict[str, list[float]] = defaultdict(list)
    for row in fills:
        if row.get("status") and row["status"].lower() != "filled":
            continue
        if row.get("filled_at") and parse_dt(row["filled_at"]) > cutoff:
            continue
        spread_values[row["symbol"]].append(as_float(row.get("spread")))
    spreads = {
        symbol: statistics.median(values) if values else 0.0
        for symbol, values in spread_values.items()
    }

    minimum_open = min(parse_dt(row["opened_at"]) for row in positions)
    fetch_start = minimum_open - FIFTEEN_MINUTES
    server_bars = load_server_ohlc(server_ohlc_path)
    api_bars = load_api_pages(raw_api_dir)
    cached_symbols = {path.stem for path in cache_dir.glob("*.json")}
    preloaded: dict[str, list[Bar]] = {}
    for symbol in symbols:
        api_candidate = api_bars.get(symbol, [])
        server_candidate = server_bars.get(symbol, [])
        if api_candidate and api_candidate[0].start <= fetch_start and api_candidate[-1].end >= cutoff:
            preloaded[symbol] = api_candidate
        elif symbol not in cached_symbols and server_candidate:
            preloaded[symbol] = server_candidate
    fetched_bars, errors = await fetch_bars(
        [symbol for symbol in symbols if symbol not in preloaded],
        fetch_start,
        cutoff,
        cache_dir,
        preloaded=preloaded,
    )
    bars_by_symbol = dict(server_bars)
    bars_by_symbol.update(api_bars)
    bars_by_symbol.update(fetched_bars)
    minimum_open_by_symbol = {
        symbol: min(parse_dt(row["opened_at"]) for row in positions if row["symbol"] == symbol)
        for symbol in symbols
    }
    partial_symbols = {}
    for symbol in symbols:
        bars = bars_by_symbol.get(symbol, [])
        needed_start = minimum_open_by_symbol[symbol] - FIFTEEN_MINUTES
        if not bars or bars[0].start > needed_start or bars[-1].end < cutoff:
            partial_symbols[symbol] = {
                "required_first_bar": needed_start.isoformat(),
                "first_bar": bars[0].start.isoformat() if bars else None,
                "last_bar": bars[-1].end.isoformat() if bars else None,
                "bars": len(bars),
            }

    results: dict[str, Any] = {
        "cutoff": cutoff.isoformat(),
        "timezone": "Asia/Shanghai",
        "rule": {
            "stop_definition": "long=min(current visible 15m low, previous completed 15m low); short=max(current visible 15m high, previous completed 15m high)",
            "entry_partial_5m": "not used for stop-hit scan; first fully completed 5m bar starts after entry",
            "execution": "stop price adjusted by historical median spread, 4bps taker fee",
            "fallback": "preserve each account's actual exit; mark positions still open at cutoff",
        },
        "counts": {
            "positions": len(positions),
            "symbols": len(symbols),
            "symbols_loaded": sum(symbol in bars_by_symbol for symbol in symbols),
            "symbols_partial": len(partial_symbols),
        },
        "partial_symbols": partial_symbols,
        "download_errors": errors,
        "accounts": {},
    }

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in positions:
        grouped[row["run_id"]].append(row)

    for run_id, label in TARGET_RUNS.items():
        rows = grouped[run_id]
        account_equity = latest_equity(equity, run_id, cutoff)
        delta = 0.0
        hit_rows: list[dict[str, Any]] = []
        close_reason_delta: dict[str, float] = defaultdict(float)
        side_delta: dict[str, float] = defaultdict(float)
        symbol_delta: dict[str, float] = defaultdict(float)
        positive_hit_count = 0
        negative_hit_count = 0
        stop_distances: list[float] = []
        for row in rows:
            opened_at = parse_dt(row["opened_at"])
            actual_closed_at = parse_dt(row["closed_at"]) if row.get("closed_at") else None
            is_closed = row.get("status") == "closed" and actual_closed_at is not None and actual_closed_at <= cutoff
            horizon = actual_closed_at if is_closed else cutoff
            bars = bars_by_symbol.get(row["symbol"], [])
            spread = spreads.get(row["symbol"], 0.0)
            stop, stop_meta = visible_structure_stop(row, bars)
            hit_at = first_stop_hit(row, bars, stop, horizon)
            if hit_at is None:
                continue
            stop_exit = executable_price(stop, row["side"], spread)
            stop_pnl = pnl_for(row, stop_exit, True)
            actual_pnl = (
                as_float(row.get("realized_pnl"))
                if is_closed
                else mark_at_cutoff(row, bars, cutoff, spread)
            )
            row_delta = stop_pnl - actual_pnl
            delta += row_delta
            side_delta[row["side"]] += row_delta
            symbol_delta[row["symbol"]] += row_delta
            if row_delta > 0:
                positive_hit_count += 1
            elif row_delta < 0:
                negative_hit_count += 1
            entry = as_float(row["entry_price"])
            distance = abs(entry - stop) / entry if entry else 0.0
            stop_distances.append(distance)
            reason = str(row.get("close_reason") or "open")
            close_reason_delta[reason] += row_delta
            hit_rows.append({
                "position_id": row["position_id"],
                "symbol": row["symbol"],
                "side": row["side"],
                "opened_at": opened_at.isoformat(),
                "hit_at": hit_at.isoformat(),
                "stop_price": stop,
                "entry_price": entry,
                "stop_distance_pct": distance,
                "actual_pnl": actual_pnl,
                "stop_pnl": stop_pnl,
                "delta_pnl": row_delta,
                "original_close_reason": reason,
                **stop_meta,
            })

        actual_equity = as_float(account_equity["equity"])
        results["accounts"][run_id] = {
            "label": label,
            "positions": len(rows),
            "latest_observed_at": account_equity["observed_at"],
            "actual_equity": actual_equity,
            "actual_net_pnl": actual_equity - 1000.0,
            "stop_triggered_positions": len(hit_rows),
            "stop_trigger_rate": len(hit_rows) / len(rows) if rows else None,
            "median_stop_distance_pct": statistics.median(stop_distances) if stop_distances else None,
            "stop_delta_pnl": delta,
            "stop_delta_by_side": dict(sorted(side_delta.items())),
            "stop_delta_by_symbol": dict(sorted(symbol_delta.items(), key=lambda item: item[1])),
            "stop_benefit_count": positive_hit_count,
            "stop_harm_count": negative_hit_count,
            "average_delta_per_trigger": delta / len(hit_rows) if hit_rows else None,
            "counterfactual_equity": actual_equity + delta,
            "counterfactual_net_pnl": actual_equity - 1000.0 + delta,
            "close_reason_delta": dict(sorted(close_reason_delta.items())),
            "stop_hits_by_side": dict(Counter(row["side"] for row in hit_rows)),
            "top_stop_deltas": sorted(hit_rows, key=lambda row: row["delta_pnl"])[:10],
            "top_stop_benefits": sorted(hit_rows, key=lambda row: row["delta_pnl"], reverse=True)[:10],
        }

    print(json.dumps(results, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    asyncio.run(main())
