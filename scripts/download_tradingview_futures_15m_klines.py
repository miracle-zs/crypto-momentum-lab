"""Download a short Binance USD-M 15m tail through TradingView.

This is a research fallback for environments where Binance's Futures REST API
returns HTTP 451.  The script resolves TradingView's ``BINANCE:<symbol>.P``
series, writes the same compact candle schema used by the orderflow replay, and
records the proxy/source distinction in a manifest.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import gzip
import json
import random
import string
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import websockets

INTERVAL = timedelta(minutes=15)
TRADINGVIEW_URL = "wss://data.tradingview.com/socket.io/websocket?from=chart/"
TRADINGVIEW_SYMBOL_ALIASES = {
    # Binance exposes the localized contract code; TradingView transliterates it.
    "龙虾USDT": "LONGXIAUSDT",
}


def parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(UTC)


def session(prefix: str) -> str:
    suffix = "".join(random.choice(string.ascii_lowercase) for _ in range(12))
    return f"{prefix}_{suffix}"


def frame(method: str, params: list[object]) -> str:
    payload = json.dumps({"m": method, "p": params}, separators=(",", ":"))
    return f"~m~{len(payload)}~m~{payload}"


def parse_frames(payload: str) -> Iterable[dict[str, Any]]:
    cursor = 0
    while cursor < len(payload):
        start = payload.find("~m~", cursor)
        if start < 0:
            return
        length_end = payload.find("~m~", start + 3)
        if length_end < 0:
            return
        size = int(payload[start + 3 : length_end])
        body_start = length_end + 3
        body = payload[body_start : body_start + size]
        cursor = body_start + size
        if body.startswith("~h~"):
            continue
        decoded = json.loads(body)
        if isinstance(decoded, dict):
            yield decoded


def parse_symbols(path: Path) -> list[str]:
    symbols = {line.strip() for line in path.read_text(encoding="utf-8").splitlines()}
    return sorted(symbol for symbol in symbols if symbol)


async def fetch_symbol(
    symbol: str,
    *,
    start: datetime,
    end: datetime,
    bar_count: int,
) -> list[tuple[str, str, str, str, str, str, str, str, str]]:
    chart = session("cs")
    chart_symbol = TRADINGVIEW_SYMBOL_ALIASES.get(symbol, symbol)
    tradingview_symbol = f"BINANCE:{chart_symbol}.P"
    resolved = json.dumps(
        {
            "symbol": tradingview_symbol,
            "adjustment": "splits",
            "session": "regular",
        },
        separators=(",", ":"),
        ensure_ascii=False,
    )
    bars: list[list[Any]] = []
    async with websockets.connect(
        TRADINGVIEW_URL,
        origin="https://www.tradingview.com",
        open_timeout=30,
        close_timeout=10,
        max_size=8 * 1024 * 1024,
    ) as websocket:
        await websocket.send(frame("set_auth_token", ["unauthorized_user_token"]))
        await websocket.send(frame("chart_create_session", [chart, ""]))
        await websocket.send(
            frame("resolve_symbol", [chart, "symbol_1", f"={resolved}"])
        )
        await websocket.send(
            frame(
                "create_series",
                [chart, "s1", "s1", "symbol_1", "15", bar_count, ""],
            )
        )
        while True:
            raw = await asyncio.wait_for(websocket.recv(), timeout=30)
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            for message in parse_frames(raw):
                method = message.get("m")
                if method in {
                    "protocol_error",
                    "critical_error",
                    "series_error",
                    "symbol_error",
                }:
                    raise RuntimeError(
                        f"{tradingview_symbol}: {json.dumps(message, ensure_ascii=False)}"
                    )
                if method == "timescale_update":
                    payload = message.get("p") or []
                    update = payload[1] if len(payload) > 1 else {}
                    series = update.get("s1") if isinstance(update, dict) else None
                    if isinstance(series, dict):
                        for item in series.get("s") or []:
                            values = item.get("v") if isinstance(item, dict) else None
                            if isinstance(values, list) and len(values) >= 6:
                                bars.append(values)
                if method == "series_completed":
                    break
            else:
                continue
            break

    rows: list[tuple[str, str, str, str, str, str, str, str, str]] = []
    for values in bars:
        candle_start = datetime.fromtimestamp(float(values[0]), UTC)
        candle_end = candle_start + INTERVAL
        if candle_start < start or candle_end > end:
            continue
        close_ms = int(candle_end.timestamp() * 1000) - 1
        rows.append(
            (
                symbol,
                candle_start.isoformat(),
                candle_end.isoformat(),
                str(values[1]),
                str(values[2]),
                str(values[3]),
                str(values[4]),
                str(values[5]),
                str(close_ms),
            )
        )
    rows.sort(key=lambda row: row[1])
    if not rows:
        raise RuntimeError(f"{tradingview_symbol}: no closed bars in requested range")
    return rows


async def download(args: argparse.Namespace) -> None:
    start = parse_datetime(args.start)
    end = parse_datetime(args.end)
    if end <= start:
        raise ValueError("end must be after start")
    symbols = parse_symbols(args.symbols_file)
    needed_bars = int((end - start) / INTERVAL) + 8
    bar_count = max(args.min_bars, needed_bars)
    semaphore = asyncio.Semaphore(args.workers)

    async def limited(symbol: str):
        async with semaphore:
            for attempt in range(4):
                try:
                    rows = await fetch_symbol(
                        symbol,
                        start=start,
                        end=end,
                        bar_count=bar_count,
                    )
                    print(f"downloaded {symbol} ({len(rows)} candles)")
                    return symbol, rows
                except Exception:
                    if attempt == 3:
                        raise
                    await asyncio.sleep(1.5 * (attempt + 1))
            raise AssertionError("retry loop exhausted")

    results = await asyncio.gather(*(limited(symbol) for symbol in symbols))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".part")
    row_count = 0
    with gzip.open(temporary, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "symbol",
                "candle_start",
                "candle_end",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "volume",
                "close_ms",
            ]
        )
        for _, rows in sorted(results):
            writer.writerows(rows)
            row_count += len(rows)
    temporary.replace(args.output)
    manifest = {
        "generated_at": datetime.now(UTC).isoformat(),
        "source": "TradingView chart proxy for Binance USD-M perpetual series",
        "tradingview_url": TRADINGVIEW_URL,
        "symbol_format": "BINANCE:<symbol>.P",
        "symbol_aliases": TRADINGVIEW_SYMBOL_ALIASES,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "symbols": symbols,
        "interval": "15m",
        "row_count": row_count,
        "bar_count_requested": bar_count,
    }
    manifest_path = args.output.with_suffix(args.output.suffix + ".manifest.json")
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols-file", type=Path, required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=4, choices=range(1, 9))
    parser.add_argument("--min-bars", type=int, default=100)
    return parser


if __name__ == "__main__":
    asyncio.run(download(build_parser().parse_args()))
