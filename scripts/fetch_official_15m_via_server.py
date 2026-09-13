#!/usr/bin/env python3
"""Fetch public Binance 15m klines from a network-enabled server container.

The local machine may be region-blocked while the server running the strategy
can reach the same public endpoint.  This helper writes only CSV data to
stdout, so the caller can compress it locally over SSH.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import UTC, datetime
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def fetch(symbol: str, start: datetime, end: datetime, base_url: str) -> list[list[object]]:
    params = urlencode(
        {
            "symbol": symbol,
            "interval": "15m",
            "startTime": int(start.timestamp() * 1000),
            "endTime": int(end.timestamp() * 1000) - 1,
            "limit": 1000,
        }
    )
    request = Request(
        f"{base_url.rstrip('/')}/fapi/v1/klines?{params}",
        headers={"User-Agent": "crypto-momentum-lab-research/1.0"},
    )
    with urlopen(request, timeout=30) as response:
        payload = json.load(response)
    if not isinstance(payload, list):
        raise RuntimeError(f"{symbol}: response is not a list")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--base-url", default="https://fapi.binance.com")
    args = parser.parse_args()
    start = parse_time(args.start)
    end = parse_time(args.end)
    symbols = sorted({item.strip().upper() for item in args.symbols.split(",") if item.strip()})
    writer = csv.writer(sys.stdout)
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
    for index, symbol in enumerate(symbols, start=1):
        try:
            rows = fetch(symbol, start, end, args.base_url)
        except Exception as error:  # noqa: BLE001 - continue other symbols
            print(f"warning: {symbol}: {error}", file=sys.stderr, flush=True)
            continue
        written = 0
        for row in rows:
            if not isinstance(row, list) or len(row) < 7:
                continue
            open_ms, open_price, high_price, low_price, close_price, volume, close_ms = row[:7]
            if not isinstance(open_ms, int) or not isinstance(close_ms, int):
                continue
            candle_start = datetime.fromtimestamp(open_ms / 1000, UTC)
            candle_end = datetime.fromtimestamp((close_ms + 1) / 1000, UTC)
            if not (start <= candle_start < end and candle_end <= end):
                continue
            writer.writerow(
                [
                    symbol,
                    candle_start.isoformat(),
                    candle_end.isoformat(),
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    close_ms,
                ]
            )
            written += 1
        sys.stdout.flush()
        print(f"fetched {index}/{len(symbols)} {symbol} ({written} candles)", file=sys.stderr, flush=True)
        time.sleep(0.05)


if __name__ == "__main__":
    main()
