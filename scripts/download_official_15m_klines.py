from __future__ import annotations

import argparse
import csv
import gzip
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

INTERVAL = timedelta(minutes=15)
MAX_LIMIT = 1500
RETRY_DELAYS = (1.0, 2.0, 5.0, 10.0)


def parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(UTC)


def align_start(value: datetime) -> datetime:
    return value.replace(
        minute=value.minute - value.minute % 15,
        second=0,
        microsecond=0,
    )


def parse_symbols(
    *,
    symbols_file: Path | None,
    states_path: Path | None,
) -> list[str]:
    symbols: set[str] = set()
    if symbols_file is not None:
        for line in symbols_file.read_text(encoding="utf-8").splitlines():
            symbol = line.strip().upper()
            if symbol:
                symbols.add(symbol)
    if states_path is not None:
        with gzip.open(states_path, "rt", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                symbol = row.get("symbol", "").strip().upper()
                if symbol:
                    symbols.add(symbol)
    if not symbols:
        raise ValueError("no symbols were provided")
    return sorted(symbols)


def fetch_page(
    *,
    base_url: str,
    symbol: str,
    start: datetime,
    end: datetime,
    limit: int,
) -> list[list[object]]:
    params = urlencode(
        {
            "symbol": symbol,
            "interval": "15m",
            "startTime": int(start.timestamp() * 1000),
            "endTime": int(end.timestamp() * 1000) - 1,
            "limit": limit,
        }
    )
    request = Request(
        f"{base_url.rstrip('/')}/fapi/v1/klines?{params}",
        headers={"User-Agent": "crypto-momentum-lab-research/1.0"},
    )
    for attempt, delay in enumerate((0.0, *RETRY_DELAYS)):
        if delay:
            time.sleep(delay)
        try:
            with urlopen(request, timeout=30) as response:
                payload = json.load(response)
            if not isinstance(payload, list):
                raise RuntimeError(f"{symbol}: Binance response is not a list")
            return payload
        except HTTPError as error:
            body = error.read().decode("utf-8", errors="replace")
            if error.code == 429 or error.code >= 500:
                if attempt < len(RETRY_DELAYS):
                    print(
                        f"retry {symbol} HTTP {error.code} after transient error",
                        file=sys.stderr,
                        flush=True,
                    )
                    continue
            raise RuntimeError(
                f"{symbol}: Binance HTTP {error.code}: {body[:500]}"
            ) from error
        except (TimeoutError, URLError, OSError) as error:
            if attempt < len(RETRY_DELAYS):
                print(
                    f"retry {symbol} after {type(error).__name__}",
                    file=sys.stderr,
                    flush=True,
                )
                continue
            raise RuntimeError(f"{symbol}: Binance request failed: {error}") from error
    raise AssertionError("retry loop exhausted")


def fetch_symbol(
    *,
    base_url: str,
    symbol: str,
    start: datetime,
    end: datetime,
    request_pause_seconds: float,
    allow_missing_tail: bool = False,
) -> list[tuple[str, str, str, str, str, str, str, str, str]]:
    cursor = start
    effective_start = start
    first_page = True
    rows_by_start: dict[
        datetime, tuple[str, str, str, str, str, str, str, str, str]
    ] = {}
    while cursor < end:
        remaining = int((end - cursor) / INTERVAL)
        limit = min(MAX_LIMIT, max(1, remaining))
        rows = fetch_page(
            base_url=base_url,
            symbol=symbol,
            start=cursor,
            end=end,
            limit=limit,
        )
        if not rows:
            raise RuntimeError(
                f"{symbol}: empty page at {cursor.isoformat()} before {end.isoformat()}"
            )
        parsed: list[
            tuple[datetime, tuple[str, str, str, str, str, str, str, str, str]]
        ] = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 7:
                raise RuntimeError(f"{symbol}: malformed kline row")
            (
                open_ms,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                close_ms,
            ) = row[:7]
            if not isinstance(open_ms, int) or not isinstance(close_ms, int):
                raise RuntimeError(f"{symbol}: kline timestamps are not integers")
            candle_start = datetime.fromtimestamp(open_ms / 1000, UTC)
            candle_end = datetime.fromtimestamp((close_ms + 1) / 1000, UTC)
            if not (start <= candle_start < end and candle_end <= end):
                continue
            values = (
                symbol,
                candle_start.isoformat(),
                candle_end.isoformat(),
                str(open_price),
                str(high_price),
                str(low_price),
                str(close_price),
                str(volume),
                str(close_ms),
            )
            rows_by_start[candle_start] = values
            parsed.append((candle_start, values))
        if not parsed:
            raise RuntimeError(
                f"{symbol}: page contained no rows at {cursor.isoformat()}"
            )
        first_start = min(item[0] for item in parsed)
        if first_start > cursor:
            if not first_page:
                raise RuntimeError(
                    f"{symbol}: API range has a gap; expected {cursor.isoformat()}, "
                    f"got {first_start.isoformat()}"
                )
            # Symbols can be listed after the study window begins. The first
            # official candle is then the correct effective start for that
            # symbol, rather than evidence of an incomplete response.
            effective_start = first_start
        next_cursor = max(item[0] for item in parsed) + INTERVAL
        if next_cursor <= cursor:
            raise RuntimeError(f"{symbol}: pagination did not advance")
        cursor = next_cursor
        first_page = False
        if request_pause_seconds > 0 and cursor < end:
            time.sleep(request_pause_seconds)
        if len(rows) < limit:
            if cursor < end:
                message = (
                    f"{symbol}: short page ended at {cursor.isoformat()}, "
                    f"expected {end.isoformat()}"
                )
                if not allow_missing_tail:
                    raise RuntimeError(message)
                print(f"warning: {message}; keeping available tail", file=sys.stderr)
            break

    expected = {
        effective_start + index * INTERVAL
        for index in range(int((end - effective_start) / INTERVAL))
    }
    missing = expected - rows_by_start.keys()
    if missing:
        preview = ", ".join(item.isoformat() for item in sorted(missing)[:5])
        suffix = "..." if len(missing) > 5 else ""
        message = (
            f"{symbol}: missing {len(missing)} official 15m candles: {preview}{suffix}"
        )
        if not allow_missing_tail:
            raise RuntimeError(message)
        print(f"warning: {message}; keeping available tail", file=sys.stderr)
    return [rows_by_start[key] for key in sorted(rows_by_start)]


def write_symbols(symbols: list[str], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text("".join(f"{symbol}\n" for symbol in symbols), encoding="utf-8")
    temporary.replace(path)


def download(args: argparse.Namespace) -> None:
    start = align_start(parse_datetime(args.start))
    end = align_start(parse_datetime(args.end))
    if end <= start:
        raise ValueError("end must be after start")
    symbols = parse_symbols(
        symbols_file=args.symbols_file,
        states_path=args.states_path,
    )
    if args.max_symbols is not None:
        symbols = symbols[: args.max_symbols]
    if args.symbols_output is not None:
        write_symbols(symbols, args.symbols_output)
    if args.list_only:
        print(json.dumps({"symbols": len(symbols), "path": str(args.symbols_output)}))
        return
    if args.output is None:
        raise ValueError("--output is required unless --list-only is used")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".part")
    manifest = {
        "generated_at": datetime.now(UTC).isoformat(),
        "base_url": args.base_url,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "symbols": symbols,
        "interval": "15m",
        "limit": MAX_LIMIT,
    }
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

        def fetch(
            symbol: str,
        ) -> list[tuple[str, str, str, str, str, str, str, str, str]]:
            return fetch_symbol(
                base_url=args.base_url,
                symbol=symbol,
                start=start,
                end=end,
                request_pause_seconds=args.request_pause_seconds,
                allow_missing_tail=args.allow_missing_tail,
            )

        if args.workers == 1:
            results = ((symbol, fetch(symbol)) for symbol in symbols)
            executor = None
        else:
            executor = ThreadPoolExecutor(max_workers=args.workers)
            futures = {symbol: executor.submit(fetch, symbol) for symbol in symbols}
            results = ((symbol, futures[symbol].result()) for symbol in symbols)
        try:
            for number, (symbol, rows) in enumerate(results, start=1):
                writer.writerows(rows)
                handle.flush()
                print(
                    f"downloaded {number}/{len(symbols)} {symbol} "
                    f"({len(rows)} candles)",
                    file=sys.stderr,
                    flush=True,
                )
        finally:
            if executor is not None:
                executor.shutdown(wait=True, cancel_futures=True)
    temporary.replace(args.output)
    manifest["row_count"] = (
        sum(1 for _ in gzip.open(args.output, "rt", encoding="utf-8", newline="")) - 1
    )
    manifest_path = args.output.with_suffix(args.output.suffix + ".manifest.json")
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download official Binance USD-M 15m klines"
    )
    parser.add_argument("--base-url", default="https://fapi.binance.com")
    parser.add_argument("--symbols-file", type=Path)
    parser.add_argument("--states-path", type=Path)
    parser.add_argument("--symbols-output", type=Path)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--max-symbols", type=int)
    parser.add_argument("--workers", type=int, default=1, choices=range(1, 9))
    parser.add_argument("--request-pause-seconds", type=float, default=0.2)
    parser.add_argument(
        "--allow-missing-tail",
        action="store_true",
        help="keep a symbol's available rows when its listing ends before --end",
    )
    parser.add_argument("--list-only", action="store_true")
    return parser


if __name__ == "__main__":
    download(build_parser().parse_args())
