#!/usr/bin/env python3
"""Download Binance USD-M daily aggTrades and build local 15-second states.

This is deliberately a local research utility.  It reads the public Binance
data archive, never connects to the research server, and writes a compatible
subset of the runtime 15-second state schema.  OHLC, trade count, notional,
and taker-side buy/sell notional are reconstructed from aggTrades.  Quote,
mark-price, and liquidation fields are not present in this source and are
left empty/zero.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


BUCKET_MS = 15_000
RETRY_DELAYS = (1.0, 2.0, 5.0, 10.0)
CSV_FIELDS = (
    "symbol",
    "bucket_start",
    "bucket_end",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "trade_count",
    "trade_notional",
    "aggressive_buy_notional",
    "aggressive_sell_notional",
    "last_bid_price",
    "last_ask_price",
    "spread",
    "midpoint",
    "mark_price",
    "liquidation_count",
    "liquidation_notional",
    "data_complete",
    "missing_agg_trade_count",
)


def parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"timestamp must include a timezone: {value!r}")
    return parsed.astimezone(UTC)


def parse_symbols(args: argparse.Namespace) -> list[str]:
    symbols: set[str] = {value.strip().upper() for value in args.symbols if value.strip()}
    if args.symbols_file is not None:
        opener = gzip.open if args.symbols_file.suffix == ".gz" else open
        with opener(args.symbols_file, "rt", encoding="utf-8", newline="") as handle:
            first = True
            for line in handle:
                value = line.strip()
                if not value:
                    continue
                if first and value.lower() == "symbol":
                    first = False
                    continue
                first = False
                if "," in value:
                    value = value.split(",", 1)[0]
                if value:
                    symbols.add(value.upper())
    if not symbols:
        raise ValueError("no symbols supplied")
    result = sorted(symbols)
    if args.max_symbols is not None:
        result = result[: args.max_symbols]
    return result


def required_days(start: datetime, end: datetime) -> list[date]:
    current = start.date()
    last = (end - timedelta(microseconds=1)).date()
    days: list[date] = []
    while current <= last:
        days.append(current)
        current += timedelta(days=1)
    return days


def archive_url(base_url: str, symbol: str, day: date) -> str:
    day_text = day.isoformat()
    encoded_symbol = quote(symbol, safe="")
    return (
        f"{base_url.rstrip('/')}/data/futures/um/daily/aggTrades/"
        f"{encoded_symbol}/{encoded_symbol}-aggTrades-{day_text}.zip"
    )


def archive_path(cache_dir: Path, symbol: str, day: date) -> Path:
    return cache_dir / symbol / f"{symbol}-aggTrades-{day.isoformat()}.zip"


def valid_zip(path: Path) -> bool:
    try:
        with zipfile.ZipFile(path) as archive:
            return bool(archive.namelist()) and archive.testzip() is None
    except (OSError, zipfile.BadZipFile):
        return False


def download_archive(
    *,
    base_url: str,
    symbol: str,
    day: date,
    target: Path,
    force: bool,
) -> dict[str, object]:
    if target.exists() and not force and valid_zip(target):
        return {"symbol": symbol, "day": day.isoformat(), "status": "cached", "path": str(target)}

    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".part")
    url = archive_url(base_url, symbol, day)
    for attempt, delay in enumerate((0.0, *RETRY_DELAYS)):
        if delay:
            time.sleep(delay)
        try:
            request = Request(
                url,
                headers={"User-Agent": "crypto-momentum-lab-research/1.0"},
            )
            with urlopen(request, timeout=60) as response, temporary.open("wb") as handle:
                while chunk := response.read(1024 * 1024):
                    handle.write(chunk)
            if not valid_zip(temporary):
                raise RuntimeError("downloaded file is not a valid zip")
            temporary.replace(target)
            return {"symbol": symbol, "day": day.isoformat(), "status": "downloaded", "path": str(target)}
        except HTTPError as error:
            if error.code == 404:
                temporary.unlink(missing_ok=True)
                return {
                    "symbol": symbol,
                    "day": day.isoformat(),
                    "status": "not_found",
                    "http_status": error.code,
                }
            if error.code == 429 or error.code >= 500:
                if attempt < len(RETRY_DELAYS):
                    continue
            temporary.unlink(missing_ok=True)
            return {
                "symbol": symbol,
                "day": day.isoformat(),
                "status": "error",
                "http_status": error.code,
                "error": str(error),
            }
        except (OSError, URLError, TimeoutError, RuntimeError) as error:
            if attempt < len(RETRY_DELAYS):
                continue
            temporary.unlink(missing_ok=True)
            return {
                "symbol": symbol,
                "day": day.isoformat(),
                "status": "error",
                "error": f"{type(error).__name__}: {error}",
            }
    raise AssertionError("retry loop exhausted")


@dataclass(slots=True)
class Bucket:
    first_ms: int = 0
    last_ms: int = 0
    open_price: float = math.nan
    high_price: float = math.nan
    low_price: float = math.nan
    close_price: float = math.nan
    trade_count: int = 0
    trade_notional: float = 0.0
    aggressive_buy: float = 0.0
    aggressive_sell: float = 0.0

    def add(self, timestamp_ms: int, price: float, quantity: float, buyer_maker: bool) -> None:
        notional = price * quantity
        if self.trade_count == 0:
            self.first_ms = timestamp_ms
            self.open_price = price
            self.high_price = price
            self.low_price = price
        else:
            self.high_price = max(self.high_price, price)
            self.low_price = min(self.low_price, price)
        self.last_ms = timestamp_ms
        self.close_price = price
        self.trade_count += 1
        self.trade_notional += notional
        if buyer_maker:
            self.aggressive_sell += notional
        else:
            self.aggressive_buy += notional


def csv_member(archive: zipfile.ZipFile) -> str:
    names = [name for name in archive.namelist() if not name.endswith("/")]
    if len(names) != 1:
        raise RuntimeError(f"expected one CSV member, found {len(names)}")
    return names[0]


def aggregate_symbol(
    *,
    symbol: str,
    files: dict[date, Path],
    expected_days: list[date],
    start_ms: int,
    end_ms: int,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    buckets: dict[int, Bucket] = {}
    prior_price = math.nan
    rows_seen = 0
    rows_in_range = 0
    for day in expected_days:
        path = files.get(day)
        if path is None:
            continue
        with zipfile.ZipFile(path) as archive:
            with archive.open(csv_member(archive), "r") as raw_handle:
                handle = (line.decode("utf-8") for line in raw_handle)
                reader = csv.DictReader(handle)
                for row in reader:
                    try:
                        timestamp_ms = int(row["transact_time"])
                        price = float(row["price"])
                        quantity = float(row["quantity"])
                    except (KeyError, TypeError, ValueError):
                        continue
                    if not (math.isfinite(price) and price > 0 and math.isfinite(quantity) and quantity >= 0):
                        continue
                    rows_seen += 1
                    if timestamp_ms < start_ms:
                        prior_price = price
                        continue
                    if timestamp_ms >= end_ms:
                        continue
                    rows_in_range += 1
                    bucket_start = timestamp_ms // BUCKET_MS * BUCKET_MS
                    bucket = buckets.setdefault(bucket_start, Bucket())
                    buyer_maker = str(row.get("is_buyer_maker", "")).strip().lower() == "true"
                    bucket.add(timestamp_ms, price, quantity, buyer_maker)
                    prior_price = price

    expected_bucket_starts = range(start_ms // BUCKET_MS * BUCKET_MS, end_ms, BUCKET_MS)
    all_days_present = len(files) == len(expected_days)
    output: list[dict[str, object]] = []
    carried_price = prior_price
    for bucket_start in expected_bucket_starts:
        bucket = buckets.get(bucket_start)
        if bucket is not None:
            carried_price = bucket.close_price
            open_price = bucket.open_price
            high_price = bucket.high_price
            low_price = bucket.low_price
            close_price = bucket.close_price
            trade_count = bucket.trade_count
            trade_notional = bucket.trade_notional
            aggressive_buy = bucket.aggressive_buy
            aggressive_sell = bucket.aggressive_sell
        else:
            open_price = carried_price
            high_price = carried_price
            low_price = carried_price
            close_price = carried_price
            trade_count = 0
            trade_notional = 0.0
            aggressive_buy = 0.0
            aggressive_sell = 0.0
        has_price = math.isfinite(close_price) and close_price > 0
        bucket_end = bucket_start + BUCKET_MS
        start_text = datetime.fromtimestamp(bucket_start / 1000, UTC).isoformat()
        end_text = datetime.fromtimestamp(bucket_end / 1000, UTC).isoformat()
        output.append(
            {
                "symbol": symbol,
                "bucket_start": start_text,
                "bucket_end": end_text,
                "open_price": open_price if has_price else "",
                "high_price": high_price if has_price else "",
                "low_price": low_price if has_price else "",
                "close_price": close_price if has_price else "",
                "trade_count": trade_count,
                "trade_notional": trade_notional,
                "aggressive_buy_notional": aggressive_buy,
                "aggressive_sell_notional": aggressive_sell,
                "last_bid_price": "",
                "last_ask_price": "",
                "spread": "",
                "midpoint": close_price if has_price else "",
                "mark_price": "",
                "liquidation_count": 0,
                "liquidation_notional": 0.0,
                "data_complete": "t" if all_days_present and has_price else "f",
                "missing_agg_trade_count": 0 if all_days_present else "",
            }
        )
    meta = {
        "symbol": symbol,
        "archive_days_present": len(files),
        "archive_days_expected": len(expected_days),
        "rows_seen": rows_seen,
        "rows_in_requested_range": rows_in_range,
        "state_rows": len(output),
        "priced_rows": sum(1 for row in output if row["close_price"] != ""),
        "complete": all_days_present and all(row["data_complete"] == "t" for row in output),
    }
    return output, meta


def write_manifest(path: Path, payload: dict[str, object]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols-file", type=Path)
    parser.add_argument("--symbols", nargs="*", default=[])
    parser.add_argument("--max-symbols", type=int)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument(
        "--base-url",
        default="https://data.binance.vision",
        help="Binance public data archive host",
    )
    parser.add_argument("--workers", type=int, default=4, choices=range(1, 9))
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    start = parse_datetime(args.start)
    end = parse_datetime(args.end)
    if end <= start:
        raise ValueError("end must be after start")
    if start.timestamp() * 1000 % BUCKET_MS or end.timestamp() * 1000 % BUCKET_MS:
        raise ValueError("start and end must align to 15 seconds")
    symbols = parse_symbols(args)
    days = required_days(start, end)
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    args.cache_dir.mkdir(parents=True, exist_ok=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)

    tasks = [(symbol, day) for symbol in symbols for day in days]
    statuses: dict[tuple[str, date], dict[str, object]] = {}
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                download_archive,
                base_url=args.base_url,
                symbol=symbol,
                day=day,
                target=archive_path(args.cache_dir, symbol, day),
                force=args.force,
            ): (symbol, day)
            for symbol, day in tasks
        }
        for number, future in enumerate(as_completed(futures), start=1):
            symbol, day = futures[future]
            status = future.result()
            statuses[(symbol, day)] = status
            if (
                number == 1
                or number % 25 == 0
                or number == len(tasks)
                or status["status"] in {"not_found", "error"}
            ):
                print(
                    f"archive {number}/{len(tasks)} {symbol} {day.isoformat()} "
                    f"{status['status']}",
                    file=sys.stderr,
                    flush=True,
                )

    temporary = args.output.with_suffix(args.output.suffix + ".part")
    symbols_meta: list[dict[str, object]] = []
    row_count = 0
    with gzip.open(temporary, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for number, symbol in enumerate(symbols, start=1):
            files = {
                day: archive_path(args.cache_dir, symbol, day)
                for day in days
                if statuses.get((symbol, day), {}).get("status") in {"cached", "downloaded"}
                and archive_path(args.cache_dir, symbol, day).exists()
            }
            rows, meta = aggregate_symbol(
                symbol=symbol,
                files=files,
                expected_days=days,
                start_ms=start_ms,
                end_ms=end_ms,
            )
            writer.writerows(rows)
            row_count += len(rows)
            symbols_meta.append(meta)
            if number == 1 or number % 25 == 0 or number == len(symbols):
                print(
                    f"aggregated {number}/{len(symbols)} {symbol} "
                    f"rows={len(rows)} priced={meta['priced_rows']}",
                    file=sys.stderr,
                    flush=True,
                )
    temporary.replace(args.output)

    status_counts: dict[str, int] = {}
    for status in statuses.values():
        name = str(status["status"])
        status_counts[name] = status_counts.get(name, 0) + 1
    manifest = {
        "generated_at": datetime.now(UTC).isoformat(),
        "source": "Binance USD-M public daily aggTrades archive",
        "base_url": args.base_url,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "interval": "15s",
        "symbols": symbols,
        "row_count": row_count,
        "archive_status_counts": status_counts,
        "reconstructed_fields": [
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "trade_count",
            "trade_notional",
            "aggressive_buy_notional",
            "aggressive_sell_notional",
        ],
        "unavailable_or_unreconstructed_fields": [
            "last_bid_price",
            "last_ask_price",
            "spread",
            "mark_price",
            "liquidation_count",
            "liquidation_notional",
        ],
        "symbols_meta": symbols_meta,
    }
    write_manifest(args.output.with_suffix(args.output.suffix + ".manifest.json"), manifest)
    print(json.dumps({
        "symbols": len(symbols),
        "rows": row_count,
        "archive_status_counts": status_counts,
        "complete_symbols": sum(bool(item["complete"]) for item in symbols_meta),
        "output": str(args.output),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
