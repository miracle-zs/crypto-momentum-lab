#!/usr/bin/env python3
"""Build and cache merged 15s price series from parquet and postgres."""

from __future__ import annotations

import argparse
import csv
import json
import pickle
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from local_optimization.mtm_engine import load_15s_price_series  # noqa: E402

PARQUET_DIR = ROOT_DIR / "local_optimization/data/all_data_parquet/environment=research"
POSTGRES_CSV = ROOT_DIR / "local_optimization/data/postgres_20260919_15s.csv"
DATA_DIR_20 = ROOT_DIR / "local_optimization/data/replay_all_collected_20260920"
DATA_DIR_19 = ROOT_DIR / "local_optimization/data/replay_all_collected_20260919"
DEFAULT_DATA_DIR = DATA_DIR_20 if DATA_DIR_20.exists() else DATA_DIR_19
CACHE_FILE = ROOT_DIR / "local_optimization/data/cache_15s_price_series.pkl"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build 15s price cache for traded symbols."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Replay events directory",
    )
    parser.add_argument(
        "--cache-file",
        type=Path,
        default=CACHE_FILE,
        help="Target cache pickle file path",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force rebuild cache even if file exists",
    )
    args = parser.parse_args()

    # Collect all traded symbols
    symbols: set[str] = set()
    for fname in [
        "raw_opportunities.parquet",
        "opportunities.parquet",
        "opportunity_pool.jsonl",
        "raw_opportunities.jsonl",
        "account_primary_events.csv",
        "account_acc02_events.csv",
    ]:
        fpath = args.data_dir / fname
        if not fpath.exists():
            continue
        if fpath.suffix == ".parquet":
            import pyarrow.parquet as pq

            syms = pq.read_table(fpath, columns=["symbol"])["symbol"].to_pylist()
            symbols.update(s for s in syms if s)
        elif fpath.suffix in (".jsonl", ".ndjson"):
            with fpath.open("r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    sym = row.get("symbol")
                    if sym:
                        symbols.add(sym)
        else:
            with fpath.open("r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    sym = r.get("symbol")
                    if sym:
                        symbols.add(sym)

    manifest_path = args.data_dir / "manifest.json"
    manifest_dict = None
    if manifest_path.exists():
        try:
            with manifest_path.open("r", encoding="utf-8") as f:
                manifest_dict = json.load(f)
        except Exception:
            manifest_dict = None

    if not args.force and args.cache_file.exists():
        try:
            cache_mtime = args.cache_file.stat().st_mtime
            with args.cache_file.open("rb") as f:
                cached = pickle.load(f)

            if not isinstance(cached, dict) or "__metadata__" not in cached:
                print("Cache invalidated: legacy format lacking provenance metadata.")
            else:
                meta = cached["__metadata__"]
                is_valid = True

                # Check manifest content hash and watermark binding
                if manifest_dict is not None:
                    exp_hash = manifest_dict.get("content_hash")
                    if exp_hash and meta.get("source_content_hash") != exp_hash:
                        print(
                            f"Cache invalidated: source content hash mismatch "
                            f"({meta.get('source_content_hash')} != {exp_hash})."
                        )
                        is_valid = False
                    exp_end = manifest_dict.get("watermark_end")
                    if exp_end and meta.get("watermark_end") != exp_end:
                        print("Cache invalidated: watermark_end mismatch.")
                        is_valid = False

                # Check cleaning rules
                expected_rules = "data_complete=True,missing_agg_trade_count=0"
                if meta.get("cleaning_rules") != expected_rules:
                    print("Cache invalidated: cleaning rules mismatch.")
                    is_valid = False

                # Check symbol coverage
                cached_prices = cached["prices"]
                if not (symbols and symbols.issubset(set(cached_prices.keys()))):
                    missing_syms = len(symbols - set(cached_prices.keys()))
                    print(
                        f"Cache invalidated: missing {missing_syms} required symbols."
                    )
                    is_valid = False

                # Check modification timestamps
                if (
                    manifest_path.exists()
                    and manifest_path.stat().st_mtime > cache_mtime
                ):
                    print("Cache invalidated: manifest.json newer than cache file.")
                    is_valid = False

                if POSTGRES_CSV.exists() and POSTGRES_CSV.stat().st_mtime > cache_mtime:
                    print("Cache invalidated: postgres CSV newer than cache file.")
                    is_valid = False

                if is_valid:
                    print(
                        f"Cache file is valid and up-to-date, covering {len(symbols)} "
                        f"symbols with verified manifest hash at {args.cache_file}"
                    )
                    return
        except Exception as e:
            print(f"Cache validation check failed ({e}), rebuilding...")

    print(f"Loading 15s price series from parquet for {len(symbols)} symbols...")
    prices = load_15s_price_series(PARQUET_DIR, symbols)
    print(f"Loaded {len(prices)} symbols from parquet.")

    # Augment with postgres_csv
    if POSTGRES_CSV.exists():
        print("Augmenting with postgres 20260919 15s data...")
        with POSTGRES_CSV.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            pg_data: dict[str, list[tuple[float, float]]] = {}
            for r in reader:
                sym = r["symbol"]
                if sym in symbols and r.get("epoch") and r.get("close_price"):
                    if sym not in pg_data:
                        pg_data[sym] = []
                    pg_data[sym].append((float(r["epoch"]), float(r["close_price"])))

        for sym, pts in pg_data.items():
            pq_t, pq_p = prices.get(sym, ([], []))
            comb = dict(zip(pq_t, pq_p, strict=True))
            for t, p in pts:
                comb[t] = p
            sorted_t = sorted(comb)
            prices[sym] = (sorted_t, [comb[t] for t in sorted_t])
        print("Augmentation complete.")

    cache_payload = {
        "__metadata__": {
            "cache_version": "v2",
            "created_at": datetime.now(UTC).isoformat(),
            "market_data_version": "15s_v1_clean",
            "watermark_start": (
                manifest_dict.get("watermark_start") if manifest_dict else None
            ),
            "watermark_end": (
                manifest_dict.get("watermark_end") if manifest_dict else None
            ),
            "source_content_hash": (
                manifest_dict.get("content_hash") if manifest_dict else None
            ),
            "symbol_count": len(prices),
            "cleaning_rules": "data_complete=True,missing_agg_trade_count=0",
        },
        "prices": prices,
    }

    args.cache_file.parent.mkdir(parents=True, exist_ok=True)
    with args.cache_file.open("wb") as f:
        pickle.dump(cache_payload, f, protocol=pickle.HIGHEST_PROTOCOL)
    size_mb = args.cache_file.stat().st_size / 1024 / 1024
    print(
        f"Saved cache with provenance metadata to {args.cache_file} ({size_mb:.2f} MB)"
    )


if __name__ == "__main__":
    main()
