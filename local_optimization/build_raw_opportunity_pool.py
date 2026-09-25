#!/usr/bin/env python3
"""Build and authenticate the parameter-independent candidate opportunity pool.

Extracts, deduplicates, and validates all raw market impulse opportunities
across the complete 18-day high-frequency research datasets (2026-09-03 to 2026-09-20)
directly from native 15s market data/features, covering the complete grid window space
(w in [2, 3, 4] x c in [1, 2, 3]) with zero feature fabrication, strict numerical
validation, and zero future lookahead bias in deduplication.
"""

from __future__ import annotations

import argparse
import bisect
import json
import math
import multiprocessing as mp
import os
import pickle
import sys
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq

# Ensure repository root is on sys.path
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from crypto_momentum_lab.strategies.order_flow_impulse.event_study import (  # noqa: E402
    OrderFlowDirection,
    OrderFlowImpulseConfig,
    find_order_flow_impulses,
)
from local_optimization.opportunity import (  # noqa: E402
    OpportunityPoolManifest,
    OpportunityStatus,
    RawOpportunity,
    compute_pool_content_hash,
    generate_opportunity_id,
    parse_utc_timestamp,
    validate_opportunity_pool,
)
from scripts.optimize_local_live_constrained import (  # noqa: E402
    build_notional_volume_ratio_lookup,
    confirmation_minimum,
    event_entry_price,
)
from scripts.optimize_research_orderflow import (  # noqa: E402
    STATE_COLUMNS,
    split_contiguous_states,
    state_from_row,
)

DEFAULT_PARQUET_DIR = (
    ROOT_DIR / "local_optimization/data/all_data_parquet/environment=research"
)
DEFAULT_OUTPUT_DIR = ROOT_DIR / "local_optimization/data/replay_all_collected_20260920"
DEFAULT_PRICE_CACHE_PATH = (
    ROOT_DIR / "local_optimization/data/cache_15s_price_series.pkl"
)

# Grid minimum search thresholds
GRID_IMPULSE_WINDOWS = (2, 3, 4)
GRID_CONFIRMATIONS = (1, 2, 3)
GRID_MIN_RETURN_PCT = Decimal("0.0040")  # 0.40% minimum
GRID_MIN_IMBALANCE = Decimal("0.30")
GRID_MIN_INTENSITY = Decimal("1.5")


from local_optimization.mtm_engine import load_cached_price_series  # noqa: E402


def save_price_series_cache(
    price_cache: dict[str, tuple[list[float], list[float]]],
    cache_path: Path,
    manifest: Any | None = None,
) -> None:
    """Save 15s price trajectories to disk with authenticated manifest metadata."""
    meta = {
        "format_version": 2,
        "created_at": datetime.now(tz=UTC).isoformat(),
        "source_content_hash": getattr(manifest, "content_hash", None),
        "watermark_end": (
            getattr(manifest, "watermark_end", None).isoformat()
            if getattr(manifest, "watermark_end", None)
            else None
        ),
        "symbol_count": len(price_cache),
        "cleaning_rules": "data_complete=True,missing_agg_trade_count=0",
    }
    payload = {"__metadata__": meta, "prices": price_cache}

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("wb") as f:
        pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)
    print(
        f"Saved price cache for {len(price_cache)} symbols to {cache_path} "
        f"({cache_path.stat().st_size / 1024 / 1024:.2f} MB)"
    )


def build_and_save_full_price_cache(
    parquet_dir: Path,
    cache_path: Path,
    manifest: Any | None = None,
) -> dict[str, tuple[list[float], list[float]]]:
    """Scan all parquet files and build 15s price trajectories for all symbols."""
    print(f"Building complete 15s price cache from {parquet_dir}...")
    t0 = time.time()
    price_cache: dict[str, tuple[list[float], list[float]]] = {}
    try:
        import duckdb

        parquet_pattern = str(parquet_dir / "**/*.parquet")
        con = duckdb.connect()
        con.execute("PRAGMA threads=8;")
        query = """
            SELECT 
                symbol, 
                list(epoch(bucket_start) + 15.0 ORDER BY bucket_start) as ts,
                list(CAST(close_price AS DOUBLE) ORDER BY bucket_start) as ps
            FROM read_parquet(?)
            WHERE data_complete AND missing_agg_trade_count = 0 AND close_price IS NOT NULL
            GROUP BY symbol
        """
        rows = con.execute(query, [parquet_pattern]).fetchall()
        for row in rows:
            sym = str(row[0])
            ts = [float(x) for x in row[1]]
            ps = [float(x) for x in row[2]]
            price_cache[sym] = (ts, ps)
        print(
            f"DuckDB accelerated scan: {len(price_cache)} symbols processed in {time.time() - t0:.2f}s."
        )
    except Exception as e:
        print(f"DuckDB scan fallback ({e}), falling back to PyArrow dataset scanner...")
        dataset = ds.dataset(str(parquet_dir), format="parquet")
        filter_expr = (
            ds.field("data_complete")
            & (ds.field("missing_agg_trade_count") == 0)
            & ds.field("close_price").is_valid()
        )
        scanner = dataset.scanner(
            columns=["symbol", "bucket_start", "close_price"],
            filter=filter_expr,
        )
        tbl = scanner.to_table()
        df = tbl.to_pandas()
        df["epoch"] = (
            df["bucket_start"].values.astype("datetime64[s]").astype("float64") + 15.0
        )
        price_cache = {}
        for sym, group in df.groupby("symbol"):
            sorted_group = group.sort_values("epoch")
            ts = sorted_group["epoch"].astype(float).tolist()
            ps = sorted_group["close_price"].astype(float).tolist()
            price_cache[str(sym)] = (ts, ps)

    save_price_series_cache(price_cache, cache_path, manifest=manifest)
    return price_cache


CandleTuple = tuple[float, float, float, float, float, float]


def build_candles_from_price_series(
    price_cache: dict[str, tuple[list[float], list[float]]],
) -> dict[str, list[CandleTuple]]:
    """Pre-aggregate 15m candles (start, end, open, high, low, close) per symbol."""
    candles_by_symbol: dict[str, list[CandleTuple]] = {}
    for sym, (ts, ps) in price_cache.items():
        c_list: list[tuple[float, float, float, float, float, float]] = []
        i = 0
        n = len(ts)
        while i < n:
            # Each tick ts[i] is formed at the end of its 15s bucket.
            # A 15m candle ends at b_end (multiple of 900.0)
            b_end = math.ceil(ts[i] / 900.0) * 900.0
            b_start = b_end - 900.0
            c_open = ps[i]
            c_high = ps[i]
            c_low = ps[i]
            c_close = ps[i]
            while i < n and ts[i] <= b_end:
                p = ps[i]
                if p > c_high:
                    c_high = p
                if p < c_low:
                    c_low = p
                c_close = p
                i += 1
            c_list.append((b_start, b_end, c_open, c_high, c_low, c_close))
        candles_by_symbol[sym] = c_list
    return candles_by_symbol


def simulate_opportunity_exit(
    entry_epoch: float,
    entry_price: float,
    ts: list[float],
    ps: list[float],
    candles: list[tuple[float, float, float, float, float, float]],
    *,
    decision_pct: float = 0.001,
    recovery_pct: float = 0.0088,
    grace_bars: int = 8,
    fee_rate: float = 0.0005,
    slippage_rate: float = 0.0002,
    funding_cost: float = 0.0,
    notional: float = 100.0,
) -> tuple[float | None, float | None, str, float | None, str, float | None]:
    """Deterministic 15m candle-exit simulation strictly bounded by available data.

    Returns:
        (exit_time, exit_price, exit_reason, net_pnl, status, exit_submitted_time)
    """
    max_market_time = ts[-1] if ts else entry_epoch
    first_eligible_start = math.floor(entry_epoch / 900.0) * 900.0 + 900.0
    c_starts = [c[0] for c in candles]
    c_idx = bisect.bisect_left(c_starts, first_eligible_start)

    exit_time: float | None = None
    exit_submitted_time: float | None = None
    exit_price: float | None = None
    exit_reason = "open_at_data_end"

    for c in candles[c_idx:]:
        _c_start, c_end, c_open, _c_high, _c_low, c_close = c
        if c_end > max_market_time:
            # Candle closes beyond available market data
            break
        if c_close >= c_open:
            continue
        # Bearish candle trigger: exit order is submitted at candle end
        exit_submitted_time = c_end
        if c_close >= entry_price * (1.0 + decision_pct):
            exit_time = c_end
            exit_price = c_close
            exit_reason = "candle_15m_bearish"
            break

        # Grace period
        rec_price = entry_price * (1.0 + recovery_pct)
        timeout = c_end + 900.0 * grace_bars
        t_left = bisect.bisect_left(ts, c_end + 1e-6)
        t_right = bisect.bisect_left(ts, timeout + 1e-6)
        hit_recovery = False
        for k in range(t_left, min(t_right, len(ps))):
            if ps[k] >= rec_price:
                exit_time = ts[k]
                exit_price = rec_price
                exit_reason = f"candle_15m_bearish_grace_limit_{grace_bars}"
                hit_recovery = True
                break
        if hit_recovery:
            break
        else:
            # Strictly verify timeout is within available market data
            if timeout <= max_market_time:
                exit_time = timeout
                pos = min(t_right, len(ps) - 1)
                exit_price = ps[pos] if pos >= 0 else c_close
                exit_reason = f"candle_15m_grace_timeout_{grace_bars}"
                break
            else:
                # Still in grace period when market data terminates
                exit_time = None
                exit_submitted_time = None
                exit_price = None
                exit_reason = "open_at_data_end"
                break

    if exit_time is not None and exit_price is not None and exit_price > 0:
        ratio = exit_price / entry_price
        gross_pnl = notional * (ratio - 1.0)
        fee = notional * fee_rate + notional * ratio * fee_rate
        slippage = notional * slippage_rate + notional * ratio * slippage_rate
        net_pnl = gross_pnl - fee - slippage - funding_cost
        return (
            exit_time,
            exit_price,
            exit_reason,
            net_pnl,
            OpportunityStatus.EXITED.value,
            exit_submitted_time,
        )
    return None, None, "open_at_data_end", None, OpportunityStatus.DETECTED.value, None


def _process_date_worker(
    args: tuple[
        Path,
        Path | None,
        dict[str, tuple[list[float], list[float]]],
        dict[str, list[CandleTuple]],
    ],
) -> list[dict[str, Any]]:
    """Worker function to process one date directory with cross-day warmup."""
    date_dir, prev_date_dir, price_cache, candles_by_symbol = args
    files = sorted(date_dir.glob("**/*.parquet"))
    if not files:
        return []

    date_str = date_dir.name.split("=")[-1]
    date_start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    date_end = date_start + timedelta(days=1)

    states_by_key = {}
    states_by_symbol: dict[str, list[Any]] = {}

    # Load warmup state from previous day (last 45m of previous day) to ensure
    # continuous 15s state sequences and valid 5m/30m volume ratio lookup across
    # midnight boundaries
    if prev_date_dir is not None and prev_date_dir.exists():
        loaded_prev = False
        try:
            import duckdb

            con = duckdb.connect()
            cols_sql = ", ".join(STATE_COLUMNS)
            query = f"""
                SELECT {cols_sql}
                FROM read_parquet(?)
                WHERE data_complete AND missing_agg_trade_count = 0 
                  AND (close_price IS NOT NULL OR midpoint IS NOT NULL)
                  AND bucket_start >= ?
            """
            prev_pattern = str(prev_date_dir / "hour=2[23]/**/*.parquet")
            warmup_cutoff = (date_start - timedelta(minutes=45)).isoformat()
            prev_table = con.execute(query, [prev_pattern, warmup_cutoff]).arrow().read_all()
            for r in prev_table.to_pylist():
                s = state_from_row(r)
                states_by_key[(s.symbol, s.bucket_start)] = s
                states_by_symbol.setdefault(s.symbol, []).append(s)
            loaded_prev = True
        except Exception:
            loaded_prev = False

        if not loaded_prev:
            prev_files = sorted(prev_date_dir.glob("hour=2[23]/**/*.parquet"))
            for f in prev_files:
                try:
                    tbl = pq.ParquetFile(f).read(columns=list(STATE_COLUMNS))
                except Exception:
                    continue
                for r in tbl.to_pylist():
                    is_complete = bool(r.get("data_complete"))
                    missing_agg = int(r.get("missing_agg_trade_count") or 0)
                    if not is_complete or missing_agg != 0:
                        continue
                    s = state_from_row(r)
                    if s.close_price is None and s.midpoint is None:
                        continue
                    if s.bucket_start >= date_start - timedelta(minutes=45):
                        states_by_key[(s.symbol, s.bucket_start)] = s
                        states_by_symbol.setdefault(s.symbol, []).append(s)

    loaded_current = False
    try:
        import duckdb

        con = duckdb.connect()
        cols_sql = ", ".join(STATE_COLUMNS)
        query = f"""
            SELECT {cols_sql}
            FROM read_parquet(?)
            WHERE data_complete AND missing_agg_trade_count = 0 
              AND (close_price IS NOT NULL OR midpoint IS NOT NULL)
        """
        day_pattern = str(date_dir / "**/*.parquet")
        table = con.execute(query, [day_pattern]).arrow().read_all()
        for r in table.to_pylist():
            s = state_from_row(r)
            states_by_key[(s.symbol, s.bucket_start)] = s
            states_by_symbol.setdefault(s.symbol, []).append(s)
        loaded_current = True
    except Exception:
        loaded_current = False

    if not loaded_current:
        for f in files:
            try:
                tbl = pq.ParquetFile(f).read(columns=list(STATE_COLUMNS))
            except Exception:
                continue
            for r in tbl.to_pylist():
                is_complete = bool(r.get("data_complete"))
                missing_agg = int(r.get("missing_agg_trade_count") or 0)
                if not is_complete or missing_agg != 0:
                    continue
                s = state_from_row(r)
                if s.close_price is None and s.midpoint is None:
                    continue
                states_by_key[(s.symbol, s.bucket_start)] = s
                states_by_symbol.setdefault(s.symbol, []).append(s)

    if not states_by_key:
        return []

    ordered_states = sorted(
        states_by_key.values(), key=lambda s: (s.bucket_start, s.symbol)
    )
    segments = split_contiguous_states(ordered_states, minimum_buckets=10)
    vol_lookup = build_notional_volume_ratio_lookup(states_by_symbol)

    extracted: list[dict[str, Any]] = []
    for w in GRID_IMPULSE_WINDOWS:
        for c in GRID_CONFIRMATIONS:
            cfg = OrderFlowImpulseConfig(
                impulse_window_buckets=w,
                baseline_window_buckets=4,
                breakout_window_buckets=4,
                min_return_pct=GRID_MIN_RETURN_PCT,
                min_aggressive_imbalance=GRID_MIN_IMBALANCE,
                min_notional_intensity=GRID_MIN_INTENSITY,
                confirmation_buckets=c,
                cooldown_buckets=0,
                forward_horizon_buckets=(4,),
            )
            for seg in segments:
                for ev in find_order_flow_impulses(seg, cfg):
                    # Strictly filter: worker only emits impulses detected on
                    # its assigned date
                    if not (date_start <= ev.detected_at < date_end):
                        continue
                    if ev.direction is not OrderFlowDirection.UP:
                        continue
                    c_min = confirmation_minimum(
                        ev, confirmation_buckets=c, state_by_key=states_by_key
                    )
                    if c_min is None or c_min < GRID_MIN_IMBALANCE:
                        continue
                    v_ratio = vol_lookup.get((ev.symbol, ev.detected_at))
                    if v_ratio is None or not math.isfinite(float(v_ratio)):
                        continue
                    p_entry, t_entry = event_entry_price(ev, states_by_key)
                    if p_entry is None or p_entry <= 0:
                        continue
                    sym = ev.symbol
                    if sym not in price_cache or sym not in candles_by_symbol:
                        continue

                    ts, ps = price_cache[sym]
                    candles = candles_by_symbol[sym]
                    ext_t, ext_p, reason, pnl, exit_status, ext_sub_t = (
                        simulate_opportunity_exit(
                            t_entry.timestamp(),
                            float(p_entry),
                            ts,
                            ps,
                            candles,
                            fee_rate=0.0005,
                            slippage_rate=0.0002,
                            funding_cost=0.0,
                        )
                    )
                    if exit_status == OpportunityStatus.EXITED.value:
                        if ext_p is None or ext_p <= 0 or not math.isfinite(ext_p):
                            continue
                        exit_t_iso = (
                            datetime.fromtimestamp(ext_t, tz=UTC).isoformat()
                            if ext_t is not None
                            else None
                        )
                        exit_sub_t_iso = (
                            datetime.fromtimestamp(ext_sub_t, tz=UTC).isoformat()
                            if ext_sub_t is not None
                            else exit_t_iso
                        )
                        exit_p_val = float(ext_p)
                        pnl_val = float(pnl) if pnl is not None else None
                        status_val = OpportunityStatus.EXITED.value
                    else:
                        exit_t_iso = None
                        exit_sub_t_iso = None
                        exit_p_val = None
                        pnl_val = None
                        status_val = OpportunityStatus.DETECTED.value

                    opp_id = generate_opportunity_id(
                        sym, ev.detected_at.timestamp(), w, c, "LONG"
                    )
                    extracted.append(
                        {
                            "opportunity_id": opp_id,
                            "symbol": sym,
                            "direction": "LONG",
                            "detected_at": ev.detected_at.isoformat(),
                            "detected_epoch": float(ev.detected_at.timestamp()),
                            "entry_eligible_at": t_entry.isoformat(),
                            "entry_reference_price": float(p_entry),
                            "impulse_window_buckets": int(w),
                            "confirmation_buckets": int(c),
                            "impulse_return_pct": float(ev.impulse_return_pct) * 100.0,
                            "aggressive_imbalance": float(ev.aggressive_imbalance),
                            "confirmation_min_imbalance": float(c_min),
                            "notional_intensity": float(ev.notional_intensity),
                            "volume_ratio": float(v_ratio),
                            "exit_time": exit_t_iso,
                            "exit_submitted_at": exit_sub_t_iso,
                            "exit_price": exit_p_val,
                            "exit_rule": reason,
                            "fee_rate": 0.0005,
                            "slippage_rate": 0.0002,
                            "funding_cost_usdt": 0.0,
                            "net_pnl_usdt": pnl_val,
                            "status": status_val,
                            "strategy_version": "v1.0",
                            "market_data_version": "v1.0",
                        }
                    )
    return extracted


def build_raw_opportunity_pool(
    parquet_dir: Path,
    output_dir: Path,
    *,
    price_cache_path: Path | None = None,
    watermark_start: datetime | None = None,
    watermark_end: datetime | None = None,
    workers: int | None = None,
    rebuild_price_cache: bool = False,
) -> tuple[list[RawOpportunity], OpportunityPoolManifest]:
    """Extract authenticated candidate opportunity pool from native market data."""
    output_dir.mkdir(parents=True, exist_ok=True)
    c_path = price_cache_path or DEFAULT_PRICE_CACHE_PATH

    # 1. Ensure price series cache
    if rebuild_price_cache or not c_path.exists():
        price_cache = build_and_save_full_price_cache(parquet_dir, c_path)
    else:
        print(f"Loading price series cache from {c_path}...")
        price_cache = load_cached_price_series(c_path, expected_manifest=None)

    # Pre-aggregate candles
    candles_by_symbol = build_candles_from_price_series(price_cache)

    # 2. Discover date directories
    date_dirs = sorted([d for d in parquet_dir.glob("date=*") if d.is_dir()])
    if not date_dirs:
        raise FileNotFoundError(f"No date partitions found in {parquet_dir}")

    print(
        f"Extracting native opportunities across {len(date_dirs)} dates "
        f"({date_dirs[0].name} to {date_dirs[-1].name})..."
    )

    n_workers = workers or min(8, os.cpu_count() or 4)
    tasks = []
    for i, d in enumerate(date_dirs):
        prev_d = None
        if i > 0:
            d_date = datetime.strptime(d.name.split("=")[-1], "%Y-%m-%d").date()
            prev_cand_date = datetime.strptime(
                date_dirs[i - 1].name.split("=")[-1], "%Y-%m-%d"
            ).date()
            if d_date - prev_cand_date == timedelta(days=1):
                prev_d = date_dirs[i - 1]
        tasks.append((d, prev_d, price_cache, candles_by_symbol))

    t0 = time.perf_counter()
    all_extracted: list[dict[str, Any]] = []
    if n_workers > 1 and len(tasks) > 1:
        with mp.Pool(processes=n_workers) as pool:
            for date_res in pool.imap_unordered(_process_date_worker, tasks):
                all_extracted.extend(date_res)
    else:
        for t in tasks:
            all_extracted.extend(_process_date_worker(t))
    t1 = time.perf_counter()
    print(
        f"Extracted {len(all_extracted):,} candidate events in {t1 - t0:.2f}s "
        f"using {n_workers} workers."
    )

    if not all_extracted:
        raise ValueError("No valid opportunities extracted from market data.")

    # 3. Deduplicate strictly by signal identity
    # Strictly prohibited from sorting by net_pnl_usdt to prevent lookahead bias
    df = pd.DataFrame(all_extracted)
    dedup_subset = [
        "symbol",
        "detected_epoch",
        "impulse_window_buckets",
        "confirmation_buckets",
        "direction",
    ]
    # Sort strictly chronologically, then by opportunity_id
    df = (
        df.sort_values(by=["detected_epoch", "opportunity_id"])
        .drop_duplicates(subset=dedup_subset, keep="first")
        .reset_index(drop=True)
    )

    print(
        f"Deduplicated to {len(df):,} parameter-independent RawOpportunities "
        f"across {df['symbol'].nunique()} symbols."
    )

    opportunities = [
        RawOpportunity.from_dict(row.to_dict()) for _, row in df.iterrows()
    ]

    # Sort opportunities chronologically
    opportunities.sort(key=lambda opp: (opp.detected_epoch, opp.opportunity_id))

    # 4. Watermarks and Manifest
    w_start = watermark_start or min(opp.detected_at for opp in opportunities)
    max_market_epoch = max(ts[-1] for ts, _ in price_cache.values() if ts)
    max_market_dt = datetime.fromtimestamp(max_market_epoch, tz=UTC)
    w_end = watermark_end or max_market_dt

    content_hash = compute_pool_content_hash(opportunities)
    manifest = OpportunityPoolManifest(
        snapshot_id=f"raw_opportunity_pool_{w_end.strftime('%Y%m%d')}",
        created_at=datetime.now(UTC),
        symbol_count=len({opp.symbol for opp in opportunities}),
        row_count=len(opportunities),
        watermark_start=w_start,
        watermark_end=w_end,
        content_hash=content_hash,
        pool_type="raw_parameter_independent",
        strategy_version="v1.0",
        schema_version="1.0",
    )

    # 5. Strict Gatekeeper Validation
    errors = validate_opportunity_pool(opportunities, manifest)
    if errors:
        err_msg = "; ".join(errors)
        raise ValueError(f"Built opportunity pool failed validation: {err_msg}")

    # 6. Save persistent files
    parquet_path = output_dir / "raw_opportunities.parquet"
    records = [opp.to_dict() for opp in opportunities]
    pd.DataFrame(records).to_parquet(parquet_path, index=False)
    print(f"Saved {len(opportunities):,} opportunities to {parquet_path}")

    jsonl_path = output_dir / "opportunity_pool.jsonl"
    with jsonl_path.open("w", encoding="utf-8") as f:
        for opp in opportunities:
            f.write(json.dumps(opp.to_dict(), ensure_ascii=False) + "\n")
    print(f"Saved {len(opportunities):,} opportunities to {jsonl_path}")

    manifest_path = output_dir / "manifest.json"
    with manifest_path.open("w", encoding="utf-8") as f:
        json.dump(manifest.to_dict(), f, indent=2, ensure_ascii=False)
    print(f"Saved manifest with SHA256 {content_hash[:16]}... to {manifest_path}")

    # 7. Update price cache metadata with finalized manifest
    save_price_series_cache(price_cache, c_path, manifest=manifest)

    return opportunities, manifest


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build authenticated RawOpportunity pool from native market data."
    )
    parser.add_argument(
        "--parquet-dir",
        type=Path,
        default=DEFAULT_PARQUET_DIR,
        help="Root path to native 15s research parquets",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Destination directory for opportunity pool and manifest",
    )
    parser.add_argument(
        "--price-cache",
        type=Path,
        default=DEFAULT_PRICE_CACHE_PATH,
        help="Path to 15s price series cache",
    )
    parser.add_argument(
        "--rebuild-price-cache",
        action="store_true",
        help="Rebuild 15s price cache from parquets",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Worker processes for multiprocessing",
    )
    parser.add_argument(
        "--watermark-start",
        type=str,
        default=None,
        help="Global start watermark (optional)",
    )
    parser.add_argument(
        "--watermark-end",
        type=str,
        default=None,
        help="Global end watermark (optional)",
    )
    args = parser.parse_args()

    w_start = (
        parse_utc_timestamp(args.watermark_start) if args.watermark_start else None
    )
    w_end = parse_utc_timestamp(args.watermark_end) if args.watermark_end else None

    build_raw_opportunity_pool(
        parquet_dir=args.parquet_dir,
        output_dir=args.output_dir,
        price_cache_path=args.price_cache,
        watermark_start=w_start,
        watermark_end=w_end,
        workers=args.workers,
        rebuild_price_cache=args.rebuild_price_cache,
    )
    print("✅ RawOpportunity pool successfully built, verified, and certified!")


if __name__ == "__main__":
    main()
