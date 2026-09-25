"""Mark-to-market replay engine.

Reconstructs continuous, true account equity curves from discrete trade records
and high-frequency (15-second) market price streams.
Calculates intra-trade floating PnL, active position margin, and true path risk.
"""

from __future__ import annotations

import bisect
import math
import pickle
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from local_optimization.equity import (
    EquityMetrics,
    EquityPoint,
    calc_cdar,
    calc_ulcer_index,
    compute_drawdown_series,
    evaluate_equity_curve,
)


@dataclass(frozen=True)
class TradeRecord:
    """Standard trade record for MTM reconstruction."""

    trade_id: str
    symbol: str
    entry_time: datetime
    entry_price: float
    exit_time: datetime | None = None
    exit_submitted_time: datetime | None = None
    exit_price: float | None = None
    notional_usdt: float = 100.0
    leverage: float = 5.0
    fee_rate: float = 0.0005  # 0.05% per side
    # Default 0.0; populated to 0.0002 by SimulationLedger / pipeline
    slippage_rate: float = 0.0
    funding_cost_usdt: float = 0.0
    direction: str = "LONG"
    net_pnl_usdt: float | None = None
    is_open: bool = False

    @property
    def quantity(self) -> float:
        if self.entry_price > 0:
            return self.notional_usdt / self.entry_price
        return 0.0

    @property
    def initial_margin_usdt(self) -> float:
        return self.notional_usdt / self.leverage

    @property
    def total_fee_usdt(self) -> float:
        if self.exit_price is not None and self.entry_price > 0:
            ratio = self.exit_price / self.entry_price
            return self.notional_usdt * self.fee_rate * (1.0 + ratio)
        return self.notional_usdt * self.fee_rate * 2.0

    @property
    def total_slippage_usdt(self) -> float:
        if self.exit_price is not None and self.entry_price > 0:
            ratio = self.exit_price / self.entry_price
            return self.notional_usdt * self.slippage_rate * (1.0 + ratio)
        return self.notional_usdt * self.slippage_rate * 2.0

    @property
    def calculated_net_pnl(self) -> float:
        if self.net_pnl_usdt is not None:
            return self.net_pnl_usdt
        if self.exit_price is None or self.entry_price <= 0:
            return 0.0
        if self.direction == "LONG":
            gross = (
                self.notional_usdt
                * (self.exit_price - self.entry_price)
                / self.entry_price
            )
        else:
            gross = (
                self.notional_usdt
                * (self.entry_price - self.exit_price)
                / self.entry_price
            )
        return (
            gross
            - self.total_fee_usdt
            - self.total_slippage_usdt
            - self.funding_cost_usdt
        )


def parse_timestamp(value: Any) -> datetime:
    """Parse string or timestamp to UTC datetime."""
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    s = str(value).replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    return dt.astimezone(UTC) if dt.tzinfo else dt.replace(tzinfo=UTC)


def load_trades_from_csv(csv_path: Path) -> list[TradeRecord]:
    """Load and parse trades from a baseline_events.csv file, including open trades."""
    import csv

    trades: list[TradeRecord] = []
    with csv_path.open("r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            if not row.get("entry_at"):
                continue
            entry_t = parse_timestamp(row["entry_at"])
            entry_p = float(row["entry_price"])
            is_closed = str(row.get("closed", "")).lower() in {"true", "1"} and bool(
                row.get("exit_at")
            )
            exit_t = parse_timestamp(row["exit_at"]) if is_closed else None
            exit_p = (
                float(row["exit_price"])
                if is_closed and row.get("exit_price")
                else None
            )
            net_pnl = (
                float(row["net_pnl_usdt"])
                if is_closed and row.get("net_pnl_usdt")
                else None
            )

            trades.append(
                TradeRecord(
                    trade_id=f"T_{idx:05d}",
                    symbol=str(row["symbol"]).strip(),
                    entry_time=entry_t,
                    entry_price=entry_p,
                    exit_time=exit_t,
                    exit_price=exit_p,
                    net_pnl_usdt=net_pnl,
                    is_open=not is_closed,
                )
            )

    trades.sort(key=lambda t: t.entry_time)
    return trades


def load_15s_price_series(
    parquet_root: Path,
    symbols: set[str],
) -> dict[str, tuple[list[float], list[float]]]:
    """Load 15s price trajectories for requested symbols from parquet files.

    Returns:
        dict: symbol -> (list of epoch timestamps, list of prices)
    """
    if not parquet_root.exists() or not symbols:
        return {}

    parquet_files = sorted(parquet_root.rglob("*.parquet"))
    symbol_array = pa.array(list(symbols), type=pa.string())

    raw_data: dict[str, list[tuple[float, float]]] = {sym: [] for sym in symbols}

    for f in parquet_files:
        pf = pq.ParquetFile(f)
        schema_names = pf.schema.names
        has_completeness = (
            "data_complete" in schema_names
            and "missing_agg_trade_count" in schema_names
        )
        cols = ["symbol", "bucket_start", "close_price"]
        if has_completeness:
            cols.extend(["data_complete", "missing_agg_trade_count"])

        table = pf.read(columns=cols)
        mask = pc.and_(
            pc.is_in(table["symbol"], symbol_array),
            pc.is_valid(table["close_price"]),
        )
        if has_completeness:
            mask = pc.and_(
                mask,
                pc.and_(
                    pc.equal(table["data_complete"], True),
                    pc.equal(table["missing_agg_trade_count"], 0),
                ),
            )
        filtered = table.filter(mask)
        if filtered.num_rows == 0:
            continue

        sym_col = filtered["symbol"].to_pylist()
        time_col = filtered["bucket_start"].to_pylist()
        price_col = filtered["close_price"].to_pylist()

        for s, t, p in zip(sym_col, time_col, price_col, strict=False):
            if p is not None and t is not None:
                if isinstance(t, datetime):
                    t_epoch = t.timestamp()
                else:
                    t_epoch = parse_timestamp(t).timestamp()
                # Bucket close price is formed and available at the END of the 15s
                # bucket (bucket_start + 15s) to strictly prevent lookahead bias.
                available_epoch = t_epoch + 15.0
                raw_data[s].append((available_epoch, float(p)))

    # Sort each symbol's series by timestamp
    price_series: dict[str, tuple[list[float], list[float]]] = {}
    for sym, entries in raw_data.items():
        if not entries:
            continue
        entries.sort(key=lambda x: x[0])
        times = [e[0] for e in entries]
        prices = [e[1] for e in entries]
        price_series[sym] = (times, prices)

    return price_series


def get_price_at(
    symbol_prices: tuple[list[float] | np.ndarray, list[float] | np.ndarray] | None,
    target_epoch: float,
    fallback_price: float,
    max_stale_seconds: float = 300.0,
) -> float:
    """Retrieve the latest close price at or before target_epoch.

    Guarantees:
    1. Zero future lookahead: if target_epoch is strictly earlier than the first
       available quote, returns fallback_price rather than future prices.
    2. Stale quote preservation: if quotes pause, retains the last known quote
       rather than artificially reverting to entry price (preventing false recovery).
    """
    if symbol_prices is None:
        return fallback_price
    times, prices = symbol_prices
    if len(times) == 0:
        return fallback_price
    if isinstance(times, np.ndarray):
        idx = int(np.searchsorted(times, target_epoch, side="right")) - 1
    else:
        idx = bisect.bisect_right(times, target_epoch) - 1
    if idx < 0:
        # Before the first available price: DO NOT use future prices[0]
        return fallback_price
    val = prices[idx]
    if isinstance(val, (float, np.floating)) and np.isnan(val):
        return fallback_price
    return float(val)


@dataclass(frozen=True)
class AlignedPriceGrid:
    """Pre-aligned price timelines on a regular sampling grid.

    Pre-computes symbol prices at discrete sampling epochs to eliminate
    repeated bisect search overhead across candidate evaluations.
    Supports dictionary-like indexing and container checks for seamless
    compatibility with simulation ledgers and price functions.
    """

    sampling_epochs: np.ndarray  # 1D float64 array of epoch timestamps
    aligned_prices: dict[str, np.ndarray]  # symbol -> 1D float64 array of length N
    grid_seconds: int = 15
    t_start: float = 0.0
    t_end: float = 0.0

    def __contains__(self, symbol: object) -> bool:
        return symbol in self.aligned_prices

    def __getitem__(self, symbol: str) -> tuple[np.ndarray, np.ndarray]:
        if symbol not in self.aligned_prices:
            raise KeyError(symbol)
        return (self.sampling_epochs, self.aligned_prices[symbol])

    def get(
        self, symbol: str, default: Any = None
    ) -> tuple[np.ndarray, np.ndarray] | Any:
        if symbol in self.aligned_prices:
            return (self.sampling_epochs, self.aligned_prices[symbol])
        return default

    def __iter__(self):
        return iter(self.aligned_prices)

    def __len__(self) -> int:
        return len(self.aligned_prices)

    def keys(self):
        return self.aligned_prices.keys()

    def values(self):
        return ((self.sampling_epochs, arr) for arr in self.aligned_prices.values())

    def items(self):
        return (
            (sym, (self.sampling_epochs, arr))
            for sym, arr in self.aligned_prices.items()
        )

    @classmethod
    def build(
        cls,
        price_series: dict[str, tuple[list[float], list[float]]] | AlignedPriceGrid,
        start_time: datetime | float,
        end_time: datetime | float,
        grid_seconds: int = 15,
        align_bounds: bool = False,
    ) -> AlignedPriceGrid:
        t_req_start = (
            start_time.timestamp()
            if isinstance(start_time, datetime)
            else float(start_time)
        )
        t_req_end = (
            end_time.timestamp() if isinstance(end_time, datetime) else float(end_time)
        )

        if align_bounds:
            t_req_start = float(int(t_req_start // grid_seconds) * grid_seconds)
            t_req_end = float(int(math.ceil(t_req_end / grid_seconds)) * grid_seconds)

        if t_req_end < t_req_start:
            t_req_end = t_req_start

        if isinstance(price_series, AlignedPriceGrid):
            if (
                abs(price_series.t_start - t_req_start) < 1e-6
                and abs(price_series.t_end - t_req_end) < 1e-6
                and price_series.grid_seconds == grid_seconds
            ):
                return price_series

        sampling_epochs_list: list[float] = [t_req_start]
        next_tick = (int(t_req_start // grid_seconds) + 1) * grid_seconds
        while next_tick < t_req_end:
            sampling_epochs_list.append(float(next_tick))
            next_tick += grid_seconds

        if t_req_end > sampling_epochs_list[-1]:
            sampling_epochs_list.append(t_req_end)

        sampling_epochs = np.array(sampling_epochs_list, dtype=np.float64)
        N = len(sampling_epochs)

        aligned: dict[str, np.ndarray] = {}
        if isinstance(price_series, AlignedPriceGrid):
            old_epochs = price_series.sampling_epochs
            for sym, val in price_series.aligned_prices.items():
                if len(val) == 0:
                    aligned[sym] = np.full(N, np.nan, dtype=np.float64)
                    continue
                idx = np.searchsorted(old_epochs, sampling_epochs, side="right") - 1
                valid_mask = (idx >= 0) & (idx < len(val))
                arr = np.full(N, np.nan, dtype=np.float64)
                arr[valid_mask] = val[idx[valid_mask]]
                aligned[sym] = arr
        else:
            raw_series = price_series or {}
            for sym, val in raw_series.items():
                if isinstance(val, np.ndarray):
                    if len(val) == N:
                        aligned[sym] = val
                    else:
                        aligned[sym] = np.resize(val, N)
                    continue
                if not isinstance(val, (tuple, list)) or len(val) != 2:
                    continue
                t_list, p_list = val
                if not t_list or not p_list:
                    continue
                t_arr = np.asarray(t_list, dtype=np.float64)
                p_arr = np.asarray(p_list, dtype=np.float64)
                idx = np.searchsorted(t_arr, sampling_epochs, side="right") - 1
                valid_mask = (idx >= 0) & (idx < len(p_arr))
                if np.all(valid_mask):
                    aligned[sym] = p_arr[idx]
                else:
                    arr = np.full(N, np.nan, dtype=np.float64)
                    arr[valid_mask] = p_arr[idx[valid_mask]]
                    aligned[sym] = arr

        return cls(
            sampling_epochs=sampling_epochs,
            aligned_prices=aligned,
            grid_seconds=grid_seconds,
            t_start=t_req_start,
            t_end=t_req_end,
        )


@dataclass(frozen=True)
class FastMtmMetrics:
    """Lightweight metrics computed directly from array-based MTM evaluation."""

    net_pnl: float
    max_drawdown_usdt: float
    max_drawdown_pct: float
    ulcer_index: float
    cdar_95: float
    peak_margin: float
    terminal_equity: float
    is_feasible: bool
    infeasible_reason: str | None = None


def _simulate_mtm_arrays(
    trades: Sequence[TradeRecord],
    grid: AlignedPriceGrid,
    initial_equity: float = 1000.0,
    is_total_equity: bool = False,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, float]:
    """Vectorized calculation of equity, realized PnL, unrealized PnL,
    and active margin.

    Returns:
        (equity_arr, realized_arr, unreal_arr, margin_arr, count_arr,
         carry_in_base_unreal)
    """
    sampling_epochs = grid.sampling_epochs
    N = len(sampling_epochs)
    if N == 0:
        empty = np.array([], dtype=np.float64)
        return empty, empty, empty, empty, np.array([], dtype=np.int32), 0.0

    t_req_start = grid.t_start

    d_realized = np.zeros(N + 1, dtype=np.float64)
    d_margin = np.zeros(N + 1, dtype=np.float64)
    d_count = np.zeros(N + 1, dtype=np.int32)
    unreal_arr = np.zeros(N, dtype=np.float64)
    carry_in_base_unreal = 0.0

    for tr in trades:
        e_epoch = tr.entry_time.timestamp()
        x_epoch = tr.exit_time.timestamp() if tr.exit_time is not None else None

        is_carry_in = e_epoch < t_req_start
        if is_carry_in:
            if x_epoch is not None and x_epoch < t_req_start:
                continue
            k_entry = 0
            if is_total_equity and tr.entry_price > 0:
                p_start = (
                    grid.aligned_prices[tr.symbol][0]
                    if tr.symbol in grid.aligned_prices
                    else tr.entry_price
                )
                if np.isnan(p_start):
                    p_start = tr.entry_price
                gross = tr.notional_usdt * (p_start - tr.entry_price) / tr.entry_price
                if tr.direction == "SHORT":
                    gross = -gross
                carry_in_base_unreal += gross - (
                    tr.notional_usdt * (tr.fee_rate + tr.slippage_rate)
                )
        else:
            k_entry = bisect.bisect_left(sampling_epochs, e_epoch)

        if x_epoch is not None:
            k_exit = bisect.bisect_left(sampling_epochs, x_epoch)
            if k_exit < N and x_epoch >= t_req_start:
                d_realized[k_exit] += tr.calculated_net_pnl
        else:
            k_exit = N

        if k_entry < k_exit and k_entry < N:
            k_end = min(k_exit, N)
            d_margin[k_entry] += tr.initial_margin_usdt
            d_margin[k_end] -= tr.initial_margin_usdt
            d_count[k_entry] += 1
            d_count[k_end] -= 1

            if tr.entry_price > 0 and tr.symbol in grid.aligned_prices:
                px_slice = grid.aligned_prices[tr.symbol][k_entry:k_end]
                if np.isnan(px_slice[0]):
                    px_slice = np.where(np.isnan(px_slice), tr.entry_price, px_slice)
                if tr.direction == "LONG":
                    gross = (tr.notional_usdt / tr.entry_price) * (
                        px_slice - tr.entry_price
                    )
                else:
                    gross = (tr.notional_usdt / tr.entry_price) * (
                        tr.entry_price - px_slice
                    )
                friction = tr.notional_usdt * (tr.fee_rate + tr.slippage_rate)
                unreal_arr[k_entry:k_end] += gross - friction
            elif k_entry < k_exit:
                friction = tr.notional_usdt * (tr.fee_rate + tr.slippage_rate)
                unreal_arr[k_entry:k_end] -= friction

    realized_arr = np.cumsum(d_realized[:N])
    margin_arr = np.cumsum(d_margin[:N])
    count_arr = np.cumsum(d_count[:N])
    equity_arr = initial_equity + realized_arr + (unreal_arr - carry_in_base_unreal)

    return (
        equity_arr,
        realized_arr,
        unreal_arr,
        margin_arr,
        count_arr,
        carry_in_base_unreal,
    )


def compute_event_peak_margin_and_count(
    trades: Sequence[TradeRecord],
    t_start_epoch: float | None = None,
    t_end_epoch: float | None = None,
) -> tuple[float, int]:
    """Compute exact continuous peak initial margin and position count from
    trade events.
    """
    if not trades:
        return 0.0, 0

    events: list[tuple[float, int, float, int]] = []
    for tr in trades:
        e_epoch = tr.entry_time.timestamp()
        x_epoch = tr.exit_time.timestamp() if tr.exit_time is not None else float("inf")
        if t_start_epoch is not None and x_epoch <= t_start_epoch:
            continue
        if t_end_epoch is not None and e_epoch >= t_end_epoch:
            continue

        eff_e = max(e_epoch, t_start_epoch) if t_start_epoch is not None else e_epoch
        eff_x = min(x_epoch, t_end_epoch) if t_end_epoch is not None else x_epoch
        m = tr.initial_margin_usdt

        # Order tie-breaker: exits (type 0) before entries (type 1) at exact same epoch
        events.append((eff_e, 1, m, 1))
        if eff_x < float("inf"):
            events.append((eff_x, 0, -m, -1))

    events.sort(key=lambda x: (x[0], x[1]))

    curr_m = 0.0
    curr_c = 0
    peak_m = 0.0
    peak_c = 0

    for _, _, dm, dc in events:
        curr_m += dm
        curr_c += dc
        if curr_m > peak_m:
            peak_m = curr_m
        if curr_c > peak_c:
            peak_c = curr_c

    return round(peak_m, 2), peak_c


def reconstruct_mtm_metrics_fast(
    trades: Sequence[TradeRecord],
    price_series: dict[str, tuple[list[float], list[float]]] | AlignedPriceGrid,
    initial_equity: float = 1000.0,
    grid_seconds: int = 15,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    margin_cap: float | None = None,
    is_total_equity: bool = False,
) -> FastMtmMetrics | None:
    """Fast, array-based evaluation of MTM performance metrics.

    Eliminates EquityPoint object creation and bisect search overhead for
    candidate filtering.
    """
    if not trades and start_time is None:
        return None

    all_entry_times = [t.entry_time for t in trades]
    all_exit_times = [t.exit_time for t in trades if t.exit_time is not None]

    if start_time is not None:
        min_time = start_time
    elif all_entry_times:
        min_time = min(all_entry_times)
    else:
        return None

    if end_time is not None:
        max_time = end_time
    else:
        all_event_times = all_entry_times + all_exit_times
        if not all_event_times:
            return None
        max_time = max(all_event_times)

    grid = AlignedPriceGrid.build(
        price_series,
        start_time=min_time,
        end_time=max_time,
        grid_seconds=grid_seconds,
        align_bounds=(start_time is None),
    )

    equity_arr, _, _, margin_arr, _, _ = _simulate_mtm_arrays(
        trades=trades,
        grid=grid,
        initial_equity=initial_equity,
        is_total_equity=is_total_equity,
    )

    if len(equity_arr) == 0:
        return None

    t_start_ep = min_time.timestamp() if min_time else None
    t_end_ep = max_time.timestamp() if max_time else None
    event_peak_margin, _ = compute_event_peak_margin_and_count(
        trades, t_start_ep, t_end_ep
    )
    grid_peak_margin = float(np.max(margin_arr)) if len(margin_arr) > 0 else 0.0
    peak_margin = max(grid_peak_margin, event_peak_margin)
    if margin_cap is not None and peak_margin > margin_cap:
        return None

    eq_rounded = np.round(equity_arr, 4)
    if not np.all(np.isfinite(eq_rounded)):
        return None

    min_eq = float(np.min(eq_rounded))
    if min_eq <= 0:
        return None

    _, abs_dd, pct_dd = compute_drawdown_series(
        eq_rounded, initial_equity=initial_equity
    )
    mdd_pct = float(np.max(pct_dd))
    mdd_usdt = float(np.max(abs_dd))
    ui = calc_ulcer_index(pct_dd)
    cdar = calc_cdar(pct_dd, alpha=0.95)
    final_eq = float(eq_rounded[-1])
    net_pnl = final_eq - initial_equity

    return FastMtmMetrics(
        net_pnl=net_pnl,
        max_drawdown_usdt=mdd_usdt,
        max_drawdown_pct=mdd_pct,
        ulcer_index=ui,
        cdar_95=cdar,
        peak_margin=peak_margin,
        terminal_equity=final_eq,
        is_feasible=True,
    )


def reconstruct_mtm_equity(
    trades: Sequence[TradeRecord],
    price_series: dict[str, tuple[list[float], list[float]]] | AlignedPriceGrid,
    initial_equity: float = 1000.0,
    grid_seconds: int = 15,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    sizing_policy: Any | None = None,
    is_total_equity: bool = False,
) -> list[EquityPoint]:
    """Reconstruct 15-second continuous MTM equity series.

    Guarantees:
    1. Preserves carry-in positions opened prior to start_time.
    2. Chronological event sequencing (same timestamp processes ENTRY then EXIT).
    3. Retains unclosed positions through the end of the simulation.
    4. Exact timeline bounded by [start_time, end_time] without dropping sub-grid
       events.
    5. Dynamic compounding position sizing and intraday risk checking when
       sizing_policy is provided.
    """
    if not trades and start_time is None:
        return []

    # Determine timeline bounds
    all_entry_times = [t.entry_time for t in trades]
    all_exit_times = [t.exit_time for t in trades if t.exit_time is not None]

    if start_time is not None:
        min_time = start_time
    elif all_entry_times:
        min_time = min(all_entry_times)
    else:
        return []

    if end_time is not None:
        max_time = end_time
    else:
        all_event_times = all_entry_times + all_exit_times
        if not all_event_times:
            return []
        max_time = max(all_event_times)

    # Fast vectorized path when dynamic sizing policy is not required
    if sizing_policy is None:
        grid = AlignedPriceGrid.build(
            price_series,
            start_time=min_time,
            end_time=max_time,
            grid_seconds=grid_seconds,
            align_bounds=(start_time is None),
        )

        equity_arr, realized_arr, unreal_arr, margin_arr, count_arr, _ = (
            _simulate_mtm_arrays(
                trades=trades,
                grid=grid,
                initial_equity=initial_equity,
                is_total_equity=is_total_equity,
            )
        )

        eq_rounded = np.round(equity_arr, 4)
        realized_rounded = np.round(realized_arr, 4)
        unreal_rounded = np.round(unreal_arr, 4)
        margin_rounded = np.round(margin_arr, 2)

        sampling_dts = [datetime.fromtimestamp(e, tz=UTC) for e in grid.sampling_epochs]

        return [
            EquityPoint(
                timestamp=dt,
                equity=float(e),
                realized_pnl=float(r),
                unrealized_pnl=float(u),
                peak_initial_margin=float(m),
                active_positions=int(c),
            )
            for dt, e, r, u, m, c in zip(
                sampling_dts,
                eq_rounded,
                realized_rounded,
                unreal_rounded,
                margin_rounded,
                count_arr,
                strict=True,
            )
        ]

    # Dynamic sizing event loop when sizing_policy is provided
    t_req_start = min_time.timestamp()
    t_req_end = max_time.timestamp()
    if t_req_end < t_req_start:
        t_req_end = t_req_start

    # If start_time was not explicitly provided, align to floor grid tick
    if start_time is None:
        t_req_start = float(int(t_req_start // grid_seconds) * grid_seconds)
    if end_time is None:
        # Use ceil so that events within the final grid bucket are not omitted
        t_req_end = float(int(math.ceil(t_req_end / grid_seconds)) * grid_seconds)
        if t_req_end < t_req_start:
            t_req_end = t_req_start

    sampling_epochs: list[float] = [t_req_start]
    next_tick = (int(t_req_start // grid_seconds) + 1) * grid_seconds
    while next_tick < t_req_end:
        sampling_epochs.append(float(next_tick))
        next_tick += grid_seconds

    if t_req_end > sampling_epochs[-1]:
        sampling_epochs.append(t_req_end)

    sampling_dts = [datetime.fromtimestamp(e, tz=UTC) for e in sampling_epochs]

    # Separate carry-in trades and window events
    active_trades: set[TradeRecord] = set()
    realized_pnl = 0.0

    admitted_trades: dict[str, TradeRecord] = {}
    current_equity = initial_equity
    last_day_cut_date = None

    if sizing_policy is not None:
        init_dt = (
            sampling_dts[0]
            if sampling_dts
            else datetime.fromtimestamp(t_req_start, tz=UTC)
        )
        sizing_policy.on_day_cut(initial_equity, init_dt)
        last_day_cut_date = init_dt.date()

    # Build chronological event list
    # Format: (timestamp_epoch, order_priority, event_type, trade)
    # order_priority: ENTRY=0, EXIT=1 (so in same epoch, entry happens before exit)
    events: list[tuple[float, int, str, TradeRecord]] = []

    for t in trades:
        e_epoch = t.entry_time.timestamp()
        x_epoch = t.exit_time.timestamp() if t.exit_time is not None else None

        if e_epoch < t_req_start:
            # Trade entered before window start
            if x_epoch is None or x_epoch >= t_req_start:
                # Still active at window start (carry-in position)
                active_trades.add(t)
                if sizing_policy is not None:
                    admitted_trades[t.trade_id] = t
            if x_epoch is not None and x_epoch >= t_req_start:
                events.append((x_epoch, 1, "EXIT", t))
        else:
            # Trade enters during or after window start
            events.append((e_epoch, 0, "ENTRY", t))
            if x_epoch is not None:
                events.append((x_epoch, 1, "EXIT", t))

    events.sort(key=lambda x: (x[0], x[1]))

    points: list[EquityPoint] = []
    last_day_cut_date = sampling_dts[0].date() if sampling_dts else None

    # Baseline unrealized PnL of carry-in trades evaluated at t_req_start.
    # When initial_equity represents total account equity at window start
    # (including existing floating PnL, as in live balance reconciliation),
    # we offset this pre-window baseline so that equity trajectory at t_req_start
    # starts exactly at initial_equity and only tracks incremental gains/losses
    # within the requested window.
    carry_in_base_unreal = 0.0
    if is_total_equity:
        for t in active_trades:
            if t.entry_time.timestamp() < t_req_start:
                sym_series = price_series.get(t.symbol)
                p_start = get_price_at(sym_series, t_req_start, t.entry_price)
                gross = t.notional_usdt * (p_start - t.entry_price) / t.entry_price
                if t.direction == "SHORT":
                    gross = -gross
                carry_in_base_unreal += gross - (
                    t.notional_usdt * (t.fee_rate + t.slippage_rate)
                )

    def _eval_current_mtm(epoch: float) -> tuple[float, float, float]:
        if not active_trades:
            return initial_equity + realized_pnl - carry_in_base_unreal, 0.0, 0.0
        unreal = 0.0
        margin = 0.0
        for t in active_trades:
            if t.entry_price <= 0:
                continue
            sym_series = price_series.get(t.symbol)
            curr_price = get_price_at(sym_series, epoch, t.entry_price)
            gross_float = t.notional_usdt * (curr_price - t.entry_price) / t.entry_price
            if t.direction == "SHORT":
                gross_float = -gross_float
            unreal += gross_float - (t.notional_usdt * (t.fee_rate + t.slippage_rate))
            margin += t.initial_margin_usdt
        eq = initial_equity + realized_pnl + (unreal - carry_in_base_unreal)
        return eq, unreal, margin

    event_idx = 0
    num_events = len(events)

    for i, curr_epoch in enumerate(sampling_epochs):
        curr_dt = sampling_dts[i]

        # Pre-mark active trades to current market price before day cut or order checks
        if sizing_policy is not None:
            current_equity, unrealized_pnl, active_margin = _eval_current_mtm(
                curr_epoch
            )

            # Trigger day-cut if calendar date advances
            if last_day_cut_date is not None and curr_dt.date() > last_day_cut_date:
                sizing_policy.on_day_cut(current_equity, curr_dt)
                last_day_cut_date = curr_dt.date()

        # Process all events occurring up to curr_epoch
        while event_idx < num_events and events[event_idx][0] <= curr_epoch:
            ev_epoch, _, ev_type, tr = events[event_idx]

            if sizing_policy is not None:
                ev_dt = (
                    tr.entry_time
                    if ev_type == "ENTRY"
                    else (tr.exit_time or datetime.fromtimestamp(ev_epoch, tz=UTC))
                )
                if last_day_cut_date is not None and ev_dt.date() > last_day_cut_date:
                    ev_eq, _, _ = _eval_current_mtm(ev_epoch)
                    sizing_policy.on_day_cut(ev_eq, ev_dt)
                    last_day_cut_date = ev_dt.date()

            if ev_type == "ENTRY":
                if sizing_policy is not None:
                    ev_eq, _, _ = _eval_current_mtm(ev_epoch)
                    curr_pos_margin = sum(t.initial_margin_usdt for t in active_trades)
                    risk_chk = sizing_policy.check_intraday_order(
                        symbol=tr.symbol,
                        price=tr.entry_price,
                        current_equity=ev_eq,
                        position_margin=curr_pos_margin,
                        reserved_margin=0.0,
                        leverage=tr.leverage,
                        current_time=ev_dt,
                    )
                    if risk_chk.allowed:
                        scale_factor = (
                            risk_chk.notional_usdt / tr.notional_usdt
                            if tr.notional_usdt > 0
                            else 1.0
                        )
                        dyn_tr = TradeRecord(
                            trade_id=tr.trade_id,
                            symbol=tr.symbol,
                            entry_time=tr.entry_time,
                            entry_price=tr.entry_price,
                            exit_time=tr.exit_time,
                            exit_price=tr.exit_price,
                            notional_usdt=risk_chk.notional_usdt,
                            leverage=tr.leverage,
                            fee_rate=tr.fee_rate,
                            slippage_rate=tr.slippage_rate,
                            funding_cost_usdt=tr.funding_cost_usdt * scale_factor,
                            direction=tr.direction,
                            is_open=tr.is_open,
                        )
                        admitted_trades[tr.trade_id] = dyn_tr
                        active_trades.add(dyn_tr)
                else:
                    active_trades.add(tr)
            elif ev_type == "EXIT":
                target_tr = (
                    admitted_trades.get(tr.trade_id, tr)
                    if sizing_policy is not None
                    else tr
                )
                if target_tr in active_trades:
                    active_trades.remove(target_tr)
                    realized_pnl += target_tr.calculated_net_pnl
            event_idx += 1

        # Re-evaluate MTM at end of curr_epoch after all entries/exits processed
        total_equity, unrealized_pnl, active_margin = _eval_current_mtm(curr_epoch)
        current_equity = total_equity
        dt = curr_dt

        points.append(
            EquityPoint(
                timestamp=dt,
                equity=round(total_equity, 4),
                realized_pnl=round(realized_pnl, 4),
                unrealized_pnl=round(unrealized_pnl, 4),
                peak_initial_margin=round(active_margin, 2),
                active_positions=len(active_trades),
            )
        )

    return points


def evaluate_legacy_trade_level_curve(
    trades: Sequence[TradeRecord],
    initial_equity: float = 1000.0,
) -> EquityMetrics:
    """Evaluate old legacy trade-level curve (step-wise jump only at trade exit).

    Simulates the exact behavior of build_pnl_series / metrics_for_split
    without intra-trade mark-to-market floating losses.
    """
    closed_trades = [t for t in trades if t.exit_time is not None]
    if not closed_trades:
        return evaluate_equity_curve([], initial_equity=initial_equity)

    # Sort trades by exit time
    sorted_trades = sorted(closed_trades, key=lambda t: t.exit_time)
    points: list[EquityPoint] = [
        EquityPoint(timestamp=sorted_trades[0].entry_time, equity=initial_equity)
    ]

    running_equity = initial_equity
    for t in sorted_trades:
        running_equity += t.calculated_net_pnl
        points.append(
            EquityPoint(
                timestamp=t.exit_time,
                equity=running_equity,
                realized_pnl=running_equity - initial_equity,
                unrealized_pnl=0.0,
            )
        )

    return evaluate_equity_curve(points, initial_equity=initial_equity)


def load_cached_price_series(
    cache_path: Path,
    expected_manifest: Any | None = None,
    required_symbols: Sequence[str] | set[str] | None = None,
    max_gap_seconds: float | None = None,
) -> dict[str, tuple[list[float], list[float]]]:
    """Load 15s price series from cache file with strict manifest binding validation."""
    if not cache_path.exists():
        raise FileNotFoundError(f"Price cache file not found: {cache_path}")
    with cache_path.open("rb") as f:
        obj = pickle.load(f)
    if isinstance(obj, dict) and "__metadata__" in obj:
        meta = obj["__metadata__"]
        if expected_manifest is not None:
            exp_hash = getattr(expected_manifest, "content_hash", None)
            if exp_hash and meta.get("source_content_hash") != exp_hash:
                raise ValueError(
                    f"Price cache content hash mismatch! "
                    f"Cache: '{meta.get('source_content_hash')}' vs "
                    f"Manifest: '{exp_hash}'. Cache is invalidated."
                )
            exp_wm_end = getattr(expected_manifest, "watermark_end", None)
            if exp_wm_end:
                wm_str = (
                    exp_wm_end.isoformat()
                    if hasattr(exp_wm_end, "isoformat")
                    else str(exp_wm_end)
                )
                if meta.get("watermark_end") != wm_str:
                    raise ValueError(
                        f"Price cache watermark mismatch! "
                        f"Cache: '{meta.get('watermark_end')}' vs "
                        f"Manifest: '{wm_str}'. Cache is invalidated."
                    )
            exp_symbols = getattr(expected_manifest, "symbols", None)
            if exp_symbols and not required_symbols:
                required_symbols = exp_symbols
        prices = obj["prices"]
    elif expected_manifest is not None:
        raise ValueError(
            f"Price cache {cache_path} is legacy unverified format without "
            "__metadata__. Fail-closed: refusing unverified fallback. "
            "Please rebuild cache with build_price_cache.py."
        )
    elif isinstance(obj, dict):
        prices = obj
    else:
        raise ValueError(f"Unrecognized cache format in {cache_path}")

    # Validate price series contents
    if required_symbols:
        req_set = {str(s).upper() for s in required_symbols}
        avail_set = {str(s).upper() for s in prices.keys()}
        missing = req_set - avail_set
        if missing:
            raise ValueError(f"Price cache missing required symbols: {sorted(missing)}")

    for sym, val in prices.items():
        if not isinstance(val, (tuple, list)) or len(val) != 2:
            raise ValueError(f"Invalid price series format for symbol {sym}")
        times, pxs = val
        if len(times) != len(pxs):
            raise ValueError(
                f"Price series length mismatch for {sym}: "
                f"{len(times)} timestamps vs {len(pxs)} prices"
            )
        if len(times) > 1:
            for i in range(len(times) - 1):
                dt = times[i + 1] - times[i]
                if dt <= 0:
                    raise ValueError(
                        f"Price series for {sym} not strictly increasing at "
                        f"index {i}: {times[i]} >= {times[i + 1]}"
                    )
                if max_gap_seconds is not None and dt > max_gap_seconds:
                    raise ValueError(
                        f"Price series for {sym} has gap {dt:.1f}s > "
                        f"{max_gap_seconds:.1f}s at index {i}"
                    )

    return prices
