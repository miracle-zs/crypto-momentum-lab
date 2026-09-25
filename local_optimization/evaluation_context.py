"""Unified Candidate Evaluation and Context Architecture.

Provides a unified execution pipeline across:
1. Worker candidate verification (fast evaluation)
2. Dashboard scenario replay and curve assembly (full curve evaluation)
3. Walk-Forward Analysis (WFA) stateful ledger fold simulation
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd

from crypto_momentum_lab.live_rollout.scheduled_risk_window import (
    ScheduledRiskWindowConfig,
)
from local_optimization.equity import (
    EquityMetrics,
    EquityPoint,
    evaluate_equity_curve,
)
from local_optimization.mtm_engine import (
    AlignedPriceGrid,
    FastMtmMetrics,
    TradeRecord,
    compute_event_peak_margin_and_count,
    get_price_at,
    reconstruct_mtm_equity,
    reconstruct_mtm_metrics_fast,
)
from local_optimization.simulation_ledger import (
    PortfolioState,
    SimulationLedger,
)
from local_optimization.snapshot import parse_stream_timestamp

INITIAL_EQUITY: float = 1000.0
NOTIONAL_PER_ENTRY: float = 100.0
LEVERAGE: float = 5.0


def compute_daily_compounding_scales(
    admitted_events: Sequence[dict[str, Any]],
    initial_equity: float = 1000.0,
    f: float = 0.10,
    leverage: float = LEVERAGE,
    prices_by_symbol: dict[str, tuple[list[float], list[float]]] | None = None,
    w_start: datetime | None = None,
    w_end: datetime | None = None,
) -> tuple[dict[str, float], float, float, float, float, float]:
    """Compute causal daily compounding scales and metrics."""
    if not admitted_events:
        return {}, -1.0, 1.0, 1.0, initial_equity, 0.0

    def _get_ent_ep(ev: Any) -> float:
        if isinstance(ev, TradeRecord):
            return ev.entry_time.timestamp()
        if "entry_epoch" in ev:
            return float(ev["entry_epoch"])
        ent = ev.get("entry_at") or ev.get("entry_time") or ev.get("entry_eligible_at")
        if isinstance(ent, datetime):
            return ent.timestamp()
        if isinstance(ent, str):
            return pd.to_datetime(ent).timestamp()
        return 0.0

    def _get_ent_dt(ev: Any, ep: float) -> datetime:
        if isinstance(ev, TradeRecord):
            return ev.entry_time
        ent = ev.get("entry_at") or ev.get("entry_time") or ev.get("entry_eligible_at")
        if isinstance(ent, datetime):
            return ent if ent.tzinfo is not None else ent.replace(tzinfo=UTC)
        return datetime.fromtimestamp(ep, tz=UTC)

    def _get_ex_ep(ev: Any, ent_ep: float) -> float:
        if isinstance(ev, TradeRecord):
            return ev.exit_time.timestamp() if ev.exit_time else float("inf")
        if "exit_epoch" in ev and ev["exit_epoch"] is not None:
            return float(ev["exit_epoch"])
        ex = ev.get("exit_at") or ev.get("exit_time")
        if isinstance(ex, datetime):
            return ex.timestamp()
        if isinstance(ex, str):
            return pd.to_datetime(ex).timestamp()
        return float("inf")

    def _get_ex_dt(ev: Any, ex_ep: float) -> datetime | None:
        if ex_ep == float("inf"):
            return None
        if isinstance(ev, TradeRecord):
            return ev.exit_time
        ex = ev.get("exit_at") or ev.get("exit_time")
        if isinstance(ex, datetime):
            return ex if ex.tzinfo is not None else ex.replace(tzinfo=UTC)
        if isinstance(ex, str):
            dt = pd.to_datetime(ex)
            return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)
        return datetime.fromtimestamp(ex_ep, tz=UTC)

    def _get_sym(ev: Any) -> str:
        if isinstance(ev, TradeRecord):
            return ev.symbol
        return str(ev.get("symbol", ""))

    def _get_direction(ev: Any) -> str:
        if isinstance(ev, TradeRecord):
            return ev.direction
        return str(ev.get("direction", "LONG"))

    def _get_ent_price(ev: Any) -> float:
        if isinstance(ev, TradeRecord):
            return ev.entry_price
        p = ev.get("entry_reference_price") or ev.get("entry_price") or 1.0
        return float(p)

    def _get_fee_slip(ev: Any) -> tuple[float, float]:
        if isinstance(ev, TradeRecord):
            return ev.fee_rate, ev.slippage_rate
        f_r = float(ev.get("fee_rate") or 0.0005)
        s_r = float(ev.get("slippage_rate") or 0.0002)
        return f_r, s_r

    def _calc_trade_pnl(ev: Any) -> float:
        if isinstance(ev, TradeRecord):
            return ev.calculated_net_pnl
        pnl = (
            ev.get("net_pnl_usdt")
            if hasattr(ev, "get")
            else getattr(ev, "net_pnl_usdt", None)
        )
        if pnl is not None:
            return float(pnl)
        ent_p = _get_ent_price(ev)
        ex_p = getattr(ev, "exit_price", None) or (
            ev.get("exit_price") if hasattr(ev, "get") else None
        )
        if ex_p is None:
            ex_p = ent_p
        direction = _get_direction(ev)
        f_r, s_r = _get_fee_slip(ev)
        fund = float(
            getattr(ev, "funding_cost_usdt", 0.0)
            or (ev.get("funding_cost_usdt", 0.0) if hasattr(ev, "get") else 0.0)
        )
        notional = float(
            getattr(ev, "notional_usdt", None)
            or (ev.get("notional_usdt") if hasattr(ev, "get") else None)
            or NOTIONAL_PER_ENTRY
        )
        if ent_p > 0:
            gross = notional * (ex_p - ent_p) / ent_p
            if direction == "SHORT":
                gross = -gross
            fee_slip = notional * (f_r * 2.0 + s_r * 2.0)
            return gross - fee_slip - fund
        return 0.0

    w_start_ep = w_start.timestamp() if w_start is not None else -float("inf")
    w_end_ep = w_end.timestamp() if w_end is not None else float("inf")

    parsed_events = []
    for ev in admitted_events:
        ent_ep = _get_ent_ep(ev)
        ent_dt = _get_ent_dt(ev, ent_ep)
        ex_ep = _get_ex_ep(ev, ent_ep)
        ex_dt = _get_ex_dt(ev, ex_ep)
        parsed_events.append((ent_ep, ent_dt, ex_ep, ex_dt, ev))

    events_sorted = sorted(parsed_events, key=lambda x: x[0])
    all_dates = set()
    entries_by_date: dict[str, list[Any]] = defaultdict(list)
    exits_by_date: dict[str, list[Any]] = defaultdict(list)

    for ent_ep, ent_dt, ex_ep, ex_dt, ev in events_sorted:
        ent_d = ent_dt.strftime("%Y-%m-%d")
        if w_start is None or ent_ep >= w_start_ep:
            all_dates.add(ent_d)
            entries_by_date[ent_d].append(ev)

        if ex_dt is not None:
            if (
                ex_dt.hour == 0
                and ex_dt.minute == 0
                and ex_dt.second == 0
                and ex_dt.microsecond == 0
                and ex_ep > ent_ep
            ):
                ex_d = (ex_dt - timedelta(seconds=1)).strftime("%Y-%m-%d")
            else:
                ex_d = ex_dt.strftime("%Y-%m-%d")
            if w_end is None or ex_ep <= w_end_ep:
                if w_start is None or ex_ep >= w_start_ep:
                    all_dates.add(ex_d)
                    exits_by_date[ex_d].append(ev)

    if w_start is not None and w_end is not None:
        curr = w_start.date()
        if w_end > w_start:
            target_end = (w_end - timedelta(seconds=1)).date()
        else:
            target_end = w_end.date()
        sorted_dates = []
        while curr <= target_end:
            sorted_dates.append(curr.strftime("%Y-%m-%d"))
            curr += timedelta(days=1)
        if not sorted_dates:
            sorted_dates = [w_start.strftime("%Y-%m-%d")]
    else:
        sorted_dates = sorted(all_dates)

    cur_realized_equity = initial_equity
    hwm = initial_equity
    mdd = 0.0
    daily_equities = [initial_equity]

    day_scales: dict[str, float] = {}
    trade_scales: dict[int, float] = {}
    time_pts: list[tuple[float, float]] = []

    def _calc_floating_at(target_epoch: float) -> float:
        unreal = 0.0
        for e_ep, _, x_ep, _, ev in events_sorted:
            if e_ep < target_epoch < x_ep:
                s = trade_scales.get(id(ev), 1.0)
                base_notional = (
                    float(ev.notional_usdt)
                    if isinstance(ev, TradeRecord)
                    else float(
                        getattr(ev, "notional_usdt", None)
                        or (ev.get("notional_usdt") if hasattr(ev, "get") else None)
                        or NOTIONAL_PER_ENTRY
                    )
                )
                notional = base_notional * s
                ent_p = _get_ent_price(ev)
                if ent_p <= 0:
                    continue
                sym = _get_sym(ev)
                sym_prices = prices_by_symbol.get(sym) if prices_by_symbol else None
                p_curr = (
                    get_price_at(sym_prices, target_epoch, ent_p)
                    if sym_prices
                    else ent_p
                )
                gross = notional * (p_curr - ent_p) / ent_p
                if _get_direction(ev) == "SHORT":
                    gross = -gross
                f_r, s_r = _get_fee_slip(ev)
                unreal += gross - notional * (f_r + s_r)
        return unreal

    for d in sorted_dates:
        d_start_dt = datetime(int(d[:4]), int(d[5:7]), int(d[8:10]), tzinfo=UTC)
        d_start_ep = max(d_start_dt.timestamp(), w_start_ep)

        floating_at_start = _calc_floating_at(d_start_ep)
        day_cut_mtm_equity = cur_realized_equity + floating_at_start

        if day_cut_mtm_equity <= 0:
            scale_d = 0.0
        else:
            n_d = f * max(10.0, day_cut_mtm_equity)
            scale_d = n_d / 100.0
        day_scales[d] = scale_d

        for ev in entries_by_date[d]:
            ev_id = id(ev)
            trade_scales[ev_id] = scale_d
            base_notional = (
                float(ev.notional_usdt)
                if isinstance(ev, TradeRecord)
                else float(
                    getattr(ev, "notional_usdt", None)
                    or (ev.get("notional_usdt") if hasattr(ev, "get") else None)
                    or NOTIONAL_PER_ENTRY
                )
            )
            ev_lev = (
                float(ev.leverage)
                if isinstance(ev, TradeRecord)
                else float(
                    getattr(ev, "leverage", None)
                    or (ev.get("leverage") if hasattr(ev, "get") else None)
                    or leverage
                )
            )
            margin_entry = (base_notional * scale_d) / ev_lev
            ent_ep = _get_ent_ep(ev)
            ex_ep = _get_ex_ep(ev, ent_ep)
            time_pts.append((ent_ep, margin_entry))
            time_pts.append((ex_ep, -margin_entry))

        day_realized = sum(
            _calc_trade_pnl(ev) * trade_scales.get(id(ev), 1.0)
            for ev in exits_by_date[d]
        )
        cur_realized_equity += day_realized

        d_end_ep = min(d_start_dt.timestamp() + 86400.0, w_end_ep)
        floating_at_end = _calc_floating_at(d_end_ep)
        day_end_mtm = cur_realized_equity + floating_at_end

        if day_end_mtm > hwm:
            hwm = day_end_mtm
        dd = (hwm - day_end_mtm) / hwm if hwm > 0 else 0.0
        if dd > mdd:
            mdd = dd
        daily_equities.append(day_end_mtm)

    time_pts.sort(key=lambda x: (x[0], x[1]))
    cur_m = 0.0
    peak_margin = 0.0
    for _, delta in time_pts:
        cur_m += delta
        if cur_m > peak_margin:
            peak_margin = cur_m

    if w_start is not None and w_end is not None and w_end > w_start:
        n_days = max(1.0, (w_end - w_start).total_seconds() / 86400.0)
    else:
        n_days = max(1.0, float(len(daily_equities) - 1))
    terminal_equity = daily_equities[-1]
    if terminal_equity <= 0:
        return day_scales, -2.0, 1.0, 1.0, terminal_equity, peak_margin

    log_growth = math.log(max(1.0, terminal_equity) / initial_equity) / n_days
    dds: list[float] = []
    run_hwm = daily_equities[0]
    for eq in daily_equities:
        if eq > run_hwm:
            run_hwm = eq
        dds.append((run_hwm - eq) / run_hwm if run_hwm > 0 else 0.0)
    ui = math.sqrt(float(np.mean([x**2 for x in dds])))
    score = log_growth - 2.0 * ui
    return day_scales, score, mdd, ui, terminal_equity, peak_margin


def evaluate_daily_compounding(
    admitted_events: Sequence[dict[str, Any]],
    initial_equity: float = 1000.0,
    f: float = 0.10,
    leverage: float = LEVERAGE,
    prices_by_symbol: dict[str, tuple[list[float], list[float]]] | None = None,
    w_start: datetime | None = None,
    w_end: datetime | None = None,
) -> tuple[float, float, float, float, float]:
    """Evaluate candidate trajectory under strictly causal daily compounding."""
    _, score, mdd, ui, cur_equity, peak_margin = compute_daily_compounding_scales(
        admitted_events=admitted_events,
        initial_equity=initial_equity,
        f=f,
        leverage=leverage,
        prices_by_symbol=prices_by_symbol,
        w_start=w_start,
        w_end=w_end,
    )
    return score, mdd, ui, cur_equity, peak_margin


def to_trade_records(
    admitted: list[dict[str, Any]],
    compounding_scale: bool = False,
    f: float = 0.10,
    initial_equity: float = 1000.0,
    prices_by_symbol: dict[str, tuple[list[float], list[float]]] | None = None,
    w_start: datetime | None = None,
    w_end: datetime | None = None,
) -> list[TradeRecord]:
    """Convert admitted events to TradeRecords with compounding scaling."""
    records: list[TradeRecord] = []
    day_scales: dict[str, float] = {}
    if compounding_scale:
        day_scales, _, _, _, _, _ = compute_daily_compounding_scales(
            admitted,
            initial_equity=initial_equity,
            f=f,
            prices_by_symbol=prices_by_symbol,
            w_start=w_start,
            w_end=w_end,
        )

    for idx, r in enumerate(admitted):
        if isinstance(r, TradeRecord):
            trade_id = r.trade_id
            ent = r.entry_time
            ex = r.exit_time
            ent_p = r.entry_price
            exit_p = r.exit_price
            pnl = r.calculated_net_pnl if r.exit_time is not None else None
            sym = r.symbol
            fee_r = r.fee_rate
            slip_r = r.slippage_rate
            fund_c = r.funding_cost_usdt
            is_open = r.is_open or (r.exit_time is None)
            base_notional = float(r.notional_usdt)
            lev = float(r.leverage)
            direction = str(r.direction)
        else:
            trade_id = str(r.get("trade_id") or f"T_{idx:05d}")
            ent = r.get("entry_at") or r.get("entry_time") or r.get("entry_eligible_at")
            if isinstance(ent, str):
                ent = parse_stream_timestamp(ent) or datetime.fromisoformat(
                    ent.replace("Z", "+00:00")
                )
            elif isinstance(ent, (int, float)):
                ent = datetime.fromtimestamp(ent, tz=UTC)
            elif hasattr(ent, "to_pydatetime"):
                ent = ent.to_pydatetime()

            if ent.tzinfo is None:
                ent = ent.replace(tzinfo=UTC)

            has_exit = False
            ex = None
            if "exit_epoch" in r and r["exit_epoch"] is not None:
                ex = datetime.fromtimestamp(float(r["exit_epoch"]), tz=UTC)
                has_exit = True
            elif ("exit_at" in r and r["exit_at"] is not None) or (
                "exit_time" in r and r["exit_time"] is not None
            ):
                ex_val = (
                    r.get("exit_at")
                    if r.get("exit_at") is not None
                    else r.get("exit_time")
                )
                if isinstance(ex_val, datetime):
                    ex = ex_val
                elif isinstance(ex_val, str):
                    ex = parse_stream_timestamp(ex_val) or datetime.fromisoformat(
                        ex_val.replace("Z", "+00:00")
                    )
                elif isinstance(ex_val, (int, float)):
                    ex = datetime.fromtimestamp(ex_val, tz=UTC)
                elif hasattr(ex_val, "to_pydatetime"):
                    ex = ex_val.to_pydatetime()
                if ex is not None:
                    has_exit = True

            ent_p = float(r.get("entry_reference_price") or r.get("entry_price") or 0.0)
            if ent_p <= 0:
                raise ValueError(f"Invalid non-positive entry price: {ent_p}")

            if has_exit and ex is not None:
                if ex.tzinfo is None:
                    ex = ex.replace(tzinfo=UTC)
                exit_p = (
                    float(r["exit_price"]) if r.get("exit_price") is not None else ent_p
                )
                pnl_val = r.get("net_pnl_usdt")
                pnl = float(pnl_val) if pnl_val is not None else None
                is_open = False
            else:
                ex = None
                exit_p = None
                pnl = None
                is_open = True

            sym = r["symbol"]
            fee_r = float(r.get("fee_rate") or 0.0005)
            slip_r = float(r.get("slippage_rate") or 0.0002)
            fund_c = float(r.get("funding_cost_usdt") or 0.0)
            base_notional = float(r.get("notional_usdt") or NOTIONAL_PER_ENTRY)
            lev = float(r.get("leverage") or LEVERAGE)
            direction = str(r.get("direction") or "LONG").upper()

        d_str = ent.strftime("%Y-%m-%d")
        scale = day_scales.get(d_str, 1.0) if compounding_scale else 1.0
        notional = base_notional * scale
        scaled_pnl = (pnl * scale) if pnl is not None else None

        records.append(
            TradeRecord(
                trade_id=trade_id,
                symbol=sym,
                entry_time=ent,
                entry_price=ent_p,
                exit_time=ex,
                exit_price=exit_p,
                notional_usdt=notional,
                leverage=lev,
                fee_rate=fee_r,
                slippage_rate=slip_r,
                funding_cost_usdt=(fund_c * scale),
                direction=direction,
                net_pnl_usdt=scaled_pnl,
                is_open=is_open,
            )
        )
    return records


@dataclass(frozen=True)
class ScenarioSpec:
    """Evaluation constraints and scenario characteristics."""

    name: str = "default"
    margin_cap: float | None = None
    compounding: bool = False
    f: float = 0.10
    max_mdd: float | None = None
    slots: int | None = None


@dataclass
class EvaluationContext:
    """Execution context and market state for candidate evaluation."""

    window_start: datetime
    window_end: datetime
    price_series: dict[str, Any] | AlignedPriceGrid
    opps_by_wc: dict[tuple[int, int], list[Any]] | None = None
    events: Sequence[Any] | None = None
    scheduled_risk_window: ScheduledRiskWindowConfig | None = None
    initial_equity: float = INITIAL_EQUITY
    notional_per_entry: float = NOTIONAL_PER_ENTRY
    leverage: float = LEVERAGE
    fee_rate: float = 0.0005
    state_in: PortfolioState | None = None
    is_total_equity: bool = False
    aligned_grid: AlignedPriceGrid | None = None
    ledger: SimulationLedger | None = None
    is_sorted: bool = False

    def __post_init__(self) -> None:
        if self.window_start >= self.window_end:
            raise ValueError(
                f"Invalid evaluation window: window_start ({self.window_start}) "
                f"must be strictly earlier than window_end ({self.window_end})"
            )

        if self.state_in is not None:
            if (
                self.state_in.total_equity_mtm > 0
                and self.initial_equity == INITIAL_EQUITY
            ):
                self.initial_equity = self.state_in.total_equity_mtm
            # When evaluating with carry-in state, initial equity is anchored
            # to total_equity_mtm; MTM evaluation must be in total equity mode
            # to deduct baseline unrealized floating PnL.
            self.is_total_equity = True

        if self.ledger is None:
            self.ledger = SimulationLedger(
                initial_cash=self.initial_equity,
                notional_usdt=self.notional_per_entry,
                leverage=self.leverage,
                fee_rate=self.fee_rate,
            )
        else:
            if abs(self.ledger.notional_usdt - self.notional_per_entry) > 1e-4:
                raise ValueError(
                    f"Conflicting notional: "
                    f"context.notional_per_entry={self.notional_per_entry} "
                    f"!= ledger.notional_usdt={self.ledger.notional_usdt}"
                )
            if abs(self.ledger.leverage - self.leverage) > 1e-4:
                raise ValueError(
                    f"Conflicting leverage: "
                    f"context.leverage={self.leverage} "
                    f"!= ledger.leverage={self.ledger.leverage}"
                )

        if self.aligned_grid is None and self.price_series is not None:
            if isinstance(self.price_series, AlignedPriceGrid):
                self.aligned_grid = AlignedPriceGrid.build(
                    self.price_series,
                    self.window_start,
                    self.window_end,
                    grid_seconds=15,
                )
            elif isinstance(self.price_series, dict) and self.price_series:
                self.aligned_grid = AlignedPriceGrid.build(
                    self.price_series,
                    self.window_start,
                    self.window_end,
                    grid_seconds=15,
                )


@dataclass(frozen=True)
class EvaluationResult:
    """Standardized output of evaluating a candidate in an EvaluationContext."""

    cand_params: dict[str, Any]
    scenario: ScenarioSpec
    admitted_trades: list[TradeRecord]
    raw_admitted: list[dict[str, Any]]
    state_out: PortfolioState | None
    fast_metrics: FastMtmMetrics | None
    metrics: EquityMetrics | None
    curve: list[EquityPoint] | None
    net_pnl: float
    mdd_usdt: float
    mdd_pct: float
    peak_margin: float
    calmar: float
    cdar_95: float
    ulcer_index: float
    terminal_equity: float
    is_feasible: bool
    infeasible_reason: str | None = None

    def to_verification_dict(self) -> dict[str, Any] | None:
        """Convert result to the dictionary format expected by candidate selection."""
        if not self.is_feasible:
            return None
        if self.scenario.compounding:
            return {
                "compounding_mdd": round(self.mdd_pct, 4),
                "compounding_ui": round(self.ulcer_index, 5),
                "compounding_peak_margin": round(self.peak_margin, 2),
                "terminal_compounded_equity": round(self.terminal_equity, 2),
                "net_pnl": round(self.net_pnl, 2),
                "mdd": round(self.mdd_usdt, 2),
                "calmar": round(self.calmar, 3),
                "peak_margin": round(self.peak_margin, 2),
                "cdar_95": round(self.cdar_95, 4),
            }
        else:
            return {
                "net_pnl": round(self.net_pnl, 2),
                "mdd": round(self.mdd_usdt, 2),
                "calmar": round(self.calmar, 3),
                "peak_margin": round(self.peak_margin, 2),
                "cdar_95": round(self.cdar_95, 4),
            }


def _build_effective_state_out(
    state_out: PortfolioState | None,
    trades: Sequence[TradeRecord],
    final_equity: float,
    peak_margin: float,
    window_end: datetime,
    state_in: PortfolioState | None,
    effective_initial_equity: float,
    is_compounding: bool,
) -> PortfolioState:
    """Build standardized PortfolioState ensuring equity and positions match
    evaluation output.
    """
    w_end_epoch = window_end.timestamp()
    active_pos = tuple(
        t
        for t in trades
        if t.exit_time is None or t.exit_time.timestamp() > w_end_epoch
    )
    if is_compounding:
        closed_trades = [
            t
            for t in trades
            if t.exit_time is not None and t.exit_time.timestamp() <= w_end_epoch
        ]
        delta_realized = sum(t.calculated_net_pnl for t in closed_trades)
        base_cash = (
            state_in.cash_usdt if state_in is not None else effective_initial_equity
        )
        base_realized = state_in.realized_pnl_base if state_in is not None else 0.0
        base_fees = state_in.cumulative_fees if state_in is not None else 0.0
        total_fees = sum(t.total_fee_usdt for t in trades)

        return PortfolioState.create(
            timestamp=window_end,
            cash_usdt=round(base_cash + delta_realized, 4),
            total_equity_mtm=round(final_equity, 4),
            active_positions=active_pos,
            realized_pnl_base=round(base_realized + delta_realized, 4),
            cumulative_fees=round(base_fees + total_fees, 4),
            peak_margin=round(peak_margin, 4),
            cooldown_until=state_out.cooldown_map if state_out else {},
        )

    return PortfolioState.create(
        timestamp=window_end,
        cash_usdt=state_out.cash_usdt if state_out else effective_initial_equity,
        total_equity_mtm=round(final_equity, 4),
        active_positions=(
            active_pos
            if active_pos
            else (state_out.active_positions if state_out else ())
        ),
        realized_pnl_base=state_out.realized_pnl_base if state_out else 0.0,
        cumulative_fees=state_out.cumulative_fees if state_out else 0.0,
        peak_margin=round(peak_margin, 4),
        cooldown_until=state_out.cooldown_map if state_out else {},
    )


def evaluate_candidate(
    context: EvaluationContext,
    candidate: dict[str, Any] | Any,
    scenario: ScenarioSpec | None = None,
    include_curve: bool = False,
) -> EvaluationResult:
    """Unified candidate evaluation function.

    Handles window simulation, trade conversion, compounding, margin caps,
    and MTM equity curve / metrics calculation in a single pipeline.
    """
    cand_params = candidate.params if hasattr(candidate, "params") else dict(candidate)
    if scenario is None:
        scenario = ScenarioSpec()

    slots = (
        scenario.slots
        if scenario.slots is not None
        else int(cand_params.get("max_open_positions", 2))
    )
    margin_cap = scenario.margin_cap

    # Extract matching opportunities
    if context.opps_by_wc is not None:
        req_w = int(cand_params.get("impulse_window_buckets", 2))
        req_c = int(cand_params.get("confirmation_buckets", 1))
        opps = context.opps_by_wc.get((req_w, req_c), [])
    else:
        opps = list(context.events) if context.events is not None else []

    has_carry_in = context.state_in is not None and bool(
        context.state_in.active_positions
    )

    effective_initial_equity = (
        context.state_in.total_equity_mtm
        if context.state_in is not None
        else context.initial_equity
    )

    if not opps and not has_carry_in:
        return EvaluationResult(
            cand_params=cand_params,
            scenario=scenario,
            admitted_trades=[],
            raw_admitted=[],
            state_out=context.state_in,
            fast_metrics=None,
            metrics=None,
            curve=[] if include_curve else None,
            net_pnl=0.0,
            mdd_usdt=0.0,
            mdd_pct=0.0,
            peak_margin=0.0,
            calmar=0.0,
            cdar_95=0.0,
            ulcer_index=0.0,
            terminal_equity=effective_initial_equity,
            is_feasible=False,
            infeasible_reason="No opportunities found",
        )

    if context.ledger is None:
        return EvaluationResult(
            cand_params=cand_params,
            scenario=scenario,
            admitted_trades=[],
            raw_admitted=[],
            state_out=context.state_in,
            fast_metrics=None,
            metrics=None,
            curve=[] if include_curve else None,
            net_pnl=0.0,
            mdd_usdt=0.0,
            mdd_pct=0.0,
            peak_margin=0.0,
            calmar=0.0,
            cdar_95=0.0,
            ulcer_index=0.0,
            terminal_equity=effective_initial_equity,
            is_feasible=False,
            infeasible_reason="No simulation ledger configured",
        )

    sim_res, state_out = context.ledger.simulate_window(
        opportunities=opps,
        params=cand_params,
        window_start=context.window_start,
        window_end=context.window_end,
        max_concurrency=slots,
        margin_cap=margin_cap,
        fast_eval=True,
        price_series=context.price_series,
        scheduled_risk_window=context.scheduled_risk_window,
        state_in=context.state_in,
        is_sorted=context.is_sorted,
    )

    carry_in_trades = (
        list(sim_res.effective_carry_in)
        if getattr(sim_res, "effective_carry_in", None) is not None
        else (
            list(context.state_in.active_positions)
            if context.state_in is not None
            else []
        )
    )
    all_window_trades = (
        list(sim_res.all_window_trades)
        if getattr(sim_res, "all_window_trades", None) is not None
        else (carry_in_trades + list(sim_res.admitted_trades))
    )

    if not all_window_trades:
        return EvaluationResult(
            cand_params=cand_params,
            scenario=scenario,
            admitted_trades=[],
            raw_admitted=[],
            state_out=state_out,
            fast_metrics=None,
            metrics=None,
            curve=[] if include_curve else None,
            net_pnl=0.0,
            mdd_usdt=0.0,
            mdd_pct=0.0,
            peak_margin=0.0,
            calmar=0.0,
            cdar_95=0.0,
            ulcer_index=0.0,
            terminal_equity=effective_initial_equity,
            is_feasible=False,
            infeasible_reason="No admitted trades",
        )

    trades = to_trade_records(
        all_window_trades,
        compounding_scale=scenario.compounding,
        f=scenario.f,
        initial_equity=effective_initial_equity,
        prices_by_symbol=context.price_series,
        w_start=context.window_start,
        w_end=context.window_end,
    )

    if not trades:
        return EvaluationResult(
            cand_params=cand_params,
            scenario=scenario,
            admitted_trades=[],
            raw_admitted=all_window_trades,
            state_out=state_out,
            fast_metrics=None,
            metrics=None,
            curve=[] if include_curve else None,
            net_pnl=0.0,
            mdd_usdt=0.0,
            mdd_pct=0.0,
            peak_margin=0.0,
            calmar=0.0,
            cdar_95=0.0,
            ulcer_index=0.0,
            terminal_equity=effective_initial_equity,
            is_feasible=False,
            infeasible_reason="No trade records generated",
        )

    grid = (
        context.aligned_grid
        if context.aligned_grid is not None
        else context.price_series
    )

    if not include_curve:
        fast_m = reconstruct_mtm_metrics_fast(
            trades=trades,
            price_series=grid,
            initial_equity=effective_initial_equity,
            grid_seconds=15,
            start_time=context.window_start,
            end_time=context.window_end,
            margin_cap=margin_cap,
            is_total_equity=context.is_total_equity,
        )
        if fast_m is None or not fast_m.is_feasible:
            event_peak_margin, _ = compute_event_peak_margin_and_count(
                trades,
                context.window_start.timestamp(),
                context.window_end.timestamp(),
            )
            peak_m = fast_m.peak_margin if fast_m is not None else event_peak_margin
            inf_reason = "Insolvent or margin cap exceeded in MTM evaluation"
            if margin_cap is not None and peak_m > margin_cap:
                inf_reason = f"Peak margin {peak_m:.2f} exceeds cap {margin_cap:.2f}"
            return EvaluationResult(
                cand_params=cand_params,
                scenario=scenario,
                admitted_trades=trades,
                raw_admitted=all_window_trades,
                state_out=state_out,
                fast_metrics=fast_m,
                metrics=None,
                curve=None,
                net_pnl=fast_m.net_pnl if fast_m else 0.0,
                mdd_usdt=fast_m.max_drawdown_usdt if fast_m else 0.0,
                mdd_pct=fast_m.max_drawdown_pct if fast_m else 0.0,
                peak_margin=peak_m,
                calmar=0.0,
                cdar_95=fast_m.cdar_95 if fast_m else 0.0,
                ulcer_index=fast_m.ulcer_index if fast_m else 0.0,
                terminal_equity=(
                    fast_m.terminal_equity if fast_m else effective_initial_equity
                ),
                is_feasible=False,
                infeasible_reason=inf_reason,
            )

        calmar = fast_m.net_pnl / max(1.0, fast_m.max_drawdown_usdt)
        is_feasible = True
        infeasible_reason = None
        if scenario.max_mdd is not None and fast_m.max_drawdown_pct > scenario.max_mdd:
            is_feasible = False
            infeasible_reason = (
                f"Max drawdown {fast_m.max_drawdown_pct:.4f} "
                f"exceeds scenario limit {scenario.max_mdd:.4f}"
            )

        state_out = _build_effective_state_out(
            state_out=state_out,
            trades=trades,
            final_equity=fast_m.terminal_equity,
            peak_margin=fast_m.peak_margin,
            window_end=context.window_end,
            state_in=context.state_in,
            effective_initial_equity=effective_initial_equity,
            is_compounding=scenario.compounding,
        )

        return EvaluationResult(
            cand_params=cand_params,
            scenario=scenario,
            admitted_trades=trades,
            raw_admitted=all_window_trades,
            state_out=state_out,
            fast_metrics=fast_m,
            metrics=None,
            curve=None,
            net_pnl=fast_m.net_pnl,
            mdd_usdt=fast_m.max_drawdown_usdt,
            mdd_pct=fast_m.max_drawdown_pct,
            peak_margin=fast_m.peak_margin,
            calmar=calmar,
            cdar_95=fast_m.cdar_95,
            ulcer_index=fast_m.ulcer_index,
            terminal_equity=fast_m.terminal_equity,
            is_feasible=is_feasible,
            infeasible_reason=infeasible_reason,
        )
    else:
        pts = reconstruct_mtm_equity(
            trades=trades,
            price_series=grid,
            initial_equity=effective_initial_equity,
            grid_seconds=15,
            start_time=context.window_start,
            end_time=context.window_end,
            is_total_equity=context.is_total_equity,
        )
        if not pts:
            return EvaluationResult(
                cand_params=cand_params,
                scenario=scenario,
                admitted_trades=trades,
                raw_admitted=all_window_trades,
                state_out=state_out,
                fast_metrics=None,
                metrics=None,
                curve=[],
                net_pnl=0.0,
                mdd_usdt=0.0,
                mdd_pct=0.0,
                peak_margin=0.0,
                calmar=0.0,
                cdar_95=0.0,
                ulcer_index=0.0,
                terminal_equity=effective_initial_equity,
                is_feasible=False,
                infeasible_reason="Empty MTM equity points generated",
            )

        metrics = evaluate_equity_curve(pts, initial_equity=effective_initial_equity)
        event_peak_margin, _ = compute_event_peak_margin_and_count(
            trades,
            context.window_start.timestamp(),
            context.window_end.timestamp(),
        )
        grid_peak_margin = max((p.peak_initial_margin for p in pts), default=0.0)
        peak_margin = max(grid_peak_margin, event_peak_margin)
        is_feasible = metrics.is_feasible and (
            margin_cap is None or peak_margin <= margin_cap
        )
        final_eq = pts[-1].equity if pts else effective_initial_equity
        net_pnl = final_eq - effective_initial_equity
        calmar = (
            net_pnl / max(1.0, metrics.max_drawdown_usdt)
            if metrics.max_drawdown_usdt > 0
            else 0.0
        )
        infeasible_reason = None
        if not metrics.is_feasible:
            infeasible_reason = metrics.infeasible_reason
        elif margin_cap is not None and peak_margin > margin_cap:
            infeasible_reason = (
                f"Peak margin {peak_margin:.2f} exceeds cap {margin_cap:.2f}"
            )
        elif (
            scenario.max_mdd is not None and metrics.max_drawdown_pct > scenario.max_mdd
        ):
            is_feasible = False
            infeasible_reason = (
                f"Max drawdown {metrics.max_drawdown_pct:.4f} "
                f"exceeds scenario limit {scenario.max_mdd:.4f}"
            )

        state_out = _build_effective_state_out(
            state_out=state_out,
            trades=trades,
            final_equity=final_eq,
            peak_margin=peak_margin,
            window_end=context.window_end,
            state_in=context.state_in,
            effective_initial_equity=effective_initial_equity,
            is_compounding=scenario.compounding,
        )

        return EvaluationResult(
            cand_params=cand_params,
            scenario=scenario,
            admitted_trades=trades,
            raw_admitted=all_window_trades,
            state_out=state_out,
            fast_metrics=None,
            metrics=metrics,
            curve=pts,
            net_pnl=net_pnl,
            mdd_usdt=metrics.max_drawdown_usdt,
            mdd_pct=metrics.max_drawdown_pct,
            peak_margin=peak_margin,
            calmar=calmar,
            cdar_95=metrics.cdar_95,
            ulcer_index=metrics.ulcer_index,
            terminal_equity=final_eq,
            is_feasible=is_feasible,
            infeasible_reason=infeasible_reason,
        )
