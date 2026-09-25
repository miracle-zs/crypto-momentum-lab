"""Unified Continuous Mark-to-Market (MTM) Simulation Ledger and State Machine.

Implements the unified bucket-by-bucket / event-driven simulation ledger,
handling portfolio states (`state_in` and `state_out`), slot concurrency,
margin caps, and cross-window carry-in position revaluation according to
the 2026-09-21 repair specification.
"""

from __future__ import annotations

import bisect
import hashlib
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any

from crypto_momentum_lab.live_rollout.scheduled_risk_window import (
    ScheduledRiskWindowConfig,
)
from local_optimization.equity import (
    EquityPoint,
    evaluate_equity_curve,
)
from local_optimization.mtm_engine import (
    TradeRecord,
    get_price_at,
    reconstruct_mtm_equity,
)
from local_optimization.opportunity import (
    OpportunityStatus,
    RawOpportunity,
)


@dataclass(frozen=True)
class PortfolioState:
    """Immutable snapshot of portfolio state at a boundary or time slice."""

    timestamp: datetime
    cash_usdt: float
    total_equity_mtm: float
    active_positions: tuple[TradeRecord, ...] = ()
    realized_pnl_base: float = 0.0
    cumulative_fees: float = 0.0
    peak_margin: float = 0.0
    cooldown_until: tuple[tuple[str, float], ...] = ()
    state_hash: str = ""

    @property
    def active_positions_map(self) -> dict[str, TradeRecord]:
        return {t.symbol: t for t in self.active_positions}

    @property
    def cooldown_map(self) -> dict[str, float]:
        return dict(self.cooldown_until)

    @classmethod
    def create(
        cls,
        timestamp: datetime,
        cash_usdt: float,
        total_equity_mtm: float,
        active_positions: Sequence[TradeRecord] = (),
        realized_pnl_base: float = 0.0,
        cumulative_fees: float = 0.0,
        peak_margin: float = 0.0,
        cooldown_until: dict[str, float] | None = None,
    ) -> PortfolioState:
        pos_tuple = tuple(sorted(active_positions, key=lambda t: t.trade_id))
        cd_tuple = tuple(sorted(cooldown_until.items())) if cooldown_until else ()

        hasher = hashlib.sha256()
        hasher.update(
            f"{timestamp.isoformat()}:{cash_usdt:.4f}:{total_equity_mtm:.4f}\n".encode()
        )
        for t in pos_tuple:
            hasher.update(f"{t.trade_id}:{t.symbol}:{t.entry_price:.6f}\n".encode())
        state_hash = hasher.hexdigest()[:16]

        return cls(
            timestamp=timestamp,
            cash_usdt=round(cash_usdt, 4),
            total_equity_mtm=round(total_equity_mtm, 4),
            active_positions=pos_tuple,
            realized_pnl_base=round(realized_pnl_base, 4),
            cumulative_fees=round(cumulative_fees, 4),
            peak_margin=round(peak_margin, 4),
            cooldown_until=cd_tuple,
            state_hash=state_hash,
        )


@dataclass(frozen=True)
class SimulationExecution:
    """Record of an opportunity's evaluation and execution outcome."""

    opportunity_id: str
    symbol: str
    direction: str
    detected_at: datetime
    entry_time: datetime
    entry_price: float
    exit_time: datetime | None
    exit_price: float | None
    net_pnl_usdt: float
    status: OpportunityStatus
    trade: TradeRecord | None = None


@dataclass
class LedgerResult:
    """Complete analytical output from a window simulation."""

    window_start: datetime
    window_end: datetime
    state_in: PortfolioState
    state_out: PortfolioState
    admitted_trades: list[TradeRecord] = field(default_factory=list)
    effective_carry_in: list[TradeRecord] = field(default_factory=list)
    all_window_trades: list[TradeRecord] = field(default_factory=list)
    executions: list[SimulationExecution] = field(default_factory=list)
    equity_points: list[EquityPoint] = field(default_factory=list)

    # Key Performance Metrics
    oos_pnl: float = 0.0
    mdd_pct: float = 0.0
    mdd_usdt: float = 0.0
    ulcer_index: float = 0.0
    cdar_95: float = 0.0
    win_rate: float = 0.0
    n_trades: int = 0
    carry_in_count: int = 0
    carry_out_count: int = 0
    peak_margin: float = 0.0


class SimulationLedger:
    """Unified chronological MTM simulation engine with portfolio state passing."""

    def __init__(
        self,
        default_leverage: float = 5.0,
        default_notional_usdt: float = 100.0,
        default_fee_rate: float = 0.0005,
        default_slippage_rate: float = 0.0002,
        default_funding_cost_per_trade: float = 0.0,
        default_initial_cash: float = 1000.0,
        **kwargs: Any,
    ) -> None:
        self.leverage = float(kwargs.get("leverage", default_leverage))
        self.notional_usdt = float(kwargs.get("notional_usdt", default_notional_usdt))
        self.fee_rate = float(kwargs.get("fee_rate", default_fee_rate))
        self.slippage_rate = float(kwargs.get("slippage_rate", default_slippage_rate))
        self.funding_cost_per_trade = float(
            kwargs.get("funding_cost_per_trade", default_funding_cost_per_trade)
        )
        self.initial_cash = float(kwargs.get("initial_cash", default_initial_cash))

    def resolve_exit(
        self,
        opp: RawOpportunity,
        params: dict[str, Any] | None,
        price_series: dict[str, tuple[list[float], list[float]]] | None,
    ) -> tuple[datetime | None, datetime | None, float | None]:
        """Resolve dynamic trade exit time, submission time, and price against 15s price series.

        Evaluates dynamic exit triggers when dynamic parameters are specified in `params`:
        - stop_loss_pct: float (e.g. 0.015 for 1.5% stop loss below entry)
        - take_profit_pct: float (e.g. 0.03 for 3.0% take profit above entry)
        - max_holding_seconds: float (e.g. 3600 for 1h timeout)

        If no dynamic exit parameters are provided or symbol price series is missing,
        falls back to opp.exit_time, opp.exit_submitted_at, and opp.exit_price.
        """
        has_dynamic = (
            params is not None
            and (
                params.get("stop_loss_pct") is not None
                or params.get("take_profit_pct") is not None
                or params.get("max_holding_seconds") is not None
            )
        )
        if not has_dynamic or not price_series or opp.symbol not in price_series:
            eff_exit_t = opp.exit_time
            eff_exit_sub_t = getattr(opp, "exit_submitted_at", None) or opp.exit_time
            eff_exit_p = opp.exit_price
            return eff_exit_t, eff_exit_sub_t, eff_exit_p

        sym_prices = price_series.get(opp.symbol)
        if not sym_prices or not sym_prices[0]:
            eff_exit_t = opp.exit_time
            eff_exit_sub_t = getattr(opp, "exit_submitted_at", None) or opp.exit_time
            eff_exit_p = opp.exit_price
            return eff_exit_t, eff_exit_sub_t, eff_exit_p

        ts, ps = sym_prices
        t_entry = opp.entry_eligible_at.timestamp()
        p_entry = opp.entry_reference_price
        direction = opp.direction or "LONG"

        stop_loss_pct = params.get("stop_loss_pct")
        take_profit_pct = params.get("take_profit_pct")
        max_holding_sec = params.get("max_holding_seconds")

        stop_price = None
        if stop_loss_pct is not None and stop_loss_pct > 0:
            stop_price = (
                p_entry * (1.0 - stop_loss_pct)
                if direction == "LONG"
                else p_entry * (1.0 + stop_loss_pct)
            )

        tp_price = None
        if take_profit_pct is not None and take_profit_pct > 0:
            tp_price = (
                p_entry * (1.0 + take_profit_pct)
                if direction == "LONG"
                else p_entry * (1.0 - take_profit_pct)
            )

        max_epoch = (t_entry + max_holding_sec) if max_holding_sec else None

        idx = bisect.bisect_left(ts, t_entry)
        exit_epoch = None
        exit_price = None

        for k in range(idx, len(ts)):
            t_curr = ts[k]
            p_curr = ps[k]

            # Check time stop
            if max_epoch is not None and t_curr >= max_epoch:
                exit_epoch = max_epoch
                exit_price = p_curr
                break

            # Check stop loss
            if stop_price is not None:
                if (direction == "LONG" and p_curr <= stop_price) or (
                    direction == "SHORT" and p_curr >= stop_price
                ):
                    exit_epoch = t_curr
                    exit_price = stop_price
                    break

            # Check take profit
            if tp_price is not None:
                if (direction == "LONG" and p_curr >= tp_price) or (
                    direction == "SHORT" and p_curr <= tp_price
                ):
                    exit_epoch = t_curr
                    exit_price = tp_price
                    break

        if exit_epoch is None:
            if opp.exit_time is not None:
                return (
                    opp.exit_time,
                    getattr(opp, "exit_submitted_at", None) or opp.exit_time,
                    opp.exit_price,
                )
            exit_epoch = ts[-1]
            exit_price = ps[-1]

        exit_dt = datetime.fromtimestamp(exit_epoch, tz=UTC)
        return exit_dt, exit_dt, float(exit_price)

    def simulate_window(
        self,
        opportunities: Sequence[RawOpportunity],
        params: dict[str, Any],
        window_start: datetime,
        window_end: datetime,
        state_in: PortfolioState | None = None,
        price_series: dict[str, tuple[list[float], list[float]]] | None = None,
        max_concurrency: int = 2,
        margin_cap: float | None = None,
        grid_seconds: int = 15,
        fast_eval: bool = False,
        scheduled_risk_window: ScheduledRiskWindowConfig | None = None,
        is_sorted: bool = False,
        close_on_third_signal: bool = False,
    ) -> tuple[LedgerResult, PortfolioState]:
        """Simulate a continuous window with state input, constraints, and MTM path."""
        # 1. Initialize State In
        if state_in is None:
            state_in = PortfolioState.create(
                timestamp=window_start,
                cash_usdt=self.initial_cash,
                total_equity_mtm=self.initial_cash,
                active_positions=(),
            )

        carry_in_positions = list(state_in.active_positions)
        if scheduled_risk_window is not None and carry_in_positions:
            updated_carry_in = []
            for t in carry_in_positions:
                flat_dt = scheduled_risk_window.next_flatten_time(t.entry_time)
                if t.exit_time is None or t.exit_time > flat_dt:
                    flat_epoch = flat_dt.timestamp()
                    flat_p = (
                        get_price_at(
                            price_series.get(t.symbol) if price_series else None,
                            flat_epoch,
                            fallback_price=t.entry_price,
                        )
                        if price_series
                        else t.entry_price
                    )
                    t_flat = TradeRecord(
                        trade_id=t.trade_id,
                        symbol=t.symbol,
                        entry_time=t.entry_time,
                        entry_price=t.entry_price,
                        exit_time=flat_dt,
                        exit_submitted_time=t.exit_submitted_time or flat_dt,
                        exit_price=flat_p,
                        notional_usdt=t.notional_usdt,
                        leverage=t.leverage,
                        fee_rate=t.fee_rate,
                        slippage_rate=t.slippage_rate,
                        funding_cost_usdt=t.funding_cost_usdt,
                        direction=t.direction,
                        net_pnl_usdt=None,
                    )
                    updated_carry_in.append(t_flat)
                else:
                    updated_carry_in.append(t)
            carry_in_positions = updated_carry_in

        cooldown_until: dict[str, float] = state_in.cooldown_map.copy()

        req_w = int(params.get("impulse_window_buckets", 2))
        req_c = int(params.get("confirmation_buckets", 1))
        min_r = float(params.get("min_return_pct", 0.5))
        min_imb = float(params.get("min_imbalance", 0.3))
        min_inten = float(params.get("min_intensity", 1.5))
        min_vol = float(params.get("min_volume_ratio", 0.0))
        cd_buckets = int(params.get("cooldown_buckets", 0))
        cd_seconds = cd_buckets * 15

        executions: list[SimulationExecution] = []
        newly_admitted_trades: list[TradeRecord] = []
        batch_suppress_until: dict[str, float] = {}

        # 2. Sequential opportunity evaluation within [window_start, window_end)
        w_start_epoch = window_start.timestamp()
        w_end_epoch = window_end.timestamp()

        # Check if opportunities already sorted
        if is_sorted:
            if opportunities and isinstance(opportunities[0], dict):
                sorted_opps = [
                    RawOpportunity.from_dict(opp) if isinstance(opp, dict) else opp
                    for opp in opportunities
                ]
            else:
                sorted_opps = opportunities
        else:
            typed_opps: list[RawOpportunity] = []
            for opp in opportunities:
                if isinstance(opp, RawOpportunity):
                    typed_opps.append(opp)
                elif isinstance(opp, dict):
                    typed_opps.append(RawOpportunity.from_dict(opp))
                else:
                    typed_opps.append(opp)

            sorted_opps = sorted(
                typed_opps,
                key=lambda x: (
                    x.entry_epoch
                    if hasattr(x, "entry_epoch")
                    else x.entry_eligible_at.timestamp(),
                    getattr(x, "detected_epoch", 0.0),
                    getattr(x, "opportunity_id", ""),
                ),
            )

        active_trades: list[TradeRecord] = [
            t
            for t in carry_in_positions
            if t.exit_time is None or t.exit_time.timestamp() > w_start_epoch
        ]
        max_concurrent_seen = len(active_trades)

        for opp in sorted_opps:
            t_entry = (
                opp.entry_epoch
                if hasattr(opp, "entry_epoch")
                else opp.entry_eligible_at.timestamp()
            )
            # Strict half-open window boundary on trade entry: [start, end)
            if t_entry < w_start_epoch or t_entry >= w_end_epoch:
                continue

            # Scheduled risk window entry gate
            if (
                scheduled_risk_window is not None
                and not scheduled_risk_window.is_entry_allowed(opp.entry_eligible_at)
            ):
                if not fast_eval:
                    executions.append(
                        SimulationExecution(
                            opportunity_id=opp.opportunity_id,
                            symbol=opp.symbol,
                            direction=opp.direction,
                            detected_at=opp.detected_at,
                            entry_time=opp.entry_eligible_at,
                            entry_price=opp.entry_reference_price,
                            exit_time=opp.exit_time,
                            exit_price=opp.exit_price,
                            net_pnl_usdt=0.0,
                            status=OpportunityStatus.ENTRY_REJECTED,
                        )
                    )
                continue

            # A. Parameter threshold filter
            is_match = (
                opp.impulse_window_buckets == req_w
                and opp.confirmation_buckets == req_c
                and opp.impulse_return_pct >= min_r
                and opp.aggressive_imbalance >= min_imb
                and opp.confirmation_min_imbalance >= min_imb
                and opp.notional_intensity >= min_inten
                and (min_vol <= 0 or opp.volume_ratio >= min_vol)
            )
            if not is_match:
                if not fast_eval:
                    executions.append(
                        SimulationExecution(
                            opportunity_id=opp.opportunity_id,
                            symbol=opp.symbol,
                            direction=opp.direction,
                            detected_at=opp.detected_at,
                            entry_time=opp.entry_eligible_at,
                            entry_price=opp.entry_reference_price,
                            exit_time=opp.exit_time,
                            exit_price=opp.exit_price,
                            net_pnl_usdt=0.0,
                            status=OpportunityStatus.FILTERED,
                        )
                    )
                continue

            # B. Cooldown filter
            if cd_seconds > 0 and t_entry < cooldown_until.get(opp.symbol, 0.0):
                if not fast_eval:
                    executions.append(
                        SimulationExecution(
                            opportunity_id=opp.opportunity_id,
                            symbol=opp.symbol,
                            direction=opp.direction,
                            detected_at=opp.detected_at,
                            entry_time=opp.entry_eligible_at,
                            entry_price=opp.entry_reference_price,
                            exit_time=opp.exit_time,
                            exit_price=opp.exit_price,
                            net_pnl_usdt=0.0,
                            status=OpportunityStatus.FILTERED,
                        )
                    )
                continue

            # Valid price check
            if opp.entry_reference_price <= 0:
                if not fast_eval:
                    executions.append(
                        SimulationExecution(
                            opportunity_id=opp.opportunity_id,
                            symbol=opp.symbol,
                            direction=opp.direction,
                            detected_at=opp.detected_at,
                            entry_time=opp.entry_eligible_at,
                            entry_price=opp.entry_reference_price,
                            exit_time=opp.exit_time,
                            exit_price=opp.exit_price,
                            net_pnl_usdt=0.0,
                            status=OpportunityStatus.ENTRY_REJECTED,
                        )
                    )
                continue

            # C. Active positions at entry_eligible_at (prune exited trades)
            active_trades = [
                t
                for t in active_trades
                if t.exit_time is None or t.exit_time.timestamp() > t_entry
            ]
            open_trades = active_trades

            if close_on_third_signal and t_entry < batch_suppress_until.get(opp.symbol, 0.0):
                if not fast_eval:
                    executions.append(
                        SimulationExecution(
                            opportunity_id=opp.opportunity_id,
                            symbol=opp.symbol,
                            direction=opp.direction,
                            detected_at=opp.detected_at,
                            entry_time=opp.entry_eligible_at,
                            entry_price=opp.entry_reference_price,
                            exit_time=opp.exit_time,
                            exit_price=opp.exit_price,
                            net_pnl_usdt=0.0,
                            status=OpportunityStatus.ENTRY_REJECTED,
                        )
                    )
                continue

            # Check per-symbol slot concurrency limit (slots per symbol).
            # A trade only occupies the active batch slot while its exit order
            # has NOT been submitted. Once its exit order is submitted
            # (exit_submitted_time <= t_entry), the batch is completed and the
            # slot is freed for new batches of this symbol.
            symbol_active_batch_trades = [
                t
                for t in open_trades
                if t.symbol == opp.symbol
                and (
                    (t.exit_submitted_time or t.exit_time) is None
                    or (t.exit_submitted_time or t.exit_time).timestamp() > t_entry
                )
            ]
            if len(symbol_active_batch_trades) >= max_concurrency:
                if close_on_third_signal:
                    max_orig_exit = max(
                        (
                            (t.exit_submitted_time or t.exit_time).timestamp()
                            for t in symbol_active_batch_trades
                            if (t.exit_submitted_time or t.exit_time)
                        ),
                        default=t_entry,
                    )
                    batch_suppress_until[opp.symbol] = max_orig_exit

                    for ot in symbol_active_batch_trades:
                        gross = (
                            ot.notional_usdt
                            * (opp.entry_reference_price - ot.entry_price)
                            / ot.entry_price
                        )
                        if ot.direction == "SHORT":
                            gross = -gross
                        fees = ot.notional_usdt * (
                            ot.fee_rate
                            + ot.slippage_rate
                            + (opp.fee_rate if opp.fee_rate else self.fee_rate)
                            + (
                                getattr(opp, "slippage_rate", None)
                                if getattr(opp, "slippage_rate", None) is not None
                                else self.slippage_rate
                            )
                        )
                        new_pnl = gross - fees - ot.funding_cost_usdt
                        new_t = replace(
                            ot,
                            exit_time=opp.entry_eligible_at,
                            exit_submitted_time=opp.entry_eligible_at,
                            exit_price=opp.entry_reference_price,
                            net_pnl_usdt=round(new_pnl, 4),
                        )
                        if ot in active_trades:
                            active_trades.remove(ot)
                        if ot in newly_admitted_trades:
                            idx = newly_admitted_trades.index(ot)
                            newly_admitted_trades[idx] = new_t

                if not fast_eval:
                    executions.append(
                        SimulationExecution(
                            opportunity_id=opp.opportunity_id,
                            symbol=opp.symbol,
                            direction=opp.direction,
                            detected_at=opp.detected_at,
                            entry_time=opp.entry_eligible_at,
                            entry_price=opp.entry_reference_price,
                            exit_time=opp.exit_time,
                            exit_price=opp.exit_price,
                            net_pnl_usdt=0.0,
                            status=OpportunityStatus.ENTRY_REJECTED,
                        )
                    )
                continue

            # Margin cap check
            new_margin = self.notional_usdt / self.leverage
            current_margin = sum(t.initial_margin_usdt for t in open_trades)
            if margin_cap is not None and (current_margin + new_margin) > margin_cap:
                if not fast_eval:
                    executions.append(
                        SimulationExecution(
                            opportunity_id=opp.opportunity_id,
                            symbol=opp.symbol,
                            direction=opp.direction,
                            detected_at=opp.detected_at,
                            entry_time=opp.entry_eligible_at,
                            entry_price=opp.entry_reference_price,
                            exit_time=opp.exit_time,
                            exit_price=opp.exit_price,
                            net_pnl_usdt=0.0,
                            status=OpportunityStatus.ENTRY_REJECTED,
                        )
                    )
                continue

            # D. Admitted! Create TradeRecord
            effective_exit_time, effective_exit_sub_time, effective_exit_price = (
                self.resolve_exit(opp, params, price_series)
            )

            if scheduled_risk_window is not None:
                flat_dt = scheduled_risk_window.next_flatten_time(opp.entry_eligible_at)
                if effective_exit_time is None or effective_exit_time > flat_dt:
                    effective_exit_time = flat_dt
                    flat_epoch = flat_dt.timestamp()
                    if price_series is not None and opp.symbol in price_series:
                        effective_exit_price = get_price_at(
                            price_series[opp.symbol],
                            flat_epoch,
                            fallback_price=opp.entry_reference_price,
                        )
                    else:
                        effective_exit_price = opp.entry_reference_price
                if effective_exit_sub_time is None or effective_exit_sub_time > flat_dt:
                    effective_exit_sub_time = flat_dt

            trade = TradeRecord(
                trade_id=opp.opportunity_id,
                symbol=opp.symbol,
                entry_time=opp.entry_eligible_at,
                entry_price=opp.entry_reference_price,
                exit_time=effective_exit_time,
                exit_submitted_time=effective_exit_sub_time,
                exit_price=effective_exit_price,
                notional_usdt=self.notional_usdt,
                leverage=self.leverage,
                fee_rate=opp.fee_rate if opp.fee_rate else self.fee_rate,
                slippage_rate=(
                    getattr(opp, "slippage_rate", None)
                    if getattr(opp, "slippage_rate", None) is not None
                    else self.slippage_rate
                ),
                funding_cost_usdt=(
                    getattr(opp, "funding_cost_usdt", None)
                    if getattr(opp, "funding_cost_usdt", None) is not None
                    else self.funding_cost_per_trade
                ),
                direction=opp.direction,
                net_pnl_usdt=None,
            )
            newly_admitted_trades.append(trade)
            active_trades.append(trade)
            if len(active_trades) > max_concurrent_seen:
                max_concurrent_seen = len(active_trades)
            if cd_seconds > 0:
                cooldown_until[opp.symbol] = t_entry + cd_seconds

            if not fast_eval:
                executions.append(
                    SimulationExecution(
                        opportunity_id=opp.opportunity_id,
                        symbol=opp.symbol,
                        direction=opp.direction,
                        detected_at=opp.detected_at,
                        entry_time=opp.entry_eligible_at,
                        entry_price=opp.entry_reference_price,
                        exit_time=effective_exit_time,
                        exit_price=effective_exit_price,
                        net_pnl_usdt=trade.calculated_net_pnl,
                        status=OpportunityStatus.ENTERED,
                        trade=trade,
                    )
                )

        all_window_trades = carry_in_positions + newly_admitted_trades

        # Short-circuit: no trades active in this window
        if not all_window_trades:
            final_equity = state_in.total_equity_mtm
            state_out = PortfolioState.create(
                timestamp=window_end,
                cash_usdt=state_in.cash_usdt,
                total_equity_mtm=final_equity,
                active_positions=(),
                realized_pnl_base=state_in.realized_pnl_base,
                cumulative_fees=state_in.cumulative_fees,
                peak_margin=0.0,
                cooldown_until=cooldown_until,
            )
            res = LedgerResult(
                window_start=window_start,
                window_end=window_end,
                state_in=state_in,
                state_out=state_out,
                admitted_trades=[],
                effective_carry_in=carry_in_positions,
                all_window_trades=carry_in_positions,
                executions=executions,
                equity_points=[],
                oos_pnl=0.0,
                mdd_pct=0.0,
                mdd_usdt=0.0,
                ulcer_index=0.0,
                cdar_95=0.0,
                win_rate=0.0,
                n_trades=0,
                carry_in_count=len(carry_in_positions),
                carry_out_count=0,
                peak_margin=0.0,
            )
            return res, state_out

        # Fast evaluation mode: skip 15s MTM tick reconstruction
        if fast_eval:
            carry_out_positions = [
                t
                for t in all_window_trades
                if t.exit_time is None or t.exit_time.timestamp() > w_end_epoch
            ]
            closed_in_window = [
                t
                for t in all_window_trades
                if t.exit_time and t.exit_time.timestamp() <= w_end_epoch
            ]
            delta_realized = sum(t.calculated_net_pnl for t in closed_in_window)

            # Evaluate floating PnL of carry-in positions at window_start
            floating_pnl_start = 0.0
            for t in carry_in_positions:
                if t.entry_price > 0:
                    sym_prices = price_series.get(t.symbol) if price_series else None
                    p_start = (
                        get_price_at(sym_prices, w_start_epoch, t.entry_price)
                        if sym_prices
                        else t.entry_price
                    )
                    gross_s = (
                        t.notional_usdt * (p_start - t.entry_price) / t.entry_price
                    )
                    if t.direction == "SHORT":
                        gross_s = -gross_s
                    floating_pnl_start += gross_s - (
                        t.notional_usdt * (t.fee_rate + t.slippage_rate)
                    )

            # Evaluate floating PnL of carry-out positions at window_end
            floating_pnl_end = 0.0
            for t in carry_out_positions:
                if t.entry_price > 0:
                    sym_prices = price_series.get(t.symbol) if price_series else None
                    p_end = (
                        get_price_at(sym_prices, w_end_epoch, t.entry_price)
                        if sym_prices
                        else t.entry_price
                    )
                    gross_e = t.notional_usdt * (p_end - t.entry_price) / t.entry_price
                    if t.direction == "SHORT":
                        gross_e = -gross_e
                    floating_pnl_end += gross_e - (
                        t.notional_usdt * (t.fee_rate + t.slippage_rate)
                    )

            final_equity = round(
                state_in.total_equity_mtm
                + delta_realized
                + floating_pnl_end
                - floating_pnl_start,
                4,
            )
            peak_margin = round(
                max_concurrent_seen * (self.notional_usdt / self.leverage), 4
            )

            cum_pnl = 0.0
            peak_pnl = 0.0
            mdd_usdt = 0.0
            sorted_closed = sorted(
                closed_in_window,
                key=lambda x: x.exit_time.timestamp() if x.exit_time else 0.0,
            )
            for t in sorted_closed:
                cum_pnl += t.calculated_net_pnl
                if cum_pnl > peak_pnl:
                    peak_pnl = cum_pnl
                dd = peak_pnl - cum_pnl
                if dd > mdd_usdt:
                    mdd_usdt = dd
            mdd_pct = (
                round(mdd_usdt / state_in.total_equity_mtm, 4)
                if state_in.total_equity_mtm > 0
                else 0.0
            )
            ulcer_index = max(0.005, (mdd_usdt / 1000.0) * 0.45)

            closed_new = [
                t
                for t in newly_admitted_trades
                if t.exit_time and t.exit_time.timestamp() <= w_end_epoch
            ]
            win_count = sum(1 for t in closed_new if t.calculated_net_pnl > 0)
            n_closed = len(closed_new)
            win_rate = round(win_count / n_closed * 100.0, 2) if n_closed else 0.0
            oos_pnl = round(final_equity - state_in.total_equity_mtm, 4)

            state_out = PortfolioState.create(
                timestamp=window_end,
                cash_usdt=round(state_in.cash_usdt + delta_realized, 4),
                total_equity_mtm=final_equity,
                active_positions=carry_out_positions,
                realized_pnl_base=round(state_in.realized_pnl_base + delta_realized, 4),
                cumulative_fees=state_in.cumulative_fees,
                peak_margin=peak_margin,
                cooldown_until=cooldown_until,
            )
            res = LedgerResult(
                window_start=window_start,
                window_end=window_end,
                state_in=state_in,
                state_out=state_out,
                admitted_trades=newly_admitted_trades,
                effective_carry_in=carry_in_positions,
                all_window_trades=all_window_trades,
                executions=[],
                equity_points=[],
                oos_pnl=oos_pnl,
                mdd_pct=mdd_pct,
                mdd_usdt=round(mdd_usdt, 4),
                ulcer_index=round(ulcer_index, 6),
                cdar_95=round(mdd_usdt, 4),
                win_rate=win_rate,
                n_trades=len(newly_admitted_trades),
                carry_in_count=len(carry_in_positions),
                carry_out_count=len(carry_out_positions),
                peak_margin=peak_margin,
            )
            return res, state_out

        # 3. Reconstruct true 15s continuous MTM equity trajectory (full mode)
        pts = reconstruct_mtm_equity(
            trades=all_window_trades,
            price_series=price_series or {},
            start_time=window_start,
            end_time=window_end,
            initial_equity=state_in.total_equity_mtm,
            grid_seconds=grid_seconds,
            is_total_equity=True,
        )

        # 4. Construct State Out at window_end
        # Positions still active at window_end are carried forward (no forced close)
        carry_out_positions = [
            t
            for t in all_window_trades
            if t.exit_time is None or t.exit_time.timestamp() > w_end_epoch
        ]

        final_equity = pts[-1].equity if pts else state_in.total_equity_mtm
        peak_margin = max((p.peak_initial_margin for p in pts), default=0.0)
        total_fees = sum(p.fee for p in pts) if pts else 0.0
        delta_realized = pts[-1].realized_pnl if pts else 0.0

        state_out = PortfolioState.create(
            timestamp=window_end,
            cash_usdt=round(state_in.cash_usdt + delta_realized, 4),
            total_equity_mtm=final_equity,
            active_positions=carry_out_positions,
            realized_pnl_base=state_in.realized_pnl_base + delta_realized,
            cumulative_fees=state_in.cumulative_fees + total_fees,
            peak_margin=peak_margin,
            cooldown_until=cooldown_until,
        )

        # 5. Evaluate Window Metrics
        metrics = evaluate_equity_curve(pts, initial_equity=state_in.total_equity_mtm)
        oos_pnl = round(final_equity - state_in.total_equity_mtm, 4)

        closed_in_window = [
            t
            for t in newly_admitted_trades
            if t.exit_time and t.exit_time.timestamp() <= w_end_epoch
        ]
        win_count = sum(1 for t in closed_in_window if t.calculated_net_pnl > 0)
        n_closed = len(closed_in_window)
        win_rate = round(win_count / n_closed * 100.0, 2) if n_closed else 0.0

        res = LedgerResult(
            window_start=window_start,
            window_end=window_end,
            state_in=state_in,
            state_out=state_out,
            admitted_trades=newly_admitted_trades,
            effective_carry_in=carry_in_positions,
            all_window_trades=all_window_trades,
            executions=executions,
            equity_points=pts,
            oos_pnl=oos_pnl,
            mdd_pct=metrics.max_drawdown_pct,
            mdd_usdt=metrics.max_drawdown_usdt,
            ulcer_index=metrics.ulcer_index,
            cdar_95=metrics.cdar_95,
            win_rate=win_rate,
            n_trades=len(newly_admitted_trades),
            carry_in_count=len(carry_in_positions),
            carry_out_count=len(carry_out_positions),
            peak_margin=state_out.peak_margin,
        )

        return res, state_out
