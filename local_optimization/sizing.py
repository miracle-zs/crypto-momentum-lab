"""Compounding position sizing engine and intraday risk validation.

Implements the specification defined in:
docs/research/2026-09-19-compounding-position-sizing-and-objectives.md

Features:
1. Experiment A: Fixed notional sizing baseline (N = f * E0 or fixed 100U).
2. Experiment B: Daily equity ratio sizing (N_d = f * E_d) at UTC 00:00 day-cut.
3. Experiment C: Risk-adaptive extensions (smoothing, vol targeting, derisking).
4. Intraday portfolio risk check:
   (M_position + M_reserved + delta_M_order) / E(t) <= m_cap.
5. Margin reservation lifecycle: working orders reserve margin without double counting.
"""

from __future__ import annotations

import math
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime

import numpy as np


@dataclass(frozen=True)
class ExternalCashFlow:
    """External cash flow (deposit or withdrawal) for TWR calculation."""

    timestamp: datetime
    amount_usdt: float  # Positive for deposit, negative for withdrawal
    flow_type: str = "DEPOSIT"  # DEPOSIT | WITHDRAWAL


@dataclass
class DailySizingState:
    """Frozen sizing benchmark state determined at day-cut."""

    date_str: str
    sizing_cutoff: datetime
    equity_observed: float
    target_notional: float
    sizing_version: int = 1
    notes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class RiskCheckResult:
    """Outcome of intraday order admission check."""

    allowed: bool
    notional_usdt: float
    margin_required_usdt: float
    reason: str = ""
    current_margin_ratio: float = 0.0


class SizingPolicy(ABC):
    """Abstract base class for position sizing policies."""

    @abstractmethod
    def on_day_cut(
        self,
        current_equity: float,
        current_time: datetime,
    ) -> DailySizingState:
        """Handle daily boundary (UTC 00:00) and freeze next day sizing."""

    @abstractmethod
    def get_order_notional(
        self,
        symbol: str,
        current_equity: float,
        current_time: datetime,
    ) -> float:
        """Retrieve the target notional amount for a new order."""

    @abstractmethod
    def check_intraday_order(
        self,
        symbol: str,
        price: float,
        current_equity: float,
        position_margin: float,
        reserved_margin: float,
        leverage: float = 5.0,
        current_time: datetime | None = None,
    ) -> RiskCheckResult:
        """Evaluate whether a proposed order complies with intraday portfolio risk."""


class FixedNotionalSizing(SizingPolicy):
    """Experiment A: Fixed notional baseline.

    Every order uses a constant notional amount regardless of equity growth.
    """

    def __init__(
        self,
        fixed_notional: float = 100.0,
        max_initial_margin_usdt: float | None = 280.0,
        margin_ratio_cap: float | None = None,
    ) -> None:
        self.fixed_notional = float(fixed_notional)
        self.max_initial_margin_usdt = max_initial_margin_usdt
        self.margin_ratio_cap = margin_ratio_cap
        self.rejected_orders_count: int = 0
        self.admitted_orders_count: int = 0
        self._current_state = DailySizingState(
            date_str="initial",
            sizing_cutoff=datetime(1970, 1, 1, tzinfo=UTC),
            equity_observed=1000.0,
            target_notional=self.fixed_notional,
            sizing_version=0,
        )

    def on_day_cut(
        self,
        current_equity: float,
        current_time: datetime,
    ) -> DailySizingState:
        date_str = current_time.strftime("%Y-%m-%d")
        self._current_state = DailySizingState(
            date_str=date_str,
            sizing_cutoff=current_time,
            equity_observed=current_equity,
            target_notional=self.fixed_notional,
            sizing_version=self._current_state.sizing_version + 1,
            notes=["Fixed notional policy: sizing unchanged"],
        )
        return self._current_state

    def get_order_notional(
        self,
        symbol: str,
        current_equity: float,
        current_time: datetime,
    ) -> float:
        return self.fixed_notional

    def check_intraday_order(
        self,
        symbol: str,
        price: float,
        current_equity: float,
        position_margin: float,
        reserved_margin: float,
        leverage: float = 5.0,
        current_time: datetime | None = None,
    ) -> RiskCheckResult:
        notional = self.fixed_notional
        order_margin = notional / leverage
        prospective_margin = position_margin + reserved_margin + order_margin

        if self.max_initial_margin_usdt is not None:
            if prospective_margin > self.max_initial_margin_usdt + 1e-4:
                self.rejected_orders_count += 1
                return RiskCheckResult(
                    allowed=False,
                    notional_usdt=notional,
                    margin_required_usdt=order_margin,
                    reason=(
                        f"Prospective margin {prospective_margin:.2f}U exceeds "
                        f"cap {self.max_initial_margin_usdt:.2f}U"
                    ),
                    current_margin_ratio=(
                        prospective_margin / current_equity
                        if current_equity > 0
                        else math.inf
                    ),
                )

        if self.margin_ratio_cap is not None and current_equity > 0:
            ratio = prospective_margin / current_equity
            if ratio > self.margin_ratio_cap + 1e-4:
                self.rejected_orders_count += 1
                return RiskCheckResult(
                    allowed=False,
                    notional_usdt=notional,
                    margin_required_usdt=order_margin,
                    reason=(
                        f"Margin ratio {ratio:.1%} exceeds cap "
                        f"{self.margin_ratio_cap:.1%}"
                    ),
                    current_margin_ratio=ratio,
                )

        self.admitted_orders_count += 1
        return RiskCheckResult(
            allowed=True,
            notional_usdt=notional,
            margin_required_usdt=order_margin,
            current_margin_ratio=(
                prospective_margin / current_equity if current_equity > 0 else 0.0
            ),
        )


class DailyEquityRatioSizing(SizingPolicy):
    """Experiment B: Daily Equity Ratio Compounding.

    At day-cut (UTC 00:00 / Beijing 08:00):
        N_d = fraction_f * E_d
    Intraday:
        Checks (M_position + M_reserved + delta_M) / E(t) <= margin_ratio_cap.
    """

    def __init__(
        self,
        fraction_f: float = 0.10,
        margin_ratio_cap: float = 0.28,
        max_initial_margin_usdt: float | None = None,
        min_order_notional: float = 10.0,
        initial_equity: float = 1000.0,
    ) -> None:
        self.fraction_f = float(fraction_f)
        self.margin_ratio_cap = float(margin_ratio_cap)
        self.max_initial_margin_usdt = max_initial_margin_usdt
        self.min_order_notional = float(min_order_notional)
        self.rejected_orders_count: int = 0
        self.admitted_orders_count: int = 0

        initial_notional = max(
            self.min_order_notional, self.fraction_f * initial_equity
        )
        self.current_state = DailySizingState(
            date_str="initial",
            sizing_cutoff=datetime(1970, 1, 1, tzinfo=UTC),
            equity_observed=initial_equity,
            target_notional=round(initial_notional, 2),
            sizing_version=0,
        )

    def on_day_cut(
        self,
        current_equity: float,
        current_time: datetime,
    ) -> DailySizingState:
        date_str = current_time.strftime("%Y-%m-%d")
        if current_equity <= 0:
            target_notional = 0.0
            notes = ["Equity non-positive: new order notional set to 0.0"]
        else:
            raw_notional = self.fraction_f * current_equity
            target_notional = max(self.min_order_notional, round(raw_notional, 2))
            notes = [
                f"Day-cut {date_str} E_d={current_equity:.2f}U "
                f"-> N_d={target_notional:.2f}U"
            ]

        self.current_state = DailySizingState(
            date_str=date_str,
            sizing_cutoff=current_time,
            equity_observed=current_equity,
            target_notional=target_notional,
            sizing_version=self.current_state.sizing_version + 1,
            notes=notes,
        )
        return self.current_state

    def get_order_notional(
        self,
        symbol: str,
        current_equity: float,
        current_time: datetime,
    ) -> float:
        return self.current_state.target_notional

    def check_intraday_order(
        self,
        symbol: str,
        price: float,
        current_equity: float,
        position_margin: float,
        reserved_margin: float,
        leverage: float = 5.0,
        current_time: datetime | None = None,
    ) -> RiskCheckResult:
        notional = self.get_order_notional(
            symbol, current_equity, current_time or datetime.now(UTC)
        )
        if notional <= 0:
            self.rejected_orders_count += 1
            return RiskCheckResult(
                allowed=False,
                notional_usdt=0.0,
                margin_required_usdt=0.0,
                reason="Sizing notional is zero or negative (insolvent)",
            )

        order_margin = notional / leverage
        prospective_margin = position_margin + reserved_margin + order_margin

        if current_equity <= 0:
            self.rejected_orders_count += 1
            return RiskCheckResult(
                allowed=False,
                notional_usdt=notional,
                margin_required_usdt=order_margin,
                reason="Account equity is non-positive",
            )

        ratio = prospective_margin / current_equity
        if ratio > self.margin_ratio_cap + 1e-4:
            self.rejected_orders_count += 1
            return RiskCheckResult(
                allowed=False,
                notional_usdt=notional,
                margin_required_usdt=order_margin,
                reason=(
                    f"Margin ratio {ratio:.1%} exceeds cap {self.margin_ratio_cap:.1%}"
                ),
                current_margin_ratio=ratio,
            )

        if self.max_initial_margin_usdt is not None:
            if prospective_margin > self.max_initial_margin_usdt + 1e-4:
                self.rejected_orders_count += 1
                return RiskCheckResult(
                    allowed=False,
                    notional_usdt=notional,
                    margin_required_usdt=order_margin,
                    reason=(
                        f"Total margin {prospective_margin:.2f}U exceeds capacity cap "
                        f"{self.max_initial_margin_usdt:.2f}U"
                    ),
                    current_margin_ratio=ratio,
                )

        self.admitted_orders_count += 1
        return RiskCheckResult(
            allowed=True,
            notional_usdt=notional,
            margin_required_usdt=order_margin,
            current_margin_ratio=ratio,
        )


class RiskAdaptiveSizing(DailyEquityRatioSizing):
    """Experiment C: Risk-Adaptive Extension.

    Supports:
    1. Asymmetric smoothing: B_d = min(E_d, (1 - alpha)*B_{d-1} + alpha*E_d).
       (Fast cut on loss, smooth scale-up on profit).
    2. Volatility scaling:
       clip(sigma_target / max(sigma_hat, sigma_floor), k_min, k_max).
    3. Drawdown derisking: reduce f during drawdowns with hysteresis.
    """

    def __init__(
        self,
        fraction_f: float = 0.10,
        margin_ratio_cap: float = 0.28,
        min_order_notional: float = 10.0,
        initial_equity: float = 1000.0,
        smoothing_alpha: float = 0.5,
        use_smoothing: bool = True,
        use_vol_scaling: bool = False,
        target_vol: float = 0.02,
        vol_window: int = 14,
        use_dd_derisking: bool = False,
        dd_trigger: float = 0.10,
        dd_multiplier: float = 0.5,
        dd_recovery: float = 0.05,
    ) -> None:
        super().__init__(
            fraction_f=fraction_f,
            margin_ratio_cap=margin_ratio_cap,
            min_order_notional=min_order_notional,
            initial_equity=initial_equity,
        )
        self.smoothing_alpha = smoothing_alpha
        self.use_smoothing = use_smoothing
        self.use_vol_scaling = use_vol_scaling
        self.target_vol = target_vol
        self.vol_window = vol_window
        self.use_dd_derisking = use_dd_derisking
        self.dd_trigger = dd_trigger
        self.dd_multiplier = dd_multiplier
        self.dd_recovery = dd_recovery

        # State
        self.smoothed_base: float | None = initial_equity if use_smoothing else None
        self.is_derisked: bool = False
        self.hwm_neutral: float = initial_equity
        self.daily_returns: list[float] = []
        self.last_day_cut_equity: float | None = initial_equity

    def on_day_cut(
        self,
        current_equity: float,
        current_time: datetime,
    ) -> DailySizingState:
        """Calculate B_d, vol multiplier, DD multiplier, and determine N_d."""
        date_str = current_time.strftime("%Y-%m-%d")

        # Record daily return for vol estimation
        if self.last_day_cut_equity is not None and self.last_day_cut_equity > 0:
            ret = (current_equity - self.last_day_cut_equity) / self.last_day_cut_equity
            self.daily_returns.append(ret)
        self.last_day_cut_equity = current_equity

        # Track HWM for DD derisking
        if current_equity > self.hwm_neutral:
            self.hwm_neutral = current_equity

        # 1. Asymmetric smoothing: B_0 = E_0
        if not self.use_smoothing:
            base_e = current_equity
        else:
            if self.smoothed_base is None:
                self.smoothed_base = current_equity
            else:
                raw_smooth = (
                    1.0 - self.smoothing_alpha
                ) * self.smoothed_base + self.smoothing_alpha * current_equity
                # Key rule from Astra Section 3: B_d = min(E_d, raw_smooth)
                self.smoothed_base = min(current_equity, raw_smooth)
            base_e = self.smoothed_base

        # 2. Drawdown derisking with hysteresis
        drawdown = 0.0
        if self.hwm_neutral > 0:
            drawdown = max(0.0, 1.0 - current_equity / self.hwm_neutral)

        mult_dd = 1.0
        if self.use_dd_derisking:
            if not self.is_derisked and drawdown >= self.dd_trigger:
                self.is_derisked = True
            elif self.is_derisked and drawdown <= self.dd_recovery:
                self.is_derisked = False

            if self.is_derisked:
                mult_dd = self.dd_multiplier

        # 3. Volatility scaling
        mult_vol = 1.0
        if self.use_vol_scaling and len(self.daily_returns) >= 3:
            recent_ret = self.daily_returns[-self.vol_window :]
            sigma_hat = float(np.std(recent_ret)) if len(recent_ret) > 1 else 0.0
            sigma_floor = 0.005
            sigma_eff = max(sigma_hat, sigma_floor)
            # Clip between 0.25 and 2.0
            mult_vol = max(0.25, min(2.0, self.target_vol / sigma_eff))

        # Effective notional
        if current_equity <= 0:
            target_notional = 0.0
            notes = ["Equity non-positive: new order notional set to 0.0"]
        else:
            raw_notional = self.fraction_f * base_e * mult_dd * mult_vol
            target_notional = max(self.min_order_notional, round(raw_notional, 2))
            notes = [
                f"Risk-adaptive day-cut {date_str}: E_d={current_equity:.2f}U, "
                f"B_d={base_e:.2f}U, DD={drawdown:.1%} "
                f"-> N_d={target_notional:.2f}U"
            ]

        self.current_state = DailySizingState(
            date_str=date_str,
            sizing_cutoff=current_time,
            equity_observed=current_equity,
            target_notional=target_notional,
            sizing_version=self.current_state.sizing_version + 1,
            notes=notes,
        )
        return self.current_state

    def get_order_notional(
        self,
        symbol: str,
        current_equity: float,
        current_time: datetime,
    ) -> float:
        return self.current_state.target_notional
