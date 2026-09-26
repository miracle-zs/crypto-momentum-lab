"""Sizing, lot partitioning, and margin capacity contracts (Phase 4 / R3).

Obeys Astra Architecture Blueprint 2026-09-25:
- Replaces hardcoded notional estimates with explicit sizing models:
  compute_plan(symbol, price, cash, ...) -> SizingPlan | SizingRejection
- Quantization rules: downward step_size truncation, tick rounding.
- Fail-closed admission: rejects orders below min_notional, outside quantity bounds,
  or breaching max portfolio leverage / margin capacity.
- Pure domain models: Decimal arithmetic, UTC datetimes, immutable frozen slots.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class SymbolLotRules:
    """Trading limits and lot quantization rules for a symbol."""

    symbol: str
    tick_size: Decimal
    step_size: Decimal
    min_quantity: Decimal
    max_quantity: Decimal
    min_notional: Decimal

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValueError("symbol must not be empty")
        for field_name in (
            "tick_size",
            "step_size",
            "min_quantity",
            "max_quantity",
            "min_notional",
        ):
            val = getattr(self, field_name)
            if val <= Decimal("0"):
                raise ValueError(f"{field_name} must be positive, got {val}")
        if self.max_quantity < self.min_quantity:
            raise ValueError("max_quantity cannot be less than min_quantity")


@dataclass(frozen=True, slots=True)
class SizingRejection:
    """Explicit, fail-closed rejection when sizing conditions cannot be satisfied."""

    reason: str  # min_notional, min/max quantity, margin, resize_beyond_tolerance
    symbol: str
    details: dict[str, str] = field(default_factory=dict)
    rejected_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if not self.reason.strip():
            raise ValueError("reason must not be empty")
        if not self.symbol.strip():
            raise ValueError("symbol must not be empty")
        if self.rejected_at.tzinfo is None:
            raise ValueError("rejected_at must be timezone-aware (UTC)")


@dataclass(frozen=True, slots=True)
class SizingPlan:
    """Authoritative sizing and lot partitioning plan for order admission."""

    symbol: str
    target_notional: Decimal
    raw_quantity: Decimal
    quantized_quantity: Decimal
    actual_notional: Decimal
    step_size: Decimal
    tick_size: Decimal
    lot_remainder: Decimal
    min_notional: Decimal
    min_quantity: Decimal
    max_quantity: Decimal
    max_leverage: Decimal = Decimal("5.0")
    max_slippage_budget_bps: Decimal = Decimal("10.0")
    margin_required: Decimal = Decimal("0")
    sizing_version: int = 1
    sizing_timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    features: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValueError("symbol must not be empty")
        if self.target_notional <= Decimal("0"):
            raise ValueError("target_notional must be positive")
        if self.quantized_quantity <= Decimal("0"):
            raise ValueError("quantized_quantity must be positive")
        if self.actual_notional <= Decimal("0"):
            raise ValueError("actual_notional must be positive")
        if self.step_size <= Decimal("0"):
            raise ValueError("step_size must be positive")
        if self.tick_size <= Decimal("0"):
            raise ValueError("tick_size must be positive")
        if self.sizing_timestamp.tzinfo is None:
            raise ValueError("sizing_timestamp must be timezone-aware (UTC)")
        # Invariant checks
        if self.quantized_quantity < self.min_quantity:
            raise ValueError(
                f"quantized_quantity {self.quantized_quantity} < "
                f"min_quantity {self.min_quantity}"
            )
        if self.quantized_quantity > self.max_quantity:
            raise ValueError(
                f"quantized_quantity {self.quantized_quantity} > "
                f"max_quantity {self.max_quantity}"
            )
        if self.actual_notional < self.min_notional:
            raise ValueError(
                f"actual_notional {self.actual_notional} < "
                f"min_notional {self.min_notional}"
            )
        if self.lot_remainder < Decimal("0"):
            raise ValueError("lot_remainder must be non-negative")


def quantize_lot_quantity(
    raw_quantity: Decimal,
    step_size: Decimal,
) -> tuple[Decimal, Decimal]:
    """Pure downward quantization according to exchange lot step_size rules.

    Returns:
        (quantized_quantity, lot_remainder)
    """
    if step_size <= Decimal("0"):
        raise ValueError("step_size must be positive")
    if raw_quantity <= Decimal("0"):
        return Decimal("0"), Decimal("0")

    units = (raw_quantity / step_size).to_integral_value(rounding=ROUND_DOWN)
    quantized = units * step_size
    remainder = max(Decimal("0"), raw_quantity - quantized)
    return quantized, remainder


class SizingModel(Protocol):
    """Protocol governing pure position sizing computation."""

    def compute_plan(
        self,
        symbol: str,
        reference_price: Decimal,
        cash_balance: Decimal,
        lot_rules: SymbolLotRules,
        *,
        as_of: datetime,
        current_margin_locked: Decimal = Decimal("0"),
        sizing_version: int = 1,
    ) -> SizingPlan | SizingRejection: ...


@dataclass(frozen=True, slots=True)
class FixedNotionalSizingModel:
    """Fixed notional sizing baseline with lot quantization and margin check."""

    target_notional: Decimal = Decimal("500.00")
    max_leverage: Decimal = Decimal("5.0")
    max_slippage_budget_bps: Decimal = Decimal("10.0")
    resize_tolerance: Decimal = Decimal("0.05")

    def __post_init__(self) -> None:
        if self.target_notional <= Decimal("0"):
            raise ValueError("target_notional must be positive")
        if self.max_leverage <= Decimal("0"):
            raise ValueError("max_leverage must be positive")
        if self.resize_tolerance < Decimal("0") or self.resize_tolerance >= Decimal(
            "1"
        ):
            raise ValueError("resize_tolerance must be in [0, 1)")

    def compute_plan(
        self,
        symbol: str,
        reference_price: Decimal,
        cash_balance: Decimal,
        lot_rules: SymbolLotRules,
        *,
        as_of: datetime,
        current_margin_locked: Decimal = Decimal("0"),
        sizing_version: int = 1,
    ) -> SizingPlan | SizingRejection:
        if reference_price <= Decimal("0"):
            return SizingRejection(
                reason="invalid_reference_price",
                symbol=symbol,
                details={"reference_price": str(reference_price)},
                rejected_at=as_of,
            )

        raw_qty = self.target_notional / reference_price
        quantized_qty, remainder = quantize_lot_quantity(raw_qty, lot_rules.step_size)
        actual_notional = quantized_qty * reference_price

        if quantized_qty < lot_rules.min_quantity:
            return SizingRejection(
                reason="below_min_quantity",
                symbol=symbol,
                details={
                    "quantized_quantity": str(quantized_qty),
                    "min_quantity": str(lot_rules.min_quantity),
                },
                rejected_at=as_of,
            )

        if quantized_qty > lot_rules.max_quantity:
            return SizingRejection(
                reason="above_max_quantity",
                symbol=symbol,
                details={
                    "quantized_quantity": str(quantized_qty),
                    "max_quantity": str(lot_rules.max_quantity),
                },
                rejected_at=as_of,
            )

        if actual_notional < lot_rules.min_notional:
            return SizingRejection(
                reason="below_min_notional",
                symbol=symbol,
                details={
                    "actual_notional": str(actual_notional),
                    "min_notional": str(lot_rules.min_notional),
                },
                rejected_at=as_of,
            )

        # Margin check: notional / leverage <= available balance
        margin_required = actual_notional / self.max_leverage
        available_cash = max(Decimal("0"), cash_balance - current_margin_locked)
        if margin_required > available_cash:
            return SizingRejection(
                reason="insufficient_margin",
                symbol=symbol,
                details={
                    "margin_required": str(margin_required),
                    "available_cash": str(available_cash),
                    "cash_balance": str(cash_balance),
                    "current_margin_locked": str(current_margin_locked),
                },
                rejected_at=as_of,
            )

        # Resize tolerance check
        fraction = (
            self.target_notional - actual_notional
        ).copy_abs() / self.target_notional
        if fraction > self.resize_tolerance:
            return SizingRejection(
                reason="resize_beyond_tolerance",
                symbol=symbol,
                details={
                    "target_notional": str(self.target_notional),
                    "actual_notional": str(actual_notional),
                    "resize_fraction": str(fraction),
                    "tolerance": str(self.resize_tolerance),
                },
                rejected_at=as_of,
            )

        return SizingPlan(
            symbol=symbol,
            target_notional=self.target_notional,
            raw_quantity=raw_qty,
            quantized_quantity=quantized_qty,
            actual_notional=actual_notional,
            step_size=lot_rules.step_size,
            tick_size=lot_rules.tick_size,
            lot_remainder=remainder,
            min_notional=lot_rules.min_notional,
            min_quantity=lot_rules.min_quantity,
            max_quantity=lot_rules.max_quantity,
            max_leverage=self.max_leverage,
            max_slippage_budget_bps=self.max_slippage_budget_bps,
            margin_required=margin_required,
            sizing_version=sizing_version,
            sizing_timestamp=as_of,
            features={
                "model": "fixed_notional",
                "resize_fraction": str(fraction),
            },
        )


@dataclass(frozen=True, slots=True)
class EquityFractionSizingModel:
    """Dynamic compounding sizing based on available account equity fraction."""

    fraction_of_equity: Decimal = Decimal("0.05")
    min_notional_floor: Decimal = Decimal("10.00")
    max_notional_cap: Decimal = Decimal("5000.00")
    max_leverage: Decimal = Decimal("5.0")
    max_slippage_budget_bps: Decimal = Decimal("10.0")
    resize_tolerance: Decimal = Decimal("0.10")

    def __post_init__(self) -> None:
        if self.fraction_of_equity <= Decimal("0") or self.fraction_of_equity > Decimal(
            "1.0"
        ):
            raise ValueError("fraction_of_equity must be in (0, 1.0]")
        if self.min_notional_floor <= Decimal("0"):
            raise ValueError("min_notional_floor must be positive")
        if self.max_notional_cap < self.min_notional_floor:
            raise ValueError("max_notional_cap must be >= min_notional_floor")

    def compute_plan(
        self,
        symbol: str,
        reference_price: Decimal,
        cash_balance: Decimal,
        lot_rules: SymbolLotRules,
        *,
        as_of: datetime,
        current_margin_locked: Decimal = Decimal("0"),
        sizing_version: int = 1,
    ) -> SizingPlan | SizingRejection:
        if reference_price <= Decimal("0"):
            return SizingRejection(
                reason="invalid_reference_price",
                symbol=symbol,
                details={"reference_price": str(reference_price)},
                rejected_at=as_of,
            )

        available_cash = max(Decimal("0"), cash_balance - current_margin_locked)
        nominal_target = available_cash * self.fraction_of_equity * self.max_leverage
        target_notional = min(
            max(nominal_target, self.min_notional_floor), self.max_notional_cap
        )

        raw_qty = target_notional / reference_price
        quantized_qty, remainder = quantize_lot_quantity(raw_qty, lot_rules.step_size)
        actual_notional = quantized_qty * reference_price

        if quantized_qty < lot_rules.min_quantity:
            return SizingRejection(
                reason="below_min_quantity",
                symbol=symbol,
                details={
                    "quantized_quantity": str(quantized_qty),
                    "min_quantity": str(lot_rules.min_quantity),
                },
                rejected_at=as_of,
            )

        if quantized_qty > lot_rules.max_quantity:
            return SizingRejection(
                reason="above_max_quantity",
                symbol=symbol,
                details={
                    "quantized_quantity": str(quantized_qty),
                    "max_quantity": str(lot_rules.max_quantity),
                },
                rejected_at=as_of,
            )

        if actual_notional < lot_rules.min_notional:
            return SizingRejection(
                reason="below_min_notional",
                symbol=symbol,
                details={
                    "actual_notional": str(actual_notional),
                    "min_notional": str(lot_rules.min_notional),
                },
                rejected_at=as_of,
            )

        margin_required = actual_notional / self.max_leverage
        if margin_required > available_cash:
            return SizingRejection(
                reason="insufficient_margin",
                symbol=symbol,
                details={
                    "margin_required": str(margin_required),
                    "available_cash": str(available_cash),
                },
                rejected_at=as_of,
            )

        fraction = (target_notional - actual_notional).copy_abs() / target_notional
        if fraction > self.resize_tolerance:
            return SizingRejection(
                reason="resize_beyond_tolerance",
                symbol=symbol,
                details={
                    "target_notional": str(target_notional),
                    "actual_notional": str(actual_notional),
                    "resize_fraction": str(fraction),
                    "tolerance": str(self.resize_tolerance),
                },
                rejected_at=as_of,
            )

        return SizingPlan(
            symbol=symbol,
            target_notional=target_notional,
            raw_quantity=raw_qty,
            quantized_quantity=quantized_qty,
            actual_notional=actual_notional,
            step_size=lot_rules.step_size,
            tick_size=lot_rules.tick_size,
            lot_remainder=remainder,
            min_notional=lot_rules.min_notional,
            min_quantity=lot_rules.min_quantity,
            max_quantity=lot_rules.max_quantity,
            max_leverage=self.max_leverage,
            max_slippage_budget_bps=self.max_slippage_budget_bps,
            margin_required=margin_required,
            sizing_version=sizing_version,
            sizing_timestamp=as_of,
            features={
                "model": "equity_fraction",
                "fraction_of_equity": str(self.fraction_of_equity),
                "resize_fraction": str(fraction),
            },
        )


def default_symbol_lot_rules(symbol: str) -> SymbolLotRules:
    """Default conservative Binance USD-M Futures lot rules."""
    sym = symbol.upper()
    if sym.startswith("BTC"):
        return SymbolLotRules(
            symbol=sym,
            tick_size=Decimal("0.10"),
            step_size=Decimal("0.001"),
            min_quantity=Decimal("0.001"),
            max_quantity=Decimal("1000.00"),
            min_notional=Decimal("5.00"),
        )
    if sym.startswith("ETH"):
        return SymbolLotRules(
            symbol=sym,
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.01"),
            min_quantity=Decimal("0.01"),
            max_quantity=Decimal("10000.00"),
            min_notional=Decimal("5.00"),
        )
    if sym.startswith("SOL"):
        return SymbolLotRules(
            symbol=sym,
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.1"),
            min_quantity=Decimal("0.1"),
            max_quantity=Decimal("50000.00"),
            min_notional=Decimal("5.00"),
        )
    return SymbolLotRules(
        symbol=sym,
        tick_size=Decimal("0.0001"),
        step_size=Decimal("1.0"),
        min_quantity=Decimal("1.0"),
        max_quantity=Decimal("1000000.00"),
        min_notional=Decimal("5.00"),
    )
