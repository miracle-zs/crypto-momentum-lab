"""Exchange trading constraints used to validate and quantize an order."""

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class SymbolTradingRules:
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
            if getattr(self, field_name) <= 0:
                raise ValueError(f"{field_name} must be positive")
        if self.max_quantity < self.min_quantity:
            raise ValueError("max_quantity must not be below min_quantity")
