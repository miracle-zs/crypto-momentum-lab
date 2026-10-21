from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum


class FuturesPositionSide(StrEnum):
    BOTH = "BOTH"
    LONG = "LONG"
    SHORT = "SHORT"


@dataclass(frozen=True, slots=True)
class ExitAllocation:
    """Explicit allocation of an exit order to a specific lot/batch."""

    batch_id: str
    allocated_quantity: Decimal
    entry_price: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        if not self.batch_id.strip():
            raise ValueError("batch_id must not be empty")
        if self.allocated_quantity <= 0:
            raise ValueError("allocated_quantity must be positive")
        if self.entry_price < 0:
            raise ValueError("entry_price must be non-negative")
