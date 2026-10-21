"""Decision command models and exit allocation plans."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from crypto_momentum_lab.domain.execution.order_state import ExitAllocation
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.trading import (
    OrderType as EntryType,
)
from crypto_momentum_lab.domain.trading import (
    TradeSide as StrategySide,
)


class ExitPolicyMode(StrEnum):
    """Exit allocation policy mode."""

    TARGET_BATCHES_ONLY = "target_batches_only"
    FULL_POSITION_CLOSE = "full_position_close"


class TradeCommandType(StrEnum):
    """Explicit intent category for a trade command."""

    ENTRY = "entry"
    EXIT = "exit"


@dataclass(frozen=True, slots=True)
class ExitAllocationPlan:
    """Authoritative exit plan establishing batch reductions."""

    position_key: PositionKey
    allocations: tuple[ExitAllocation, ...]
    total_allocated_quantity: Decimal
    policy: ExitPolicyMode
    unallocated_remainder: Decimal = Decimal("0")
    reason: str = ""
    projection_version: str | None = None
    reservation_id: str | None = None
    batch_quantities: Mapping[str, Decimal] | None = None

    def __post_init__(self) -> None:
        if self.total_allocated_quantity < 0:
            raise ValueError("total_allocated_quantity must be non-negative")
        if self.unallocated_remainder < 0:
            raise ValueError("unallocated_remainder must be non-negative")
        allocated_sum = sum(
            (a.allocated_quantity for a in self.allocations),
            start=Decimal("0"),
        )
        if allocated_sum != self.total_allocated_quantity:
            raise ValueError(
                f"total_allocated_quantity {self.total_allocated_quantity} does not"
                "match"
                f"sum of allocations {allocated_sum}"
            )


@dataclass(frozen=True, slots=True)
class TradeCommand:
    """Trade command emitted by a policy transition.

    Invariants:
    - requested_quantity must be positive;
    - If allocation_plan is attached, requested_quantity must strictly equal
      allocation_plan.total_allocated_quantity;
    - Consumers must use the requested quantity unchanged.
    """

    command_id: str
    position_key: PositionKey
    command_type: TradeCommandType
    side: StrategySide
    order_type: EntryType
    requested_quantity: Decimal
    limit_price: Decimal | None = None
    reduce_only: bool = False
    allocation_plan: ExitAllocationPlan | None = None
    reason: str = ""
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    fencing_token: str | None = None
    idempotency_key: str | None = None
    expected_projection_version: str | None = None
    reservation_id: str | None = None

    def __post_init__(self) -> None:
        if not self.command_id.strip():
            raise ValueError("command_id must not be empty")
        if self.requested_quantity <= 0:
            raise ValueError("requested_quantity must be positive")
        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")
        if self.allocation_plan is not None:
            if self.allocation_plan.position_key != self.position_key:
                raise ValueError(
                    "allocation_plan position_key does not match command position_key"
                )
            if self.allocation_plan.total_allocated_quantity != self.requested_quantity:
                raise ValueError(
                    f"requested_quantity {self.requested_quantity} must "
                    "strictly match allocation plan total "
                    f"{self.allocation_plan.total_allocated_quantity}"
                )

__all__ = [
    "ExitAllocation",
    "ExitAllocationPlan",
    "ExitPolicyMode",
    "TradeCommand",
    "TradeCommandType",
]
