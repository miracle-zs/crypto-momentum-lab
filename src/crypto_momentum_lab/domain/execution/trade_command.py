"""Domain models and pure services for trade commands and exit allocation planning."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from crypto_momentum_lab.domain.execution.order_state import (
    ExitAllocation,
    deterministic_client_order_id,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionKey,
    PositionLedgerBatch,
    PositionView,
)
from crypto_momentum_lab.domain.trading import (
    OrderType as EntryType,
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
class PositionReservation:
    """
    Transactional reservation on a specific position lot to prevent concurrent
    over-exit.
    """

    reservation_id: str
    command_id: str
    position_key: PositionKey
    batch_id: str
    reserved_quantity: Decimal
    consumed_quantity: Decimal = Decimal("0")
    released_quantity: Decimal = Decimal("0")
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if not self.reservation_id.strip():
            raise ValueError("reservation_id must not be empty")
        if not self.command_id.strip():
            raise ValueError("command_id must not be empty")
        if not self.batch_id.strip():
            raise ValueError("batch_id must not be empty")
        if self.reserved_quantity <= 0:
            raise ValueError("reserved_quantity must be positive")
        if self.consumed_quantity < 0:
            raise ValueError("consumed_quantity must be non-negative")
        if self.released_quantity < 0:
            raise ValueError("released_quantity must be non-negative")
        if self.consumed_quantity + self.released_quantity > self.reserved_quantity:
            raise ValueError(
                "consumed + released quantity cannot exceed reserved quantity"
            )

    @property
    def active_quantity(self) -> Decimal:
        return self.reserved_quantity - self.consumed_quantity - self.released_quantity

    def release(self, quantity: Decimal) -> PositionReservation:
        if quantity <= Decimal("0"):
            raise ValueError("quantity to release must be positive")
        if quantity > self.active_quantity:
            raise ValueError(
                f"cannot release {quantity} exceeding active quantity "
                f"{self.active_quantity}"
            )
        return replace(self, released_quantity=self.released_quantity + quantity)

    def consume(self, quantity: Decimal) -> PositionReservation:
        if quantity <= Decimal("0"):
            raise ValueError("quantity to consume must be positive")
        if quantity > self.active_quantity:
            raise ValueError(
                f"cannot consume {quantity} exceeding active quantity "
                f"{self.active_quantity}"
            )
        return replace(self, consumed_quantity=self.consumed_quantity + quantity)


@dataclass(frozen=True, slots=True)
class TradeCommand:
    """Authoritative command submitted to execution coordinator.

    Invariants:
    - requested_quantity must be positive;
    - If allocation_plan is attached, requested_quantity must strictly equal
      allocation_plan.total_allocated_quantity;
    - Execution layer must never alter or inflate requested_quantity.
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

    def client_order_id(self, run_id: str) -> str:
        """Use the same exchange identity for submission and receipt recovery."""
        return self.idempotency_key or deterministic_client_order_id(
            run_id, self.command_id
        )

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


def plan_exit_allocations(
    projection: PositionView,
    *,
    target_batch_ids: tuple[str, ...] | None = None,
    requested_quantity: Decimal | None = None,
    policy: ExitPolicyMode = ExitPolicyMode.TARGET_BATCHES_ONLY,
    active_reservations: tuple[PositionReservation, ...] = (),
    reason: str = "",
) -> ExitAllocationPlan:
    """
    Plan exit allocations respecting explicit policies, active reservations,
    and lot boundaries.
    """
    open_batches = projection.batches
    total_active_quantity = projection.total_quantity
    pos_key = projection.key
    proj_ver = projection.projection_version

    def get_batch_available(batch: PositionLedgerBatch) -> Decimal:
        reserved = sum(
            (
                r.active_quantity
                for r in active_reservations
                if r.batch_id == batch.batch_id
            ),
            start=Decimal("0"),
        )
        batch_qty = batch.quantity
        return max(Decimal("0"), batch_qty - reserved)

    if target_batch_ids is not None:
        target_set = set(target_batch_ids)
        candidate_batches = tuple(b for b in open_batches if b.batch_id in target_set)
    else:
        candidate_batches = open_batches

    batch_caps = {b.batch_id: get_batch_available(b) for b in candidate_batches}

    if not candidate_batches or total_active_quantity <= 0:
        return ExitAllocationPlan(
            position_key=pos_key,
            allocations=(),
            total_allocated_quantity=Decimal("0"),
            policy=policy,
            reason=reason,
            projection_version=proj_ver,
            batch_quantities=batch_caps,
        )

    remaining_to_allocate = (
        sum(batch_caps.values(), start=Decimal("0"))
        if policy == ExitPolicyMode.FULL_POSITION_CLOSE or requested_quantity is None
        else requested_quantity
    )
    allocations_list: list[ExitAllocation] = []
    for batch in candidate_batches:
        if remaining_to_allocate <= 0:
            break
        avail = batch_caps[batch.batch_id]
        if avail <= Decimal("0"):
            continue
        allocated = min(avail, remaining_to_allocate)
        allocations_list.append(
            ExitAllocation(
                batch_id=batch.batch_id,
                allocated_quantity=allocated,
                entry_price=batch.entry_price,
            )
        )
        remaining_to_allocate -= allocated

    unallocated = max(Decimal("0"), remaining_to_allocate)
    total = sum((a.allocated_quantity for a in allocations_list), start=Decimal("0"))
    return ExitAllocationPlan(
        position_key=pos_key,
        allocations=tuple(allocations_list),
        total_allocated_quantity=total,
        policy=policy,
        unallocated_remainder=unallocated,
        reason=reason,
        projection_version=proj_ver,
        batch_quantities=batch_caps,
    )


__all__ = [
    "ExitAllocation",
    "ExitAllocationPlan",
    "plan_exit_allocations",
    "ExitPolicyMode",
    "PositionReservation",
    "TradeCommand",
    "TradeCommandType",
]
