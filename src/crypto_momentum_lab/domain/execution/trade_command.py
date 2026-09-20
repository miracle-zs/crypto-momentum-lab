"""Domain models and pure services for trade commands and exit allocation planning."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from uuid import uuid4

from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionKey,
    PositionLedgerProjection,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide


class ExitPolicyMode(str, Enum):
    """Exit allocation policy mode."""

    TARGET_BATCHES_ONLY = "target_batches_only"
    CONSOLIDATE_ELIGIBLE = "consolidate_eligible"
    FULL_POSITION_CLOSE = "full_position_close"
    ABSORB_DUST_SINGLE_BATCH = "absorb_dust_single_batch"


class TradeCommandType(str, Enum):
    """Explicit intent category for a trade command."""

    ENTRY = "entry"
    EXIT = "exit"
    EMERGENCY_FLATTEN = "emergency_flatten"


@dataclass(frozen=True, slots=True)
class ExitAllocation:
    """Explicit allocation of an exit order to a specific lot/batch."""

    batch_id: str
    allocated_quantity: Decimal
    entry_price: Decimal

    def __post_init__(self) -> None:
        if not self.batch_id.strip():
            raise ValueError("batch_id must not be empty")
        if self.allocated_quantity <= 0:
            raise ValueError("allocated_quantity must be positive")
        if self.entry_price < 0:
            raise ValueError("entry_price must be non-negative")


@dataclass(frozen=True, slots=True)
class ExitAllocationPlan:
    """Authoritative exit plan establishing batch reductions and remaining dust."""

    position_key: PositionKey
    allocations: tuple[ExitAllocation, ...]
    total_allocated_quantity: Decimal
    policy: ExitPolicyMode
    absorbed_dust: Decimal = Decimal("0")
    unallocated_remainder: Decimal = Decimal("0")
    reason: str = ""

    def __post_init__(self) -> None:
        if self.total_allocated_quantity < 0:
            raise ValueError("total_allocated_quantity must be non-negative")
        if self.absorbed_dust < 0:
            raise ValueError("absorbed_dust must be non-negative")
        if self.unallocated_remainder < 0:
            raise ValueError("unallocated_remainder must be non-negative")
        allocated_sum = sum(
            (a.allocated_quantity for a in self.allocations),
            start=Decimal("0"),
        )
        if allocated_sum != self.total_allocated_quantity:
            raise ValueError(
                f"total_allocated_quantity {self.total_allocated_quantity} does not match "
                f"sum of allocations {allocated_sum}"
            )


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
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    fencing_token: str | None = None
    idempotency_key: str | None = None

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
                    f"requested_quantity {self.requested_quantity} must strictly match "
                    f"allocation plan total {self.allocation_plan.total_allocated_quantity}"
                )


class ExitAllocator:
    """Authoritative pure domain service for planning position exits and lot allocations."""

    @classmethod
    def plan_exit(
        cls,
        projection: PositionLedgerProjection,
        *,
        target_batch_ids: tuple[str, ...] | None = None,
        requested_quantity: Decimal | None = None,
        policy: ExitPolicyMode = ExitPolicyMode.TARGET_BATCHES_ONLY,
        reference_price: Decimal | None = None,
        min_notional: Decimal | None = None,
        reason: str = "",
    ) -> ExitAllocationPlan:
        """Plan exit allocations respecting explicit policies and lot boundaries."""
        open_batches = projection.active_batches
        if target_batch_ids is not None:
            target_set = set(target_batch_ids)
            candidate_batches = tuple(
                b for b in open_batches if b.batch_id in target_set
            )
        else:
            candidate_batches = open_batches

        if not candidate_batches or projection.total_active_quantity <= 0:
            return ExitAllocationPlan(
                position_key=projection.position_key,
                allocations=(),
                total_allocated_quantity=Decimal("0"),
                policy=policy,
                reason=reason,
            )

        if policy == ExitPolicyMode.FULL_POSITION_CLOSE:
            allocations = tuple(
                ExitAllocation(
                    batch_id=b.batch_id,
                    allocated_quantity=b.quantity,
                    entry_price=b.entry_price,
                )
                for b in candidate_batches
            )
            total = sum((a.allocated_quantity for a in allocations), start=Decimal("0"))
            return ExitAllocationPlan(
                position_key=projection.position_key,
                allocations=allocations,
                total_allocated_quantity=total,
                policy=policy,
                reason=reason,
            )

        if policy == ExitPolicyMode.ABSORB_DUST_SINGLE_BATCH:
            # Only apply dust absorption if there is strictly one single active batch across the entire position!
            if len(open_batches) == 1:
                batch = open_batches[0]
                req = (
                    requested_quantity
                    if requested_quantity is not None
                    else batch.quantity
                )
                if req < batch.quantity:
                    dust_remainder = batch.quantity - req
                    if (
                        reference_price is not None
                        and min_notional is not None
                        and (dust_remainder * reference_price) < min_notional
                    ):
                        # Explicitly absorb dust into this exit allocation plan at decision time
                        allocations = (
                            ExitAllocation(
                                batch_id=batch.batch_id,
                                allocated_quantity=batch.quantity,
                                entry_price=batch.entry_price,
                            ),
                        )
                        return ExitAllocationPlan(
                            position_key=projection.position_key,
                            allocations=allocations,
                            total_allocated_quantity=batch.quantity,
                            policy=policy,
                            absorbed_dust=dust_remainder,
                            reason=f"{reason} (absorbed_dust={dust_remainder})".strip(),
                        )

        # Standard FIFO allocation across candidate batches
        if requested_quantity is None:
            allocations = tuple(
                ExitAllocation(
                    batch_id=b.batch_id,
                    allocated_quantity=b.quantity,
                    entry_price=b.entry_price,
                )
                for b in candidate_batches
            )
            total = sum((a.allocated_quantity for a in allocations), start=Decimal("0"))
            return ExitAllocationPlan(
                position_key=projection.position_key,
                allocations=allocations,
                total_allocated_quantity=total,
                policy=policy,
                reason=reason,
            )

        remaining_to_allocate = requested_quantity
        allocations_list: list[ExitAllocation] = []
        for batch in candidate_batches:
            if remaining_to_allocate <= 0:
                break
            allocated = min(batch.quantity, remaining_to_allocate)
            if allocated > 0:
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
            position_key=projection.position_key,
            allocations=tuple(allocations_list),
            total_allocated_quantity=total,
            policy=policy,
            unallocated_remainder=unallocated,
            reason=reason,
        )

    @classmethod
    def create_exit_command(
        cls,
        projection: PositionLedgerProjection,
        *,
        command_id: str | None = None,
        target_batch_ids: tuple[str, ...] | None = None,
        requested_quantity: Decimal | None = None,
        policy: ExitPolicyMode = ExitPolicyMode.TARGET_BATCHES_ONLY,
        order_type: EntryType = EntryType.MARKET,
        limit_price: Decimal | None = None,
        reference_price: Decimal | None = None,
        min_notional: Decimal | None = None,
        reason: str = "",
        fencing_token: str | None = None,
        idempotency_key: str | None = None,
    ) -> TradeCommand | None:
        """Generate an authorized exit command with an explicit allocation plan."""
        plan = cls.plan_exit(
            projection,
            target_batch_ids=target_batch_ids,
            requested_quantity=requested_quantity,
            policy=policy,
            reference_price=reference_price,
            min_notional=min_notional,
            reason=reason,
        )
        if plan.total_allocated_quantity <= 0:
            return None

        if projection.active_episode is not None:
            side = projection.active_episode.side
        elif projection.position_key.position_side == FuturesPositionSide.SHORT:
            side = StrategySide.SHORT
        else:
            side = StrategySide.LONG

        return TradeCommand(
            command_id=command_id or f"cmd-exit-{uuid4().hex[:12]}",
            position_key=projection.position_key,
            command_type=TradeCommandType.EXIT,
            side=side,
            order_type=order_type,
            requested_quantity=plan.total_allocated_quantity,
            limit_price=limit_price,
            reduce_only=True,
            allocation_plan=plan,
            reason=reason,
            fencing_token=fencing_token,
            idempotency_key=idempotency_key,
        )


__all__ = [
    "ExitAllocation",
    "ExitAllocationPlan",
    "ExitAllocator",
    "ExitPolicyMode",
    "TradeCommand",
    "TradeCommandType",
]
