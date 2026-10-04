from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Final, Protocol

from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.strategy import StrategySide


@dataclass(frozen=True, slots=True)
class ManagedLivePositionBatch:
    """One live position batch separated by a reduce-only order boundary."""

    batch_id: str
    quantity: Decimal
    entry_price: Decimal
    opened_at: datetime
    exit_order_submitted_at: datetime | None = None
    recovery_order_client_id: str | None = None
    recovery_order_plan: OrderExecutionPlan | None = None
    recovery_order_remaining_quantity: Decimal | None = None
    closing_order_filled: bool = False
    entry_order_count: int = 1
    entry_client_order_ids: frozenset[str] = frozenset()
    projection_version: str | None = None

    def __post_init__(self) -> None:
        if not self.batch_id.strip():
            raise ValueError("batch_id must not be empty")
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        if self.entry_price <= 0:
            raise ValueError("entry_price must be positive")
        if self.entry_order_count <= 0:
            raise ValueError("entry_order_count must be positive")
        if self.opened_at.tzinfo is None or self.opened_at.utcoffset() is None:
            raise ValueError("opened_at must be timezone-aware")
        if self.exit_order_submitted_at is not None and (
            self.exit_order_submitted_at.tzinfo is None
            or self.exit_order_submitted_at.utcoffset() is None
        ):
            raise ValueError("exit_order_submitted_at must be timezone-aware")
        if (
            self.recovery_order_remaining_quantity is not None
            and self.recovery_order_remaining_quantity < 0
        ):
            raise ValueError("recovery_order_remaining_quantity must be non-negative")


@dataclass(frozen=True, slots=True)
class PositionObservation:
    symbol: str
    side: StrategySide
    position_side: FuturesPositionSide
    position_amt: Decimal
    entry_price: Decimal
    observed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class PositionOrderFact:
    symbol: str
    position_side: FuturesPositionSide
    side: str
    reduce_only: bool
    order_type: str
    quantity: Decimal
    executed_quantity: Decimal
    state: ExchangeOrderState
    client_order_id: str | None
    exchange_order_id: str | None
    created_at: datetime
    updated_at: datetime
    price: Decimal | None
    plan: OrderExecutionPlan | None = None
    exit_batch_id: str | None = None


_EXIT_SUBMITTED_STATES: Final[frozenset[ExchangeOrderState]] = frozenset(
    {
        ExchangeOrderState.SUBMITTING,
        ExchangeOrderState.CANCELING,
        ExchangeOrderState.SUBMITTED,
        ExchangeOrderState.ACKNOWLEDGED,
        ExchangeOrderState.PARTIALLY_FILLED,
        ExchangeOrderState.FILLED,
        ExchangeOrderState.CANCELED,
        ExchangeOrderState.ABSENT_RECONCILED,
        ExchangeOrderState.EXPIRED,
        ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
    }
)


class BatchConcurrencyPosition(Protocol):
    @property
    def symbol(self) -> str: ...
    @property
    def batches(self) -> tuple[ManagedLivePositionBatch, ...]: ...
    @property
    def recovery_exit_started_at(self) -> datetime | None: ...
    @property
    def closing_order_filled(self) -> bool: ...


def count_active_symbol_batch_concurrency(
    symbol: str,
    managed_positions: Sequence[BatchConcurrencyPosition] = (),
    pending_entry_plans: Sequence[OrderExecutionPlan] = (),
) -> int:
    """Calculate the active batch entry concurrency for a symbol.

    Requirements:
    - Same symbol, same batch: at most max_concurrency_per_symbol entry orders.
    - If a batch has submitted an exit order (exit_order_submitted_at is not None,
      even if not filled yet), that batch is ended and does NOT count towards the
      active entry batch.
    - If there is an active batch (exit_order_submitted_at is None
      and not closing_order_filled), its entry_order_count represents
      how many entry orders were merged into this batch.
    - Unresolved (pending) non-reduce-only entry orders for this symbol add to the
      concurrency count (excluding any client_order_id already recorded in the batch).
    - Different batches and different symbols are independent.
    """
    active_batch_orders = 0
    known_entry_order_ids: set[str] = set()

    for p in managed_positions:
        if p.symbol != symbol:
            continue
        batches = p.batches
        if batches:
            for b in batches:
                has_exit_submitted = b.exit_order_submitted_at is not None
                is_closing_filled = b.closing_order_filled
                if not has_exit_submitted and not is_closing_filled:
                    active_batch_orders += b.entry_order_count
                    entry_ids = b.entry_client_order_ids
                    known_entry_order_ids.update(entry_ids)
        else:
            has_exit_started = p.recovery_exit_started_at is not None
            is_closing_filled = p.closing_order_filled
            if not has_exit_started and not is_closing_filled:
                active_batch_orders += 1

    pending_order_count = 0
    for o in pending_entry_plans:
        if o.symbol != symbol:
            continue
        if o.reduce_only:
            continue
        client_order_id = o.client_order_id
        if client_order_id and client_order_id in known_entry_order_ids:
            continue
        pending_order_count += 1

    return active_batch_orders + pending_order_count
