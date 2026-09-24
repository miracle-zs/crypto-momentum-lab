from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime
from decimal import Decimal
from typing import Final

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
    legacy_attribution: bool = False
    entry_order_count: int = 1
    entry_client_order_ids: frozenset[str] = frozenset()

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
    legacy_exit_attribution: bool = False


@dataclass(frozen=True, slots=True)
class PositionHistory:
    orders: Sequence[PositionOrderFact]
    fill_times: Mapping[str, datetime] = field(default_factory=dict)
    fill_prices: Mapping[str, Decimal] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RebuildDiagnostic:
    kind: str
    symbol: str
    client_order_id: str | None
    bound_batch_id: str | None
    target_batch_id: str | None
    filled_quantity: Decimal
    reassigned_quantity: Decimal


@dataclass(frozen=True, slots=True)
class PositionRebuildResult:
    batches: tuple[ManagedLivePositionBatch, ...]
    diagnostics: tuple[RebuildDiagnostic, ...] = ()


@dataclass(slots=True)
class _PositionBatchAccumulator:
    batch_id: str
    opened_at: datetime
    entry_quantity: Decimal
    entry_notional: Decimal
    entry_order_count: int = 1
    entry_client_order_ids: set[str] = field(default_factory=set)
    entry_orders: list[PositionOrderFact] = field(default_factory=list)
    exit_order_submitted_at: datetime | None = None
    exit_orders: list[PositionOrderFact] = field(default_factory=list)
    exit_filled_quantity: Decimal = Decimal("0")
    legacy_exit_attribution: bool = False


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


def rebuild_position_batches(
    observation: PositionObservation,
    history: PositionHistory,
) -> PositionRebuildResult:
    """Pure domain calculation reconstructing position batches from immutable facts."""
    events: list[tuple[datetime, int, int, str, PositionOrderFact]] = []
    for index, order in enumerate(history.orders):
        if not order.reduce_only and _opening_order_matches_side(
            order.side, observation.side
        ):
            if _is_entry_fill_observed(order, history.fill_times):
                events.append(
                    (
                        _order_entry_time(order, history.fill_times),
                        0,
                        index,
                        "entry",
                        order,
                    )
                )
        elif (
            order.reduce_only
            and not _opening_order_matches_side(order.side, observation.side)
            and order.state in _EXIT_SUBMITTED_STATES
        ):
            events.append(
                (order.created_at, 1, index, "exit", order)
            )

    events.sort(key=lambda event: event[:3])
    accumulators: list[_PositionBatchAccumulator] = []
    current: _PositionBatchAccumulator | None = None
    diagnostics: list[RebuildDiagnostic] = []

    for event_at, _event_priority, _index, event_kind, order in events:
        if event_kind == "entry":
            entry_quantity = _entry_fill_quantity(order, history.fill_times)
            if entry_quantity <= 0:
                continue
            entry_price = _entry_price(
                order, history.fill_prices, observation.entry_price
            )
            if current is None or current.exit_order_submitted_at is not None:
                current = _PositionBatchAccumulator(
                    batch_id=_batch_id_for_entry(order),
                    opened_at=event_at,
                    entry_quantity=entry_quantity,
                    entry_notional=entry_quantity * entry_price,
                    entry_order_count=1,
                    entry_client_order_ids=(
                        {order.client_order_id}
                        if order.client_order_id
                        else set()
                    ),
                    entry_orders=[order],
                )
                accumulators.append(current)
            else:
                current.entry_quantity += entry_quantity
                current.entry_notional += entry_quantity * entry_price
                current.opened_at = max(current.opened_at, event_at)
                current.entry_order_count += 1
                if order.client_order_id:
                    current.entry_client_order_ids.add(order.client_order_id)
                current.entry_orders.append(order)
            continue

        target = current
        if order.exit_batch_id is not None:
            target = next(
                (
                    batch for batch in accumulators
                    if batch.batch_id == order.exit_batch_id
                ),
                None,
            )
        if target is not None and target.opened_at > order.created_at:
            target = None

        filled_quantity = _exit_fill_quantity(order)
        remaining_fill = filled_quantity

        def attach(
            batch: _PositionBatchAccumulator,
            quantity: Decimal,
            *,
            exit_order: PositionOrderFact = order,
            submitted_at: datetime = event_at,
            legacy_attribution: bool = order.legacy_exit_attribution,
        ) -> None:
            if legacy_attribution:
                batch.legacy_exit_attribution = True
            if batch.exit_order_submitted_at is None:
                batch.exit_order_submitted_at = submitted_at
            batch.exit_orders.append(exit_order)
            batch.exit_filled_quantity += quantity

        if target is not None:
            available = max(
                Decimal("0"),
                target.entry_quantity - target.exit_filled_quantity,
            )
            if filled_quantity <= 0:
                if available > 0:
                    attach(target, Decimal("0"))
                else:
                    target = None
            elif available > 0:
                allocated = min(available, remaining_fill)
                attach(target, allocated)
                remaining_fill -= allocated
            else:
                target = None

        if remaining_fill > 0:
            fallback_candidates = reversed(accumulators)
            for fallback in fallback_candidates:
                if fallback is target:
                    continue
                if fallback.opened_at > order.created_at:
                    continue
                available = max(
                    Decimal("0"),
                    fallback.entry_quantity - fallback.exit_filled_quantity,
                )
                if available <= 0:
                    continue
                allocated = min(available, remaining_fill)
                attach(fallback, allocated)
                if order.exit_batch_id is not None:
                    diagnostics.append(
                        RebuildDiagnostic(
                            kind="reassigned",
                            symbol=order.symbol,
                            client_order_id=order.client_order_id,
                            bound_batch_id=order.exit_batch_id,
                            target_batch_id=fallback.batch_id,
                            filled_quantity=filled_quantity,
                            reassigned_quantity=allocated,
                        )
                    )
                remaining_fill -= allocated
                if remaining_fill <= 0:
                    break

        total_open = sum(
            (
                max(Decimal("0"), acc.entry_quantity - acc.exit_filled_quantity)
                for acc in accumulators
            ),
            start=Decimal("0"),
        )
        if total_open == 0:
            current = None

    if not accumulators:
        return PositionRebuildResult(batches=(), diagnostics=tuple(diagnostics))

    batches: list[ManagedLivePositionBatch] = []
    for accumulator in accumulators:
        remaining_quantity = max(
            Decimal("0"),
            accumulator.entry_quantity - accumulator.exit_filled_quantity,
        )
        if remaining_quantity <= 0:
            continue
        entry_price = (
            accumulator.entry_notional / accumulator.entry_quantity
            if accumulator.entry_quantity > 0
            else observation.entry_price
        )
        active_limit_orders = [
            order
            for order in accumulator.exit_orders
            if order.plan is not None
            and order.plan.reduce_only
            and order.order_type == "LIMIT"
            and not order.state.terminal
        ]
        active_market_order = any(
            order.plan is not None
            and order.plan.reduce_only
            and order.order_type == "MARKET"
            and not order.state.terminal
            for order in accumulator.exit_orders
        )
        recovery_order = max(
            active_limit_orders,
            key=lambda order: (order.created_at, order.updated_at),
            default=None,
        )
        recovery_remaining = None
        if recovery_order is not None and recovery_order.plan is not None:
            recovery_remaining = max(
                Decimal("0"),
                recovery_order.plan.quantity - recovery_order.executed_quantity,
            )
        batches.append(
            ManagedLivePositionBatch(
                batch_id=accumulator.batch_id,
                quantity=remaining_quantity,
                entry_price=entry_price,
                opened_at=accumulator.opened_at,
                exit_order_submitted_at=accumulator.exit_order_submitted_at,
                recovery_order_client_id=(
                    None
                    if recovery_order is None or recovery_order.plan is None
                    else recovery_order.plan.client_order_id
                ),
                recovery_order_plan=(
                    None
                    if recovery_order is None
                    else recovery_order.plan
                ),
                recovery_order_remaining_quantity=recovery_remaining,
                closing_order_filled=active_market_order,
                legacy_attribution=accumulator.legacy_exit_attribution,
                entry_order_count=accumulator.entry_order_count,
                entry_client_order_ids=frozenset(accumulator.entry_client_order_ids),
            )
        )

    reconciled_batches = _reconcile_batch_quantities(
        batches,
        target_quantity=abs(observation.position_amt),
    )
    return PositionRebuildResult(
        batches=reconciled_batches,
        diagnostics=tuple(diagnostics),
    )


def _reconcile_batch_quantities(
    batches: list[ManagedLivePositionBatch],
    *,
    target_quantity: Decimal,
) -> tuple[ManagedLivePositionBatch, ...]:
    if target_quantity <= 0:
        return ()
    if not batches:
        return ()
    has_legacy_attribution = any(
        batch.legacy_attribution for batch in batches
    )
    if has_legacy_attribution:
        batches = [
            batch for batch in batches if not batch.legacy_attribution
        ]
        if not batches:
            return ()
        clean_quantity = sum(
            (batch.quantity for batch in batches),
            start=Decimal("0"),
        )
        if clean_quantity < target_quantity:
            return ()
        if clean_quantity == target_quantity:
            return tuple(batches)

        excess = clean_quantity - target_quantity
        legacy_reconciled: list[ManagedLivePositionBatch] = []
        for batch in batches:
            remove = min(excess, batch.quantity)
            remaining = batch.quantity - remove
            excess -= remove
            if remaining > 0:
                legacy_reconciled.append(replace(batch, quantity=remaining))
        return tuple(legacy_reconciled)
    total_quantity = sum((batch.quantity for batch in batches), start=Decimal("0"))
    if total_quantity < target_quantity:
        return tuple(batches)
    if total_quantity == target_quantity:
        return tuple(batches)

    excess = total_quantity - target_quantity
    reconciled: list[ManagedLivePositionBatch] = []
    for batch in batches:
        remove = min(excess, batch.quantity)
        remaining = batch.quantity - remove
        excess -= remove
        if remaining > 0:
            reconciled.append(replace(batch, quantity=remaining))
    return tuple(reconciled)


def _opening_order_matches_side(order_side: str, side: StrategySide) -> bool:
    return (order_side == "BUY") is (side is StrategySide.LONG)


def _is_entry_fill_observed(
    order: PositionOrderFact,
    fill_times: Mapping[str, datetime],
) -> bool:
    return (
        order.state in {
            ExchangeOrderState.PARTIALLY_FILLED,
            ExchangeOrderState.FILLED,
        }
        or _entry_fill_at(order, fill_times) is not None
        or order.executed_quantity > 0
    )


def _order_entry_time(
    order: PositionOrderFact,
    fill_times: Mapping[str, datetime],
) -> datetime:
    return _entry_fill_at(order, fill_times) or order.updated_at


def _entry_fill_at(
    order: PositionOrderFact | None,
    fill_times: Mapping[str, datetime],
) -> datetime | None:
    if order is None:
        return None
    for identifier in (order.exchange_order_id, order.client_order_id):
        if identifier is not None:
            fill_at = fill_times.get(identifier)
            if fill_at is not None:
                return fill_at
    return None


def _entry_fill_quantity(
    order: PositionOrderFact,
    fill_times: Mapping[str, datetime],
) -> Decimal:
    if order.executed_quantity > 0:
        return order.executed_quantity
    if order.state is ExchangeOrderState.FILLED:
        return order.quantity
    if (
        order.state
        in {
            ExchangeOrderState.PARTIALLY_FILLED,
            ExchangeOrderState.ACKNOWLEDGED,
            ExchangeOrderState.SUBMITTED,
            ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
        }
        and _entry_fill_at(order, fill_times) is not None
    ):
        return order.quantity
    return Decimal("0")


def _entry_price(
    order: PositionOrderFact,
    fill_prices: Mapping[str, Decimal],
    fallback: Decimal,
) -> Decimal:
    for identifier in (order.exchange_order_id, order.client_order_id):
        if identifier is not None and identifier in fill_prices:
            price = fill_prices[identifier]
            if price > 0:
                return price
    if order.price is not None and order.price > 0:
        return order.price
    return fallback


def _batch_id_for_entry(order: PositionOrderFact) -> str:
    identifier = order.client_order_id or order.exchange_order_id
    if identifier is None:
        identifier = (
            f"{order.created_at.isoformat()}:{order.side}:{order.quantity}"
        )
    return f"{order.symbol}:{order.position_side.value}:{identifier}"


def _exit_fill_quantity(order: PositionOrderFact) -> Decimal:
    if order.state not in _EXIT_SUBMITTED_STATES:
        return Decimal("0")
    if order.executed_quantity > 0:
        return order.executed_quantity
    return order.quantity if order.state is ExchangeOrderState.FILLED else Decimal("0")


def count_active_symbol_batch_concurrency(
    symbol: str,
    managed_positions: Sequence[object] = (),
    unresolved_orders: Sequence[object] = (),
) -> int:
    """Calculate the active batch entry concurrency for a symbol.

    Requirements:
    - Same symbol, same batch: at most max_concurrency entry orders.
    - If a batch has submitted an exit order (exit_order_submitted_at is not None,
      even if not filled yet), that batch is ended and does NOT count towards the
      active entry batch.
    - If there is an active batch (exit_order_submitted_at is None and not closing_order_filled),
      its entry_order_count represents how many entry orders were merged into this batch.
    - Unresolved (pending) non-reduce-only entry orders for this symbol add to the
      concurrency count (excluding any client_order_id already recorded in the batch).
    - Different batches and different symbols are independent.
    """
    active_batch_orders = 0
    known_entry_order_ids: set[str] = set()

    for p in managed_positions:
        if getattr(p, "symbol", "") != symbol:
            continue
        batches = getattr(p, "batches", None)
        if batches:
            for b in batches:
                has_exit_submitted = (
                    getattr(b, "exit_order_submitted_at", None) is not None
                )
                is_closing_filled = bool(
                    getattr(b, "closing_order_filled", False)
                )
                if not has_exit_submitted and not is_closing_filled:
                    active_batch_orders += int(
                        getattr(b, "entry_order_count", 1)
                    )
                    entry_ids = (
                        getattr(b, "entry_client_order_ids", ()) or ()
                    )
                    known_entry_order_ids.update(entry_ids)
        else:
            has_exit_started = (
                getattr(p, "recovery_exit_started_at", None) is not None
            )
            is_closing_filled = bool(
                getattr(p, "closing_order_filled", False)
            )
            if not has_exit_started and not is_closing_filled:
                active_batch_orders += 1

    pending_order_count = 0
    for o in unresolved_orders:
        if getattr(o, "symbol", "") != symbol:
            continue
        if getattr(o, "reduce_only", False):
            continue
        client_order_id = getattr(o, "client_order_id", None)
        if client_order_id and client_order_id in known_entry_order_ids:
            continue
        pending_order_count += 1

    return active_batch_orders + pending_order_count
