"""In-memory pending-entry reservation and reconciliation state."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from decimal import Decimal

from crypto_momentum_lab.domain.execution.order_read_models import (
    PersistedExchangeOrder,
)
from crypto_momentum_lab.domain.execution.order_result import OrderExecutionResult
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.live_rollout.context import LiveDaemonRuntimeContext
from crypto_momentum_lab.live_rollout.gates import order_state_is_uncertain


class TerminalOrderBarrier:
    """Shared authority for terminal order identities across live rollout components.

    Enforces Invariant I1: once an order identity reaches a terminal state,
    it cannot be resurrected. Terminal executed quantity is strictly monotonic.
    """

    def __init__(
        self,
        capacity: int = 2048,
        durable_checker: Callable[[str], bool] | None = None,
        durable_reader: Callable[[str], tuple[bool, Decimal] | None] | None = None,
        on_terminal: Callable[[str, Decimal], None] | None = None,
    ) -> None:
        self._capacity = max(1, capacity)
        self._terminal_orders: dict[str, Decimal] = {}
        self._terminal_tombstones: set[str] = set()
        self._durable_checker = durable_checker
        self._durable_reader = durable_reader
        self._on_terminal = on_terminal

    def is_terminal(self, client_order_id: str) -> bool:
        if (
            client_order_id in self._terminal_orders
            or client_order_id in self._terminal_tombstones
        ):
            return True
        if self._durable_reader is not None:
            res = self._durable_reader(client_order_id)
            if res is not None and res[0]:
                self._terminal_tombstones.add(client_order_id)
                return True
        if self._durable_checker is not None and self._durable_checker(
            client_order_id
        ):
            self._terminal_tombstones.add(client_order_id)
            return True
        return False

    def get_executed_quantity(self, client_order_id: str) -> Decimal | None:
        if client_order_id in self._terminal_orders:
            return self._terminal_orders[client_order_id]
        if self._durable_reader is not None:
            res = self._durable_reader(client_order_id)
            if res is not None and res[0]:
                return res[1]
        return None

    def record_terminal(
        self, client_order_id: str, executed: Decimal = Decimal("0")
    ) -> Decimal:
        self._terminal_tombstones.add(client_order_id)
        existing = self._terminal_orders.get(client_order_id)
        if existing is None and self._durable_reader is not None:
            res = self._durable_reader(client_order_id)
            if res is not None and res[0]:
                existing = res[1]
        if existing is not None:
            executed = max(existing, executed)

        if client_order_id in self._terminal_orders:
            self._terminal_orders.pop(client_order_id)
        elif len(self._terminal_orders) >= self._capacity:
            oldest = next(iter(self._terminal_orders))
            self._terminal_orders.pop(oldest, None)
        self._terminal_orders[client_order_id] = executed
        if self._on_terminal is not None:
            self._on_terminal(client_order_id, executed)
        return executed


class LivePendingEntryRegistry:
    """Keep local entry reservations coherent with durable order state."""

    def __init__(
        self,
        *,
        clock: Callable[[], datetime],
        terminal_capacity: int = 1024,
        terminal_barrier: TerminalOrderBarrier | None = None,
        durable_checker: Callable[[str], bool] | None = None,
    ) -> None:
        self._clock = clock
        self._pending: dict[
            str,
            tuple[OrderExecutionPlan, Decimal],
        ] = {}
        self._uncertain: set[str] = set()
        self._settled: dict[str, tuple[OrderExecutionPlan, Decimal]] = {}
        self._exchange_ids: dict[str, str] = {}
        self._counted_entries: set[str] = set()
        self._counted_entry_quantities: dict[str, Decimal] = {}
        self._order_plan_quantities: dict[str, Decimal] = {}
        self._terminal_capacity = terminal_capacity
        self._known_terminal_orders: dict[str, Decimal] = {}
        self._terminal_tombstones: set[str] = set()
        if terminal_barrier is not None:
            self._terminal_barrier = terminal_barrier
            if self._terminal_barrier._durable_checker is None:
                self._terminal_barrier._durable_checker = (
                    durable_checker or self._is_durable_terminal
                )
            if self._terminal_barrier._durable_reader is None:
                self._terminal_barrier._durable_reader = self._durable_reader
        else:
            self._terminal_barrier = TerminalOrderBarrier(
                capacity=terminal_capacity,
                durable_checker=durable_checker or self._is_durable_terminal,
                durable_reader=self._durable_reader,
            )

    @property
    def terminal_barrier(self) -> TerminalOrderBarrier:
        return self._terminal_barrier

    def _durable_reader(self, client_order_id: str) -> tuple[bool, Decimal] | None:
        if client_order_id in self._known_terminal_orders:
            return True, self._known_terminal_orders[client_order_id]
        if client_order_id in self._terminal_tombstones:
            return True, Decimal("0")
        plan_qty = self._order_plan_quantities.get(client_order_id)
        if client_order_id in self._counted_entries:
            qty = self._counted_entry_quantities.get(client_order_id, Decimal("0"))
            if plan_qty is not None and qty >= plan_qty:
                return True, qty
        return None

    def _is_durable_terminal(self, client_order_id: str) -> bool:
        if (
            client_order_id in self._terminal_tombstones
            or client_order_id in self._known_terminal_orders
        ):
            return True
        plan_qty = self._order_plan_quantities.get(client_order_id)
        if client_order_id in self._counted_entries:
            qty = self._counted_entry_quantities.get(client_order_id, Decimal("0"))
            if plan_qty is not None and qty >= plan_qty:
                return True
        return False

    def _record_terminal(self, client_order_id: str, executed: Decimal) -> Decimal:
        self._terminal_tombstones.add(client_order_id)
        prev = self._known_terminal_orders.get(client_order_id, Decimal("0"))
        new_exec = max(prev, executed)
        if client_order_id in self._known_terminal_orders:
            self._known_terminal_orders.pop(client_order_id)
        elif len(self._known_terminal_orders) >= self._terminal_capacity:
            oldest = next(iter(self._known_terminal_orders))
            self._known_terminal_orders.pop(oldest, None)
            self._order_plan_quantities.pop(oldest, None)
        self._known_terminal_orders[client_order_id] = new_exec
        return self._terminal_barrier.record_terminal(client_order_id, executed)

    def is_terminal(self, client_order_id: str) -> bool:
        """Check whether this order identity has reached terminal state."""
        return self._terminal_barrier.is_terminal(client_order_id)

    def remember(
        self,
        plan: OrderExecutionPlan,
        result: OrderExecutionResult,
    ) -> None:
        self._order_plan_quantities[plan.client_order_id] = plan.quantity
        if result.exchange_order_id is not None:
            self._exchange_ids[plan.client_order_id] = result.exchange_order_id
        if self.is_terminal(plan.client_order_id):
            # I1: Order already observed terminal.
            # Late non-terminal must not recreate pending.
            # If the late event provides a higher executed quantity,
            # refine the settlement.
            existing_executed = self._terminal_barrier.get_executed_quantity(
                plan.client_order_id
            ) or Decimal("0")
            if result.executed_quantity > existing_executed:
                refined_executed = self._record_terminal(
                    plan.client_order_id, result.executed_quantity
                )
                if plan.client_order_id not in self._counted_entries:
                    self._settled[plan.client_order_id] = (plan, refined_executed)
            return
        if result.state.terminal or result.executed_quantity >= plan.quantity:
            executed = self._record_terminal(
                plan.client_order_id, result.executed_quantity
            )
            self._pending.pop(plan.client_order_id, None)
            self._uncertain.discard(plan.client_order_id)
            if executed > 0 and plan.client_order_id not in self._counted_entries:
                self._settled[plan.client_order_id] = (plan, executed)
            else:
                self._settled.pop(plan.client_order_id, None)
            return
        self._pending[plan.client_order_id] = (
            plan,
            result.executed_quantity,
        )
        if order_state_is_uncertain(result.state):
            self._uncertain.add(plan.client_order_id)
        else:
            self._uncertain.discard(plan.client_order_id)

    def observe_order_event(
        self,
        plan: OrderExecutionPlan,
        event: ExchangeOrderEvent,
    ) -> None:
        """Release a reservation as soon as a terminal entry event arrives."""

        if plan.reduce_only:
            return
        self._order_plan_quantities[plan.client_order_id] = plan.quantity
        if event.exchange_order_id is not None:
            self._exchange_ids[plan.client_order_id] = event.exchange_order_id
        if event.state.terminal:
            pending = self._pending.get(plan.client_order_id)
            raw_executed = event.details.get("executed_quantity")
            if raw_executed is not None and not isinstance(raw_executed, str):
                raise ValueError("order event executed_quantity must be a string")
            executed = (
                Decimal(raw_executed)
                if raw_executed is not None
                else plan.quantity
                if event.state is ExchangeOrderState.FILLED
                else pending[1]
                if pending
                else Decimal(0)
            )
            if not executed.is_finite() or executed < 0:
                raise ValueError(
                    "order event executed_quantity must be finite and nonnegative"
                )
            existing_executed = self._terminal_barrier.get_executed_quantity(
                plan.client_order_id
            ) or Decimal("0")
            executed = max(executed, existing_executed)
            self._record_terminal(plan.client_order_id, executed)
            if executed > 0 and plan.client_order_id not in self._counted_entries:
                self._settled[plan.client_order_id] = (plan, executed)
            else:
                self._settled.pop(plan.client_order_id, None)
            self._pending.pop(plan.client_order_id, None)
            self._uncertain.discard(plan.client_order_id)
            return
        if self.is_terminal(plan.client_order_id):
            # I1: Do not reactivate once terminal
            return
        if plan.client_order_id in self._pending:
            raw_executed = event.details.get("executed_quantity")
            if raw_executed is not None and isinstance(raw_executed, str):
                cur_plan, cur_exec = self._pending[plan.client_order_id]
                new_exec = Decimal(raw_executed)
                if new_exec > cur_exec:
                    self._pending[plan.client_order_id] = (cur_plan, new_exec)
        if (
            order_state_is_uncertain(event.state)
            and plan.client_order_id in self._pending
        ):
            self._uncertain.add(plan.client_order_id)
        else:
            self._uncertain.discard(plan.client_order_id)

    def sync(self, context: LiveDaemonRuntimeContext) -> None:
        """Merge a fresh durable view without releasing unconfirmed orders."""

        persisted = {
            item.plan.client_order_id: item
            for item in context.unresolved_orders
            if not item.plan.reduce_only
        }
        for item in context.unresolved_orders:
            self._order_plan_quantities[item.plan.client_order_id] = item.plan.quantity
        observed_entry_quantities: dict[str, Decimal] = {}
        for position in context.managed_positions:
            for batch in position.batches:
                remaining_batch_qty = getattr(batch, "quantity", Decimal("0"))
                for client_id in batch.entry_client_order_ids:
                    plan_qty = self._order_plan_quantities.get(client_id)
                    already = observed_entry_quantities.get(
                        client_id, Decimal("0")
                    )
                    if plan_qty is not None:
                        needed = max(Decimal("0"), plan_qty - already)
                        alloc = min(remaining_batch_qty, needed)
                    else:
                        alloc = remaining_batch_qty
                    observed_entry_quantities[client_id] = already + alloc
                    remaining_batch_qty = max(
                        Decimal("0"), remaining_batch_qty - alloc
                    )
        observed_exchange_entries = {
            exchange_id
            for position in context.managed_positions
            for batch in position.batches
            for exchange_id in getattr(batch, "entry_exchange_order_ids", ())
        }
        for client_id, exchange_id in self._exchange_ids.items():
            if exchange_id in observed_exchange_entries:
                for position in context.managed_positions:
                    for batch in position.batches:
                        if exchange_id in getattr(
                            batch, "entry_exchange_order_ids", ()
                        ):
                            remaining_batch_qty = getattr(
                                batch, "quantity", Decimal("0")
                            )
                            for cid in batch.entry_client_order_ids:
                                p_qty = self._order_plan_quantities.get(cid)
                                if p_qty is not None:
                                    remaining_batch_qty = max(
                                        Decimal("0"),
                                        remaining_batch_qty - p_qty,
                                    )
                            plan_qty = self._order_plan_quantities.get(client_id)
                            already = observed_entry_quantities.get(
                                client_id, Decimal("0")
                            )
                            if plan_qty is not None:
                                needed = max(Decimal("0"), plan_qty - already)
                                alloc = min(remaining_batch_qty, needed)
                            else:
                                alloc = remaining_batch_qty
                            observed_entry_quantities[client_id] = (
                                already + alloc
                            )
        self._counted_entry_quantities = observed_entry_quantities
        self._counted_entries = set(observed_entry_quantities)
        for client_order_id, executed_qty in observed_entry_quantities.items():
            persisted_order = persisted.get(client_order_id)
            if persisted_order is not None and not (
                persisted_order.state.terminal
                or persisted_order.executed_quantity >= persisted_order.plan.quantity
            ):
                # Working on exchange (e.g. partially filled); NOT terminal!
                continue
            plan_qty = self._order_plan_quantities.get(client_order_id)
            if (
                plan_qty is not None
                and executed_qty < plan_qty
                and persisted_order is None
            ):
                # Partial fill alone without terminal confirmation is not terminal
                continue
            self._record_terminal(client_order_id, executed_qty)
        for client_order_id in tuple(self._settled):
            if client_order_id in self._counted_entries:
                self._settled.pop(client_order_id)
        for client_order_id, (plan, executed_quantity) in tuple(self._pending.items()):
            if self.is_terminal(client_order_id):
                self._pending.pop(client_order_id, None)
                self._uncertain.discard(client_order_id)
                continue
            persisted_order = persisted.get(client_order_id)
            if persisted_order is None:
                # Keep a just-submitted order until a fresh account/context
                # read confirms its terminal state.
                # A local TTL or a missing unresolved row is not terminal proof.
                continue
            if (
                persisted_order.state.terminal
                or persisted_order.executed_quantity >= plan.quantity
            ):
                executed = self._record_terminal(
                    client_order_id, persisted_order.executed_quantity
                )
                self._pending.pop(client_order_id, None)
                self._uncertain.discard(client_order_id)
                if executed > 0:
                    if client_order_id not in self._counted_entries:
                        self._settled[client_order_id] = (plan, executed)
                else:
                    self._settled.pop(client_order_id, None)
            elif persisted_order.executed_quantity > executed_quantity:
                self._pending[client_order_id] = (
                    plan,
                    persisted_order.executed_quantity,
                )
            if order_state_is_uncertain(persisted_order.state):
                self._uncertain.add(client_order_id)
            else:
                self._uncertain.discard(client_order_id)

        self._exchange_ids = {
            client_id: exchange_id
            for client_id, exchange_id in self._exchange_ids.items()
            if client_id in self._pending or client_id in self._settled
        }
        active_order_ids = (
            set(self._pending)
            | set(self._settled)
            | set(persisted)
            | self._counted_entries
            | set(self._known_terminal_orders)
        )
        if len(self._order_plan_quantities) > 2 * self._terminal_capacity:
            for cid in list(self._order_plan_quantities):
                if cid not in active_order_ids:
                    self._order_plan_quantities.pop(cid, None)
                    if len(self._order_plan_quantities) <= self._terminal_capacity:
                        break

    def reservation(
        self,
        persisted_orders: tuple[PersistedExchangeOrder, ...],
    ) -> tuple[Decimal, frozenset[str]]:
        pending: dict[str, tuple[OrderExecutionPlan, Decimal]] = {
            item.plan.client_order_id: (item.plan, item.executed_quantity)
            for item in persisted_orders
            if not item.plan.reduce_only and not item.state.terminal
        }
        pending.update(self._pending)
        reserved_notional = Decimal("0")
        reserved_symbols: set[str] = set()
        for plan, executed_quantity in pending.values():
            remaining_quantity = max(
                Decimal("0"),
                plan.quantity - executed_quantity,
            )
            if remaining_quantity <= 0:
                continue
            effective_price = (
                plan.price if plan.price is not None else plan.reference_price
            )
            if effective_price is not None and effective_price > 0:
                reserved_notional += remaining_quantity * effective_price
            reserved_symbols.add(plan.symbol)
        # A synchronous fill can arrive before the next position snapshot.
        # Reserve its exposure as well as its slot until the Book shows it.
        for plan, executed_quantity in self._settled.values():
            effective_price = (
                plan.price if plan.price is not None else plan.reference_price
            )
            if effective_price is not None and effective_price > 0:
                reserved_notional += executed_quantity * effective_price
            reserved_symbols.add(plan.symbol)
        return reserved_notional, frozenset(reserved_symbols)

    def snapshot(self) -> tuple[tuple[OrderExecutionPlan, Decimal], ...]:
        return tuple(self._pending.values())

    def admission_snapshot(self) -> tuple[tuple[OrderExecutionPlan, Decimal], ...]:
        """Include settled fills until the authoritative position shows them.

        Cancellation uses snapshot(), never these already terminal orders.
        """
        return tuple(
            value
            for client_id, value in {**self._settled, **self._pending}.items()
            if client_id not in self._counted_entries
        )

    def pending_symbols(self) -> frozenset[str]:
        return frozenset(plan.symbol for plan, _quantity in self.admission_snapshot())

    def has_uncertain_entry(self, symbol: str) -> bool:
        return any(
            plan.symbol == symbol and client_id in self._uncertain
            for client_id, (plan, _) in self._pending.items()
        )


__all__ = ["LivePendingEntryRegistry", "TerminalOrderBarrier"]
