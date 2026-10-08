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


class LivePendingEntryRegistry:
    """Keep local entry reservations coherent with durable order state."""

    def __init__(self, *, clock: Callable[[], datetime]) -> None:
        self._clock = clock
        self._pending: dict[
            str,
            tuple[OrderExecutionPlan, Decimal],
        ] = {}
        self._uncertain: set[str] = set()
        self._settled: dict[str, tuple[OrderExecutionPlan, Decimal]] = {}
        self._exchange_ids: dict[str, str] = {}
        self._counted_entries: set[str] = set()

    def remember(
        self,
        plan: OrderExecutionPlan,
        result: OrderExecutionResult,
    ) -> None:
        if result.exchange_order_id is not None:
            self._exchange_ids[plan.client_order_id] = result.exchange_order_id
        if result.state.terminal or result.executed_quantity >= plan.quantity:
            self._pending.pop(plan.client_order_id, None)
            self._uncertain.discard(plan.client_order_id)
            if result.executed_quantity > 0:
                self._settled[plan.client_order_id] = (plan, result.executed_quantity)
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
            if executed > 0:
                self._settled[plan.client_order_id] = (plan, executed)
            else:
                self._settled.pop(plan.client_order_id, None)
            self._pending.pop(plan.client_order_id, None)
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
        observed_entries = {
            client_id
            for position in context.managed_positions
            for batch in position.batches
            for client_id in batch.entry_client_order_ids
        }
        observed_exchange_entries = {
            exchange_id
            for position in context.managed_positions
            for batch in position.batches
            for exchange_id in batch.entry_exchange_order_ids
        }
        observed_entries.update(
            client_id
            for client_id, exchange_id in self._exchange_ids.items()
            if exchange_id in observed_exchange_entries
        )
        self._counted_entries = observed_entries
        for client_order_id in tuple(self._settled):
            if client_order_id in observed_entries:
                self._settled.pop(client_order_id)
        for client_order_id, (plan, executed_quantity) in tuple(self._pending.items()):
            item = persisted.get(client_order_id)
            if item is None:
                # Keep a just-submitted order until a fresh account/context
                # read confirms its terminal state.
                # A local TTL or a missing unresolved row is not terminal proof.
                continue
            if item.state.terminal or item.executed_quantity >= plan.quantity:
                self._pending.pop(client_order_id, None)
                if item.executed_quantity > 0:
                    if client_order_id not in observed_entries:
                        self._settled[client_order_id] = (plan, item.executed_quantity)
                else:
                    self._settled.pop(client_order_id, None)
            elif item.executed_quantity > executed_quantity:
                self._pending[client_order_id] = (
                    plan,
                    item.executed_quantity,
                )
            if order_state_is_uncertain(item.state):
                self._uncertain.add(client_order_id)
            else:
                self._uncertain.discard(client_order_id)

        self._exchange_ids = {
            client_id: exchange_id
            for client_id, exchange_id in self._exchange_ids.items()
            if client_id in self._pending or client_id in self._settled
        }

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


__all__ = ["LivePendingEntryRegistry"]
