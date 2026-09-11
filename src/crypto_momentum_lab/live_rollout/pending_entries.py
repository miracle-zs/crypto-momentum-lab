"""In-memory pending-entry reservation and reconciliation state."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from decimal import Decimal

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    OrderExecutionPlan,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
)
from crypto_momentum_lab.live_rollout.context import LiveDaemonRuntimeContext
from crypto_momentum_lab.persistence.postgres.order_repository import (
    PersistedExchangeOrder,
)


class LivePendingEntryRegistry:
    """Keep local entry reservations coherent with durable order state."""

    def __init__(self, *, clock: Callable[[], datetime]) -> None:
        self._clock = clock
        self._pending: dict[
            str,
            tuple[OrderExecutionPlan, Decimal],
        ] = {}

    def remember(
        self,
        plan: OrderExecutionPlan,
        result: OrderExecutionResult,
    ) -> None:
        if result.state.terminal or result.executed_quantity >= plan.quantity:
            self._pending.pop(plan.client_order_id, None)
            return
        self._pending[plan.client_order_id] = (
            plan,
            result.executed_quantity,
        )

    def observe_order_event(
        self,
        plan: OrderExecutionPlan,
        event: ExchangeOrderEvent,
    ) -> None:
        """Release a reservation as soon as a terminal entry event arrives."""

        if plan.reduce_only:
            return
        state = getattr(event, "state", None)
        if state is not None and getattr(state, "terminal", False):
            self._pending.pop(plan.client_order_id, None)

    def sync(self, context: LiveDaemonRuntimeContext) -> None:
        """Merge a fresh durable view without releasing unconfirmed orders."""

        persisted = {
            item.plan.client_order_id: item
            for item in context.unresolved_orders
            if not item.plan.reduce_only
        }
        for client_order_id, (plan, executed_quantity) in tuple(
            self._pending.items()
        ):
            item = persisted.get(client_order_id)
            if item is None:
                # Keep a just-submitted order until a fresh account/context
                # read confirms its terminal state.
                if plan.expires_at is not None and plan.expires_at <= self._clock():
                    self._pending.pop(client_order_id, None)
                continue
            if item.state.terminal or item.executed_quantity >= plan.quantity:
                self._pending.pop(client_order_id, None)
            elif item.executed_quantity > executed_quantity:
                self._pending[client_order_id] = (
                    plan,
                    item.executed_quantity,
                )

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
            if remaining_quantity <= 0 or plan.price is None:
                continue
            reserved_notional += remaining_quantity * plan.price
            reserved_symbols.add(plan.symbol)
        return reserved_notional, frozenset(reserved_symbols)

    def snapshot(self) -> tuple[tuple[OrderExecutionPlan, Decimal], ...]:
        return tuple(self._pending.values())

    def pending_symbols(self) -> frozenset[str]:
        return frozenset(plan.symbol for plan, _quantity in self._pending.values())


__all__ = ["LivePendingEntryRegistry"]
