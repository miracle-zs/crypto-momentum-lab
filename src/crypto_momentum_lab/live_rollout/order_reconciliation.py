"""Durable order reconciliation for a live strategy session.

The account WebSocket is an acceleration path, not the source of truth.  This
module owns both sides of that contract: reconcile an affected order before an
account snapshot is published, and periodically reconcile every unresolved
order as the eventual-consistency safety net.
"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import structlog

from crypto_momentum_lab.domain.execution import ExchangeOrderState
from crypto_momentum_lab.execution_account.hub import AccountEvent
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionPort,
)
from crypto_momentum_lab.persistence.postgres.order_repository import (
    PostgresOrderRepository,
)

log = structlog.get_logger()

DEFAULT_RECONCILE_INTERVAL_SECONDS = 60.0


@dataclass(slots=True)
class LiveOrderReconciliation:
    """Coordinate immediate and periodic reconciliation for one live run.

    ``reconcile_account_event`` is deliberately narrow: callers invoke it
    before publishing the account projection so an order-trade update cannot
    make a newly opened position visible before its durable order state.
    ``run_periodically`` is best-effort and never replaces the durable order
    state machine; it only retries unresolved orders after transient failures.
    """

    order_repository: PostgresOrderRepository
    state_machine: OrderExecutionPort
    run_id: str
    interval_seconds: float = DEFAULT_RECONCILE_INTERVAL_SECONDS
    on_unknown_order: Callable[[str], None] | None = None

    def __post_init__(self) -> None:
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")

    async def reconcile_account_event(self, event: AccountEvent) -> None:
        """Reconcile the unresolved order identified by an account event."""

        if not event.client_order_id:
            return
        unresolved = await self.order_repository.load_unresolved_orders(
            self.run_id
        )
        for order in unresolved:
            if order.plan.client_order_id == event.client_order_id:
                await self.state_machine.reconcile_order(order.plan)
                return
        load_order = getattr(self.order_repository, "load_order", None)
        if callable(load_order):
            persisted = await load_order(event.client_order_id)
            if persisted is not None:
                if persisted.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION:
                    await self.state_machine.reconcile_order(persisted.plan)
                    return
                if not persisted.state.terminal:
                    await self.state_machine.reconcile_order(persisted.plan)
                    return
                log.info(
                    "live_account_event_duplicate_terminal_order",
                    run_id=self.run_id,
                    client_order_id=event.client_order_id,
                    state=persisted.state.value,
                )
                return
        reason = "account_event_order_missing_from_local_journal"
        log.warning(
            "live_account_event_order_missing_from_local_journal",
            run_id=self.run_id,
            client_order_id=event.client_order_id,
        )
        if self.on_unknown_order is not None:
            self.on_unknown_order(reason)

    async def reconcile_all(self) -> None:
        """Reconcile every unresolved order for the live session."""

        for order in await self.order_repository.load_unresolved_orders(
            self.run_id
        ):
            await self.state_machine.reconcile_order(order.plan)

    async def run_periodically(
        self,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """Run the eventual-consistency safety net until cancelled."""

        while True:
            await sleep(self.interval_seconds)
            try:
                await self.reconcile_all()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception(
                    "live_periodic_order_reconcile_failed",
                    run_id=self.run_id,
                    interval_seconds=self.interval_seconds,
                )


__all__ = [
    "DEFAULT_RECONCILE_INTERVAL_SECONDS",
    "LiveOrderReconciliation",
]
