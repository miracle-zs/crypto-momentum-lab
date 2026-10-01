"""Durable order reconciliation for a live strategy session.

Complete account WebSocket order facts update the existing durable state machine.
REST reconciles incomplete events and periodically repairs unresolved orders.
"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import structlog

from crypto_momentum_lab.domain.execution.order_read_repository import (
    OrderReadRepository,
)
from crypto_momentum_lab.execution_account.binance.user_data_parser import (
    order_snapshot_from_update,
)
from crypto_momentum_lab.execution_account.hub import AccountEvent
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionPort,
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

    order_repository: OrderReadRepository
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
        persisted = await self.order_repository.load_order(event.client_order_id)
        if persisted is not None:
            snapshot = (
                None
                if event.order_update is None
                else order_snapshot_from_update(event.order_update, persisted.plan)
            )
            if (
                snapshot is not None
                and persisted.exchange_order_id is not None
                and (snapshot.exchange_order_id != persisted.exchange_order_id)
            ):
                raise ValueError("WS order update conflicts with its durable identity")
            if persisted.state.terminal and (
                snapshot is None
                or snapshot.executed_quantity <= persisted.executed_quantity
            ):
                log.info(
                    "live_account_event_duplicate_terminal_order",
                    run_id=self.run_id,
                    client_order_id=event.client_order_id,
                    state=persisted.state.value,
                )
                return
            if snapshot is None:
                await self.state_machine.reconcile_order(persisted.plan)
            elif snapshot.executed_quantity < persisted.executed_quantity or (
                snapshot.executed_quantity == persisted.executed_quantity
                and snapshot.observed_at < persisted.updated_at
            ):
                # Replayed facts must not roll back a newer observation.
                return
            else:
                await self.state_machine.apply_observed_snapshot(
                    persisted.plan, snapshot
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

        for order in await self.order_repository.load_unresolved_orders(self.run_id):
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
