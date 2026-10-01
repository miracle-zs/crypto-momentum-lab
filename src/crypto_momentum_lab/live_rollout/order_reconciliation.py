"""Durable order reconciliation for a live strategy session.

Complete account WebSocket order facts update the existing durable state machine.
The existing repair worker reconciles incomplete events and unresolved orders.
"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

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
    before publishing the account projection: complete facts become durable
    observations, incomplete updates become durable uncertainty.
    ``run_periodically`` is best-effort and never replaces the durable order
    state machine; it only retries unresolved orders after transient failures.
    """

    order_repository: OrderReadRepository
    state_machine: OrderExecutionPort
    run_id: str
    interval_seconds: float = DEFAULT_RECONCILE_INTERVAL_SECONDS
    on_unknown_order: Callable[[str], None] | None = None
    recover_exits: Callable[[], Awaitable[None]] | None = None
    _requested: asyncio.Event = field(
        default_factory=asyncio.Event, init=False, repr=False
    )

    _requested_runs: set[str] = field(default_factory=set, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")

    def request_recovery(self) -> None:
        """Wake the existing repair worker without awaiting exchange work."""
        self._requested.set()

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
                await self.state_machine.mark_reconciliation_pending(
                    persisted.plan,
                )
                self._requested_runs.add(persisted.plan.run_id)
                self._requested.set()
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

        # WS can report a still-open order from an earlier run. Keep its run
        # scope in the same worker rather than dropping that repair request.
        runs = self._requested_runs | {self.run_id}
        self._requested_runs.clear()
        try:
            for run_id in sorted(runs):
                for order in await self.order_repository.load_unresolved_orders(run_id):
                    await self.state_machine.reconcile_order(order.plan)
        except BaseException:
            self._requested_runs.update(runs)
            raise

    async def run_periodically(self) -> None:
        """Run the existing repair worker on request or periodic timeout."""

        while True:
            try:
                await asyncio.wait_for(
                    self._requested.wait(),
                    timeout=self.interval_seconds,
                )
            except TimeoutError:
                pass
            # Clear before the scan so a request arriving during REST work
            # remains set and triggers another scan after this one finishes.
            self._requested.clear()
            try:
                await self.reconcile_all()
                if self.recover_exits is not None:
                    await self.recover_exits()
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
