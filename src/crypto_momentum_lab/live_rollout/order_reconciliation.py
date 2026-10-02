"""Event-triggered repair of uncertain orders for a live strategy session.

Complete account WebSocket order facts update the existing durable state machine.
The existing repair worker reconciles incomplete events and unresolved orders.
"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

import structlog

from crypto_momentum_lab.domain.execution.order_read_models import (
    PersistedExchangeOrder,
)
from crypto_momentum_lab.domain.execution.order_read_repository import (
    OrderReadRepository,
)
from crypto_momentum_lab.domain.execution.order_state import ExchangeOrderState
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
    """Apply WS facts directly and repair only explicitly requested uncertainty.

    ``reconcile_account_event`` is deliberately narrow: callers invoke it
    before publishing the account projection: complete facts become durable
    observations, incomplete updates become durable uncertainty.
    The worker sleeps while idle and retries only unfinished repair work.
    """

    order_repository: OrderReadRepository
    state_machine: OrderExecutionPort
    run_id: str
    interval_seconds: float = DEFAULT_RECONCILE_INTERVAL_SECONDS
    on_unknown_order: Callable[[str], None] | None = None
    recover_exits: Callable[[], Awaitable[bool]] | None = None
    recover_commands: Callable[[], Awaitable[bool]] | None = None
    repair_positions: Callable[[], Awaitable[None]] | None = None
    request_unknown_exit: Callable[[PersistedExchangeOrder], bool] | None = None
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

    async def reconcile_all(self, *, include_confirmed: bool = False) -> bool:
        """Reconcile every unresolved order for the live session."""

        # WS can report a still-open order from an earlier run. Keep its run
        # scope in the same worker rather than dropping that repair request.
        runs = self._requested_runs | {self.run_id}
        self._requested_runs.clear()
        pending = False
        try:
            if self.recover_commands is not None:
                pending = await self.recover_commands()
            for run_id in sorted(runs):
                for order in await self.order_repository.load_unresolved_orders(run_id):
                    if not include_confirmed and order.state in {
                        ExchangeOrderState.ACKNOWLEDGED,
                        ExchangeOrderState.PARTIALLY_FILLED,
                    }:
                        continue
                    pending = True
                    self._requested_runs.add(run_id)
                    if (
                        self.request_unknown_exit is not None
                        and order.plan.reduce_only
                        and order.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
                        and self.request_unknown_exit(order)
                    ):
                        continue
                    await self.state_machine.reconcile_order(order.plan)
        except BaseException:
            self._requested_runs.update(runs)
            raise
        return pending

    async def run_requested(self) -> None:
        """Wait for requests; a timeout is used only while repairs remain pending."""

        retry_pending = False
        while True:
            if retry_pending:
                try:
                    await asyncio.wait_for(
                        self._requested.wait(), timeout=self.interval_seconds
                    )
                except TimeoutError:
                    pass
            else:
                await self._requested.wait()
            # Clear before the scan so a request arriving during REST work
            # remains set and triggers another scan after this one finishes.
            self._requested.clear()
            retry_pending = False
            if self.repair_positions is not None:
                try:
                    await self.repair_positions()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    retry_pending = True
                    log.exception(
                        "live_position_repair_failed", run_id=self.run_id
                    )
            try:
                retry_pending = await self.reconcile_all() or retry_pending
                if self.recover_exits is not None:
                    retry_pending = await self.recover_exits() or retry_pending
            except asyncio.CancelledError:
                raise
            except Exception:
                retry_pending = True
                log.exception(
                    "live_order_repair_failed",
                    run_id=self.run_id,
                    interval_seconds=self.interval_seconds,
                )


__all__ = [
    "DEFAULT_RECONCILE_INTERVAL_SECONDS",
    "LiveOrderReconciliation",
]
