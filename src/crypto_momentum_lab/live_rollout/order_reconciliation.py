"""Event-triggered repair of uncertain orders for a live strategy session.

Complete account WebSocket order facts update the existing durable state machine.
The existing repair worker reconciles incomplete events and unresolved orders.
"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Literal

import structlog

from crypto_momentum_lab.domain.execution.command_models import DispatchState
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.order_execution_port import (
    CoordinatedOrderExecutionPort,
)
from crypto_momentum_lab.domain.execution.order_read_models import (
    PersistedExchangeOrder,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.ports import (
    OrderReadRepository,
)
from crypto_momentum_lab.execution_account.binance.user_data_parser import (
    order_snapshot_from_update,
)
from crypto_momentum_lab.execution_account.hub import AccountEvent
from crypto_momentum_lab.live_rollout.command_receipt_recovery import (
    recover_restored_commands,
)

log = structlog.get_logger()

DEFAULT_RECONCILE_INTERVAL_SECONDS = 60.0
RecoveryTask = Literal["positions", "orders", "exits"]
_RECOVERY_TASKS: tuple[RecoveryTask, ...] = ("positions", "orders", "exits")


@dataclass(slots=True)
class LiveOrderReconciliation:
    """Apply WS facts directly and repair only explicitly requested uncertainty.

    ``reconcile_account_event`` is deliberately narrow: callers invoke it
    before publishing the account projection: complete facts become durable
    observations, incomplete updates become durable uncertainty.
    The worker sleeps while idle and retries only unfinished repair work.
    """

    order_repository: OrderReadRepository
    state_machine: CoordinatedOrderExecutionPort
    run_id: str
    interval_seconds: float = DEFAULT_RECONCILE_INTERVAL_SECONDS
    max_order_lookups_per_pass: int = 32
    family_timeout_seconds: float = 30.0
    on_unknown_order: Callable[[str], None] | None = None
    recover_exits: Callable[[], Awaitable[bool]] | None = None
    execution_book: ExecutionBook | None = None
    repair_positions: Callable[[], Awaitable[bool]] | None = None
    request_unknown_exit: Callable[[PersistedExchangeOrder], bool] | None = None
    _requested: asyncio.Event = field(
        default_factory=asyncio.Event, init=False, repr=False
    )

    _requested_runs: set[str] = field(default_factory=set, init=False, repr=False)
    _requested_tasks: set[RecoveryTask] = field(
        default_factory=set, init=False, repr=False
    )
    _retry_at: dict[RecoveryTask, float] = field(
        default_factory=dict, init=False, repr=False
    )
    _running_tasks: set[RecoveryTask] = field(
        default_factory=set, init=False, repr=False
    )
    _last_lookup_id: str | None = field(default=None, init=False, repr=False)
    _orphan_cancels: dict[str, OrderExecutionPlan] = field(
        default_factory=dict, init=False, repr=False
    )

    def __post_init__(self) -> None:
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        if self.max_order_lookups_per_pass <= 0 or self.family_timeout_seconds <= 0:
            raise ValueError("recovery lookup and time budgets must be positive")

    def request_recovery(self) -> None:
        """Discover all recovery families at startup or explicit full repair."""
        self._requested_tasks.update(_RECOVERY_TASKS)
        self._requested.set()

    def request_order_recovery(
        self, orphan_plans: tuple[OrderExecutionPlan, ...] = ()
    ) -> None:
        """Repair order/command uncertainty without scanning exits or positions."""
        self._orphan_cancels.update(
            (plan.client_order_id, plan) for plan in orphan_plans
        )
        self._requested_tasks.add("orders")
        self._requested.set()

    def request_exit_recovery(self) -> None:
        """Resume the existing exit owner without restarting order discovery."""
        self._requested_tasks.add("exits")
        self._requested.set()

    def request_position_recovery(self) -> None:
        """Repair unmanaged exposure without restarting order discovery."""
        self._requested_tasks.add("positions")
        self._requested.set()

    def notify_account_facts_changed(self) -> None:
        """Resume existing repairs after fact publication; idle facts need no scan."""
        self._requested_tasks.update(self._retry_at)
        self._requested_tasks.update(self._running_tasks)
        if self._requested_tasks:
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
            is_outbox_terminal = True
            book = self.execution_book
            if book is not None:
                entry = book.get_outbox(event.client_order_id)
                if entry is not None and entry.state not in (
                    DispatchState.TERMINAL,
                    DispatchState.REJECTED,
                ):
                    is_outbox_terminal = False

            if (
                is_outbox_terminal
                and persisted.state.terminal
                and (
                    snapshot is None
                    or snapshot.executed_quantity <= persisted.executed_quantity
                )
            ):
                log.info(
                    "live_account_event_duplicate_terminal_order",
                    run_id=self.run_id,
                    client_order_id=event.client_order_id,
                    state=persisted.state.value,
                )
                return

            if not is_outbox_terminal and persisted.state.terminal:
                if persisted.terminal_receipt is not None:
                    await self.state_machine.observe_recovered_receipt(
                        persisted.plan, persisted.terminal_receipt
                    )
                    return
                elif snapshot is not None:
                    await self.state_machine.apply_observed_snapshot(
                        persisted.plan, snapshot
                    )
                    return
                else:
                    # Incomplete WS facts schedule the existing repair worker;
                    # account publication must never await a REST round trip.
                    self._requested_runs.add(persisted.plan.run_id)
                    self.request_order_recovery()
                    return
            if snapshot is None:
                await self.state_machine.mark_reconciliation_pending(
                    persisted.plan,
                )
                self._requested_runs.add(persisted.plan.run_id)
                self.request_order_recovery()
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
        plans: dict[str, OrderExecutionPlan] = {}

        try:
            cancel_batch = tuple(self._orphan_cancels.values())[
                : self.max_order_lookups_per_pass
            ]
            for plan in cancel_batch:
                # Rotate before I/O so one failing order cannot starve cleanup.
                self._orphan_cancels.pop(plan.client_order_id, None)
                self._orphan_cancels[plan.client_order_id] = plan
                try:
                    result = await self.state_machine.cancel_order(plan)
                except Exception as error:
                    log.warning(
                        "live_orphan_cancel_deferred",
                        run_id=self.run_id,
                        client_order_id=plan.client_order_id,
                        error_type=type(error).__name__,
                    )
                    continue
                if result.state.terminal:
                    self._orphan_cancels.pop(plan.client_order_id, None)
            pending = bool(self._orphan_cancels)
            if self.execution_book is not None:
                command_pending, command_plans = await recover_restored_commands(
                    book=self.execution_book,
                    coordinator=self.state_machine,
                    orders=self.order_repository,
                )
                pending = command_pending or pending
                plans.update((plan.client_order_id, plan) for plan in command_plans)
            for run_id in sorted(runs):
                for order in await self.order_repository.load_unresolved_orders(run_id):
                    if not include_confirmed and order.state in {
                        ExchangeOrderState.ACKNOWLEDGED,
                        ExchangeOrderState.PARTIALLY_FILLED,
                    }:
                        continue
                    if (
                        self.request_unknown_exit is not None
                        and order.plan.reduce_only
                        and order.state
                        is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
                        and self.request_unknown_exit(order)
                    ):
                        self.request_exit_recovery()
                        continue
                    pending = True
                    self._requested_runs.add(run_id)
                    plans.setdefault(order.plan.client_order_id, order.plan)
            ordered = list(plans.values())
            ids = list(plans)
            if self._last_lookup_id in ids:
                start = ids.index(self._last_lookup_id) + 1
                ordered = ordered[start:] + ordered[:start]
            lookup_budget = self.max_order_lookups_per_pass - len(cancel_batch)
            for plan in ordered[:lookup_budget]:
                # Move the cursor before I/O so a failing order cannot starve
                # the remaining commands on the next requested pass.
                self._last_lookup_id = plan.client_order_id
                await self.state_machine.reconcile_order(plan)
            if len(ordered) > lookup_budget:
                self._requested_runs.update(runs)
                pending = True
        except BaseException:
            self._requested_runs.update(runs)
            raise
        return pending

    async def run_requested(self) -> None:
        """Run requested families; retry only unfinished work on its own deadline."""

        loop = asyncio.get_running_loop()
        while True:
            if self._retry_at:
                try:
                    await asyncio.wait_for(
                        self._requested.wait(),
                        timeout=max(0.0, min(self._retry_at.values()) - loop.time()),
                    )
                except TimeoutError:
                    pass
            else:
                await self._requested.wait()
            tasks = self._requested_tasks | {
                task
                for task, deadline in self._retry_at.items()
                if deadline <= loop.time()
            }
            self._requested_tasks.clear()
            self._requested.clear()
            # New requests are retained independently while this pass awaits I/O.
            self._running_tasks = tasks
            try:
                for task in _RECOVERY_TASKS:
                    if task not in tasks:
                        continue
                    self._retry_at.pop(task, None)
                    try:
                        pending = False
                        async with asyncio.timeout(self.family_timeout_seconds):
                            if (
                                task == "positions"
                                and self.repair_positions is not None
                            ):
                                pending = await self.repair_positions()
                            elif task == "orders":
                                pending = await self.reconcile_all()
                            elif task == "exits" and self.recover_exits is not None:
                                pending = await self.recover_exits()
                    except asyncio.CancelledError:
                        self._requested_tasks.update(self._running_tasks)
                        self._requested.set()
                        raise
                    except Exception:
                        pending = True
                        log.exception(
                            "live_position_repair_failed"
                            if task == "positions"
                            else "live_order_repair_failed",
                            run_id=self.run_id,
                            recovery_task=task,
                            interval_seconds=self.interval_seconds,
                        )
                    if pending:
                        self._retry_at[task] = loop.time() + self.interval_seconds
                    self._running_tasks.discard(task)
            finally:
                self._running_tasks.clear()


__all__ = [
    "DEFAULT_RECONCILE_INTERVAL_SECONDS",
    "LiveOrderReconciliation",
]
