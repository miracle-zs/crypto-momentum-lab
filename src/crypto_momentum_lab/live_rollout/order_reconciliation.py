"""Event-triggered repair of uncertain orders for a live strategy session.

Complete account WebSocket order facts update the existing durable state machine.
The existing repair worker reconciles incomplete events and unresolved orders.
"""

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Literal

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


@dataclass(frozen=True, slots=True)
class ReconciliationPassResult:
    tasks: frozenset[str]
    success: bool
    verified_symbols: frozenset[str]
    failed_symbols: frozenset[str]
    pending_tasks: frozenset[str]
    has_unknown: bool
    has_pending_work: bool
    failed_tasks: frozenset[str] = frozenset()
    has_exception: bool = False


def _extract_plan_symbol(plan: Any) -> str | None:
    sym = getattr(plan, "symbol", None)
    if isinstance(sym, str) and sym.strip():
        return sym.strip()
    return None


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
    on_reconcile_pass_completed: (
        Callable[[ReconciliationPassResult], None] | Callable[[], None] | None
    ) = None
    time_source: Callable[[], float] = time.monotonic
    _requested: asyncio.Event = field(
        default_factory=asyncio.Event, init=False, repr=False
    )

    _requested_runs: set[str] = field(default_factory=set, init=False, repr=False)
    _requested_tasks: set[RecoveryTask] = field(
        default_factory=set, init=False, repr=False
    )
    _pass_verified_symbols: set[str] = field(
        default_factory=set, init=False, repr=False
    )
    _pass_failed_symbols: set[str] = field(
        default_factory=set, init=False, repr=False
    )
    _pass_has_unknown: bool = field(default=False, init=False, repr=False)
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
    _last_failure: dict[RecoveryTask, str] = field(
        default_factory=dict, init=False, repr=False
    )
    _evidence_version: dict[RecoveryTask, int] = field(
        default_factory=lambda: {"positions": 0, "orders": 0, "exits": 0},
        init=False,
        repr=False,
    )
    _evidence_version_at_retry: dict[RecoveryTask, int] = field(
        default_factory=dict, init=False, repr=False
    )
    _active_reconciliation_order: str | None = field(
        default=None, init=False, repr=False
    )
    _known_unresolved_order_ids: set[str] = field(
        default_factory=set, init=False, repr=False
    )

    def __post_init__(self) -> None:
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        if self.max_order_lookups_per_pass <= 0 or self.family_timeout_seconds <= 0:
            raise ValueError("recovery lookup and time budgets must be positive")

    def _record_failed_symbol(self, plan: Any) -> None:
        sym = _extract_plan_symbol(plan)
        if sym:
            self._pass_failed_symbols.add(sym)
        else:
            self._pass_failed_symbols.add("*")

    def _record_verified_symbol(self, plan: Any) -> None:
        sym = _extract_plan_symbol(plan)
        if sym:
            self._pass_verified_symbols.add(sym)

    def request_recovery(self) -> None:
        """Discover all recovery families at startup or explicit full repair."""
        for task in _RECOVERY_TASKS:
            self._evidence_version[task] = self._evidence_version.get(task, 0) + 1
        self._requested_tasks.update(_RECOVERY_TASKS)
        self._requested.set()

    def request_order_recovery(
        self,
        orphan_plans: tuple[OrderExecutionPlan, ...] | OrderExecutionPlan = (),
        *,
        plan: OrderExecutionPlan | None = None,
        client_order_id: str | None = None,
        from_account_event: bool = False,
    ) -> None:
        """Repair order/command uncertainty without scanning exits or positions."""
        if isinstance(orphan_plans, OrderExecutionPlan):
            plan = orphan_plans
            orphan_plans = ()
        elif isinstance(orphan_plans, (list, set)):
            orphan_plans = tuple(orphan_plans)

        target_client_id = (
            client_order_id
            if client_order_id is not None
            else (plan.client_order_id if plan is not None else None)
        )

        new_orphans = {
            p.client_order_id: p
            for p in orphan_plans
            if p.client_order_id not in self._orphan_cancels
        }
        self._orphan_cancels.update((p.client_order_id, p) for p in orphan_plans)

        # Distinguish material new evidence from:
        # 1. Unresolved result of the currently executing query (self-triggered)
        # 2. Duplicate evidence / repeated notification of already-known orders
        is_material = bool(new_orphans)

        if not is_material:
            if from_account_event:
                is_material = True
            elif self._active_reconciliation_order is not None:
                if target_client_id == self._active_reconciliation_order:
                    # Callback for the currently in-flight order — the
                    # query's own unresolved result, not material.
                    is_material = False
                elif target_client_id is not None:
                    # A genuinely different order — material new evidence.
                    if target_client_id not in self._known_unresolved_order_ids:
                        is_material = True
                        self._known_unresolved_order_ids.add(target_client_id)
                else:
                    # Generic callback (no specific order) during an active
                    # lookup — suppress to avoid self-triggering.
                    is_material = False
            elif target_client_id is not None:
                if target_client_id not in self._known_unresolved_order_ids:
                    is_material = True
                    self._known_unresolved_order_ids.add(target_client_id)
            elif "orders" not in self._retry_at:
                is_material = True

        if is_material:
            self._evidence_version["orders"] = (
                self._evidence_version.get("orders", 0) + 1
            )
            self._requested_tasks.add("orders")
            self._requested.set()
        elif "orders" not in self._retry_at:
            self._requested_tasks.add("orders")
            self._requested.set()

    def request_exit_recovery(self) -> None:
        """Resume the existing exit owner without restarting order discovery."""
        self._evidence_version["exits"] = self._evidence_version.get("exits", 0) + 1
        self._requested_tasks.add("exits")
        self._requested.set()

    def request_position_recovery(self) -> None:
        """Repair unmanaged exposure without restarting order discovery."""
        self._evidence_version["positions"] = (
            self._evidence_version.get("positions", 0) + 1
        )
        self._requested_tasks.add("positions")
        self._requested.set()

    def notify_account_facts_changed(self) -> None:
        """Resume existing repairs after fact publication; idle facts need no scan."""
        if "exits" in self._retry_at or "exits" in self._running_tasks:
            self._evidence_version["exits"] = self._evidence_version.get("exits", 0) + 1
            self._requested_tasks.add("exits")
        now = self.time_source()
        self._requested_tasks.update(
            task for task, deadline in self._retry_at.items() if deadline <= now
        )
        if self._requested_tasks:
            self._requested.set()

    def recovery_status(self) -> tuple[dict[str, object], ...]:
        """Recovery families: pending/running state, failure class, deadline."""
        now = self.time_source()
        return tuple(
            {
                "family": task,
                "requested": task in self._requested_tasks,
                "running": task in self._running_tasks,
                "last_failure": self._last_failure.get(task),
                "retry_in_seconds": (
                    round(max(0.0, self._retry_at[task] - now), 3)
                    if task in self._retry_at
                    else None
                ),
            }
            for task in _RECOVERY_TASKS
        )

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
                    self.request_order_recovery(
                        plan=persisted.plan, from_account_event=True
                    )
                    return
            if snapshot is None:
                await self.state_machine.mark_reconciliation_pending(
                    persisted.plan,
                )
                self._requested_runs.add(persisted.plan.run_id)
                self.request_order_recovery(
                    plan=persisted.plan, from_account_event=True
                )
            elif snapshot.state.terminal and not persisted.state.terminal:
                # Valid terminal observation takes priority over non-terminal
                # local state, even if exchange timestamp is earlier than local
                # REST reception time (P1-1).
                if snapshot.executed_quantity < persisted.executed_quantity:
                    # Quantity conflict: preserve existing watermark and repair.
                    self._requested_runs.add(persisted.plan.run_id)
                    self._known_unresolved_order_ids.discard(
                        persisted.plan.client_order_id
                    )
                    self.request_order_recovery(
                        plan=persisted.plan, from_account_event=True
                    )
                    return
                await self.state_machine.apply_observed_snapshot(
                    persisted.plan, snapshot
                )
            elif persisted.state.terminal and not snapshot.state.terminal:
                # I1: Terminal state is irreversible. Late non-terminal observation
                # cannot revive it.
                return
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
                    self._record_failed_symbol(plan)
                    log.warning(
                        "live_orphan_cancel_deferred",
                        run_id=self.run_id,
                        client_order_id=plan.client_order_id,
                        error_type=type(error).__name__,
                    )
                    continue
                if result.state.terminal:
                    self._orphan_cancels.pop(plan.client_order_id, None)
                    self._record_verified_symbol(plan)
                else:
                    self._record_failed_symbol(plan)
            pending = bool(self._orphan_cancels)
            if self.execution_book is not None:
                command_pending, command_plans = await recover_restored_commands(
                    book=self.execution_book,
                    coordinator=self.state_machine,
                    orders=self.order_repository,
                )
                pending = command_pending or pending
                plans.update((plan.client_order_id, plan) for plan in command_plans)
                if command_pending:
                    for plan in command_plans:
                        self._record_failed_symbol(plan)
            for run_id in sorted(runs):
                for order in await self.order_repository.load_unresolved_orders(run_id):
                    requires_recovery = (
                        self.execution_book is not None
                        and self.execution_book.command_requires_recovery(
                            order.plan.client_order_id
                        )
                    )
                    if (
                        not include_confirmed
                        and not requires_recovery
                        and order.state
                        in {
                            ExchangeOrderState.ACKNOWLEDGED,
                            ExchangeOrderState.PARTIALLY_FILLED,
                        }
                    ):
                        continue
                    if (
                        self.request_unknown_exit is not None
                        and getattr(order.plan, "reduce_only", False)
                        and order.state
                        is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
                        and self.request_unknown_exit(order)
                    ):
                        self._pass_has_unknown = True
                        self._record_failed_symbol(order.plan)
                        self.request_exit_recovery()
                        continue
                    if order.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION:
                        self._pass_has_unknown = True
                        self._record_failed_symbol(order.plan)
                    pending = True
                    self._requested_runs.add(run_id)
                    plans.setdefault(order.plan.client_order_id, order.plan)
            ordered = list(plans.values())
            ids = list(plans)
            self._known_unresolved_order_ids.update(plans.keys())
            self._known_unresolved_order_ids.update(self._orphan_cancels.keys())
            if self._last_lookup_id in ids:
                start = ids.index(self._last_lookup_id) + 1
                ordered = ordered[start:] + ordered[:start]
            lookup_budget = self.max_order_lookups_per_pass - len(cancel_batch)
            for plan in ordered[:lookup_budget]:
                # Move the cursor before I/O so a failing order cannot starve
                # the remaining commands on the next requested pass.
                self._last_lookup_id = plan.client_order_id
                self._active_reconciliation_order = plan.client_order_id
                try:
                    res = await self.state_machine.reconcile_order(plan)
                    if (
                        res is not None
                        and getattr(res, "state", None)
                        is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
                    ):
                        self._pass_has_unknown = True
                        self._record_failed_symbol(plan)
                    else:
                        self._record_verified_symbol(plan)
                except Exception:
                    self._record_failed_symbol(plan)
                    raise
                finally:
                    self._active_reconciliation_order = None
            if len(ordered) > lookup_budget:
                self._requested_runs.update(runs)
                pending = True
                for plan in ordered[lookup_budget:]:
                    self._record_failed_symbol(plan)
        except BaseException:
            self._requested_runs.update(runs)
            raise
        return pending

    async def run_requested(self) -> None:
        """Run requested families; retry only unfinished work on its own deadline."""

        while True:
            if self._retry_at:
                try:
                    await asyncio.wait_for(
                        self._requested.wait(),
                        timeout=max(
                            0.0, min(self._retry_at.values()) - self.time_source()
                        ),
                    )
                except TimeoutError:
                    pass
            else:
                await self._requested.wait()
            tasks = self._requested_tasks | {
                task
                for task, deadline in self._retry_at.items()
                if deadline <= self.time_source()
            }
            self._requested_tasks.clear()
            self._requested.clear()
            # New requests are retained independently while this pass awaits I/O.
            self._running_tasks = tasks
            executed_tasks: set[str] = set()
            failed_tasks: set[str] = set()
            pending_tasks: set[str] = set()
            self._pass_verified_symbols.clear()
            self._pass_failed_symbols.clear()
            self._pass_has_unknown = False
            pass_has_exception = False
            try:
                for task in _RECOVERY_TASKS:
                    if task not in tasks:
                        continue
                    executed_tasks.add(task)
                    now = self.time_source()
                    deadline = self._retry_at.get(task)
                    if (
                        deadline is not None
                        and now < deadline
                        and self._evidence_version.get(task, 0)
                        <= self._evidence_version_at_retry.get(task, 0)
                    ):
                        continue
                    self._retry_at.pop(task, None)
                    current_evidence_version = self._evidence_version.get(task, 0)
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
                    except TimeoutError:
                        pending = True
                        failed_tasks.add(task)
                        self._last_failure[task] = "timeout"
                        log.warning(
                            "live_recovery_family_timed_out",
                            run_id=self.run_id,
                            recovery_task=task,
                            timeout_seconds=self.family_timeout_seconds,
                        )
                    except Exception as error:
                        pending = True
                        failed_tasks.add(task)
                        pass_has_exception = True
                        self._last_failure[task] = f"error:{type(error).__name__}"
                        log.exception(
                            "live_position_repair_failed"
                            if task == "positions"
                            else "live_order_repair_failed",
                            run_id=self.run_id,
                            recovery_task=task,
                            interval_seconds=self.interval_seconds,
                        )
                    else:
                        self._last_failure.pop(task, None)
                    if pending:
                        pending_tasks.add(task)
                        self._retry_at[task] = (
                            self.time_source() + self.interval_seconds
                        )
                        self._evidence_version_at_retry[task] = current_evidence_version
                    else:
                        self._evidence_version_at_retry.pop(task, None)
                    self._running_tasks.discard(task)
            finally:
                self._running_tasks.clear()
                if self.on_reconcile_pass_completed is not None:
                    pass_success = (
                        not pass_has_exception
                        and not failed_tasks
                        and not pending_tasks
                        and not self._pass_has_unknown
                        and not self._pass_failed_symbols
                        and not self._retry_at
                        and bool(executed_tasks)
                    )
                    pass_result = ReconciliationPassResult(
                        tasks=frozenset(executed_tasks),
                        success=pass_success,
                        verified_symbols=frozenset(
                            self._pass_verified_symbols - self._pass_failed_symbols
                        ),
                        failed_symbols=frozenset(self._pass_failed_symbols),
                        pending_tasks=frozenset(pending_tasks | set(self._retry_at.keys())),
                        failed_tasks=frozenset(failed_tasks),
                        has_unknown=self._pass_has_unknown,
                        has_pending_work=bool(
                            pending_tasks or self._retry_at or self._orphan_cancels
                        ),
                        has_exception=pass_has_exception,
                    )
                    try:
                        self.on_reconcile_pass_completed(pass_result)
                    except TypeError:
                        self.on_reconcile_pass_completed()
                    except Exception as err:
                        log.warning("on_reconcile_pass_completed_error", error=str(err))


__all__ = [
    "DEFAULT_RECONCILE_INTERVAL_SECONDS",
    "LiveOrderReconciliation",
    "ReconciliationPassResult",
]
