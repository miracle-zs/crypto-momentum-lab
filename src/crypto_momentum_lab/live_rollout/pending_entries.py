"""In-memory pending-entry reservation and reconciliation state."""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from datetime import datetime
from decimal import Decimal

import structlog

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

log = structlog.get_logger(__name__)


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
        durable_async_checker: (
            Callable[[str], Awaitable[tuple[bool, Decimal] | None]] | None
        ) = None,
        on_terminal: Callable[[str, Decimal], None] | None = None,
        negative_cache_ttl: float = 5.0,
    ) -> None:
        self._capacity = max(1, capacity)
        self._terminal_orders: dict[str, Decimal] = {}
        self._order_plan_quantities: dict[str, Decimal] = {}
        self._non_terminal_orders: dict[str, float] = {}
        self._negative_cache_ttl = negative_cache_ttl
        self._durable_checker = durable_checker
        self._durable_reader = durable_reader
        self._durable_async_checker = durable_async_checker
        self._terminal_listeners: list[Callable[[str, Decimal], None]] = []
        if on_terminal is not None:
            self._terminal_listeners.append(on_terminal)

    def add_terminal_listener(
        self, listener: Callable[[str, Decimal], None]
    ) -> None:
        self._terminal_listeners.append(listener)

    def record_plan(self, client_order_id: str, quantity: Decimal) -> None:
        if len(self._order_plan_quantities) >= self._capacity:
            oldest = next(iter(self._order_plan_quantities))
            self._order_plan_quantities.pop(oldest, None)
        self._order_plan_quantities[client_order_id] = quantity

    def get_plan_quantity(self, client_order_id: str) -> Decimal | None:
        return self._order_plan_quantities.get(client_order_id)

    def is_terminal(self, client_order_id: str) -> bool:
        if client_order_id in self._terminal_orders:
            return True
        if self._durable_reader is not None:
            res = self._durable_reader(client_order_id)
            if res is not None and res[0]:
                self.record_terminal(client_order_id, res[1])
                return True
        if self._durable_checker is not None and self._durable_checker(
            client_order_id
        ):
            self.record_terminal(client_order_id, Decimal("0"))
            return True
        return False

    async def check_durable_terminal(
        self, client_order_id: str, *, fresh: bool = False
    ) -> tuple[bool, Decimal] | None:
        """Consult durable storage for a terminal fact.

        ``fresh`` skips the short-lived negative cache. A caller that is about
        to act on the identity (a submission that just returned from the
        exchange) must not let a cached non-terminal verdict from an earlier
        observation mask a terminal transition that happened since.
        """
        if client_order_id in self._terminal_orders:
            return True, self._terminal_orders[client_order_id]
        now = time.monotonic()
        if not fresh:
            cached_at = self._non_terminal_orders.get(client_order_id)
            if cached_at is not None:
                if now - cached_at < self._negative_cache_ttl:
                    return None
                self._non_terminal_orders.pop(client_order_id, None)

        if self._durable_async_checker is not None:
            res = await self._durable_async_checker(client_order_id)
            if res is not None and res[0]:
                self._non_terminal_orders.pop(client_order_id, None)
                self.record_terminal(client_order_id, res[1])
                return res
            if len(self._non_terminal_orders) >= self._capacity:
                self._non_terminal_orders.pop(
                    next(iter(self._non_terminal_orders)), None
                )
            self._non_terminal_orders[client_order_id] = now
            return res
        return None

    def get_executed_quantity(self, client_order_id: str) -> Decimal | None:
        if client_order_id in self._terminal_orders:
            return self._terminal_orders[client_order_id]
        if self._durable_reader is not None:
            res = self._durable_reader(client_order_id)
            if res is not None and res[0]:
                return res[1]
        return None

    @property
    def has_async_checker(self) -> bool:
        return self._durable_async_checker is not None

    def has_local_terminal_fact(self, client_order_id: str) -> bool:
        """Whether the bounded in-memory cache still holds this terminal fact."""
        return client_order_id in self._terminal_orders

    def record_terminal(
        self, client_order_id: str, executed: Decimal = Decimal("0")
    ) -> Decimal:
        self._non_terminal_orders.pop(client_order_id, None)
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
        for listener in self._terminal_listeners:
            listener(client_order_id, executed)
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
        durable_reader: Callable[[str], tuple[bool, Decimal] | None] | None = None,
    ) -> None:
        self._clock = clock
        self._pending: dict[
            str,
            tuple[OrderExecutionPlan, Decimal],
        ] = {}
        self._uncertain: set[str] = set()
        # Per-order settlement ledger: client order id -> (plan, terminal
        # executed quantity). Entries stay until the position view counts that
        # quantity, and are matched by identity so a late terminal replay
        # cannot reserve the same fill twice.
        self._settled: dict[str, tuple[OrderExecutionPlan, Decimal]] = {}
        self._tracked_plans: dict[str, OrderExecutionPlan] = {}
        self._exchange_ids: dict[str, str] = {}
        self._counted_entry_quantities: dict[str, Decimal] = {}
        self._guessed_counted_cids: set[str] = set()
        self._settlement_pressure = False
        self._order_plan_quantities: dict[str, Decimal] = {}
        self._terminal_capacity = terminal_capacity
        self._known_terminal_orders: dict[str, Decimal] = {}
        self._external_durable_reader = durable_reader
        if terminal_barrier is not None:
            self._terminal_barrier = terminal_barrier
            if self._terminal_barrier._durable_checker is None:
                if durable_checker is not None:
                    self._terminal_barrier._durable_checker = durable_checker
                elif self._terminal_barrier._durable_reader is None:
                    # Only fall back to the local inference when the barrier has
                    # no durable evidence source of its own; a shared barrier
                    # already answers terminality through its reader.
                    self._terminal_barrier._durable_checker = (
                        self._is_durable_terminal
                    )
            if self._terminal_barrier._durable_reader is None:
                self._terminal_barrier._durable_reader = (
                    durable_reader or self._durable_reader
                )
        else:
            self._terminal_barrier = TerminalOrderBarrier(
                capacity=terminal_capacity,
                durable_checker=durable_checker or self._is_durable_terminal,
                durable_reader=durable_reader or self._durable_reader,
            )
        self._terminal_barrier.add_terminal_listener(self.on_terminal_discovered)

    @property
    def terminal_barrier(self) -> TerminalOrderBarrier:
        return self._terminal_barrier

    def _track_plan(self, plan: OrderExecutionPlan) -> None:
        cid = plan.client_order_id
        if cid in self._tracked_plans:
            self._tracked_plans.pop(cid)
        elif len(self._tracked_plans) >= self._terminal_capacity:
            oldest = next(iter(self._tracked_plans))
            self._tracked_plans.pop(oldest, None)
        self._tracked_plans[cid] = plan

    def _record_settlement(
        self,
        client_order_id: str,
        plan: OrderExecutionPlan,
        executed: Decimal,
    ) -> None:
        """Track a terminal fill until the position view accounts for it.

        The ledger keeps the order identity and its monotonic executed
        quantity -- never an aggregate and never a pre-computed remainder --
        because both the release rule and late-terminal deduplication need to
        match the identity against the entry batches that eventually appear:
        ``sync`` releases the entry once the batch view counts that quantity,
        and replaying a terminal event for an already counted entry derives a
        zero remainder instead of reserving the same fill twice.
        """
        if executed <= 0:
            self._settled.pop(client_order_id, None)
            return
        previous = self._settled.get(client_order_id)
        if previous is not None:
            executed = max(executed, previous[1])
        self._settled[client_order_id] = (plan, executed)
        if (
            len(self._settled) > self._terminal_capacity
            and not self._settlement_pressure
        ):
            # Evidence is never dropped to satisfy the bound: a growing ledger
            # means the position view is not reflecting fills, so surface it and
            # keep reserving until the batches catch up.
            self._settlement_pressure = True
            log.error(
                "pending_entries_settlement_ledger_exceeded",
                entries=len(self._settled),
                capacity=self._terminal_capacity,
            )

    def _record_known_terminal(
        self, client_order_id: str, executed: Decimal
    ) -> None:
        prev = self._known_terminal_orders.get(client_order_id, Decimal("0"))
        new_exec = max(prev, executed)
        if client_order_id in self._known_terminal_orders:
            self._known_terminal_orders.pop(client_order_id)
        elif len(self._known_terminal_orders) >= self._terminal_capacity:
            oldest = next(iter(self._known_terminal_orders))
            self._known_terminal_orders.pop(oldest, None)
        self._known_terminal_orders[client_order_id] = new_exec

    def on_terminal_discovered(
        self, client_order_id: str, executed: Decimal
    ) -> None:
        # Shared components (for example the GTD entry lifecycle) record
        # terminal facts on the same barrier, so the registry's own watermark
        # must be merged here too: a terminal event carrying an older quantity
        # must never lower the settled exposure.
        watermark = self._terminal_watermark(client_order_id, executed)
        self._record_known_terminal(client_order_id, watermark)
        plan_and_qty = self._pending.pop(client_order_id, None)
        self._uncertain.discard(client_order_id)
        # The plan may have been evicted from the bounded pending/tracked
        # caches; an existing settlement still carries it, and dropping the
        # ledger entry here would release exposure we already know about.
        plan = (
            plan_and_qty[0]
            if plan_and_qty is not None
            else self._settled[client_order_id][0]
            if client_order_id in self._settled
            else self._tracked_plans.get(client_order_id)
        )
        term_exec = self._terminal_barrier.get_executed_quantity(client_order_id)
        final_exec = max(watermark, term_exec or Decimal("0"))
        if plan is not None:
            self._record_settlement(client_order_id, plan, final_exec)
        else:
            self._settled.pop(client_order_id, None)

    def mark_uncertain(
        self,
        plan: OrderExecutionPlan,
        executed: Decimal = Decimal("0"),
    ) -> None:
        self._track_plan(plan)
        self._order_plan_quantities[plan.client_order_id] = plan.quantity
        self._terminal_barrier.record_plan(plan.client_order_id, plan.quantity)
        self._pending[plan.client_order_id] = (plan, executed)
        self._uncertain.add(plan.client_order_id)

    def _durable_reader(self, client_order_id: str) -> tuple[bool, Decimal] | None:
        if self._external_durable_reader is not None:
            ext = self._external_durable_reader(client_order_id)
            if ext is not None:
                return ext
        if client_order_id in self._known_terminal_orders:
            return True, self._known_terminal_orders[client_order_id]
        # A counted quantity only proves terminality when it was attributed
        # from evidence. A split between several unproven orders in one batch
        # is a plan-quantity guess and must never become terminal proof.
        if client_order_id in self._guessed_counted_cids:
            return None
        plan_qty = self._plan_quantity(client_order_id)
        if client_order_id in self._counted_entry_quantities:
            qty = self._counted_entry_quantities.get(client_order_id, Decimal("0"))
            if plan_qty is not None and qty >= plan_qty:
                return True, qty
        return None

    def _is_durable_terminal(self, client_order_id: str) -> bool:
        if client_order_id in self._known_terminal_orders:
            return True
        if client_order_id in self._guessed_counted_cids:
            return False
        plan_qty = self._plan_quantity(client_order_id)
        if client_order_id in self._counted_entry_quantities:
            qty = self._counted_entry_quantities.get(client_order_id, Decimal("0"))
            if plan_qty is not None and qty >= plan_qty:
                return True
        return False

    def _record_terminal(self, client_order_id: str, executed: Decimal) -> Decimal:
        return self._terminal_barrier.record_terminal(client_order_id, executed)

    def _terminal_watermark(self, client_order_id: str, executed: Decimal) -> Decimal:
        """Never settle a terminal order below an already observed quantity.

        Cumulative executed quantity is monotonic (I1): a terminal event or a
        stale durable read may report less than a partial fill this registry
        already tracked in ``_pending``. Keeping the higher watermark prevents
        releasing exposure that was really filled.
        """
        watermark = max(
            executed,
            self._terminal_barrier.get_executed_quantity(client_order_id)
            or Decimal("0"),
            self._known_terminal_orders.get(client_order_id, Decimal("0")),
        )
        pending = self._pending.get(client_order_id)
        if pending is not None:
            watermark = max(watermark, pending[1])
        return watermark

    def _settle_terminal(self, client_order_id: str, executed: Decimal) -> Decimal:
        """Record terminal evidence at or above the observed watermark."""
        return self._record_terminal(
            client_order_id,
            self._terminal_watermark(client_order_id, executed),
        )

    async def check_durable_terminal(
        self, client_order_id: str
    ) -> tuple[bool, Decimal] | None:
        """Check whether this order is terminal in durable storage."""
        return await self._terminal_barrier.check_durable_terminal(
            client_order_id
        )

    def get_executed_quantity(self, client_order_id: str) -> Decimal | None:
        """Return the executed quantity if known terminal.

        The barrier and this registry keep bounded local caches of the same
        fact; report the higher one so the answer stays monotonic.
        """
        term_exec = self._terminal_barrier.get_executed_quantity(client_order_id)
        known_exec = self._known_terminal_orders.get(client_order_id)
        if term_exec is None and known_exec is None:
            return None
        return max(term_exec or Decimal("0"), known_exec or Decimal("0"))

    def is_terminal(self, client_order_id: str) -> bool:
        """Check whether this order identity has reached terminal state."""
        return self._terminal_barrier.is_terminal(client_order_id)

    def remember(
        self,
        plan: OrderExecutionPlan,
        result: OrderExecutionResult,
    ) -> None:
        self._track_plan(plan)
        self._order_plan_quantities[plan.client_order_id] = plan.quantity
        self._terminal_barrier.record_plan(plan.client_order_id, plan.quantity)
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
                refined_executed = self._settle_terminal(
                    plan.client_order_id, result.executed_quantity
                )
                self._record_settlement(
                    plan.client_order_id, plan, refined_executed
                )
            return
        if result.state.terminal or result.executed_quantity >= plan.quantity:
            executed = self._settle_terminal(
                plan.client_order_id, result.executed_quantity
            )
            self._pending.pop(plan.client_order_id, None)
            self._uncertain.discard(plan.client_order_id)
            self._record_settlement(plan.client_order_id, plan, executed)
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
        self._track_plan(plan)
        self._order_plan_quantities[plan.client_order_id] = plan.quantity
        self._terminal_barrier.record_plan(plan.client_order_id, plan.quantity)
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
            executed = self._settle_terminal(plan.client_order_id, executed)
            self._pending.pop(plan.client_order_id, None)
            self._uncertain.discard(plan.client_order_id)
            self._record_settlement(plan.client_order_id, plan, executed)
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

        persisted = self._track_context_orders(context)
        counted, guessed, unproven_multi = self._attribute_entry_batches(
            context, persisted
        )
        self._counted_entry_quantities = counted
        self._guessed_counted_cids = guessed
        self._record_attributed_terminals(persisted, unproven_multi)
        self._release_counted_settlements()
        self._reconcile_pending(persisted)
        self._prune_caches(persisted)

    async def refresh_durable_facts(self, context: LiveDaemonRuntimeContext) -> None:
        """Recover evicted terminal facts before the next attribution pass.

        Terminal facts are held in bounded caches, so an order still referenced
        by a live position batch may no longer carry its executed quantity
        locally; attribution would then fall back to the plan quantity and
        mis-assign fills. Read those identities from durable storage first. A
        missing local fact is never treated as evidence on its own.
        """
        if not self._terminal_barrier.has_async_checker:
            return
        client_ids = {
            client_id
            for position in context.managed_positions
            for batch in position.batches
            for client_id in getattr(batch, "entry_client_order_ids", ())
            if isinstance(client_id, str) and client_id
        }
        for client_id in sorted(client_ids):
            if self._has_local_terminal_fact(client_id):
                continue
            try:
                await self.check_durable_terminal(client_id)
            except Exception as error:
                log.warning(
                    "pending_entries_durable_fact_refresh_failed",
                    client_order_id=client_id,
                    error_type=type(error).__name__,
                )

    def _has_local_terminal_fact(self, client_order_id: str) -> bool:
        return (
            client_order_id in self._known_terminal_orders
            or client_order_id in self._settled
            or self._terminal_barrier.has_local_terminal_fact(client_order_id)
        )

    def _track_context_orders(
        self, context: LiveDaemonRuntimeContext
    ) -> dict[str, PersistedExchangeOrder]:
        persisted = {
            item.plan.client_order_id: item
            for item in context.unresolved_orders
            if not item.plan.reduce_only
        }
        for item in context.unresolved_orders:
            self._track_plan(item.plan)
            self._order_plan_quantities[item.plan.client_order_id] = item.plan.quantity
            self._terminal_barrier.record_plan(
                item.plan.client_order_id, item.plan.quantity
            )
        return persisted

    def _get_proven_exec(
        self, persisted: dict[str, PersistedExchangeOrder], cid: str
    ) -> tuple[bool, Decimal]:
        persisted_order = persisted.get(cid)
        if persisted_order is not None and (
            persisted_order.state.terminal
            or persisted_order.executed_quantity >= persisted_order.plan.quantity
        ):
            return True, persisted_order.executed_quantity

        term_exec = self._terminal_barrier.get_executed_quantity(cid)
        known_exec = self._known_terminal_orders.get(cid)
        if term_exec is not None or known_exec is not None:
            return True, max(term_exec or Decimal("0"), known_exec or Decimal("0"))
        if cid in self._settled:
            return True, self._settled[cid][1]
        if self._terminal_barrier.is_terminal(cid):
            return True, Decimal("0")
        if persisted_order is not None and persisted_order.executed_quantity > 0:
            return True, persisted_order.executed_quantity
        if cid in self._pending and self._pending[cid][1] > 0:
            return True, self._pending[cid][1]
        return False, Decimal("0")

    def _plan_quantity(self, cid: str) -> Decimal | None:
        """Planned quantity from whichever bounded store still holds it."""
        plan_qty = self._order_plan_quantities.get(cid)
        if plan_qty is not None:
            return plan_qty
        settled = self._settled.get(cid)
        if settled is not None:
            return settled[0].quantity
        return self._terminal_barrier.get_plan_quantity(cid)

    def _max_order_exec(
        self,
        persisted: dict[str, PersistedExchangeOrder],
        cid: str,
        max_bound: Decimal | None = None,
    ) -> Decimal:
        is_proven, proven = self._get_proven_exec(persisted, cid)
        if is_proven:
            return proven

        plan_qty = self._plan_quantity(cid)
        if plan_qty is not None:
            return min(plan_qty, max_bound) if max_bound is not None else plan_qty
        return max_bound if max_bound is not None else Decimal("0")

    def _allocation_priority(
        self,
        persisted: dict[str, PersistedExchangeOrder],
        cid: str,
        batch_qty: Decimal,
    ) -> tuple[int, str]:
        is_proven, proven = self._get_proven_exec(persisted, cid)
        plan_qty = self._plan_quantity(cid)
        if is_proven and plan_qty is not None and proven < plan_qty:
            return (0, cid)
        if is_proven:
            return (1, cid)
        if cid in self._pending or cid in persisted:
            return (2, cid)
        return (3, cid)

    def _allocate_batch_remainder(
        self,
        persisted: dict[str, PersistedExchangeOrder],
        client_id: str,
        remaining: Decimal | None,
        observed: dict[str, Decimal],
        max_bound: Decimal | None = None,
    ) -> Decimal:
        """Attribute one order out of a batch quantity and record the share."""
        max_exec = self._max_order_exec(persisted, client_id, max_bound)
        already = observed.get(client_id, Decimal("0"))
        needed = max(Decimal("0"), max_exec - already)
        alloc = needed if remaining is None else min(remaining, needed)
        observed[client_id] = already + alloc
        return alloc

    def _attribute_entry_batches(
        self,
        context: LiveDaemonRuntimeContext,
        persisted: dict[str, PersistedExchangeOrder],
    ) -> tuple[dict[str, Decimal], set[str], set[str]]:
        observed_entry_quantities: dict[str, Decimal] = {}
        unproven_multi_batch_cids: set[str] = set()
        guessed_cids: set[str] = set()

        for position in context.managed_positions:
            for batch in position.batches:
                raw_batch_qty = getattr(batch, "quantity", None)
                batch_qty = (
                    Decimal(str(raw_batch_qty))
                    if raw_batch_qty is not None
                    else None
                )
                remaining_batch_qty = batch_qty
                raw_cids = tuple(getattr(batch, "entry_client_order_ids", ()))
                is_multi = len(raw_cids) > 1
                cids = sorted(
                    raw_cids,
                    key=lambda cid: self._allocation_priority(
                        persisted,
                        cid,
                        batch_qty
                        if batch_qty is not None
                        else Decimal("Infinity"),
                    ),
                )
                unproven_cids: list[str] = []
                for client_id in cids:
                    is_proven, _ = self._get_proven_exec(persisted, client_id)
                    if not is_proven:
                        unproven_cids.append(client_id)
                        if is_multi:
                            unproven_multi_batch_cids.add(client_id)
                    alloc = self._allocate_batch_remainder(
                        persisted,
                        client_id,
                        remaining_batch_qty,
                        observed_entry_quantities,
                        batch_qty,
                    )
                    if remaining_batch_qty is not None:
                        remaining_batch_qty = max(
                            Decimal("0"), remaining_batch_qty - alloc
                        )
                if len(unproven_cids) > 1:
                    # Two or more unproven orders in one batch cannot be told
                    # apart: their split is a plan-quantity guess, not evidence.
                    guessed_cids.update(unproven_cids)
        observed_exchange_entries = {
            exchange_id
            for position in context.managed_positions
            for batch in position.batches
            for exchange_id in getattr(batch, "entry_exchange_order_ids", ())
        }
        for client_id, exchange_id in self._exchange_ids.items():
            if exchange_id not in observed_exchange_entries:
                continue
            for position in context.managed_positions:
                for batch in position.batches:
                    if exchange_id not in getattr(
                        batch, "entry_exchange_order_ids", ()
                    ):
                        continue
                    raw_batch_qty = getattr(batch, "quantity", None)
                    remaining: Decimal | None = None
                    if raw_batch_qty is not None:
                        remaining = Decimal(str(raw_batch_qty))
                        for cid in getattr(batch, "entry_client_order_ids", ()):
                            remaining = max(
                                Decimal("0"),
                                remaining - self._max_order_exec(persisted, cid),
                            )
                    self._allocate_batch_remainder(
                        persisted,
                        client_id,
                        remaining,
                        observed_entry_quantities,
                    )
        return observed_entry_quantities, guessed_cids, unproven_multi_batch_cids

    def _record_attributed_terminals(
        self,
        persisted: dict[str, PersistedExchangeOrder],
        unproven_multi_batch_cids: set[str],
    ) -> None:
        for client_order_id, executed_qty in self._counted_entry_quantities.items():
            persisted_order = persisted.get(client_order_id)
            if persisted_order is not None and not (
                persisted_order.state.terminal
                or persisted_order.executed_quantity >= persisted_order.plan.quantity
            ):
                # Working on exchange (e.g. partially filled); NOT terminal!
                continue
            plan_qty = self._plan_quantity(client_order_id)
            if (
                plan_qty is not None
                and executed_qty < plan_qty
                and persisted_order is None
            ):
                # Partial fill alone without terminal confirmation is not terminal
                continue
            if (
                client_order_id in unproven_multi_batch_cids
                and persisted_order is None
                and not self._terminal_barrier.is_terminal(client_order_id)
            ):
                # Unproven residual allocation in multi-order batch must NEVER
                # be written to _record_terminal
                continue
            self._settle_terminal(client_order_id, executed_qty)

    def _release_counted_settlements(self) -> None:
        """Drop a settlement once the batch view counts its executed quantity.

        The ledger carries the executed quantity itself, so this converges on
        the position view alone: evicted barrier/known-terminal caches cannot
        keep an entry alive, and an entry replayed after it was released
        derives a zero remainder instead of reserving the fill twice.
        """
        for client_order_id in tuple(self._settled):
            plan, executed = self._settled[client_order_id]
            counted_qty = self._counted_entry_quantities.get(
                client_order_id, Decimal("0")
            )
            if executed > counted_qty:
                self._settled[client_order_id] = (plan, executed)
            else:
                self._settled.pop(client_order_id, None)
        if self._settlement_pressure and len(self._settled) <= self._terminal_capacity:
            self._settlement_pressure = False

    def _reconcile_pending(
        self, persisted: dict[str, PersistedExchangeOrder]
    ) -> None:
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
                executed = self._settle_terminal(
                    client_order_id, persisted_order.executed_quantity
                )
                self._pending.pop(client_order_id, None)
                self._uncertain.discard(client_order_id)
                self._record_settlement(client_order_id, plan, executed)
            elif persisted_order.executed_quantity > executed_quantity:
                self._pending[client_order_id] = (
                    plan,
                    persisted_order.executed_quantity,
                )
            if order_state_is_uncertain(persisted_order.state):
                self._uncertain.add(client_order_id)
            else:
                self._uncertain.discard(client_order_id)

    def _prune_caches(self, persisted: dict[str, PersistedExchangeOrder]) -> None:
        self._exchange_ids = {
            client_id: exchange_id
            for client_id, exchange_id in self._exchange_ids.items()
            if client_id in self._pending or client_id in self._settled
        }
        active_order_ids = (
            set(self._pending)
            | set(persisted)
            | set(self._counted_entry_quantities)
            | set(self._known_terminal_orders)
        )
        # Settled entries carry their own plan (see ``_plan_quantity``), so the
        # plan-quantity cache stays bounded independently of the ledger.
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
        # Reserve the part the position view has not counted yet; the batch
        # view releases it on the sync that accounts for the fill.
        for cid, (plan, executed_quantity) in self._settled.items():
            uncounted = max(
                Decimal("0"),
                executed_quantity
                - self._counted_entry_quantities.get(cid, Decimal("0")),
            )
            if uncounted <= 0:
                continue
            effective_price = (
                plan.price if plan.price is not None else plan.reference_price
            )
            if effective_price is not None and effective_price > 0:
                reserved_notional += uncounted * effective_price
            reserved_symbols.add(plan.symbol)
        return reserved_notional, frozenset(reserved_symbols)

    def snapshot(self) -> tuple[tuple[OrderExecutionPlan, Decimal], ...]:
        return tuple(self._pending.values())

    def _uncounted_settlements(
        self,
    ) -> dict[str, tuple[OrderExecutionPlan, Decimal]]:
        uncounted: dict[str, tuple[OrderExecutionPlan, Decimal]] = {}
        for cid, (plan, executed) in self._settled.items():
            remainder = max(
                Decimal("0"),
                executed - self._counted_entry_quantities.get(cid, Decimal("0")),
            )
            if remainder > 0:
                uncounted[cid] = (plan, remainder)
        return uncounted

    def admission_snapshot(self) -> tuple[tuple[OrderExecutionPlan, Decimal], ...]:
        """Include settled fills until the authoritative position shows them.

        Cancellation uses snapshot(), never these already terminal orders.
        """
        combined: dict[str, tuple[OrderExecutionPlan, Decimal]] = dict(
            self._uncounted_settlements()
        )
        for cid, (plan, qty) in self._pending.items():
            if cid not in self._counted_entry_quantities:
                combined[cid] = (plan, qty)
        return tuple(combined.values())

    def pending_symbols(self) -> frozenset[str]:
        return frozenset(
            plan.symbol for plan, _quantity in self.admission_snapshot()
        )

    @property
    def settlement_pressure(self) -> bool:
        """Whether the settlement ledger outgrew its soft capacity.

        Evidence is never dropped to satisfy the bound, so an over-capacity
        ledger closes entry admission instead (see
        :meth:`entry_admission_blocked`). The flag clears itself on the sync
        that drains the ledger back to capacity.
        """
        return self._settlement_pressure

    def has_uncertain_entry(self, symbol: str) -> bool:
        """Whether an uncertain in-flight entry blocks new risk for the symbol."""
        return any(
            plan.symbol == symbol and client_id in self._uncertain
            for client_id, (plan, _) in self._pending.items()
        )

    def entry_admission_blocked(self, symbol: str) -> bool:
        """Whether new entry risk must be refused for this symbol.

        This is the gate the live entry path consumes. Besides genuinely
        uncertain in-flight orders it closes whenever the settlement ledger is
        over capacity: admitting more entries would keep producing terminal
        fills the position view has not counted, which is the only way the
        per-order ledger can grow without bound. Entry admission resumes on the
        sync that drains the ledger, so the bound is enforced by construction
        rather than by discarding settlement evidence.
        """
        return self._settlement_pressure or self.has_uncertain_entry(symbol)


__all__ = ["LivePendingEntryRegistry", "TerminalOrderBarrier"]
