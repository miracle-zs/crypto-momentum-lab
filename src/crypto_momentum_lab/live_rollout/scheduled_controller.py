"""Wall-clock scheduled-risk controller for live entry and flattening safety.

The controller owns the recurring window state machine: it closes entries,
cancels resting entry orders, requests reduce-only flattening, verifies the
authoritative exchange position, and reopens entries only after verification.
The daemon supplies context, execution, and gate adapters at this seam.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Protocol

import structlog

from crypto_momentum_lab.domain.account import AccountPositionSnapshot
from crypto_momentum_lab.domain.execution import (
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionPort,
)
from crypto_momentum_lab.live_rollout.context import (
    LiveContextProvider,
    LiveDaemonRuntimeContext,
)
from crypto_momentum_lab.live_rollout.exits import (
    LiveExitCancellationRequest,
    LiveExitManager,
    LiveExitRequest,
)
from crypto_momentum_lab.live_rollout.scheduled_risk_window import (
    ScheduledRiskWindowConfig,
    ScheduledRiskWindowPhase,
)

log = structlog.get_logger()


class ScheduledEntryGate(Protocol):
    def __call__(self, blocked: bool, *, reason: str) -> None: ...


class ScheduledContextPublisher(Protocol):
    async def __call__(self, context: LiveDaemonRuntimeContext) -> None: ...


class ScheduledExitRequestProcessor(Protocol):
    async def __call__(
        self,
        requests: tuple[LiveExitRequest, ...],
        *,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
        reference_price: Decimal | None = None,
        invalidate_context: bool = True,
    ) -> tuple[int, int, str | None]: ...


@dataclass(frozen=True, slots=True)
class ScheduledRiskWindowControllerConfig:
    run_id: str
    scheduled_risk_window: ScheduledRiskWindowConfig | None

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("run_id must not be empty")


class ScheduledRiskWindowController:
    """Drive scheduled entry blocking, flattening, and verification."""

    def __init__(
        self,
        *,
        config: ScheduledRiskWindowControllerConfig,
        exit_manager: LiveExitManager | None,
        state_machine: OrderExecutionPort,
        context_provider: LiveContextProvider,
        sync_pending_entry_plans: Callable[[LiveDaemonRuntimeContext], None],
        publish_managed_position_symbols: ScheduledContextPublisher,
        invalidate_context_cache: Callable[[], None],
        process_exit_requests: ScheduledExitRequestProcessor,
        set_entry_blocked: ScheduledEntryGate,
        pending_entry_plans: Callable[
            [], tuple[tuple[OrderExecutionPlan, Decimal], ...]
        ],
        cancel_unfilled_entry_orders: (
            Callable[[tuple[OrderExecutionPlan, ...]], Awaitable[int]] | None
        ),
        fetch_exchange_positions: (
            Callable[[], Awaitable[tuple[AccountPositionSnapshot, ...]]] | None
        ),
        clock: Callable[[], datetime],
    ) -> None:
        self._config = config
        self._exit_manager = exit_manager
        self._state_machine = state_machine
        self._context_provider = context_provider
        self._sync_pending_entry_plans = sync_pending_entry_plans
        self._publish_managed_position_symbols = publish_managed_position_symbols
        self._invalidate_context_cache = invalidate_context_cache
        self._process_exit_requests = process_exit_requests
        self._set_entry_blocked = set_entry_blocked
        self._pending_entry_plans = pending_entry_plans
        self._cancel_unfilled_entry_orders = cancel_unfilled_entry_orders
        self._fetch_exchange_positions = fetch_exchange_positions
        self._clock = clock
        self._latest_market_states: dict[str, MarketState15s] = {}
        self._scheduled_window_lock = asyncio.Lock()
        self._scheduled_window_day: date | None = None
        self._scheduled_entry_blocked = False
        self._scheduled_entry_orders_cancelled = False
        self._scheduled_deadline_entry_orders_cancelled = False
        self._scheduled_flatten_attempt = 0
        self._scheduled_flatten_last_attempt_at: datetime | None = None
        self._scheduled_positions_verified = False
        self._scheduled_last_verification_at: datetime | None = None
        self._scheduled_approved_intent_count = 0
        self._scheduled_submitted_order_count = 0

    def _set_scheduled_entry_blocked(
        self,
        blocked: bool,
        *,
        reason: str,
    ) -> None:
        self._scheduled_entry_blocked = blocked
        self._set_entry_blocked(blocked, reason=reason)

    @property
    def approved_intent_count(self) -> int:
        return self._scheduled_approved_intent_count

    @property
    def submitted_order_count(self) -> int:
        return self._scheduled_submitted_order_count

    def observe_state(self, state: MarketState15s) -> None:
        previous_state = self._latest_market_states.get(state.symbol)
        if previous_state is None or state.bucket_end >= previous_state.bucket_end:
            self._latest_market_states[state.symbol] = state

    async def cancel_all_open_entries(self) -> str | None:
        """Cancel every known and exchange-visible opening order.

        The operation shares the scheduled controller lock so an operator
        command cannot race the recurring risk window's cancellation pass.
        The injected callback remains responsible for coordinator/state-machine
        execution and the final exchange orphan scan.
        """

        async with self._scheduled_window_lock:
            return await self._cancel_scheduled_entry_orders()

    async def request_flatten(
        self,
        *,
        now: datetime | None = None,
    ) -> str | None:
        """Request a one-shot reduce-only flatten through the exit processor."""

        observed_at = self._clock() if now is None else now
        async with self._scheduled_window_lock:
            return await self._submit_scheduled_flatten(
                observed_at,
                force=True,
            )

    async def process(
        self,
        *,
        now: datetime | None = None,
    ) -> str | None:
        """Apply the daily 07:45--10:00 entry and flattening controls.

        This method is public so a supervisor can invoke it independently in
        tests or during a controlled recovery.  The normal live run starts a
        one-second wall-clock task that calls it continuously; it does not
        depend on a market-state bucket arriving at the exact boundary.
        """

        schedule = self._config.scheduled_risk_window
        if schedule is None:
            return None
        observed_at = self._clock() if now is None else now
        local_observed_at = schedule.localize(observed_at)
        phase = schedule.phase(observed_at)
        async with self._scheduled_window_lock:
            new_scheduled_window_day = (
                self._scheduled_window_day != local_observed_at.date()
            )
            self._reset_scheduled_window_day(local_observed_at.date())
            if phase is ScheduledRiskWindowPhase.PRE_WINDOW:
                return None

            # A daemon started after the window cannot tell whether current
            # positions were opened before or after today's flattening window.
            # Treat that already-reopened day as complete instead of replaying
            # the previous window's forced flatten against normal positions.
            if (
                phase is ScheduledRiskWindowPhase.REOPENED
                and new_scheduled_window_day
            ):
                self._scheduled_positions_verified = True
                return None

            if (
                phase is ScheduledRiskWindowPhase.REOPENED
                and self._scheduled_positions_verified
            ):
                if self._scheduled_entry_blocked:
                    self._set_scheduled_entry_blocked(
                        False,
                        reason="scheduled_risk_window_complete",
                    )
                return None

            self._set_scheduled_entry_blocked(
                True,
                reason="scheduled_risk_window",
            )
            if not self._scheduled_entry_orders_cancelled:
                cancellation_failure = (
                    await self._cancel_scheduled_entry_orders()
                )
                if cancellation_failure is not None:
                    return cancellation_failure
                self._scheduled_entry_orders_cancelled = True
                if phase is not ScheduledRiskWindowPhase.FLATTENING:
                    self._scheduled_deadline_entry_orders_cancelled = True

            if (
                phase is ScheduledRiskWindowPhase.DEADLINE
                and not self._scheduled_deadline_entry_orders_cancelled
            ):
                cancellation_failure = (
                    await self._cancel_scheduled_entry_orders()
                )
                if cancellation_failure is not None:
                    return cancellation_failure
                self._scheduled_deadline_entry_orders_cancelled = True

            if phase is ScheduledRiskWindowPhase.FLATTENING:
                return await self._submit_scheduled_flatten(
                    observed_at,
                    force=False,
                )
            if phase is ScheduledRiskWindowPhase.DEADLINE:
                return await self._submit_scheduled_flatten(
                    observed_at,
                    force=True,
                )

            failure: str | None = None
            if not self._scheduled_positions_verified:
                # If the process first sees the window after 07:58, still make
                # one market reduce-only attempt before the authoritative
                # position read.  This keeps a late-started daemon safe while
                # preserving the same idempotent order path.
                if self._scheduled_flatten_attempt == 0:
                    failure = await self._submit_scheduled_flatten(
                        observed_at,
                        force=True,
                    )
                verification_failure, residual = (
                    await self._verify_scheduled_positions(observed_at)
                )
                if verification_failure is not None:
                    failure = failure or verification_failure
                elif residual:
                    # A residual position is both an alert condition and a
                    # reason to issue another reduce-only attempt.  The entry
                    # gate remains blocked until a later verification reads
                    # zero on the exchange.
                    flatten_failure = await self._submit_scheduled_flatten(
                        observed_at,
                        force=True,
                    )
                    failure = failure or flatten_failure

            if (
                phase is ScheduledRiskWindowPhase.REOPENED
                and self._scheduled_positions_verified
            ):
                self._set_scheduled_entry_blocked(
                    False,
                    reason="scheduled_risk_window_complete",
                )
            return failure

    async def run(self) -> None:
        schedule = self._config.scheduled_risk_window
        if schedule is None:
            return
        while True:
            try:
                failure = await self.process()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                # A schedule read or cancellation failure must not terminate
                # the market/exit daemon.  The entry gate remains closed and
                # the next poll retries the control operation.
                log.exception(
                    "live_scheduled_risk_window_failed",
                    run_id=self._config.run_id,
                    error_type=type(error).__name__,
                )
            else:
                if failure is not None:
                    log.error(
                        "live_scheduled_risk_window_action_failed",
                        run_id=self._config.run_id,
                        reason=failure,
                    )
            await asyncio.sleep(schedule.poll_interval_seconds)

    def _reset_scheduled_window_day(self, local_day: date) -> None:
        if self._scheduled_window_day == local_day:
            return
        self._scheduled_window_day = local_day
        self._scheduled_entry_orders_cancelled = False
        self._scheduled_deadline_entry_orders_cancelled = False
        self._scheduled_flatten_attempt = 0
        self._scheduled_flatten_last_attempt_at = None
        self._scheduled_positions_verified = False
        self._scheduled_last_verification_at = None
        self._set_scheduled_entry_blocked(
            False,
            reason="outside_scheduled_risk_window",
        )

    async def _cancel_scheduled_entry_orders(self) -> str | None:
        wait_for_idle = getattr(
            self._state_machine,
            "wait_for_entry_submissions_idle",
            None,
        )
        if callable(wait_for_idle):
            try:
                await wait_for_idle()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                log.error(
                    "live_scheduled_entry_submission_drain_failed",
                    run_id=self._config.run_id,
                    error_type=type(error).__name__,
                )
                return (
                    "scheduled_entry_submission_drain_failed:"
                    f"{type(error).__name__}"
                )
        known_plans: dict[str, OrderExecutionPlan] = {}
        context: LiveDaemonRuntimeContext | None = None
        state = self._latest_scheduled_state()
        if state is not None:
            try:
                self._invalidate_context_cache()
                context = await self._context_provider(state)
                self._sync_pending_entry_plans(context)
            except Exception as error:
                if self._cancel_unfilled_entry_orders is None:
                    return (
                        "scheduled_entry_order_context_failed:"
                        f"{type(error).__name__}"
                    )
                log.warning(
                    "live_scheduled_entry_order_context_unavailable",
                    run_id=self._config.run_id,
                    error_type=type(error).__name__,
                )
        if context is not None:
            for item in context.unresolved_orders:
                if (
                    not item.plan.reduce_only
                    and not item.state.terminal
                ):
                    known_plans[item.plan.client_order_id] = item.plan
            if context.account_snapshot is not None:
                known_ids = set(known_plans)
                unknown_open_entries = tuple(
                    order
                    for order in context.account_snapshot.open_orders
                    if not order.reduce_only
                    and order.client_order_id not in known_ids
                )
                if (
                    unknown_open_entries
                    and self._cancel_unfilled_entry_orders is None
                ):
                    return "scheduled_entry_order_cancellation_unavailable"
        for plan, _executed_quantity in self._pending_entry_plans():
            known_plans.setdefault(plan.client_order_id, plan)

        plans = tuple(
            sorted(
                known_plans.values(),
                key=lambda item: (item.symbol, item.client_order_id),
            )
        )
        try:
            if self._cancel_unfilled_entry_orders is not None:
                cancelled_count = await self._cancel_unfilled_entry_orders(plans)
            else:
                cancelled_count = 0
                for plan in plans:
                    result = await self._state_machine.cancel_order(plan)
                    if not result.state.terminal:
                        return "scheduled_entry_order_cancel_not_confirmed"
                    cancelled_count += 1
        except asyncio.CancelledError:
            raise
        except Exception as error:
            log.error(
                "live_scheduled_entry_order_cancel_failed",
                run_id=self._config.run_id,
                error_type=type(error).__name__,
            )
            return f"scheduled_entry_order_cancel_failed:{type(error).__name__}"
        self._invalidate_context_cache()
        log.info(
            "live_scheduled_entry_orders_cancelled",
            run_id=self._config.run_id,
            known_plan_count=len(plans),
            cancelled_count=cancelled_count,
        )
        return None

    async def _submit_scheduled_flatten(
        self,
        now: datetime,
        *,
        force: bool,
    ) -> str | None:
        schedule = self._config.scheduled_risk_window
        exit_manager = self._exit_manager
        if exit_manager is None:
            return "scheduled_flatten_exit_manager_unavailable"
        if schedule is None and not force:
            return "scheduled_flatten_exit_manager_unavailable"
        last_attempt = self._scheduled_flatten_last_attempt_at
        retry_interval_seconds = (
            0.0 if schedule is None else schedule.retry_interval_seconds
        )
        if (
            not force
            and last_attempt is not None
            and (now - last_attempt).total_seconds()
            < retry_interval_seconds
        ):
            return None
        states = self._latest_scheduled_states()
        if not states:
            log.error(
                "live_scheduled_flatten_market_state_unavailable",
                run_id=self._config.run_id,
            )
            return "scheduled_flatten_market_state_unavailable"

        self._scheduled_flatten_attempt += 1
        attempt = self._scheduled_flatten_attempt
        self._scheduled_flatten_last_attempt_at = now
        total_approved = 0
        total_submitted = 0
        failure: str | None = None
        for state in states:
            try:
                self._invalidate_context_cache()
                context = await self._context_provider(state)
                self._sync_pending_entry_plans(context)
                await self._publish_managed_position_symbols(context)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                failure = (
                    "scheduled_flatten_context_failed:"
                    f"{type(error).__name__}"
                )
                log.error(
                    "live_scheduled_flatten_context_failed",
                    run_id=self._config.run_id,
                    symbol=state.symbol,
                    error_type=type(error).__name__,
                )
                continue
            if state.symbol in context.pending_position_symbols:
                symbols = ",".join(
                    sorted(context.pending_position_symbols)
                )
                failure = f"pending_live_positions:{symbols}"
                log.warning(
                    "live_scheduled_flatten_position_sync_pending",
                    run_id=self._config.run_id,
                    symbols=symbols,
                )
                continue
            if state.symbol in context.unmanaged_position_symbols:
                symbols = ",".join(sorted(context.unmanaged_position_symbols))
                failure = f"unmanaged_live_positions:{symbols}"
                log.error(
                    "live_scheduled_flatten_unmanaged_position",
                    run_id=self._config.run_id,
                    symbols=symbols,
                )
                continue
            positions = tuple(
                position
                for position in context.managed_positions
                if position.symbol == state.symbol
            )
            if not positions:
                continue
            requests = await exit_manager.requests_for_scheduled_flatten(
                positions,
                now=now,
                symbol=state.symbol,
                reference_prices={
                    state.symbol: _scheduled_reference_price(state)
                },
                attempt=attempt,
            )
            if not requests:
                continue
            active_exit_plans = tuple(
                item.plan
                for item in context.unresolved_orders
                if item.plan.symbol == state.symbol
                and item.plan.reduce_only
                and not item.state.terminal
            )
            cancellation_ids = {
                request.cancel_plan.client_order_id
                for request in requests
                if isinstance(request, LiveExitCancellationRequest)
            }
            active_exit_plans_to_cancel = tuple(
                plan
                for plan in active_exit_plans
                if plan.client_order_id not in cancellation_ids
            )
            if active_exit_plans_to_cancel and not force:
                log.warning(
                    "live_scheduled_flatten_waiting_for_active_exit",
                    run_id=self._config.run_id,
                    symbol=state.symbol,
                    client_order_ids=sorted(
                        plan.client_order_id
                        for plan in active_exit_plans_to_cancel
                    ),
                )
                continue
            if active_exit_plans_to_cancel:
                cancel_failure = (
                    await self._cancel_active_scheduled_exit_orders(
                        active_exit_plans_to_cancel
                    )
                )
                if cancel_failure is not None:
                    failure = cancel_failure
                    continue
                self._invalidate_context_cache()
                try:
                    context = await self._context_provider(state)
                    self._sync_pending_entry_plans(context)
                    await self._publish_managed_position_symbols(context)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    failure = (
                        "scheduled_flatten_context_failed:"
                        f"{type(error).__name__}"
                    )
                    continue
                if state.symbol in context.pending_position_symbols:
                    symbols = ",".join(
                        sorted(context.pending_position_symbols)
                    )
                    failure = f"pending_live_positions:{symbols}"
                    continue
                if state.symbol in context.unmanaged_position_symbols:
                    symbols = ",".join(
                        sorted(context.unmanaged_position_symbols)
                    )
                    failure = f"unmanaged_live_positions:{symbols}"
                    continue
                positions = tuple(
                    position
                    for position in context.managed_positions
                    if position.symbol == state.symbol
                )
                if not positions:
                    continue
                requests = await exit_manager.requests_for_scheduled_flatten(
                    positions,
                    now=now,
                    symbol=state.symbol,
                    reference_prices={
                        state.symbol: _scheduled_reference_price(state)
                    },
                    attempt=attempt,
                )
                if not requests:
                    continue
                if any(
                    item.plan.reduce_only
                    and not item.state.terminal
                    and item.plan.symbol == state.symbol
                    for item in context.unresolved_orders
                ):
                    failure = "scheduled_active_exit_cancel_not_confirmed"
                    continue
            try:
                approved, submitted, request_failure = (
                    await self._process_exit_requests(
                        requests,
                        state=state,
                        context=context,
                        reference_price=_scheduled_reference_price(state),
                    )
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                approved = submitted = 0
                request_failure = (
                    "scheduled_flatten_execution_failed:"
                    f"{type(error).__name__}"
                )
                log.error(
                    "live_scheduled_flatten_execution_failed",
                    run_id=self._config.run_id,
                    symbol=state.symbol,
                    error_type=type(error).__name__,
                )
            total_approved += approved
            total_submitted += submitted
            if request_failure is not None:
                failure = request_failure

        self._scheduled_approved_intent_count += total_approved
        self._scheduled_submitted_order_count += total_submitted
        log.info(
            "live_scheduled_flatten_attempted",
            run_id=self._config.run_id,
            attempt=attempt,
            approved_intent_count=total_approved,
            submitted_order_count=total_submitted,
            failure=failure,
        )
        return failure

    async def _cancel_active_scheduled_exit_orders(
        self,
        plans: tuple[OrderExecutionPlan, ...],
    ) -> str | None:
        for plan in plans:
            try:
                result = await self._state_machine.cancel_order(plan)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                log.error(
                    "live_scheduled_active_exit_cancel_failed",
                    run_id=self._config.run_id,
                    symbol=plan.symbol,
                    client_order_id=plan.client_order_id,
                    error_type=type(error).__name__,
                )
                return f"scheduled_active_exit_cancel_failed:{type(error).__name__}"
            if result.state is ExchangeOrderState.REJECTED:
                return "scheduled_active_exit_cancel_rejected"
            if not result.state.terminal:
                log.warning(
                    "live_scheduled_active_exit_cancel_unconfirmed",
                    run_id=self._config.run_id,
                    symbol=plan.symbol,
                    client_order_id=plan.client_order_id,
                    state=result.state.value,
                )
                return "scheduled_active_exit_cancel_not_confirmed"
        return None

    async def _verify_scheduled_positions(
        self,
        now: datetime,
    ) -> tuple[str | None, tuple[AccountPositionSnapshot, ...] | None]:
        schedule = self._config.scheduled_risk_window
        if schedule is None:
            return None, ()
        last_verification = self._scheduled_last_verification_at
        if (
            last_verification is not None
            and (now - last_verification).total_seconds()
            < schedule.verify_retry_interval_seconds
        ):
            return None, None
        self._scheduled_last_verification_at = now
        if self._fetch_exchange_positions is None:
            log.error(
                "live_scheduled_position_verification_unavailable",
                run_id=self._config.run_id,
            )
            return "scheduled_position_verification_unavailable", None
        try:
            positions = tuple(await self._fetch_exchange_positions())
        except asyncio.CancelledError:
            raise
        except Exception as error:
            log.error(
                "live_scheduled_position_verification_failed",
                run_id=self._config.run_id,
                error_type=type(error).__name__,
            )
            return (
                f"scheduled_position_verification_failed:{type(error).__name__}",
                None,
            )
        residual = tuple(
            position
            for position in positions
            if position.position_amt != 0
        )
        if residual:
            log.error(
                "live_scheduled_risk_window_residual_positions",
                run_id=self._config.run_id,
                positions=[
                    {
                        "symbol": position.symbol,
                        "position_side": position.position_side,
                        "position_amt": str(position.position_amt),
                    }
                    for position in residual
                ],
            )
            return None, residual
        self._scheduled_positions_verified = True
        log.info(
            "live_scheduled_risk_window_positions_flat",
            run_id=self._config.run_id,
        )
        return None, ()

    def _latest_scheduled_state(self) -> MarketState15s | None:
        states = self._latest_scheduled_states()
        return states[-1] if states else None

    def _latest_scheduled_states(self) -> tuple[MarketState15s, ...]:
        return tuple(
            sorted(
                self._latest_market_states.values(),
                key=lambda state: (state.bucket_end, state.symbol),
            )
        )


def _scheduled_reference_price(state: MarketState15s) -> Decimal:
    for price in (
        state.mark_price,
        state.last_bid_price,
        state.last_ask_price,
        state.close_price,
    ):
        if price is not None and price > 0:
            return price
    raise ValueError(f"market state has no positive reference price: {state.symbol}")
