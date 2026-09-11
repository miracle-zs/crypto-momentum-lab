"""Decision and recovery processor for all live reduce-only exit triggers.

The processor owns the shared per-symbol decision lock, recovery backoff, and
request fallback semantics.  The queueing lane and the daemon only provide
events, runtime context, and the injected submission/context adapters.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

import structlog

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.market.models import (
    JsonValue,
    MarketState15s,
    RealtimeMarketQuote,
)
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategySide,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionPort,
)
from crypto_momentum_lab.execution_account.orders.recovery import (
    ExitRecoveryClient,
    ExitRecoveryInspectionUnknownError,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
)
from crypto_momentum_lab.live_rollout.closed_candle_feed import (
    ClosedCandle15mEvent,
)
from crypto_momentum_lab.live_rollout.context import (
    LiveContextProvider,
    LiveDaemonRuntimeContext,
)
from crypto_momentum_lab.live_rollout.exit_lane import ExitLaneOutcome
from crypto_momentum_lab.live_rollout.exits import (
    LiveExitCancellationRequest,
    LiveExitManager,
    LiveExitRequest,
)
from crypto_momentum_lab.live_rollout.submission import LiveCandidateSubmission
from crypto_momentum_lab.live_rollout.telemetry import (
    LIVE_LANE_EXIT,
    LiveTelemetrySink,
)
from crypto_momentum_lab.persistence.postgres.order_repository import (
    PersistedExchangeOrder,
)

log = structlog.get_logger()

_EXIT_RECOVERY_PREFIX = "live-exit-recovery-"
_EXIT_RECOVERY_MAX_ATTEMPTS = 3
_EXIT_RECOVERY_RETRY_DELAYS_SECONDS = (2.0, 5.0, 15.0)


class ExitContextPublisher(Protocol):
    async def __call__(self, context: LiveDaemonRuntimeContext) -> None: ...


@dataclass(frozen=True, slots=True)
class ExitProcessorConfig:
    run_id: str

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("run_id must not be empty")


class LiveExitProcessor:
    """Process every reduce-only exit event through one shared decision seam."""

    def __init__(
        self,
        *,
        config: ExitProcessorConfig,
        exit_manager: LiveExitManager | None,
        exit_recovery_client: ExitRecoveryClient | None,
        state_machine: OrderExecutionPort,
        submission: LiveCandidateSubmission,
        telemetry: LiveTelemetrySink | None,
        clock: Callable[[], datetime],
        is_exit_enabled: Callable[[], bool],
        context_provider: LiveContextProvider,
        sync_pending_entry_plans: Callable[[LiveDaemonRuntimeContext], None],
        publish_managed_position_symbols: ExitContextPublisher,
        invalidate_context_cache: Callable[[], None],
        context_is_current: Callable[[LiveDaemonRuntimeContext], bool],
    ) -> None:
        self._config = config
        self._exit_manager = exit_manager
        self._exit_recovery_client = exit_recovery_client
        self._state_machine = state_machine
        self._submission = submission
        self._telemetry = telemetry
        self._clock = clock
        self._is_exit_enabled = is_exit_enabled
        self._context_provider = context_provider
        self._sync_pending_entry_plans = sync_pending_entry_plans
        self._publish_managed_position_symbols = publish_managed_position_symbols
        self._invalidate_context_cache = invalidate_context_cache
        self._context_is_current = context_is_current
        self._exit_symbol_locks: dict[str, asyncio.Lock] = {}
        self._exit_recovery_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._exit_recovery_attempts: dict[str, int] = {}
        self._exit_recovery_next_attempt_at: dict[str, datetime] = {}

    async def process_state(
        self,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
    ) -> ExitLaneOutcome:
        if self._exit_manager is None or not self._is_exit_enabled():
            return ExitLaneOutcome()
        if self._telemetry is not None:
            await self._telemetry.market_state_received(
                state,
                occurred_at=self._clock(),
                lane=LIVE_LANE_EXIT,
            )
        lock = self._exit_symbol_locks.setdefault(state.symbol, asyncio.Lock())
        async with lock:
            if not self._context_is_current(context):
                return ExitLaneOutcome()
            recovery_outcome = await self._recover_pending_exit_orders(
                state=state,
                context=context,
            )
            if recovery_outcome is not None:
                return recovery_outcome
            if not self._context_is_current(context):
                return ExitLaneOutcome()
            if not self._exit_manager.uses_market_state_exit:
                return ExitLaneOutcome()
            requests = await self._exit_manager.requests_for_state(
                state,
                context.managed_positions,
            )
            approved, submitted, failure = await self._process_requests(
                requests,
                state=state,
                context=context,
            )
        return ExitLaneOutcome(
            approved_intent_count=approved,
            submitted_order_count=submitted,
            failure=failure,
        )

    async def process_closed_candle(
        self,
        event: ClosedCandle15mEvent,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
        latest_quote: RealtimeMarketQuote | None,
    ) -> ExitLaneOutcome:
        if self._exit_manager is None or not self._is_exit_enabled():
            return ExitLaneOutcome()
        if self._telemetry is not None:
            await self._telemetry.market_state_received(
                state,
                occurred_at=event.received_at,
                lane=LIVE_LANE_EXIT,
            )
        lock = self._exit_symbol_locks.setdefault(
            event.candle.symbol,
            asyncio.Lock(),
        )
        async with lock:
            if not self._context_is_current(context):
                return ExitLaneOutcome()
            recovery_outcome = await self._recover_pending_exit_orders(
                state=state,
                context=context,
            )
            if recovery_outcome is not None:
                return recovery_outcome
            if not self._context_is_current(context):
                return ExitLaneOutcome()
            requests = await self._exit_manager.requests_for_closed_candle(
                event.candle,
                context.managed_positions,
                latest_quote=latest_quote,
                received_at=event.received_at,
            )
            approved, submitted, failure = await self._process_requests(
                requests,
                state=state,
                context=context,
                invalidate_context=False,
            )
        return ExitLaneOutcome(
            approved_intent_count=approved,
            submitted_order_count=submitted,
            failure=failure,
        )

    async def process_grace_timeout(
        self,
        state: MarketState15s,
        now: datetime,
        context: LiveDaemonRuntimeContext,
        latest_quote: RealtimeMarketQuote | None,
    ) -> ExitLaneOutcome:
        if self._exit_manager is None or not self._is_exit_enabled():
            return ExitLaneOutcome()
        lock = self._exit_symbol_locks.setdefault(state.symbol, asyncio.Lock())
        async with lock:
            if not self._context_is_current(context):
                return ExitLaneOutcome()
            recovery_outcome = await self._recover_pending_exit_orders(
                state=state,
                context=context,
            )
            if recovery_outcome is not None:
                return recovery_outcome
            if not self._context_is_current(context):
                return ExitLaneOutcome()
            requests = await self._exit_manager.requests_for_grace_timeout(
                now=now,
                state=state,
                positions=context.managed_positions,
                latest_quote=latest_quote,
            )
            approved, submitted, failure = await self._process_requests(
                requests,
                state=state,
                context=context,
            )
        return ExitLaneOutcome(
            approved_intent_count=approved,
            submitted_order_count=submitted,
            failure=failure,
        )

    async def process_quote(
        self,
        quote: RealtimeMarketQuote,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
    ) -> ExitLaneOutcome:
        if self._exit_manager is None or not self._is_exit_enabled():
            return ExitLaneOutcome()
        # Quote, candle, grace, and recovery decisions for one position share
        # the same decision lock.  The execution coordinator still serializes
        # exchange I/O, but it cannot deduplicate different candidate IDs
        # after they have already been generated.
        lock = self._exit_symbol_locks.setdefault(
            quote.symbol,
            asyncio.Lock(),
        )
        async with lock:
            if not self._context_is_current(context):
                return ExitLaneOutcome()
            recovery_outcome = await self._recover_pending_exit_orders(
                state=state,
                context=context,
            )
            if recovery_outcome is not None:
                return recovery_outcome
            if not self._context_is_current(context):
                return ExitLaneOutcome()
            requests = await self._exit_manager.requests_for_quote(
                quote,
                context.managed_positions,
            )
            approved, submitted, failure = await self._process_requests(
                requests,
                state=state,
                context=context,
            )
        return ExitLaneOutcome(
            approved_intent_count=approved,
            submitted_order_count=submitted,
            failure=failure,
        )

    async def _recover_pending_exit_orders(
        self,
        *,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
    ) -> ExitLaneOutcome | None:
        """Act on unknown reduce-only orders before evaluating new exits."""
        if self._exit_recovery_client is None:
            return None
        pending_by_root: dict[str, PersistedExchangeOrder] = {}
        for order in context.unresolved_orders:
            if (
                order.plan.symbol != state.symbol
                or not order.plan.reduce_only
                or order.state
                is not ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
            ):
                continue
            root, attempt = _exit_recovery_identity(order.plan)
            previous = pending_by_root.get(root)
            if previous is None or attempt > _exit_recovery_identity(previous.plan)[1]:
                pending_by_root[root] = order
        has_pending_exit = bool(pending_by_root)
        for order in sorted(
            pending_by_root.values(),
            key=lambda item: (item.updated_at, item.plan.client_order_id),
        ):
            result = await self._recover_unknown_exit(
                plan=order.plan,
                known_executed_quantity=order.executed_quantity,
                state=state,
                context=context,
            )
            if result is not None:
                return _exit_recovery_outcome(
                    original_client_order_id=order.plan.client_order_id,
                    result=result,
                )
        # Even when the inspection is deferred, the original order is still
        # unresolved. Do not let a normal exit evaluation create a second
        # order while the recovery backoff is in effect.
        return ExitLaneOutcome() if has_pending_exit else None

    async def _recover_unknown_exit(
        self,
        *,
        plan: OrderExecutionPlan,
        known_executed_quantity: Decimal,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
        source_candidate: OrderIntentCandidate | None = None,
        reference_price: Decimal | None = None,
        recovery_entry_type: EntryType | None = None,
        recovery_limit_price: Decimal | None = None,
    ) -> OrderExecutionResult | None:
        """Confirm an unknown exit and submit at most one safe next attempt."""
        recovery_client = self._exit_recovery_client
        if recovery_client is None or not plan.reduce_only:
            return None
        root, current_attempt = _exit_recovery_identity(plan)
        current_attempt = max(
            current_attempt,
            self._exit_recovery_attempts.get(root, 0),
        )
        if current_attempt >= _EXIT_RECOVERY_MAX_ATTEMPTS:
            log.error(
                "live_exit_recovery_attempts_exhausted",
                run_id=self._config.run_id,
                symbol=plan.symbol,
                position_side=plan.position_side.value,
                original_client_order_id=root,
                attempts=current_attempt,
            )
            return None
        now = self._clock()
        next_attempt_at = self._exit_recovery_next_attempt_at.get(root)
        if next_attempt_at is not None and now < next_attempt_at:
            return None
        lock_key = (plan.symbol, plan.position_side.value)
        lock = self._exit_recovery_locks.setdefault(lock_key, asyncio.Lock())
        async with lock:
            now = self._clock()
            current_attempt = max(
                current_attempt,
                self._exit_recovery_attempts.get(root, 0),
            )
            next_attempt_at = self._exit_recovery_next_attempt_at.get(root)
            if next_attempt_at is not None and now < next_attempt_at:
                return None
            if current_attempt >= _EXIT_RECOVERY_MAX_ATTEMPTS:
                return None
            try:
                observation = await recovery_client.inspect_exit_order(plan)
            except ExitRecoveryInspectionUnknownError as error:
                self._exit_recovery_next_attempt_at[root] = now + timedelta(
                    seconds=_EXIT_RECOVERY_RETRY_DELAYS_SECONDS[0]
                )
                log.warning(
                    "live_exit_recovery_inspection_deferred",
                    run_id=self._config.run_id,
                    symbol=plan.symbol,
                    client_order_id=plan.client_order_id,
                    error_type=type(error).__name__,
                )
                return None
            except Exception as error:
                # A malformed or incomplete read is still an unknown exchange
                # outcome.  Keep the exit lane alive and retry after the same
                # short backoff instead of making the whole daemon halt.
                self._exit_recovery_next_attempt_at[root] = now + timedelta(
                    seconds=_EXIT_RECOVERY_RETRY_DELAYS_SECONDS[0]
                )
                log.warning(
                    "live_exit_recovery_inspection_deferred",
                    run_id=self._config.run_id,
                    symbol=plan.symbol,
                    client_order_id=plan.client_order_id,
                    error_type=type(error).__name__,
                )
                return None

            observed_result: OrderExecutionResult | None = None
            if observation.order is not None:
                if not observation.order.state.terminal:
                    log.info(
                        "live_exit_recovery_original_order_still_active",
                        run_id=self._config.run_id,
                        symbol=plan.symbol,
                        client_order_id=plan.client_order_id,
                        active_order_client_ids=(
                            list(observation.active_exit_order_client_ids)
                        ),
                    )
                    return None
                observed_result = await self._state_machine.apply_observed_snapshot(
                    plan,
                    observation.order,
                )
                if (
                    observation.order.state is ExchangeOrderState.FILLED
                    and observation.position_quantity <= 0
                ):
                    self._exit_recovery_attempts.pop(root, None)
                    self._exit_recovery_next_attempt_at.pop(root, None)
                    return observed_result
            elif plan.client_order_id not in observation.active_exit_order_client_ids:
                observed_result = await self._state_machine.mark_absent_reconciled(
                    plan,
                    details={
                        "reason": "exit_recovery_original_absent",
                        "recovery": True,
                        "position_quantity": str(observation.position_quantity),
                        "active_exit_order_client_ids": list(
                            observation.active_exit_order_client_ids
                        ),
                        "observed_at": observation.observed_at.isoformat(),
                    },
                )
            if observation.active_exit_order_client_ids:
                log.info(
                    "live_exit_recovery_other_exit_order_active",
                    run_id=self._config.run_id,
                    symbol=plan.symbol,
                    client_order_id=plan.client_order_id,
                    active_order_client_ids=(
                        list(observation.active_exit_order_client_ids)
                    ),
                )
                return observed_result
            if observation.position_quantity <= 0:
                if observed_result is not None:
                    self._exit_recovery_attempts.pop(root, None)
                    self._exit_recovery_next_attempt_at.pop(root, None)
                    return observed_result
                details: dict[str, JsonValue] = {
                    "reason": "exit_recovery_position_flat",
                    "recovery": True,
                    "position_quantity": str(observation.position_quantity),
                    "active_exit_order_client_ids": list(
                        observation.active_exit_order_client_ids
                    ),
                    "observed_at": observation.observed_at.isoformat(),
                }
                resolved = await self._state_machine.mark_absent_reconciled(
                    plan,
                    details=details,
                )
                self._exit_recovery_attempts.pop(root, None)
                self._exit_recovery_next_attempt_at.pop(root, None)
                return resolved

            # The exchange position is the authoritative remaining close
            # quantity.  Do not cap it by the previous order's quantity: that
            # would under-close after a partial fill on an earlier recovery
            # attempt.  The order is reduce-only (or position-side scoped in
            # hedge mode), so an exchange-side quantity check still prevents
            # opening or reversing a position.
            recovery_quantity = observation.position_quantity
            if recovery_quantity <= 0:
                log.error(
                    "live_exit_recovery_position_quantity_inconsistent",
                    run_id=self._config.run_id,
                    symbol=plan.symbol,
                    client_order_id=plan.client_order_id,
                    position_quantity=str(observation.position_quantity),
                    order_quantity=str(plan.quantity),
                    known_executed_quantity=str(known_executed_quantity),
                )
                return observed_result
            recovery_attempt = current_attempt + 1
            recovery_candidate = _build_exit_recovery_candidate(
                plan=plan,
                source_candidate=source_candidate,
                context=context,
                state=state,
                now=now,
                reference_price=reference_price,
                root_client_order_id=root,
                attempt=recovery_attempt,
                quantity=recovery_quantity,
                recovery_entry_type=recovery_entry_type,
                recovery_limit_price=recovery_limit_price,
            )
            if recovery_candidate is None:
                return observed_result
            self._exit_recovery_attempts[root] = recovery_attempt
            delay_index = min(
                recovery_attempt - 1,
                len(_EXIT_RECOVERY_RETRY_DELAYS_SECONDS) - 1,
            )
            self._exit_recovery_next_attempt_at[root] = now + timedelta(
                seconds=_EXIT_RECOVERY_RETRY_DELAYS_SECONDS[delay_index]
            )
            recovery_result = await self._submission.execute(
                recovery_candidate,
                requested_quantity=recovery_quantity,
                state=state,
                context=context,
                reference_price=reference_price,
            )
            if recovery_result is None:
                log.error(
                    "live_exit_recovery_not_submitted",
                    run_id=self._config.run_id,
                    symbol=plan.symbol,
                    client_order_id=plan.client_order_id,
                    recovery_attempt=recovery_attempt,
                )
                return observed_result
            log.warning(
                "live_exit_recovery_submitted",
                run_id=self._config.run_id,
                symbol=plan.symbol,
                original_client_order_id=root,
                recovery_client_order_id=recovery_result.client_order_id,
                recovery_attempt=recovery_attempt,
                order_type=recovery_candidate.entry_type.value,
                quantity=str(recovery_quantity),
                position_quantity=str(observation.position_quantity),
                known_executed_quantity=str(known_executed_quantity),
                outcome=recovery_result.state.value,
            )
            if recovery_result.state is ExchangeOrderState.FILLED:
                self._exit_recovery_next_attempt_at.pop(root, None)
            return recovery_result

    async def process_requests(
        self,
        requests: tuple[LiveExitRequest, ...],
        *,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
        reference_price: Decimal | None = None,
        invalidate_context: bool = True,
    ) -> tuple[int, int, str | None]:
        """Serialize scheduled/manual request batches with normal exits."""
        lock = self._exit_symbol_locks.setdefault(
            state.symbol,
            asyncio.Lock(),
        )
        async with lock:
            return await self._process_requests(
                requests,
                state=state,
                context=context,
                reference_price=reference_price,
                invalidate_context=invalidate_context,
            )

    async def _process_requests(
        self,
        requests: tuple[LiveExitRequest, ...],
        *,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
        reference_price: Decimal | None = None,
        invalidate_context: bool = True,
    ) -> tuple[int, int, str | None]:
        approved = 0
        submitted = 0
        for request in requests:
            if isinstance(request, LiveExitCancellationRequest):
                cancel_result = await self._state_machine.cancel_order(
                    request.cancel_plan
                )
                if invalidate_context:
                    self._invalidate_context_cache()
                if (
                    cancel_result.state
                    is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
                ):
                    if cancel_result.plan is not None:
                        recovery_result = await self._recover_unknown_exit(
                            plan=cancel_result.plan,
                            known_executed_quantity=cancel_result.executed_quantity,
                            source_candidate=request.fallback_candidate,
                            state=state,
                            context=context,
                            reference_price=reference_price,
                            recovery_entry_type=request.fallback_candidate.entry_type,
                            recovery_limit_price=request.fallback_candidate.limit_price,
                        )
                        if (
                            recovery_result is not None
                            and recovery_result.client_order_id
                            != cancel_result.client_order_id
                        ):
                            approved += 1
                            submitted += int(not recovery_result.suppressed)
                    log.warning(
                        "live_cancel_outcome_pending_reconciliation",
                        run_id=self._config.run_id,
                        symbol=request.cancel_plan.symbol,
                        client_order_id=request.cancel_plan.client_order_id,
                    )
                    return approved, submitted, None
                if not cancel_result.state.terminal:
                    return approved, submitted, "cancel_not_confirmed"
                if cancel_result.state is ExchangeOrderState.REJECTED:
                    return approved, submitted, "cancel_rejected"
                if cancel_result.state is ExchangeOrderState.ABSENT_RECONCILED:
                    # The cancel response proved that the old recovery order
                    # is gone.  Refresh the account view before submitting a
                    # market fallback so a late fill cannot make us reuse
                    # the stale planned quantity.
                    self._invalidate_context_cache()
                    context = await self._context_provider(state)
                    self._sync_pending_entry_plans(context)
                    await self._publish_managed_position_symbols(context)
                    if state.symbol in context.pending_position_symbols:
                        symbols = ",".join(
                            sorted(context.pending_position_symbols)
                        )
                        return approved, submitted, (
                            f"pending_live_positions:{symbols}"
                        )
                    if state.symbol in context.unmanaged_position_symbols:
                        symbols = ",".join(
                            sorted(context.unmanaged_position_symbols)
                        )
                        return approved, submitted, (
                            f"unmanaged_live_positions:{symbols}"
                        )
                remaining = max(
                    Decimal("0"),
                    request.cancel_plan.quantity - cancel_result.executed_quantity,
                )
                if remaining <= 0 and not request.fallback_to_current_position:
                    continue
                if request.fallback_to_current_position:
                    # A scheduled flatten must size the fallback from a
                    # freshly loaded position after canceling the recovery
                    # order.  The recovery order may have partially filled or
                    # been filled while the cancel request was in flight.
                    self._invalidate_context_cache()
                    context = await self._context_provider(state)
                    self._sync_pending_entry_plans(context)
                    await self._publish_managed_position_symbols(context)
                    if state.symbol in context.pending_position_symbols:
                        symbols = ",".join(
                            sorted(context.pending_position_symbols)
                        )
                        return approved, submitted, (
                            f"pending_live_positions:{symbols}"
                        )
                    if state.symbol in context.unmanaged_position_symbols:
                        symbols = ",".join(
                            sorted(context.unmanaged_position_symbols)
                        )
                        return approved, submitted, (
                            f"unmanaged_live_positions:{symbols}"
                        )
                current_position_quantity = next(
                    (
                        position.quantity
                        for position in context.managed_positions
                        if position.symbol == request.cancel_plan.symbol
                        and position.position_side
                        is request.cancel_plan.position_side
                    ),
                    Decimal("0"),
                )
                fallback_quantity = min(
                    request.fallback_quantity,
                    current_position_quantity,
                )
                if not request.fallback_to_current_position:
                    fallback_quantity = min(fallback_quantity, remaining)
                if fallback_quantity <= 0:
                    log.info(
                        "live_exit_fallback_skipped_position_flat",
                        run_id=self._config.run_id,
                        symbol=request.cancel_plan.symbol,
                        client_order_id=request.cancel_plan.client_order_id,
                    )
                    continue
                fallback_candidate = _resize_reduce_only_candidate(
                    request.fallback_candidate,
                    fallback_quantity,
                )
                result = await self._submission.execute(
                    fallback_candidate,
                    requested_quantity=fallback_quantity,
                    state=state,
                    context=context,
                    reference_price=reference_price,
                )
                if result is None:
                    continue
                if invalidate_context:
                    self._invalidate_context_cache()
                approved += 1
                submitted += int(not result.suppressed)
                if (
                    result.state
                    is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
                ):
                    if result.plan is not None:
                        recovery_result = await self._recover_unknown_exit(
                            plan=result.plan,
                            known_executed_quantity=result.executed_quantity,
                            source_candidate=fallback_candidate,
                            state=state,
                            context=context,
                            reference_price=reference_price,
                        )
                        if (
                            recovery_result is not None
                            and recovery_result.client_order_id
                            != result.client_order_id
                        ):
                            approved += 1
                            submitted += int(not recovery_result.suppressed)
                    log.warning(
                        "live_exit_outcome_pending_reconciliation",
                        run_id=self._config.run_id,
                        symbol=request.fallback_candidate.symbol,
                        client_order_id=result.client_order_id,
                    )
                    return approved, submitted, None
                if result.state is ExchangeOrderState.REJECTED:
                    return approved, submitted, "grace_timeout_market_close_rejected"
                continue
            result = await self._submission.execute(
                request.candidate,
                requested_quantity=request.quantity,
                state=state,
                context=context,
                reference_price=reference_price,
            )
            if result is None:
                continue
            if invalidate_context:
                self._invalidate_context_cache()
            approved += 1
            submitted += int(not result.suppressed)
            if result.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION:
                if result.plan is not None:
                    recovery_result = await self._recover_unknown_exit(
                        plan=result.plan,
                        known_executed_quantity=result.executed_quantity,
                        source_candidate=request.candidate,
                        state=state,
                        context=context,
                        reference_price=reference_price,
                    )
                    if (
                        recovery_result is not None
                        and recovery_result.client_order_id != result.client_order_id
                    ):
                        approved += 1
                        submitted += int(not recovery_result.suppressed)
                log.warning(
                    "live_exit_outcome_pending_reconciliation",
                    run_id=self._config.run_id,
                    symbol=request.candidate.symbol,
                    client_order_id=result.client_order_id,
                )
                return approved, submitted, None
        return approved, submitted, None


def _exit_recovery_identity(plan: OrderExecutionPlan) -> tuple[str, int]:
    """Return the root client id and attempt encoded in a recovery plan."""
    marker = plan.intent_id
    if marker.startswith(_EXIT_RECOVERY_PREFIX):
        payload = marker[len(_EXIT_RECOVERY_PREFIX) :]
        root, separator, raw_attempt = payload.rpartition("-")
        if separator and root and raw_attempt.isdigit():
            return root, int(raw_attempt)
    return plan.client_order_id, 0


def _exit_recovery_outcome(
    *,
    original_client_order_id: str,
    result: OrderExecutionResult,
) -> ExitLaneOutcome:
    is_new_order = result.client_order_id != original_client_order_id
    return ExitLaneOutcome(
        approved_intent_count=int(is_new_order),
        submitted_order_count=int(is_new_order and not result.suppressed),
    )


def _build_exit_recovery_candidate(
    *,
    plan: OrderExecutionPlan,
    source_candidate: OrderIntentCandidate | None,
    context: LiveDaemonRuntimeContext,
    state: MarketState15s,
    now: datetime,
    reference_price: Decimal | None,
    root_client_order_id: str,
    attempt: int,
    quantity: Decimal,
    recovery_entry_type: EntryType | None = None,
    recovery_limit_price: Decimal | None = None,
) -> OrderIntentCandidate | None:
    order_type = (
        plan.order_type.upper()
        if recovery_entry_type is None
        else recovery_entry_type.value.upper()
    )
    if order_type not in {"MARKET", "LIMIT"}:
        log.error(
            "live_exit_recovery_unsupported_order_type",
            symbol=plan.symbol,
            client_order_id=plan.client_order_id,
            order_type=plan.order_type,
        )
        return None
    resolved_reference_price = (
        reference_price
        or plan.price
        or state.mark_price
        or state.close_price
        or state.midpoint
    )
    if resolved_reference_price is None or resolved_reference_price <= 0:
        log.error(
            "live_exit_recovery_missing_reference_price",
            symbol=plan.symbol,
            client_order_id=plan.client_order_id,
        )
        return None
    limit_price = (
        plan.price if recovery_entry_type is None else recovery_limit_price
    )
    if order_type == "LIMIT" and limit_price is None:
        log.error(
            "live_exit_recovery_missing_limit_price",
            symbol=plan.symbol,
            client_order_id=plan.client_order_id,
        )
        return None
    candidate_id = f"{_EXIT_RECOVERY_PREFIX}{root_client_order_id}-{attempt}"
    signal_id = f"live-exit-recovery-signal-{uuid5(NAMESPACE_URL, candidate_id)}"
    base = source_candidate
    features: dict[str, JsonValue] = (
        {} if base is None else dict(base.features)
    )
    features.update(
        {
            "recovery": True,
            "recovery_of": root_client_order_id,
            "recovery_attempt": attempt,
            "original_client_order_id": plan.client_order_id,
            "original_order_type": plan.order_type.upper(),
            "recovery_order_type": order_type,
            "position_side": plan.position_side.value,
            "quantity": str(quantity),
            "reference_price": str(resolved_reference_price),
            "state_bucket_end": state.bucket_end.astimezone(UTC).isoformat(),
        }
    )
    return OrderIntentCandidate(
        candidate_id=candidate_id,
        signal_id=signal_id,
        run_id=plan.run_id,
        strategy_name=(
            base.strategy_name
            if base is not None
            else context.gate_context.strategy_name
        ),
        strategy_version=base.strategy_version if base is not None else "v0",
        config_hash=(
            base.config_hash
            if base is not None
            else context.gate_context.strategy_config_hash
        ),
        symbol=plan.symbol,
        side=_exit_strategy_side(plan),
        entry_type=EntryType(order_type.lower()),
        limit_price=limit_price if order_type == "LIMIT" else None,
        desired_notional=quantity * resolved_reference_price,
        reduce_only=True,
        expires_at=now + timedelta(seconds=60),
        created_at=now,
        reason=f"exit_recovery_{order_type.lower()}_attempt_{attempt}",
        features=features,
    )


def _resize_reduce_only_candidate(
    candidate: OrderIntentCandidate,
    quantity: Decimal,
) -> OrderIntentCandidate:
    """Keep fallback intent metadata aligned with its final quantity."""

    if not candidate.reduce_only:
        raise ValueError("only reduce-only candidates may be resized here")
    if quantity <= 0:
        raise ValueError("reduce-only candidate quantity must be positive")
    features = dict(candidate.features)
    features["quantity"] = str(quantity)
    desired_notional = candidate.desired_notional
    raw_reference_price = features.get("reference_price")
    if desired_notional is not None and isinstance(raw_reference_price, str):
        try:
            reference_price = Decimal(raw_reference_price)
        except ArithmeticError:
            reference_price = None
        if reference_price is not None and reference_price > 0:
            desired_notional = quantity * reference_price
    return replace(
        candidate,
        desired_notional=desired_notional,
        features=features,
    )


def _exit_strategy_side(plan: OrderExecutionPlan) -> StrategySide:
    if plan.position_side.value == "LONG":
        return StrategySide.LONG
    if plan.position_side.value == "SHORT":
        return StrategySide.SHORT
    if plan.side == "SELL":
        return StrategySide.LONG
    if plan.side == "BUY":
        return StrategySide.SHORT
    raise ValueError(f"unsupported exit side: {plan.side}")



