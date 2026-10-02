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

import crypto_momentum_lab.live_rollout.order_identity_errors as order_identity_errors
from crypto_momentum_lab.domain.execution.execution_coordinator import (
    ExecutionReadinessError,
)
from crypto_momentum_lab.domain.execution.order_read_models import (
    PersistedExchangeOrder,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderProjectionConflictError,
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
    ExitRecoveryObservation,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
)
from crypto_momentum_lab.live_rollout.closed_candle_feed import (
    ClosedCandle15mEvent,
)
from crypto_momentum_lab.live_rollout.context import (
    LiveContextChangedDuringLoad,
    LiveContextProvider,
    LiveDaemonRuntimeContext,
    exit_position_block_reason,
)
from crypto_momentum_lab.live_rollout.exit_lane import ExitLaneOutcome
from crypto_momentum_lab.live_rollout.exits import (
    LiveExitCancellationRequest,
    LiveExitManager,
    LiveExitOrderRequest,
    LiveExitRequest,
)
from crypto_momentum_lab.live_rollout.submission import LiveCandidateSubmission
from crypto_momentum_lab.live_rollout.telemetry import (
    LIVE_LANE_EXIT,
    LiveTelemetrySink,
)

log = structlog.get_logger()

_EXIT_RECOVERY_PREFIX = "live-exit-recovery-"
_EXIT_RECOVERY_MAX_ATTEMPTS = 3
_EXIT_RECOVERY_RETRY_DELAYS_SECONDS = (2.0, 5.0, 15.0)


class ExitContextPublisher(Protocol):
    def __call__(self, context: LiveDaemonRuntimeContext) -> None: ...


@dataclass(frozen=True, slots=True)
class ExitProcessorConfig:
    run_id: str

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("run_id must not be empty")


@dataclass(frozen=True, slots=True)
class _RequestedExitRecovery:
    plan: OrderExecutionPlan
    known_executed_quantity: Decimal
    state: MarketState15s
    source_candidate: OrderIntentCandidate | None = None
    reference_price: Decimal | None = None
    recovery_entry_type: EntryType | None = None
    recovery_limit_price: Decimal | None = None


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
        apply_context: ExitContextPublisher,
        invalidate_context_cache: Callable[[], None],
        context_is_current: Callable[[LiveDaemonRuntimeContext], bool],
        request_recovery: Callable[[], None] = lambda: None,
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
        self._apply_context = apply_context
        self._invalidate_context_cache = invalidate_context_cache
        self._context_is_current = context_is_current
        self._request_recovery = request_recovery
        self._requested_recoveries: dict[str, _RequestedExitRecovery] = {}
        self._exit_symbol_locks: dict[str, asyncio.Lock] = {}
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
                return ExitLaneOutcome(failure=f"pending_live_context:{state.symbol}")
            recovery_outcome = self._defer_pending_exit_orders(
                state=state,
                context=context,
            )
            if recovery_outcome is not None:
                return recovery_outcome
            if not self._context_is_current(context):
                return ExitLaneOutcome(failure=f"pending_live_context:{state.symbol}")
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
                return ExitLaneOutcome(failure=f"pending_live_context:{state.symbol}")
            recovery_outcome = self._defer_pending_exit_orders(
                state=state,
                context=context,
            )
            if recovery_outcome is not None:
                # Recovery is not evaluation of the official closing event.
                # Revisit it after committed order/account facts refresh context.
                return replace(
                    recovery_outcome,
                    failure=recovery_outcome.failure
                    or (f"pending_exit_order_recovery:{event.candle.symbol}"),
                )
            if not self._context_is_current(context):
                return ExitLaneOutcome(failure=f"pending_live_context:{state.symbol}")
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
                return ExitLaneOutcome(failure=f"pending_live_context:{state.symbol}")
            recovery_outcome = self._defer_pending_exit_orders(
                state=state,
                context=context,
            )
            if recovery_outcome is not None:
                return recovery_outcome
            if not self._context_is_current(context):
                return ExitLaneOutcome(failure=f"pending_live_context:{state.symbol}")
            requests = await self._exit_manager.requests_for_grace_timeout(
                now=now,
                state=state,
                positions=context.managed_positions,
                latest_quote=latest_quote,
            )
            try:
                approved, submitted, failure = await self._process_requests(
                    requests,
                    state=state,
                    context=context,
                )
            except Exception as error:
                if (
                    self._exit_manager is not None
                    and order_identity_errors.is_durable_order_identity_conflict(error)
                ):
                    self._exit_manager.note_order_identity_conflict(state.symbol)
                raise
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
                return ExitLaneOutcome(failure=f"pending_live_context:{state.symbol}")
            recovery_outcome = self._defer_pending_exit_orders(
                state=state,
                context=context,
            )
            if recovery_outcome is not None:
                return recovery_outcome
            if not self._context_is_current(context):
                return ExitLaneOutcome(failure=f"pending_live_context:{state.symbol}")
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

    def _defer_pending_exit_orders(
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
                or order.state is not ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
            ):
                continue
            root, attempt = _exit_recovery_identity(order.plan)
            previous = pending_by_root.get(root)
            if previous is None or attempt > _exit_recovery_identity(previous.plan)[1]:
                pending_by_root[root] = order
        for order in pending_by_root.values():
            self.request_exit_recovery(
                plan=order.plan,
                known_executed_quantity=order.executed_quantity,
                state=state,
            )
        return (
            ExitLaneOutcome(failure=f"pending_exit_order_recovery:{state.symbol}")
            if pending_by_root
            else None
        )

    def request_exit_recovery(
        self,
        *,
        plan: OrderExecutionPlan,
        known_executed_quantity: Decimal,
        state: MarketState15s,
        source_candidate: OrderIntentCandidate | None = None,
        reference_price: Decimal | None = None,
        recovery_entry_type: EntryType | None = None,
        recovery_limit_price: Decimal | None = None,
    ) -> None:
        if self._exit_recovery_client is None:
            return
        root, attempt = _exit_recovery_identity(plan)
        previous = self._requested_recoveries.get(root)
        if previous is not None:
            if _exit_recovery_identity(previous.plan)[1] > attempt:
                return
            if source_candidate is None and previous.plan == plan:
                source_candidate = previous.source_candidate
                recovery_entry_type = previous.recovery_entry_type
                recovery_limit_price = previous.recovery_limit_price
        if previous is None and len(self._requested_recoveries) >= 4096:
            raise RuntimeError("exit recovery request capacity exhausted")
        self._requested_recoveries[root] = _RequestedExitRecovery(
            plan,
            known_executed_quantity,
            state,
            source_candidate,
            reference_price,
            recovery_entry_type,
            recovery_limit_price,
        )
        if previous is None or (
            self._clock()
            >= self._exit_recovery_next_attempt_at.get(root, self._clock())
        ):
            self._request_recovery()

    @property
    def has_pending_recovery(self) -> bool:
        return bool(self._requested_recoveries)

    async def recover_requested_exits(
        self, *, limit: int = 5
    ) -> tuple[tuple[str, ExitLaneOutcome], ...]:
        """Run in the existing repair worker; remote inspection never owns a decision lock."""
        outcomes = []
        for root in tuple(self._requested_recoveries)[:limit]:
            work = self._requested_recoveries[root]
            try:
                context = await self._context_provider(work.state)
                result = await self._recover_unknown_exit(
                    plan=work.plan,
                    known_executed_quantity=work.known_executed_quantity,
                    state=work.state,
                    context=context,
                    source_candidate=work.source_candidate,
                    reference_price=work.reference_price,
                    recovery_entry_type=work.recovery_entry_type,
                    recovery_limit_price=work.recovery_limit_price,
                )
            except LiveContextChangedDuringLoad:
                continue
            except Exception as error:
                outcomes.append(
                    (
                        work.state.symbol,
                        ExitLaneOutcome(
                            failure=f"exit_recovery_execution_failed:{type(error).__name__}",
                            fatal_failure=True,
                        ),
                    )
                )
                continue
            finally:
                # Rotate every inspected root, including failures and stale contexts.
                latest = self._requested_recoveries.pop(root, None)
                if latest is not None:
                    self._requested_recoveries[root] = latest
            if result is None:
                continue
            latest = self._requested_recoveries.get(root)
            if latest is not None and latest.plan == work.plan:
                self._requested_recoveries.pop(root)
            outcomes.append(
                (
                    work.state.symbol,
                    _exit_recovery_outcome(
                        original_client_order_id=work.plan.client_order_id,
                        result=result,
                    ),
                )
            )
            if (
                result.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
                and result.plan is not None
            ):
                self.request_exit_recovery(
                    plan=result.plan,
                    known_executed_quantity=result.executed_quantity,
                    state=work.state,
                )
        return tuple(outcomes)

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
        # Only the existing repair worker reaches remote inspection.
        if not self._context_is_current(context):
            return None
        self._exit_recovery_next_attempt_at[root] = now + timedelta(
            seconds=_EXIT_RECOVERY_RETRY_DELAYS_SECONDS[0]
        )
        try:
            observation = await recovery_client.inspect_exit_order(plan)
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

        if not self._context_is_current(context):
            return None
        fresh_context = await self._context_provider(state)
        if not self._context_is_current(context):
            return None
        lock = self._exit_symbol_locks.setdefault(state.symbol, asyncio.Lock())
        async with lock:
            latest = self._requested_recoveries.get(root)
            if latest is not None and latest.plan != plan:
                return None
            if not self._context_is_current(fresh_context):
                return None
            return await self._apply_exit_recovery_observation(
                plan=plan,
                known_executed_quantity=known_executed_quantity,
                state=state,
                context=fresh_context,
                observation=observation,
                source_candidate=source_candidate,
                reference_price=reference_price,
                recovery_entry_type=recovery_entry_type,
                recovery_limit_price=recovery_limit_price,
                root=root,
                current_attempt=current_attempt,
                now=now,
            )

    async def _apply_exit_recovery_observation(
        self,
        *,
        plan: OrderExecutionPlan,
        known_executed_quantity: Decimal,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
        observation: ExitRecoveryObservation,
        source_candidate: OrderIntentCandidate | None,
        reference_price: Decimal | None,
        recovery_entry_type: EntryType | None,
        recovery_limit_price: Decimal | None,
        root: str,
        current_attempt: int,
        now: datetime,
    ) -> OrderExecutionResult | None:
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
                active_result = await self._state_machine.apply_observed_snapshot(
                    plan,
                    observation.order,
                )
                self._invalidate_context_cache()
                return active_result
            observed_result = await self._state_machine.apply_observed_snapshot(
                plan,
                observation.order,
            )
            self._invalidate_context_cache()
            if observed_result.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION:
                # A terminal exchange status can still lack its fill quote.
                # Keep recovering this identity even if the position snapshot
                # lags; it is not permission to submit a replacement order.
                return observed_result
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
            self._invalidate_context_cache()
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
            self._invalidate_context_cache()
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
        context, failure = await self._refresh_context_if_stale(
            state=state, context=context
        )
        if failure is not None:
            self._exit_recovery_next_attempt_at[root] = now + timedelta(
                seconds=_EXIT_RECOVERY_RETRY_DELAYS_SECONDS[0]
            )
            return None
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
        recovery_quantity = Decimal(str(recovery_candidate.features["quantity"]))
        self._exit_recovery_attempts[root] = recovery_attempt
        delay_index = min(
            recovery_attempt - 1,
            len(_EXIT_RECOVERY_RETRY_DELAYS_SECONDS) - 1,
        )
        self._exit_recovery_next_attempt_at[root] = now + timedelta(
            seconds=_EXIT_RECOVERY_RETRY_DELAYS_SECONDS[delay_index]
        )
        try:
            recovery_result = await self._submission.execute(
                recovery_candidate,
                requested_quantity=recovery_quantity,
                state=state,
                context=context,
                reference_price=reference_price,
            )
        except OrderProjectionConflictError:
            self._invalidate_context_cache()
            self._exit_recovery_attempts[root] = current_attempt
            self._exit_recovery_next_attempt_at[root] = now + timedelta(
                seconds=_EXIT_RECOVERY_RETRY_DELAYS_SECONDS[0]
            )
            # A terminal original receipt does not complete its replacement.
            return None
        except Exception as error:
            if not (
                _is_position_readiness_guard(error) or _is_missing_position_facts(error)
            ):
                raise
            # No POST passed the guard, so this did not consume a retry.
            self._exit_recovery_attempts[root] = current_attempt
            self._exit_recovery_next_attempt_at[root] = now + timedelta(
                seconds=_EXIT_RECOVERY_RETRY_DELAYS_SECONDS[0]
            )
            log.warning(
                "live_exit_recovery_position_not_ready",
                run_id=self._config.run_id,
                symbol=plan.symbol,
                client_order_id=plan.client_order_id,
            )
            return None
        if recovery_result is None:
            self._exit_recovery_attempts[root] = current_attempt
            self._exit_recovery_next_attempt_at[root] = now + timedelta(
                seconds=_EXIT_RECOVERY_RETRY_DELAYS_SECONDS[0]
            )
            log.error(
                "live_exit_recovery_not_submitted",
                run_id=self._config.run_id,
                symbol=plan.symbol,
                client_order_id=plan.client_order_id,
                recovery_attempt=recovery_attempt,
            )
            return None
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

    async def _refresh_context_if_stale(
        self,
        *,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
    ) -> tuple[LiveDaemonRuntimeContext, str | None]:
        """Reload an exit context invalidated by another live lane.

        Account events and exits for other symbols share one provider cache.
        A refresh in a different lane can therefore fence a perfectly valid
        reduce-only candidate between risk approval and submission.  Exits
        are safe to retry against a fresh context; silently dropping them is
        not.
        """

        if self._context_is_current(context):
            return context, None
        return await self._load_fresh_exit_context(state=state, context=context)

    async def _load_fresh_exit_context(
        self,
        *,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
    ) -> tuple[LiveDaemonRuntimeContext, str | None]:
        self._invalidate_context_cache()
        try:
            refreshed = await self._context_provider(state)
            self._apply_context(refreshed)
        except asyncio.CancelledError:
            raise
        except LiveContextChangedDuringLoad:
            return context, f"pending_live_context:{state.symbol}"
        except Exception as error:
            return context, f"exit_context_refresh_failed:{type(error).__name__}"
        failure = exit_position_block_reason(refreshed, state.symbol)
        if failure is not None:
            return refreshed, failure
        if not self._context_is_current(refreshed):
            return refreshed, f"pending_live_context:{state.symbol}"
        return refreshed, None

    async def _execute_exit_submission(
        self,
        request: LiveExitOrderRequest,
        *,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
        reference_price: Decimal | None,
    ) -> tuple[
        OrderExecutionResult | None,
        LiveDaemonRuntimeContext,
        str | None,
    ]:
        """Submit a reduce-only candidate, retrying one stale-context fence."""

        context, failure = await self._refresh_context_if_stale(
            state=state,
            context=context,
        )
        if failure is not None:
            return None, context, failure
        for attempt in range(2):
            try:
                result = await self._submission.execute(
                    request.candidate,
                    requested_quantity=request.quantity,
                    state=state,
                    context=context,
                    reference_price=reference_price,
                )
            except OrderProjectionConflictError:
                # Reevaluate the trigger with fresh batches and quantities.
                # Retrying this candidate would merely reuse its old allocation.
                self._invalidate_context_cache()
                log.info(
                    "live_exit_projection_refresh_required",
                    symbol=state.symbol,
                    candidate_id=request.candidate.candidate_id,
                )
                return None, context, f"pending_live_context:{state.symbol}"
            except Exception as error:
                if _is_position_readiness_guard(error):
                    log.warning(
                        "live_exit_position_not_ready",
                        run_id=self._config.run_id,
                        symbol=request.candidate.symbol,
                        candidate_id=request.candidate.candidate_id,
                    )
                    return None, context, "position_not_ready"
                if _is_missing_position_facts(error):
                    log.error(
                        "live_exit_position_facts_unavailable",
                        run_id=self._config.run_id,
                        symbol=request.candidate.symbol,
                        candidate_id=request.candidate.candidate_id,
                        error=str(error),
                    )
                    return None, context, "position_facts_not_restored"
                if (
                    self._exit_manager is not None
                    and order_identity_errors.is_durable_order_identity_conflict(error)
                ):
                    log.error(
                        "live_exit_order_identity_conflict",
                        run_id=self._config.run_id,
                        symbol=request.candidate.symbol,
                        candidate_id=request.candidate.candidate_id,
                        error_type=type(error).__name__,
                        error=str(error),
                    )
                    self._exit_manager.note_order_identity_conflict(
                        request.candidate.symbol
                    )
                    return None, context, "order_identity_conflict"
                raise
            if result is not None or self._context_is_current(context):
                return result, context, None
            if attempt == 1:
                break
            context, failure = await self._refresh_context_if_stale(
                state=state,
                context=context,
            )
            if failure is not None:
                return None, context, failure
        return None, context, f"pending_live_context:{state.symbol}"

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
                        self.request_exit_recovery(
                            plan=cancel_result.plan,
                            known_executed_quantity=cancel_result.executed_quantity,
                            source_candidate=request.fallback_candidate,
                            state=state,
                            reference_price=reference_price,
                            recovery_entry_type=request.fallback_candidate.entry_type,
                            recovery_limit_price=request.fallback_candidate.limit_price,
                        )
                    log.warning(
                        "live_cancel_outcome_pending_reconciliation",
                        run_id=self._config.run_id,
                        symbol=request.cancel_plan.symbol,
                        client_order_id=request.cancel_plan.client_order_id,
                    )
                    return (
                        approved,
                        submitted,
                        f"pending_exit_order_recovery:{state.symbol}",
                    )
                if not cancel_result.state.terminal:
                    return approved, submitted, "cancel_not_confirmed"
                if cancel_result.state is ExchangeOrderState.REJECTED:
                    return approved, submitted, "cancel_rejected"
                if cancel_result.state is ExchangeOrderState.ABSENT_RECONCILED:
                    # The cancel response proved that the old recovery order
                    # is gone.  Refresh the account view before submitting a
                    # market fallback so a late fill cannot make us reuse
                    # the stale planned quantity.
                    context, failure = await self._load_fresh_exit_context(
                        state=state,
                        context=context,
                    )
                    if failure is not None:
                        return approved, submitted, failure
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
                    context, failure = await self._load_fresh_exit_context(
                        state=state,
                        context=context,
                    )
                    if failure is not None:
                        return approved, submitted, failure
                current_position_quantity = next(
                    (
                        position.quantity
                        for position in context.managed_positions
                        if position.symbol == request.cancel_plan.symbol
                        and position.position_side is request.cancel_plan.position_side
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
                try:
                    result = await self._submission.execute(
                        fallback_candidate,
                        requested_quantity=fallback_quantity,
                        state=state,
                        context=context,
                        reference_price=reference_price,
                    )
                except OrderProjectionConflictError:
                    self._invalidate_context_cache()
                    return approved, submitted, f"pending_live_context:{state.symbol}"
                except Exception as error:
                    if _is_missing_position_facts(error):
                        log.error(
                            "live_exit_position_facts_unavailable",
                            run_id=self._config.run_id,
                            symbol=fallback_candidate.symbol,
                            candidate_id=fallback_candidate.candidate_id,
                            error=str(error),
                        )
                        return approved, submitted, "position_facts_not_restored"
                    if (
                        self._exit_manager is not None
                        and order_identity_errors.is_durable_order_identity_conflict(
                            error
                        )
                    ):
                        log.error(
                            "live_exit_order_identity_conflict",
                            run_id=self._config.run_id,
                            symbol=fallback_candidate.symbol,
                            candidate_id=fallback_candidate.candidate_id,
                            error_type=type(error).__name__,
                            error=str(error),
                        )
                        self._exit_manager.note_order_identity_conflict(
                            fallback_candidate.symbol
                        )
                        return approved, submitted, "order_identity_conflict"
                    raise
                if result is None:
                    continue
                if invalidate_context:
                    self._invalidate_context_cache()
                approved += 1
                submitted += int(not result.suppressed)
                if result.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION:
                    if result.plan is not None:
                        self.request_exit_recovery(
                            plan=result.plan,
                            known_executed_quantity=result.executed_quantity,
                            source_candidate=fallback_candidate,
                            state=state,
                            reference_price=reference_price,
                        )
                    log.warning(
                        "live_exit_outcome_pending_reconciliation",
                        run_id=self._config.run_id,
                        symbol=request.fallback_candidate.symbol,
                        client_order_id=result.client_order_id,
                    )
                    return (
                        approved,
                        submitted,
                        f"pending_exit_order_recovery:{state.symbol}",
                    )
                if result.state is ExchangeOrderState.REJECTED:
                    return approved, submitted, "grace_timeout_market_close_rejected"
                continue
            result, context, context_failure = await self._execute_exit_submission(
                request,
                state=state,
                context=context,
                reference_price=reference_price,
            )
            if context_failure is not None:
                return approved, submitted, context_failure
            if result is None:
                continue
            if invalidate_context:
                self._invalidate_context_cache()
            approved += 1
            submitted += int(not result.suppressed)
            if result.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION:
                if result.plan is not None:
                    self.request_exit_recovery(
                        plan=result.plan,
                        known_executed_quantity=result.executed_quantity,
                        source_candidate=request.candidate,
                        state=state,
                        reference_price=reference_price,
                    )
                log.warning(
                    "live_exit_outcome_pending_reconciliation",
                    run_id=self._config.run_id,
                    symbol=request.candidate.symbol,
                    client_order_id=result.client_order_id,
                )
                return (
                    approved,
                    submitted,
                    f"pending_exit_order_recovery:{state.symbol}",
                )
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
    limit_price = plan.price if recovery_entry_type is None else recovery_limit_price
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
    features: dict[str, JsonValue] = {} if base is None else dict(base.features)
    target_batch_id = plan.batch_id or features.get("batch_id")
    if target_batch_id:
        target = next(
            (
                batch
                for position in context.managed_positions
                if position.symbol == plan.symbol
                and position.position_side == plan.position_side
                for batch in position.batch_views()
                if batch.batch_id == target_batch_id
            ),
            None,
        )
        if target is None:
            # The original batch has closed. New add-ons belong to their own
            # exit decision and must not inherit this recovery request.
            return None
        quantity = min(quantity, target.quantity)
        features["batch_id"] = target.batch_id
        features["projection_version"] = target.projection_version
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


def _is_missing_position_facts(error: Exception) -> bool:
    if "Position facts are not durably restored" in str(error):
        return True
    cause = error.__cause__
    return isinstance(cause, Exception) and _is_missing_position_facts(cause)


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


def _is_position_readiness_guard(error: Exception) -> bool:
    if isinstance(error, ExecutionReadinessError):
        return True
    cause = error.__cause__
    return isinstance(cause, Exception) and _is_position_readiness_guard(cause)
