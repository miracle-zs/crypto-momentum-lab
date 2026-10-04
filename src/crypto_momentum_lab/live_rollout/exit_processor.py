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
from typing import Literal, Protocol, cast
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
    LiveContextProvider,
    LiveDaemonRuntimeContext,
)
from crypto_momentum_lab.live_rollout.exit_lane import ExitLaneOutcome
from crypto_momentum_lab.live_rollout.exits import (
    LiveExitCancellationRequest,
    LiveExitManager,
    LiveExitOrderRequest,
    LiveExitRequest,
)
from crypto_momentum_lab.live_rollout.position_lifecycle import (
    PositionLifecycleLocks,
    live_symbol_position_key,
)
from crypto_momentum_lab.live_rollout.submission import LiveCandidateSubmission
from crypto_momentum_lab.live_rollout.telemetry import (
    LIVE_LANE_EXIT,
    LiveTelemetrySink,
)

log = structlog.get_logger()

_EXIT_RECOVERY_PREFIX = "live-exit-recovery-"
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
        position_locks: PositionLifecycleLocks | None = None,
        account_label: str | None = None,
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
        self._position_locks = position_locks or PositionLifecycleLocks()
        self._account_label = account_label or config.run_id
        self._request_recovery = request_recovery
        self._requested_recoveries: dict[str, _RequestedExitRecovery] = {}
        self._exit_recovery_attempts: dict[str, int] = {}
        self._exit_recovery_next_attempt_at: dict[str, datetime] = {}

    async def process_state(
        self,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
    ) -> ExitLaneOutcome:
        return await self._position_locks.run(
            live_symbol_position_key(self._account_label, state.symbol),
            lambda: self._process_trigger("state", state, context),
        )

    async def process_closed_candle(
        self,
        event: ClosedCandle15mEvent,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
        latest_quote: RealtimeMarketQuote | None,
    ) -> ExitLaneOutcome:
        return await self._position_locks.run(
            live_symbol_position_key(self._account_label, event.candle.symbol),
            lambda: self._process_trigger(
                "closed_candle", state, context, event=event, quote=latest_quote
            ),
        )

    async def process_grace_timeout(
        self,
        state: MarketState15s,
        now: datetime,
        context: LiveDaemonRuntimeContext,
        latest_quote: RealtimeMarketQuote | None,
    ) -> ExitLaneOutcome:
        return await self._position_locks.run(
            live_symbol_position_key(self._account_label, state.symbol),
            lambda: self._process_trigger(
                "grace_timeout", state, context, now=now, quote=latest_quote
            ),
        )

    async def process_quote(
        self,
        quote: RealtimeMarketQuote,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
    ) -> ExitLaneOutcome:
        return await self._position_locks.run(
            live_symbol_position_key(self._account_label, quote.symbol),
            lambda: self._process_trigger("quote", state, context, quote=quote),
        )

    async def _process_trigger(
        self,
        trigger: Literal["state", "closed_candle", "grace_timeout", "quote"],
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
        *,
        event: ClosedCandle15mEvent | None = None,
        quote: RealtimeMarketQuote | None = None,
        now: datetime | None = None,
    ) -> ExitLaneOutcome:
        manager = self._exit_manager
        if manager is None or not self._is_exit_enabled():
            return ExitLaneOutcome()
        if self._telemetry is not None and trigger in ("state", "closed_candle"):
            await self._telemetry.market_state_received(
                state,
                occurred_at=event.received_at if event is not None else self._clock(),
                lane=LIVE_LANE_EXIT,
            )
        self._schedule_pending_exit_recoveries(state=state, context=context)
        if trigger == "state":
            if not manager.uses_market_state_exit:
                return ExitLaneOutcome()
            requests = await manager.requests_for_state(
                state, context.managed_positions
            )
        elif trigger == "closed_candle" and event is not None:
            requests = await manager.requests_for_closed_candle(
                event.candle,
                context.managed_positions,
                latest_quote=quote,
                received_at=event.received_at,
            )
        elif trigger == "grace_timeout" and now is not None:
            requests = await manager.requests_for_grace_timeout(
                now=now,
                state=state,
                positions=context.managed_positions,
                latest_quote=quote,
            )
        elif trigger == "quote" and quote is not None:
            requests = await manager.requests_for_quote(
                quote, context.managed_positions
            )
        else:
            raise ValueError(f"invalid exit trigger: {trigger}")
        try:
            approved, submitted, failure = await self._process_requests(
                requests,
                state=state,
                context=context,
                invalidate_context=trigger != "closed_candle",
            )
        except Exception as error:
            if (
                trigger == "grace_timeout"
                and order_identity_errors.is_durable_order_identity_conflict(error)
            ):
                manager.note_order_identity_conflict(state.symbol)
            raise
        if event is not None and failure == "candidate_expired":
            failure = f"closed_candle_evaluation_expired:{event.candle.symbol}"
        return ExitLaneOutcome(
            approved_intent_count=approved,
            submitted_order_count=submitted,
            failure=failure,
        )

    def _schedule_pending_exit_recoveries(
        self,
        *,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
    ) -> None:
        """Queue single-order lookups without blocking unrelated exit decisions."""
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
        return None

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
            except Exception as error:
                outcomes.append(
                    (
                        work.state.symbol,
                        ExitLaneOutcome(
                            failure=f"exit_recovery_execution_failed:{type(error).__name__}",
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
        now = self._clock()
        next_attempt_at = self._exit_recovery_next_attempt_at.get(root)
        if next_attempt_at is not None and now < next_attempt_at:
            return None
        # Only the existing repair worker reaches remote inspection.
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

        fresh_context = await self._context_provider(state)

        async def apply_observation() -> OrderExecutionResult | None:
            latest = self._requested_recoveries.get(root)
            if latest is not None and latest.plan != plan:
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

        return await self._position_locks.run(
            live_symbol_position_key(self._account_label, state.symbol),
            apply_observation,
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
            if (
                observed_result.state
                is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
            ):
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

    async def _load_fresh_exit_context(
        self,
        *,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
    ) -> LiveDaemonRuntimeContext:
        self._invalidate_context_cache()
        try:
            refreshed = await self._context_provider(state)
            self._apply_context(refreshed)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            log.warning(
                "exit_context_refresh_deferred",
                symbol=state.symbol,
                error_type=type(error).__name__,
            )
            return context
        return refreshed

    async def _execute_exit_submission(
        self,
        request: LiveExitOrderRequest,
        *,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
        reference_price: Decimal | None,
        require_current_position: bool = False,
    ) -> tuple[
        OrderExecutionResult | None,
        LiveDaemonRuntimeContext,
        str | None,
    ]:
        """Submit the reduce-only candidate once; cache epochs are not gates."""

        refreshed_request = _rebase_exit_request(
            request, context, require_current_position=require_current_position
        )
        if refreshed_request is None:
            return None, context, "exit_position_already_flat"
        request = refreshed_request
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
        if result is not None:
            return result, context, None
        if request.candidate.expires_at <= self._clock():
            return None, context, "candidate_expired"
        return None, context, "exit_submission_not_executed"

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
        return await self._position_locks.run(
            live_symbol_position_key(self._account_label, state.symbol),
            lambda: self._process_requests(
                requests,
                state=state,
                context=context,
                reference_price=reference_price,
                invalidate_context=invalidate_context,
            ),
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
        """Execute exit requests while their account/symbol actor is held."""
        approved = 0
        submitted = 0
        failure: str | None = None
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
                    failure = failure or f"pending_exit_order_recovery:{state.symbol}"
                    continue
                if not cancel_result.state.terminal:
                    failure = failure or "cancel_not_confirmed"
                    continue
                if cancel_result.state is ExchangeOrderState.REJECTED:
                    failure = failure or "cancel_rejected"
                    continue
                remaining = max(
                    Decimal("0"),
                    request.cancel_plan.quantity - cancel_result.executed_quantity,
                )
                if remaining <= 0 and not request.fallback_to_current_position:
                    continue
                # Canceling a recovery order can publish account facts and
                # invalidate the context that produced this fallback. Always
                # rebuild a reduce-only request from the post-cancel position.
                context = await self._load_fresh_exit_context(
                    state=state,
                    context=context,
                )
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
                result, context, context_failure = await self._execute_exit_submission(
                    LiveExitOrderRequest(
                        candidate=fallback_candidate,
                        quantity=fallback_quantity,
                    ),
                    state=state,
                    context=context,
                    reference_price=reference_price,
                    require_current_position=True,
                )
                if context_failure is not None:
                    failure = failure or context_failure
                    continue
                if result is None:
                    failure = failure or "exit_fallback_not_executed"
                    continue
                if invalidate_context:
                    self._invalidate_context_cache()
                approved += 1
                submitted += 1
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
                    failure = failure or f"pending_exit_order_recovery:{state.symbol}"
                    continue
                if result.state is ExchangeOrderState.REJECTED:
                    failure = failure or "grace_timeout_market_close_rejected"
                    continue
                continue
            result, context, context_failure = await self._execute_exit_submission(
                request,
                state=state,
                context=context,
                reference_price=reference_price,
            )
            if context_failure is not None:
                failure = failure or context_failure
                continue
            if result is None:
                failure = failure or "exit_submission_failed"
                continue
            if invalidate_context:
                self._invalidate_context_cache()
            approved += 1
            submitted += 1
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
                failure = failure or f"pending_exit_order_recovery:{state.symbol}"
                continue
        return approved, submitted, failure


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
        submitted_order_count=int(is_new_order),
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
    *,
    allocations: list[dict[str, str]] | None = None,
    projection_version: str | None = None,
) -> OrderIntentCandidate:
    """Keep fallback intent metadata aligned with its final quantity."""

    if not candidate.reduce_only:
        raise ValueError("only reduce-only candidates may be resized here")
    if quantity <= 0:
        raise ValueError("reduce-only candidate quantity must be positive")
    features = dict(candidate.features)
    features["quantity"] = str(quantity)
    if allocations is not None:
        features["exit_allocations"] = cast(JsonValue, allocations)
    if projection_version is not None:
        features["projection_version"] = projection_version
    desired_notional = candidate.desired_notional
    raw_reference_price = features.get("reference_price")
    if desired_notional is not None and isinstance(raw_reference_price, str):
        try:
            reference_price = Decimal(raw_reference_price)
        except ArithmeticError:
            reference_price = None
        if reference_price is not None and reference_price > 0:
            desired_notional = quantity * reference_price
    identity_changed = (
        quantity != Decimal(str(candidate.features.get("quantity", quantity)))
        or allocations is not None
        and allocations != candidate.features.get("exit_allocations")
        or projection_version is not None
        and projection_version != candidate.features.get("projection_version")
    )
    candidate_id = candidate.candidate_id
    signal_id = candidate.signal_id
    if identity_changed:
        projection_identity = projection_version or features.get(
            "projection_version", ""
        )
        allocation_identity = allocations or features.get("exit_allocations", [])
        identity = (
            f"{candidate.signal_id}:quantity:{quantity}:"
            f"projection:{projection_identity}:"
            f"allocations:{allocation_identity}"
        )
        identity_id = uuid5(NAMESPACE_URL, identity)
        candidate_id = f"live-exit-{identity_id}"
        signal_id = f"live-exit-signal-{identity_id}"
    return replace(
        candidate,
        candidate_id=candidate_id,
        signal_id=signal_id,
        desired_notional=desired_notional,
        features=features,
    )


def _rebase_exit_request(
    request: LiveExitOrderRequest,
    context: LiveDaemonRuntimeContext,
    *,
    require_current_position: bool = False,
) -> LiveExitOrderRequest | None:
    """Rebuild a reduce-only quantity/allocation against the latest position."""
    candidate = request.candidate
    managed_positions = getattr(context, "managed_positions", None)
    if not candidate.reduce_only or managed_positions is None:
        return request
    raw_side = candidate.features.get("position_side")
    candidate_side = getattr(candidate.side, "value", candidate.side)
    position = next(
        (
            item
            for item in managed_positions
            if item.symbol == candidate.symbol
            and (
                item.position_side.value == raw_side
                if raw_side is not None
                else getattr(item.side, "value", item.side) == candidate_side
            )
        ),
        None,
    )
    if position is None:
        return None if require_current_position else request

    quantity = min(request.quantity, position.quantity)
    raw_allocations = candidate.features.get("exit_allocations")
    allocations: list[dict[str, str]] | None = None
    if isinstance(raw_allocations, list) and raw_allocations:
        available: dict[str, Decimal] = {
            batch.batch_id: batch.quantity for batch in getattr(position, "batches", ())
        }
        if not available and position.batch_id:
            available[position.batch_id] = position.quantity
        allocations = []
        remaining = quantity
        for allocation in raw_allocations:
            if not isinstance(allocation, dict):
                continue
            batch_id = allocation.get("batch_id")
            raw_quantity = allocation.get("quantity")
            if not isinstance(batch_id, str) or not isinstance(raw_quantity, str):
                continue
            try:
                allocated = Decimal(raw_quantity)
            except ArithmeticError:
                continue
            batch_available = available.get(batch_id, Decimal("0"))
            resized = min(allocated, batch_available, remaining)
            if resized > 0:
                allocations.append({"batch_id": batch_id, "quantity": str(resized)})
                available[batch_id] = batch_available - resized
                remaining -= resized
        quantity -= remaining
    if quantity <= 0:
        return None
    resized_candidate = _resize_reduce_only_candidate(
        candidate,
        quantity,
        allocations=allocations,
        projection_version=getattr(position, "projection_version", None),
    )
    return LiveExitOrderRequest(candidate=resized_candidate, quantity=quantity)


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
