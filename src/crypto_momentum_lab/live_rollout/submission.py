"""Submission seam shared by the live entry and exit decision lanes.

The submission module owns the safety-critical sequence after a lane has
accepted a candidate:

* re-check entry state and candidate freshness;
* apply fixed live limits and the risk gateway;
* quantize the intent into an exchange plan;
* durably prepare the order before the exchange request;
* let the execution coordinator serialize preparation and submission; and
* update the daemon's pending-entry and limit-order bookkeeping.

The surrounding daemon supplies context and bookkeeping callbacks.  This
keeps the module deep: callers need one ``execute`` operation while the
implementation retains the ordering and fail-closed invariants.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import Protocol, cast

import structlog

from crypto_momentum_lab.domain.execution import OrderExecutionPlan
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.risk import RiskDecision, RiskEvaluation
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionPort,
)
from crypto_momentum_lab.execution_account.orders.quantization import (
    QuantizationRejection,
    SymbolTradingRules,
    quantize_order_plan,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
    PreparedOrderSubmission,
)
from crypto_momentum_lab.live_rollout.context import LiveDaemonRuntimeContext
from crypto_momentum_lab.live_rollout.entry_lane import (
    _prepare_entry_candidate_for_observation,
)
from crypto_momentum_lab.live_rollout.gates import order_state_is_uncertain
from crypto_momentum_lab.live_rollout.limits import (
    FixedLiveLimits,
    LiveLimitContext,
    evaluate_fixed_live_limits,
)
from crypto_momentum_lab.live_rollout.telemetry import (
    LIVE_LANE_ENTRY,
    LIVE_LANE_EXIT,
    LiveTelemetrySink,
)
from crypto_momentum_lab.persistence.postgres.order_repository import (
    PersistedExchangeOrder,
)
from crypto_momentum_lab.risk.gateway import RiskContext, RiskGateway

log = structlog.get_logger()


class LiveSubmissionRepository(Protocol):
    async def save_approved_intent(
        self,
        intent: OrderIntentCandidate,
        evaluation: RiskEvaluation,
    ) -> None: ...

    async def prepare_submission(
        self,
        *,
        intent: OrderIntentCandidate,
        evaluation: RiskEvaluation,
        plan: OrderExecutionPlan,
        prepared_at: datetime,
        environment: str | None = None,
        account_label: str | None = None,
        strategy_name: str | None = None,
        required_lease_owner: str | None = None,
        required_lease_id: str | None = None,
        required_code_generation: str | None = None,
        required_session_id: str | None = None,
        max_open_positions: int | None = None,
        max_daily_loss: Decimal | None = None,
        max_gross_exposure: Decimal | None = None,
        current_daily_pnl: Decimal | None = None,
        current_gross_exposure: Decimal | None = None,
        open_position_symbols: frozenset[str] | None = None,
        exposure_notional: Decimal | None = None,
    ) -> PreparedOrderSubmission | None: ...


class LiveEntryOrderLifecycle(Protocol):
    async def track(
        self,
        plan: OrderExecutionPlan,
        result: OrderExecutionResult,
    ) -> None: ...


class ReduceOnlySignalRecorder(Protocol):
    def __call__(
        self,
        *,
        candidate: OrderIntentCandidate,
        state: MarketState15s,
        recorded_at: datetime,
        context: LiveDaemonRuntimeContext,
    ) -> None: ...


class PendingEntryReservation(Protocol):
    def __call__(
        self,
        persisted_orders: tuple[PersistedExchangeOrder, ...],
    ) -> tuple[Decimal, frozenset[str]]: ...


@dataclass(frozen=True, slots=True)
class LiveSubmissionConfig:
    run_id: str
    resize_tolerance: Decimal
    hedge_mode: bool
    entry_order_type: EntryType
    entry_limit_ttl_seconds: int

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("run_id must not be empty")
        if self.resize_tolerance < 0 or self.resize_tolerance >= 1:
            raise ValueError("resize_tolerance must be in [0, 1)")
        if not isinstance(self.entry_order_type, EntryType):
            raise TypeError("entry_order_type must be an EntryType")
        if self.entry_limit_ttl_seconds < 601:
            raise ValueError("entry_limit_ttl_seconds must be at least 601")


class LiveCandidateSubmission:
    """Execute one accepted candidate through the live safety pipeline."""

    def __init__(
        self,
        *,
        risk_gateway: RiskGateway,
        limits: FixedLiveLimits,
        repository: LiveSubmissionRepository,
        state_machine: OrderExecutionPort,
        config: LiveSubmissionConfig,
        clock: Callable[[], datetime],
        entry_enabled: Callable[[], bool],
        entry_enabled_reason: Callable[[], str],
        context_is_current: Callable[[LiveDaemonRuntimeContext], bool],
        pending_entry_reservation: PendingEntryReservation,
        remember_pending_entry: Callable[
            [OrderExecutionPlan, OrderExecutionResult], None
        ],
        record_signal_candidate: ReduceOnlySignalRecorder,
        telemetry: LiveTelemetrySink | None = None,
        entry_order_lifecycle: LiveEntryOrderLifecycle | None = None,
    ) -> None:
        self._risk_gateway = risk_gateway
        self._limits = limits
        self._repository = repository
        self._state_machine = state_machine
        self._config = config
        self._clock = clock
        self._entry_enabled = entry_enabled
        self._entry_enabled_reason = entry_enabled_reason
        self._context_is_current = context_is_current
        self._pending_entry_reservation = pending_entry_reservation
        self._remember_pending_entry = remember_pending_entry
        self._record_signal_candidate = record_signal_candidate
        self._telemetry = telemetry
        self._entry_order_lifecycle = entry_order_lifecycle

    async def execute(
        self,
        candidate: OrderIntentCandidate,
        *,
        requested_quantity: Decimal | None,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
        reference_price: Decimal | None = None,
    ) -> OrderExecutionResult | None:
        execution_now = self._clock()
        if not candidate.reduce_only and not self._entry_enabled():
            log.info(
                "live_entry_blocked_before_execution",
                run_id=self._config.run_id,
                candidate_id=candidate.candidate_id,
                symbol=candidate.symbol,
                reason=self._entry_enabled_reason(),
            )
            return None
        if candidate.expires_at <= execution_now:
            log.warning(
                "live_candidate_expired_before_execution",
                run_id=self._config.run_id,
                candidate_id=candidate.candidate_id,
                symbol=candidate.symbol,
                candidate_expires_at=candidate.expires_at,
                execution_now=execution_now,
                market_state_bucket_end=state.bucket_end,
            )
            return None
        executable_candidate = _prepare_entry_candidate_for_observation(
            candidate,
            state=state,
            execution_now=execution_now,
            entry_order_type=self._config.entry_order_type,
            limit_ttl_seconds=self._config.entry_limit_ttl_seconds,
        )
        risk_open_position_symbols = context.open_position_symbols or frozenset()
        if not candidate.reduce_only:
            pending_notional, pending_symbols = self._pending_entry_reservation(
                context.unresolved_orders
            )
            risk_open_position_symbols |= pending_symbols
            limit_decision = evaluate_fixed_live_limits(
                self._limits,
                LiveLimitContext(
                    symbol=candidate.symbol,
                    requested_notional=candidate.desired_notional,
                    open_position_symbols=risk_open_position_symbols,
                    realized_pnl=context.realized_pnl,
                    unrealized_pnl=context.unrealized_pnl,
                    gross_exposure=(
                        None
                        if context.gross_exposure is None
                        else context.gross_exposure + pending_notional
                    ),
                    min_notional=_min_notional(
                        context.trading_rules.get(candidate.symbol)
                    ),
                    has_unresolved_order=any(
                        order_state_is_uncertain(item)
                        for item in context.unresolved_order_states
                    ),
                ),
            )
            if not limit_decision.allowed:
                return None
            executable_candidate = replace(
                executable_candidate,
                desired_notional=limit_decision.capped_notional,
            )
        else:
            limit_decision = None
        lane = (
            LIVE_LANE_EXIT if executable_candidate.reduce_only else LIVE_LANE_ENTRY
        )
        if executable_candidate.reduce_only:
            self._record_signal_candidate(
                candidate=executable_candidate,
                state=state,
                recorded_at=self._clock(),
                context=context,
            )
        if self._telemetry is not None:
            await self._telemetry.candidate_accepted(
                executable_candidate,
                state=state,
                occurred_at=self._clock(),
                lane=lane,
            )
        if not self._context_is_current(context):
            log.info(
                "live_candidate_context_invalidated_before_risk",
                run_id=self._config.run_id,
                candidate_id=executable_candidate.candidate_id,
                symbol=executable_candidate.symbol,
            )
            return None
        evaluation = self._risk_gateway.evaluate(
            executable_candidate,
            RiskContext(
                now=context.now,
                active_lease=context.active_lease,
                latest_market_state=state,
                account_state=context.account_state,
                open_position_symbols=risk_open_position_symbols,
                active_halts=context.active_halts,
                risk_config=context.risk_config,
                strategy_state=context.strategy_state,
                enforce_market_state_age=False,
                required_lease_owner=context.gate_context.required_lease_owner,
                required_lease_id=(
                    None
                    if context.active_lease is None
                    else context.active_lease.lease_id
                ),
                required_account_label=context.gate_context.account_label,
                required_strategy_name=context.gate_context.strategy_name,
            ),
        )
        if evaluation.decision is not RiskDecision.APPROVED:
            return None
        if self._telemetry is not None:
            await self._telemetry.risk_approved(
                executable_candidate,
                state=state,
                occurred_at=self._clock(),
                lane=lane,
                evaluation_id=evaluation.evaluation_id,
            )
        if not self._context_is_current(context):
            log.info(
                "live_candidate_context_invalidated_after_risk",
                run_id=self._config.run_id,
                candidate_id=executable_candidate.candidate_id,
                symbol=executable_candidate.symbol,
            )
            return None
        rules = context.trading_rules.get(candidate.symbol)
        execution_reference_price = reference_price
        if execution_reference_price is None:
            candidate_reference_price = executable_candidate.features.get(
                "reference_price"
            )
            if isinstance(candidate_reference_price, str):
                try:
                    execution_reference_price = Decimal(candidate_reference_price)
                except ArithmeticError:
                    execution_reference_price = None
        if execution_reference_price is None:
            execution_reference_price = state.mark_price or state.close_price
        if rules is None or execution_reference_price is None:
            return None
        plan = quantize_order_plan(
            executable_candidate,
            rules,
            reference_price=execution_reference_price,
            resize_tolerance=self._config.resize_tolerance,
            hedge_mode=self._config.hedge_mode,
            requested_quantity=requested_quantity,
        )
        if isinstance(plan, QuantizationRejection):
            return None
        if (
            not executable_candidate.reduce_only
            and self._config.entry_order_type is EntryType.LIMIT
        ):
            plan = replace(
                plan,
                time_in_force="GTD",
                expires_at=executable_candidate.expires_at,
            )
        if not executable_candidate.reduce_only and not self._entry_enabled():
            log.info(
                "live_entry_blocked_before_persistence",
                run_id=self._config.run_id,
                candidate_id=executable_candidate.candidate_id,
                symbol=executable_candidate.symbol,
                reason=self._entry_enabled_reason(),
            )
            return None
        if not self._context_is_current(context):
            log.info(
                "live_candidate_context_invalidated_before_submission",
                run_id=self._config.run_id,
                candidate_id=executable_candidate.candidate_id,
                symbol=executable_candidate.symbol,
            )
            return None
        prepared_submission: PreparedOrderSubmission | None = None
        prepare_submission = getattr(self._repository, "prepare_submission", None)
        prepare_and_execute = getattr(
            self._state_machine,
            "prepare_and_execute",
            None,
        )
        intent_saved_at = self._clock()
        if callable(prepare_submission):

            async def prepare_for_execution() -> PreparedOrderSubmission | None:
                nonlocal prepared_submission, intent_saved_at
                if not executable_candidate.reduce_only and not self._entry_enabled():
                    log.info(
                        "live_entry_blocked_inside_submission_scheduler",
                        run_id=self._config.run_id,
                        candidate_id=executable_candidate.candidate_id,
                        symbol=executable_candidate.symbol,
                        reason=self._entry_enabled_reason(),
                    )
                    return None
                if not self._context_is_current(context):
                    log.info(
                        "live_candidate_context_invalidated_inside_submission_scheduler",
                        run_id=self._config.run_id,
                        candidate_id=executable_candidate.candidate_id,
                        symbol=executable_candidate.symbol,
                    )
                    return None
                prepared_submission = await prepare_submission(
                    intent=executable_candidate,
                    evaluation=evaluation,
                    plan=plan,
                    prepared_at=self._clock(),
                    environment=(
                        None
                        if context.active_lease is None
                        else context.active_lease.environment
                    ),
                    account_label=context.gate_context.account_label,
                    strategy_name=context.gate_context.strategy_name,
                    required_lease_owner=(
                        context.gate_context.required_lease_owner
                    ),
                    required_lease_id=(
                        None
                        if context.active_lease is None
                        else context.active_lease.lease_id
                    ),
                    required_code_generation=(
                        None
                        if context.active_lease is None
                        else context.active_lease.code_generation
                    ),
                    required_session_id=self._config.run_id,
                    max_open_positions=(
                        None
                        if executable_candidate.reduce_only
                        else self._limits.max_open_positions
                    ),
                    max_daily_loss=(
                        None
                        if executable_candidate.reduce_only
                        else self._limits.max_daily_loss
                    ),
                    max_gross_exposure=(
                        None
                        if executable_candidate.reduce_only
                        else self._limits.max_gross_exposure
                    ),
                    current_daily_pnl=(
                        None
                        if (
                            executable_candidate.reduce_only
                            or context.realized_pnl is None
                            or context.unrealized_pnl is None
                        )
                        else context.realized_pnl + context.unrealized_pnl
                    ),
                    current_gross_exposure=(
                        None
                        if executable_candidate.reduce_only
                        else context.gross_exposure
                    ),
                    open_position_symbols=(
                        None
                        if executable_candidate.reduce_only
                        else risk_open_position_symbols
                    ),
                    exposure_notional=(
                        None
                        if limit_decision is None
                        else limit_decision.capped_notional
                    ),
                )
                if prepared_submission is not None:
                    intent_saved_at = prepared_submission.submitting_event.occurred_at
                return prepared_submission

            if callable(prepare_and_execute):
                coordinated_prepare_and_execute = cast(
                    Callable[..., Awaitable[OrderExecutionResult | None]],
                    prepare_and_execute,
                )
                result = await coordinated_prepare_and_execute(
                    plan,
                    prepare_submission=prepare_for_execution,
                )
                if result is None:
                    log.info(
                        "live_duplicate_submission_suppressed",
                        run_id=self._config.run_id,
                        symbol=plan.symbol,
                        client_order_id=plan.client_order_id,
                    )
                    return None
            else:
                prepared_submission = await prepare_for_execution()
                if prepared_submission is None:
                    log.info(
                        "live_duplicate_submission_suppressed",
                        run_id=self._config.run_id,
                        symbol=plan.symbol,
                        client_order_id=plan.client_order_id,
                    )
                    return None
                result = await self._state_machine.execute_approved_intent(
                    plan,
                    prepared_submission=prepared_submission,
                )
        else:
            await self._repository.save_approved_intent(
                executable_candidate,
                evaluation,
            )
            result = await self._state_machine.execute_approved_intent(plan)
        if self._telemetry is not None:
            await self._telemetry.intent_saved(
                executable_candidate,
                state=state,
                occurred_at=intent_saved_at,
                lane=lane,
            )
        if not plan.reduce_only:
            self._remember_pending_entry(plan, result)
            lifecycle = self._entry_order_lifecycle
            if lifecycle is not None:
                try:
                    await lifecycle.track(plan, result)
                except Exception as error:
                    # Expiry bookkeeping is safety/observability support. It
                    # must never turn a successful exchange submission into a
                    # failed live command.
                    log.warning(
                        "live_entry_limit_lifecycle_track_failed",
                        run_id=self._config.run_id,
                        symbol=plan.symbol,
                        client_order_id=plan.client_order_id,
                        error_type=type(error).__name__,
                    )
        return result


def _min_notional(rules: SymbolTradingRules | None) -> Decimal | None:
    return None if rules is None else rules.min_notional
