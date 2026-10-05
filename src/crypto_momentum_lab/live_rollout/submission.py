"""Submission seam shared by the live entry and exit decision lanes.

The submission module owns the safety-critical sequence after a lane has
accepted a candidate:

* re-check entry state and candidate freshness;
* apply fixed live limits and the risk gateway;
* quantize the intent into an exchange plan;
* pass a data-only preparation request to the execution coordinator; and
* update the daemon's pending-entry and limit-order bookkeeping.

The surrounding daemon supplies context and bookkeeping callbacks.  This
keeps the module deep: callers need one ``execute`` operation while the
implementation retains the ordering and fail-closed invariants.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import Protocol

import structlog

from crypto_momentum_lab.domain.execution.order_read_models import (
    PersistedExchangeOrder,
)
from crypto_momentum_lab.domain.execution.order_rules import SymbolTradingRules
from crypto_momentum_lab.domain.execution.order_state import (
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderSubmissionPreparation,
)
from crypto_momentum_lab.domain.execution.position_batches import (
    count_active_symbol_batch_concurrency,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitAllocation,
    ExitAllocationPlan,
    ExitPolicyMode,
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.risk import RiskDecision
from crypto_momentum_lab.domain.risk.limits import (
    LiveLimitContext,
    validate_quantized_notional,
)
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategySide,
)
from crypto_momentum_lab.domain.strategy.entry_candidate import prepare_entry_candidate
from crypto_momentum_lab.execution_account.orders.coordinator import (
    CoordinatedOrderExecutionPort,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
)
from crypto_momentum_lab.execution_account.orders.trade_command_planner import (
    plan_order_execution,
)
from crypto_momentum_lab.live_rollout.context import LiveDaemonRuntimeContext
from crypto_momentum_lab.live_rollout.gates import has_entry_order_conflict
from crypto_momentum_lab.live_rollout.position_lifecycle import (
    PositionLifecycleLocks,
    live_symbol_position_key,
)
from crypto_momentum_lab.live_rollout.telemetry import (
    LIVE_LANE_ENTRY,
    LIVE_LANE_EXIT,
    LiveTelemetrySink,
)
from crypto_momentum_lab.risk.gateway import RiskContext, RiskGateway

log = structlog.get_logger()


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


def resolve_authoritative_reference_price(
    candidate: OrderIntentCandidate,
    state: MarketState15s,
    explicit_reference_price: Decimal | None = None,
) -> tuple[Decimal | None, datetime, str]:
    """Determine once the single authoritative reference price, timestamp, and source.

    Downstream risk and execution planning share the identical price basis
    without secondary implicit fallbacks.
    """
    ts = state.bucket_end
    if explicit_reference_price is not None and explicit_reference_price > 0:
        return explicit_reference_price, ts, "explicit"
    feature_price = candidate.features.get("reference_price")
    if feature_price is not None:
        try:
            parsed = Decimal(str(feature_price))
            if parsed > 0:
                return parsed, ts, "candidate_features"
        except (ArithmeticError, ValueError):
            pass
    market_price = state.mark_price or state.close_price
    if market_price is not None and market_price > 0:
        return market_price, ts, "market_state"
    return None, ts, "unavailable"


@dataclass(frozen=True, slots=True)
class LiveSubmissionConfig:
    run_id: str
    account_label: str
    resize_tolerance: Decimal
    hedge_mode: bool
    entry_order_type: EntryType
    entry_limit_ttl_seconds: int

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("run_id must not be empty")
        if not self.account_label.strip():
            raise ValueError("account_label must not be empty")
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
        state_machine: CoordinatedOrderExecutionPort,
        config: LiveSubmissionConfig,
        clock: Callable[[], datetime],
        pending_entry_reservation: PendingEntryReservation,
        remember_pending_entry: Callable[
            [OrderExecutionPlan, OrderExecutionResult], None
        ],
        record_signal_candidate: ReduceOnlySignalRecorder,
        telemetry: LiveTelemetrySink | None = None,
        entry_order_lifecycle: LiveEntryOrderLifecycle | None = None,
        position_locks: PositionLifecycleLocks,
    ) -> None:
        self._risk_gateway = risk_gateway
        limits = risk_gateway.limits
        if limits is None:
            raise ValueError("live submission requires configured risk limits")
        self._limits = limits
        self._state_machine = state_machine
        self._config = config
        self._clock = clock
        self._pending_entry_reservation = pending_entry_reservation
        self._remember_pending_entry = remember_pending_entry
        self._record_signal_candidate = record_signal_candidate
        self._telemetry = telemetry
        self._entry_order_lifecycle = entry_order_lifecycle
        self._position_locks = position_locks

    async def execute(
        self,
        candidate: OrderIntentCandidate,
        *,
        requested_quantity: Decimal | None,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
        reference_price: Decimal | None = None,
    ) -> OrderExecutionResult | None:
        # Exit orchestration already holds this same lifecycle lock.
        async with (
            nullcontext()
            if candidate.reduce_only
            else self._position_locks.hold(
                live_symbol_position_key(self._config.account_label, candidate.symbol)
            )
        ):
            execution_now = self._clock()
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
            executable_candidate = prepare_entry_candidate(
                candidate,
                state=state,
                execution_now=execution_now,
                entry_order_type=self._config.entry_order_type,
                limit_ttl_seconds=self._config.entry_limit_ttl_seconds,
            )
            risk_open_position_symbols = context.open_position_symbols or frozenset()
            limit_context: LiveLimitContext | None = None
            if not candidate.reduce_only:
                pending_notional, pending_symbols = self._pending_entry_reservation(
                    context.unresolved_orders
                )
                risk_open_position_symbols |= pending_symbols
                managed_positions = context.managed_positions
                unresolved_orders = context.unresolved_orders
                symbol_concurrency = count_active_symbol_batch_concurrency(
                    symbol=candidate.symbol,
                    managed_positions=managed_positions,
                    pending_entry_plans=tuple(
                        order.plan for order in unresolved_orders
                    ),
                )
                limit_context = LiveLimitContext(
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
                    has_unresolved_order=has_entry_order_conflict(
                        candidate.symbol,
                        context.unresolved_orders,
                        context.unresolved_order_states,
                    ),
                    symbol_concurrency=symbol_concurrency,
                )
            risk_context = RiskContext(
                now=context.now,
                open_position_symbols=risk_open_position_symbols,
                active_halts=context.active_halts,
                risk_config=context.risk_config,
                strategy_state=context.strategy_state,
            )
            assessment = self._risk_gateway.evaluate(
                executable_candidate,
                risk_context,
                limit_context=limit_context,
            )
            if assessment.candidate is None:
                return None
            executable_candidate = assessment.candidate
            evaluation = assessment.evaluation
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
            rules = context.trading_rules.get(candidate.symbol)
            execution_reference_price, ref_time, ref_source = (
                resolve_authoritative_reference_price(
                    executable_candidate,
                    state,
                    explicit_reference_price=reference_price,
                )
            )
            if rules is None or execution_reference_price is None:
                return None
            trade_command = self._build_trade_command(
                candidate=executable_candidate,
                reference_price=execution_reference_price,
                requested_quantity=requested_quantity,
            )
            if trade_command is None:
                return None

            execution_result = plan_order_execution(
                trade_command,
                rules,
                run_id=self._config.run_id,
                reference_price=execution_reference_price,
                hedge_mode=self._config.hedge_mode,
            )
            if execution_result.plan is None:
                return None
            plan = replace(
                execution_result.plan,
                strategy_name=executable_candidate.strategy_name,
                strategy_version=executable_candidate.strategy_version,
            )

            actual_notional: Decimal | None = None
            if plan.quantity is not None:
                actual_notional = plan.quantity * execution_reference_price

            if (
                requested_quantity is None
                and executable_candidate.desired_notional is not None
                and executable_candidate.desired_notional > 0
                and actual_notional is not None
            ):
                resize_fraction = (
                    executable_candidate.desired_notional - actual_notional
                ).copy_abs() / executable_candidate.desired_notional
                if resize_fraction > self._config.resize_tolerance:
                    return None

            # Re-verify hard risk limits on actual quantized notional for non-reduce_only entries via RiskGateway
            if not executable_candidate.reduce_only and actual_notional is not None:
                allowed, ceiling_reason = (
                    validate_quantized_notional(
                        actual_notional,
                        gross_exposure=(
                            limit_context.gross_exposure
                            if limit_context is not None
                            else None
                        ),
                        approved_notional=executable_candidate.desired_notional,
                        max_order_notional=risk_context.risk_config.max_order_notional,
                        max_gross_notional=(
                            risk_context.risk_config.max_gross_notional
                        ),
                    )
                )
                if not allowed:
                    log.info(
                        "live_entry_blocked_by_quantized_ceiling",
                        run_id=self._config.run_id,
                        candidate_id=executable_candidate.candidate_id,
                        symbol=executable_candidate.symbol,
                        actual_notional=str(actual_notional),
                        reason=ceiling_reason,
                    )
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
            result = await self._state_machine.prepare_and_execute(
                plan,
                preparation=OrderSubmissionPreparation(
                    intent=executable_candidate,
                    evaluation=evaluation,
                    environment="live",
                    account_label=context.gate_context.account_label,
                    strategy_name=context.gate_context.strategy_name,
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
                        None if executable_candidate.reduce_only else actual_notional
                    ),
                    baseline_observed_at=context.account_observed_at,
                ),
            )
            if result is None:
                log.info(
                    "live_submission_suppressed",
                    run_id=self._config.run_id,
                    symbol=plan.symbol,
                    client_order_id=plan.client_order_id,
                )
                return None
            if self._telemetry is not None:
                await self._telemetry.intent_saved(
                    executable_candidate,
                    state=state,
                    occurred_at=result.prepared_at or self._clock(),
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

    def _build_trade_command(
        self,
        *,
        candidate: OrderIntentCandidate,
        reference_price: Decimal,
        requested_quantity: Decimal | None,
    ) -> TradeCommand | None:
        raw_position_side = candidate.features.get("position_side")
        if isinstance(raw_position_side, str) and raw_position_side.strip():
            position_side = FuturesPositionSide(raw_position_side.strip().upper())
        elif self._config.hedge_mode:
            position_side = (
                FuturesPositionSide.LONG
                if candidate.side is StrategySide.LONG
                else FuturesPositionSide.SHORT
            )
        else:
            position_side = FuturesPositionSide.BOTH

        account_label = self._config.account_label
        position_key = PositionKey(
            environment="live",
            account_label=account_label,
            symbol=candidate.symbol,
            position_side=position_side,
        )

        sizing_price = (
            candidate.limit_price
            if candidate.limit_price is not None and candidate.limit_price > 0
            else reference_price
        )
        if requested_quantity is None:
            raw_quantized = candidate.features.get("quantized_quantity")
            if raw_quantized is not None:
                req_qty = Decimal(str(raw_quantized))
            elif candidate.desired_notional is None or sizing_price <= 0:
                return None
            else:
                req_qty = candidate.desired_notional / sizing_price
        else:
            req_qty = requested_quantity

        raw_idempotency = candidate.features.get("idempotency_key")
        idempotency_key = (
            str(raw_idempotency)
            if isinstance(raw_idempotency, str) and raw_idempotency.strip()
            else None
        )

        allocation_plan: ExitAllocationPlan | None = None
        expected_projection_version: str | None = (
            str(candidate.features["projection_version"]).strip()
            if candidate.features.get("projection_version")
            else None
        )
        if candidate.reduce_only:
            raw_allocations = candidate.features.get("exit_allocations", [])
            if not isinstance(raw_allocations, list):
                raise ValueError("exit_allocations must be a list")
            parsed_allocations: list[ExitAllocation] = []
            for item in raw_allocations:
                if not isinstance(item, dict) or set(item) != {"batch_id", "quantity"}:
                    raise ValueError("exit allocation requires batch_id and quantity")
                batch_id = item["batch_id"]
                quantity = item["quantity"]
                if not isinstance(batch_id, str) or not isinstance(quantity, str):
                    raise ValueError("exit allocation values must be strings")
                parsed_allocations.append(
                    ExitAllocation(
                        batch_id=batch_id, allocated_quantity=Decimal(quantity)
                    )
                )
            allocations = tuple(parsed_allocations)
            if allocations:
                allocation_plan = ExitAllocationPlan(
                    position_key=position_key,
                    allocations=allocations,
                    total_allocated_quantity=sum(
                        (item.allocated_quantity for item in allocations), Decimal("0")
                    ),
                    policy=ExitPolicyMode.TARGET_BATCHES_ONLY,
                    projection_version=expected_projection_version,
                )

        return TradeCommand(
            command_id=candidate.candidate_id,
            position_key=position_key,
            command_type=(
                TradeCommandType.EXIT
                if candidate.reduce_only
                else TradeCommandType.ENTRY
            ),
            side=candidate.side,
            order_type=candidate.entry_type,
            requested_quantity=req_qty,
            limit_price=candidate.limit_price,
            reduce_only=candidate.reduce_only,
            allocation_plan=allocation_plan,
            expected_projection_version=expected_projection_version,
            idempotency_key=idempotency_key,
            created_at=candidate.created_at,
        )


def _min_notional(rules: SymbolTradingRules | None) -> Decimal | None:
    return None if rules is None else rules.min_notional
