"""Ordered live market-state orchestration.

The loop owns ordering, gap handling, admission, and checkpoint progress.  It
does not construct exchange clients or persistence adapters; those are passed
in as the already-separated lanes and coordinators.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

import structlog
from sqlalchemy.exc import SQLAlchemyError

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderState,
)
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import (
    StrategyCheckpoint,
    StrategyDecision,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionPort,
)
from crypto_momentum_lab.live_rollout.checkpoint_coordinator import (
    LiveCheckpointCoordinator,
)
from crypto_momentum_lab.live_rollout.context import LiveDaemonRuntimeContext
from crypto_momentum_lab.live_rollout.context_prefetch import (
    LiveContextPrefetcher,
)
from crypto_momentum_lab.live_rollout.entry_lane import EntryExecutionLane
from crypto_momentum_lab.live_rollout.exit_lane import ExitExecutionLane
from crypto_momentum_lab.live_rollout.exits import LiveExitManager
from crypto_momentum_lab.live_rollout.market_admission import (
    LiveMarketStateAdmission,
)
from crypto_momentum_lab.live_rollout.runtime_cache import (
    LiveRuntimeCacheMaintenance,
)
from crypto_momentum_lab.live_rollout.scheduled_controller import (
    ScheduledRiskWindowController,
)
from crypto_momentum_lab.live_rollout.telemetry import (
    LIVE_LANE_ENTRY,
    LiveTelemetrySink,
)

log = structlog.get_logger()


class LiveRuntimeStrategy(Protocol):
    def on_market_state(self, state: MarketState15s) -> StrategyDecision: ...

    def checkpoint(
        self,
        *,
        include_market_state_buffers: bool = True,
    ) -> StrategyCheckpoint: ...

    def warm_market_state(self, state: MarketState15s) -> None: ...

    def clear_market_state_buffers(self) -> None: ...


class LiveMarketStateContinuityError(RuntimeError):
    """Raised when an ordered live state stream skips a required bucket."""

    def __init__(
        self,
        *,
        symbol: str,
        previous_at: datetime,
        current_at: datetime,
        expected_interval_seconds: int,
    ) -> None:
        observed_delta_seconds = int(
            (current_at - previous_at).total_seconds()
        )
        super().__init__(
            "missing market-state bucket: "
            f"symbol={symbol} previous={previous_at.isoformat()} "
            f"current={current_at.isoformat()} "
            f"expected_interval_seconds={expected_interval_seconds} "
            f"observed_delta_seconds={observed_delta_seconds}"
        )
        self.symbol = symbol
        self.previous_at = previous_at
        self.current_at = current_at
        self.expected_interval_seconds = expected_interval_seconds
        self.observed_delta_seconds = observed_delta_seconds


@dataclass(frozen=True, slots=True)
class LiveDaemonResult:
    processed_state_count: int
    approved_intent_count: int
    submitted_order_count: int
    halt_reason: str | None
    final_state_at: datetime | None


class LiveMarketLoop:
    """Run ordered market states through admission and execution lanes."""

    def __init__(
        self,
        *,
        run_id: str,
        strategy: LiveRuntimeStrategy,
        context_prefetcher: LiveContextPrefetcher,
        runtime_cache: LiveRuntimeCacheMaintenance,
        scheduled_controller: ScheduledRiskWindowController,
        scheduled_risk_window_enabled: bool,
        telemetry: LiveTelemetrySink | None,
        exit_lane: ExitExecutionLane,
        exit_manager: LiveExitManager | None,
        exit_enabled: Callable[[], bool],
        reconcile_orders: Callable[[], Awaitable[None]] | None,
        reconcile_once_per_bucket: bool,
        market_admission: LiveMarketStateAdmission,
        checkpoint_coordinator: LiveCheckpointCoordinator,
        entry_lane: EntryExecutionLane,
        state_machine: OrderExecutionPort,
        clock: Callable[[], datetime],
    ) -> None:
        if not run_id.strip():
            raise ValueError("run_id must not be empty")
        self._run_id = run_id
        self._strategy = strategy
        self._context_prefetcher = context_prefetcher
        self._runtime_cache = runtime_cache
        self._scheduled_controller = scheduled_controller
        self._scheduled_risk_window_enabled = scheduled_risk_window_enabled
        self._telemetry = telemetry
        self._exit_lane = exit_lane
        self._exit_manager = exit_manager
        self._exit_enabled = exit_enabled
        self._reconcile_orders = reconcile_orders
        self._reconcile_once_per_bucket = reconcile_once_per_bucket
        self._market_admission = market_admission
        self._checkpoint_coordinator = checkpoint_coordinator
        self._entry_lane = entry_lane
        self._state_machine = state_machine
        self._clock = clock
        self._market_gap_generation = 0
        self._strategy_gap_reset_generation_by_symbol: dict[str, int] = {}
        self._last_transient_gate_reasons: tuple[str, ...] | None = None

    @property
    def market_gap_generation(self) -> int:
        return self._market_gap_generation

    def notify_market_state_gap(self, *, reason: str) -> None:
        if not reason.strip():
            raise ValueError("reason must not be empty")
        self._market_gap_generation += 1
        log.warning(
            "live_strategy_market_state_gap_detected",
            run_id=self._run_id,
            reason=reason,
            generation=self._market_gap_generation,
        )

    async def run(
        self,
        states: AsyncIterable[MarketState15s],
    ) -> LiveDaemonResult:
        self._entry_lane.reset()
        processed = approved = submitted = 0
        final_state_at: datetime | None = None
        last_reconciled_bucket: datetime | None = None
        max_gap_seconds = _strategy_max_gap_seconds(self._strategy)
        state_interval_seconds = _strategy_state_interval_seconds(self._strategy)
        async for prefetched in self._context_prefetcher.stream(states):
            state = prefetched.state
            try:
                _validate_market_state_continuity(
                    state=state,
                    last_processed_at=(
                        self._checkpoint_coordinator.last_processed_at(
                            state.symbol
                        )
                    ),
                    expected_interval_seconds=state_interval_seconds,
                )
            except LiveMarketStateContinuityError as error:
                self.notify_market_state_gap(reason=str(error))
                await self._checkpoint_coordinator.save_final()
                raise
            self._runtime_cache.prune(
                now=self._clock(),
                current_symbol=state.symbol,
                active_symbols=self._entry_lane.entry_symbols,
            )
            self._scheduled_controller.observe_state(state)
            if self._scheduled_risk_window_enabled:
                try:
                    await self._scheduled_controller.process(now=self._clock())
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    # The independent wall-clock task retries this control
                    # path; the schedule gate remains fail-closed meanwhile.
                    log.exception(
                        "live_inline_scheduled_risk_window_failed",
                        run_id=self._run_id,
                        error_type=type(error).__name__,
                    )
            if self._telemetry is not None:
                await self._telemetry.market_state_received(
                    state,
                    occurred_at=prefetched.received_at,
                    lane=LIVE_LANE_ENTRY,
                )
                market_state_progress = getattr(
                    self._telemetry,
                    "market_state_progress",
                    None,
                )
                if callable(market_state_progress):
                    market_state_progress(
                        state,
                        occurred_at=prefetched.received_at,
                        received_at=prefetched.received_at,
                    )
            exit_lane_failure = self._exit_lane.failure
            if exit_lane_failure is not None:
                await self._checkpoint_coordinator.save_final()
                return LiveDaemonResult(
                    processed,
                    approved,
                    submitted,
                    exit_lane_failure,
                    final_state_at,
                )
            if self._reconcile_orders is not None and (
                not self._reconcile_once_per_bucket
                or last_reconciled_bucket != state.bucket_start
            ):
                try:
                    await self._reconcile_orders()
                    last_reconciled_bucket = state.bucket_start
                except Exception as error:
                    if _is_transient_runtime_error(error):
                        # Reconciliation is an eventual-consistency safety
                        # net. A temporary database outage must not tear down
                        # the live process; the next bucket retries it.
                        last_reconciled_bucket = state.bucket_start
                        log.warning(
                            "live_order_reconciliation_degraded",
                            run_id=self._run_id,
                            error_type=type(error).__name__,
                        )
                        continue
                    await self._checkpoint_coordinator.save_final()
                    return LiveDaemonResult(
                        processed,
                        approved,
                        submitted,
                        f"order_reconciliation_failed:{type(error).__name__}",
                        final_state_at,
                    )
            gap_generation = self._market_gap_generation
            if (
                gap_generation
                > self._strategy_gap_reset_generation_by_symbol.get(
                    state.symbol,
                    0,
                )
            ):
                reset = getattr(self._strategy, "reset_symbol", None)
                if callable(reset):
                    reset(state.symbol)
                    self._checkpoint_coordinator.forget_symbol(state.symbol)
                    log.info(
                        "live_strategy_symbol_reset_after_market_gap",
                        run_id=self._run_id,
                        symbol=state.symbol,
                        generation=gap_generation,
                    )
                self._strategy_gap_reset_generation_by_symbol[state.symbol] = (
                    gap_generation
                )
            _reset_strategy_for_gap(
                strategy=self._strategy,
                symbol=state.symbol,
                current_at=state.bucket_start,
                last_processed_at=self._checkpoint_coordinator.last_processed_at(
                    state.symbol
                ),
                max_gap_seconds=max_gap_seconds,
            )
            admission = await self._market_admission.prepare(prefetched)
            if admission.error is not None:
                admission_error = admission.error
                if not _is_transient_runtime_error(admission_error):
                    raise admission_error
                # Keep indicators moving but never authorize without a fresh
                # context. The next state retries the full context read.
                decision = self._strategy.on_market_state(state)
                self._entry_lane.record_decision(
                    decision=decision,
                    state=state,
                    recorded_at=self._clock(),
                    filter_context={
                        "context_available": False,
                        "context_error_type": type(admission_error).__name__,
                    },
                )
                if self._telemetry is not None:
                    await self._telemetry.strategy_decision(
                        state,
                        occurred_at=self._clock(),
                        signal_count=len(decision.signals),
                        candidate_count=len(decision.candidates),
                    )
                processed += 1
                final_state_at = state.bucket_start
                self._checkpoint_coordinator.record_processed_state(
                    state,
                    saved_at=state.bucket_end,
                )
                log.warning(
                    "live_runtime_context_degraded",
                    run_id=self._run_id,
                    symbol=state.symbol,
                    error_type=type(admission_error).__name__,
                )
                continue
            if admission.context is None or admission.gate is None:
                raise RuntimeError("market state admission is incomplete")
            context = admission.context
            gate = admission.gate
            if not gate.approved:
                if _is_transient_live_gate(gate.reasons):
                    if self._last_transient_gate_reasons != gate.reasons:
                        log.warning(
                            "live_gate_temporarily_blocked",
                            run_id=self._run_id,
                            reasons=gate.reasons,
                        )
                        self._last_transient_gate_reasons = gate.reasons
                    decision = self._strategy.on_market_state(state)
                    self._entry_lane.record_decision(
                        decision=decision,
                        state=state,
                        recorded_at=self._clock(),
                        context=context,
                        gate_reasons=gate.reasons,
                        filter_context={
                            "context_available": True,
                            "gate_approved": False,
                        },
                    )
                    if self._telemetry is not None:
                        await self._telemetry.strategy_decision(
                            state,
                            occurred_at=self._clock(),
                            signal_count=len(decision.signals),
                            candidate_count=len(decision.candidates),
                        )
                    processed += 1
                    final_state_at = state.bucket_start
                    self._checkpoint_coordinator.record_processed_state(
                        state,
                        saved_at=context.now,
                    )
                    continue
                self._last_transient_gate_reasons = None
                await self._checkpoint_coordinator.save_final()
                return LiveDaemonResult(
                    processed,
                    approved,
                    submitted,
                    f"live_gate:{','.join(gate.reasons)}",
                    final_state_at,
                )
            self._last_transient_gate_reasons = None
            if context.unmanaged_position_symbols:
                await self._checkpoint_coordinator.save_final()
                symbols = ",".join(sorted(context.unmanaged_position_symbols))
                return LiveDaemonResult(
                    processed,
                    approved,
                    submitted,
                    f"unmanaged_live_positions:{symbols}",
                    final_state_at,
                )
            orphan_cancel_reason = await self._cancel_orphan_exit_orders(context)
            if orphan_cancel_reason is not None:
                await self._checkpoint_coordinator.save_final()
                return LiveDaemonResult(
                    processed,
                    approved,
                    submitted,
                    orphan_cancel_reason,
                    final_state_at,
                )
            if (
                self._exit_manager is not None
                and self._exit_enabled()
                and self._exit_manager.uses_market_state_exit
            ):
                await self._exit_lane.submit_market(state, context)
                await asyncio.sleep(0)
                exit_lane_failure = self._exit_lane.failure
                if exit_lane_failure is not None:
                    await self._checkpoint_coordinator.save_final()
                    return LiveDaemonResult(
                        processed,
                        approved,
                        submitted,
                        exit_lane_failure,
                        final_state_at,
                    )
            decision = self._strategy.on_market_state(state)
            decision_recorded_at = self._clock()
            if self._telemetry is not None:
                await self._telemetry.strategy_decision(
                    state,
                    occurred_at=decision_recorded_at,
                    signal_count=len(decision.signals),
                    candidate_count=len(decision.candidates),
                )
            entry_outcome = await self._entry_lane.process(
                decision=decision,
                state=state,
                context=context,
                gate_reasons=gate.reasons,
                recorded_at=decision_recorded_at,
            )
            approved += entry_outcome.approved_intent_count
            submitted += entry_outcome.submitted_order_count
            processed += 1
            final_state_at = state.bucket_start
            self._checkpoint_coordinator.record_processed_state(
                state,
                saved_at=context.now,
            )
        await self._checkpoint_coordinator.save_final()
        return LiveDaemonResult(
            processed,
            approved,
            submitted,
            None,
            final_state_at,
        )

    async def _cancel_orphan_exit_orders(
        self,
        context: LiveDaemonRuntimeContext,
    ) -> str | None:
        open_symbols = context.open_position_symbols or frozenset()
        for item in context.unresolved_orders:
            plan = getattr(item, "plan", None)
            if plan is None or not plan.reduce_only:
                continue
            if plan.order_type != "LIMIT":
                continue
            if plan.symbol in open_symbols:
                continue
            result = await self._state_machine.cancel_order(plan)
            if result.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION:
                log.warning(
                    "live_orphan_cancel_pending_reconciliation",
                    run_id=self._run_id,
                    symbol=plan.symbol,
                    client_order_id=plan.client_order_id,
                )
                continue
            if not result.state.terminal:
                return "orphan_cancel_not_confirmed"
        return None


def _is_transient_runtime_error(error: Exception) -> bool:
    return isinstance(
        error,
        (SQLAlchemyError, TimeoutError, ConnectionError, OSError),
    )


def _is_transient_live_gate(reasons: tuple[str, ...]) -> bool:
    return bool(reasons) and set(reasons) <= {
        "missing_active_lease",
        "inactive_or_expired_lease",
        "account_not_ready",
        "unresolved_order_uncertainty",
    }


def _strategy_max_gap_seconds(strategy: LiveRuntimeStrategy) -> int | None:
    required_data = getattr(strategy, "required_data", None)
    if not callable(required_data):
        return None
    requirement = required_data()
    value = getattr(requirement, "max_gap_seconds", None)
    return None if value is None else int(value)


def _strategy_state_interval_seconds(strategy: LiveRuntimeStrategy) -> int:
    required_data = getattr(strategy, "required_data", None)
    if not callable(required_data):
        return 15
    requirement = required_data()
    value = getattr(requirement, "base_state_interval_seconds", 15)
    interval_seconds = int(value)
    if interval_seconds <= 0:
        raise ValueError("strategy state interval must be positive")
    return interval_seconds


def _validate_market_state_continuity(
    *,
    state: MarketState15s,
    last_processed_at: datetime | None,
    expected_interval_seconds: int,
) -> None:
    if last_processed_at is None:
        return
    delta_seconds = (state.bucket_start - last_processed_at).total_seconds()
    # Multiple messages for one bucket are valid because the Hub publishes
    # one state per symbol.  A strictly later timestamp must be the next
    # canonical bucket; otherwise at least one bucket was lost or skipped.
    if delta_seconds <= 0 or delta_seconds == expected_interval_seconds:
        return
    raise LiveMarketStateContinuityError(
        symbol=state.symbol,
        previous_at=last_processed_at,
        current_at=state.bucket_start,
        expected_interval_seconds=expected_interval_seconds,
    )


def _reset_strategy_for_gap(
    *,
    strategy: LiveRuntimeStrategy,
    symbol: str,
    current_at: datetime,
    last_processed_at: datetime | None,
    max_gap_seconds: int | None,
) -> None:
    if last_processed_at is None or max_gap_seconds is None:
        return
    if (current_at - last_processed_at).total_seconds() <= max_gap_seconds:
        return
    reset = getattr(strategy, "reset_symbol", None)
    if callable(reset):
        reset(symbol)


__all__ = [
    "LiveDaemonResult",
    "LiveMarketLoop",
    "LiveMarketStateContinuityError",
    "LiveRuntimeStrategy",
]
