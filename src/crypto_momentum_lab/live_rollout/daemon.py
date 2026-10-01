from collections.abc import (
    AsyncIterable,
    Awaitable,
    Callable,
    Mapping,
)
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import structlog

import crypto_momentum_lab.live_rollout.market_runtime_contracts as market_runtime_contracts
from crypto_momentum_lab.domain.account import (
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.order_read_models import (
    PersistedExchangeOrder,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderSubmissionRepository,
)
from crypto_momentum_lab.domain.execution.progress_contract import (
    ExecutionReadiness,
    ReadinessEvaluator,
)
from crypto_momentum_lab.domain.market.models import (
    MarketState15s,
    RealtimeMarketQuote,
)
from crypto_momentum_lab.domain.strategy import (
    OrderIntentCandidate,
    StrategyDecision,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    CoordinatedOrderExecutionPort,
)
from crypto_momentum_lab.execution_account.orders.recovery import (
    ExitRecoveryClient,
)
from crypto_momentum_lab.live_rollout.checkpoint_coordinator import (
    LiveCheckpointCoordinator,
)
from crypto_momentum_lab.live_rollout.checkpoint_writer import (
    CheckpointWriter,
    PersistCheckpoint,
)
from crypto_momentum_lab.live_rollout.closed_candle_feed import (
    ClosedCandle15mEvent,
)
from crypto_momentum_lab.live_rollout.context import (
    LiveContextProvider,
    LiveContextRuntime,
    LiveDaemonRuntimeContext,
)
from crypto_momentum_lab.live_rollout.context_prefetch import (
    LiveContextPrefetcher,
)
from crypto_momentum_lab.live_rollout.daemon_lifecycle import (
    LiveDaemonLifecycle,
)
from crypto_momentum_lab.live_rollout.entry_control import (
    LiveEntryControlGate,
)
from crypto_momentum_lab.live_rollout.entry_lane import (
    EntryExecutionLane,
    EntryLaneConfig,
    _live_signal_account_context,
)
from crypto_momentum_lab.live_rollout.exit_event_coordinator import (
    LiveExitEventCoordinator,
)
from crypto_momentum_lab.live_rollout.exit_failure_policy import (
    is_pending_exit_evaluation,
)
from crypto_momentum_lab.live_rollout.exit_lane import (
    ExitExecutionLane,
    ExitLaneOutcome,
)
from crypto_momentum_lab.live_rollout.exit_processor import (
    ExitProcessorConfig,
    LiveExitProcessor,
)
from crypto_momentum_lab.live_rollout.exits import LiveExitManager
from crypto_momentum_lab.live_rollout.limits import FixedLiveLimits
from crypto_momentum_lab.live_rollout.market_admission import (
    LiveMarketStateAdmission,
)
from crypto_momentum_lab.live_rollout.market_loop import LiveMarketLoop
from crypto_momentum_lab.live_rollout.pending_entries import (
    LivePendingEntryRegistry,
)
from crypto_momentum_lab.live_rollout.runtime_cache import (
    LiveRuntimeCacheMaintenance,
    StrategyCacheMetrics,
)
from crypto_momentum_lab.live_rollout.scheduled_controller import (
    ScheduledRiskWindowController,
    ScheduledRiskWindowControllerConfig,
)
from crypto_momentum_lab.live_rollout.scheduled_risk_window import (
    ScheduledRiskWindowConfig,
)
from crypto_momentum_lab.live_rollout.signal_recorder import (
    LiveSignalRecorderPort,
)
from crypto_momentum_lab.live_rollout.submission import (
    LiveCandidateSubmission,
    LiveEntryOrderLifecycle,
    LiveSubmissionConfig,
)
from crypto_momentum_lab.live_rollout.submission_admission import (
    LiveSubmissionAdmission,
)
from crypto_momentum_lab.live_rollout.telemetry import LiveTelemetrySink
from crypto_momentum_lab.risk.gateway import RiskGateway

log = structlog.get_logger()


@dataclass(frozen=True, slots=True, kw_only=True)
class LiveDaemonConfig(EntryLaneConfig):
    account_label: str
    resize_tolerance: Decimal
    checkpoint_every_states: int
    checkpoint_every_seconds: float = 60.0
    checkpoint_phase_seconds: float = 0.0
    max_dirty_age_seconds: float = 90.0
    hedge_mode: bool = True
    scheduled_risk_window: ScheduledRiskWindowConfig | None = None
    unmanaged_halt_debounce_seconds: float = 15.0
    decision_filter: (
        Callable[
            [StrategyDecision, MarketState15s],
            Awaitable[StrategyDecision] | StrategyDecision,
        ]
        | None
    ) = None
    decision_fact_binder: Callable[[Any], None] | None = None

    def __post_init__(self) -> None:
        EntryLaneConfig.__post_init__(self)
        if not self.account_label.strip():
            raise ValueError("account_label must not be empty")
        if self.resize_tolerance < 0 or self.resize_tolerance >= 1:
            raise ValueError("resize_tolerance must be in [0, 1)")
        if self.checkpoint_every_states <= 0:
            raise ValueError("checkpoint_every_states must be positive")
        if self.checkpoint_every_seconds <= 0:
            raise ValueError("checkpoint_every_seconds must be positive")
        if not 0 <= self.checkpoint_phase_seconds < self.checkpoint_every_seconds:
            raise ValueError(
                "checkpoint_phase_seconds must be in [0, checkpoint_every_seconds)"
            )
        if self.max_dirty_age_seconds <= 0:
            raise ValueError("max_dirty_age_seconds must be positive")


class LiveStrategyDaemon:
    def __init__(
        self,
        *,
        strategy: market_runtime_contracts.LiveRuntimeStrategy,
        risk_gateway: RiskGateway,
        limits: FixedLiveLimits,
        submission_repository: OrderSubmissionRepository,
        persist_checkpoint: PersistCheckpoint,
        state_machine: CoordinatedOrderExecutionPort,
        context_provider: LiveContextProvider,
        config: LiveDaemonConfig,
        exit_manager: LiveExitManager | None = None,
        exit_recovery_client: ExitRecoveryClient | None = None,
        telemetry: LiveTelemetrySink | None = None,
        signal_recorder: LiveSignalRecorderPort | None = None,
        entry_order_lifecycle: LiveEntryOrderLifecycle | None = None,
        clock: Callable[[], datetime] | None = None,
        on_managed_position_symbols: (
            Callable[[frozenset[str]], None] | None
        ) = None,
        cancel_unfilled_entry_orders: (
            Callable[[tuple[OrderExecutionPlan, ...]], Awaitable[int]] | None
        ) = None,
        fetch_exchange_positions: (
            Callable[[], Awaitable[tuple[AccountPositionSnapshot, ...]]] | None
        ) = None,
        recover_market_state_gap: market_runtime_contracts.MarketStateGapRecovery
        | None = None,
        hub_cursor_provider: Callable[[], Mapping[str, str | int] | None] | None = None,
        commit_market_state_cursor: Callable[[MarketState15s], None] | None = None,
        entered_symbol_lookup: Callable[[str], bool] | None = None,
        on_checkpoint_saved: Callable[[], None] | None = None,
        cached_context_provider: Callable[[], LiveDaemonRuntimeContext | None] | None = None,
        request_exit_recovery: Callable[[], None] = lambda: None,
    ) -> None:
        self._strategy = strategy
        self._risk_gateway = risk_gateway
        self._limits = limits
        self._state_machine = state_machine
        self._clock = clock or (lambda: datetime.now(tz=UTC))
        self._entry_control = LiveEntryControlGate(
            run_id=config.run_id,
            state_machine=self._state_machine,
            scheduled_risk_window=config.scheduled_risk_window,
            clock=self._clock,
        )
        self._context_provider = context_provider
        self._cached_context_provider = cached_context_provider or (
            lambda: getattr(context_provider, "cached_context", None)
            or getattr(context_provider, "_cached_context", None)
        )
        self._config = config
        if (
            exit_manager is not None
            and getattr(exit_manager, "_config", None) is not None
        ):
            if getattr(exit_manager._config, "account_label", None) is None:
                exit_manager._config = replace(
                    exit_manager._config,
                    account_label=config.account_label,
                )
        self._exit_manager = exit_manager
        self._exit_recovery_client = exit_recovery_client
        self._checkpoint_coordinator = LiveCheckpointCoordinator(
            writer=CheckpointWriter(
                run_id=config.run_id,
                persist=persist_checkpoint,
                on_persist_success=on_checkpoint_saved,
            ),
            strategy=self._strategy,
            checkpoint_every_states=config.checkpoint_every_states,
            checkpoint_every_seconds=config.checkpoint_every_seconds,
            checkpoint_phase_seconds=config.checkpoint_phase_seconds,
            max_dirty_age_seconds=config.max_dirty_age_seconds,
            hub_cursor_provider=hub_cursor_provider,
        )
        self._telemetry = telemetry
        self._signal_recorder = signal_recorder
        self._entry_order_lifecycle = entry_order_lifecycle
        self._cancel_unfilled_entry_orders = cancel_unfilled_entry_orders
        self._fetch_exchange_positions = fetch_exchange_positions
        self._run_active = False
        self._exit_enabled = True
        self._pending_entries = LivePendingEntryRegistry(clock=self._clock)
        strategy_protected_symbols = getattr(strategy, "cache_protected_symbols", None)
        strategy_pruner = getattr(strategy, "prune_inactive_symbols", None)
        self._runtime_cache = LiveRuntimeCacheMaintenance(
            run_id=config.run_id,
            strategy_metrics_provider=lambda: StrategyCacheMetrics(
                buffered_symbol_count=getattr(strategy, "buffered_symbol_count", None),
                buffered_state_count=getattr(strategy, "buffered_state_count", None),
            ),
            pending_entry_symbols=self._pending_entries.pending_symbols,
            strategy_protected_symbols=(
                strategy_protected_symbols
                if callable(strategy_protected_symbols)
                else None
            ),
            strategy_pruner=strategy_pruner if callable(strategy_pruner) else None,
            volume_metrics_provider=(
                (lambda: getattr(self._signal_recorder, "volume_metrics", {}))
                if self._signal_recorder is not None
                else None
            ),
        )
        self._context_runtime = LiveContextRuntime(
            run_id=config.run_id,
            context_provider=self._context_provider,
            sync_pending_entry_plans=self._pending_entries.sync,
            set_pending_position_symbols=(
                self._entry_control.set_pending_position_symbols
            ),
            update_managed_symbols=(
                lambda position_symbols, order_symbols: (
                    self._runtime_cache.update_managed_symbols(
                        position_symbols=position_symbols,
                        order_symbols=order_symbols,
                    )
                )
            ),
            on_managed_position_symbols=on_managed_position_symbols,
        )
        self._context_prefetcher = LiveContextPrefetcher(
            context_provider=self._context_provider,
            context_generation=lambda: self._context_runtime.generation,
            clock=self._clock,
        )
        self._market_admission = LiveMarketStateAdmission(
            context_provider=self._context_provider,
            context_generation=lambda: self._context_runtime.generation,
            apply_context=(
                self._context_runtime.apply_context
            ),
            telemetry=self._telemetry,
            clock=self._clock,
            invalidate_context=self._context_runtime.invalidate,
        )
        self._state_machine.configure_submission(
            submission_repository,
            admission=LiveSubmissionAdmission(
                self._entry_control, self._context_runtime.is_current,
            ),
            clock=self._clock,
        )
        self._submission = LiveCandidateSubmission(
            risk_gateway=self._risk_gateway,
            limits=self._limits,
            state_machine=self._state_machine,
            config=LiveSubmissionConfig(
                run_id=config.run_id,
                account_label=config.account_label,
                resize_tolerance=config.resize_tolerance,
                hedge_mode=config.hedge_mode,
                entry_order_type=config.entry_order_type,
                entry_limit_ttl_seconds=config.entry_limit_ttl_seconds,
            ),
            clock=self._clock,
            entry_enabled=lambda: self.entry_enabled,
            entry_enabled_reason=lambda: self.entry_enabled_reason,
            context_is_current=self._context_runtime.is_current,
            pending_entry_reservation=self._pending_entries.reservation,
            remember_pending_entry=self._pending_entries.remember,
            record_signal_candidate=self._record_signal_candidate,
            telemetry=self._telemetry,
            entry_order_lifecycle=self._entry_order_lifecycle,
        )
        self._exit_processor = LiveExitProcessor(
            config=ExitProcessorConfig(run_id=config.run_id),
            exit_manager=self._exit_manager,
            exit_recovery_client=self._exit_recovery_client,
            state_machine=self._state_machine,
            submission=self._submission,
            telemetry=self._telemetry,
            clock=self._clock,
            is_exit_enabled=lambda: self._exit_enabled,
            context_provider=self._context_provider,
            apply_context=(
                self._context_runtime.apply_context
            ),
            invalidate_context_cache=self._context_runtime.invalidate,
            context_is_current=self._context_runtime.is_current,
            request_recovery=request_exit_recovery,
        )
        self._exit_lane = ExitExecutionLane(
            lambda state: self._exit_events.process_market_work(state),
            lambda quote, state: self._exit_events.process_quote_work(quote, state),
            on_outcome=self._on_exit_lane_outcome,
        )
        self._exit_events = LiveExitEventCoordinator(
            run_id=config.run_id,
            exit_enabled=lambda: (
                self._exit_manager is not None and self._exit_enabled
            ),
            run_active=lambda: self._run_active,
            context_provider=self._context_provider,
            apply_context=(
                self._context_runtime.apply_context
            ),
            invalidate_context_cache=self._context_runtime.invalidate,
            exit_processor=self._exit_processor,
            exit_lane=self._exit_lane,
        )
        entry_submission_waiter = getattr(
            self._state_machine, "wait_for_entry_submissions_idle", None
        )
        if not callable(entry_submission_waiter):
            entry_submission_waiter = None
        self._scheduled_controller = ScheduledRiskWindowController(
            config=ScheduledRiskWindowControllerConfig(
                run_id=config.run_id,
                scheduled_risk_window=config.scheduled_risk_window,
            ),
            exit_manager=self._exit_manager,
            state_machine=self._state_machine,
            context_provider=self._context_provider,
            apply_context=(
                self._context_runtime.apply_context
            ),
            invalidate_context_cache=self._context_runtime.invalidate,
            process_exit_requests=self._exit_processor.process_requests,
            set_entry_blocked=self.set_scheduled_entry_blocked,
            pending_entry_plans=self._pending_entries.snapshot,
            cancel_unfilled_entry_orders=self._cancel_unfilled_entry_orders,
            fetch_exchange_positions=self._fetch_exchange_positions,
            clock=self._clock,
            wait_for_entry_submissions_idle=entry_submission_waiter,
        )
        self._entry_lane = EntryExecutionLane(
            config=config,
            clock=self._clock,
            entry_enabled=lambda: self.entry_enabled,
            entry_enabled_reason=lambda: self.entry_enabled_reason,
            execute_candidate=self._submission.execute,
            invalidate_context=self._context_runtime.invalidate,
            telemetry=self._telemetry,
            signal_recorder=self._signal_recorder,
        )
        self._market_loop = LiveMarketLoop(
            run_id=config.run_id,
            strategy=self._strategy,
            context_prefetcher=self._context_prefetcher,
            runtime_cache=self._runtime_cache,
            scheduled_controller=self._scheduled_controller,
            telemetry=self._telemetry,
            exit_lane=self._exit_lane,
            exit_manager=self._exit_manager,
            exit_enabled=lambda: self._exit_enabled,
            market_admission=self._market_admission,
            checkpoint_coordinator=self._checkpoint_coordinator,
            entry_lane=self._entry_lane,
            state_machine=self._state_machine,
            clock=self._clock,
            recover_market_state_gap=recover_market_state_gap,
            hub_cursor_provider=hub_cursor_provider,
            commit_market_state_cursor=commit_market_state_cursor,
            entered_symbol_lookup=entered_symbol_lookup,
            unmanaged_halt_debounce_seconds=config.unmanaged_halt_debounce_seconds,
            decision_filter=config.decision_filter,
            decision_fact_binder=config.decision_fact_binder,
        )
        self._lifecycle = LiveDaemonLifecycle(
            run_id=config.run_id,
            checkpoint_coordinator=self._checkpoint_coordinator,
            exit_lane=self._exit_lane,
            exit_manager=self._exit_manager,
            scheduled_controller=self._scheduled_controller,
            scheduled_risk_window_enabled=(config.scheduled_risk_window is not None),
            run_market_loop=self._market_loop.run,
            set_run_active=self._set_run_active,
        )

    @property
    def entry_enabled(self) -> bool:
        return self._entry_control.entry_enabled

    @property
    def entry_enabled_reason(self) -> str:
        return self._entry_control.entry_enabled_reason

    @property
    def exit_enabled(self) -> bool:
        return self._exit_enabled

    @property
    def managed_position_symbols(self) -> frozenset[str]:
        return self._context_runtime.managed_position_symbols

    def note_order_identity_conflict(self, symbol: str) -> None:
        if self._exit_manager is not None:
            self._exit_manager.note_order_identity_conflict(symbol)

    @property
    def checkpoint_coordinator(self) -> LiveCheckpointCoordinator:
        return self._checkpoint_coordinator

    def set_entry_enabled(self, enabled: bool, *, reason: str) -> None:
        self._entry_control.set_entry_enabled(enabled, reason=reason)

    def _on_exit_lane_outcome(self, symbol: str, outcome: ExitLaneOutcome) -> None:
        failure = self._exit_lane.failure or outcome.failure
        if not is_pending_exit_evaluation(failure):
            self._entry_control.set_exit_failure(symbol, failure)

    def set_exit_failure(
        self,
        symbol: str,
        failure: str | None,
    ) -> bool:
        return self._entry_control.set_exit_failure(symbol, failure)

    def set_entry_filter_cache_ready(self, ready: bool) -> None:
        self._entry_control.set_entry_filter_cache_ready(ready)

    def refresh_entry_prerequisites(
        self,
        *,
        lease_heartbeat_degraded: bool,
        session_draining: bool,
        market_state_available: bool,
        market_state_unavailable_reason: str,
        account_snapshot_available: bool,
        strategy_warmup_ready: bool,
        strategy_warmup_reason: str = "strategy_warmup_ready",
    ) -> None:
        self._entry_control.refresh_entry_prerequisites(
            lease_heartbeat_degraded=lease_heartbeat_degraded,
            session_draining=session_draining,
            strategy_warmup_ready=strategy_warmup_ready,
            strategy_warmup_reason=strategy_warmup_reason,
            market_state_available=market_state_available,
            market_state_unavailable_reason=market_state_unavailable_reason,
            account_snapshot_available=account_snapshot_available,
        )

    def set_risk_control_entry_blocked(
        self,
        blocked: bool,
        *,
        reason: str,
    ) -> None:
        self._entry_control.set_risk_control_entry_blocked(
            blocked,
            reason=reason,
        )

    def set_scheduled_entry_blocked(
        self,
        blocked: bool,
        *,
        reason: str,
    ) -> None:
        self._entry_control.set_scheduled_entry_blocked(
            blocked,
            reason=reason,
        )

    def set_exit_enabled(self, enabled: bool, *, reason: str) -> None:
        if not isinstance(enabled, bool):
            raise TypeError("enabled must be a bool")
        if self._exit_enabled != enabled:
            self._exit_enabled = enabled
            log.warning("live_exit_lane_state_changed", enabled=enabled,
                        reason=reason, run_id=self._config.run_id)

    def observe_entry_order_event(
        self,
        plan: OrderExecutionPlan,
        event: ExchangeOrderEvent,
    ) -> None:
        """Release an in-memory reservation after a terminal entry event.

        The exchange/order callback can arrive before the next database
        context refresh.  Removing the reservation here prevents a filled or
        canceled limit entry from consuming gross-exposure capacity for the
        remainder of its 15-minute lifetime.
        """
        self._pending_entries.observe_order_event(plan, event)

    def notify_market_state_gap(self, *, reason: str) -> None:
        """Force each symbol to rebuild indicators after a skipped batch."""
        self._market_loop.notify_market_state_gap(reason=reason)

    async def process_account_event(
        self,
        state: MarketState15s,
        *,
        quote: RealtimeMarketQuote | None = None,
    ) -> str | None:
        return await self._exit_events.process_account_event(state, quote=quote)

    async def process_market_quote(
        self,
        quote: RealtimeMarketQuote,
        state: MarketState15s,
    ) -> str | None:
        return await self._exit_events.process_market_quote(quote, state)

    async def process_closed_candle(
        self,
        event: ClosedCandle15mEvent,
        *,
        latest_quote: RealtimeMarketQuote | None = None,
    ) -> str | None:
        return await self._exit_events.process_closed_candle(
            event,
            latest_quote=latest_quote,
        )

    async def process_grace_timeout(
        self,
        state: MarketState15s,
        *,
        now: datetime,
        latest_quote: RealtimeMarketQuote | None = None,
    ) -> str | None:
        return await self._exit_events.process_grace_timeout(
            state,
            now=now,
            latest_quote=latest_quote,
        )

    async def run(
        self,
        states: AsyncIterable[MarketState15s],
    ) -> market_runtime_contracts.LiveDaemonResult:
        return await self._lifecycle.run(states)

    def _set_run_active(self, active: bool) -> None:
        self._run_active = active

    @property
    def latest_watermark(self) -> datetime | None:
        market_wm = (
            self._checkpoint_coordinator.latest_watermark
            or self._market_loop.active_state_at
        )
        account_wm: datetime | None = None
        ctx = self._cached_context_provider()
        if ctx is not None and ctx.account_observed_at is not None:
            account_wm = ctx.account_observed_at

        candidates = [wm for wm in (market_wm, account_wm) if wm is not None]
        return min(candidates) if candidates else None

    def evaluate_readiness(self, symbol: str | None = None) -> ExecutionReadiness:
        reconciliation_gap = Decimal("0")
        ctx = self._cached_context_provider()
        if ctx is not None:
            if symbol is not None:
                gap_count = int(symbol in ctx.unmanaged_position_symbols) + sum(
                    1
                    for o in ctx.unresolved_orders
                    if getattr(o, "symbol", None) == symbol
                )
            else:
                gap_count = len(ctx.unmanaged_position_symbols) + len(
                    ctx.unresolved_orders
                )
            reconciliation_gap = Decimal(str(gap_count))

        assessment = ReadinessEvaluator.evaluate(
            current_time=self._clock(),
            watermark_time=self.latest_watermark,
            reconciliation_gap=reconciliation_gap,
        )
        return assessment.readiness

    def request_unknown_exit_recovery(self, order: PersistedExchangeOrder, state: MarketState15s) -> None:
        self._exit_processor.request_exit_recovery(
            plan=order.plan, known_executed_quantity=order.executed_quantity, state=state)

    @property
    def has_pending_exit_recovery(self) -> bool:
        return self._exit_processor.has_pending_recovery

    async def recover_requested_exits(self) -> bool:
        outcomes = await self._exit_processor.recover_requested_exits()
        for symbol, outcome in outcomes:
            self._exit_lane.record_recovery_outcome(symbol, outcome)
        return any(not outcome.fatal_failure for _, outcome in outcomes)

    async def process_scheduled_risk_window(
        self,
        *,
        now: datetime | None = None,
    ) -> str | None:
        """Run the scheduled-risk controller for one observation."""
        return await self._scheduled_controller.process(now=now)

    async def cancel_all_open_entries(self) -> str | None:
        """Cancel open entry orders through the live coordinator seam."""
        return await self._scheduled_controller.cancel_all_open_entries()

    async def request_flatten(self, *, now: datetime | None = None) -> str | None:
        """Request a reduce-only flatten through the live exit processor."""
        return await self._scheduled_controller.request_flatten(now=now)

    def _record_signal_candidate(
        self,
        *,
        candidate: OrderIntentCandidate,
        state: MarketState15s,
        recorded_at: datetime,
        context: LiveDaemonRuntimeContext,
    ) -> None:
        recorder = self._signal_recorder
        if recorder is None:
            return
        try:
            recorder.record_candidate(
                candidate=candidate,
                state=state,
                recorded_at=recorded_at,
                account_context=_live_signal_account_context(context),
                filter_context={
                    "entry_enabled": self.entry_enabled,
                    "entry_enabled_reason": self.entry_enabled_reason,
                    "entry_long_only": self._config.entry_long_only,
                    "candidate_execution_path": "reduce_only_exit",
                },
            )
        except Exception as error:
            log.warning(
                "live_strategy_signal_candidate_recorder_failed",
                run_id=self._config.run_id,
                candidate_id=candidate.candidate_id,
                error_type=type(error).__name__,
            )
