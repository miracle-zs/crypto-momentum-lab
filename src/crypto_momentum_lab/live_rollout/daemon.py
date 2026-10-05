import asyncio
from collections.abc import (
    AsyncIterable,
    Awaitable,
    Callable,
    Mapping,
)
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import structlog

import crypto_momentum_lab.live_rollout.market_runtime_contracts as market_runtime_contracts
from crypto_momentum_lab.domain.account import (
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.exit_recovery import (
    ExitRecoveryClient,
)
from crypto_momentum_lab.domain.execution.order_execution_port import (
    CoordinatedOrderExecutionPort,
)
from crypto_momentum_lab.domain.execution.order_read_models import (
    PersistedExchangeOrder,
)
from crypto_momentum_lab.domain.execution.order_state import (
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderSubmissionRepository,
)
from crypto_momentum_lab.domain.market.models import (
    MarketState15s,
    RealtimeMarketQuote,
)
from crypto_momentum_lab.domain.risk import RiskGateway
from crypto_momentum_lab.domain.strategy import (
    OrderIntentCandidate,
    StrategyDecision,
)
from crypto_momentum_lab.domain.strategy.runtime import RuntimeStrategy
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
    LiveContextReader,
    LiveContextRuntime,
    LiveDaemonRuntimeContext,
)
from crypto_momentum_lab.live_rollout.context_prefetch import (
    LiveContextPrefetcher,
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
from crypto_momentum_lab.live_rollout.exit_lane import (
    ExitExecutionLane,
    ExitLaneOutcome,
)
from crypto_momentum_lab.live_rollout.exit_processor import (
    ExitProcessorConfig,
    LiveExitProcessor,
)
from crypto_momentum_lab.live_rollout.exits import LiveExitManager
from crypto_momentum_lab.live_rollout.market_loop import LiveMarketLoop
from crypto_momentum_lab.live_rollout.pending_entries import (
    LivePendingEntryRegistry,
)
from crypto_momentum_lab.live_rollout.position_lifecycle import (
    PositionLifecycleLocks,
)
from crypto_momentum_lab.live_rollout.runtime_cache import (
    LiveRuntimeCacheMaintenance,
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
from crypto_momentum_lab.live_rollout.telemetry import LiveTelemetrySink

log = structlog.get_logger()
_SHUTDOWN_TIMEOUT_SECONDS = 10.0


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
    decision_filter: (
        Callable[
            [StrategyDecision, MarketState15s],
            Awaitable[StrategyDecision],
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
        strategy: RuntimeStrategy,
        risk_gateway: RiskGateway,
        submission_repository: OrderSubmissionRepository,
        persist_checkpoint: PersistCheckpoint,
        state_machine: CoordinatedOrderExecutionPort,
        context_provider: LiveContextReader,
        config: LiveDaemonConfig,
        exit_manager: LiveExitManager | None = None,
        exit_recovery_client: ExitRecoveryClient | None = None,
        telemetry: LiveTelemetrySink | None = None,
        signal_recorder: LiveSignalRecorderPort | None = None,
        entry_order_lifecycle: LiveEntryOrderLifecycle | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        on_managed_position_symbols: (Callable[[frozenset[str]], None] | None) = None,
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
        request_exit_recovery: Callable[[], None],
        request_order_cleanup: Callable[
            [tuple[OrderExecutionPlan, ...]], None
        ],
        is_symbol_warmed: Callable[[str], bool] | None = None,
        on_unwarmed_symbol: Callable[[str], None] | None = None,
    ) -> None:
        self._strategy = strategy
        self._risk_gateway = risk_gateway
        self._state_machine = state_machine
        self._clock = clock
        self._entry_control = LiveEntryControlGate(
            run_id=config.run_id,
            state_machine=self._state_machine,
            scheduled_risk_window=config.scheduled_risk_window,
            clock=self._clock,
            is_symbol_warmed=is_symbol_warmed,
            on_unwarmed_symbol=on_unwarmed_symbol,
        )
        self._context_provider = context_provider
        self._config = config
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
        self._position_locks = PositionLifecycleLocks()
        self._pending_entries = LivePendingEntryRegistry(clock=self._clock)
        self._runtime_cache = LiveRuntimeCacheMaintenance(
            run_id=config.run_id,
            strategy=strategy,
            pending_entry_symbols=self._pending_entries.pending_symbols,
            volume_metrics_provider=(
                (lambda: self._signal_recorder.volume_metrics)
                if self._signal_recorder is not None
                else None
            ),
        )
        self._context_runtime = LiveContextRuntime(
            telemetry=self._telemetry,
            clock=self._clock,
            run_id=config.run_id,
            context_provider=self._context_provider,
            sync_pending_entry_plans=self._pending_entries.sync,
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
        self._state_machine.configure_submission(
            submission_repository,
            clock=self._clock,
        )
        self._submission = LiveCandidateSubmission(
            risk_gateway=self._risk_gateway,
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
            pending_entry_reservation=self._pending_entries.reservation,
            remember_pending_entry=self._pending_entries.remember,
            record_signal_candidate=self._record_signal_candidate,
            telemetry=self._telemetry,
            entry_order_lifecycle=self._entry_order_lifecycle,
            position_locks=self._position_locks,
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
            apply_context=(self._context_runtime.apply_context),
            invalidate_context_cache=self._context_runtime.invalidate,
            position_locks=self._position_locks,
            account_label=config.account_label,
            request_recovery=request_exit_recovery,
        )
        self._exit_lane = ExitExecutionLane(
            lambda state: self._exit_events.process_market_work(state),
            lambda quote, state: self._exit_events.process_quote_work(quote, state),
        )
        self._exit_events = LiveExitEventCoordinator(
            run_id=config.run_id,
            exit_enabled=lambda: self._exit_manager is not None and self._exit_enabled,
            run_active=lambda: self._run_active,
            context_provider=self._context_provider,
            apply_context=(self._context_runtime.apply_context),
            invalidate_context_cache=self._context_runtime.invalidate,
            exit_processor=self._exit_processor,
            exit_lane=self._exit_lane,
        )
        self._scheduled_controller = ScheduledRiskWindowController(
            config=ScheduledRiskWindowControllerConfig(
                run_id=config.run_id,
                scheduled_risk_window=config.scheduled_risk_window,
            ),
            exit_manager=self._exit_manager,
            state_machine=self._state_machine,
            context_provider=self._context_provider,
            apply_context=(self._context_runtime.apply_context),
            invalidate_context_cache=self._context_runtime.invalidate,
            process_exit_requests=self._exit_processor.process_requests,
            set_entry_blocked=self.set_scheduled_entry_blocked,
            pending_entry_plans=self._pending_entries.snapshot,
            cancel_unfilled_entry_orders=self._cancel_unfilled_entry_orders,
            fetch_exchange_positions=self._fetch_exchange_positions,
            clock=self._clock,
            wait_for_entry_submissions_idle=self._state_machine.wait_for_entry_submissions_idle,
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
            context_runtime=self._context_runtime,
            checkpoint_coordinator=self._checkpoint_coordinator,
            entry_lane=self._entry_lane,
            request_order_cleanup=request_order_cleanup,
            clock=self._clock,
            recover_market_state_gap=recover_market_state_gap,
            hub_cursor_provider=hub_cursor_provider,
            commit_market_state_cursor=commit_market_state_cursor,
            entered_symbol_lookup=entered_symbol_lookup,
            decision_filter=config.decision_filter,
            decision_fact_binder=config.decision_fact_binder,
        )

    @property
    def entry_enabled(self) -> bool:
        return self._entry_control.entry_enabled

    @property
    def entry_enabled_reason(self) -> str:
        return self._entry_control.entry_enabled_reason

    def is_symbol_entry_allowed(self, symbol: str) -> tuple[bool, str]:
        return self._entry_control.is_symbol_entry_allowed(symbol)

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

    def set_entry_filter_cache_ready(self, ready: bool) -> None:
        self._entry_control.set_entry_filter_cache_ready(ready)

    def refresh_entry_prerequisites(
        self,
        *,
        session_draining: bool,
        market_state_available: bool,
        market_state_unavailable_reason: str,
        strategy_warmup_ready: bool,
        strategy_warmup_reason: str = "strategy_warmup_ready",
    ) -> None:
        self._entry_control.refresh_entry_prerequisites(
            session_draining=session_draining,
            strategy_warmup_ready=strategy_warmup_ready,
            strategy_warmup_reason=strategy_warmup_reason,
            market_state_available=market_state_available,
            market_state_unavailable_reason=market_state_unavailable_reason,
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
            log.warning(
                "live_exit_lane_state_changed",
                enabled=enabled,
                reason=reason,
                run_id=self._config.run_id,
            )

    @property
    def pending_entries(self) -> LivePendingEntryRegistry:
        return self._pending_entries

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
        expires_at = self.closed_candle_expires_at(event)
        if expires_at is not None and self._clock() >= expires_at:
            return f"closed_candle_evaluation_expired:{event.candle.symbol}"
        return await self._exit_events.process_closed_candle(
            event,
            latest_quote=latest_quote,
        )

    def closed_candle_expires_at(self, event: ClosedCandle15mEvent) -> datetime | None:
        return (
            self._exit_manager.closed_candle_expires_at(event.received_at)
            if self._exit_manager is not None
            else None
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
        self._set_run_active(True)
        result: market_runtime_contracts.LiveDaemonResult | None = None
        exit_outcome = ExitLaneOutcome()
        shutdown_failure: str | None = None
        scheduled_task: asyncio.Task[None] | None = None
        try:
            await self._checkpoint_coordinator.start()
            if self._exit_manager is not None:
                await self._exit_lane.start()
            if self._config.scheduled_risk_window is not None:
                scheduled_task = asyncio.create_task(
                    self._scheduled_controller.run(),
                    name=f"live-scheduled-risk-window:{self._config.run_id}",
                )
            result = await self._market_loop.run(states)
        finally:
            if scheduled_task is not None:
                scheduled_task.cancel()
                try:
                    async with asyncio.timeout(_SHUTDOWN_TIMEOUT_SECONDS):
                        await asyncio.gather(scheduled_task, return_exceptions=True)
                except TimeoutError:
                    log.warning(
                        "live_scheduled_controller_shutdown_timed_out",
                        run_id=self._config.run_id,
                        timeout_seconds=_SHUTDOWN_TIMEOUT_SECONDS,
                    )
            if self._exit_manager is not None:
                try:
                    async with asyncio.timeout(_SHUTDOWN_TIMEOUT_SECONDS):
                        exit_outcome = await self._exit_lane.stop()
                except TimeoutError:
                    log.warning(
                        "live_exit_lane_shutdown_timed_out",
                        run_id=self._config.run_id,
                        timeout_seconds=_SHUTDOWN_TIMEOUT_SECONDS,
                    )
                    shutdown_failure = "exit_lane_shutdown_timed_out"
                except Exception:
                    log.exception(
                        "live_exit_lane_shutdown_failed",
                        run_id=self._config.run_id,
                    )
                    shutdown_failure = "exit_lane_shutdown_failed"
            try:
                async with asyncio.timeout(_SHUTDOWN_TIMEOUT_SECONDS):
                    await self._checkpoint_coordinator.stop()
            except TimeoutError:
                log.warning(
                    "live_checkpoint_shutdown_timed_out",
                    run_id=self._config.run_id,
                    timeout_seconds=_SHUTDOWN_TIMEOUT_SECONDS,
                )
            except Exception:
                log.exception(
                    "live_checkpoint_shutdown_failed",
                    run_id=self._config.run_id,
                )
            try:
                async with asyncio.timeout(_SHUTDOWN_TIMEOUT_SECONDS):
                    await self._position_locks.drain()
            except TimeoutError:
                log.warning(
                    "live_position_actor_shutdown_timed_out",
                    run_id=self._config.run_id,
                    timeout_seconds=_SHUTDOWN_TIMEOUT_SECONDS,
                )
            except Exception:
                log.exception(
                    "live_position_actor_shutdown_failed",
                    run_id=self._config.run_id,
                )
            self._set_run_active(False)
        if result is None:
            raise RuntimeError("live daemon stopped without a result")
        return market_runtime_contracts.LiveDaemonResult(
            processed_state_count=result.processed_state_count,
            approved_intent_count=(
                result.approved_intent_count
                + exit_outcome.approved_intent_count
                + self._scheduled_controller.approved_intent_count
            ),
            submitted_order_count=(
                result.submitted_order_count
                + exit_outcome.submitted_order_count
                + self._scheduled_controller.submitted_order_count
            ),
            halt_reason=result.halt_reason or shutdown_failure,
            final_state_at=result.final_state_at,
        )

    def _set_run_active(self, active: bool) -> None:
        self._run_active = active

    def request_unknown_exit_recovery(
        self, order: PersistedExchangeOrder, state: MarketState15s
    ) -> None:
        self._exit_processor.request_exit_recovery(
            plan=order.plan,
            known_executed_quantity=order.executed_quantity,
            state=state,
        )

    @property
    def has_pending_exit_recovery(self) -> bool:
        return self._exit_processor.has_pending_recovery

    async def recover_requested_exits(self) -> bool:
        outcomes = await self._exit_processor.recover_requested_exits()
        for symbol, outcome in outcomes:
            self._exit_lane.record_recovery_outcome(symbol, outcome)
        return any(outcome.failure is None for _, outcome in outcomes)

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
