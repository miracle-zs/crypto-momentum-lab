from collections.abc import (
    AsyncIterable,
    Awaitable,
    Callable,
    Mapping,
)
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Protocol

import structlog

from crypto_momentum_lab.domain.account import (
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.market.models import (
    MarketState15s,
    RealtimeMarketQuote,
)
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategyCheckpoint,
    UniverseRankingSnapshot,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionPort,
)
from crypto_momentum_lab.execution_account.orders.recovery import (
    ExitRecoveryClient,
)
from crypto_momentum_lab.live_rollout.checkpoint_coordinator import (
    LiveCheckpointCoordinator,
)
from crypto_momentum_lab.live_rollout.checkpoint_writer import CheckpointWriter
from crypto_momentum_lab.live_rollout.closed_candle_feed import (
    ClosedCandle15mEvent,
)
from crypto_momentum_lab.live_rollout.context import (
    LiveContextProvider,
    LiveDaemonRuntimeContext,
    LiveEntryFilterContext,
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
from crypto_momentum_lab.live_rollout.exit_lane import ExitExecutionLane
from crypto_momentum_lab.live_rollout.exit_processor import (
    ExitProcessorConfig,
    LiveExitProcessor,
)
from crypto_momentum_lab.live_rollout.exits import LiveExitManager
from crypto_momentum_lab.live_rollout.limits import FixedLiveLimits
from crypto_momentum_lab.live_rollout.market_admission import (
    LiveMarketStateAdmission,
)
from crypto_momentum_lab.live_rollout.market_loop import (
    LiveDaemonResult as _LiveDaemonResult,
)
from crypto_momentum_lab.live_rollout.market_loop import (
    LiveMarketLoop,
)
from crypto_momentum_lab.live_rollout.market_loop import (
    LiveRuntimeStrategy as _LiveRuntimeStrategy,
)
from crypto_momentum_lab.live_rollout.market_loop import (
    _is_transient_live_gate as _market_loop_is_transient_live_gate,
)
from crypto_momentum_lab.live_rollout.pending_entries import (
    LivePendingEntryRegistry,
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
    LiveSubmissionRepository,
)
from crypto_momentum_lab.live_rollout.telemetry import LiveTelemetrySink
from crypto_momentum_lab.risk.gateway import RiskGateway

log = structlog.get_logger()

# Compatibility exports for callers that historically imported these contracts
# from daemon.py; ownership now lives with the market loop module.
LiveDaemonResult = _LiveDaemonResult
LiveRuntimeStrategy = _LiveRuntimeStrategy

_EXIT_RECOVERY_PREFIX = "live-exit-recovery-"
_EXIT_RECOVERY_MAX_ATTEMPTS = 3
_EXIT_RECOVERY_RETRY_DELAYS_SECONDS = (2.0, 5.0, 15.0)


class LiveDaemonRepository(LiveSubmissionRepository, Protocol):
    async def save_checkpoint(
        self,
        run_id: str,
        checkpoint: StrategyCheckpoint,
        saved_at: datetime,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class LiveDaemonConfig:
    run_id: str
    resize_tolerance: Decimal
    checkpoint_every_states: int
    reconcile_once_per_bucket: bool = True
    hedge_mode: bool = False
    entry_long_only: bool = False
    entry_symbol_refresh_seconds: float = 15.0
    entry_symbol_loader: Callable[[datetime], Awaitable[frozenset[str]]] | None = None
    require_price_above_ema5: bool = False
    require_price_above_ema10: bool = False
    entry_filter_context_loader: (
        Callable[[MarketState15s], Awaitable["LiveEntryFilterContext | None"]] | None
    ) = None
    entry_universe_context_provider: (
        Callable[[str, datetime], Mapping[str, object] | None] | None
    ) = None
    entry_universe_snapshot_provider: (
        Callable[[datetime], UniverseRankingSnapshot | None] | None
    ) = None
    entry_policy_compare_only: bool = False
    entry_policy_enforce: bool = False
    entry_order_type: EntryType = EntryType.LIMIT
    entry_limit_ttl_seconds: int = 900
    scheduled_risk_window: ScheduledRiskWindowConfig | None = None

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("run_id must not be empty")
        if self.resize_tolerance < 0 or self.resize_tolerance >= 1:
            raise ValueError("resize_tolerance must be in [0, 1)")
        if self.checkpoint_every_states <= 0:
            raise ValueError("checkpoint_every_states must be positive")
        if not isinstance(self.reconcile_once_per_bucket, bool):
            raise TypeError("reconcile_once_per_bucket must be a bool")
        if not isinstance(self.entry_policy_compare_only, bool):
            raise TypeError("entry_policy_compare_only must be a bool")
        if not isinstance(self.entry_policy_enforce, bool):
            raise TypeError("entry_policy_enforce must be a bool")
        if self.entry_policy_compare_only and self.entry_policy_enforce:
            raise ValueError(
                "entry_policy_compare_only and "
                "entry_policy_enforce are mutually exclusive"
            )
        if self.entry_symbol_refresh_seconds <= 0:
            raise ValueError("entry_symbol_refresh_seconds must be positive")
        if not isinstance(self.entry_order_type, EntryType):
            raise TypeError("entry_order_type must be an EntryType")
        if self.entry_limit_ttl_seconds < 601:
            raise ValueError("entry_limit_ttl_seconds must be at least 601")


class LiveStrategyDaemon:
    def __init__(
        self,
        *,
        strategy: LiveRuntimeStrategy,
        risk_gateway: RiskGateway,
        limits: FixedLiveLimits,
        repository: LiveDaemonRepository,
        state_machine: OrderExecutionPort,
        context_provider: LiveContextProvider,
        config: LiveDaemonConfig,
        exit_manager: LiveExitManager | None = None,
        exit_recovery_client: ExitRecoveryClient | None = None,
        reconcile_orders: Callable[[], Awaitable[None]] | None = None,
        telemetry: LiveTelemetrySink | None = None,
        signal_recorder: LiveSignalRecorderPort | None = None,
        entry_order_lifecycle: LiveEntryOrderLifecycle | None = None,
        clock: Callable[[], datetime] | None = None,
        on_managed_position_symbols: (
            Callable[[frozenset[str]], Awaitable[None]] | None
        ) = None,
        cancel_unfilled_entry_orders: (
            Callable[[tuple[OrderExecutionPlan, ...]], Awaitable[int]] | None
        ) = None,
        fetch_exchange_positions: (
            Callable[[], Awaitable[tuple[AccountPositionSnapshot, ...]]] | None
        ) = None,
    ) -> None:
        self._strategy = strategy
        self._risk_gateway = risk_gateway
        self._limits = limits
        self._repository = repository
        self._state_machine = state_machine
        self._entry_control = LiveEntryControlGate(
            run_id=config.run_id,
            state_machine=self._state_machine,
        )
        self._context_provider = context_provider
        self._config = config
        self._exit_manager = exit_manager
        self._exit_recovery_client = exit_recovery_client
        self._reconcile_orders = reconcile_orders
        self._checkpoint_coordinator = LiveCheckpointCoordinator(
            writer=CheckpointWriter(
                run_id=config.run_id,
                persist=self._repository.save_checkpoint,
            ),
            strategy=self._strategy,
            checkpoint_every_states=config.checkpoint_every_states,
        )
        self._telemetry = telemetry
        self._signal_recorder = signal_recorder
        self._entry_order_lifecycle = entry_order_lifecycle
        self._clock = clock or (lambda: datetime.now(tz=UTC))
        self._on_managed_position_symbols = on_managed_position_symbols
        self._cancel_unfilled_entry_orders = cancel_unfilled_entry_orders
        self._fetch_exchange_positions = fetch_exchange_positions
        self._managed_position_symbols: frozenset[str] = frozenset()
        self._context_generation = 0
        self._context_prefetcher = LiveContextPrefetcher(
            context_provider=self._context_provider,
            context_generation=lambda: self._context_generation,
            clock=self._clock,
        )
        self._run_active = False
        self._exit_enabled = True
        self._pending_entries = LivePendingEntryRegistry(clock=self._clock)
        self._runtime_cache = LiveRuntimeCacheMaintenance(
            run_id=config.run_id,
            strategy=self._strategy,
            telemetry=self._telemetry,
            pending_entry_symbols=self._pending_entries.pending_symbols,
        )
        self._market_admission = LiveMarketStateAdmission(
            context_provider=self._context_provider,
            context_generation=lambda: self._context_generation,
            sync_pending_entry_plans=self._pending_entries.sync,
            publish_managed_position_symbols=self._publish_managed_position_symbols,
            telemetry=self._telemetry,
            clock=self._clock,
        )
        self._submission = LiveCandidateSubmission(
            risk_gateway=self._risk_gateway,
            limits=self._limits,
            repository=self._repository,
            state_machine=self._state_machine,
            config=LiveSubmissionConfig(
                run_id=config.run_id,
                resize_tolerance=config.resize_tolerance,
                hedge_mode=config.hedge_mode,
                entry_order_type=config.entry_order_type,
                entry_limit_ttl_seconds=config.entry_limit_ttl_seconds,
            ),
            clock=self._clock,
            entry_enabled=lambda: self.entry_enabled,
            entry_enabled_reason=lambda: self.entry_enabled_reason,
            context_is_current=self._context_is_current,
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
            is_exit_enabled=lambda: self.exit_enabled,
            context_provider=self._context_provider,
            sync_pending_entry_plans=self._pending_entries.sync,
            publish_managed_position_symbols=self._publish_managed_position_symbols,
            invalidate_context_cache=self._invalidate_context_cache,
            context_is_current=self._context_is_current,
        )
        self._exit_lane = ExitExecutionLane(
            self._exit_processor.process_state,
            self._exit_processor.process_quote,
        )
        self._exit_events = LiveExitEventCoordinator(
            run_id=config.run_id,
            exit_manager=self._exit_manager,
            exit_enabled=lambda: self.exit_enabled,
            run_active=lambda: self._run_active,
            context_provider=self._context_provider,
            sync_pending_entry_plans=self._pending_entries.sync,
            publish_managed_position_symbols=self._publish_managed_position_symbols,
            invalidate_context_cache=self._invalidate_context_cache,
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
            sync_pending_entry_plans=self._pending_entries.sync,
            publish_managed_position_symbols=self._publish_managed_position_symbols,
            invalidate_context_cache=self._invalidate_context_cache,
            process_exit_requests=self._exit_processor.process_requests,
            set_entry_blocked=self.set_scheduled_entry_blocked,
            pending_entry_plans=self._pending_entries.snapshot,
            cancel_unfilled_entry_orders=self._cancel_unfilled_entry_orders,
            fetch_exchange_positions=self._fetch_exchange_positions,
            clock=self._clock,
        )
        self._entry_lane = EntryExecutionLane(
            config=EntryLaneConfig(
                run_id=config.run_id,
                entry_symbol_loader=config.entry_symbol_loader,
                entry_symbol_refresh_seconds=(
                    config.entry_symbol_refresh_seconds
                ),
                entry_filter_context_loader=config.entry_filter_context_loader,
                entry_universe_context_provider=(
                    config.entry_universe_context_provider
                ),
                entry_universe_snapshot_provider=(
                    config.entry_universe_snapshot_provider
                ),
                entry_long_only=config.entry_long_only,
                require_price_above_ema5=config.require_price_above_ema5,
                require_price_above_ema10=config.require_price_above_ema10,
                entry_policy_compare_only=config.entry_policy_compare_only,
                entry_policy_enforce=config.entry_policy_enforce,
                entry_order_type=config.entry_order_type,
                entry_limit_ttl_seconds=config.entry_limit_ttl_seconds,
            ),
            clock=self._clock,
            entry_enabled=lambda: self.entry_enabled,
            entry_enabled_reason=lambda: self.entry_enabled_reason,
            execute_candidate=self._submission.execute,
            invalidate_context=self._invalidate_context_cache,
            telemetry=self._telemetry,
            signal_recorder=self._signal_recorder,
        )
        self._market_loop = LiveMarketLoop(
            run_id=config.run_id,
            strategy=self._strategy,
            context_prefetcher=self._context_prefetcher,
            runtime_cache=self._runtime_cache,
            scheduled_controller=self._scheduled_controller,
            scheduled_risk_window_enabled=(
                config.scheduled_risk_window is not None
            ),
            telemetry=self._telemetry,
            exit_lane=self._exit_lane,
            exit_manager=self._exit_manager,
            exit_enabled=lambda: self.exit_enabled,
            reconcile_orders=self._reconcile_orders,
            reconcile_once_per_bucket=config.reconcile_once_per_bucket,
            market_admission=self._market_admission,
            checkpoint_coordinator=self._checkpoint_coordinator,
            entry_lane=self._entry_lane,
            state_machine=self._state_machine,
            clock=self._clock,
        )
        self._lifecycle = LiveDaemonLifecycle(
            run_id=config.run_id,
            checkpoint_coordinator=self._checkpoint_coordinator,
            exit_lane=self._exit_lane,
            exit_manager=self._exit_manager,
            scheduled_controller=self._scheduled_controller,
            scheduled_risk_window_enabled=(
                config.scheduled_risk_window is not None
            ),
            run_market_loop=self._run_market_loop,
            set_run_active=self._set_run_active,
        )

    async def _publish_managed_position_symbols(
        self,
        context: LiveDaemonRuntimeContext,
    ) -> None:
        if not self._context_is_current(context):
            log.info(
                "live_managed_position_symbols_stale_context_ignored",
                run_id=self._config.run_id,
            )
            return
        symbols = frozenset(
            (context.open_position_symbols or frozenset())
            | context.unmanaged_position_symbols
            | context.pending_position_symbols
        )
        self._managed_position_symbols = symbols
        self._entry_control.set_pending_position_symbols(
            context.pending_position_symbols
        )
        managed_order_symbols = frozenset(
            order.plan.symbol.strip().upper()
            for order in context.unresolved_orders
            if order.plan.symbol.strip()
        )
        self._runtime_cache.update_managed_symbols(
            position_symbols=symbols,
            order_symbols=managed_order_symbols,
        )
        if self._on_managed_position_symbols is not None:
            await self._on_managed_position_symbols(symbols)

    def _context_is_current(self, context: LiveDaemonRuntimeContext) -> bool:
        checker = getattr(self._context_provider, "is_context_current", None)
        if not callable(checker):
            return True
        try:
            return bool(checker(context))
        except Exception as error:
            log.warning(
                "live_context_currentness_check_failed",
                run_id=self._config.run_id,
                error_type=type(error).__name__,
            )
            return False

    def _invalidate_context_cache(self) -> None:
        self._context_generation += 1
        invalidate = getattr(self._context_provider, "invalidate_cache", None)
        if callable(invalidate):
            invalidate()

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
        return self._managed_position_symbols

    def set_entry_enabled(self, enabled: bool, *, reason: str) -> None:
        self._entry_control.set_entry_enabled(enabled, reason=reason)

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
        if self._exit_enabled == enabled:
            return
        self._exit_enabled = enabled
        log.warning(
            "live_exit_lane_state_changed",
            enabled=enabled,
            reason=reason,
            run_id=self._config.run_id,
        )

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
    ) -> LiveDaemonResult:
        return await self._lifecycle.run(states)

    def _set_run_active(self, active: bool) -> None:
        self._run_active = active

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

    async def _run_market_loop(
        self,
        states: AsyncIterable[MarketState15s],
    ) -> LiveDaemonResult:
        """Run the separated ordered market loop."""
        return await self._market_loop.run(states)

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

def _is_transient_live_gate(reasons: tuple[str, ...]) -> bool:
    """Compatibility export for callers that used the old daemon helper."""
    return _market_loop_is_transient_live_gate(reasons)
