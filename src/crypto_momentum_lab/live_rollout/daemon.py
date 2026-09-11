import asyncio
from collections.abc import (
    AsyncIterable,
    Awaitable,
    Callable,
    Mapping,
)
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Protocol

import structlog
from sqlalchemy.exc import SQLAlchemyError

from crypto_momentum_lab.domain.account import (
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    ExchangeOrderState,
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
    StrategyDecision,
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
from crypto_momentum_lab.live_rollout.entry_control import (
    LiveEntryControlGate,
)
from crypto_momentum_lab.live_rollout.entry_lane import (
    EntryExecutionLane,
    EntryLaneConfig,
    _live_signal_account_context,
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
from crypto_momentum_lab.live_rollout.telemetry import (
    LIVE_LANE_ENTRY,
    LiveTelemetrySink,
)
from crypto_momentum_lab.risk.gateway import RiskGateway
from crypto_momentum_lab.strategy_runner.position_exit import ClosedCandle15m

log = structlog.get_logger()

_EXIT_RECOVERY_PREFIX = "live-exit-recovery-"
_EXIT_RECOVERY_MAX_ATTEMPTS = 3
_EXIT_RECOVERY_RETRY_DELAYS_SECONDS = (2.0, 5.0, 15.0)


class LiveRuntimeStrategy(Protocol):
    def on_market_state(self, state: MarketState15s) -> StrategyDecision: ...

    def checkpoint(
        self,
        *,
        include_market_state_buffers: bool = True,
    ) -> StrategyCheckpoint: ...

    def warm_market_state(self, state: MarketState15s) -> None: ...


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


@dataclass(frozen=True, slots=True)
class LiveDaemonResult:
    processed_state_count: int
    approved_intent_count: int
    submitted_order_count: int
    halt_reason: str | None
    final_state_at: datetime | None


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
        self._scheduled_task: asyncio.Task[None] | None = None
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
        self._market_gap_generation = 0
        self._strategy_gap_reset_generation_by_symbol: dict[str, int] = {}
        self._last_transient_gate_reasons: tuple[str, ...] | None = None
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
        if not reason.strip():
            raise ValueError("reason must not be empty")
        self._market_gap_generation += 1
        log.warning(
            "live_strategy_market_state_gap_detected",
            run_id=self._config.run_id,
            reason=reason,
            generation=self._market_gap_generation,
        )

    async def process_account_event(
        self,
        state: MarketState15s,
        *,
        quote: RealtimeMarketQuote | None = None,
    ) -> str | None:
        """Run the exit lane from the latest account event.

        The entry lane remains driven by market buckets.  This method is a
        separate seam for account/order events: it refreshes the account view
        and evaluates only reduce-only requests against the newest market
        state already held in memory.  It never calls the strategy entry
        function and therefore cannot create a new position.
        """
        if self._exit_manager is None or not self._exit_enabled:
            return None
        self._invalidate_context_cache()
        context = await self._context_provider(state)
        self._pending_entries.sync(context)
        await self._publish_managed_position_symbols(context)
        if state.symbol in context.pending_position_symbols:
            symbols = ",".join(sorted(context.pending_position_symbols))
            return f"pending_live_positions:{symbols}"
        if state.symbol in context.unmanaged_position_symbols:
            symbols = ",".join(sorted(context.unmanaged_position_symbols))
            return f"unmanaged_live_positions:{symbols}"
        if self._run_active:
            await self._exit_lane.start()
            if quote is None:
                outcome = await self._exit_lane.submit_account(state, context)
            else:
                # Account events already have their own channel.  Execute the
                # quote-triggered check directly here so an account update
                # cannot be replaced by a newer ticker in the coalescing
                # quote queue.
                outcome = await self._exit_processor.process_quote(
                    quote,
                    state,
                    context,
                )
        else:
            outcome = (
                await self._exit_processor.process_state(state, context)
                if quote is None
                else await self._exit_processor.process_quote(
                    quote,
                    state,
                    context,
                )
            )
        self._invalidate_context_cache()
        if outcome.failure is not None:
            log.error(
                "live_account_event_exit_failed",
                symbol=state.symbol,
                reason=outcome.failure,
            )
        return outcome.failure

    async def process_market_quote(
        self,
        quote: RealtimeMarketQuote,
        state: MarketState15s,
    ) -> str | None:
        """Submit a latest-value quote to the reduce-only exit lane."""
        if self._exit_manager is None or not self._exit_enabled:
            return None
        if state.symbol != quote.symbol:
            return None
        # The provider caches the account/risk view for the current state
        # bucket.  No invalidation happens on ticker arrival; account events
        # are the explicit cache-refresh seam.
        context = await self._context_provider(state)
        self._pending_entries.sync(context)
        await self._publish_managed_position_symbols(context)
        if state.symbol in context.pending_position_symbols:
            symbols = ",".join(sorted(context.pending_position_symbols))
            return f"pending_live_positions:{symbols}"
        if state.symbol in context.unmanaged_position_symbols:
            symbols = ",".join(sorted(context.unmanaged_position_symbols))
            return f"unmanaged_live_positions:{symbols}"
        if self._run_active:
            await self._exit_lane.start()
            await self._exit_lane.submit_quote(quote, state, context)
            return None
        outcome = await self._exit_processor.process_quote(
            quote,
            state,
            context,
        )
        if outcome.failure is not None:
            log.error(
                "live_quote_exit_failed",
                symbol=quote.symbol,
                reason=outcome.failure,
            )
        return outcome.failure

    async def process_closed_candle(
        self,
        event: ClosedCandle15mEvent,
        *,
        latest_quote: RealtimeMarketQuote | None = None,
    ) -> str | None:
        """Process one final 15m candle on the independent exit path."""

        if self._exit_manager is None or not self._exit_enabled:
            return None
        state = _market_state_for_closed_candle(
            event.candle,
            received_at=event.received_at,
            quote=latest_quote,
        )
        # All symbols closing at the same boundary share one synthetic state
        # bucket.  Reuse the provider's snapshot across that burst; account
        # events and order execution remain the explicit invalidation seams.
        context = await self._context_provider(state)
        self._pending_entries.sync(context)
        await self._publish_managed_position_symbols(context)
        if state.symbol in context.pending_position_symbols:
            symbols = ",".join(sorted(context.pending_position_symbols))
            return f"pending_live_positions:{symbols}"
        if state.symbol in context.unmanaged_position_symbols:
            symbols = ",".join(sorted(context.unmanaged_position_symbols))
            return f"unmanaged_live_positions:{symbols}"
        outcome = await self._exit_processor.process_closed_candle(
            event,
            state,
            context,
            latest_quote,
        )
        if outcome.failure is not None:
            log.error(
                "live_closed_candle_exit_failed",
                symbol=event.candle.symbol,
                reason=outcome.failure,
            )
        return outcome.failure

    async def process_grace_timeout(
        self,
        state: MarketState15s,
        *,
        now: datetime,
        latest_quote: RealtimeMarketQuote | None = None,
    ) -> str | None:
        """Run the wall-clock fallback for an expired candle grace order."""

        if self._exit_manager is None or not self._exit_enabled:
            return None
        context = await self._context_provider(state)
        self._pending_entries.sync(context)
        await self._publish_managed_position_symbols(context)
        if state.symbol in context.pending_position_symbols:
            symbols = ",".join(sorted(context.pending_position_symbols))
            return f"pending_live_positions:{symbols}"
        if state.symbol in context.unmanaged_position_symbols:
            symbols = ",".join(sorted(context.unmanaged_position_symbols))
            return f"unmanaged_live_positions:{symbols}"
        outcome = await self._exit_processor.process_grace_timeout(
            state,
            now,
            context,
            latest_quote,
        )
        if outcome.failure is not None:
            log.error(
                "live_grace_timeout_exit_failed",
                symbol=state.symbol,
                reason=outcome.failure,
            )
        return outcome.failure

    async def run(
        self,
        states: AsyncIterable[MarketState15s],
    ) -> LiveDaemonResult:
        self._run_active = True
        await self._checkpoint_coordinator.start()
        result: LiveDaemonResult | None = None
        exit_outcome = ExitLaneOutcome()
        try:
            if self._exit_manager is not None:
                await self._exit_lane.start()
            if self._config.scheduled_risk_window is not None:
                self._scheduled_task = asyncio.create_task(
                    self._scheduled_controller.run(),
                    name=f"live-scheduled-risk-window:{self._config.run_id}",
                )
            result = await self._run_market_loop(states)
        finally:
            if self._scheduled_task is not None:
                self._scheduled_task.cancel()
                await asyncio.gather(
                    self._scheduled_task,
                    return_exceptions=True,
                )
                self._scheduled_task = None
            if self._exit_manager is not None:
                exit_outcome = await self._exit_lane.stop()
            await self._checkpoint_coordinator.stop()
            self._run_active = False
        if result is None:
            raise RuntimeError("live daemon stopped without a result")
        return replace(
            result,
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
            halt_reason=(
                result.halt_reason
                or (
                    exit_outcome.failure
                    if exit_outcome.fatal_failure
                    else None
                )
            ),
        )

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
        self._entry_lane.reset()
        processed = approved = submitted = 0
        final_state_at: datetime | None = None
        last_reconciled_bucket: datetime | None = None
        max_gap_seconds = _strategy_max_gap_seconds(self._strategy)
        async for prefetched in self._context_prefetcher.stream(states):
            state = prefetched.state
            self._runtime_cache.prune(
                now=self._clock(),
                current_symbol=state.symbol,
                active_symbols=self._entry_lane.entry_symbols,
            )
            self._scheduled_controller.observe_state(state)
            if self._config.scheduled_risk_window is not None:
                try:
                    await self.process_scheduled_risk_window(
                        now=self._clock()
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    # The independent wall-clock task will retry this control
                    # path.  Keep this state iteration alive, but the
                    # schedule gate remains fail-closed.
                    log.exception(
                        "live_inline_scheduled_risk_window_failed",
                        run_id=self._config.run_id,
                        error_type=type(error).__name__,
                    )
            if self._telemetry is not None:
                await self._telemetry.market_state_received(
                    state,
                    occurred_at=prefetched.received_at,
                    lane=LIVE_LANE_ENTRY,
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
                not self._config.reconcile_once_per_bucket
                or last_reconciled_bucket != state.bucket_start
            ):
                try:
                    await self._reconcile_orders()
                    last_reconciled_bucket = state.bucket_start
                except Exception as error:
                    if _is_transient_runtime_error(error):
                        # Reconciliation is an eventual-consistency safety
                        # net.  A temporary database outage must not tear down
                        # the live process; the next bucket retries it and the
                        # gate remains fail-closed for entries meanwhile.
                        last_reconciled_bucket = state.bucket_start
                        log.warning(
                            "live_order_reconciliation_degraded",
                            run_id=self._config.run_id,
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
                        run_id=self._config.run_id,
                        symbol=state.symbol,
                        generation=gap_generation,
                    )
                self._strategy_gap_reset_generation_by_symbol[
                    state.symbol
                ] = gap_generation
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
                # Keep the strategy's in-memory indicators moving, but do not
                # authorize or submit anything without a fresh risk context.
                # Once PostgreSQL recovers, the next state reloads the full
                # context and trading resumes without a process restart.
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
                    run_id=self._config.run_id,
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
                            run_id=self._config.run_id,
                            reasons=gate.reasons,
                        )
                        self._last_transient_gate_reasons = gate.reasons
                    # Process the state for indicator continuity while the
                    # risk gate is closed.  No entry or exit is evaluated.
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
                and self._exit_enabled
                and self._exit_manager.uses_market_state_exit
            ):
                await self._exit_lane.submit_market(state, context)
                # Give the independent exit worker a scheduling opportunity
                # without waiting for network-backed candle evaluation.
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
                    run_id=self._config.run_id,
                    symbol=plan.symbol,
                    client_order_id=plan.client_order_id,
                )
                continue
            if not result.state.terminal:
                return "orphan_cancel_not_confirmed"
        return None

def _market_state_for_closed_candle(
    candle: ClosedCandle15m,
    *,
    received_at: datetime,
    quote: RealtimeMarketQuote | None,
) -> MarketState15s:
    bid_price = quote.bid_price if quote is not None else None
    ask_price = quote.ask_price if quote is not None else None
    if bid_price is not None and ask_price is not None:
        spread = ask_price - bid_price
        midpoint = (bid_price + ask_price) / Decimal("2")
    else:
        spread = None
        midpoint = candle.close_price
    return MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="live",
        symbol=candle.symbol,
        bucket_start=candle.candle_end - timedelta(seconds=15),
        bucket_end=candle.candle_end,
        open_price=candle.open_price,
        high_price=None,
        low_price=None,
        close_price=candle.close_price,
        trade_count=0,
        trade_notional=Decimal("0"),
        aggressive_buy_notional=Decimal("0"),
        aggressive_sell_notional=Decimal("0"),
        last_bid_price=bid_price,
        last_ask_price=ask_price,
        spread=spread,
        midpoint=midpoint,
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=midpoint,
        closed_kline_count=1,
        source_event_count=1,
        first_received_at=received_at,
        last_received_at=received_at,
    )


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
