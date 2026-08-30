import asyncio
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Mapping,
)
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from inspect import Parameter, signature
from time import perf_counter
from typing import Protocol

import structlog
from sqlalchemy.exc import SQLAlchemyError

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.market.models import (
    MarketState15s,
    RealtimeMarketQuote,
)
from crypto_momentum_lab.domain.risk import (
    RiskConfigSnapshot,
    RiskDecision,
    RiskEvaluation,
    RiskHalt,
    StrategyLiveState,
    TradingLease,
)
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategyCheckpoint,
    StrategyDecision,
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
from crypto_momentum_lab.live_rollout.checkpoint_writer import CheckpointWriter
from crypto_momentum_lab.live_rollout.closed_candle_feed import (
    ClosedCandle15mEvent,
)
from crypto_momentum_lab.live_rollout.exits import (
    LiveExitCancellationRequest,
    LiveExitManager,
    LiveExitRequest,
    ManagedLivePosition,
)
from crypto_momentum_lab.live_rollout.gates import (
    LiveGateContext,
    evaluate_live_gate,
    order_state_is_uncertain,
)
from crypto_momentum_lab.live_rollout.limits import (
    FixedLiveLimits,
    LiveLimitContext,
    evaluate_fixed_live_limits,
)
from crypto_momentum_lab.live_rollout.signal_recorder import (
    LiveSignalRecorderPort,
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
from crypto_momentum_lab.strategy_runner.position_exit import ClosedCandle15m

log = structlog.get_logger()


class LiveRuntimeStrategy(Protocol):
    def on_market_state(self, state: MarketState15s) -> StrategyDecision: ...

    def checkpoint(
        self,
        *,
        include_market_state_buffers: bool = True,
    ) -> StrategyCheckpoint: ...

    def warm_market_state(self, state: MarketState15s) -> None: ...


class LiveDaemonRepository(Protocol):
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
    ) -> PreparedOrderSubmission: ...

    async def save_checkpoint(
        self,
        run_id: str,
        checkpoint: StrategyCheckpoint,
        saved_at: datetime,
    ) -> None: ...


class LiveEntryOrderLifecycle(Protocol):
    async def track(
        self,
        plan: OrderExecutionPlan,
        result: OrderExecutionResult,
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
    entry_order_type: EntryType = EntryType.LIMIT
    entry_limit_ttl_seconds: int = 900

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("run_id must not be empty")
        if self.resize_tolerance < 0 or self.resize_tolerance >= 1:
            raise ValueError("resize_tolerance must be in [0, 1)")
        if self.checkpoint_every_states <= 0:
            raise ValueError("checkpoint_every_states must be positive")
        if not isinstance(self.reconcile_once_per_bucket, bool):
            raise TypeError("reconcile_once_per_bucket must be a bool")
        if self.entry_symbol_refresh_seconds <= 0:
            raise ValueError("entry_symbol_refresh_seconds must be positive")
        if not isinstance(self.entry_order_type, EntryType):
            raise TypeError("entry_order_type must be an EntryType")
        if self.entry_limit_ttl_seconds < 601:
            raise ValueError("entry_limit_ttl_seconds must be at least 601")


@dataclass(frozen=True, slots=True)
class LiveEntryFilterContext:
    entry_price: Decimal | None
    ema5: Decimal | None = None
    ema10: Decimal | None = None


@dataclass(frozen=True, slots=True)
class LiveDaemonRuntimeContext:
    now: datetime
    gate_context: LiveGateContext
    active_lease: TradingLease | None
    account_state: ExecutionAccountStatus
    account_observed_at: datetime | None
    open_position_symbols: frozenset[str] | None
    realized_pnl: Decimal | None
    unrealized_pnl: Decimal | None
    gross_exposure: Decimal | None
    active_halts: tuple[RiskHalt, ...]
    unresolved_order_states: tuple[ExchangeOrderState, ...]
    risk_config: RiskConfigSnapshot
    strategy_state: StrategyLiveState
    trading_rules: dict[str, SymbolTradingRules]
    managed_positions: tuple[ManagedLivePosition, ...] = ()
    unmanaged_position_symbols: frozenset[str] = frozenset()
    unresolved_orders: tuple[PersistedExchangeOrder, ...] = ()


@dataclass(frozen=True, slots=True)
class LiveDaemonResult:
    processed_state_count: int
    approved_intent_count: int
    submitted_order_count: int
    halt_reason: str | None
    final_state_at: datetime | None


@dataclass(frozen=True, slots=True)
class _PrefetchedContext:
    state: MarketState15s
    generation: int
    received_at: datetime
    context: LiveDaemonRuntimeContext | None
    error: Exception | None


@dataclass(slots=True)
class _PendingContext:
    state: MarketState15s
    generation: int
    received_at: datetime
    task: asyncio.Task[LiveDaemonRuntimeContext]


@dataclass(frozen=True, slots=True)
class _ExitLaneOutcome:
    approved_intent_count: int = 0
    submitted_order_count: int = 0
    failure: str | None = None

    def merge(self, other: "_ExitLaneOutcome") -> "_ExitLaneOutcome":
        return _ExitLaneOutcome(
            approved_intent_count=(
                self.approved_intent_count + other.approved_intent_count
            ),
            submitted_order_count=(
                self.submitted_order_count + other.submitted_order_count
            ),
            failure=self.failure or other.failure,
        )


@dataclass(frozen=True, slots=True)
class _ExitLaneWork:
    state: MarketState15s
    context: LiveDaemonRuntimeContext
    completion: asyncio.Future[_ExitLaneOutcome] | None = None


@dataclass(frozen=True, slots=True)
class _QuoteLaneWork:
    quote: RealtimeMarketQuote
    state: MarketState15s
    context: LiveDaemonRuntimeContext
    completion: asyncio.Future[_ExitLaneOutcome] | None = None


class _ExitExecutionLane:
    """Run account exits independently from the market-state consumer.

    The market worker deliberately keeps only the newest pending state for a
    symbol.  A slow candle lookup for one symbol therefore cannot build a
    queue of stale exit decisions for that symbol.  Account events have a
    separate worker so they are not queued behind unrelated market work.
    """

    _MARKET_WORKER_COUNT = 4

    def __init__(
        self,
        processor: Callable[
            [MarketState15s, LiveDaemonRuntimeContext],
            Awaitable[_ExitLaneOutcome],
        ],
        quote_processor: Callable[
            [RealtimeMarketQuote, MarketState15s, LiveDaemonRuntimeContext],
            Awaitable[_ExitLaneOutcome],
        ],
    ) -> None:
        self._processor = processor
        self._quote_processor = quote_processor
        self._started = False
        self._account_queue: asyncio.Queue[_ExitLaneWork | None] | None = None
        self._market_queue: asyncio.Queue[str | None] | None = None
        self._quote_queue: asyncio.Queue[str | None] | None = None
        self._market_state_lock: asyncio.Lock | None = None
        self._market_latest: dict[str, _ExitLaneWork] = {}
        self._market_enqueued: set[str] = set()
        self._quote_latest: dict[str, _QuoteLaneWork] = {}
        self._quote_enqueued: set[str] = set()
        self._account_worker: asyncio.Task[None] | None = None
        self._market_workers: tuple[asyncio.Task[None], ...] = ()
        self._quote_workers: tuple[asyncio.Task[None], ...] = ()
        self._idle: asyncio.Event | None = None
        self._outstanding_work = 0
        self._outcome = _ExitLaneOutcome()

    @property
    def started(self) -> bool:
        return self._started

    @property
    def failure(self) -> str | None:
        return self._outcome.failure

    async def start(self) -> None:
        if self._started:
            return
        self._account_queue = asyncio.Queue()
        self._market_queue = asyncio.Queue()
        self._quote_queue = asyncio.Queue()
        self._market_state_lock = asyncio.Lock()
        self._market_latest = {}
        self._market_enqueued = set()
        self._quote_latest = {}
        self._quote_enqueued = set()
        self._outstanding_work = 0
        self._outcome = _ExitLaneOutcome()
        self._idle = asyncio.Event()
        self._idle.set()
        self._started = True
        self._account_worker = asyncio.create_task(
            self._run_account_worker(),
            name="live-exit-account-worker",
        )
        self._market_workers = tuple(
            asyncio.create_task(
                self._run_market_worker(),
                name=f"live-exit-market-worker-{index}",
            )
            for index in range(self._MARKET_WORKER_COUNT)
        )
        self._quote_workers = tuple(
            asyncio.create_task(
                self._run_quote_worker(),
                name=f"live-exit-quote-worker-{index}",
            )
            for index in range(self._MARKET_WORKER_COUNT)
        )

    async def submit_account(
        self,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
    ) -> _ExitLaneOutcome:
        if not self._started or self._account_queue is None or self._idle is None:
            raise RuntimeError("exit lane is not started")
        completion: asyncio.Future[_ExitLaneOutcome] = (
            asyncio.get_running_loop().create_future()
        )
        self._outstanding_work += 1
        self._idle.clear()
        await self._account_queue.put(_ExitLaneWork(state, context, completion))
        return await completion

    async def submit_market(
        self,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
    ) -> None:
        if (
            not self._started
            or self._market_queue is None
            or self._market_state_lock is None
            or self._idle is None
        ):
            raise RuntimeError("exit lane is not started")
        async with self._market_state_lock:
            if state.symbol not in self._market_latest:
                self._outstanding_work += 1
            self._idle.clear()
            self._market_latest[state.symbol] = _ExitLaneWork(state, context)
            if state.symbol not in self._market_enqueued:
                self._market_enqueued.add(state.symbol)
                self._market_queue.put_nowait(state.symbol)

    async def submit_quote(
        self,
        quote: RealtimeMarketQuote,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
        *,
        wait: bool = False,
    ) -> _ExitLaneOutcome | None:
        if (
            not self._started
            or self._quote_queue is None
            or self._market_state_lock is None
            or self._idle is None
        ):
            raise RuntimeError("exit lane is not started")
        completion: asyncio.Future[_ExitLaneOutcome] | None = None
        if wait:
            completion = asyncio.get_running_loop().create_future()
        async with self._market_state_lock:
            if quote.symbol not in self._quote_latest:
                self._outstanding_work += 1
            self._idle.clear()
            self._quote_latest[quote.symbol] = _QuoteLaneWork(
                quote,
                state,
                context,
                completion,
            )
            if quote.symbol not in self._quote_enqueued:
                self._quote_enqueued.add(quote.symbol)
                self._quote_queue.put_nowait(quote.symbol)
        if completion is None:
            return None
        return await completion

    async def stop(self) -> _ExitLaneOutcome:
        if not self._started:
            return _ExitLaneOutcome()
        await self.drain()
        account_queue = self._account_queue
        market_queue = self._market_queue
        quote_queue = self._quote_queue
        account_worker = self._account_worker
        market_workers = self._market_workers
        quote_workers = self._quote_workers
        if account_queue is not None:
            account_queue.put_nowait(None)
        if market_queue is not None:
            for _ in market_workers:
                market_queue.put_nowait(None)
        if quote_queue is not None:
            for _ in quote_workers:
                quote_queue.put_nowait(None)
        workers: tuple[asyncio.Task[None], ...] = (
            (account_worker,) if account_worker is not None else ()
        )
        workers += market_workers
        workers += quote_workers
        if workers:
            await asyncio.gather(*workers)
        outcome = self._outcome
        self._started = False
        self._market_workers = ()
        self._quote_workers = ()
        return outcome

    async def drain(self) -> None:
        if self._started and self._idle is not None:
            await self._idle.wait()

    async def _run_account_worker(self) -> None:
        assert self._account_queue is not None
        while True:
            work = await self._account_queue.get()
            if work is None:
                return
            outcome = await self._run_work(work)
            self._record_outcome(outcome)
            if work.completion is not None and not work.completion.done():
                work.completion.set_result(outcome)
            self._outstanding_work -= 1
            self._mark_idle_if_ready()

    async def _run_market_worker(self) -> None:
        assert self._market_queue is not None
        assert self._market_state_lock is not None
        while True:
            symbol = await self._market_queue.get()
            if symbol is None:
                return
            async with self._market_state_lock:
                work = self._market_latest.pop(symbol, None)
                self._market_enqueued.discard(symbol)
            if work is None:
                self._mark_idle_if_ready()
                continue
            outcome = await self._run_work(work)
            self._record_outcome(outcome)
            self._outstanding_work -= 1
            self._mark_idle_if_ready()

    async def _run_quote_worker(self) -> None:
        assert self._quote_queue is not None
        assert self._market_state_lock is not None
        while True:
            symbol = await self._quote_queue.get()
            if symbol is None:
                return
            async with self._market_state_lock:
                work = self._quote_latest.pop(symbol, None)
                self._quote_enqueued.discard(symbol)
            if work is None:
                self._mark_idle_if_ready()
                continue
            outcome = await self._run_quote_work(work)
            self._record_outcome(outcome)
            if work.completion is not None and not work.completion.done():
                work.completion.set_result(outcome)
            self._outstanding_work -= 1
            self._mark_idle_if_ready()

    async def _run_work(self, work: _ExitLaneWork) -> _ExitLaneOutcome:
        try:
            return await self._processor(work.state, work.context)
        except Exception as error:
            log.exception(
                "live_exit_lane_work_failed",
                symbol=work.state.symbol,
                error_type=type(error).__name__,
            )
            return _ExitLaneOutcome(
                failure=f"exit_execution_failed:{type(error).__name__}"
            )

    async def _run_quote_work(self, work: _QuoteLaneWork) -> _ExitLaneOutcome:
        try:
            return await self._quote_processor(
                work.quote,
                work.state,
                work.context,
            )
        except Exception as error:
            log.exception(
                "live_quote_exit_lane_work_failed",
                symbol=work.quote.symbol,
                error_type=type(error).__name__,
            )
            return _ExitLaneOutcome(
                failure=f"quote_exit_execution_failed:{type(error).__name__}"
            )

    def _record_outcome(self, outcome: _ExitLaneOutcome) -> None:
        self._outcome = self._outcome.merge(outcome)
        if outcome.failure is not None:
            log.error(
                "live_exit_lane_failed_closed",
                reason=outcome.failure,
            )

    def _mark_idle_if_ready(self) -> None:
        if (
            self._idle is not None
            and self._outstanding_work == 0
        ):
            self._idle.set()


class LiveStrategyDaemon:
    def __init__(
        self,
        *,
        strategy: LiveRuntimeStrategy,
        risk_gateway: RiskGateway,
        limits: FixedLiveLimits,
        repository: LiveDaemonRepository,
        state_machine: OrderExecutionPort,
        context_provider: Callable[
            [MarketState15s],
            Awaitable[LiveDaemonRuntimeContext],
        ],
        config: LiveDaemonConfig,
        exit_manager: LiveExitManager | None = None,
        reconcile_orders: Callable[[], Awaitable[None]] | None = None,
        telemetry: LiveTelemetrySink | None = None,
        signal_recorder: LiveSignalRecorderPort | None = None,
        entry_order_lifecycle: LiveEntryOrderLifecycle | None = None,
        clock: Callable[[], datetime] | None = None,
        on_managed_position_symbols: (
            Callable[[frozenset[str]], Awaitable[None]] | None
        ) = None,
    ) -> None:
        self._strategy = strategy
        self._risk_gateway = risk_gateway
        self._limits = limits
        self._repository = repository
        self._state_machine = state_machine
        self._context_provider = context_provider
        self._config = config
        self._exit_manager = exit_manager
        self._reconcile_orders = reconcile_orders
        self._exit_symbol_locks: dict[str, asyncio.Lock] = {}
        self._quote_symbol_locks: dict[str, asyncio.Lock] = {}
        self._exit_lane = _ExitExecutionLane(
            self._process_exit_work,
            self._process_quote_work,
        )
        self._checkpoint_writer = CheckpointWriter(
            run_id=config.run_id,
            persist=self._repository.save_checkpoint,
        )
        self._telemetry = telemetry
        self._signal_recorder = signal_recorder
        self._entry_order_lifecycle = entry_order_lifecycle
        self._clock = clock or (lambda: datetime.now(tz=UTC))
        self._on_managed_position_symbols = on_managed_position_symbols
        self._managed_position_symbols: frozenset[str] = frozenset()
        self._context_generation = 0
        self._run_active = False
        self._entry_enabled = True
        self._exit_enabled = True
        self._entry_enabled_reason = "initializing"
        self._pending_entry_plans: dict[
            str,
            tuple[OrderExecutionPlan, Decimal],
        ] = {}
        self._market_gap_generation = 0
        self._strategy_gap_reset_generation_by_symbol: dict[str, int] = {}
        self._last_transient_gate_reasons: tuple[str, ...] | None = None

    async def _publish_managed_position_symbols(
        self,
        context: LiveDaemonRuntimeContext,
    ) -> None:
        symbols = context.open_position_symbols or frozenset()
        self._managed_position_symbols = symbols
        if self._on_managed_position_symbols is not None:
            await self._on_managed_position_symbols(symbols)

    def _invalidate_context_cache(self) -> None:
        self._context_generation += 1
        invalidate = getattr(self._context_provider, "invalidate_cache", None)
        if callable(invalidate):
            invalidate()

    @property
    def entry_enabled(self) -> bool:
        return self._entry_enabled

    @property
    def entry_enabled_reason(self) -> str:
        return self._entry_enabled_reason

    @property
    def exit_enabled(self) -> bool:
        return self._exit_enabled

    @property
    def managed_position_symbols(self) -> frozenset[str]:
        return self._managed_position_symbols

    def set_entry_enabled(self, enabled: bool, *, reason: str) -> None:
        if not isinstance(enabled, bool):
            raise TypeError("enabled must be a bool")
        if not reason.strip():
            raise ValueError("reason must not be empty")
        if (
            self._entry_enabled == enabled
            and self._entry_enabled_reason == reason
        ):
            return
        state_changed = self._entry_enabled != enabled
        self._entry_enabled = enabled
        self._entry_enabled_reason = reason
        log.warning(
            "live_entry_lane_state_changed",
            enabled=enabled,
            state_changed=state_changed,
            reason=reason,
            run_id=self._config.run_id,
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

        if plan.reduce_only:
            return
        state = getattr(event, "state", None)
        if state is not None and getattr(state, "terminal", False):
            self._pending_entry_plans.pop(plan.client_order_id, None)

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
        self._sync_pending_entry_plans(context)
        await self._publish_managed_position_symbols(context)
        if context.unmanaged_position_symbols:
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
                outcome = await self._process_quote_work(quote, state, context)
        else:
            outcome = (
                await self._process_exit_work(state, context)
                if quote is None
                else await self._process_quote_work(quote, state, context)
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
        self._sync_pending_entry_plans(context)
        await self._publish_managed_position_symbols(context)
        if context.unmanaged_position_symbols:
            symbols = ",".join(sorted(context.unmanaged_position_symbols))
            return f"unmanaged_live_positions:{symbols}"
        if self._run_active:
            await self._exit_lane.start()
            await self._exit_lane.submit_quote(quote, state, context)
            return None
        outcome = await self._process_quote_work(quote, state, context)
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
        self._sync_pending_entry_plans(context)
        await self._publish_managed_position_symbols(context)
        if context.unmanaged_position_symbols:
            symbols = ",".join(sorted(context.unmanaged_position_symbols))
            return f"unmanaged_live_positions:{symbols}"
        outcome = await self._process_closed_candle_work(
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
        self._sync_pending_entry_plans(context)
        await self._publish_managed_position_symbols(context)
        if context.unmanaged_position_symbols:
            symbols = ",".join(sorted(context.unmanaged_position_symbols))
            return f"unmanaged_live_positions:{symbols}"
        outcome = await self._process_grace_timeout_work(
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
        await self._checkpoint_writer.start()
        result: LiveDaemonResult | None = None
        exit_outcome = _ExitLaneOutcome()
        try:
            if self._exit_manager is not None:
                await self._exit_lane.start()
            result = await self._run_market_loop(states)
        finally:
            if self._exit_manager is not None:
                exit_outcome = await self._exit_lane.stop()
            await self._checkpoint_writer.stop()
            self._run_active = False
        if result is None:
            raise RuntimeError("live daemon stopped without a result")
        return replace(
            result,
            approved_intent_count=(
                result.approved_intent_count
                + exit_outcome.approved_intent_count
            ),
            submitted_order_count=(
                result.submitted_order_count
                + exit_outcome.submitted_order_count
            ),
            halt_reason=result.halt_reason or exit_outcome.failure,
        )

    async def _states_with_prefetched_context(
        self,
        states: AsyncIterable[MarketState15s],
    ) -> AsyncIterator[_PrefetchedContext]:
        """Read one state ahead and overlap its context I/O with live work.

        Strategy evaluation remains strictly ordered.  Only the context
        lookup is overlapped.  A generation change caused by an account event
        or a completed order invalidates the prefetched result before it can
        authorize a decision.
        """

        queue: asyncio.Queue[_PendingContext | None] = asyncio.Queue(maxsize=2)
        pending_tasks: set[asyncio.Task[LiveDaemonRuntimeContext]] = set()
        producer_error: BaseException | None = None

        async def producer() -> None:
            nonlocal producer_error
            try:
                async for state in states:
                    generation = self._context_generation
                    received_at = self._clock()
                    context_task: asyncio.Task[LiveDaemonRuntimeContext] = (
                        asyncio.ensure_future(self._context_provider(state))
                    )
                    pending_tasks.add(context_task)
                    await queue.put(
                        _PendingContext(
                            state=state,
                            generation=generation,
                            received_at=received_at,
                            task=context_task,
                        )
                    )
            except asyncio.CancelledError:
                raise
            except BaseException as error:
                producer_error = error
            finally:
                await queue.put(None)

        producer_task = asyncio.create_task(
            producer(),
            name="live-market-state-prefetch",
        )
        try:
            while True:
                pending = await queue.get()
                if pending is None:
                    await producer_task
                    if producer_error is not None:
                        raise producer_error
                    return
                try:
                    context = await pending.task
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    yield _PrefetchedContext(
                        state=pending.state,
                        generation=pending.generation,
                        received_at=pending.received_at,
                        context=None,
                        error=error,
                    )
                else:
                    yield _PrefetchedContext(
                        state=pending.state,
                        generation=pending.generation,
                        received_at=pending.received_at,
                        context=context,
                        error=None,
                    )
                finally:
                    pending_tasks.discard(pending.task)
        finally:
            if not producer_task.done():
                producer_task.cancel()
            await asyncio.gather(producer_task, return_exceptions=True)
            for task in pending_tasks:
                if not task.done():
                    task.cancel()
            if pending_tasks:
                await asyncio.gather(*pending_tasks, return_exceptions=True)

    def _record_strategy_decision(
        self,
        *,
        decision: StrategyDecision,
        state: MarketState15s,
        recorded_at: datetime,
        context: LiveDaemonRuntimeContext | None = None,
        gate_reasons: tuple[str, ...] = (),
        entry_symbols: frozenset[str] | None = None,
        entry_filter_context: LiveEntryFilterContext | None = None,
        filter_context: Mapping[str, object] | None = None,
    ) -> None:
        recorder = self._signal_recorder
        if recorder is None:
            return
        candidate_filter_results: dict[str, object] = {}
        for candidate in decision.candidates:
            rejection_reason = _live_entry_candidate_rejection_reason(
                candidate,
                entry_enabled=self._entry_enabled,
                entry_long_only=self._config.entry_long_only,
                entry_symbols=entry_symbols,
                context=entry_filter_context,
                require_price_above_ema5=self._config.require_price_above_ema5,
                require_price_above_ema10=self._config.require_price_above_ema10,
                now=recorded_at,
            )
            candidate_filter_results[candidate.candidate_id] = {
                "symbol": candidate.symbol,
                "side": _enum_text(candidate.side),
                "reduce_only": candidate.reduce_only,
                "passed": rejection_reason is None,
                "rejection_reason": rejection_reason,
            }
        details: dict[str, object] = dict(filter_context or {})
        details.update(
            {
                "entry_enabled": self._entry_enabled,
                "entry_enabled_reason": self._entry_enabled_reason,
                "entry_long_only": self._config.entry_long_only,
                "entry_symbol_pool_configured": entry_symbols is not None,
                "entry_symbol_pool_size": (
                    None if entry_symbols is None else len(entry_symbols)
                ),
                "require_price_above_ema5": (
                    self._config.require_price_above_ema5
                ),
                "require_price_above_ema10": (
                    self._config.require_price_above_ema10
                ),
                "gate_reasons": list(gate_reasons),
                "signal_count": len(decision.signals),
                "candidate_count": len(decision.candidates),
                "rejection_count": len(decision.rejections),
                "strategy_rejections": [
                    {
                        "reason": _enum_text(rejection.reason),
                        "symbol": rejection.symbol,
                        "bucket_start": rejection.bucket_start,
                        "details": rejection.details,
                    }
                    for rejection in decision.rejections
                ],
                "entry_filter_values": _entry_filter_values(
                    entry_filter_context
                ),
                "market_state_bucket_start": state.bucket_start,
                "market_state_bucket_end": state.bucket_end,
                "market_state_last_received_at": state.last_received_at,
                "market_state_age_seconds": round(
                    _market_state_age_seconds(state, recorded_at),
                    3,
                ),
                "candidate_filter_results": candidate_filter_results,
            }
        )
        try:
            recorder.record_decision(
                decision=decision,
                state=state,
                recorded_at=recorded_at,
                account_context=_live_signal_account_context(
                    context,
                    gate_reasons=gate_reasons,
                ),
                filter_context=details,
            )
        except Exception as error:
            # A third-party recorder implementation is observational only.
            # Never let it alter the decision or order path.
            log.warning(
                "live_strategy_signal_recorder_failed",
                run_id=self._config.run_id,
                error_type=type(error).__name__,
            )

    async def _run_market_loop(
        self,
        states: AsyncIterable[MarketState15s],
    ) -> LiveDaemonResult:
        processed = approved = submitted = 0
        final_state_at: datetime | None = None
        checkpoint_dirty = False
        last_checkpoint_saved_at: datetime | None = None
        last_reconciled_bucket: datetime | None = None
        last_processed_at_by_symbol = dict(
            _checkpoint_for_persistence(self._strategy)
            .last_processed_at_by_symbol
        )
        max_gap_seconds = _strategy_max_gap_seconds(self._strategy)
        entry_symbols: frozenset[str] | None = None
        entry_symbols_loaded_at: datetime | None = None
        async for prefetched in self._states_with_prefetched_context(states):
            state = prefetched.state
            if self._telemetry is not None:
                await self._telemetry.market_state_received(
                    state,
                    occurred_at=prefetched.received_at,
                    lane=LIVE_LANE_ENTRY,
                )
            exit_lane_failure = self._exit_lane.failure
            if exit_lane_failure is not None:
                await self._save_final_checkpoint(
                    dirty=checkpoint_dirty,
                    saved_at=last_checkpoint_saved_at,
                )
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
                    await self._save_final_checkpoint(
                        dirty=checkpoint_dirty,
                        saved_at=last_checkpoint_saved_at,
                    )
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
                    last_processed_at_by_symbol.pop(state.symbol, None)
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
                last_processed_at=last_processed_at_by_symbol.get(state.symbol),
                max_gap_seconds=max_gap_seconds,
            )
            context_reloaded = prefetched.generation != self._context_generation
            try:
                if context_reloaded:
                    context = await self._context_provider(state)
                elif prefetched.error is not None:
                    raise prefetched.error
                else:
                    if prefetched.context is None:
                        raise RuntimeError("prefetched live context is missing")
                    context = prefetched.context
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if not _is_transient_runtime_error(error):
                    raise
                # Keep the strategy's in-memory indicators moving, but do not
                # authorize or submit anything without a fresh risk context.
                # Once PostgreSQL recovers, the next state reloads the full
                # context and trading resumes without a process restart.
                decision = self._strategy.on_market_state(state)
                self._record_strategy_decision(
                    decision=decision,
                    state=state,
                    recorded_at=self._clock(),
                    filter_context={
                        "context_available": False,
                        "context_error_type": type(error).__name__,
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
                last_processed_at_by_symbol[state.symbol] = state.bucket_start
                checkpoint_dirty = True
                last_checkpoint_saved_at = state.bucket_end
                log.warning(
                    "live_runtime_context_degraded",
                    run_id=self._config.run_id,
                    symbol=state.symbol,
                    error_type=type(error).__name__,
                )
                continue
            self._sync_pending_entry_plans(context)
            if self._telemetry is not None:
                await self._telemetry.context_ready(
                    state,
                    occurred_at=self._clock(),
                    prefetched=not context_reloaded,
                    reloaded=context_reloaded,
                )
            await self._publish_managed_position_symbols(context)
            gate = evaluate_live_gate(
                replace(
                    context.gate_context,
                    now=context.now,
                    active_lease=context.active_lease,
                    account_state=context.account_state,
                    active_halts=context.active_halts,
                    unresolved_order_states=context.unresolved_order_states,
                )
            )
            if self._telemetry is not None:
                await self._telemetry.gate_evaluated(
                    state,
                    occurred_at=self._clock(),
                    approved=gate.approved,
                    reasons=gate.reasons,
                )
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
                    self._record_strategy_decision(
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
                    last_processed_at_by_symbol[state.symbol] = state.bucket_start
                    checkpoint_dirty = True
                    last_checkpoint_saved_at = context.now
                    continue
                self._last_transient_gate_reasons = None
                await self._save_final_checkpoint(
                    dirty=checkpoint_dirty,
                    saved_at=last_checkpoint_saved_at,
                )
                return LiveDaemonResult(
                    processed,
                    approved,
                    submitted,
                    f"live_gate:{','.join(gate.reasons)}",
                    final_state_at,
                )
            self._last_transient_gate_reasons = None
            if context.unmanaged_position_symbols:
                await self._save_final_checkpoint(
                    dirty=checkpoint_dirty,
                    saved_at=last_checkpoint_saved_at,
                )
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
                await self._save_final_checkpoint(
                    dirty=checkpoint_dirty,
                    saved_at=last_checkpoint_saved_at,
                )
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
                    await self._save_final_checkpoint(
                        dirty=checkpoint_dirty,
                        saved_at=last_checkpoint_saved_at,
                    )
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
            has_entry_candidates = self._entry_enabled and any(
                not candidate.reduce_only for candidate in decision.candidates
            )
            if has_entry_candidates and self._config.entry_symbol_loader is not None:
                if (
                    entry_symbols_loaded_at is None
                    or (state.bucket_start - entry_symbols_loaded_at).total_seconds()
                    >= self._config.entry_symbol_refresh_seconds
                ):
                    try:
                        entry_symbols = await self._config.entry_symbol_loader(
                            state.bucket_start
                        )
                    except Exception:
                        # Entry-pool lookup is fail-closed. Exit handling above
                        # still runs, so an outage cannot strand an open position.
                        entry_symbols = frozenset()
                    entry_symbols_loaded_at = state.bucket_start
            entry_filter_context = None
            if (
                has_entry_candidates
                and (
                    self._config.require_price_above_ema5
                    or self._config.require_price_above_ema10
                )
                and self._config.entry_filter_context_loader is not None
            ):
                try:
                    entry_filter_context = (
                        await self._config.entry_filter_context_loader(state)
                    )
                except Exception:
                    # Missing or stale EMA data must not authorize a live entry.
                    entry_filter_context = None
            if self._telemetry is not None:
                await self._telemetry.entry_filter_ready(
                    state,
                    occurred_at=self._clock(),
                    candidate_count=len(decision.candidates),
                    symbol_pool_loaded=(
                        not has_entry_candidates
                        or entry_symbols is not None
                    ),
                    ema_context_loaded=(
                        not has_entry_candidates
                        or not (
                            self._config.require_price_above_ema5
                            or self._config.require_price_above_ema10
                        )
                        or entry_filter_context is not None
                    ),
                )
            self._record_strategy_decision(
                decision=decision,
                state=state,
                recorded_at=decision_recorded_at,
                context=context,
                gate_reasons=gate.reasons,
                entry_symbols=entry_symbols,
                entry_filter_context=entry_filter_context,
            )
            if self._telemetry is not None:
                await self._telemetry.signal_recorded(
                    state,
                    occurred_at=self._clock(),
                    candidate_count=len(decision.candidates),
                )
            processed += 1
            final_state_at = state.bucket_start
            last_processed_at_by_symbol[state.symbol] = state.bucket_start
            checkpoint_dirty = True
            last_checkpoint_saved_at = context.now
            for candidate in decision.candidates:
                if _live_entry_candidate_rejection_reason(
                    candidate,
                    entry_enabled=self._entry_enabled,
                    entry_long_only=self._config.entry_long_only,
                    entry_symbols=entry_symbols,
                    context=entry_filter_context,
                    require_price_above_ema5=self._config.require_price_above_ema5,
                    require_price_above_ema10=self._config.require_price_above_ema10,
                    now=decision_recorded_at,
                ) is not None:
                    continue
                result = await self._execute_candidate(
                    candidate,
                    requested_quantity=None,
                    state=state,
                    context=context,
                )
                if result is not None:
                    self._invalidate_context_cache()
                    approved += 1
                    submitted += int(not result.suppressed)
                    if (
                        result.state
                        is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
                    ):
                        log.warning(
                            "live_order_outcome_pending_reconciliation",
                            run_id=self._config.run_id,
                            symbol=candidate.symbol,
                            client_order_id=result.client_order_id,
                        )
                        # The durable order state is now fail-closed.  Stop
                        # evaluating additional candidates from this snapshot;
                        # the next context load will keep entries blocked until
                        # reconciliation confirms the exchange state.
                        break
            if processed % self._config.checkpoint_every_states == 0:
                self._checkpoint_writer.submit(
                    _checkpoint_for_persistence(self._strategy),
                    context.now,
                )
                checkpoint_dirty = False
                last_checkpoint_saved_at = None
        await self._save_final_checkpoint(
            dirty=checkpoint_dirty,
            saved_at=last_checkpoint_saved_at,
        )
        return LiveDaemonResult(
            processed,
            approved,
            submitted,
            None,
            final_state_at,
        )

    async def _process_exit_work(
        self,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
    ) -> _ExitLaneOutcome:
        if (
            self._exit_manager is None
            or not self._exit_enabled
            or not self._exit_manager.uses_market_state_exit
        ):
            return _ExitLaneOutcome()
        if self._telemetry is not None:
            await self._telemetry.market_state_received(
                state,
                occurred_at=self._clock(),
                lane=LIVE_LANE_EXIT,
            )
        lock = self._exit_symbol_locks.setdefault(state.symbol, asyncio.Lock())
        async with lock:
            requests = await self._exit_manager.requests_for_state(
                state,
                context.managed_positions,
            )
            approved, submitted, failure = await self._process_exit_requests(
                requests,
                state=state,
                context=context,
            )
        return _ExitLaneOutcome(
            approved_intent_count=approved,
            submitted_order_count=submitted,
            failure=failure,
        )

    async def _process_closed_candle_work(
        self,
        event: ClosedCandle15mEvent,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
        latest_quote: RealtimeMarketQuote | None,
    ) -> _ExitLaneOutcome:
        if self._exit_manager is None or not self._exit_enabled:
            return _ExitLaneOutcome()
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
            requests = await self._exit_manager.requests_for_closed_candle(
                event.candle,
                context.managed_positions,
                latest_quote=latest_quote,
                received_at=event.received_at,
            )
            approved, submitted, failure = await self._process_exit_requests(
                requests,
                state=state,
                context=context,
                invalidate_context=False,
            )
        return _ExitLaneOutcome(
            approved_intent_count=approved,
            submitted_order_count=submitted,
            failure=failure,
        )

    async def _process_grace_timeout_work(
        self,
        state: MarketState15s,
        now: datetime,
        context: LiveDaemonRuntimeContext,
        latest_quote: RealtimeMarketQuote | None,
    ) -> _ExitLaneOutcome:
        if self._exit_manager is None or not self._exit_enabled:
            return _ExitLaneOutcome()
        lock = self._exit_symbol_locks.setdefault(state.symbol, asyncio.Lock())
        async with lock:
            requests = await self._exit_manager.requests_for_grace_timeout(
                now=now,
                state=state,
                positions=context.managed_positions,
                latest_quote=latest_quote,
            )
            approved, submitted, failure = await self._process_exit_requests(
                requests,
                state=state,
                context=context,
            )
        return _ExitLaneOutcome(
            approved_intent_count=approved,
            submitted_order_count=submitted,
            failure=failure,
        )

    async def _process_quote_work(
        self,
        quote: RealtimeMarketQuote,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
    ) -> _ExitLaneOutcome:
        if self._exit_manager is None or not self._exit_enabled:
            return _ExitLaneOutcome()
        # This lock is intentionally separate from the 15-minute candle exit
        # lock.  A slow REST candle lookup must never hold the realtime quote
        # lane behind it; the execution coordinator still serializes the
        # resulting reduce-only exchange commands per symbol.
        lock = self._quote_symbol_locks.setdefault(
            quote.symbol,
            asyncio.Lock(),
        )
        async with lock:
            requests = await self._exit_manager.requests_for_quote(
                quote,
                context.managed_positions,
            )
            approved, submitted, failure = await self._process_exit_requests(
                requests,
                state=state,
                context=context,
            )
        return _ExitLaneOutcome(
            approved_intent_count=approved,
            submitted_order_count=submitted,
            failure=failure,
        )

    async def _process_exit_requests(
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
                remaining = max(
                    Decimal("0"),
                    request.cancel_plan.quantity - cancel_result.executed_quantity,
                )
                if remaining <= 0:
                    continue
                result = await self._execute_candidate(
                    request.fallback_candidate,
                    requested_quantity=min(request.fallback_quantity, remaining),
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
            result = await self._execute_candidate(
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
                log.warning(
                    "live_exit_outcome_pending_reconciliation",
                    run_id=self._config.run_id,
                    symbol=request.candidate.symbol,
                    client_order_id=result.client_order_id,
                )
                return approved, submitted, None
        return approved, submitted, None

    async def _execute_candidate(
        self,
        candidate: OrderIntentCandidate,
        *,
        requested_quantity: Decimal | None,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
        reference_price: Decimal | None = None,
    ) -> OrderExecutionResult | None:
        execution_now = self._clock()
        if not candidate.reduce_only and candidate.expires_at <= execution_now:
            log.warning(
                "live_entry_candidate_expired_before_execution",
                run_id=self._config.run_id,
                candidate_id=candidate.candidate_id,
                symbol=candidate.symbol,
                candidate_expires_at=candidate.expires_at,
                execution_now=execution_now,
                market_state_bucket_end=state.bucket_end,
            )
            return None
        executable_candidate = self._prepare_entry_candidate(
            candidate,
            state=state,
            execution_now=execution_now,
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
        lane = LIVE_LANE_EXIT if executable_candidate.reduce_only else LIVE_LANE_ENTRY
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
        prepared_submission: PreparedOrderSubmission | None = None
        prepare_submission = getattr(self._repository, "prepare_submission", None)
        if callable(prepare_submission):
            prepared_submission = await prepare_submission(
                intent=executable_candidate,
                evaluation=evaluation,
                plan=plan,
                prepared_at=self._clock(),
            )
            intent_saved_at = prepared_submission.submitting_event.occurred_at
        else:
            await self._repository.save_approved_intent(
                executable_candidate,
                evaluation,
            )
            intent_saved_at = self._clock()
        if self._telemetry is not None:
            await self._telemetry.intent_saved(
                executable_candidate,
                state=state,
                occurred_at=intent_saved_at,
                lane=lane,
            )
        result = await self._state_machine.execute_approved_intent(
            plan,
            prepared_submission=prepared_submission,
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

    def _prepare_entry_candidate(
        self,
        candidate: OrderIntentCandidate,
        *,
        state: MarketState15s,
        execution_now: datetime,
    ) -> OrderIntentCandidate:
        if candidate.reduce_only or self._config.entry_order_type is EntryType.MARKET:
            return candidate
        signal_price = (
            candidate.limit_price
            or state.close_price
            or state.mark_price
            or state.midpoint
        )
        return replace(
            candidate,
            entry_type=EntryType.LIMIT,
            limit_price=signal_price,
            expires_at=execution_now
            + timedelta(seconds=self._config.entry_limit_ttl_seconds),
        )

    def _remember_pending_entry(
        self,
        plan: OrderExecutionPlan,
        result: OrderExecutionResult,
    ) -> None:
        if result.state.terminal or result.executed_quantity >= plan.quantity:
            self._pending_entry_plans.pop(plan.client_order_id, None)
            return
        self._pending_entry_plans[plan.client_order_id] = (
            plan,
            result.executed_quantity,
        )

    def _sync_pending_entry_plans(
        self,
        context: LiveDaemonRuntimeContext,
    ) -> None:
        persisted = {
            item.plan.client_order_id: item
            for item in context.unresolved_orders
            if not item.plan.reduce_only
        }
        for client_order_id, (plan, executed_quantity) in tuple(
            self._pending_entry_plans.items()
        ):
            item = persisted.get(client_order_id)
            if item is None:
                # Keep a just-submitted order until a fresh account/context
                # read confirms its terminal state. This avoids releasing a
                # reservation between two candidates in one market snapshot.
                if plan.expires_at is not None and plan.expires_at <= self._clock():
                    self._pending_entry_plans.pop(client_order_id, None)
                continue
            if item.state.terminal or item.executed_quantity >= plan.quantity:
                self._pending_entry_plans.pop(client_order_id, None)
            elif item.executed_quantity > executed_quantity:
                self._pending_entry_plans[client_order_id] = (
                    plan,
                    item.executed_quantity,
                )

    def _pending_entry_reservation(
        self,
        persisted_orders: tuple[PersistedExchangeOrder, ...],
    ) -> tuple[Decimal, frozenset[str]]:
        pending: dict[str, tuple[OrderExecutionPlan, Decimal]] = {
            item.plan.client_order_id: (item.plan, item.executed_quantity)
            for item in persisted_orders
            if not item.plan.reduce_only and not item.state.terminal
        }
        pending.update(self._pending_entry_plans)
        reserved_notional = Decimal("0")
        reserved_symbols: set[str] = set()
        for plan, executed_quantity in pending.values():
            remaining_quantity = max(
                Decimal("0"),
                plan.quantity - executed_quantity,
            )
            if remaining_quantity <= 0 or plan.price is None:
                continue
            reserved_notional += remaining_quantity * plan.price
            reserved_symbols.add(plan.symbol)
        return reserved_notional, frozenset(reserved_symbols)

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
                    "entry_enabled": self._entry_enabled,
                    "entry_enabled_reason": self._entry_enabled_reason,
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

    async def _save_final_checkpoint(
        self,
        *,
        dirty: bool,
        saved_at: datetime | None,
    ) -> None:
        if not dirty or saved_at is None:
            return
        await self._checkpoint_writer.save_now(
            _checkpoint_for_persistence(self._strategy),
            saved_at,
        )


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


def _checkpoint_for_persistence(strategy: LiveRuntimeStrategy) -> StrategyCheckpoint:
    """Build a compact checkpoint without breaking lightweight test adapters."""
    started = perf_counter()
    checkpoint_method = strategy.checkpoint
    parameters: Mapping[str, Parameter] | None = None
    try:
        parameters = signature(checkpoint_method).parameters
    except (TypeError, ValueError):
        pass
    if parameters is not None and "include_market_state_buffers" in parameters and (
        parameters["include_market_state_buffers"].kind
        in {Parameter.KEYWORD_ONLY, Parameter.POSITIONAL_OR_KEYWORD}
    ):
        checkpoint = checkpoint_method(include_market_state_buffers=False)
        log.info(
            "live_checkpoint_built",
            build_ms=round((perf_counter() - started) * 1000, 3),
            payload_keys=tuple(sorted(checkpoint.payload)),
        )
        return checkpoint

    checkpoint = checkpoint_method()
    payload = {
        key: value
        for key, value in checkpoint.payload.items()
        if key not in {"market_state_buffers", "signal_buffers"}
    }
    compact = replace(checkpoint, payload=payload)
    log.info(
        "live_checkpoint_built",
        build_ms=round((perf_counter() - started) * 1000, 3),
        payload_keys=tuple(sorted(compact.payload)),
    )
    return compact


def _min_notional(rules: SymbolTradingRules | None) -> Decimal | None:
    return None if rules is None else rules.min_notional


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


def _live_entry_candidate_passes(
    candidate: OrderIntentCandidate,
    *,
    context: LiveEntryFilterContext | None,
    require_price_above_ema5: bool,
    require_price_above_ema10: bool,
) -> bool:
    if candidate.reduce_only:
        return True
    if not require_price_above_ema5 and not require_price_above_ema10:
        return True
    if context is None or context.entry_price is None:
        return False
    if require_price_above_ema5 and (
        context.ema5 is None or context.entry_price <= context.ema5
    ):
        return False
    if require_price_above_ema10 and (
        context.ema10 is None or context.entry_price <= context.ema10
    ):
        return False
    return True


def _live_entry_candidate_rejection_reason(
    candidate: OrderIntentCandidate,
    *,
    entry_enabled: bool,
    entry_long_only: bool,
    entry_symbols: frozenset[str] | None,
    context: LiveEntryFilterContext | None,
    require_price_above_ema5: bool,
    require_price_above_ema10: bool,
    now: datetime | None = None,
) -> str | None:
    """Return the same entry-filter reason used by the live execution loop."""

    if candidate.reduce_only:
        return None
    if not entry_enabled:
        return "entry_disabled"
    if now is not None and candidate.expires_at <= now:
        return "candidate_expired"
    if (
        entry_long_only
        and getattr(candidate.side, "value", candidate.side) != "long"
    ):
        return "short_entries_disabled"
    if entry_symbols is not None and candidate.symbol not in entry_symbols:
        return "outside_entry_symbol_pool"
    if not _live_entry_candidate_passes(
        candidate,
        context=context,
        require_price_above_ema5=require_price_above_ema5,
        require_price_above_ema10=require_price_above_ema10,
    ):
        return "ema_filter_failed"
    return None


def _market_state_age_seconds(
    state: MarketState15s,
    now: datetime,
) -> float:
    return max(0.0, (now - state.bucket_end).total_seconds())


def _entry_filter_values(
    context: LiveEntryFilterContext | None,
) -> dict[str, object]:
    if context is None:
        return {
            "entry_price": None,
            "ema5": None,
            "ema10": None,
        }
    return {
        "entry_price": context.entry_price,
        "ema5": context.ema5,
        "ema10": context.ema10,
    }


def _live_signal_account_context(
    context: LiveDaemonRuntimeContext | None,
    *,
    gate_reasons: tuple[str, ...] = (),
) -> dict[str, object]:
    if context is None:
        return {
            "context_available": False,
            "gate_reasons": list(gate_reasons),
        }
    lease = context.active_lease
    return {
        "context_available": True,
        "account_state": _enum_text(context.account_state),
        "account_observed_at": context.account_observed_at,
        "realized_pnl": context.realized_pnl,
        "unrealized_pnl": context.unrealized_pnl,
        "gross_exposure": context.gross_exposure,
        "open_position_symbols": sorted(context.open_position_symbols or ()),
        "managed_position_symbols": sorted(
            position.symbol for position in context.managed_positions
        ),
        "unmanaged_position_symbols": sorted(
            context.unmanaged_position_symbols
        ),
        "active_halt_count": len(context.active_halts),
        "active_halt_reasons": [halt.reason for halt in context.active_halts],
        "unresolved_order_count": len(context.unresolved_order_states),
        "unresolved_order_states": [
            _enum_text(state) for state in context.unresolved_order_states
        ],
        "active_lease_state": (
            None if lease is None else _enum_text(lease.state)
        ),
        "active_lease_expires_at": (
            None if lease is None else lease.expires_at
        ),
        "risk_config": {
            "max_order_notional": context.risk_config.max_order_notional,
            "max_gross_notional": context.risk_config.max_gross_notional,
            "max_daily_loss": context.risk_config.max_daily_loss,
            "max_open_positions": context.risk_config.max_open_positions,
        },
        "gate_reasons": list(gate_reasons),
    }


def _enum_text(value: object) -> str:
    return str(getattr(value, "value", value))


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
