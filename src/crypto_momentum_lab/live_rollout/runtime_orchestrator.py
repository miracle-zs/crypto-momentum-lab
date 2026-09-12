"""Live runtime composition and long-running orchestration.

The CLI module resolves operator input and environment configuration.  This
module owns the runtime assembly and lifecycle of one live daemon.
"""

import asyncio
import os
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import structlog
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution import OrderExecutionPlan
from crypto_momentum_lab.domain.live_rollout import LiveSessionState
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.risk import RiskEvaluation, TradingLease
from crypto_momentum_lab.domain.strategy import (
    OrderIntentCandidate,
    RunMode,
    StrategyCheckpoint,
    StrategyRunIdentity,
)
from crypto_momentum_lab.execution_account.binance import BinanceUsdMTradeClient
from crypto_momentum_lab.execution_account.hub import (
    AccountEvent,
    WebSocketAccountEventSource,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
    OrderExecutionPort,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionStateMachine,
    PreparedOrderSubmission,
    SubmitPolicy,
)
from crypto_momentum_lab.execution_account.risk_control_hub import (
    RiskControlEvent,
    WebSocketRiskControlSource,
)
from crypto_momentum_lab.health import LocalHealthWriter
from crypto_momentum_lab.live_rollout.account_channel import LiveAccountEventRuntime
from crypto_momentum_lab.live_rollout.closed_candle_feed import (
    BinanceClosedCandle15mFeed,
    ClosedCandle15mFeedConfig,
)
from crypto_momentum_lab.live_rollout.control_plane import LiveControlPlaneRuntime
from crypto_momentum_lab.live_rollout.daemon import (
    LiveDaemonConfig,
    LiveDaemonResult,
    LiveStrategyDaemon,
)
from crypto_momentum_lab.live_rollout.entry_expectations import (
    LiveEntryExpectationRegistrar,
)
from crypto_momentum_lab.live_rollout.entry_order_cancellation import (
    LiveEntryOrderCanceller,
)
from crypto_momentum_lab.live_rollout.entry_orders import LiveLimitOrderLifecycle
from crypto_momentum_lab.live_rollout.entry_runtime import LiveEntryRuntime
from crypto_momentum_lab.live_rollout.exit_channels import LiveExitChannelRuntime
from crypto_momentum_lab.live_rollout.exits import (
    LiveExitConfig,
    LiveExitManager,
)
from crypto_momentum_lab.live_rollout.gates import (
    LiveGateContext,
    evaluate_live_gate,
)
from crypto_momentum_lab.live_rollout.health_monitor import LiveHealthMonitor
from crypto_momentum_lab.live_rollout.lease import (
    LeaseHeartbeatConfig,
    LiveLeaseHeartbeat,
)
from crypto_momentum_lab.live_rollout.limits import FixedLiveLimits
from crypto_momentum_lab.live_rollout.market_cache import (
    LatestMarketQuoteCache,
    LatestMarketStateCache,
)
from crypto_momentum_lab.live_rollout.order_event_runtime import LiveOrderEventRuntime
from crypto_momentum_lab.live_rollout.order_reconciliation import (
    LiveOrderReconciliation,
)
from crypto_momentum_lab.live_rollout.postgres_runtime import (
    PostgresLiveContextProvider,
    live_limits_from_approval,
    poll_live_market_states,
)
from crypto_momentum_lab.live_rollout.readiness import LiveReadinessPublisher
from crypto_momentum_lab.live_rollout.resource_lifecycle import LiveResourceLifecycle
from crypto_momentum_lab.live_rollout.risk_control import (
    LiveRiskControlRuntime,
    RiskControlCommandDispatcher,
)
from crypto_momentum_lab.live_rollout.runtime_config import (
    _BINANCE_SHARED_COMMAND_PACER_PATH_ENV,
    _BINANCE_SHARED_REQUEST_PACER_PATH_ENV,
    _LIVE_AUTO_REACQUIRE_LEASE_TTL_SECONDS,
    _LIVE_LEASE_HEARTBEAT_INTERVAL_SECONDS,
    _LIVE_LEASE_RENEW_BEFORE_SECONDS,
    _LIVE_RUNTIME_SHUTDOWN_TIMEOUT_SECONDS,
    _LIVE_STARTUP_BUFFER_LIMIT,
    _ORDER_IDENTITY_CONFLICT_MESSAGE,
    _PENDING_POSITION_RETRY_DELAYS_SECONDS,
    LiveRuntimeConfig,
    _live_strategy_config,
    _live_strategy_config_hash,
)
from crypto_momentum_lab.live_rollout.runtime_supervisor import (
    LiveRuntimeSupervisor,
    LiveRuntimeTasks,
)
from crypto_momentum_lab.live_rollout.scheduled_risk_window import (
    ScheduledRiskWindowConfig,
)
from crypto_momentum_lab.live_rollout.session import (
    LiveSessionConfig,
    LiveSessionLifecycle,
)
from crypto_momentum_lab.live_rollout.signal_recorder import (
    LiveStrategySignalRecorder,
)
from crypto_momentum_lab.live_rollout.startup_market_buffer import (
    StartupMarketStateBuffer,
)
from crypto_momentum_lab.live_rollout.startup_recovery import (
    checkpoint_needs_market_recovery as _checkpoint_needs_market_recovery,
)
from crypto_momentum_lab.live_rollout.startup_recovery import (
    cursor_after_market_bucket as _cursor_after_market_bucket,
)
from crypto_momentum_lab.live_rollout.startup_recovery import (
    live_market_state_cutover as _live_market_state_cutover,
)
from crypto_momentum_lab.live_rollout.startup_recovery import (
    restore_live_strategy_from_checkpoint as _restore_live_strategy_from_checkpoint,
)
from crypto_momentum_lab.live_rollout.startup_recovery import (
    strategy_last_processed_at_by_symbol as _strategy_last_processed_at_by_symbol,
)
from crypto_momentum_lab.live_rollout.startup_recovery import (
    warm_live_strategy as _warm_live_strategy,
)
from crypto_momentum_lab.live_rollout.startup_recovery import (
    warm_live_strategy_then_start_fresh as _warm_live_strategy_then_start_fresh,
)
from crypto_momentum_lab.live_rollout.startup_resilience import (
    LiveStartupRetryableError as _LiveStartupRetryableError,
)
from crypto_momentum_lab.live_rollout.startup_resilience import (
    is_retryable_live_startup_error as _is_retryable_live_startup_error,
)
from crypto_momentum_lab.live_rollout.startup_resilience import (
    maybe_auto_reacquire_live_lease as _maybe_auto_reacquire_live_lease,
)
from crypto_momentum_lab.live_rollout.stream_recovery import (
    resilient_market_state_stream as _resilient_market_state_stream,
)
from crypto_momentum_lab.live_rollout.stream_recovery import (
    resilient_risk_control_stream as _resilient_risk_control_stream,
)
from crypto_momentum_lab.live_rollout.submission_fence import LiveSubmissionFence
from crypto_momentum_lab.live_rollout.telemetry import (
    PERSISTED_OPERATIONAL_TELEMETRY_EVENTS,
    PERSISTED_ORDER_TELEMETRY_EVENTS,
    LiveRuntimeTelemetry,
    LiveTelemetrySink,
)
from crypto_momentum_lab.live_rollout.volume import WebSocketQuoteVolumeProvider
from crypto_momentum_lab.market_data.hub import WebSocketMarketStateSource
from crypto_momentum_lab.market_data.quote_hub import (
    WebSocketMarketQuoteSource,
    WebSocketMarketQuoteVolumeSource,
)
from crypto_momentum_lab.persistence.postgres.live_rollout_repository import (
    PostgresLiveRolloutRepository,
)
from crypto_momentum_lab.persistence.postgres.live_signal_repository import (
    PostgresLiveSignalRepository,
)
from crypto_momentum_lab.persistence.postgres.models import (
    LiveSessionTransitionRow,
    ShadowSessionRow,
)
from crypto_momentum_lab.persistence.postgres.order_repository import (
    PostgresOrderRepository,
)
from crypto_momentum_lab.persistence.postgres.paper_daemon_repository import (
    PostgresPaperDaemonRepository,
)
from crypto_momentum_lab.persistence.postgres.repository import (
    PostgresUniverseRepository,
)
from crypto_momentum_lab.persistence.postgres.risk_repository import (
    PostgresRiskRepository,
)
from crypto_momentum_lab.persistence.postgres.runtime_context import (
    load_latest_account_state as _latest_account_state,
)
from crypto_momentum_lab.persistence.postgres.runtime_context import (
    load_latest_risk_config as _latest_risk_config,
)
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    PostgresRuntimeMarketStateRepository,
    RuntimeStateCursor,
)
from crypto_momentum_lab.persistence.postgres.runtime_telemetry_repository import (
    PostgresRuntimeTelemetryRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_checkpoint_database_engine,
    create_execution_database_engine,
    create_market_database_engine,
    create_observability_database_engine,
)
from crypto_momentum_lab.risk.gateway import RiskGateway
from crypto_momentum_lab.strategy_runner.candle_source import (
    BinanceRestClosedCandle15mSource,
    ClosedCandleEmaProvider,
)
from crypto_momentum_lab.strategy_runner.position_exit import (
    PositionExitMode,
    PositionExitPolicy,
)
from crypto_momentum_lab.strategy_runner.registry import build_runtime_strategy

log = structlog.get_logger()

async def run_live_daemon(
    config: LiveRuntimeConfig,
    *,
    shutdown_requested: asyncio.Event | None = None,
) -> LiveDaemonResult:
    execution_database_url = config.databases.execution_database_url
    market_database_url = config.databases.market_database_url
    observability_database_url = config.databases.observability_database_url

    account_label = config.identity.account_label
    strategy_name = config.identity.strategy_name
    session_id = config.identity.session_id
    operator = config.identity.operator
    lease_owner = config.identity.lease_owner
    strategy_config_hash = config.identity.strategy_config_hash
    git_commit_hash = config.identity.git_commit_hash
    migration_revision = config.identity.migration_revision

    market_environment = config.market.market_environment
    market_state_source = config.market.market_state_source
    market_state_hub_url = config.market.market_state_hub_url
    market_quote_hub_url = config.market.market_quote_hub_url
    market_quote_volume_hub_url = config.market.market_quote_volume_hub_url
    market_websocket_url = config.market.market_websocket_url
    account_event_hub_url = config.market.account_event_hub_url
    risk_control_hub_url = config.market.risk_control_hub_url

    profile = config.strategy.profile
    entry_positive_gainer_top_count = (
        config.strategy.entry_positive_gainer_top_count
    )
    require_price_above_ema5 = config.strategy.require_price_above_ema5
    require_price_above_ema10 = config.strategy.require_price_above_ema10
    entry_order_type = config.strategy.entry_order_type
    entry_limit_ttl_seconds = config.strategy.entry_limit_ttl_seconds
    entry_policy_compare_only = config.strategy.entry_policy_compare_only
    entry_policy_enforce = config.strategy.entry_policy_enforce

    hedge_mode = config.execution.hedge_mode
    exit_mode = config.execution.exit_mode
    take_profit_pct = config.execution.take_profit_pct
    stop_loss_pct = config.execution.stop_loss_pct
    entry_long_only = config.execution.entry_long_only
    entry_leverage = config.execution.entry_leverage
    margin_type = config.execution.margin_type
    candle_grace_bars = config.execution.candle_grace_bars
    candle_grace_decision_profit_pct = (
        config.execution.candle_grace_decision_profit_pct
    )
    candle_grace_profit_pct = config.execution.candle_grace_profit_pct

    max_runtime_seconds = config.lifecycle.max_runtime_seconds
    poll_interval_seconds = config.lifecycle.poll_interval_seconds
    checkpoint_every_states = config.lifecycle.checkpoint_every_states
    persist_exchange_operations = config.lifecycle.persist_exchange_operations
    acknowledge_missing_shadow_preflight = (
        config.lifecycle.acknowledge_missing_shadow_preflight
    )

    base_url = config.credentials.base_url
    api_key = config.credentials.api_key
    api_secret = config.credentials.api_secret

    risk_control_enabled = bool(
        risk_control_hub_url is not None and risk_control_hub_url.strip()
    )
    if market_state_source not in {"hub", "postgres"}:
        raise ValueError("market_state_source must be 'hub' or 'postgres'")
    if market_state_source == "hub" and not market_state_hub_url.strip():
        raise ValueError("market_state_hub_url must not be empty in hub mode")
    if market_state_source == "hub" and not market_quote_hub_url.strip():
        raise ValueError("market_quote_hub_url must not be empty in hub mode")
    if market_state_source == "hub" and not market_quote_volume_hub_url.strip():
        raise ValueError("market_quote_volume_hub_url must not be empty in hub mode")
    if not account_event_hub_url.strip():
        raise ValueError("account_event_hub_url must not be empty")
    health = LocalHealthWriter.from_environment()
    live_readiness: LiveReadinessPublisher | None = None

    def mark_live_database_ok() -> None:
        if health is None:
            return
        try:
            health.database_ok()
        except Exception:
            log.exception("live_health_database_marker_failed")

    def mark_live_ready() -> None:
        if health is None:
            return
        try:
            health.heartbeat(database_ok=True)
            if live_readiness is not None:
                live_readiness.publish()
        except Exception:
            log.exception("live_health_marker_failed")

    now = datetime.now(tz=UTC)
    execution_engine = create_execution_database_engine(execution_database_url)
    market_engine = create_market_database_engine(market_database_url)
    observability_engine = create_observability_database_engine(
        observability_database_url
    )
    checkpoint_engine = create_checkpoint_database_engine(observability_database_url)
    heartbeat_engine: AsyncEngine | None = None
    client: BinanceUsdMTradeClient | None = None
    execution_coordinator: OrderExecutionCoordinator | None = None
    candle_source: BinanceRestClosedCandle15mSource | None = None
    closed_candle_feed: BinanceClosedCandle15mFeed | None = None
    ema_candle_source: BinanceRestClosedCandle15mSource | None = None
    entry_runtime: LiveEntryRuntime | None = None
    entry_filter_cache_task: asyncio.Task[None] | None = None
    entry_symbol_cache_task: asyncio.Task[None] | None = None
    entry_order_lifecycle: LiveLimitOrderLifecycle | None = None
    live_repository: PostgresLiveRolloutRepository | None = None
    telemetry: LiveRuntimeTelemetry | None = None
    volume_cache: WebSocketQuoteVolumeProvider | None = None
    volume_source: WebSocketMarketQuoteVolumeSource | None = None
    signal_recorder: LiveStrategySignalRecorder | None = None
    daemon: LiveStrategyDaemon | None = None
    hub_source: WebSocketMarketStateSource | None = None
    startup_market_buffer: StartupMarketStateBuffer | None = None
    startup_market_state_task: asyncio.Task[None] | None = None
    risk_control_source: WebSocketRiskControlSource | None = None
    risk_control_task: asyncio.Task[None] | None = None
    risk_control_runtime: LiveRiskControlRuntime | None = None
    control_plane_runtime: LiveControlPlaneRuntime | None = None
    session_lifecycle: LiveSessionLifecycle | None = None
    shutdown_task: asyncio.Task[bool] | None = None
    risk_config_hash = ""
    startup_phase = True
    try:
        execution_factory = async_sessionmaker(
            execution_engine,
            expire_on_commit=False,
        )
        market_factory = async_sessionmaker(
            market_engine,
            expire_on_commit=False,
        )
        observability_factory = async_sessionmaker(
            observability_engine,
            expire_on_commit=False,
        )
        checkpoint_factory = async_sessionmaker(
            checkpoint_engine,
            expire_on_commit=False,
        )
        live_repository = PostgresLiveRolloutRepository(execution_factory)
        risk_repository = PostgresRiskRepository(execution_factory)
        # Lease liveness is a control-plane concern.  Give it one isolated
        # connection with a short driver timeout so a slow market/reconcile
        # query cannot consume the pool needed by the heartbeat.
        heartbeat_engine = create_execution_database_engine(
            execution_database_url,
            pool_size=1,
            max_overflow=0,
            pool_timeout_seconds=3,
            command_timeout_seconds=5,
        )
        heartbeat_factory = async_sessionmaker(
            heartbeat_engine,
            expire_on_commit=False,
        )
        heartbeat_risk_repository = PostgresRiskRepository(heartbeat_factory)
        order_repository = PostgresOrderRepository(execution_factory)
        checkpoint_repository = PostgresPaperDaemonRepository(checkpoint_factory)
        telemetry_repository = PostgresRuntimeTelemetryRepository(observability_factory)
        telemetry = LiveRuntimeTelemetry(
            run_id=session_id,
            persist=telemetry_repository.save_runtime_events,
            persist_event_types=(
                PERSISTED_ORDER_TELEMETRY_EVENTS
                | PERSISTED_OPERATIONAL_TELEMETRY_EVENTS
            ),
            persist_exchange_operations=persist_exchange_operations,
        )
        await telemetry.start()
        volume_source = WebSocketMarketQuoteVolumeSource(
            url=market_quote_volume_hub_url,
            environment=market_environment,
            consumer_id=f"live-volume:{session_id}",
        )
        volume_cache = WebSocketQuoteVolumeProvider(volume_source)
        await volume_cache.start()
        signal_repository = PostgresLiveSignalRepository(observability_factory)
        signal_recorder = LiveStrategySignalRecorder(
            run_id=session_id,
            account_label=account_label,
            strategy_name=strategy_name,
            strategy_version="v0",
            config_hash=strategy_config_hash,
            code_commit=git_commit_hash,
            quote_volume_provider=volume_cache,
            persist=signal_repository.save_signals,
        )
        await signal_recorder.start()
        risk_config = await _latest_risk_config(execution_factory, account_label)
        risk_config_hash = risk_config.config_hash
        assert live_repository is not None
        session_lifecycle = LiveSessionLifecycle(
            repository=live_repository,
            config=LiveSessionConfig(
                session_id=session_id,
                operator=operator,
                strategy_config_hash=strategy_config_hash,
                risk_config_hash=risk_config_hash,
            ),
            clock=lambda: datetime.now(tz=UTC),
        )
        client = BinanceUsdMTradeClient(
            api_key=api_key,
            api_secret=api_secret,
            environment="live",
            account_label=account_label,
            live_submit_enabled=True,
            base_url=base_url,
            shared_request_pacer_path=(
                os.environ.get(_BINANCE_SHARED_REQUEST_PACER_PATH_ENV, "").strip()
                or None
            ),
            shared_command_request_pacer_path=(
                os.environ.get(
                    _BINANCE_SHARED_COMMAND_PACER_PATH_ENV,
                    "",
                ).strip()
                or None
            ),
            entry_leverage=entry_leverage,
            margin_type=margin_type,
        )
        account_config = await client.fetch_account_config()
        if account_config.hedge_mode != hedge_mode:
            expected = "hedge" if hedge_mode else "one-way"
            actual = "hedge" if account_config.hedge_mode else "one-way"
            raise RuntimeError(
                f"position mode mismatch: expected {expected}, got {actual}"
            )
        register_expected_entry = LiveEntryExpectationRegistrar(
            account_event_hub_url=account_event_hub_url,
            account_label=account_label,
        )
        order_event_runtime = LiveOrderEventRuntime(telemetry=telemetry)

        submission_fence = LiveSubmissionFence(
            risk_state=heartbeat_risk_repository,
            environment="live",
            account_label=account_label,
            strategy_name=strategy_name,
            lease_owner=lease_owner,
            code_generation=git_commit_hash,
            active_lease=lambda: active_lease,
            entry_enabled=lambda: daemon is not None and daemon.entry_enabled,
        )

        state_machine = OrderExecutionStateMachine(
            exchange=client,
            repository=order_repository,
            submit_policy=SubmitPolicy.LIVE_SUBMIT,
            live_submit_enabled=True,
            clock=lambda: datetime.now(tz=UTC),
            on_event=order_event_runtime.handle,
            on_before_submit=register_expected_entry,
            on_before_exchange_submit=submission_fence.validate,
            on_exchange_request=telemetry.exchange_request_started,
            on_exchange_response=telemetry.exchange_response_received,
            serialize_commands=False,
        )
        execution_coordinator = OrderExecutionCoordinator(
            backend=state_machine,
            account_label=account_label,
        )

        assert execution_coordinator is not None
        assert client is not None
        entry_order_canceller = LiveEntryOrderCanceller(
            exchange=client,
            state_machine=execution_coordinator,
            repository=order_repository,
            run_id=session_id,
        )
        order_reconciliation = LiveOrderReconciliation(
            order_repository=order_repository,
            state_machine=execution_coordinator,
            run_id=session_id,
        )
        await order_reconciliation.reconcile_all()
        draining = await _session_is_draining(execution_factory, session_id)
        if not draining:
            assert session_lifecycle is not None
            await session_lifecycle.transition(LiveSessionState.PREFLIGHT)
        approval = await live_repository.load_active_approval(
            account_label=account_label,
            strategy_name=strategy_name,
            now=now,
        )
        unresolved = await order_repository.load_unresolved_orders(session_id)
        entry_order_lifecycle = LiveLimitOrderLifecycle(
            cancel_order=execution_coordinator.cancel_order,
        )
        order_event_runtime.set_entry_order_lifecycle(entry_order_lifecycle)
        await entry_order_lifecycle.restore(unresolved)
        active_lease = await risk_repository.load_active_lease(
            "live", account_label, now
        )
        gate_context = LiveGateContext(
            now=now,
            live_submit_enabled=True,
            account_label=account_label,
            strategy_name=strategy_name,
            strategy_config_hash=strategy_config_hash,
            git_commit_hash=git_commit_hash,
            database_migration_revision=migration_revision,
            required_lease_owner=lease_owner,
            requested_submit_policy=SubmitPolicy.LIVE_SUBMIT,
            active_lease=active_lease,
            risk_config=risk_config,
            approval=approval,
            account_state=await _latest_account_state(
                execution_factory,
                account_label,
            ),
            active_halts=await risk_repository.load_active_halts("live", account_label),
            unresolved_order_states=tuple(item.state for item in unresolved),
        )
        active_lease = await _maybe_auto_reacquire_live_lease(
            factory=execution_factory,
            risk_repository=risk_repository,
            gate_context=gate_context,
            session_id=session_id,
            draining=draining,
            lease_ttl_seconds=_LIVE_AUTO_REACQUIRE_LEASE_TTL_SECONDS,
        )
        gate_context = replace(gate_context, active_lease=active_lease)
        gate = evaluate_live_gate(gate_context)
        if not gate.approved:
            raise RuntimeError(f"live gate blocked: {','.join(gate.reasons)}")
        if approval is None:
            raise RuntimeError("live approval is required")
        if active_lease is None:
            raise RuntimeError("live lease is required")

        strategy_config = _live_strategy_config(profile)
        computed_hash = _live_strategy_config_hash(
            strategy_name,
            profile=profile,
            entry_positive_gainer_top_count=entry_positive_gainer_top_count,
            require_price_above_ema5=require_price_above_ema5,
            require_price_above_ema10=require_price_above_ema10,
            entry_policy_enforce=entry_policy_enforce,
            entry_order_type=entry_order_type,
            entry_limit_ttl_seconds=entry_limit_ttl_seconds,
        )
        if computed_hash != strategy_config_hash:
            raise RuntimeError(
                "strategy config hash does not match the live runtime configuration"
            )
        if not draining:
            assert session_lifecycle is not None
            await session_lifecycle.transition(LiveSessionState.SHADOW_PREFLIGHT)
        await _warn_if_shadow_preflight_missing(
            execution_factory,
            strategy_name=strategy_name,
            strategy_config_hash=strategy_config_hash,
            account_label=account_label,
            session_id=session_id,
            acknowledged=acknowledge_missing_shadow_preflight,
        )

        strategy = build_runtime_strategy(
            strategy_name,
            config=strategy_config,
            identity=StrategyRunIdentity(
                run_id=session_id,
                strategy_name=strategy_name,
                strategy_version="v0",
                config_hash=strategy_config_hash,
                run_mode=RunMode.LIVE,
                code_commit=git_commit_hash,
                created_at=now,
                source_paths=(f"{market_state_source}:{market_environment}",),
            ),
        )
        required_data = getattr(strategy, "required_data", None)
        if not callable(required_data):
            raise RuntimeError("live strategy does not expose required data")
        live_readiness = LiveReadinessPublisher(
            health=health,
            account_label=account_label,
            session_id=session_id,
            strategy=strategy_name,
            code_commit=git_commit_hash,
            migration_revision=migration_revision,
            entry_universe_target_count=entry_positive_gainer_top_count,
            warmup_required_buckets=int(required_data().warmup_buckets),
        )
        checkpoint = await checkpoint_repository.load_checkpoint(session_id)
        if checkpoint is not None:
            strategy.restore_checkpoint(checkpoint)
        state_repository = PostgresRuntimeMarketStateRepository(market_factory)
        startup_cutover = _live_market_state_cutover(datetime.now(tz=UTC))
        startup_warmup_symbols: frozenset[str] | None = None
        if entry_positive_gainer_top_count is not None:
            universe_repository = PostgresUniverseRepository(market_factory)
            startup_warmup_symbols = (
                await universe_repository.load_positive_gainer_symbols_at(
                    startup_cutover,
                    top_count=entry_positive_gainer_top_count,
                )
            )
            log.info(
                "live_startup_warmup_symbols_selected",
                symbol_count=len(startup_warmup_symbols),
                top_count=entry_positive_gainer_top_count,
                cutover_at=startup_cutover.isoformat(),
            )
            live_readiness.set_expected_warmup_symbols(startup_warmup_symbols)
        if market_state_source == "hub":
            startup_market_buffer = StartupMarketStateBuffer(
                max_states=_LIVE_STARTUP_BUFFER_LIMIT
            )

            def on_startup_market_connection_change(
                available: bool,
                reason: str | None,
            ) -> None:
                assert startup_market_buffer is not None
                startup_market_buffer.observe_connection_change(
                    available,
                    reason,
                )
                if control_plane_runtime is not None:
                    control_plane_runtime.on_market_connection_change(
                        available,
                        reason,
                    )

            hub_source = WebSocketMarketStateSource(
                url=market_state_hub_url,
                environment=market_environment,
                consumer_id=f"live-strategy:{session_id}",
                on_connection_change=on_startup_market_connection_change,
            )
            startup_market_state_task = asyncio.create_task(
                _collect_startup_market_states(
                    source=hub_source,
                    buffer=startup_market_buffer,
                ),
                name=f"live-startup-market-buffer:{session_id}",
            )
        market_cursor: RuntimeStateCursor | None = None
        if checkpoint is not None:
            if _checkpoint_needs_market_recovery(checkpoint):
                await _restore_live_strategy_from_checkpoint(
                    strategy=strategy,
                    checkpoint=checkpoint,
                    repository=state_repository,
                    environment=market_environment,
                    cutover_at=startup_cutover,
                    warmup_symbols=startup_warmup_symbols,
                    on_warmup_status=live_readiness.update_warmup,
                )
            market_cursor = _cursor_after_market_bucket(startup_cutover)
        elif market_state_source == "postgres":
            market_cursor = await _warm_live_strategy_then_start_fresh(
                strategy=strategy,
                repository=state_repository,
                environment=market_environment,
                now=now,
                cutover_at=startup_cutover,
                warmup_symbols=startup_warmup_symbols,
                on_warmup_status=live_readiness.update_warmup,
            )
        else:
            await _warm_live_strategy(
                strategy=strategy,
                repository=state_repository,
                environment=market_environment,
                now=now,
                cutover_at=startup_cutover,
                warmup_symbols=startup_warmup_symbols,
                on_warmup_status=live_readiness.update_warmup,
            )
        if checkpoint is not None and not _checkpoint_needs_market_recovery(checkpoint):
            live_readiness.update_warmup_progress(
                strategy,
                expected_symbols=startup_warmup_symbols,
            )
        notional_cap, max_positions, max_loss, max_gross = live_limits_from_approval(
            approval=approval,
            risk_config=risk_config,
        )
        ema_provider: ClosedCandleEmaProvider | None = None
        if exit_mode is PositionExitMode.CANDLE_15M:
            candle_source = BinanceRestClosedCandle15mSource(base_url)
            closed_candle_feed = BinanceClosedCandle15mFeed(
                config=ClosedCandle15mFeedConfig(
                    websocket_url=market_websocket_url,
                    environment=market_environment,
                    consumer_id=f"live-exit-candles:{session_id}",
                ),
                backfill_source=candle_source,
            )
        if require_price_above_ema5 or require_price_above_ema10:
            if candle_source is None:
                candle_source = BinanceRestClosedCandle15mSource(base_url)
            ema_provider = ClosedCandleEmaProvider(candle_source)

        assert client is not None
        entry_runtime = LiveEntryRuntime(
            market_session_factory=market_factory,
            client=client,
            ema_provider=ema_provider,
            positive_gainer_top_count=entry_positive_gainer_top_count,
            entry_leverage=entry_leverage,
            margin_type=margin_type,
        )
        await entry_runtime.warm_exchange(now)
        live_readiness.update_entry_gate(
            entry_universe_count=entry_runtime.entry_universe_count(now),
            entry_enabled=False,
            entry_enabled_reason="entry_runtime_initializing",
        )
        entry_filter_cache_required = entry_runtime.entry_filter_cache_required
        entry_symbol_cache_required = entry_runtime.entry_symbol_cache_required
        daemon_entry_symbol_loader = entry_runtime.entry_symbol_loader

        entry_filter_context_loader = entry_runtime.entry_filter_context_loader
        context_provider = PostgresLiveContextProvider(
            execution_session_factory=execution_factory,
            market_session_factory=market_factory,
            account_label=account_label,
            run_id=session_id,
            strategy_name=strategy_name,
            strategy_config_hash=strategy_config_hash,
            git_commit_hash=git_commit_hash,
            migration_revision=migration_revision,
            lease_owner=lease_owner,
            approval_id=approval.approval_id,
        )
        heartbeat_context_provider = PostgresLiveContextProvider(
            execution_session_factory=heartbeat_factory,
            market_session_factory=market_factory,
            account_label=account_label,
            run_id=session_id,
            strategy_name=strategy_name,
            strategy_config_hash=strategy_config_hash,
            git_commit_hash=git_commit_hash,
            migration_revision=migration_revision,
            lease_owner=lease_owner,
            approval_id=approval.approval_id,
        )
        latest_market_states = LatestMarketStateCache()
        latest_market_quotes = LatestMarketQuoteCache()
        entry_universe_context_provider = entry_runtime.entry_universe_context_provider
        entry_universe_snapshot_provider = (
            entry_runtime.entry_universe_snapshot_provider
        )
        daemon = LiveStrategyDaemon(
            strategy=strategy,
            risk_gateway=RiskGateway(),
            limits=FixedLiveLimits(
                notional_cap=notional_cap,
                max_open_positions=max_positions,
                max_daily_loss=max_loss,
                max_gross_exposure=max_gross,
            ),
            repository=_LiveDaemonRepositoryAdapter(
                order_repository,
                checkpoint_repository,
                mark_live_database_ok,
            ),
            state_machine=execution_coordinator,
            context_provider=context_provider,
            telemetry=telemetry,
            signal_recorder=signal_recorder,
            entry_order_lifecycle=entry_order_lifecycle,
            config=LiveDaemonConfig(
                run_id=session_id,
                resize_tolerance=Decimal("0.10"),
                checkpoint_every_states=checkpoint_every_states,
                hedge_mode=hedge_mode,
                entry_long_only=entry_long_only,
                entry_symbol_loader=daemon_entry_symbol_loader,
                require_price_above_ema5=require_price_above_ema5,
                require_price_above_ema10=require_price_above_ema10,
                entry_filter_context_loader=entry_filter_context_loader,
                entry_universe_context_provider=(entry_universe_context_provider),
                entry_universe_snapshot_provider=(entry_universe_snapshot_provider),
                entry_policy_compare_only=entry_policy_compare_only,
                entry_policy_enforce=entry_policy_enforce,
                entry_order_type=entry_order_type,
                entry_limit_ttl_seconds=entry_limit_ttl_seconds,
                scheduled_risk_window=ScheduledRiskWindowConfig(),
            ),
            exit_manager=LiveExitManager(
                config=LiveExitConfig(
                    run_id=session_id,
                    strategy_name=strategy_name,
                    strategy_version="v0",
                    strategy_config_hash=strategy_config_hash,
                    policy=PositionExitPolicy(
                        take_profit_pct=take_profit_pct,
                        stop_loss_pct=stop_loss_pct,
                        max_holding_seconds=None,
                        mode=exit_mode,
                    ),
                    candle_grace_bars=candle_grace_bars,
                    candle_grace_decision_profit_pct=(candle_grace_decision_profit_pct),
                    candle_grace_profit_pct=candle_grace_profit_pct,
                ),
                candle_loader=None,
            ),
            exit_recovery_client=client,
            cancel_unfilled_entry_orders=entry_order_canceller.cancel,
            fetch_exchange_positions=client.fetch_positions,
            on_managed_position_symbols=(
                None if closed_candle_feed is None else closed_candle_feed.set_symbols
            ),
        )
        order_event_runtime.set_daemon(daemon)
        assert live_repository is not None
        risk_control_dispatcher = RiskControlCommandDispatcher(
            repository=live_repository,
            account_label=account_label,
            strategy_name=strategy_name,
            session_id=session_id,
            cancel_all_open_entries=daemon.cancel_all_open_entries,
            request_flatten=daemon.request_flatten,
            clock=lambda: datetime.now(tz=UTC),
        )

        async def load_risk_control_state() -> tuple[bool, bool]:
            draining_now = await _session_is_draining(
                heartbeat_factory,
                session_id,
            )
            active_halts = await heartbeat_risk_repository.load_active_halts(
                "live",
                account_label,
            )
            return draining_now, bool(active_halts)

        def invalidate_live_contexts() -> None:
            context_provider.invalidate_cache()
            heartbeat_context_provider.invalidate_cache()

        def refresh_entry_enabled() -> None:
            assert risk_control_runtime is not None
            assert control_plane_runtime is not None
            assert entry_runtime is not None
            risk_blocked, risk_reason = risk_control_runtime.entry_gate()
            daemon.set_risk_control_entry_blocked(
                risk_blocked,
                reason=risk_reason,
            )
            daemon.refresh_entry_prerequisites(
                lease_heartbeat_degraded=(
                    control_plane_runtime.lease_heartbeat_degraded
                ),
                session_draining=draining,
                strategy_warmup_ready=control_plane_runtime.strategy_warmup_ready,
                strategy_warmup_reason=(control_plane_runtime.strategy_warmup_reason),
                market_state_available=control_plane_runtime.market_state_available,
                market_state_unavailable_reason=(
                    control_plane_runtime.market_state_unavailable_reason
                ),
                account_snapshot_available=(
                    control_plane_runtime.account_snapshot_available
                ),
            )
            live_readiness.update_entry_gate(
                entry_universe_count=entry_runtime.entry_universe_count(
                    datetime.now(tz=UTC)
                ),
                entry_enabled=daemon.entry_enabled,
                entry_enabled_reason=daemon.entry_enabled_reason,
            )

        async def reacquire_live_lease(
            gate_context: LiveGateContext,
        ) -> TradingLease | None:
            return await _maybe_auto_reacquire_live_lease(
                factory=heartbeat_factory,
                risk_repository=heartbeat_risk_repository,
                gate_context=gate_context,
                session_id=session_id,
                draining=await _session_is_draining(
                    heartbeat_factory,
                    session_id,
                ),
                lease_ttl_seconds=_LIVE_AUTO_REACQUIRE_LEASE_TTL_SECONDS,
            )

        control_plane_runtime = LiveControlPlaneRuntime(
            session_id=session_id,
            context_provider=context_provider,
            heartbeat_context_provider=heartbeat_context_provider,
            latest_market_states=latest_market_states,
            reacquire_lease=reacquire_live_lease,
            market_state_available=(
                True
                if startup_market_buffer is None
                else startup_market_buffer.connection_available
            ),
            strategy_warmup_ready=True,
            notify_market_state_gap=(
                lambda reason: daemon.notify_market_state_gap(reason=reason)
            ),
            refresh_entry_gate=refresh_entry_enabled,
            mark_database_ok=mark_live_database_ok,
            telemetry=telemetry,
            clock=lambda: datetime.now(tz=UTC),
        )

        risk_control_runtime = LiveRiskControlRuntime(
            enabled=risk_control_enabled,
            session_id=session_id,
            load_durable_state=load_risk_control_state,
            dispatch=risk_control_dispatcher.dispatch,
            invalidate_contexts=invalidate_live_contexts,
            refresh_entry_gate=refresh_entry_enabled,
            telemetry=telemetry,
            clock=lambda: datetime.now(tz=UTC),
        )
        lease_heartbeat = LiveLeaseHeartbeat(
            repository=heartbeat_risk_repository,
            lease=active_lease,
            owner=lease_owner,
            config=LeaseHeartbeatConfig(
                lease_ttl_seconds=_LIVE_AUTO_REACQUIRE_LEASE_TTL_SECONDS,
                renew_before_seconds=_LIVE_LEASE_RENEW_BEFORE_SECONDS,
                poll_interval_seconds=_LIVE_LEASE_HEARTBEAT_INTERVAL_SECONDS,
            ),
            on_renewed=control_plane_runtime.on_lease_renewed,
            on_error=control_plane_runtime.on_lease_error,
            recover=control_plane_runtime.recover_live_lease,
        )

        def on_exit_failure(symbol: str, failure: str | None) -> None:
            daemon.set_exit_failure(symbol, failure)
            refresh_entry_enabled()

        def on_entry_filter_cache_ready(ready: bool) -> None:
            daemon.set_entry_filter_cache_ready(ready)
            refresh_entry_enabled()

        daemon.set_entry_filter_cache_ready(
            not (entry_filter_cache_required or entry_symbol_cache_required)
        )
        entry_runtime.set_ready_callback(on_entry_filter_cache_ready)
        refresh_entry_enabled()
        if not draining:
            assert session_lifecycle is not None
            await session_lifecycle.transition(LiveSessionState.LIVE_ENABLED)
        mark_live_ready()
        startup_phase = False
        quote_source: WebSocketMarketQuoteSource | None = None
        state_stream: AsyncIterable[MarketState15s]
        if market_state_source == "hub":
            assert hub_source is not None
            assert startup_market_buffer is not None
            state_stream = startup_market_buffer.stream(
                skip_through=_strategy_last_processed_at_by_symbol(strategy)
            )
        else:
            state_stream = poll_live_market_states(
                repository=state_repository,
                environment=market_environment,
                max_runtime_seconds=max_runtime_seconds,
                poll_interval_seconds=poll_interval_seconds,
                cursor=market_cursor,
            )
        account_source = WebSocketAccountEventSource(
            url=account_event_hub_url,
            environment="live",
            account_label=account_label,
            consumer_id=f"live-exit:{session_id}",
            on_recovery=control_plane_runtime.on_account_snapshot_recovery,
        )
        exit_channel_runtime = LiveExitChannelRuntime(
            daemon=daemon,
            latest_market_quotes=latest_market_quotes,
            latest_market_states=latest_market_states,
            is_transient_error=_is_transient_live_runtime_error,
            is_order_identity_conflict=_is_order_identity_conflict,
            on_exit_failure=on_exit_failure,
            pending_position_retry_delays=_PENDING_POSITION_RETRY_DELAYS_SECONDS,
        )
        account_event_runtime = LiveAccountEventRuntime(
            daemon=daemon,
            latest_market_states=latest_market_states,
            latest_market_quotes=latest_market_quotes,
            order_reconciliation=order_reconciliation,
            run_id=session_id,
            telemetry=telemetry,
            is_transient_error=_is_transient_live_runtime_error,
            is_order_identity_conflict=_is_order_identity_conflict,
            on_exit_failure=on_exit_failure,
            on_account_snapshot=control_plane_runtime.on_account_snapshot,
            pending_position_retry_delays=_PENDING_POSITION_RETRY_DELAYS_SECONDS,
        )
        if risk_control_enabled:
            assert risk_control_hub_url is not None
            risk_control_source = WebSocketRiskControlSource(
                url=risk_control_hub_url,
                environment="live",
                account_label=account_label,
                strategy_name=strategy_name,
                session_id=session_id,
                consumer_id=f"live-risk-control:{session_id}",
                on_connection_change=risk_control_runtime.on_connection_change,
            )
        quote_task: asyncio.Task[None] | None = None
        closed_candle_task: asyncio.Task[None] | None = None
        grace_timeout_task: asyncio.Task[None] | None = None
        if closed_candle_feed is not None:
            await closed_candle_feed.start()
            closed_candle_task = asyncio.create_task(
                exit_channel_runtime.run_closed_candle_channel(
                    source=closed_candle_feed,
                ),
                name=f"live-closed-candle:{session_id}",
            )
            grace_timeout_task = asyncio.create_task(
                exit_channel_runtime.run_grace_timeout_channel(),
                name=f"live-grace-timeout:{session_id}",
            )
        if market_state_source == "hub":
            quote_source = WebSocketMarketQuoteSource(
                url=market_quote_hub_url,
                environment=market_environment,
                consumer_id=f"live-exit-quotes:{session_id}",
            )
            quote_task = asyncio.create_task(
                exit_channel_runtime.run_quote_channel(
                    source=quote_source,
                )
            )
        assert entry_runtime is not None
        assert live_readiness is not None
        market_task = asyncio.create_task(
            daemon.run(
                _observe_market_states(
                    state_stream,
                    latest_market_states,
                    on_observed=live_readiness.observe_market_state,
                    strategy=strategy,
                    entry_universe_count=entry_runtime.entry_universe_count,
                )
            )
        )
        account_task = asyncio.create_task(
            account_event_runtime.run(account_source),
            name=f"live-account-events:{session_id}",
        )
        if risk_control_source is not None:
            risk_control_task = asyncio.create_task(
                _run_risk_control_channel(
                    source=risk_control_source,
                    on_event=risk_control_runtime.on_event,
                ),
                name=f"live-risk-control:{session_id}",
            )
        (
            entry_filter_cache_task,
            entry_symbol_cache_task,
        ) = entry_runtime.start()
        lease_task = asyncio.create_task(lease_heartbeat.run())
        reconcile_task = asyncio.create_task(order_reconciliation.run_periodically())
        local_health_task: asyncio.Task[None] | None = None

        if health is not None:
            health_monitor = LiveHealthMonitor(
                health=health,
                interval_seconds=_LIVE_LEASE_HEARTBEAT_INTERVAL_SECONDS,
                is_degraded=lambda: (
                    market_task.done()
                    or account_task.done()
                    or lease_task.done()
                    or (risk_control_task is not None and risk_control_task.done())
                ),
            )
            local_health_task = asyncio.create_task(
                health_monitor.run(),
                name=f"live-local-health:{session_id}",
            )

        def stop_runtime_sources() -> None:
            if hub_source is not None:
                hub_source.stop()
            if quote_source is not None:
                quote_source.stop()
            account_source.stop()
            if risk_control_source is not None:
                risk_control_source.stop()

        async def close_risk_control() -> None:
            if risk_control_runtime is not None:
                await risk_control_runtime.close()

        async def stop_entry_caches() -> None:
            assert entry_runtime is not None
            await entry_runtime.stop()

        if shutdown_requested is not None:
            shutdown_task = asyncio.create_task(
                shutdown_requested.wait(),
                name=f"live-shutdown-request:{session_id}",
            )
        runtime_supervisor = LiveRuntimeSupervisor(
            tasks=LiveRuntimeTasks(
                market=market_task,
                account=account_task,
                lease=lease_task,
                reconcile=reconcile_task,
                startup_market=startup_market_state_task,
                quote=quote_task,
                closed_candle=closed_candle_task,
                grace_timeout=grace_timeout_task,
                risk_control=risk_control_task,
                entry_filter_cache=entry_filter_cache_task,
                entry_symbol_cache=entry_symbol_cache_task,
                local_health=local_health_task,
                shutdown=shutdown_task,
            ),
            block_entry_submissions=(
                lambda: (
                    execution_coordinator.block_entry_submissions()
                    if execution_coordinator is not None
                    else None
                )
            ),
            stop_sources=stop_runtime_sources,
            close_risk_control=close_risk_control,
            stop_entry_caches=stop_entry_caches,
            wait_for_entry_submissions_idle=(
                execution_coordinator.wait_for_entry_submissions_idle
                if execution_coordinator is not None
                else None
            ),
            shutdown_timeout_seconds=_LIVE_RUNTIME_SHUTDOWN_TIMEOUT_SECONDS,
        )
        try:
            result = await runtime_supervisor.run()
        finally:
            await runtime_supervisor.stop()
        assert session_lifecycle is not None
        await session_lifecycle.transition(
            LiveSessionState.HALTED
            if result.halt_reason is not None
            else LiveSessionState.COMPLETED,
            reason=result.halt_reason,
        )
        return result
    except Exception as exc:
        if startup_phase and _is_retryable_live_startup_error(exc):
            raise _LiveStartupRetryableError(exc) from exc
        if session_lifecycle is not None and risk_config_hash:
            await session_lifecycle.transition(
                LiveSessionState.HALTED,
                reason=str(exc),
            )
        raise
    finally:
        if hub_source is not None:
            hub_source.stop()
        if (
            startup_market_state_task is not None
            and not startup_market_state_task.done()
        ):
            startup_market_state_task.cancel()
        if startup_market_state_task is not None:
            try:
                async with asyncio.timeout(_LIVE_RUNTIME_SHUTDOWN_TIMEOUT_SECONDS):
                    await asyncio.gather(
                        startup_market_state_task,
                        return_exceptions=True,
                    )
            except TimeoutError:
                log.warning(
                    "live_startup_market_buffer_shutdown_timed_out",
                    timeout_seconds=_LIVE_RUNTIME_SHUTDOWN_TIMEOUT_SECONDS,
                )
            except asyncio.CancelledError:
                raise
        if shutdown_task is not None and not shutdown_task.done():
            shutdown_task.cancel()
        if shutdown_task is not None:
            await asyncio.gather(shutdown_task, return_exceptions=True)
        await LiveResourceLifecycle(
            entry_runtime=entry_runtime,
            entry_order_lifecycle=entry_order_lifecycle,
            execution_coordinator=execution_coordinator,
            client=client,
            closed_candle_feed=closed_candle_feed,
            candle_source=candle_source,
            ema_candle_source=ema_candle_source,
            signal_recorder=signal_recorder,
            telemetry=telemetry,
            volume_cache=volume_cache,
            volume_rest_client=None,
            execution_engine=execution_engine,
            market_engine=market_engine,
            observability_engine=observability_engine,
            checkpoint_engine=checkpoint_engine,
            heartbeat_engine=heartbeat_engine,
            health=health,
            shutdown_timeout_seconds=_LIVE_RUNTIME_SHUTDOWN_TIMEOUT_SECONDS,
        ).close()

async def _observe_market_states(
    states: AsyncIterable[MarketState15s],
    cache: LatestMarketStateCache,
    *,
    on_observed: Callable[..., None] | None = None,
    strategy: object | None = None,
    entry_universe_count: Callable[[datetime], int] | None = None,
) -> AsyncIterator[MarketState15s]:
    async for state in states:
        cache.observe(state)
        if on_observed is not None and strategy is not None:
            count = (
                0
                if entry_universe_count is None
                else entry_universe_count(state.bucket_start)
            )
            on_observed(
                state,
                strategy=strategy,
                entry_universe_count=count,
            )
        yield state


async def _collect_startup_market_states(
    *,
    source: WebSocketMarketStateSource,
    buffer: StartupMarketStateBuffer,
) -> None:
    """Consume Hub data during DB warmup and hand the same stream forward."""

    try:
        async for state in _resilient_market_state_stream(source):
            await buffer.append(state)
    except asyncio.CancelledError:
        raise
    except Exception as error:
        buffer.close(error)
        raise
    else:
        buffer.close()


async def _run_risk_control_channel(
    *,
    source: WebSocketRiskControlSource,
    on_event: Callable[[RiskControlEvent], Awaitable[None]],
) -> None:
    async for event in _resilient_risk_control_stream(source):
        await on_event(event)


async def _run_account_event_channel(
    *,
    source: WebSocketAccountEventSource,
    daemon: LiveStrategyDaemon,
    latest_market_states: LatestMarketStateCache,
    latest_market_quotes: LatestMarketQuoteCache,
    order_reconciliation: LiveOrderReconciliation | None = None,
    order_repository: PostgresOrderRepository | None = None,
    state_machine: OrderExecutionPort | None = None,
    run_id: str | None = None,
    telemetry: LiveTelemetrySink | None = None,
    on_exit_failure: Callable[[str, str | None], None] | None = None,
    on_account_snapshot: Callable[[AccountEvent], None] | None = None,
) -> None:
    if (
        order_reconciliation is None
        and order_repository is not None
        and state_machine is not None
        and run_id is not None
    ):
        order_reconciliation = LiveOrderReconciliation(
            order_repository=order_repository,
            state_machine=state_machine,
            run_id=run_id,
        )
    runtime = LiveAccountEventRuntime(
        daemon=daemon,
        latest_market_states=latest_market_states,
        latest_market_quotes=latest_market_quotes,
        order_reconciliation=order_reconciliation,
        run_id=run_id,
        telemetry=telemetry,
        is_transient_error=_is_transient_live_runtime_error,
        is_order_identity_conflict=_is_order_identity_conflict,
        on_exit_failure=on_exit_failure,
        on_account_snapshot=on_account_snapshot,
        pending_position_retry_delays=_PENDING_POSITION_RETRY_DELAYS_SECONDS,
    )
    await runtime.run(source)


async def _run_grace_timeout_channel(
    *,
    daemon: LiveStrategyDaemon,
    latest_market_states: LatestMarketStateCache,
    latest_market_quotes: LatestMarketQuoteCache,
    interval_seconds: float = 1.0,
    on_exit_failure: Callable[[str, str | None], None] | None = None,
) -> None:
    """Keep the historical test/CLI seam backed by the extracted runtime."""

    runtime = LiveExitChannelRuntime(
        daemon=daemon,
        latest_market_quotes=latest_market_quotes,
        latest_market_states=latest_market_states,
        is_transient_error=_is_transient_live_runtime_error,
        is_order_identity_conflict=_is_order_identity_conflict,
        on_exit_failure=on_exit_failure,
        pending_position_retry_delays=_PENDING_POSITION_RETRY_DELAYS_SECONDS,
    )
    await runtime.run_grace_timeout_channel(interval_seconds=interval_seconds)


def _is_transient_live_runtime_error(error: Exception) -> bool:
    return isinstance(
        error,
        (SQLAlchemyError, TimeoutError, ConnectionError, OSError),
    )


def _is_order_identity_conflict(error: Exception) -> bool:
    return (
        isinstance(error, ValueError) and str(error) == _ORDER_IDENTITY_CONFLICT_MESSAGE
    )


class _LiveDaemonRepositoryAdapter:
    def __init__(
        self,
        order_repository: PostgresOrderRepository,
        checkpoint_repository: PostgresPaperDaemonRepository,
        on_database_success: Callable[[], None] | None = None,
    ) -> None:
        self._orders = order_repository
        self._checkpoints = checkpoint_repository
        self._on_database_success = on_database_success

    async def save_approved_intent(
        self,
        intent: OrderIntentCandidate,
        evaluation: RiskEvaluation,
    ) -> None:
        await self._orders.save_approved_intent(intent, evaluation)

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
    ) -> PreparedOrderSubmission | None:
        return await self._orders.prepare_submission(
            intent=intent,
            evaluation=evaluation,
            plan=plan,
            prepared_at=prepared_at,
            environment=environment,
            account_label=account_label,
            strategy_name=strategy_name,
            required_lease_owner=required_lease_owner,
            required_lease_id=required_lease_id,
            required_code_generation=required_code_generation,
            required_session_id=required_session_id,
            max_open_positions=max_open_positions,
            max_daily_loss=max_daily_loss,
            max_gross_exposure=max_gross_exposure,
            current_daily_pnl=current_daily_pnl,
            current_gross_exposure=current_gross_exposure,
            open_position_symbols=open_position_symbols,
            exposure_notional=exposure_notional,
        )

    async def save_checkpoint(
        self,
        run_id: str,
        checkpoint: StrategyCheckpoint,
        saved_at: datetime,
    ) -> None:
        await self._checkpoints.save_checkpoint(run_id, checkpoint, saved_at)
        if self._on_database_success is not None:
            self._on_database_success()


async def _has_matching_shadow_session(
    factory: async_sessionmaker[AsyncSession],
    *,
    strategy_name: str,
    strategy_config_hash: str,
) -> bool:
    async with factory() as database_session:
        completed_shadow = await database_session.scalar(
            select(ShadowSessionRow.run_id)
            .where(
                ShadowSessionRow.strategy_name == strategy_name,
                ShadowSessionRow.strategy_config_hash == strategy_config_hash,
                ShadowSessionRow.state == "completed",
            )
            .order_by(ShadowSessionRow.ended_at.desc())
            .limit(1)
        )
    return completed_shadow is not None


async def _warn_if_shadow_preflight_missing(
    factory: async_sessionmaker[AsyncSession],
    *,
    strategy_name: str,
    strategy_config_hash: str,
    account_label: str,
    session_id: str,
    acknowledged: bool = False,
) -> None:
    if await _has_matching_shadow_session(
        factory,
        strategy_name=strategy_name,
        strategy_config_hash=strategy_config_hash,
    ):
        return
    details = {
        "account_label": account_label,
        "session_id": session_id,
        "strategy_name": strategy_name,
        "strategy_config_hash": strategy_config_hash,
    }
    if acknowledged:
        log.info("live_shadow_preflight_missing_acknowledged", **details)
    else:
        log.warning("live_shadow_preflight_missing", **details)


async def _session_is_draining(
    factory: async_sessionmaker[AsyncSession],
    session_id: str,
) -> bool:
    async with factory() as database_session:
        latest_state = await database_session.scalar(
            select(LiveSessionTransitionRow.state)
            .where(
                LiveSessionTransitionRow.session_id == session_id,
                LiveSessionTransitionRow.state.not_in(
                    (
                        LiveSessionState.PREFLIGHT.value,
                        LiveSessionState.SHADOW_PREFLIGHT.value,
                    )
                ),
            )
            .order_by(LiveSessionTransitionRow.occurred_at.desc())
            .limit(1)
        )
    return latest_state == LiveSessionState.DRAINING.value
