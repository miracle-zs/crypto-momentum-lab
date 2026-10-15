"""Live runtime composition and long-running orchestration.

The CLI module resolves operator input and environment configuration.  This
module owns the runtime assembly and lifecycle of one live daemon.
"""

import asyncio
import json
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import structlog
from sqlalchemy.ext.asyncio import AsyncEngine

import crypto_momentum_lab.live_rollout.order_identity_errors as order_identity_errors
import crypto_momentum_lab.live_rollout.runtime_errors as runtime_errors
import crypto_momentum_lab.live_rollout.session_state as session_state
from crypto_momentum_lab.domain.decision.decision_engine import (
    create_authoritative_async_decision_filter,
)
from crypto_momentum_lab.domain.execution.order_read_models import (
    PersistedExchangeOrder,
)
from crypto_momentum_lab.domain.execution.order_result import OrderExecutionResult
from crypto_momentum_lab.domain.execution.order_rules import SymbolTradingRules
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    ExitAllocation,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderSubmissionPreparation,
)
from crypto_momentum_lab.domain.execution.trade_command import TradeCommand
from crypto_momentum_lab.domain.live_rollout import (
    LiveSessionState,
)
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.market.runtime_state_models import RuntimeStateCursor
from crypto_momentum_lab.domain.operational.runtime_metadata import (
    RuntimeMetadataSnapshot,
    TradingRuleHashInput,
    compute_trading_rules_hash,
)
from crypto_momentum_lab.domain.risk import RiskDecision, RiskEvaluation, RiskGateway
from crypto_momentum_lab.domain.risk.limits import FixedLiveLimits
from crypto_momentum_lab.domain.strategy import (
    OrderIntentCandidate,
    RunMode,
    StrategyRunIdentity,
    StrategySide,
)
from crypto_momentum_lab.domain.strategy.position_exit import (
    PositionExitPolicy,
)
from crypto_momentum_lab.domain.strategy.sizing import SymbolLotRules
from crypto_momentum_lab.execution_account.binance.client import (
    BinanceUsdMPrivateReadClient,
    BinanceUsdMTradeClient,
)
from crypto_momentum_lab.execution_account.binance.user_data import (
    DEFAULT_BINANCE_USDM_USER_DATA_WEBSOCKET_URL,
)
from crypto_momentum_lab.execution_account.expectations import (
    AccountPositionExpectationRegistry,
)
from crypto_momentum_lab.execution_account.hub import (
    AccountEvent,
    InProcessAccountEventSource,
    WebSocketAccountPositionExpectationPublisher,
)
from crypto_momentum_lab.execution_account.in_process_producer import (
    InProcessAccountStreamProducer,
)
from crypto_momentum_lab.domain.live_rollout.authorization import (
    CANCEL_ALL_OPEN_ENTRIES_COMMAND,
    EMERGENCY_FLATTEN_COMMAND,
)
from crypto_momentum_lab.execution_account.risk_control_hub import (
    RiskControlAction,
    RiskControlEvent,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
)
from crypto_momentum_lab.health.resources import ProcessResourceSampler
from crypto_momentum_lab.live_rollout.account_channel import LiveAccountEventRuntime
from crypto_momentum_lab.live_rollout.closed_candle_feed import (
    BinanceClosedCandle15mFeed,
)
from crypto_momentum_lab.domain.account import (
    ExecutionAccountProcessState,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.domain.account.snapshot_models import AccountSnapshot
from crypto_momentum_lab.domain.account.models import (
    AccountFillLoadScan,
)
from crypto_momentum_lab.execution_account.fill_scan_plan import plan_fill_scan
from crypto_momentum_lab.persistence.postgres.fill_recovery_sources import (
    load_fill_recovery_sources,
)
from crypto_momentum_lab.execution_account.balance_history import (
    BalanceValue,
    balance_value,
    select_balance_history,
)
from crypto_momentum_lab.execution_account.reconciliation_records import (
    position_state_details,
    reconciliation_run,
    user_data_reconciliation_id,
)
from crypto_momentum_lab.execution_account.sync_models import (
    ExecutionAccountSyncConfig,
)
from crypto_momentum_lab.persistence.postgres.account_repository import (
    PostgresAccountRepository,
)
from crypto_momentum_lab.live_rollout.control_plane import LiveControlPlaneRuntime
from crypto_momentum_lab.live_rollout.daemon import LiveDaemonConfig, LiveStrategyDaemon
from crypto_momentum_lab.live_rollout.decision_facts import LiveDecisionFactSource
from crypto_momentum_lab.live_rollout.entry_expectations import (
    DirectPositionExpectationPublisher,
    LiveEntryExpectationRegistrar,
)
from crypto_momentum_lab.live_rollout.entry_order_cancellation import (
    LiveEntryOrderCanceller,
)
from crypto_momentum_lab.live_rollout.entry_orders import LiveLimitOrderLifecycle
from crypto_momentum_lab.live_rollout.entry_runtime import LiveEntryRuntime
from crypto_momentum_lab.live_rollout.execution_runtime import (
    build_live_execution_runtime,
    build_live_policy,
)
from crypto_momentum_lab.live_rollout.exit_channels import LiveExitChannelRuntime
from crypto_momentum_lab.live_rollout.exit_receipt_recovery import (
    LiveExitReceiptRecovery,
)
from crypto_momentum_lab.live_rollout.exits import (
    LiveExitConfig,
    LiveExitManager,
)
from crypto_momentum_lab.live_rollout.health_monitor import LiveHealthMonitor
from crypto_momentum_lab.live_rollout.hub_cursor import (
    LiveHubCursorState,
    hub_cursor_for_startup,
    hub_cursor_from_checkpoint_payload,
)
from crypto_momentum_lab.live_rollout.market_assembly import (
    LiveChannelSources,
    LiveStartupMarketAssembly,
    assemble_live_candle_sources,
    assemble_live_channel_sources,
    assemble_live_quote_volume,
    assemble_live_startup_market_buffer,
    build_live_market_state_stream,
)
from crypto_momentum_lab.live_rollout.market_assembly import (
    observe_market_states as _observe_market_states,
)
from crypto_momentum_lab.live_rollout.market_assembly import (
    run_risk_control_channel as _run_risk_control_channel,
)
from crypto_momentum_lab.live_rollout.market_cache import (
    LatestMarketQuoteCache,
    LatestMarketStateCache,
)
from crypto_momentum_lab.live_rollout.market_runtime_contracts import (
    LiveDaemonResult,
    LiveMarketStateContinuityError,
)
from crypto_momentum_lab.live_rollout.order_event_runtime import LiveOrderEventRuntime
from crypto_momentum_lab.live_rollout.order_reconciliation import (
    LiveOrderReconciliation,
    ReconciliationPassResult,
)
from crypto_momentum_lab.live_rollout.pending_entries import TerminalOrderBarrier
from crypto_momentum_lab.live_rollout.position_self_healing import (
    LiveUnmanagedPositionRepair,
)
from crypto_momentum_lab.live_rollout.postgres_runtime import (
    PostgresLiveContextProvider,
)
from crypto_momentum_lab.live_rollout.readiness import (
    LiveReadinessPublisher,
    ReadinessStrategy,
)
from crypto_momentum_lab.live_rollout.resource_lifecycle import LiveResourceLifecycle
from crypto_momentum_lab.live_rollout.risk_control import (
    LiveRiskControlRuntime,
    RiskControlCommandDispatcher,
)
from crypto_momentum_lab.live_rollout.runtime_config import (
    _BINANCE_SHARED_COMMAND_PACER_PATH_ENV,
    _BINANCE_SHARED_REQUEST_PACER_PATH_ENV,
    _LIVE_RUNTIME_SHUTDOWN_TIMEOUT_SECONDS,
    _LIVE_STATUS_HEARTBEAT_INTERVAL_SECONDS,
    LiveRuntimeConfig,
    _live_strategy_config,
    _live_strategy_config_hash,
)
from crypto_momentum_lab.live_rollout.runtime_session import (
    ResourceOwnershipRegistry,
    RuntimeSession,
)
from crypto_momentum_lab.live_rollout.runtime_startup import (
    LiveRuntimeStartupStatus,
    validate_live_runtime_endpoints,
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
    load_live_market_state_gap as _load_live_market_state_gap,
)
from crypto_momentum_lab.live_rollout.startup_recovery import (
    restore_live_strategy_from_checkpoint as _restore_live_strategy_from_checkpoint,
)
from crypto_momentum_lab.live_rollout.startup_recovery import (
    wait_for_durable_market_state_cutover as _wait_for_durable_market_state_cutover,
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
from crypto_momentum_lab.live_rollout.telemetry import (
    PERSISTED_OPERATIONAL_TELEMETRY_EVENTS,
    PERSISTED_ORDER_TELEMETRY_EVENTS,
    RUNTIME_METADATA_SNAPSHOT,
    LiveRuntimeTelemetry,
)
from crypto_momentum_lab.live_rollout.volume import WebSocketQuoteVolumeProvider
from crypto_momentum_lab.market_data.candle_source import (
    BinanceRestClosedCandle15mSource,
)
from crypto_momentum_lab.market_data.hub import WebSocketMarketStateSource
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
    AsyncPostgresDecisionUnitOfWork,
)
from crypto_momentum_lab.persistence.postgres.live_rollout_repository import (
    PostgresLiveRolloutRepository,
)
from crypto_momentum_lab.persistence.postgres.live_runtime_assembly import (
    assemble_live_persistence,
)
from crypto_momentum_lab.persistence.postgres.live_signal_repository import (
    PostgresLiveSignalRepository,
)
from crypto_momentum_lab.persistence.postgres.order_read_repository import (
    PostgresOrderReadRepository,
)
from crypto_momentum_lab.persistence.postgres.position_repair import (
    PostgresPositionRepairUnitOfWork,
)
from crypto_momentum_lab.persistence.postgres.repository import (
    PostgresUniverseRepository,
)
from crypto_momentum_lab.persistence.postgres.runtime_context import (
    load_latest_risk_config as _latest_risk_config,
)
from crypto_momentum_lab.persistence.postgres.runtime_context import (
    load_trading_rules as _load_trading_rules,
)
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    PostgresRuntimeMarketStateRepository,
)
from crypto_momentum_lab.strategies.registry import build_runtime_strategy

log = structlog.get_logger()


def _positions_for_persistence(
    positions: tuple[Any, ...],
    previous_amounts: Mapping[tuple[str, str], Decimal],
) -> tuple[Any, ...]:
    """Keep non-zero rows and the row that closes a previously held position."""

    persisted: list[Any] = []
    for position in positions:
        identity = (position.symbol, position.position_side)
        if position.position_amt != Decimal("0") or previous_amounts.get(
            identity, Decimal("0")
        ) != Decimal("0"):
            persisted.append(position)
    return tuple(persisted)


# Account facts (private-stream continuity plus an accepted account baseline)
# start from the same bounded startup budget the account stream already uses.
_ACCOUNT_FACTS_STARTUP_TIMEOUT_SECONDS = 30.0


def resolve_live_read_credentials(
    config: LiveRuntimeConfig,
) -> tuple[str, str]:
    """Resolve the read-only account credentials required by live execution.

    The live process owns the private account stream, so it always needs the
    read credential role.  There is deliberately no fallback to the trade
    key/secret: a trade credential with wider authority must never silently
    stand in for a read credential, and a missing read credential must stop the
    process at startup rather than degrade into a different role.
    """

    read_api_key = (config.credentials.read_api_key or "").strip()
    read_api_secret = (config.credentials.read_api_secret or "").strip()
    missing = [
        name
        for name, value in (
            ("BINANCE_READ_API_KEY", read_api_key),
            ("BINANCE_READ_API_SECRET", read_api_secret),
        )
        if not value
    ]
    if missing:
        raise ValueError(
            "Missing required read credentials for live execution: "
            + ", ".join(missing)
        )
    return read_api_key, read_api_secret


def _order_observation_payload(
    coordinator: OrderExecutionCoordinator,
) -> Callable[[], Mapping[str, object]]:
    """Publish the observation queue as a JSON-safe mapping."""

    def _payload() -> Mapping[str, object]:
        metrics = coordinator.order_observation_metrics
        return {
            "capacity": metrics.capacity,
            "depth": metrics.depth,
            "high_watermark": metrics.high_watermark,
            "overflow_count": metrics.overflow_count,
            "enqueued_count": metrics.enqueued_count,
            "dequeued_count": metrics.dequeued_count,
            "oldest_pending_age_seconds": metrics.oldest_pending_age_seconds,
        }

    return _payload


async def run_live_daemon(
    config: LiveRuntimeConfig,
    *,
    shutdown_requested: asyncio.Event | None = None,
) -> LiveDaemonResult:
    validate_live_runtime_endpoints(config)
    # Read credentials are an account-capability requirement of the live
    # execution process (it owns the private account stream), never a function
    # of where market data happens to come from.
    read_api_key, read_api_secret = resolve_live_read_credentials(config)
    execution_database_url = config.databases.execution_database_url
    market_database_url = config.databases.market_database_url
    observability_database_url = config.databases.observability_database_url

    account_label = config.identity.account_label
    strategy_name = config.identity.strategy_name
    session_id = config.identity.session_id
    operator = config.identity.operator
    strategy_config_hash = config.identity.strategy_config_hash
    git_commit_hash = config.identity.git_commit_hash

    market_environment = config.market.market_environment
    market_state_source = config.market.market_state_source
    market_state_hub_url = config.market.market_state_hub_url
    market_quote_hub_url = config.market.market_quote_hub_url
    market_quote_volume_hub_url = config.market.market_quote_volume_hub_url
    market_websocket_url = config.market.market_websocket_url
    account_event_hub_url = config.market.account_event_hub_url
    risk_control_hub_url = config.market.risk_control_hub_url

    profile = config.strategy.profile
    entry_positive_gainer_top_count = config.strategy.entry_positive_gainer_top_count
    require_price_above_ema5 = config.strategy.require_price_above_ema5
    require_price_above_ema10 = config.strategy.require_price_above_ema10
    entry_order_type = config.strategy.entry_order_type
    entry_limit_ttl_seconds = config.strategy.entry_limit_ttl_seconds

    hedge_mode = config.execution.hedge_mode
    exit_mode = config.execution.exit_mode
    entry_long_only = config.execution.entry_long_only
    entry_leverage = config.execution.entry_leverage
    margin_type = config.execution.margin_type
    candle_grace_bars = config.execution.candle_grace_bars
    candle_grace_decision_profit_pct = config.execution.candle_grace_decision_profit_pct
    candle_grace_profit_pct = config.execution.candle_grace_profit_pct
    max_concurrency_per_symbol = config.execution.max_concurrency_per_symbol

    max_runtime_seconds = config.lifecycle.max_runtime_seconds
    poll_interval_seconds = config.lifecycle.poll_interval_seconds
    checkpoint_every_states = config.lifecycle.checkpoint_every_states
    checkpoint_every_seconds = config.lifecycle.checkpoint_every_seconds
    checkpoint_phase_seconds = config.lifecycle.checkpoint_phase_seconds
    persist_exchange_operations = config.lifecycle.persist_exchange_operations

    base_url = config.credentials.base_url
    api_key = config.credentials.api_key
    api_secret = config.credentials.api_secret

    risk_control_enabled = True
    startup_status = LiveRuntimeStartupStatus.from_environment()
    health = startup_status.health
    live_readiness: LiveReadinessPublisher | None = None
    session: RuntimeSession | None = None

    now = datetime.now(tz=UTC)
    ownership_registry = ResourceOwnershipRegistry(run_id=session_id)
    # The producer is assembled before its combined baseline consumer.
    account_baseline_listener: (
        Callable[[AccountEvent, AccountSnapshot], Awaitable[None]] | None
    ) = None
    account_facts_budget_tasks: list[asyncio.Task[bool]] = []
    account_repository: PostgresAccountRepository
    last_balance_values: dict[str, BalanceValue] = {}
    last_position_amounts: dict[tuple[str, str], Decimal] = {}
    execution_engine: AsyncEngine | None = None
    market_engine: AsyncEngine | None = None
    observability_engine: AsyncEngine | None = None
    checkpoint_engine: AsyncEngine | None = None
    heartbeat_engine: AsyncEngine | None = None
    client: BinanceUsdMTradeClient | None = None
    execution_coordinator: OrderExecutionCoordinator | None = None
    order_reconciliation: LiveOrderReconciliation | None = None
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
    signal_recorder: LiveStrategySignalRecorder | None = None
    daemon: LiveStrategyDaemon | None = None
    startup_market_assembly: LiveStartupMarketAssembly | None = None
    hub_source: WebSocketMarketStateSource | None = None
    startup_market_state_task: asyncio.Task[None] | None = None
    channel_sources: LiveChannelSources | None = None
    risk_control_task: asyncio.Task[None] | None = None
    risk_control_runtime: LiveRiskControlRuntime | None = None
    control_plane_runtime: LiveControlPlaneRuntime | None = None
    session_lifecycle: LiveSessionLifecycle | None = None
    shutdown_task: asyncio.Task[bool] | None = None
    risk_config_hash = ""
    startup_phase = True

    def account_facts_status() -> Mapping[str, object]:
        if control_plane_runtime is None:
            return {"ready": None, "reason": "account_facts_unreported"}
        return control_plane_runtime.account_facts_status()

    def account_facts_ready() -> bool:
        if control_plane_runtime is None:
            return False
        return control_plane_runtime.account_snapshot_available and (
            in_process_account_source is None
            or in_process_account_source.is_risk_allowed()
        )

    try:
        persistence = assemble_live_persistence(
            execution_database_url=execution_database_url,
            market_database_url=market_database_url,
            observability_database_url=observability_database_url,
            account_label=account_label,
            strategy_name=strategy_name,
            ownership_registry=ownership_registry,
        )
        execution_engine = persistence.engines.execution_engine
        market_engine = persistence.engines.market_engine
        observability_engine = persistence.engines.observability_engine
        checkpoint_engine = persistence.engines.checkpoint_engine
        heartbeat_engine = persistence.engines.heartbeat_engine

        execution_factory = persistence.factories.execution_factory
        account_repository = PostgresAccountRepository(execution_factory)
        market_factory = persistence.factories.market_factory
        observability_factory = persistence.factories.observability_factory

        live_repository = persistence.repositories.live_repository
        heartbeat_live_repository = persistence.repositories.heartbeat_live_repository
        heartbeat_risk_repository = persistence.repositories.heartbeat_risk_repository
        order_adoption_repository = persistence.repositories.order_adoption_repository
        order_read_repository = persistence.repositories.order_read_repository
        order_event_repository = persistence.repositories.order_event_repository
        submission_repository = persistence.repositories.submission_repository
        checkpoint_repository = persistence.repositories.checkpoint_repository
        telemetry_repository = persistence.repositories.telemetry_repository
        telemetry = LiveRuntimeTelemetry(
            run_id=session_id,
            account_label=account_label,
            strategy_config_hash=strategy_config_hash,
            persist=telemetry_repository.save_runtime_events,
            persist_event_types=(
                PERSISTED_ORDER_TELEMETRY_EVENTS
                | PERSISTED_OPERATIONAL_TELEMETRY_EVENTS
            ),
            persist_exchange_operations=persist_exchange_operations,
        )
        ownership_registry.register("telemetry", telemetry.stop)
        await telemetry.start()
        volume_cache = await assemble_live_quote_volume(
            market_quote_volume_hub_url=market_quote_volume_hub_url,
            market_environment=market_environment,
            session_id=session_id,
            ownership_registry=ownership_registry,
        )
        signal_repository = PostgresLiveSignalRepository(observability_factory)
        decision_unit_of_work = AsyncPostgresDecisionUnitOfWork(execution_factory)
        fact_source = LiveDecisionFactSource(
            account_label,
            obsolete_exit_reasons=frozenset({"max_holding_period"}),
            strategy_name=strategy_name,
            decision_unit_of_work=decision_unit_of_work,
            hedge_mode=hedge_mode,
            request_exit_recovery=lambda: (
                order_reconciliation.request_exit_recovery()
                if order_reconciliation is not None
                else None
            ),
        )
        signal_recorder = LiveStrategySignalRecorder(
            run_id=session_id,
            account_label=account_label,
            code_commit=git_commit_hash,
            quote_volume_provider=volume_cache,
            persist=signal_repository.save_signals,
        )
        ownership_registry.register("signal_recorder", signal_recorder.stop)
        await signal_recorder.start()
        risk_config = await _latest_risk_config(execution_factory, account_label)
        startup_status.log_phase("risk_config_loaded")
        risk_config_hash = risk_config.config_hash
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
        loaded_trading_rules: Mapping[str, SymbolTradingRules] = {}
        try:
            loaded_trading_rules = await _load_trading_rules(market_factory, None)
            trading_rules_hash = compute_trading_rules_hash(
                cast(Mapping[str, TradingRuleHashInput], loaded_trading_rules)
            )
        except Exception as rules_err:
            log.warning("trading_rules_hash_load_failed", error=str(rules_err))
            trading_rules_hash = compute_trading_rules_hash({})
        metadata_snapshot = RuntimeMetadataSnapshot.create(
            environment=market_environment,
            account_label=account_label,
            git_commit=git_commit_hash,
            strategy_config_hash=strategy_config_hash,
            risk_config_hash=risk_config_hash,
            trading_rules_hash=trading_rules_hash,
            started_at=datetime.now(tz=UTC),
        )
        snapshot_dict = metadata_snapshot.to_dict()
        log.info(
            "runtime_metadata_snapshot_created",
            **snapshot_dict,
        )
        try:
            await telemetry.record(
                RUNTIME_METADATA_SNAPSHOT,
                payload=snapshot_dict,
            )
        except Exception as tel_err:
            log.warning("runtime_metadata_telemetry_failed", error=str(tel_err))

        def _write_metadata_disk() -> None:
            meta_dir = Path(os.environ.get("CML_RUNTIME_METADATA_DIR", "/tmp"))
            if meta_dir.exists():
                meta_file = meta_dir / f"runtime_metadata_{session_id}.json"
                meta_file.write_text(json.dumps(snapshot_dict, indent=2))

        try:
            await asyncio.to_thread(_write_metadata_disk)
        except Exception as disk_err:
            log.debug("runtime_metadata_disk_write_failed", error=str(disk_err))

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
        ownership_registry.register("trade_client", client.aclose)
        account_config = await client.fetch_account_config()
        startup_status.log_phase("exchange_account_config_loaded")
        if account_config.hedge_mode != hedge_mode:
            expected = "hedge" if hedge_mode else "one-way"
            actual = "hedge" if account_config.hedge_mode else "one-way"
            raise RuntimeError(
                f"position mode mismatch: expected {expected}, got {actual}"
            )
        in_process_account_source: InProcessAccountEventSource | None = None
        in_process_producer: InProcessAccountStreamProducer | None = None
        if account_event_hub_url and account_event_hub_url.strip():
            expectation_publisher: Any = WebSocketAccountPositionExpectationPublisher(
                url=account_event_hub_url,
                environment="live",
                account_label=account_label,
            )
        else:
            in_process_registry = AccountPositionExpectationRegistry(
                environment="live",
                account_label=account_label,
            )
            expectation_publisher = DirectPositionExpectationPublisher(
                in_process_registry
            )
            in_process_account_source = InProcessAccountEventSource(
                environment="live",
                account_label=account_label,
            )
            read_client = BinanceUsdMPrivateReadClient(
                api_key=read_api_key,
                api_secret=read_api_secret,
                environment="live",
                account_label=account_label,
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
            )
            ownership_registry.register("read_client", read_client.aclose)
            user_data_websocket_url = (
                config.market.user_data_websocket_url
                or os.environ.get("CML_BINANCE_USER_DATA_WEBSOCKET_URL", "").strip()
                or os.environ.get("CML_USER_DATA_WEBSOCKET_URL", "").strip()
                or DEFAULT_BINANCE_USDM_USER_DATA_WEBSOCKET_URL
            )
            if not user_data_websocket_url:
                # The live account stream is not a market-data concern: it is
                # always required by the execution process.
                raise ValueError(
                    "Missing required user_data_websocket_url for the "
                    "in-process account stream"
                )

            def _on_unverified_position(
                symbol: str, position_side: str, quantity: Decimal
            ) -> None:
                log.warning(
                    "in_process_unverified_position_detected",
                    symbol=symbol,
                    position_side=position_side,
                    quantity=str(quantity),
                )
                if in_process_account_source is not None:
                    in_process_account_source._set_fact_gap((symbol,))
                if order_reconciliation is not None:
                    order_reconciliation.request_order_recovery()

            async def _publish_in_process_baseline(
                snapshot: AccountSnapshot,
                event: AccountEvent,
            ) -> AccountSnapshot:
                """Expose the merged account projection to the live context.

                The startup/refresh baseline is published through the same
                control-plane seam as every later frame, so the live context,
                risk limits and the dashboard read one account projection
                instead of the retired execution-account sync process.
                """

                if account_baseline_listener is None:
                    raise RuntimeError("account baseline consumer is not assembled")
                execution_snapshot = snapshot
                if snapshot.positions:
                    loader = (
                        None
                        if entry_runtime is None
                        else entry_runtime.entry_symbol_loader
                    )
                    symbols = (
                        None
                        if loader is None
                        else await loader(snapshot.config.observed_at)
                    )
                    ordered_symbols = {
                        order.symbol.strip().upper() for order in snapshot.open_orders
                    }
                    selected = tuple(
                        position
                        for position in snapshot.positions
                        if position.position_amt != 0
                        or symbols is None
                        or position.symbol.strip().upper() in symbols
                        or position.symbol.strip().upper() in ordered_symbols
                    )
                    recovery_positions = tuple(
                        position
                        for position in selected
                        if position.position_amt != 0
                        or position.symbol.strip().upper() in ordered_symbols
                    )
                    anchors = (
                        await load_fill_recovery_sources(
                            execution_factory,
                            environment="live",
                            account_label=account_label,
                            position_keys=tuple(
                                (position.symbol, position.position_side.upper())
                                for position in recovery_positions
                            ),
                        )
                        if recovery_positions
                        else {}
                    )
                    scans = []
                    fills = {}
                    plans = tuple(
                        (
                            position,
                            plan_fill_scan(
                                position,
                                anchors.get(
                                    (position.symbol, position.position_side.upper())
                                ),
                            ),
                        )
                        for position in recovery_positions
                    )
                    ranges = {}
                    for _, plan in plans:
                        if plan is not None:
                            previous = ranges.get(plan.symbol)
                            ranges[plan.symbol] = (
                                plan.start_time_ms
                                if previous is None
                                else min(previous[0], plan.start_time_ms),
                                plan.checked_through
                                if previous is None
                                else max(previous[1], plan.checked_through),
                            )
                    loaded = {}
                    for symbol, (start, end) in ranges.items():
                        loaded[symbol] = await read_client.fetch_fills_with_provenance(
                            symbol, start_time_ms=start, checked_through=end
                        )
                    for position, plan in plans:
                        if plan is None:
                            continue
                        loaded_fills, page = loaded[plan.symbol]
                        for fill in loaded_fills:
                            if plan.includes_fill(fill):
                                fills[(fill.symbol, fill.trade_id)] = fill
                        scans.append(
                            AccountFillLoadScan(
                                environment="live",
                                account_label=account_label,
                                symbol=position.symbol,
                                position_side=position.position_side.upper(),
                                page_scan=page,
                                observed_at=position.observed_at,
                                source_anchor_id=plan.source_anchor_id,
                                source_anchor_event_cut=plan.source_anchor_event_cut,
                                source_anchor_kind=plan.source_anchor_kind,
                                source_stream_id=plan.source_stream_id,
                                source_stream_epoch=plan.source_stream_epoch,
                                source_anchor_snapshot=plan.source_anchor_snapshot,
                            )
                        )
                    event = replace(
                        event, fills=tuple(fills.values()), fill_load_scans=tuple(scans)
                    )
                    execution_snapshot = replace(snapshot, positions=selected)
                await account_baseline_listener(event, execution_snapshot)
                return snapshot

            async def _persist_in_process_snapshot(
                snapshot: AccountSnapshot,
                event: AccountEvent,
            ) -> None:
                """Persist the account observation and its exchange fills."""

                observed_at = snapshot.config.observed_at
                sync_config = ExecutionAccountSyncConfig(
                    environment="live",
                    account_label=account_label,
                    expected_multi_assets_mode=account_config.multi_assets_mode,
                    expected_hedge_mode=hedge_mode,
                    observed_at=observed_at,
                )
                persisted_balances = select_balance_history(
                    snapshot.balances, last_balance_values
                )
                persisted_positions = _positions_for_persistence(
                    snapshot.positions, last_position_amounts
                )
                await account_repository.save_reconciliation_snapshot(
                    config=snapshot.config,
                    balances=persisted_balances,
                    positions=persisted_positions,
                    open_orders=snapshot.open_orders,
                    fills=event.fills,
                    run=reconciliation_run(
                        sync_config,
                        reconciliation_id=user_data_reconciliation_id(
                            sync_config, event.event_id
                        ),
                        status="ready",
                        mismatch_count=0,
                        details={
                            "source": "in_process_user_data_stream",
                            "event_id": event.event_id,
                            "event_type": event.event_type,
                            "event_at": event.event_at.isoformat(),
                            **position_state_details(snapshot.positions),
                        },
                        balance_count=len(persisted_balances),
                        position_count=sum(
                            1
                            for position in snapshot.positions
                            if position.position_amt != Decimal("0")
                        ),
                        open_order_count=len(snapshot.open_orders),
                        fill_count=len(event.fills),
                    ),
                )
                for balance in snapshot.balances:
                    last_balance_values[balance.asset] = balance_value(balance)
                for position in snapshot.positions:
                    last_position_amounts[(position.symbol, position.position_side)] = (
                        position.position_amt
                    )
                await account_repository.save_process_state(
                    ExecutionAccountProcessState(
                        environment="live",
                        account_label=account_label,
                        state=event.account_state or ExecutionAccountStatus.RUNNING,
                        occurred_at=observed_at,
                        reason=None,
                    )
                )

            in_process_producer = InProcessAccountStreamProducer(
                client=read_client,
                registry=in_process_registry,
                source=in_process_account_source,
                environment="live",
                account_label=account_label,
                websocket_url=user_data_websocket_url,
                require_live_stream=True,
                on_unverified_position=_on_unverified_position,
                read_client=read_client,
                expected_multi_assets_mode=account_config.multi_assets_mode,
                expected_hedge_mode=hedge_mode,
                publish_baseline=_publish_in_process_baseline,
                persist_snapshot=_persist_in_process_snapshot,
            )
        register_expected_entry = LiveEntryExpectationRegistrar(
            account_label=account_label,
            publisher=expectation_publisher,
        )
        order_event_runtime = LiveOrderEventRuntime(
            telemetry=telemetry,
            request_recovery=lambda plan=None: (
                order_reconciliation.request_order_recovery(plan=plan)
                if order_reconciliation is not None
                else None
            ),
        )

        target_notional = config.execution.target_notional
        if target_notional is None or target_notional <= 0:
            raise ValueError(
                "target_notional must be explicitly configured and > 0, "
                f"got {target_notional}"
            )

        effective_policy = build_live_policy(
            config=config,
            target_notional=target_notional,
            risk_config=risk_config,
            strategy_name=strategy_name,
        )

        def _on_projection_recovery_required(client_order_id: str) -> None:
            if order_reconciliation is not None:
                order_reconciliation.request_order_recovery(
                    client_order_id=client_order_id
                )

        execution_runtime = await build_live_execution_runtime(
            sessions=execution_factory,
            exchange=client,
            event_repository=order_event_repository,
            account_label=account_label,
            strategy_name=strategy_name,
            on_event=order_event_runtime.handle,
            on_before_submit=register_expected_entry,
            on_exchange_request=telemetry.exchange_request_started,
            on_exchange_response=telemetry.exchange_response_received,
            submission_repository=submission_repository,
            on_projection_recovery_required=_on_projection_recovery_required,
        )
        execution_book = execution_runtime.book
        execution_coordinator = execution_runtime.coordinator
        ownership_registry.register(
            "execution_coordinator", execution_coordinator.aclose
        )
        fact_source.set_execution_book(
            execution_book,
            register_account_stream=execution_book.register_active_stream,
        )

        async def _handle_decision_exit(cmd: TradeCommand) -> OrderExecutionResult:
            allocs: tuple[ExitAllocation, ...] = ()
            if cmd.allocation_plan:
                allocs = cmd.allocation_plan.allocations
            exit_client_order_id = cmd.client_order_id(session_id)
            candidate_id = f"intent_exit_{cmd.command_id}"
            signal_id = f"sig_exit_{cmd.command_id}"

            features: dict[str, Any] = {
                "command_id": cmd.command_id,
                "position_side": cmd.position_key.position_side.value,
                "quantity": str(cmd.requested_quantity),
                "projection_version": cmd.expected_projection_version,
            }
            if allocs:
                features["batch_id"] = allocs[0].batch_id
                if allocs[0].entry_price is not None:
                    features["entry_price"] = str(allocs[0].entry_price)

            intent = OrderIntentCandidate(
                candidate_id=candidate_id,
                signal_id=signal_id,
                run_id=session_id,
                strategy_name=strategy_name,
                strategy_version="v0",
                config_hash=strategy_config_hash,
                symbol=cmd.position_key.symbol,
                side=cmd.side,
                entry_type=cmd.order_type,
                limit_price=cmd.limit_price,
                desired_notional=None,
                reduce_only=True,
                expires_at=cmd.created_at + timedelta(seconds=600),
                created_at=cmd.created_at,
                reason=cmd.reason or "decision_exit",
                features=features,
            )

            evaluation = RiskEvaluation(
                evaluation_id=f"eval_exit_{cmd.command_id}",
                candidate_id=candidate_id,
                decision=RiskDecision.APPROVED,
                reason="decision_exit_approved",
                evaluated_at=cmd.created_at,
                details={"command_id": cmd.command_id},
            )

            plan = OrderExecutionPlan(
                intent_id=candidate_id,
                run_id=session_id,
                client_order_id=exit_client_order_id,
                symbol=cmd.position_key.symbol,
                side="SELL" if cmd.side == StrategySide.LONG else "BUY",
                order_type=cmd.order_type.value,
                quantity=cmd.requested_quantity,
                price=cmd.limit_price,
                reduce_only=True,
                position_side=cmd.position_key.position_side,
                created_at=cmd.created_at,
                quantized=True,
                allocations=allocs,
                projection_version=cmd.expected_projection_version,
                strategy_name=strategy_name,
                strategy_version="v0",
            )

            res = await execution_coordinator.prepare_and_execute(
                plan,
                preparation=OrderSubmissionPreparation(
                    intent=intent,
                    evaluation=evaluation,
                    environment="live",
                    account_label=account_label,
                    strategy_name=strategy_name,
                ),
            )
            if res is None:
                return OrderExecutionResult(
                    client_order_id=plan.client_order_id,
                    state=ExchangeOrderState.REJECTED,
                    exchange_order_id=None,
                    plan=plan,
                )
            return res

        fact_source.set_exit_handler(_handle_decision_exit)
        fact_source.set_exit_recovery_handler(
            LiveExitReceiptRecovery(
                execution_factory,
                exchange=client,
                account_label=account_label,
                run_id=session_id,
                obsolete_exit_reasons=frozenset({"max_holding_period"}),
            )
        )
        await fact_source.restore()

        entry_order_canceller = LiveEntryOrderCanceller(
            exchange=client,
            state_machine=execution_coordinator,
            repository=order_adoption_repository,
            run_id=session_id,
        )

        async def recover_decision_exits() -> bool:
            if daemon is not None and await daemon.recover_requested_exits():
                exit_channel_runtime.note_account_facts_changed()
            pending = await fact_source.recover_pending_exits(limit=5)
            return pending or (daemon is not None and daemon.has_pending_exit_recovery)

        repair_order_repository = PostgresOrderReadRepository(
            persistence.factories.heartbeat_factory
        )
        order_reconciliation = LiveOrderReconciliation(
            order_repository=repair_order_repository,
            state_machine=execution_coordinator,
            run_id=session_id,
            recover_exits=recover_decision_exits,
            execution_book=execution_book,
        )
        # Restore local facts at startup; remote uncertainty is repaired by the
        # supervised background worker after the trading channels are running.
        startup_status.log_phase("local_order_state_restored")
        draining = await session_state.session_is_draining(live_repository, session_id)
        if not draining:
            await session_lifecycle.transition(LiveSessionState.PREFLIGHT)
        unresolved = await order_read_repository.load_unresolved_orders(session_id)
        durable_terminal_orders: dict[str, Decimal] = {}
        if hasattr(order_read_repository, "load_terminal_orders"):
            try:
                durable_terminal_orders = (
                    await order_read_repository.load_terminal_orders(
                        session_id, limit=2048
                    )
                )
            except Exception as err:
                log.warning("load_terminal_orders_failed", error=str(err))

        terminal_barrier = TerminalOrderBarrier(
            capacity=2048,
            # One durable source answers both "is it terminal" and "how much
            # executed": a separate checker would re-query the same snapshot.
            durable_reader=lambda cid: (
                (True, durable_terminal_orders[cid])
                if cid in durable_terminal_orders
                else None
            ),
            durable_async_checker=getattr(
                order_read_repository, "is_order_terminal", None
            ),
        )
        for cid, exec_qty in durable_terminal_orders.items():
            terminal_barrier.record_terminal(cid, exec_qty)

        entry_order_lifecycle = LiveLimitOrderLifecycle(
            cancel_order=execution_coordinator.cancel_order,
            terminal_barrier=terminal_barrier,
        )
        ownership_registry.register("entry_order_lifecycle", entry_order_lifecycle.stop)
        order_event_runtime.set_terminal_barrier(terminal_barrier)
        order_event_runtime.set_entry_order_lifecycle(entry_order_lifecycle)
        await entry_order_lifecycle.restore(unresolved)
        strategy_config = _live_strategy_config(profile)
        computed_hash = _live_strategy_config_hash(
            strategy_name,
            profile=profile,
            entry_positive_gainer_top_count=entry_positive_gainer_top_count,
            require_price_above_ema5=require_price_above_ema5,
            require_price_above_ema10=require_price_above_ema10,
            entry_order_type=entry_order_type,
            entry_limit_ttl_seconds=entry_limit_ttl_seconds,
        )
        if computed_hash != strategy_config_hash:
            log.warning(
                "strategy_config_hash_mismatch",
                computed=computed_hash,
                configured=strategy_config_hash,
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
        required_data = strategy.required_data
        live_readiness = LiveReadinessPublisher(
            health=health,
            on_publish=telemetry.runtime_readiness,
            account_label=account_label,
            session_id=session_id,
            strategy=strategy_name,
            entry_universe_target_count=entry_positive_gainer_top_count,
            warmup_required_buckets=int(required_data().warmup_buckets),
            # Queue depth and oldest pending age belong next to the gates they
            # affect, so operators see backpressure before it overflows.
            order_observation_metrics=(
                None
                if execution_coordinator is None
                else _order_observation_payload(execution_coordinator)
            ),
            account_facts_status=account_facts_status,
        )
        checkpoint = await checkpoint_repository.load_checkpoint(session_id)
        requires_market_recovery = checkpoint is not None and (
            _checkpoint_needs_market_recovery(checkpoint)
        )
        hub_cursor_state = LiveHubCursorState()
        if checkpoint is not None:
            strategy.restore_checkpoint(checkpoint)
            restored_hub_cursor = hub_cursor_for_startup(
                checkpoint,
                requires_market_recovery=requires_market_recovery,
            )
            if restored_hub_cursor is not None:
                hub_cursor_state.restore(restored_hub_cursor)
            elif (
                requires_market_recovery
                and hub_cursor_from_checkpoint_payload(checkpoint) is not None
            ):
                log.info(
                    "live_hub_cursor_discarded_before_durable_rewarm",
                    reason="checkpoint_requires_market_recovery",
                )
        state_repository = PostgresRuntimeMarketStateRepository(market_factory)
        requested_startup_cutover = _live_market_state_cutover(datetime.now(tz=UTC))
        startup_cutover = await _wait_for_durable_market_state_cutover(
            repository=state_repository,
            environment=market_environment,
            requested_cutover=requested_startup_cutover,
        )
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
        startup_market_assembly = assemble_live_startup_market_buffer(
            market_state_source=market_state_source,
            market_state_hub_url=market_state_hub_url,
            market_environment=market_environment,
            session_id=session_id,
            hub_cursor_state=hub_cursor_state,
        )
        startup_market_buffer = startup_market_assembly.buffer
        hub_source = startup_market_assembly.hub_source
        startup_market_state_task = startup_market_assembly.task
        market_cursor: RuntimeStateCursor | None = None
        if checkpoint is not None:
            if requires_market_recovery:
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
        startup_status.log_phase("strategy_market_warmup_completed")
        if checkpoint is not None and not requires_market_recovery:
            live_readiness.update_warmup_progress(
                strategy,
                expected_symbols=startup_warmup_symbols,
            )
        candle_assembly = assemble_live_candle_sources(
            exit_mode=exit_mode,
            base_url=base_url,
            market_websocket_url=market_websocket_url,
            market_environment=market_environment,
            session_id=session_id,
            require_price_above_ema5=require_price_above_ema5,
            require_price_above_ema10=require_price_above_ema10,
            ownership_registry=ownership_registry,
        )
        candle_source = candle_assembly.candle_source
        closed_candle_feed = candle_assembly.closed_candle_feed
        ema_provider = candle_assembly.ema_provider

        entry_runtime = LiveEntryRuntime(
            universe_reader=(
                PostgresUniverseRepository(market_factory)
                if entry_positive_gainer_top_count is not None
                else None
            ),
            client=client,
            ema_provider=ema_provider,
            positive_gainer_top_count=entry_positive_gainer_top_count,
            entry_leverage=entry_leverage,
            margin_type=margin_type,
        )
        ownership_registry.register("entry_runtime", entry_runtime.stop)
        await entry_runtime.warm_exchange(now)
        startup_status.log_phase("entry_exchange_warmup_completed")
        live_readiness.update_entry_gate(
            entry_universe_count=entry_runtime.entry_universe_count(now),
            entry_enabled=False,
            entry_enabled_reason="entry_runtime_initializing",
        )
        entry_filter_cache_required = entry_runtime.entry_filter_cache_required
        entry_symbol_cache_required = entry_runtime.entry_symbol_cache_required
        daemon_entry_symbol_loader = entry_runtime.entry_symbol_loader

        entry_filter_context_loader = entry_runtime.entry_filter_context_loader

        async def recover_market_state_gap(
            error: LiveMarketStateContinuityError,
        ) -> tuple[MarketState15s, ...]:
            requirement = required_data()
            interval_seconds = requirement.base_state_interval_seconds
            return await _load_live_market_state_gap(
                repository=state_repository,
                environment=market_environment,
                symbol=error.symbol,
                previous_at=error.previous_at,
                current_at=error.current_at,
                interval_seconds=interval_seconds,
            )

        def invalidate_repaired_context() -> None:
            context_provider.invalidate()
            exit_channel_runtime.note_account_facts_changed()

        position_repair = LiveUnmanagedPositionRepair(
            account_label=account_label,
            run_id=session_id,
            book=execution_book,
            uow=PostgresPositionRepairUnitOfWork(
                execution_factory, strategy_name=strategy_name
            ),
            context_is_current=(lambda context: context_provider.is_current(context)),
            invalidate_context=invalidate_repaired_context,
            request_recovery=order_reconciliation.request_position_recovery,
            # One backoff, owned by the recovery worker: an unchanged
            # divergence waits for its retry deadline instead of re-running on
            # every account cut.
            retry_interval_seconds=order_reconciliation.interval_seconds,
        )
        order_reconciliation.repair_positions = position_repair.repair_pending
        context_provider = PostgresLiveContextProvider(
            execution_session_factory=execution_factory,
            market_session_factory=market_factory,
            account_label=account_label,
            run_id=session_id,
            strategy_name=strategy_name,
            strategy_config_hash=strategy_config_hash,
            request_position_repair=position_repair.request,
        )
        context_provider.set_execution_book(execution_book)
        if startup_warmup_symbols:
            await context_provider.warm_symbol_rules(startup_warmup_symbols, now)
        entry_runtime.set_trading_rules_warmer(context_provider.warm_symbol_rules)
        latest_market_states = LatestMarketStateCache()
        latest_market_quotes = LatestMarketQuoteCache()

        def request_unknown_exit(order: PersistedExchangeOrder) -> bool:
            if daemon is None:
                return False
            state = next(
                iter(latest_market_states.for_symbols((order.plan.symbol,))),
                None,
            )
            if state is None:
                return False
            daemon.request_unknown_exit_recovery(order, state)
            return True

        order_reconciliation.request_unknown_exit = request_unknown_exit
        entry_universe_context_provider = entry_runtime.entry_universe_context_provider
        entry_universe_snapshot_provider = (
            entry_runtime.entry_universe_snapshot_provider
        )

        def _resolve_symbol_lot_rules(sym: str) -> SymbolLotRules | None:
            rules = loaded_trading_rules.get(sym)
            if rules is not None:
                return SymbolLotRules(
                    symbol=rules.symbol,
                    tick_size=rules.tick_size,
                    step_size=rules.step_size,
                    min_quantity=rules.min_quantity,
                    max_quantity=rules.max_quantity,
                    min_notional=rules.min_notional,
                )
            return None

        daemon = LiveStrategyDaemon(
            request_exit_recovery=order_reconciliation.request_exit_recovery,
            request_order_cleanup=order_reconciliation.request_order_recovery,
            is_symbol_warmed=lambda s: (
                entry_runtime.is_symbol_warmed(s)
                and context_provider.is_symbol_rules_warmed(s)
            ),
            on_unwarmed_symbol=entry_runtime.trigger_symbol_warmup,
            strategy=strategy,
            risk_gateway=RiskGateway(
                limits=FixedLiveLimits(
                    notional_cap=risk_config.max_order_notional,
                    max_open_positions=risk_config.max_open_positions,
                    max_daily_loss=risk_config.max_daily_loss,
                    max_gross_exposure=risk_config.max_gross_notional,
                    max_concurrency_per_symbol=max_concurrency_per_symbol,
                ),
            ),
            submission_repository=submission_repository,
            persist_checkpoint=checkpoint_repository.save_checkpoint,
            on_checkpoint_saved=startup_status.mark_database_ok,
            execution_coordinator=execution_coordinator,
            context_provider=context_provider,
            telemetry=telemetry,
            market_timing_provider=(
                None
                if startup_market_assembly.timing_tracker is None
                else startup_market_assembly.timing_tracker.timing_for
            ),
            signal_recorder=signal_recorder,
            entry_order_lifecycle=entry_order_lifecycle,
            config=LiveDaemonConfig(
                run_id=session_id,
                account_label=account_label,
                resize_tolerance=Decimal("0.10"),
                checkpoint_every_states=checkpoint_every_states,
                checkpoint_every_seconds=checkpoint_every_seconds,
                checkpoint_phase_seconds=checkpoint_phase_seconds,
                hedge_mode=hedge_mode,
                entry_long_only=entry_long_only,
                entry_symbol_loader=daemon_entry_symbol_loader,
                require_price_above_ema5=require_price_above_ema5,
                require_price_above_ema10=require_price_above_ema10,
                entry_filter_context_loader=entry_filter_context_loader,
                entry_universe_context_provider=(entry_universe_context_provider),
                entry_universe_snapshot_provider=(entry_universe_snapshot_provider),
                entry_order_type=entry_order_type,
                entry_limit_ttl_seconds=entry_limit_ttl_seconds,
                scheduled_risk_window=ScheduledRiskWindowConfig.from_environment(),
                decision_filter=create_authoritative_async_decision_filter(
                    strategy_name,
                    fact_provider=fact_source.build,
                    durable_decision_commit=fact_source.commit_decision,
                    effective_policy_provider=lambda state: replace(
                        effective_policy,
                        symbol_lot_rules=_resolve_symbol_lot_rules(state.symbol),
                    ),
                    clock_sequence_provider=lambda state: max(
                        1, state.source_event_count
                    ),
                ),
                decision_fact_binder=fact_source.bind_context,
            ),
            exit_manager=LiveExitManager(
                config=LiveExitConfig(
                    run_id=session_id,
                    strategy_name=strategy_name,
                    strategy_version="v0",
                    strategy_config_hash=strategy_config_hash,
                    policy=PositionExitPolicy(
                        max_holding_seconds=None,
                        mode=exit_mode,
                    ),
                    account_label=account_label,
                    candle_grace_bars=candle_grace_bars,
                    candle_grace_decision_profit_pct=candle_grace_decision_profit_pct,
                    candle_grace_profit_pct=candle_grace_profit_pct,
                ),
            ),
            exit_recovery_client=client,
            cancel_unfilled_entry_orders=entry_order_canceller.cancel,
            fetch_exchange_positions=client.fetch_positions,
            on_managed_position_symbols=(
                None if closed_candle_feed is None else closed_candle_feed.set_symbols
            ),
            recover_market_state_gap=recover_market_state_gap,
            hub_cursor_provider=(hub_cursor_state.snapshot),
            commit_market_state_cursor=hub_cursor_state.acknowledge_state,
            entered_symbol_lookup=hub_cursor_state.consume_entered_symbol,
            is_symbol_fact_ready=(
                in_process_account_source.is_risk_allowed
                if in_process_account_source is not None
                else None
            ),
            # Account facts (private-stream continuity plus an accepted account
            # baseline) are a separate prerequisite from one symbol's range.
            is_account_facts_ready=account_facts_ready,
        )
        order_event_runtime.set_entry_observer(
            daemon.pending_entries.observe_order_event
        )
        risk_control_dispatcher = RiskControlCommandDispatcher(
            repository=live_repository,
            account_label=account_label,
            strategy_name=strategy_name,
            session_id=session_id,
            cancel_all_open_entries=lambda _command_id: (
                daemon.cancel_all_open_entries()
            ),
            request_flatten=lambda command_id: daemon.request_flatten(
                operation_id=command_id
            ),
            clock=lambda: datetime.now(tz=UTC),
        )

        async def load_risk_control_state() -> tuple[bool, bool]:
            draining_now = await session_state.session_is_draining(
                heartbeat_live_repository,
                session_id,
            )
            active_halts = await heartbeat_risk_repository.load_active_halts(
                "live",
                account_label,
            )
            return draining_now, bool(active_halts)

        def invalidate_live_contexts() -> None:
            context_provider.invalidate()

        def _enforce_account_facts_budget() -> None:
            """Keep entry closed and report the reason while account facts are missing.

            Synchronous-on-purpose: the readiness refresh is called from the
            entry-gate path, which must never await network I/O.
            """

            if control_plane_runtime is None:
                return
            if control_plane_runtime.account_snapshot_available:
                return
            if control_plane_runtime.account_facts_degraded:
                return
            observed_at = control_plane_runtime.account_facts_observed_at
            if observed_at is None:
                # No account evidence has been expected yet; the stream's own
                # failure path owns the reason in that window.
                return
            elapsed = (datetime.now(tz=UTC) - observed_at).total_seconds()
            if elapsed < control_plane_runtime.account_facts_timeout_seconds:
                return
            startup_status.mark_account_facts_timeout(
                control_plane_runtime.account_facts_reason or "account_facts_pending"
            )
            _request_account_facts_budget_check()

        def _request_account_facts_budget_check() -> None:
            task = asyncio.get_running_loop().create_task(
                control_plane_runtime.observe_account_facts_budget()
            )
            account_facts_budget_tasks.append(task)

        def refresh_entry_enabled() -> None:
            if (
                risk_control_runtime is None
                or control_plane_runtime is None
                or entry_runtime is None
            ):
                return
            _enforce_account_facts_budget()
            risk_blocked, risk_reason = risk_control_runtime.entry_gate()
            if (
                in_process_account_source is not None
                and in_process_account_source.has_account_wide_gap
            ):
                # Only an account-wide (transport continuity) gap blocks every
                # symbol.  A symbol-scoped gap is enforced per symbol by the
                # entry gate's is_symbol_fact_ready check.
                risk_blocked = True
                gap_symbols_str = ",".join(
                    sorted(in_process_account_source.gap_symbols)
                )
                risk_reason = f"account_event_fact_gap:{gap_symbols_str}"
            daemon.set_risk_control_entry_blocked(
                risk_blocked,
                reason=risk_reason,
            )
            daemon.refresh_entry_prerequisites(
                session_draining=draining,
                strategy_warmup_ready=control_plane_runtime.strategy_warmup_ready,
                strategy_warmup_reason=(control_plane_runtime.strategy_warmup_reason),
                market_state_available=control_plane_runtime.market_state_available,
                market_state_unavailable_reason=(
                    control_plane_runtime.market_state_unavailable_reason
                ),
            )
            live_readiness.update_entry_gate(
                entry_universe_count=entry_runtime.entry_universe_count(
                    datetime.now(tz=UTC)
                ),
                entry_enabled=daemon.entry_enabled,
                entry_enabled_reason=daemon.entry_enabled_reason,
            )

        control_plane_runtime = LiveControlPlaneRuntime(
            session_id=session_id,
            context_provider=context_provider,
            market_state_available=(
                True
                if startup_market_buffer is None
                else startup_market_buffer.connection_available
            ),
            strategy_warmup_ready=(
                live_readiness.has_warmup_ready_symbols
                if live_readiness is not None
                else False
            ),
            notify_market_state_gap=(
                lambda reason: daemon.notify_market_state_gap(reason=reason)
            ),
            refresh_entry_gate=refresh_entry_enabled,
            telemetry=telemetry,
            clock=lambda: datetime.now(tz=UTC),
            # Reuse the account stream's existing startup budget rather than
            # inventing a second startup policy.
            account_facts_timeout_seconds=_ACCOUNT_FACTS_STARTUP_TIMEOUT_SECONDS,
            # Account facts are asserted only once the account runtime has
            # accepted a baseline; a fresh process starts with them unknown.
            account_facts_ready=(
                in_process_producer is not None
                and in_process_producer.account_state is ExecutionAccountStatus.RUNNING
            ),
        )
        startup_market_assembly.set_control_plane_listener(
            control_plane_runtime.on_market_connection_change
        )
        # The merged producer writes the account projection through this
        # listener, so startup baselines and WS deltas reach the live context
        # exactly like Hub frames used to.
        order_reconciliation.on_unknown_order = (
            control_plane_runtime.on_account_snapshot_recovery
        )

        def _on_overflow_reconcile(affected_symbols: tuple[str, ...]) -> None:
            log.warning(
                "in_process_account_source_overflow_triggered_recovery",
                symbols=affected_symbols,
            )
            if order_reconciliation is not None:
                order_reconciliation.request_order_recovery()
            refresh_entry_enabled()

        if in_process_account_source is not None:
            in_process_account_source.set_on_overflow_reconcile(_on_overflow_reconcile)

        def _on_reconciliation_pass_completed(
            result: ReconciliationPassResult | None = None,
        ) -> None:
            if (
                in_process_account_source is None
                or not in_process_account_source.has_fact_gap
            ):
                return
            if result is None:
                return
            if (
                result.has_exception
                or "positions" in result.failed_tasks
                or "orders" in result.failed_tasks
            ):
                log.warning(
                    "reconciliation_pass_infrastructure_failed_retaining_fact_gaps",
                    failed_tasks=list(result.failed_tasks),
                    failed_symbols=list(result.failed_symbols),
                )
                return

            if in_process_account_source.has_account_wide_gap:
                if (
                    result.success
                    and not result.failed_symbols
                    and not result.has_unknown
                    and not result.has_pending_work
                ):
                    in_process_account_source.clear_fact_gap()
                    refresh_entry_enabled()
                else:
                    log.warning(
                        "reconciliation_pass_wildcard_retained",
                        has_unknown=result.has_unknown,
                        has_pending_work=result.has_pending_work,
                        failed_symbols=list(result.failed_symbols),
                    )
            else:
                cleared_any = False
                for sym in result.verified_symbols:
                    if sym not in result.failed_symbols and sym != "*":
                        in_process_account_source.clear_fact_gap(sym)
                        cleared_any = True
                if cleared_any:
                    refresh_entry_enabled()

        order_reconciliation.on_reconcile_pass_completed = (
            _on_reconciliation_pass_completed
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
            database_mode=(
                not risk_control_hub_url or not risk_control_hub_url.strip()
            ),
        )

        def on_exit_failure(symbol: str, failure: str | None) -> None:
            if failure is not None:
                log.warning("live_exit_failure_observed", symbol=symbol, reason=failure)

        def on_entry_filter_cache_ready(ready: bool) -> None:
            daemon.set_entry_filter_cache_ready(ready)
            refresh_entry_enabled()

        daemon.set_entry_filter_cache_ready(
            not (entry_filter_cache_required or entry_symbol_cache_required)
        )
        entry_runtime.set_ready_callback(on_entry_filter_cache_ready)
        refresh_entry_enabled()
        if session_lifecycle is not None:
            await session_lifecycle.transition(
                LiveSessionState.DRAINING if draining else LiveSessionState.LIVE_ENABLED
            )
        startup_status.readiness = live_readiness
        startup_status.mark_ready()
        startup_status.log_phase("live_readiness_published")
        state_stream = build_live_market_state_stream(
            market_state_source=market_state_source,
            startup_market_buffer=startup_market_buffer,
            strategy=strategy,
            state_repository=state_repository,
            market_environment=market_environment,
            max_runtime_seconds=float(max_runtime_seconds),
            poll_interval_seconds=poll_interval_seconds,
            market_cursor=market_cursor,
        )
        channel_sources = assemble_live_channel_sources(
            market_state_source=market_state_source,
            market_quote_hub_url=market_quote_hub_url,
            market_environment=market_environment,
            account_event_hub_url=account_event_hub_url,
            account_label=account_label,
            session_id=session_id,
            on_account_recovery=control_plane_runtime.on_account_snapshot_recovery,
            hub_source=hub_source,
            risk_control_enabled=risk_control_enabled,
            risk_control_hub_url=risk_control_hub_url,
            strategy_name=strategy_name,
            on_risk_control_connection_change=(
                risk_control_runtime.on_connection_change
                if risk_control_runtime is not None
                else None
            ),
            in_process_account_source=in_process_account_source,
        )
        exit_channel_runtime = LiveExitChannelRuntime(
            daemon=daemon,
            closed_candle_expires_at=daemon.closed_candle_expires_at,
            latest_market_quotes=latest_market_quotes,
            latest_market_states=latest_market_states,
            is_transient_error=runtime_errors.is_transient_runtime_error,
            is_order_identity_conflict=(
                order_identity_errors.is_runtime_order_identity_conflict
            ),
            on_exit_failure=on_exit_failure,
            on_order_identity_conflict=daemon.note_order_identity_conflict,
        )

        async def _on_account_snapshot_combined(
            event: AccountEvent, execution_snapshot: AccountSnapshot | None = None
        ) -> None:
            # The account channel owns and awaits fact ingestion. Publish the
            # ready context only after the execution projection has accepted it.
            if execution_coordinator is not None and (
                event.account_snapshot is not None or event.fills
            ):
                await execution_coordinator.observe_account_snapshot(
                    execution_snapshot
                    if execution_snapshot is not None
                    else event.account_snapshot,
                    fills=event.fills,
                    fill_load_scans=event.fill_load_scans,
                    stream_id="account_event_hub",
                    stream_epoch=event.stream_epoch,
                    sequence=event.sequence,
                    evidence_id=event.event_id,
                    hedge_mode=hedge_mode,
                )
                if event.stream_epoch is None or event.sequence <= 0:
                    raise ValueError(
                        "durable account event is missing stream epoch or sequence"
                    )
                fact_source.bind_account_stream(
                    stream_id="account_event_hub",
                    stream_epoch=event.stream_epoch,
                    sequence=event.sequence,
                )
                # A durable accepted exit is dispatchable only after the
                # account facts for the current Hub epoch have reached the
                # restored Book and its original projection token still wins.
                order_reconciliation.notify_account_facts_changed()
            control_plane_runtime.on_account_snapshot(event)
            if event.account_snapshot is not None or event.fills:
                exit_channel_runtime.note_account_facts_changed(event.symbols)

        account_baseline_listener = _on_account_snapshot_combined
        account_event_runtime = LiveAccountEventRuntime(
            daemon=daemon,
            latest_market_states=latest_market_states,
            latest_market_quotes=latest_market_quotes,
            order_reconciliation=order_reconciliation,
            run_id=session_id,
            telemetry=telemetry,
            is_transient_error=runtime_errors.is_transient_runtime_error,
            is_order_identity_conflict=(
                order_identity_errors.is_runtime_order_identity_conflict
            ),
            on_exit_failure=on_exit_failure,
            on_account_snapshot=_on_account_snapshot_combined,
            on_account_snapshot_recovery=(
                control_plane_runtime.on_account_snapshot_recovery
            ),
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
        if channel_sources.quote_source is not None:
            quote_task = asyncio.create_task(
                exit_channel_runtime.run_quote_channel(
                    source=channel_sources.quote_source,
                )
            )

        def observe_live_market_state(
            state: MarketState15s,
            *,
            strategy: ReadinessStrategy,
            entry_universe_count: int,
        ) -> None:
            live_readiness.observe_market_state(
                state,
                strategy=strategy,
                entry_universe_count=entry_universe_count,
            )
            warmup_ready = live_readiness.has_warmup_ready_symbols
            control_plane_runtime.set_strategy_warmup_ready(
                warmup_ready,
                reason=(
                    "strategy_warmup_ready"
                    if warmup_ready
                    else "strategy_warmup_incomplete"
                ),
            )

        market_task = asyncio.create_task(
            daemon.run(
                _observe_market_states(
                    state_stream,
                    latest_market_states,
                    on_observed=observe_live_market_state,
                    strategy=strategy,
                    entry_universe_count=entry_runtime.entry_universe_count,
                )
            )
        )
        account_task = asyncio.create_task(
            account_event_runtime.run(channel_sources.account_source),
            name=f"live-account-events:{session_id}",
        )
        account_producer_task: asyncio.Task[None] | None = None
        if in_process_producer is not None:
            ownership_registry.register("in_process_producer", in_process_producer.stop)
            account_producer_task = in_process_producer.start()

        async def _run_durable_risk_control_poller() -> None:
            if risk_control_runtime is not None:
                await risk_control_runtime.reconcile()
            while True:
                try:
                    if risk_control_runtime is not None:
                        await risk_control_runtime.reconcile()
                    pending_commands = (
                        await live_repository.load_pending_requested_commands(
                            account_label=account_label,
                            strategy_name=strategy_name,
                            session_id=session_id,
                            limit=16,
                        )
                    )
                    if risk_control_dispatcher is not None:
                        # Operator work claimed before a restart is not
                        # "requested" any more; continue its responsibility
                        # explicitly instead of stranding it.
                        await risk_control_dispatcher.recover_executing_commands(
                            limit=16
                        )
                    for cmd in pending_commands:
                        action: RiskControlAction | None = None
                        if cmd.command_type == CANCEL_ALL_OPEN_ENTRIES_COMMAND:
                            action = RiskControlAction.CANCEL_ALL_OPEN_ENTRIES
                        elif cmd.command_type == EMERGENCY_FLATTEN_COMMAND:
                            action = RiskControlAction.REQUEST_FLATTEN
                        if action is not None:
                            evt = RiskControlEvent(
                                environment="live",
                                account_label=account_label,
                                strategy_name=strategy_name,
                                session_id=session_id,
                                action=action,
                                event_id=f"evt-{cmd.command_id}",
                                command_id=cmd.command_id,
                                reason="durable_poller",
                                issued_at=cmd.requested_at,
                                details={"command_type": cmd.command_type},
                            )
                            await risk_control_runtime.on_event(evt)
                except asyncio.CancelledError:
                    break
                except Exception as err:
                    log.warning("durable_risk_control_poller_error", error=str(err))
                    if risk_control_runtime is not None:
                        risk_control_runtime.mark_database_failed()
                await asyncio.sleep(1.0)

        risk_control_task: asyncio.Task[None] | None = None
        if (
            channel_sources.risk_control_source is not None
            and risk_control_runtime is not None
        ):
            risk_control_task = asyncio.create_task(
                _run_risk_control_channel(
                    source=channel_sources.risk_control_source,
                    on_event=risk_control_runtime.on_event,
                ),
                name=f"live-risk-control:{session_id}",
            )
        elif risk_control_runtime is not None:
            risk_control_task = asyncio.create_task(
                _run_durable_risk_control_poller(),
                name=f"live-durable-risk-control:{session_id}",
            )
        (
            entry_filter_cache_task,
            entry_symbol_cache_task,
        ) = entry_runtime.start()
        order_reconciliation.request_recovery()
        reconcile_task = asyncio.create_task(order_reconciliation.run_requested())
        local_health_task: asyncio.Task[None] | None = None

        if health is not None:
            resource_sampler = ProcessResourceSampler()

            def account_event_resource_snapshot() -> dict[str, int | float | None]:
                metrics = channel_sources.account_source.metrics
                return {
                    **resource_sampler.snapshot(),
                    "account_event_client_queue_capacity": metrics.queue_capacity,
                    "account_event_client_queue_depth": metrics.queue_depth,
                    "account_event_client_queue_high_watermark": (
                        metrics.queue_high_watermark
                    ),
                    "account_event_client_enqueue_count": (
                        metrics.enqueued_event_count
                    ),
                    "account_event_client_dequeue_count": (
                        metrics.dequeued_event_count
                    ),
                    "account_event_client_order_trade_update_count": (
                        metrics.order_trade_update_count
                    ),
                    "account_event_client_account_update_count": (
                        metrics.account_update_count
                    ),
                    "account_event_client_max_queue_wait_ms": (
                        metrics.max_queue_wait_ms
                    ),
                    "account_event_client_queue_overflow_count": (
                        metrics.queue_overflow_count
                    ),
                }

            def runtime_is_degraded() -> bool:
                exit_stream_available = (
                    closed_candle_feed is None
                    or closed_candle_feed.connection_available
                )
                live_readiness.update_exit_stream(available=exit_stream_available)
                return (
                    not exit_stream_available
                    or market_task.done()
                    or account_task.done()
                    or (risk_control_task is not None and risk_control_task.done())
                )

            health_monitor = LiveHealthMonitor(
                health=health,
                interval_seconds=_LIVE_STATUS_HEARTBEAT_INTERVAL_SECONDS,
                is_degraded=runtime_is_degraded,
                resource_dimensions={
                    "account_label": account_label,
                    "run_id": session_id,
                },
                resource_snapshot=account_event_resource_snapshot,
            )
            local_health_task = asyncio.create_task(
                health_monitor.run(),
                name=f"live-local-health:{session_id}",
            )

        async def close_risk_control() -> None:
            if risk_control_runtime is not None:
                await risk_control_runtime.close()

        async def stop_entry_caches() -> None:
            if entry_runtime is not None:
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
                reconcile=reconcile_task,
                startup_market=startup_market_state_task,
                quote=quote_task,
                closed_candle=closed_candle_task,
                grace_timeout=grace_timeout_task,
                risk_control=risk_control_task,
                entry_filter_cache=entry_filter_cache_task,
                entry_symbol_cache=entry_symbol_cache_task,
                local_health=local_health_task,
                account_producer=account_producer_task,
                shutdown=shutdown_task,
            ),
            block_entry_submissions=(
                lambda: (
                    execution_coordinator.block_entry_submissions()
                    if execution_coordinator is not None
                    else None
                )
            ),
            stop_sources=channel_sources.stop_all,
            close_risk_control=close_risk_control,
            stop_entry_caches=stop_entry_caches,
            wait_for_entry_submissions_idle=(
                execution_coordinator.wait_for_entry_submissions_idle
                if execution_coordinator is not None
                else None
            ),
            run_id=session_id,
            shutdown_timeout_seconds=_LIVE_RUNTIME_SHUTDOWN_TIMEOUT_SECONDS,
        )
        resource_lifecycle = LiveResourceLifecycle(
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
        )

        async def _save_final_checkpoint(timeout_seconds: float | None) -> bool:
            if daemon is not None:
                return await daemon.checkpoint_coordinator.save_final(
                    timeout_seconds=timeout_seconds
                )
            return True

        async def _transition_terminal_state(reason: str | None) -> None:
            if session_lifecycle is not None:
                await session_lifecycle.transition(
                    LiveSessionState.HALTED
                    if reason is not None
                    else LiveSessionState.COMPLETED,
                    reason=reason,
                )

        session = RuntimeSession(
            run_id=session_id,
            supervisor=runtime_supervisor,
            lifecycle=resource_lifecycle,
            ownership_registry=ownership_registry,
            save_final_checkpoint=_save_final_checkpoint,
            transition_terminal_state=_transition_terminal_state,
            health=health,
            shutdown_budget_seconds=_LIVE_RUNTIME_SHUTDOWN_TIMEOUT_SECONDS,
        )
        ownership_registry.disarm()
        startup_phase = False
        return await session.run()

    except Exception as exc:
        log.exception(
            "live_runtime_failed",
            account_label=account_label,
            session_id=session_id,
            error_type=type(exc).__name__,
        )
        if startup_phase and _is_retryable_live_startup_error(exc):
            raise _LiveStartupRetryableError(exc) from exc
        if session_lifecycle is not None and risk_config_hash:
            try:
                await session_lifecycle.transition(
                    LiveSessionState.HALTED,
                    reason=str(exc),
                )
            except Exception:
                log.exception("session_lifecycle_halt_transition_failed")
        raise
    finally:
        if session is not None:
            await session.close()
        else:
            if hub_source is not None:
                try:
                    hub_source.stop()
                except Exception:
                    log.exception("startup_hub_source_stop_failed")
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
                except (TimeoutError, asyncio.CancelledError, Exception):
                    pass
            if shutdown_task is not None and not shutdown_task.done():
                shutdown_task.cancel()
            if shutdown_task is not None:
                try:
                    await asyncio.gather(shutdown_task, return_exceptions=True)
                except (TimeoutError, asyncio.CancelledError, Exception):
                    pass
            try:
                await ownership_registry.teardown_all()
            except Exception:
                log.exception("ownership_registry_teardown_failed")
