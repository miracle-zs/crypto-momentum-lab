import asyncio
import json
import os
import re
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Collection,
)
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Annotated
from uuid import uuid4

import structlog
import typer
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from crypto_momentum_lab.config import (
    BinanceCredentialRole,
    CredentialResolutionError,
    ResolvedBinanceCredentials,
    resolve_database_url,
    resolve_role_credentials,
)
from crypto_momentum_lab.domain.execution import (
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.live_rollout import (
    LiveOperatorApproval,
    LiveSessionState,
    LiveSessionTransition,
    RollbackCommand,
)
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.risk import (
    RiskConfigSnapshot,
    RiskEvaluation,
    TradingLease,
    TradingLeaseState,
)
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    RunMode,
    StrategyCheckpoint,
    StrategyRunIdentity,
    deterministic_config_hash,
)
from crypto_momentum_lab.execution_account.binance import (
    BinanceUsdMTradeClient,
)
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
    RiskControlAction,
    RiskControlEvent,
    WebSocketRiskControlPublisher,
    WebSocketRiskControlSource,
)
from crypto_momentum_lab.health import LocalHealthWriter
from crypto_momentum_lab.live_rollout.account_channel import (
    LiveAccountEventRuntime,
)
from crypto_momentum_lab.live_rollout.closed_candle_feed import (
    BinanceClosedCandle15mFeed,
    ClosedCandle15mFeedConfig,
)
from crypto_momentum_lab.live_rollout.commands import (
    CANCEL_ALL_OPEN_ENTRIES_COMMAND,
    CANCEL_ALL_OPEN_ENTRIES_CONFIRMATION,
    EMERGENCY_FLATTEN_COMMAND,
    EMERGENCY_FLATTEN_CONFIRMATION,
)
from crypto_momentum_lab.live_rollout.control_plane import (
    LiveControlPlaneRuntime,
)
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
from crypto_momentum_lab.live_rollout.exit_channels import (
    LiveExitChannelRuntime,
)
from crypto_momentum_lab.live_rollout.exits import (
    LiveExitConfig,
    LiveExitManager,
)
from crypto_momentum_lab.live_rollout.gates import LiveGateContext, evaluate_live_gate
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
from crypto_momentum_lab.live_rollout.missing_order_resolution import (
    resolve_missing_live_order as _resolve_missing_live_order,
)
from crypto_momentum_lab.live_rollout.missing_order_resolution import (
    validate_missing_order_resolution as _validate_missing_order_resolution_impl,
)
from crypto_momentum_lab.live_rollout.order_event_runtime import (
    LiveOrderEventRuntime,
)
from crypto_momentum_lab.live_rollout.order_reconciliation import (
    LiveOrderReconciliation,
)
from crypto_momentum_lab.live_rollout.postgres_runtime import (
    PostgresLiveContextProvider,
    live_limits_from_approval,
    poll_live_market_states,
)
from crypto_momentum_lab.live_rollout.profile import LiveOrderFlowImpulseProfile
from crypto_momentum_lab.live_rollout.risk_control import (
    LiveRiskControlRuntime,
    RiskControlCommandDispatcher,
)
from crypto_momentum_lab.live_rollout.runtime_manifest import (
    LiveRuntimeAccount,
    RuntimeManifestError,
    load_live_runtime_manifest,
)
from crypto_momentum_lab.live_rollout.runtime_supervisor import (
    LiveRuntimeSupervisor,
    LiveRuntimeTasks,
)
from crypto_momentum_lab.live_rollout.scheduled_risk_window import (
    ScheduledRiskWindowConfig,
)
from crypto_momentum_lab.live_rollout.session import (
    LiveRolloutSession,
    LiveSessionConfig,
    LiveSessionResult,
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
from crypto_momentum_lab.live_rollout.startup_resilience import (
    run_with_live_startup_backoff as _run_with_live_startup_backoff,
)
from crypto_momentum_lab.live_rollout.stream_recovery import (
    resilient_market_state_stream as _resilient_market_state_stream,
)
from crypto_momentum_lab.live_rollout.stream_recovery import (
    resilient_risk_control_stream as _resilient_risk_control_stream,
)
from crypto_momentum_lab.live_rollout.submission_fence import (
    LiveSubmissionFence,
)
from crypto_momentum_lab.live_rollout.telemetry import (
    PERSISTED_OPERATIONAL_TELEMETRY_EVENTS,
    PERSISTED_ORDER_TELEMETRY_EVENTS,
    LiveRuntimeTelemetry,
    LiveTelemetrySink,
)
from crypto_momentum_lab.live_rollout.volume import Binance24hQuoteVolumeCache
from crypto_momentum_lab.market_data.binance.rest import BinanceUsdMRestClient
from crypto_momentum_lab.market_data.hub import (
    WebSocketMarketStateSource,
)
from crypto_momentum_lab.market_data.quote_hub import (
    WebSocketMarketQuoteSource,
)
from crypto_momentum_lab.persistence.postgres.live_rollout_repository import (
    PostgresLiveRolloutRepository,
)
from crypto_momentum_lab.persistence.postgres.live_signal_repository import (
    PostgresLiveSignalRepository,
)
from crypto_momentum_lab.persistence.postgres.models import (
    LiveSessionTransitionRow,
    OrderIntentExecutionRow,
    ShadowSessionRow,
)
from crypto_momentum_lab.persistence.postgres.order_repository import (
    PostgresOrderRepository,
)
from crypto_momentum_lab.persistence.postgres.paper_daemon_repository import (
    PostgresPaperDaemonRepository,
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
from crypto_momentum_lab.strategy_runner.registry import (
    build_runtime_config,
    build_runtime_strategy,
)

app = typer.Typer(no_args_is_help=True)
log = structlog.get_logger()


_PREPARE_CONFIRMATION = "PREPARE LIVE RISK GATES"
_RENEW_LEASE_CONFIRMATION = "RENEW LIVE RISK LEASE"
_RESOLVE_MISSING_ORDER_CONFIRMATION = "RESOLVE MISSING LIVE ORDER"
_LIVE_ENTRY_POLICY_MODES = frozenset({"legacy", "compare_only", "enforce"})
# These two columns are retained by the existing risk-config schema for paper
# and shadow sessions. Live execution no longer enforces state-age limits; the
# large compatibility value makes that explicit without a destructive schema
# migration.
_LIVE_UNENFORCED_STATE_AGE_SECONDS = 1_000_000_000.0
_LIVE_STARTUP_BUFFER_LIMIT = 100_000
_LIVE_AUTO_REACQUIRE_LEASE_TTL_SECONDS = 300
_LIVE_LEASE_RENEW_BEFORE_SECONDS = 120
_LIVE_LEASE_HEARTBEAT_INTERVAL_SECONDS = 15.0
_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT = 100
_LIVE_ENTRY_PRICE_ABOVE_EMA5 = False
_LIVE_ENTRY_PRICE_ABOVE_EMA10 = False
_LIVE_ENTRY_ORDER_TYPE = EntryType.LIMIT
_LIVE_ENTRY_LIMIT_TTL_SECONDS = 900
_LIVE_ORDERFLOW_PROFILE = LiveOrderFlowImpulseProfile()
_LIVE_MARKET_WEBSOCKET_URL = "wss://fstream.binance.com/market/ws"
_DEFAULT_PERSIST_EXCHANGE_OPERATIONS = frozenset({"submit", "cancel"})
_GIT_COMMIT_HASH_LENGTH = 40
_CONFIG_HASH_LENGTH = 64
_HEX_HASH_PATTERN = re.compile(r"^[0-9a-f]+$")
_PENDING_POSITION_RETRY_DELAYS_SECONDS = (
    0.25,
    0.5,
    1.0,
    2.0,
    4.0,
    8.0,
    16.0,
    32.0,
)


@dataclass(frozen=True, slots=True)
class _PreflightRuntimeStrategyConfig:
    """Runtime strategy inputs used by the preflight hash diagnostic."""

    profile: LiveOrderFlowImpulseProfile
    entry_positive_gainer_top_count: int
    require_price_above_ema5: bool
    require_price_above_ema10: bool
    entry_policy_mode: str
    entry_order_type: EntryType
    entry_limit_ttl_seconds: int

    @property
    def entry_policy_enforce(self) -> bool:
        return self.entry_policy_mode == "enforce"


@app.callback()
def live_rollout_app() -> None:
    """Operate explicitly approved small-capital live sessions."""


@app.command("strategy-config-hash")
def strategy_config_hash_command(
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    runtime_manifest: Annotated[
        Path | None,
        typer.Option(
            "--runtime-manifest",
            help="Derive hash inputs from the desired runtime manifest.",
        ),
    ] = None,
    impulse_window_buckets: Annotated[
        int | None,
        typer.Option("--impulse-window-buckets", min=2),
    ] = None,
    confirmation_buckets: Annotated[
        int | None,
        typer.Option("--confirmation-buckets", min=1),
    ] = None,
    min_return_pct: Annotated[
        str | None,
        typer.Option("--min-return-pct"),
    ] = None,
    min_imbalance: Annotated[
        str | None,
        typer.Option("--min-imbalance"),
    ] = None,
    min_intensity: Annotated[
        str | None,
        typer.Option("--min-intensity"),
    ] = None,
    min_notional_5m_vs_30m: Annotated[
        str | None,
        typer.Option("--min-notional-5m-vs-30m"),
    ] = None,
    cooldown_buckets: Annotated[
        int | None,
        typer.Option("--cooldown-buckets", min=0),
    ] = None,
    entry_positive_gainer_top_count: Annotated[
        int | None,
        typer.Option("--entry-positive-gainer-top-count", min=1),
    ] = None,
    entry_price_above_ema5: Annotated[
        bool,
        typer.Option("--entry-price-above-ema5/--no-entry-price-above-ema5"),
    ] = _LIVE_ENTRY_PRICE_ABOVE_EMA5,
    entry_price_above_ema10: Annotated[
        bool,
        typer.Option("--entry-price-above-ema10/--no-entry-price-above-ema10"),
    ] = _LIVE_ENTRY_PRICE_ABOVE_EMA10,
    entry_policy_enforce: Annotated[
        bool,
        typer.Option(
            "--entry-policy-enforce/--no-entry-policy-enforce",
            help="Use the shared Policy for real entry eligibility decisions.",
        ),
    ] = False,
    entry_order_type: Annotated[
        EntryType,
        typer.Option("--entry-order-type"),
    ] = _LIVE_ENTRY_ORDER_TYPE,
    entry_limit_ttl_seconds: Annotated[
        int,
        typer.Option("--entry-limit-ttl-seconds", min=601),
    ] = _LIVE_ENTRY_LIMIT_TTL_SECONDS,
) -> None:
    if runtime_manifest is not None:
        manifest_account = _runtime_manifest_account_for_cli(
            runtime_manifest,
            account_label=account_label,
            strategy=strategy,
        )
        typer.echo(_runtime_manifest_strategy_config_hash(manifest_account))
        return
    profile = _resolve_live_profile_options(
        impulse_window_buckets=impulse_window_buckets,
        confirmation_buckets=confirmation_buckets,
        min_return_pct=min_return_pct,
        min_imbalance=min_imbalance,
        min_intensity=min_intensity,
        min_notional_5m_vs_30m=min_notional_5m_vs_30m,
        cooldown_buckets=cooldown_buckets,
    )
    entry_positive_gainer_top_count = _resolve_live_entry_positive_gainer_top_count(
        entry_positive_gainer_top_count
    )
    typer.echo(
        _live_strategy_config_hash(
            strategy,
            profile=profile,
            entry_positive_gainer_top_count=entry_positive_gainer_top_count,
            require_price_above_ema5=entry_price_above_ema5,
            require_price_above_ema10=entry_price_above_ema10,
            entry_policy_enforce=entry_policy_enforce,
            entry_order_type=entry_order_type,
            entry_limit_ttl_seconds=entry_limit_ttl_seconds,
        )
    )


@app.command("prepare")
def prepare_command(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    runtime_manifest: Annotated[
        Path | None,
        typer.Option(
            "--runtime-manifest",
            help="Validate this account against the desired runtime manifest.",
        ),
    ] = None,
    impulse_window_buckets: Annotated[
        int | None,
        typer.Option("--impulse-window-buckets", min=2),
    ] = None,
    confirmation_buckets: Annotated[
        int | None,
        typer.Option("--confirmation-buckets", min=1),
    ] = None,
    min_return_pct: Annotated[
        str | None,
        typer.Option("--min-return-pct"),
    ] = None,
    min_imbalance: Annotated[
        str | None,
        typer.Option("--min-imbalance"),
    ] = None,
    min_intensity: Annotated[
        str | None,
        typer.Option("--min-intensity"),
    ] = None,
    min_notional_5m_vs_30m: Annotated[
        str | None,
        typer.Option("--min-notional-5m-vs-30m"),
    ] = None,
    cooldown_buckets: Annotated[
        int | None,
        typer.Option("--cooldown-buckets", min=0),
    ] = None,
    lease_owner: Annotated[str, typer.Option("--lease-owner")] = "live-worker",
    git_commit_hash: Annotated[
        str,
        typer.Option("--git-commit-hash"),
    ] = "",
    migration_revision: Annotated[
        str,
        typer.Option("--migration-revision"),
    ] = "",
    lease_ttl_seconds: Annotated[
        int,
        typer.Option("--lease-ttl-seconds", min=180),
    ] = 300,
    max_order_notional: Annotated[
        str,
        typer.Option("--max-order-notional"),
    ] = "unlimited",
    max_gross_notional: Annotated[
        str,
        typer.Option("--max-gross-notional"),
    ] = "unlimited",
    max_daily_loss: Annotated[
        str,
        typer.Option("--max-daily-loss"),
    ] = "unlimited",
    max_open_positions: Annotated[
        str,
        typer.Option("--max-open-positions"),
    ] = "unlimited",
    entry_positive_gainer_top_count: Annotated[
        int,
        typer.Option("--entry-positive-gainer-top-count", min=1),
    ] = _LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT,
    entry_price_above_ema5: Annotated[
        bool,
        typer.Option("--entry-price-above-ema5/--no-entry-price-above-ema5"),
    ] = _LIVE_ENTRY_PRICE_ABOVE_EMA5,
    entry_price_above_ema10: Annotated[
        bool,
        typer.Option("--entry-price-above-ema10/--no-entry-price-above-ema10"),
    ] = _LIVE_ENTRY_PRICE_ABOVE_EMA10,
    entry_policy_enforce: Annotated[
        bool,
        typer.Option(
            "--entry-policy-enforce/--no-entry-policy-enforce",
            help="Use the shared Policy for real entry eligibility decisions.",
        ),
    ] = False,
    entry_order_type: Annotated[
        EntryType,
        typer.Option("--entry-order-type"),
    ] = _LIVE_ENTRY_ORDER_TYPE,
    entry_limit_ttl_seconds: Annotated[
        int,
        typer.Option("--entry-limit-ttl-seconds", min=601),
    ] = _LIVE_ENTRY_LIMIT_TTL_SECONDS,
    confirmation: Annotated[str, typer.Option("--confirmation")] = "",
) -> None:
    if confirmation != _PREPARE_CONFIRMATION:
        raise typer.BadParameter(f"--confirmation must equal '{_PREPARE_CONFIRMATION}'")
    manifest_account = (
        None
        if runtime_manifest is None
        else _runtime_manifest_account_for_cli(
            runtime_manifest,
            account_label=account_label,
            strategy=strategy,
        )
    )
    configured_git_commit = git_commit_hash.strip() or os.environ.get(
        "CML_CODE_COMMIT",
        "",
    ).strip()
    if manifest_account is not None:
        manifest_git_commit = _validate_hex_hash(
            manifest_account.image_commit,
            "runtime manifest image_commit",
            _GIT_COMMIT_HASH_LENGTH,
        )
        if (
            configured_git_commit
            and configured_git_commit.lower() != manifest_git_commit
        ):
            raise typer.BadParameter(
                "git commit does not match the runtime manifest"
            )
        configured_git_commit = manifest_git_commit
    git_commit_hash = _validate_hex_hash(
        configured_git_commit,
        "--git-commit-hash or CML_CODE_COMMIT",
        _GIT_COMMIT_HASH_LENGTH,
    )
    if manifest_account is not None:
        configured_migration_revision = migration_revision.strip() or os.environ.get(
            "CML_LIVE_MIGRATION_REVISION",
            "",
        ).strip()
        if (
            configured_migration_revision
            and configured_migration_revision != manifest_account.migration_revision
        ):
            raise typer.BadParameter(
                "migration revision does not match the runtime manifest"
            )
    if manifest_account is not None and lease_owner != manifest_account.lease_owner:
        raise typer.BadParameter("lease owner does not match the runtime manifest")
    if manifest_account is None:
        profile = _resolve_live_profile_options(
            impulse_window_buckets=impulse_window_buckets,
            confirmation_buckets=confirmation_buckets,
            min_return_pct=min_return_pct,
            min_imbalance=min_imbalance,
            min_intensity=min_intensity,
            min_notional_5m_vs_30m=min_notional_5m_vs_30m,
            cooldown_buckets=cooldown_buckets,
        )
    else:
        strategy_inputs = manifest_account.strategy_inputs
        profile = strategy_inputs.profile
        entry_positive_gainer_top_count = (
            strategy_inputs.entry_positive_gainer_top_count
        )
        entry_price_above_ema5 = strategy_inputs.require_price_above_ema5
        entry_price_above_ema10 = strategy_inputs.require_price_above_ema10
        entry_policy_enforce = strategy_inputs.entry_policy_enforce
        entry_order_type = strategy_inputs.entry_order_type
        entry_limit_ttl_seconds = strategy_inputs.entry_limit_ttl_seconds
    strategy_config_hash = _live_strategy_config_hash(
        strategy,
        profile=profile,
        entry_positive_gainer_top_count=entry_positive_gainer_top_count,
        require_price_above_ema5=entry_price_above_ema5,
        require_price_above_ema10=entry_price_above_ema10,
        entry_policy_enforce=entry_policy_enforce,
        entry_order_type=entry_order_type,
        entry_limit_ttl_seconds=entry_limit_ttl_seconds,
    )
    if (
        manifest_account is not None
        and strategy_config_hash
        != _runtime_manifest_strategy_config_hash(manifest_account)
    ):
        raise typer.BadParameter(
            "prepared strategy inputs do not match the runtime manifest hash"
        )
    payload = asyncio.run(
        _prepare_live_risk_gates(
            database_url=_database_url(database_url),
            account_label=account_label,
            strategy_name=strategy,
            lease_owner=lease_owner,
            code_generation=git_commit_hash,
            lease_ttl_seconds=lease_ttl_seconds,
            max_order_notional=_parse_optional_decimal_limit(
                max_order_notional,
                "--max-order-notional",
            ),
            max_gross_notional=_parse_optional_decimal_limit(
                max_gross_notional,
                "--max-gross-notional",
            ),
            max_daily_loss=_parse_optional_decimal_limit(
                max_daily_loss,
                "--max-daily-loss",
            ),
            max_open_positions=_parse_optional_integer_limit(
                max_open_positions,
                "--max-open-positions",
            ),
            profile=profile,
            entry_positive_gainer_top_count=entry_positive_gainer_top_count,
            require_price_above_ema5=entry_price_above_ema5,
            require_price_above_ema10=entry_price_above_ema10,
            entry_policy_enforce=entry_policy_enforce,
            entry_order_type=entry_order_type,
            entry_limit_ttl_seconds=entry_limit_ttl_seconds,
        )
    )
    typer.echo(json.dumps(payload, sort_keys=True))


@app.command("renew-lease")
def renew_lease_command(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    lease_owner: Annotated[str, typer.Option("--lease-owner")] = "live-worker",
    lease_ttl_seconds: Annotated[
        int,
        typer.Option("--lease-ttl-seconds", min=300),
    ] = 3600,
    confirmation: Annotated[str, typer.Option("--confirmation")] = "",
) -> None:
    if confirmation != _RENEW_LEASE_CONFIRMATION:
        raise typer.BadParameter(
            f"--confirmation must equal '{_RENEW_LEASE_CONFIRMATION}'"
        )
    payload = asyncio.run(
        _renew_live_lease(
            database_url=_database_url(database_url),
            account_label=account_label,
            strategy_name=strategy,
            lease_owner=lease_owner,
            lease_ttl_seconds=lease_ttl_seconds,
        )
    )
    typer.echo(json.dumps(payload, sort_keys=True))


@app.command("approve")
def approve_command(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    strategy_config_hash: Annotated[str, typer.Option("--strategy-config-hash")] = "",
    risk_config_hash: Annotated[str, typer.Option("--risk-config-hash")] = "",
    git_commit_hash: Annotated[str, typer.Option("--git-commit-hash")] = "",
    migration_revision: Annotated[str, typer.Option("--migration-revision")] = "",
    notional_cap: Annotated[str, typer.Option("--notional-cap")] = "unlimited",
    max_open_positions: Annotated[
        str, typer.Option("--max-open-positions")
    ] = "unlimited",
    max_daily_loss: Annotated[str, typer.Option("--max-daily-loss")] = "unlimited",
    approver: Annotated[str, typer.Option("--approver")] = "",
    confirmation: Annotated[str, typer.Option("--confirmation")] = "",
    expires_in_minutes: Annotated[str, typer.Option("--expires-in-minutes")] = "never",
) -> None:
    strategy_config_hash = _validate_hex_hash(
        strategy_config_hash,
        "--strategy-config-hash",
        _CONFIG_HASH_LENGTH,
    )
    risk_config_hash = _validate_hex_hash(
        risk_config_hash,
        "--risk-config-hash",
        _CONFIG_HASH_LENGTH,
    )
    git_commit_hash = _validate_hex_hash(
        git_commit_hash,
        "--git-commit-hash",
        _GIT_COMMIT_HASH_LENGTH,
    )
    migration_revision = migration_revision.strip()
    if not migration_revision:
        raise typer.BadParameter("--migration-revision must not be empty")
    configured_strategy_hash = (
        os.environ.get("CML_LIVE_STRATEGY_CONFIG_HASH", "").strip().lower()
    )
    if (
        configured_strategy_hash
        and configured_strategy_hash != "unset"
        and strategy_config_hash != configured_strategy_hash
    ):
        raise typer.BadParameter(
            "--strategy-config-hash does not match the configured Live runtime hash"
        )
    resolved_database_url = _database_url(database_url)
    latest_risk_hash = asyncio.run(
        _latest_risk_config_hash(resolved_database_url, account_label)
    )
    if risk_config_hash != latest_risk_hash:
        raise typer.BadParameter(
            "--risk-config-hash does not match the latest persisted risk config"
        )
    now = datetime.now(tz=UTC)
    approval = LiveOperatorApproval(
        approval_id=f"approval-{uuid4()}",
        account_label=account_label,
        strategy_name=strategy,
        strategy_config_hash=strategy_config_hash,
        risk_config_hash=risk_config_hash,
        git_commit_hash=git_commit_hash,
        database_migration_revision=migration_revision,
        approved_notional_cap=_parse_optional_decimal_limit(
            notional_cap,
            "--notional-cap",
        ),
        approved_max_open_positions=_parse_optional_integer_limit(
            max_open_positions,
            "--max-open-positions",
        ),
        approved_max_daily_loss=_parse_optional_decimal_limit(
            max_daily_loss,
            "--max-daily-loss",
        ),
        approver_name=approver,
        approval_text=confirmation,
        expires_at=_parse_approval_expiration(now, expires_in_minutes),
        created_at=now,
    )
    asyncio.run(_save_approval(resolved_database_url, approval))
    typer.echo(f"Live approval recorded: {approval.approval_id}")


@app.command("approve-runtime")
def approve_runtime_command(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    git_commit_hash: Annotated[str, typer.Option("--git-commit-hash")] = "",
    migration_revision: Annotated[
        str, typer.Option("--migration-revision")
    ] = "",
    notional_cap: Annotated[str, typer.Option("--notional-cap")] = "unlimited",
    max_open_positions: Annotated[
        str, typer.Option("--max-open-positions")
    ] = "unlimited",
    max_daily_loss: Annotated[str, typer.Option("--max-daily-loss")] = "unlimited",
    approver: Annotated[str, typer.Option("--approver")] = "",
    confirmation: Annotated[str, typer.Option("--confirmation")] = "",
    expires_in_minutes: Annotated[str, typer.Option("--expires-in-minutes")] = "never",
) -> None:
    """Record approval values derived from the running account environment."""

    resolved_database_url = _database_url(database_url)
    strategy_config_hash = _validate_hex_hash(
        _runtime_strategy_config_hash(strategy),
        "runtime strategy config hash",
        _CONFIG_HASH_LENGTH,
    )
    configured_strategy_hash = (
        os.environ.get("CML_LIVE_STRATEGY_CONFIG_HASH", "").strip().lower()
    )
    if (
        configured_strategy_hash
        and configured_strategy_hash != "unset"
        and configured_strategy_hash != strategy_config_hash
    ):
        raise typer.BadParameter(
            "configured Live strategy hash does not match the runtime hash"
        )
    risk_config_hash = asyncio.run(
        _latest_risk_config_hash(resolved_database_url, account_label)
    )
    git_commit_hash = _validate_hex_hash(
        git_commit_hash.strip() or os.environ.get("CML_CODE_COMMIT", ""),
        "--git-commit-hash or CML_CODE_COMMIT",
        _GIT_COMMIT_HASH_LENGTH,
    )
    migration_revision = (
        migration_revision.strip()
        or os.environ.get("CML_LIVE_MIGRATION_REVISION", "").strip()
    )
    if not migration_revision:
        raise typer.BadParameter(
            "--migration-revision or CML_LIVE_MIGRATION_REVISION is required"
        )
    now = datetime.now(tz=UTC)
    approval = LiveOperatorApproval(
        approval_id=f"approval-{uuid4()}",
        account_label=account_label,
        strategy_name=strategy,
        strategy_config_hash=strategy_config_hash,
        risk_config_hash=risk_config_hash,
        git_commit_hash=git_commit_hash,
        database_migration_revision=migration_revision,
        approved_notional_cap=_parse_optional_decimal_limit(
            notional_cap,
            "--notional-cap",
        ),
        approved_max_open_positions=_parse_optional_integer_limit(
            max_open_positions,
            "--max-open-positions",
        ),
        approved_max_daily_loss=_parse_optional_decimal_limit(
            max_daily_loss,
            "--max-daily-loss",
        ),
        approver_name=approver,
        approval_text=confirmation,
        expires_at=_parse_approval_expiration(now, expires_in_minutes),
        created_at=now,
    )
    asyncio.run(_save_approval(resolved_database_url, approval))
    typer.echo(
        json.dumps(
            {
                "approval_id": approval.approval_id,
                "account_label": account_label,
                "strategy_config_hash": strategy_config_hash,
                "risk_config_hash": risk_config_hash,
                "git_commit_hash": git_commit_hash,
                "database_migration_revision": migration_revision,
            },
            sort_keys=True,
        )
    )


@app.command("refresh-approval-runtime")
def refresh_approval_runtime_command(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    git_commit_hash: Annotated[str, typer.Option("--git-commit-hash")] = "",
    migration_revision: Annotated[
        str, typer.Option("--migration-revision")
    ] = "",
) -> None:
    """Refresh an active approval while preserving its operator limits."""

    resolved_database_url = _database_url(database_url)
    now = datetime.now(tz=UTC)
    current_approval = asyncio.run(
        _load_active_approval(
            resolved_database_url,
            account_label,
            strategy,
            now,
        )
    )
    if current_approval is None:
        raise typer.BadParameter(
            f"active approval is missing for {account_label}/{strategy}"
        )
    strategy_config_hash = _validate_hex_hash(
        _runtime_strategy_config_hash(strategy),
        "runtime strategy config hash",
        _CONFIG_HASH_LENGTH,
    )
    configured_strategy_hash = (
        os.environ.get("CML_LIVE_STRATEGY_CONFIG_HASH", "").strip().lower()
    )
    if (
        configured_strategy_hash
        and configured_strategy_hash != "unset"
        and configured_strategy_hash != strategy_config_hash
    ):
        raise typer.BadParameter(
            "configured Live strategy hash does not match the runtime hash"
        )
    risk_config_hash = asyncio.run(
        _latest_risk_config_hash(resolved_database_url, account_label)
    )
    git_commit_hash = _validate_hex_hash(
        git_commit_hash.strip() or os.environ.get("CML_CODE_COMMIT", ""),
        "--git-commit-hash or CML_CODE_COMMIT",
        _GIT_COMMIT_HASH_LENGTH,
    )
    migration_revision = (
        migration_revision.strip()
        or os.environ.get("CML_LIVE_MIGRATION_REVISION", "").strip()
    )
    if not migration_revision:
        raise typer.BadParameter(
            "--migration-revision or CML_LIVE_MIGRATION_REVISION is required"
        )
    refreshed_approval = replace(
        current_approval,
        approval_id=f"approval-{uuid4()}",
        strategy_config_hash=strategy_config_hash,
        risk_config_hash=risk_config_hash,
        git_commit_hash=git_commit_hash,
        database_migration_revision=migration_revision,
        created_at=now,
    )
    asyncio.run(_save_approval(resolved_database_url, refreshed_approval))
    typer.echo(
        json.dumps(
            {
                "approval_id": refreshed_approval.approval_id,
                "account_label": account_label,
                "strategy_config_hash": strategy_config_hash,
                "risk_config_hash": risk_config_hash,
                "git_commit_hash": git_commit_hash,
                "database_migration_revision": migration_revision,
                "preserved_notional_cap": (
                    None
                    if refreshed_approval.approved_notional_cap is None
                    else str(refreshed_approval.approved_notional_cap)
                ),
                "preserved_max_open_positions": (
                    refreshed_approval.approved_max_open_positions
                ),
                "preserved_max_daily_loss": (
                    None
                    if refreshed_approval.approved_max_daily_loss is None
                    else str(refreshed_approval.approved_max_daily_loss)
                ),
            },
            sort_keys=True,
        )
    )


def _runtime_manifest_account_for_cli(
    path: Path,
    *,
    account_label: str,
    strategy: str,
) -> LiveRuntimeAccount:
    try:
        manifest = load_live_runtime_manifest(path)
        account = manifest.account(account_label)
    except RuntimeManifestError as error:
        raise typer.BadParameter(str(error)) from error
    if strategy != account.strategy:
        raise typer.BadParameter(
            "--strategy does not match the runtime manifest account"
        )
    return account


def _runtime_manifest_strategy_config_hash(
    account: LiveRuntimeAccount,
) -> str:
    inputs = account.strategy_inputs
    computed = _live_strategy_config_hash(
        account.strategy,
        profile=inputs.profile,
        entry_positive_gainer_top_count=(
            inputs.entry_positive_gainer_top_count
        ),
        require_price_above_ema5=inputs.require_price_above_ema5,
        require_price_above_ema10=inputs.require_price_above_ema10,
        entry_policy_enforce=inputs.entry_policy_enforce,
        entry_order_type=inputs.entry_order_type,
        entry_limit_ttl_seconds=inputs.entry_limit_ttl_seconds,
    )
    if account.strategy_config_hash != "unset":
        configured = _validate_hex_hash(
            account.strategy_config_hash,
            "runtime manifest strategy_config_hash",
            _CONFIG_HASH_LENGTH,
        )
        if configured != computed:
            raise typer.BadParameter(
                "runtime manifest strategy_config_hash does not match "
                "its strategy_config inputs"
            )
    return computed


@app.command("preflight")
def preflight_command(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    runtime_manifest: Annotated[
        Path | None,
        typer.Option(
            "--runtime-manifest",
            help="Validate this account against the desired runtime manifest.",
        ),
    ] = None,
    strict: Annotated[bool, typer.Option("--strict")] = False,
    expected_git_commit: Annotated[
        str | None, typer.Option("--expected-git-commit")
    ] = None,
    expected_migration_revision: Annotated[
        str | None, typer.Option("--expected-migration-revision")
    ] = None,
) -> None:
    manifest_account = None
    if runtime_manifest is not None:
        manifest_account = _runtime_manifest_account_for_cli(
            runtime_manifest,
            account_label=account_label,
            strategy=strategy,
        )
        manifest_git_commit = _validate_hex_hash(
            manifest_account.image_commit,
            "runtime manifest image_commit",
            _GIT_COMMIT_HASH_LENGTH,
        )
        if expected_git_commit is None:
            expected_git_commit = manifest_git_commit
        elif expected_git_commit.lower() != manifest_git_commit:
            raise typer.BadParameter(
                "--expected-git-commit does not match the runtime manifest"
            )
        if expected_migration_revision is None:
            expected_migration_revision = manifest_account.migration_revision
        elif expected_migration_revision != manifest_account.migration_revision:
            raise typer.BadParameter(
                "--expected-migration-revision does not match the runtime manifest"
            )
    if expected_git_commit is not None:
        expected_git_commit = _validate_hex_hash(
            expected_git_commit,
            "--expected-git-commit",
            _GIT_COMMIT_HASH_LENGTH,
        )
    if expected_migration_revision is not None:
        expected_migration_revision = expected_migration_revision.strip()
        if not expected_migration_revision:
            raise typer.BadParameter(
                "--expected-migration-revision must not be empty"
            )
    payload = asyncio.run(
        _preflight_summary(
            _database_url(database_url),
            account_label,
            strategy,
            expected_git_commit=expected_git_commit,
            expected_migration_revision=expected_migration_revision,
            expected_lease_owner=(
                None if manifest_account is None else manifest_account.lease_owner
            ),
            expected_strategy_config_hash=(
                None
                if manifest_account is None
                else _runtime_manifest_strategy_config_hash(manifest_account)
            ),
        )
    )
    typer.echo(json.dumps(payload, sort_keys=True))
    if strict and payload["preflight_ok"] is not True:
        raise typer.Exit(code=1)


@app.command("resolve-missing-order")
def resolve_missing_order_command(
    client_order_id: Annotated[str, typer.Option("--client-order-id")],
    operator: Annotated[str, typer.Option("--operator")],
    confirmation: Annotated[str, typer.Option("--confirmation")] = "",
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    min_missing_age_seconds: Annotated[
        int,
        typer.Option("--min-missing-age-seconds", min=60),
    ] = 600,
    base_url: Annotated[str, typer.Option("--base-url")] = "https://fapi.binance.com",
    api_key_env: Annotated[str, typer.Option("--api-key-env")] = "BINANCE_API_KEY",
    api_secret_env: Annotated[
        str, typer.Option("--api-secret-env")
    ] = "BINANCE_API_SECRET",
) -> None:
    """Resolve one old reduce-only order after proving it is absent on Binance."""
    if confirmation != _RESOLVE_MISSING_ORDER_CONFIRMATION:
        raise typer.BadParameter(
            f"--confirmation must equal '{_RESOLVE_MISSING_ORDER_CONFIRMATION}'"
        )
    if not client_order_id.strip():
        raise typer.BadParameter("--client-order-id must not be empty")
    if not operator.strip():
        raise typer.BadParameter("--operator must not be empty")
    api_key = os.environ.get(api_key_env)
    api_secret = os.environ.get(api_secret_env)
    if not api_key or not api_secret:
        raise typer.BadParameter(f"{api_key_env} and {api_secret_env} are required")
    payload = asyncio.run(
        _resolve_missing_live_order(
            database_url=_database_url(database_url),
            account_label=account_label,
            client_order_id=client_order_id,
            operator=operator,
            min_missing_age_seconds=min_missing_age_seconds,
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
        )
    )
    typer.echo(json.dumps(payload, sort_keys=True, default=str))


@app.command("submit-plan")
def submit_plan_command(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "compression_breakout",
    session_id: Annotated[str, typer.Option("--session-id")] = "live-manual",
    operator: Annotated[str, typer.Option("--operator")] = "",
    lease_owner: Annotated[str, typer.Option("--lease-owner")] = "live-worker",
    strategy_config_hash: Annotated[str, typer.Option("--strategy-config-hash")] = "",
    git_commit_hash: Annotated[str, typer.Option("--git-commit-hash")] = "",
    migration_revision: Annotated[str, typer.Option("--migration-revision")] = "",
    order_plan_json: Annotated[
        Path | None, typer.Option("--order-plan-json", exists=True, dir_okay=False)
    ] = None,
    account_event_hub_url: Annotated[
        str,
        typer.Option("--account-event-hub-url"),
    ] = "ws://execution-account-live:8767",
    base_url: Annotated[str, typer.Option("--base-url")] = "https://fapi.binance.com",
    api_key_env: Annotated[str, typer.Option("--api-key-env")] = "BINANCE_API_KEY",
    api_secret_env: Annotated[
        str, typer.Option("--api-secret-env")
    ] = "BINANCE_API_SECRET",
    entry_leverage: Annotated[
        int, typer.Option("--entry-leverage", min=1, max=125)
    ] = 1,
    margin_type: Annotated[
        str,
        typer.Option(
            "--margin-type",
            help="Entry margin mode: CROSSED or ISOLATED.",
        ),
    ] = "CROSSED",
    confirmation: Annotated[
        bool, typer.Option("--i-understand-this-places-real-orders")
    ] = False,
) -> None:
    if not confirmation:
        raise typer.BadParameter("--i-understand-this-places-real-orders is required")
    if order_plan_json is None:
        raise typer.BadParameter("--order-plan-json is required")
    api_key = os.environ.get(api_key_env)
    api_secret = os.environ.get(api_secret_env)
    if not api_key or not api_secret:
        raise typer.BadParameter(f"{api_key_env} and {api_secret_env} are required")
    plan = _load_plan(order_plan_json)
    result = asyncio.run(
        _run_live_plan(
            database_url=_database_url(database_url),
            account_label=account_label,
            strategy_name=strategy,
            session_id=session_id,
            operator=operator,
            lease_owner=lease_owner,
            strategy_config_hash=strategy_config_hash,
            git_commit_hash=git_commit_hash,
            migration_revision=migration_revision,
            plan=plan,
            account_event_hub_url=account_event_hub_url,
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
            entry_leverage=entry_leverage,
            margin_type=margin_type,
        )
    )
    typer.echo(json.dumps(asdict(result), default=str, sort_keys=True))


@app.command("run")
def run_command(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    runtime_manifest: Annotated[
        Path | None,
        typer.Option(
            "--runtime-manifest",
            help="Use the desired runtime manifest for this worker.",
        ),
    ] = None,
    impulse_window_buckets: Annotated[
        int | None,
        typer.Option("--impulse-window-buckets", min=2),
    ] = None,
    confirmation_buckets: Annotated[
        int | None,
        typer.Option("--confirmation-buckets", min=1),
    ] = None,
    min_return_pct: Annotated[
        str | None,
        typer.Option("--min-return-pct"),
    ] = None,
    min_imbalance: Annotated[
        str | None,
        typer.Option("--min-imbalance"),
    ] = None,
    min_intensity: Annotated[
        str | None,
        typer.Option("--min-intensity"),
    ] = None,
    min_notional_5m_vs_30m: Annotated[
        str | None,
        typer.Option("--min-notional-5m-vs-30m"),
    ] = None,
    cooldown_buckets: Annotated[
        int | None,
        typer.Option("--cooldown-buckets", min=0),
    ] = None,
    market_environment: Annotated[
        str,
        typer.Option("--market-environment"),
    ] = "research",
    market_state_source: Annotated[
        str,
        typer.Option(
            "--market-state-source",
            help="Realtime source: hub (default) or postgres (explicit recovery mode).",
        ),
    ] = "hub",
    market_state_hub_url: Annotated[
        str,
        typer.Option("--market-state-hub-url"),
    ] = "ws://market-data:8766",
    market_quote_hub_url: Annotated[
        str,
        typer.Option("--market-quote-hub-url"),
    ] = "ws://market-data:8768",
    market_websocket_url: Annotated[
        str,
        typer.Option(
            "--market-websocket-url",
            help="Direct Binance market WebSocket used by closed-candle exits.",
        ),
    ] = _LIVE_MARKET_WEBSOCKET_URL,
    account_event_hub_url: Annotated[
        str,
        typer.Option("--account-event-hub-url"),
    ] = "ws://execution-account-live:8767",
    risk_control_hub_url: Annotated[
        str,
        typer.Option(
            "--risk-control-hub-url",
            help=(
                "Low-volume operator control stream. PostgreSQL remains the "
                "durable authority."
            ),
        ),
    ] = "ws://execution-account-live:8769",
    session_id: Annotated[str | None, typer.Option("--session-id")] = None,
    operator: Annotated[str, typer.Option("--operator")] = "",
    lease_owner: Annotated[str | None, typer.Option("--lease-owner")] = None,
    strategy_config_hash: Annotated[str, typer.Option("--strategy-config-hash")] = "",
    git_commit_hash: Annotated[str, typer.Option("--git-commit-hash")] = "",
    migration_revision: Annotated[str, typer.Option("--migration-revision")] = "",
    max_runtime_seconds: Annotated[
        int, typer.Option("--max-runtime-seconds", min=1)
    ] = 3600,
    poll_interval_seconds: Annotated[
        float, typer.Option("--poll-interval-seconds", min=0.1)
    ] = 0.25,
    checkpoint_every_states: Annotated[
        int, typer.Option("--checkpoint-every-states", min=1)
    ] = 100,
    hedge_mode: Annotated[
        bool,
        typer.Option("--hedge-mode/--one-way-mode"),
    ] = True,
    exit_mode: Annotated[
        PositionExitMode,
        typer.Option("--exit-mode"),
    ] = PositionExitMode.CANDLE_15M,
    take_profit_pct: Annotated[
        str,
        typer.Option("--take-profit-pct"),
    ] = "0.02",
    stop_loss_pct: Annotated[
        str,
        typer.Option("--stop-loss-pct"),
    ] = "0.01",
    entry_long_only: Annotated[
        bool,
        typer.Option("--entry-long-only/--entry-all-sides"),
    ] = True,
    entry_positive_gainer_top_count: Annotated[
        int | None,
        typer.Option("--entry-positive-gainer-top-count", min=1),
    ] = None,
    entry_price_above_ema5: Annotated[
        bool,
        typer.Option("--entry-price-above-ema5/--no-entry-price-above-ema5"),
    ] = _LIVE_ENTRY_PRICE_ABOVE_EMA5,
    entry_price_above_ema10: Annotated[
        bool,
        typer.Option("--entry-price-above-ema10/--no-entry-price-above-ema10"),
    ] = _LIVE_ENTRY_PRICE_ABOVE_EMA10,
    entry_order_type: Annotated[
        EntryType,
        typer.Option("--entry-order-type"),
    ] = _LIVE_ENTRY_ORDER_TYPE,
    entry_limit_ttl_seconds: Annotated[
        int,
        typer.Option("--entry-limit-ttl-seconds", min=601),
    ] = _LIVE_ENTRY_LIMIT_TTL_SECONDS,
    candle_grace_bars: Annotated[
        int,
        typer.Option("--candle-grace-bars", min=0),
    ] = 1,
    candle_grace_decision_profit_pct: Annotated[
        str,
        typer.Option("--candle-grace-decision-profit-pct"),
    ] = "0.001",
    candle_grace_profit_pct: Annotated[
        str,
        typer.Option("--candle-grace-profit-pct"),
    ] = "0.0088",
    base_url: Annotated[str, typer.Option("--base-url")] = "https://fapi.binance.com",
    api_key_env: Annotated[
        str | None,
        typer.Option(
            "--api-key-env",
            help="Override the trade credential key environment variable.",
        ),
    ] = None,
    api_secret_env: Annotated[
        str | None,
        typer.Option(
            "--api-secret-env",
            help="Override the trade credential secret environment variable.",
        ),
    ] = None,
    allow_legacy_credential_fallback: Annotated[
        bool,
        typer.Option(
            "--allow-legacy-credential-fallback/--no-allow-legacy-credential-fallback",
            help=(
                "Temporarily fall back to BINANCE_API_KEY/SECRET during migration."
            ),
        ),
    ] = False,
    entry_leverage: Annotated[
        int, typer.Option("--entry-leverage", min=1, max=125)
    ] = 1,
    margin_type: Annotated[
        str,
        typer.Option(
            "--margin-type",
            help="Entry margin mode: CROSSED or ISOLATED.",
        ),
    ] = "CROSSED",
    entry_policy_compare_only: Annotated[
        bool,
        typer.Option(
            "--entry-policy-compare-only/--no-entry-policy-compare-only",
            help=(
                "Record legacy-vs-Policy entry differences without changing "
                "order decisions."
            ),
        ),
    ] = False,
    entry_policy_enforce: Annotated[
        bool,
        typer.Option(
            "--entry-policy-enforce/--no-entry-policy-enforce",
            help="Use the shared Policy for real entry eligibility decisions.",
        ),
    ] = False,
    acknowledge_missing_shadow_preflight: Annotated[
        bool,
        typer.Option(
            "--acknowledge-missing-shadow-preflight",
            help=(
                "Acknowledge the advisory when no matching completed Shadow "
                "session exists."
            ),
        ),
    ] = False,
    persist_exchange_operations: Annotated[
        str,
        typer.Option(
            "--persist-exchange-operations",
            help=(
                "Comma-separated exchange operations to persist (default: "
                "submit,cancel); use 'all' for a temporary full-operation "
                "diagnostic capture."
            ),
        ),
    ] = "submit,cancel",
    confirmation: Annotated[
        bool, typer.Option("--i-understand-this-places-real-orders")
    ] = False,
) -> None:
    if entry_policy_compare_only and entry_policy_enforce:
        raise typer.BadParameter(
            "--entry-policy-compare-only and "
            "--entry-policy-enforce are mutually exclusive"
        )
    if not confirmation:
        raise typer.BadParameter("--i-understand-this-places-real-orders is required")
    manifest_account = (
        None
        if runtime_manifest is None
        else _runtime_manifest_account_for_cli(
            runtime_manifest,
            account_label=account_label,
            strategy=strategy,
        )
    )
    if manifest_account is None:
        session_id = session_id or "live-manual"
        lease_owner = lease_owner or "live-worker"
        profile = _resolve_live_profile_options(
            impulse_window_buckets=impulse_window_buckets,
            confirmation_buckets=confirmation_buckets,
            min_return_pct=min_return_pct,
            min_imbalance=min_imbalance,
            min_intensity=min_intensity,
            min_notional_5m_vs_30m=min_notional_5m_vs_30m,
            cooldown_buckets=cooldown_buckets,
        )
        entry_positive_gainer_top_count = (
            _resolve_live_entry_positive_gainer_top_count(
                entry_positive_gainer_top_count
            )
        )
    else:
        if session_id is not None and session_id != manifest_account.session_id:
            raise typer.BadParameter(
                "session id does not match the runtime manifest"
            )
        if lease_owner is not None and lease_owner != manifest_account.lease_owner:
            raise typer.BadParameter(
                "lease owner does not match the runtime manifest"
            )
        session_id = manifest_account.session_id
        lease_owner = manifest_account.lease_owner
        strategy_inputs = manifest_account.strategy_inputs
        profile = strategy_inputs.profile
        entry_positive_gainer_top_count = (
            strategy_inputs.entry_positive_gainer_top_count
        )
        entry_price_above_ema5 = strategy_inputs.require_price_above_ema5
        entry_price_above_ema10 = strategy_inputs.require_price_above_ema10
        entry_policy_compare_only = (
            strategy_inputs.entry_policy_mode == "compare_only"
        )
        entry_policy_enforce = strategy_inputs.entry_policy_enforce
        entry_order_type = strategy_inputs.entry_order_type
        entry_limit_ttl_seconds = strategy_inputs.entry_limit_ttl_seconds

        configured_git_commit = git_commit_hash.strip() or os.environ.get(
            "CML_CODE_COMMIT",
            "",
        ).strip()
        manifest_git_commit = _validate_hex_hash(
            manifest_account.image_commit,
            "runtime manifest image_commit",
            _GIT_COMMIT_HASH_LENGTH,
        )
        if (
            configured_git_commit
            and configured_git_commit.lower() != manifest_git_commit
        ):
            raise typer.BadParameter(
                "git commit does not match the runtime manifest"
            )
        git_commit_hash = manifest_git_commit

        configured_migration_revision = migration_revision.strip() or os.environ.get(
            "CML_LIVE_MIGRATION_REVISION",
            "",
        ).strip()
        if (
            configured_migration_revision
            and configured_migration_revision != manifest_account.migration_revision
        ):
            raise typer.BadParameter(
                "migration revision does not match the runtime manifest"
            )
        migration_revision = manifest_account.migration_revision

        manifest_strategy_hash = _runtime_manifest_strategy_config_hash(
            manifest_account
        )
        configured_strategy_hash = strategy_config_hash.strip().lower()
        if configured_strategy_hash not in {"", "unset"}:
            configured_strategy_hash = _validate_hex_hash(
                configured_strategy_hash,
                "--strategy-config-hash",
                _CONFIG_HASH_LENGTH,
            )
            if configured_strategy_hash != manifest_strategy_hash:
                raise typer.BadParameter(
                    "strategy config hash does not match the runtime manifest"
                )
        strategy_config_hash = manifest_strategy_hash
    credentials = _resolve_live_cli_credentials(
        api_key_env=api_key_env,
        api_secret_env=api_secret_env,
        allow_legacy_fallback=allow_legacy_credential_fallback,
    )

    async def run_once() -> LiveDaemonResult:
        return await _run_live_daemon(
            execution_database_url=_execution_database_url(database_url),
            market_database_url=_market_database_url(database_url),
            observability_database_url=_observability_database_url(database_url),
            account_label=account_label,
            strategy_name=strategy,
            market_environment=market_environment,
            market_state_source=market_state_source,
            market_state_hub_url=market_state_hub_url,
            market_quote_hub_url=market_quote_hub_url,
            market_websocket_url=market_websocket_url,
            account_event_hub_url=account_event_hub_url,
            risk_control_hub_url=risk_control_hub_url,
            session_id=session_id,
            operator=operator,
            lease_owner=lease_owner,
            strategy_config_hash=strategy_config_hash,
            git_commit_hash=git_commit_hash,
            migration_revision=migration_revision,
            profile=profile,
            max_runtime_seconds=max_runtime_seconds,
            poll_interval_seconds=poll_interval_seconds,
            checkpoint_every_states=checkpoint_every_states,
            hedge_mode=hedge_mode,
            exit_mode=exit_mode,
            take_profit_pct=Decimal(take_profit_pct),
            stop_loss_pct=Decimal(stop_loss_pct),
            entry_long_only=entry_long_only,
            entry_positive_gainer_top_count=entry_positive_gainer_top_count,
            require_price_above_ema5=entry_price_above_ema5,
            require_price_above_ema10=entry_price_above_ema10,
            entry_order_type=entry_order_type,
            entry_limit_ttl_seconds=entry_limit_ttl_seconds,
            candle_grace_bars=candle_grace_bars,
            candle_grace_decision_profit_pct=Decimal(
                candle_grace_decision_profit_pct
            ),
            candle_grace_profit_pct=Decimal(candle_grace_profit_pct),
            base_url=base_url,
            api_key=credentials.api_key,
            api_secret=credentials.api_secret,
            entry_leverage=entry_leverage,
            margin_type=margin_type,
            entry_policy_compare_only=entry_policy_compare_only,
            entry_policy_enforce=entry_policy_enforce,
            acknowledge_missing_shadow_preflight=acknowledge_missing_shadow_preflight,
            persist_exchange_operations=_parse_exchange_operations(
                persist_exchange_operations
            ),
        )

    result = asyncio.run(_run_with_live_startup_backoff(run_once))
    typer.echo(json.dumps(asdict(result), default=str, sort_keys=True))


@app.command("status")
def status_command(
    session_id: Annotated[str, typer.Option("--session-id")],
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
) -> None:
    transition = asyncio.run(_load_transition(_database_url(database_url), session_id))
    typer.echo(
        json.dumps(
            None if transition is None else asdict(transition),
            default=str,
        )
    )


@app.command("disable-new-entries")
def disable_new_entries_command(
    session_id: Annotated[str, typer.Option("--session-id")],
    operator: Annotated[str, typer.Option("--operator")],
    strategy_config_hash: Annotated[str, typer.Option("--strategy-config-hash")],
    risk_config_hash: Annotated[str, typer.Option("--risk-config-hash")],
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    risk_control_hub_url: Annotated[
        str,
        typer.Option("--risk-control-hub-url"),
    ] = "",
    risk_control_hub_token: Annotated[
        str | None,
        typer.Option(
            "--risk-control-hub-token",
            help="Optional token; CML_RISK_CONTROL_HUB_TOKEN is used when omitted.",
        ),
    ] = None,
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
) -> None:
    resolved_risk_control_hub_url = (
        risk_control_hub_url.strip()
        or os.environ.get("CML_RISK_CONTROL_HUB_URL", "").strip()
    )
    transition = asyncio.run(
        _save_transition(
            _database_url(database_url),
            session_id,
            operator,
            strategy_config_hash,
            risk_config_hash,
            LiveSessionState.DRAINING,
            "operator_disabled_new_entries",
        )
    )
    push_error: Exception | None = None
    if resolved_risk_control_hub_url:
        event = RiskControlEvent(
            environment="live",
            account_label=account_label,
            strategy_name=strategy,
            session_id=session_id,
            action=RiskControlAction.DRAIN,
            event_id=transition.transition_id,
            command_id=transition.transition_id,
            reason="operator_disabled_new_entries",
            issued_at=transition.occurred_at,
            details={"transition_id": transition.transition_id},
        )
        try:
            asyncio.run(
                _publish_risk_control_event(
                    url=resolved_risk_control_hub_url,
                    token=risk_control_hub_token,
                    event=event,
                )
            )
        except Exception as error:
            push_error = error
            log.warning(
                "risk_control_push_failed_db_fallback_active",
                error_type=type(error).__name__,
            )
    if push_error is None:
        typer.echo("Live session is draining")
    else:
        typer.echo(
            "Live session is draining "
            "(risk-control push unavailable; PostgreSQL fallback remains active)"
        )


@app.command("cancel-all-open-entries")
def cancel_all_open_entries_command(
    session_id: Annotated[str, typer.Option("--session-id")],
    operator: Annotated[str, typer.Option("--operator")],
    idempotency_key: Annotated[str, typer.Option("--idempotency-key")],
    confirmation: Annotated[str, typer.Option("--confirmation")] = "",
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    risk_control_hub_url: Annotated[
        str,
        typer.Option("--risk-control-hub-url"),
    ] = "",
    risk_control_hub_token: Annotated[
        str | None,
        typer.Option(
            "--risk-control-hub-token",
            help="Optional token; CML_RISK_CONTROL_HUB_TOKEN is used when omitted.",
        ),
    ] = None,
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
) -> None:
    """Durably request cancellation of all live opening orders."""

    _issue_one_shot_risk_control_command(
        action=RiskControlAction.CANCEL_ALL_OPEN_ENTRIES,
        command_type=CANCEL_ALL_OPEN_ENTRIES_COMMAND,
        confirmation_text=CANCEL_ALL_OPEN_ENTRIES_CONFIRMATION,
        reason="operator_cancelled_all_open_entries",
        session_id=session_id,
        operator=operator,
        idempotency_key=idempotency_key,
        confirmation=confirmation,
        account_label=account_label,
        strategy=strategy,
        risk_control_hub_url=risk_control_hub_url,
        risk_control_hub_token=risk_control_hub_token,
        database_url=database_url,
    )


@app.command("request-flatten")
def request_flatten_command(
    session_id: Annotated[str, typer.Option("--session-id")],
    operator: Annotated[str, typer.Option("--operator")],
    idempotency_key: Annotated[str, typer.Option("--idempotency-key")],
    confirmation: Annotated[str, typer.Option("--confirmation")] = "",
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    risk_control_hub_url: Annotated[
        str,
        typer.Option("--risk-control-hub-url"),
    ] = "",
    risk_control_hub_token: Annotated[
        str | None,
        typer.Option(
            "--risk-control-hub-token",
            help="Optional token; CML_RISK_CONTROL_HUB_TOKEN is used when omitted.",
        ),
    ] = None,
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
) -> None:
    """Durably request a reduce-only flatten through the live exit lane."""

    _issue_one_shot_risk_control_command(
        action=RiskControlAction.REQUEST_FLATTEN,
        command_type=EMERGENCY_FLATTEN_COMMAND,
        confirmation_text=EMERGENCY_FLATTEN_CONFIRMATION,
        reason="operator_requested_flatten",
        session_id=session_id,
        operator=operator,
        idempotency_key=idempotency_key,
        confirmation=confirmation,
        account_label=account_label,
        strategy=strategy,
        risk_control_hub_url=risk_control_hub_url,
        risk_control_hub_token=risk_control_hub_token,
        database_url=database_url,
    )


@app.command("report")
def report_command(
    session_id: Annotated[str, typer.Option("--session-id")],
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
) -> None:
    transition = asyncio.run(_load_transition(_database_url(database_url), session_id))
    typer.echo(
        json.dumps(
            None if transition is None else asdict(transition),
            default=str,
        )
    )


async def _run_live_plan(
    *,
    database_url: str,
    account_label: str,
    strategy_name: str,
    session_id: str,
    operator: str,
    lease_owner: str,
    strategy_config_hash: str,
    git_commit_hash: str,
    migration_revision: str,
    plan: OrderExecutionPlan,
    account_event_hub_url: str,
    base_url: str,
    api_key: str,
    api_secret: str,
    entry_leverage: int,
    margin_type: str = "CROSSED",
) -> LiveSessionResult:
    if plan.run_id != session_id:
        raise ValueError("order plan run_id must match session_id")
    now = datetime.now(tz=UTC)
    engine = create_execution_database_engine(database_url)
    client: BinanceUsdMTradeClient | None = None
    execution_coordinator: OrderExecutionCoordinator | None = None
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        live_repository = PostgresLiveRolloutRepository(factory)
        risk_repository = PostgresRiskRepository(factory)
        order_repository = PostgresOrderRepository(factory)
        risk_config = await _latest_risk_config(factory, account_label)
        approval = await live_repository.load_active_approval(
            account_label=account_label,
            strategy_name=strategy_name,
            now=now,
        )
        unresolved = await order_repository.load_unresolved_orders(session_id)
        context = LiveGateContext(
            now=now,
            live_submit_enabled=True,
            account_label=account_label,
            strategy_name=strategy_name,
            strategy_config_hash=strategy_config_hash,
            git_commit_hash=git_commit_hash,
            database_migration_revision=migration_revision,
            required_lease_owner=lease_owner,
            requested_submit_policy=SubmitPolicy.LIVE_SUBMIT,
            active_lease=await risk_repository.load_active_lease(
                "live", account_label, now
            ),
            risk_config=risk_config,
            approval=approval,
            account_state=await _latest_account_state(factory, account_label),
            active_halts=await risk_repository.load_active_halts("live", account_label),
            unresolved_order_states=tuple(item.state for item in unresolved),
        )
        gate = evaluate_live_gate(context)
        if not gate.approved:
            raise RuntimeError(f"live gate blocked: {','.join(gate.reasons)}")
        desired_notional = await _approved_intent_notional(factory, plan.intent_id)
        if desired_notional is None:
            raise RuntimeError("live plan has no persisted approved intent notional")
        if (
            risk_config.max_order_notional is not None
            and desired_notional > risk_config.max_order_notional
        ):
            raise RuntimeError("live plan exceeds current risk notional cap")
        if approval is None or (
            approval.approved_notional_cap is not None
            and desired_notional > approval.approved_notional_cap
        ):
            raise RuntimeError("live plan exceeds operator-approved notional cap")
        client = BinanceUsdMTradeClient(
            api_key=api_key,
            api_secret=api_secret,
            environment="live",
            account_label=account_label,
            live_submit_enabled=True,
            base_url=base_url,
            entry_leverage=entry_leverage,
            margin_type=margin_type,
        )
        account_config = await client.fetch_account_config()
        plan_uses_hedge_mode = plan.position_side is not FuturesPositionSide.BOTH
        if account_config.hedge_mode != plan_uses_hedge_mode:
            raise RuntimeError("order plan position mode does not match Binance")

        register_expected_entry = LiveEntryExpectationRegistrar(
            account_event_hub_url=account_event_hub_url,
            account_label=account_label,
        )

        submission_fence = LiveSubmissionFence(
            risk_state=risk_repository,
            environment="live",
            account_label=account_label,
            strategy_name=strategy_name,
            lease_owner=lease_owner,
            code_generation=git_commit_hash,
            active_lease=lambda: context.active_lease,
            is_draining=lambda: _session_is_draining(factory, session_id),
        )

        machine = OrderExecutionStateMachine(
            exchange=client,
            repository=order_repository,
            submit_policy=SubmitPolicy.LIVE_SUBMIT,
            live_submit_enabled=True,
            clock=lambda: datetime.now(tz=UTC),
            on_before_submit=register_expected_entry,
            on_before_exchange_submit=submission_fence.validate,
            serialize_commands=False,
        )
        execution_coordinator = OrderExecutionCoordinator(
            backend=machine,
            account_label=account_label,
        )
        session = LiveRolloutSession(
            repository=live_repository,
            execute_plan=execution_coordinator.execute_approved_intent,
            config=LiveSessionConfig(
                session_id=session_id,
                operator=operator,
                strategy_config_hash=strategy_config_hash,
                risk_config_hash=risk_config.config_hash,
            ),
            clock=lambda: datetime.now(tz=UTC),
        )

        async def shadow_preflight() -> bool:
            await _warn_if_shadow_preflight_missing(
                factory,
                strategy_name=strategy_name,
                strategy_config_hash=strategy_config_hash,
                account_label=account_label,
                session_id=session_id,
            )
            return True

        return await session.run_one(
            gate_context=context,
            shadow_preflight=shadow_preflight,
            plan=plan,
        )
    finally:
        if execution_coordinator is not None:
            await execution_coordinator.aclose()
        if client is not None:
            await client.aclose()
        await engine.dispose()


def _validate_missing_order_resolution(
    *,
    state: str,
    reduce_only: bool,
    exchange_order_id: str | None,
    created_at: datetime,
    now: datetime,
    order_quantity: Decimal,
    executed_quantity: Decimal,
    position_quantity: Decimal,
    exchange_order_found: bool,
    matching_open_order_found: bool,
    min_missing_age_seconds: float,
) -> None:
    """Compatibility entry point for the operator safety guard."""
    _validate_missing_order_resolution_impl(
        state=state,
        reduce_only=reduce_only,
        exchange_order_id=exchange_order_id,
        created_at=created_at,
        now=now,
        order_quantity=order_quantity,
        executed_quantity=executed_quantity,
        position_quantity=position_quantity,
        exchange_order_found=exchange_order_found,
        matching_open_order_found=matching_open_order_found,
        min_missing_age_seconds=min_missing_age_seconds,
    )


async def _run_live_daemon(
    *,
    execution_database_url: str,
    market_database_url: str,
    observability_database_url: str,
    account_label: str,
    strategy_name: str,
    market_environment: str,
    market_state_source: str,
    market_state_hub_url: str,
    market_quote_hub_url: str,
    account_event_hub_url: str,
    session_id: str,
    operator: str,
    lease_owner: str,
    strategy_config_hash: str,
    git_commit_hash: str,
    migration_revision: str,
    profile: LiveOrderFlowImpulseProfile,
    max_runtime_seconds: int,
    poll_interval_seconds: float,
    checkpoint_every_states: int,
    hedge_mode: bool,
    exit_mode: PositionExitMode,
    take_profit_pct: Decimal,
    stop_loss_pct: Decimal,
    entry_long_only: bool,
    entry_positive_gainer_top_count: int | None,
    require_price_above_ema5: bool,
    require_price_above_ema10: bool,
    entry_order_type: EntryType,
    entry_limit_ttl_seconds: int,
    candle_grace_bars: int,
    candle_grace_decision_profit_pct: Decimal,
    candle_grace_profit_pct: Decimal,
    base_url: str,
    api_key: str,
    api_secret: str,
    entry_leverage: int,
    margin_type: str = "CROSSED",
    persist_exchange_operations: Collection[str] | None = (
        _DEFAULT_PERSIST_EXCHANGE_OPERATIONS
    ),
    entry_policy_compare_only: bool = False,
    entry_policy_enforce: bool = False,
    acknowledge_missing_shadow_preflight: bool = False,
    market_websocket_url: str = _LIVE_MARKET_WEBSOCKET_URL,
    risk_control_hub_url: str | None = None,
) -> LiveDaemonResult:
    risk_control_enabled = bool(
        risk_control_hub_url is not None and risk_control_hub_url.strip()
    )
    if market_state_source not in {"hub", "postgres"}:
        raise ValueError("market_state_source must be 'hub' or 'postgres'")
    if market_state_source == "hub" and not market_state_hub_url.strip():
        raise ValueError("market_state_hub_url must not be empty in hub mode")
    if market_state_source == "hub" and not market_quote_hub_url.strip():
        raise ValueError("market_quote_hub_url must not be empty in hub mode")
    if not account_event_hub_url.strip():
        raise ValueError("account_event_hub_url must not be empty")
    health = LocalHealthWriter.from_environment()

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
        except Exception:
            log.exception("live_health_marker_failed")

    now = datetime.now(tz=UTC)
    execution_engine = create_execution_database_engine(execution_database_url)
    market_engine = create_market_database_engine(market_database_url)
    observability_engine = create_observability_database_engine(
        observability_database_url
    )
    checkpoint_engine = create_checkpoint_database_engine(
        observability_database_url
    )
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
    volume_rest_client: BinanceUsdMRestClient | None = None
    volume_cache: Binance24hQuoteVolumeCache | None = None
    signal_recorder: LiveStrategySignalRecorder | None = None
    daemon: LiveStrategyDaemon | None = None
    hub_source: WebSocketMarketStateSource | None = None
    startup_market_buffer: StartupMarketStateBuffer | None = None
    startup_market_state_task: asyncio.Task[None] | None = None
    risk_control_source: WebSocketRiskControlSource | None = None
    risk_control_task: asyncio.Task[None] | None = None
    risk_control_runtime: LiveRiskControlRuntime | None = None
    control_plane_runtime: LiveControlPlaneRuntime | None = None
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
        telemetry_repository = PostgresRuntimeTelemetryRepository(
            observability_factory
        )
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
        volume_rest_client = BinanceUsdMRestClient(base_url)
        volume_cache = Binance24hQuoteVolumeCache(volume_rest_client)
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
        client = BinanceUsdMTradeClient(
            api_key=api_key,
            api_secret=api_secret,
            environment="live",
            account_label=account_label,
            live_submit_enabled=True,
            base_url=base_url,
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
            entry_enabled=lambda: (
                daemon is not None and daemon.entry_enabled
            ),
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
            await _record_transition(
                live_repository,
                session_id=session_id,
                operator=operator,
                strategy_config_hash=strategy_config_hash,
                risk_config_hash=risk_config_hash,
                state=LiveSessionState.PREFLIGHT,
            )
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
            await _record_transition(
                live_repository,
                session_id=session_id,
                operator=operator,
                strategy_config_hash=strategy_config_hash,
                risk_config_hash=risk_config_hash,
                state=LiveSessionState.SHADOW_PREFLIGHT,
            )
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
        checkpoint = await checkpoint_repository.load_checkpoint(session_id)
        if checkpoint is not None:
            strategy.restore_checkpoint(checkpoint)
        state_repository = PostgresRuntimeMarketStateRepository(market_factory)
        startup_cutover = _live_market_state_cutover(
            datetime.now(tz=UTC)
        )
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
                )
            market_cursor = _cursor_after_market_bucket(startup_cutover)
        elif market_state_source == "postgres":
            market_cursor = await _warm_live_strategy_then_start_fresh(
                strategy=strategy,
                repository=state_repository,
                environment=market_environment,
                now=now,
                cutover_at=startup_cutover,
            )
        else:
            await _warm_live_strategy(
                strategy=strategy,
                repository=state_repository,
                environment=market_environment,
                now=now,
                cutover_at=startup_cutover,
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
            ema_candle_source = BinanceRestClosedCandle15mSource(base_url)
            ema_provider = ClosedCandleEmaProvider(ema_candle_source)

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
                entry_universe_context_provider=(
                    entry_universe_context_provider
                ),
                entry_universe_snapshot_provider=(
                    entry_universe_snapshot_provider
                ),
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
                    candle_grace_decision_profit_pct=(
                        candle_grace_decision_profit_pct
                    ),
                    candle_grace_profit_pct=candle_grace_profit_pct,
                ),
                candle_loader=None,
            ),
            exit_recovery_client=client,
            cancel_unfilled_entry_orders=entry_order_canceller.cancel,
            fetch_exchange_positions=client.fetch_positions,
            on_managed_position_symbols=(
                None
                if closed_candle_feed is None
                else closed_candle_feed.set_symbols
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
                strategy_warmup_reason=(
                    control_plane_runtime.strategy_warmup_reason
                ),
                market_state_available=control_plane_runtime.market_state_available,
                market_state_unavailable_reason=(
                    control_plane_runtime.market_state_unavailable_reason
                ),
                account_snapshot_available=(
                    control_plane_runtime.account_snapshot_available
                ),
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
            await _record_transition(
                live_repository,
                session_id=session_id,
                operator=operator,
                strategy_config_hash=strategy_config_hash,
                risk_config_hash=risk_config_hash,
                state=LiveSessionState.LIVE_ENABLED,
            )
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
        market_task = asyncio.create_task(
            daemon.run(_observe_market_states(state_stream, latest_market_states))
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
        reconcile_task = asyncio.create_task(
            order_reconciliation.run_periodically()
        )
        local_health_task: asyncio.Task[None] | None = None

        if health is not None:
            health_monitor = LiveHealthMonitor(
                health=health,
                interval_seconds=_LIVE_LEASE_HEARTBEAT_INTERVAL_SECONDS,
                is_degraded=lambda: (
                    market_task.done()
                    or account_task.done()
                    or lease_task.done()
                    or (
                        risk_control_task is not None
                        and risk_control_task.done()
                    )
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
            ),
            block_entry_submissions=(
                lambda: execution_coordinator.block_entry_submissions()
                if execution_coordinator is not None
                else None
            ),
            stop_sources=stop_runtime_sources,
            close_risk_control=close_risk_control,
            stop_entry_caches=stop_entry_caches,
        )
        try:
            result = await runtime_supervisor.run()
        finally:
            await runtime_supervisor.stop()
        await _record_transition(
            live_repository,
            session_id=session_id,
            operator=operator,
            strategy_config_hash=strategy_config_hash,
            risk_config_hash=risk_config_hash,
            state=(
                LiveSessionState.HALTED
                if result.halt_reason is not None
                else LiveSessionState.COMPLETED
            ),
            reason=result.halt_reason,
        )
        return result
    except Exception as exc:
        if startup_phase and _is_retryable_live_startup_error(exc):
            raise _LiveStartupRetryableError(exc) from exc
        if live_repository is not None and risk_config_hash:
            await _record_transition(
                live_repository,
                session_id=session_id,
                operator=operator,
                strategy_config_hash=strategy_config_hash,
                risk_config_hash=risk_config_hash,
                state=LiveSessionState.HALTED,
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
            await asyncio.gather(
                startup_market_state_task,
                return_exceptions=True,
            )
        if entry_order_lifecycle is not None:
            await entry_order_lifecycle.stop()
        if execution_coordinator is not None:
            await execution_coordinator.aclose()
        if client is not None:
            await client.aclose()
        if closed_candle_feed is not None:
            await closed_candle_feed.stop()
        if candle_source is not None:
            candle_source.close()
        if ema_candle_source is not None:
            ema_candle_source.close()
        if signal_recorder is not None:
            await signal_recorder.stop()
        if telemetry is not None:
            await telemetry.stop()
        if volume_cache is not None:
            await volume_cache.stop()
        if volume_rest_client is not None:
            await volume_rest_client.aclose()
        await execution_engine.dispose()
        await market_engine.dispose()
        await observability_engine.dispose()
        await checkpoint_engine.dispose()
        if heartbeat_engine is not None:
            await heartbeat_engine.dispose()
        if health is not None:
            try:
                health.stopped()
            except Exception:
                log.exception("live_health_stop_marker_failed")


async def _observe_market_states(
    states: AsyncIterable[MarketState15s],
    cache: LatestMarketStateCache,
) -> AsyncIterator[MarketState15s]:
    async for state in states:
        cache.observe(state)
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
        on_exit_failure=on_exit_failure,
        on_account_snapshot=on_account_snapshot,
        pending_position_retry_delays=_PENDING_POSITION_RETRY_DELAYS_SECONDS,
    )
    await runtime.run(source)


def _is_transient_live_runtime_error(error: Exception) -> bool:
    return isinstance(
        error,
        (SQLAlchemyError, TimeoutError, ConnectionError, OSError),
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


async def _record_transition(
    repository: PostgresLiveRolloutRepository,
    *,
    session_id: str,
    operator: str,
    strategy_config_hash: str,
    risk_config_hash: str,
    state: LiveSessionState,
    reason: str | None = None,
) -> None:
    occurred_at = datetime.now(tz=UTC)
    await repository.save_transition(
        LiveSessionTransition(
            transition_id=f"transition-{uuid4()}",
            session_id=session_id,
            state=state,
            occurred_at=occurred_at,
            operator=operator,
            strategy_config_hash=strategy_config_hash,
            risk_config_hash=risk_config_hash,
            reason=reason,
            details={},
        )
    )


def _load_plan(path: Path) -> OrderExecutionPlan:
    payload = json.loads(path.read_text(encoding="utf-8"))
    plan = OrderExecutionPlan(
        intent_id=str(payload["intent_id"]),
        run_id=str(payload["run_id"]),
        client_order_id=str(payload["client_order_id"]),
        symbol=str(payload["symbol"]),
        side=str(payload["side"]),
        order_type=str(payload["order_type"]),
        quantity=Decimal(str(payload["quantity"])),
        price=None if payload.get("price") is None else Decimal(str(payload["price"])),
        reduce_only=bool(payload["reduce_only"]),
        created_at=datetime.fromisoformat(str(payload["created_at"])),
        position_side=FuturesPositionSide(
            str(payload.get("position_side", FuturesPositionSide.BOTH.value))
        ),
        quantized=bool(payload.get("quantized", False)),
        time_in_force=(
            None
            if payload.get("time_in_force") is None
            else str(payload["time_in_force"])
        ),
        expires_at=(
            None
            if payload.get("expires_at") is None
            else datetime.fromisoformat(str(payload["expires_at"]))
        ),
    )
    if not plan.quantized:
        raise typer.BadParameter("order plan must be quantized")
    return plan


def _live_strategy_config(
    profile: LiveOrderFlowImpulseProfile | None = None,
) -> dict[str, object]:
    resolved_profile = profile or _LIVE_ORDERFLOW_PROFILE
    return {
        "candidate_notional": Decimal("100"),
        "candidate_ttl_buckets": 4,
        "order_flow_impulse_impulse_window_buckets": (
            resolved_profile.impulse_window_buckets
        ),
        "order_flow_impulse_confirmation_buckets": (
            resolved_profile.confirmation_buckets
        ),
        "order_flow_impulse_min_return_pct": resolved_profile.min_return_pct,
        "order_flow_impulse_min_aggressive_imbalance": (
            resolved_profile.min_aggressive_imbalance
        ),
        "order_flow_impulse_min_notional_intensity": (
            resolved_profile.min_notional_intensity
        ),
        "order_flow_impulse_min_notional_5m_vs_30m": (
            resolved_profile.min_notional_5m_vs_30m
        ),
        "cooldown_buckets": resolved_profile.cooldown_buckets,
    }


def _live_strategy_config_hash(
    strategy_name: str,
    *,
    profile: LiveOrderFlowImpulseProfile | None = None,
    entry_positive_gainer_top_count: int | None = _LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT,
    require_price_above_ema5: bool = _LIVE_ENTRY_PRICE_ABOVE_EMA5,
    require_price_above_ema10: bool = _LIVE_ENTRY_PRICE_ABOVE_EMA10,
    entry_policy_enforce: bool = False,
    entry_order_type: EntryType = _LIVE_ENTRY_ORDER_TYPE,
    entry_limit_ttl_seconds: int = _LIVE_ENTRY_LIMIT_TTL_SECONDS,
) -> str:
    if (
        entry_positive_gainer_top_count is not None
        and entry_positive_gainer_top_count <= 0
    ):
        raise ValueError("entry_positive_gainer_top_count must be positive")
    if not isinstance(entry_order_type, EntryType):
        raise TypeError("entry_order_type must be an EntryType")
    if not isinstance(entry_policy_enforce, bool):
        raise TypeError("entry_policy_enforce must be a bool")
    if entry_limit_ttl_seconds < 601:
        raise ValueError("entry_limit_ttl_seconds must be at least 601")
    return deterministic_config_hash(
        {
            "strategy": build_runtime_config(
                strategy_name,
                config=_live_strategy_config(profile),
            ),
            "entry_filter": {
                "entry_positive_gainer_top_count": entry_positive_gainer_top_count,
                "require_price_above_ema5": require_price_above_ema5,
                "require_price_above_ema10": require_price_above_ema10,
                "entry_policy_enforce": entry_policy_enforce,
            },
            "entry_execution": {
                "order_type": entry_order_type.value,
                "limit_ttl_seconds": entry_limit_ttl_seconds,
            },
        }
    )


def _validate_hex_hash(
    raw_value: str,
    option_name: str,
    expected_length: int,
) -> str:
    """Normalize and validate an operator-supplied immutable hash value."""

    value = raw_value.strip().lower()
    if len(value) != expected_length or _HEX_HASH_PATTERN.fullmatch(value) is None:
        raise typer.BadParameter(
            f"{option_name} must be exactly {expected_length} lowercase hex characters"
        )
    return value


def _runtime_strategy_config_hash(strategy_name: str) -> str:
    """Compute the hash from the same environment values used by Live run."""

    runtime_config = _preflight_runtime_strategy_config()
    return _live_strategy_config_hash(
        strategy_name,
        profile=runtime_config.profile,
        entry_positive_gainer_top_count=runtime_config.entry_positive_gainer_top_count,
        require_price_above_ema5=runtime_config.require_price_above_ema5,
        require_price_above_ema10=runtime_config.require_price_above_ema10,
        entry_policy_enforce=runtime_config.entry_policy_enforce,
        entry_order_type=runtime_config.entry_order_type,
        entry_limit_ttl_seconds=runtime_config.entry_limit_ttl_seconds,
    )


async def _latest_risk_config_hash(
    database_url: str,
    account_label: str,
) -> str:
    engine = create_execution_database_engine(database_url)
    try:
        config = await _latest_risk_config(
            async_sessionmaker(engine, expire_on_commit=False),
            account_label,
        )
        return _validate_hex_hash(
            config.config_hash,
            "latest risk config hash",
            _CONFIG_HASH_LENGTH,
        )
    finally:
        await engine.dispose()


async def _load_active_approval(
    database_url: str,
    account_label: str,
    strategy_name: str,
    now: datetime,
) -> LiveOperatorApproval | None:
    engine = create_execution_database_engine(database_url)
    try:
        return await PostgresLiveRolloutRepository(
            async_sessionmaker(engine, expire_on_commit=False)
        ).load_active_approval(
            account_label=account_label,
            strategy_name=strategy_name,
            now=now,
        )
    finally:
        await engine.dispose()


async def _prepare_live_risk_gates(
    *,
    database_url: str,
    account_label: str,
    strategy_name: str,
    lease_owner: str,
    code_generation: str,
    lease_ttl_seconds: int,
    max_order_notional: Decimal | None,
    max_gross_notional: Decimal | None,
    max_daily_loss: Decimal | None,
    max_open_positions: int | None,
    profile: LiveOrderFlowImpulseProfile,
    entry_positive_gainer_top_count: int | None,
    require_price_above_ema5: bool,
    require_price_above_ema10: bool,
    entry_policy_enforce: bool,
    entry_order_type: EntryType,
    entry_limit_ttl_seconds: int,
) -> dict[str, str]:
    now = datetime.now(tz=UTC)
    risk_config = RiskConfigSnapshot(
        environment="live",
        account_label=account_label,
        max_order_notional=max_order_notional,
        max_gross_notional=max_gross_notional,
        max_daily_loss=max_daily_loss,
        max_open_positions=max_open_positions,
        max_market_state_age_seconds=_LIVE_UNENFORCED_STATE_AGE_SECONDS,
        max_account_state_age_seconds=_LIVE_UNENFORCED_STATE_AGE_SECONDS,
        allow_reduce_only_while_draining=True,
        created_at=now,
    )
    lease = TradingLease(
        lease_id=f"lease-{uuid4()}",
        environment="live",
        account_label=account_label,
        strategy_name=strategy_name,
        owner=lease_owner,
        code_generation=code_generation,
        state=TradingLeaseState.ACTIVE,
        acquired_at=now,
        expires_at=now + timedelta(seconds=lease_ttl_seconds),
    )
    engine = create_execution_database_engine(database_url)
    try:
        repository = PostgresRiskRepository(
            async_sessionmaker(engine, expire_on_commit=False)
        )
        await repository.save_risk_config(risk_config)
        await repository.acquire_lease(lease)
    finally:
        await engine.dispose()
    return {
        "lease_id": lease.lease_id,
        "lease_expires_at": lease.expires_at.isoformat(),
        "risk_config_hash": risk_config.config_hash,
        "strategy_config_hash": _live_strategy_config_hash(
            strategy_name,
            profile=profile,
            entry_positive_gainer_top_count=entry_positive_gainer_top_count,
            require_price_above_ema5=require_price_above_ema5,
            require_price_above_ema10=require_price_above_ema10,
            entry_policy_enforce=entry_policy_enforce,
            entry_order_type=entry_order_type,
            entry_limit_ttl_seconds=entry_limit_ttl_seconds,
        ),
    }


async def _renew_live_lease(
    *,
    database_url: str,
    account_label: str,
    strategy_name: str,
    lease_owner: str,
    lease_ttl_seconds: int,
) -> dict[str, str]:
    if lease_ttl_seconds < 300:
        raise ValueError("lease_ttl_seconds must be at least 300")
    now = datetime.now(tz=UTC)
    engine = create_execution_database_engine(database_url)
    try:
        repository = PostgresRiskRepository(
            async_sessionmaker(engine, expire_on_commit=False)
        )
        lease = await repository.load_active_lease("live", account_label, now)
        if lease is None:
            raise RuntimeError(
                f"active live lease is missing for account {account_label}"
            )
        if lease.owner != lease_owner:
            raise RuntimeError(
                f"live lease owner mismatch for account {account_label}"
            )
        if lease.strategy_name != strategy_name:
            raise RuntimeError(
                f"live lease strategy mismatch for account {account_label}"
            )
        renewed = await repository.renew_lease(
            lease_id=lease.lease_id,
            owner=lease_owner,
            expires_at=now + timedelta(seconds=lease_ttl_seconds),
        )
    finally:
        await engine.dispose()
    return {
        "account_label": renewed.account_label,
        "lease_id": renewed.lease_id,
        "lease_owner": renewed.owner,
        "lease_expires_at": renewed.expires_at.isoformat(),
    }


async def _save_approval(
    database_url: str,
    approval: LiveOperatorApproval,
) -> None:
    engine = create_execution_database_engine(database_url)
    try:
        repository = PostgresLiveRolloutRepository(
            async_sessionmaker(engine, expire_on_commit=False)
        )
        await repository.save_approval(approval)
    finally:
        await engine.dispose()


_UNLIMITED_VALUES = frozenset({"none", "unlimited"})


def _resolve_live_entry_positive_gainer_top_count(value: int | None) -> int:
    if value is not None:
        return value
    raw = os.environ.get("CML_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT", "").strip()
    if not raw:
        return _LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT
    try:
        resolved = int(raw)
    except ValueError as error:
        raise typer.BadParameter(
            "CML_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT must be an integer"
        ) from error
    if resolved <= 0:
        raise typer.BadParameter(
            "CML_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT must be positive"
        )
    return resolved


def _resolve_live_profile_options(
    *,
    impulse_window_buckets: int | None,
    confirmation_buckets: int | None,
    min_return_pct: str | None,
    min_imbalance: str | None,
    min_intensity: str | None,
    min_notional_5m_vs_30m: str | None,
    cooldown_buckets: int | None,
) -> LiveOrderFlowImpulseProfile:
    """Build one account profile from all CLI values or the service env.

    Partial overrides are rejected so a profile cannot accidentally combine
    one account's values with another account's defaults.
    """

    values = (
        impulse_window_buckets,
        confirmation_buckets,
        min_return_pct,
        min_imbalance,
        min_intensity,
        min_notional_5m_vs_30m,
        cooldown_buckets,
    )
    if not any(value is not None for value in values):
        try:
            return LiveOrderFlowImpulseProfile.from_environment()
        except ValueError as error:
            raise typer.BadParameter(str(error)) from error
    legacy_values = values[:5] + values[6:]
    if min_notional_5m_vs_30m is None and all(
        value is not None for value in legacy_values
    ):
        # Keep the six-option CLI form backward-compatible.  The seventh
        # dimension is opt-in and disabled when omitted from a manual command.
        min_notional_5m_vs_30m = "0"
    if not all(value is not None for value in values):
        raise typer.BadParameter(
            "all seven order-flow profile options must be provided together"
        )
    assert impulse_window_buckets is not None
    assert confirmation_buckets is not None
    assert min_return_pct is not None
    assert min_imbalance is not None
    assert min_intensity is not None
    assert min_notional_5m_vs_30m is not None
    assert cooldown_buckets is not None
    try:
        return LiveOrderFlowImpulseProfile(
            impulse_window_buckets=impulse_window_buckets,
            confirmation_buckets=confirmation_buckets,
            min_return_pct=Decimal(min_return_pct),
            min_aggressive_imbalance=Decimal(min_imbalance),
            min_notional_intensity=Decimal(min_intensity),
            min_notional_5m_vs_30m=Decimal(min_notional_5m_vs_30m),
            cooldown_buckets=cooldown_buckets,
        )
    except (InvalidOperation, ValueError) as error:
        raise typer.BadParameter(f"invalid live order-flow profile: {error}") from error


def _parse_exchange_operations(
    raw_value: str,
) -> frozenset[str] | None:
    """Parse the durable exchange telemetry allow-list.

    Empty values use the auditable ``submit,cancel`` default. ``all`` is the
    explicit operator-facing spelling for temporary full-operation diagnostics;
    it must not be combined with an allow-list because the two policies are
    mutually exclusive.
    """

    normalized_value = raw_value.strip()
    if not normalized_value:
        return _DEFAULT_PERSIST_EXCHANGE_OPERATIONS
    if normalized_value.lower() == "all":
        return None
    operations = tuple(operation.strip() for operation in raw_value.split(","))
    if any(not operation for operation in operations):
        raise typer.BadParameter(
            "--persist-exchange-operations must be a comma-separated list "
            "of non-empty operation names"
        )
    if any(operation.lower() == "all" for operation in operations):
        raise typer.BadParameter(
            "--persist-exchange-operations accepts 'all' only by itself"
        )
    return frozenset(operations)


def _parse_optional_decimal_limit(
    raw_value: str,
    option_name: str,
) -> Decimal | None:
    normalized = raw_value.strip().lower()
    if normalized in _UNLIMITED_VALUES:
        return None
    try:
        value = Decimal(normalized)
    except InvalidOperation as error:
        raise typer.BadParameter(
            f"{option_name} must be positive or 'unlimited'"
        ) from error
    if not value.is_finite() or value <= 0:
        raise typer.BadParameter(f"{option_name} must be positive or 'unlimited'")
    return value


def _parse_optional_integer_limit(
    raw_value: str,
    option_name: str,
) -> int | None:
    normalized = raw_value.strip().lower()
    if normalized in _UNLIMITED_VALUES:
        return None
    try:
        value = int(normalized)
    except ValueError as error:
        raise typer.BadParameter(
            f"{option_name} must be a positive integer or 'unlimited'"
        ) from error
    if value <= 0:
        raise typer.BadParameter(
            f"{option_name} must be a positive integer or 'unlimited'"
        )
    return value


def _parse_approval_expiration(
    now: datetime,
    expires_in_minutes: str,
) -> datetime | None:
    normalized = expires_in_minutes.strip().lower()
    if normalized in {"never", *_UNLIMITED_VALUES}:
        return None
    try:
        minutes = int(normalized)
    except ValueError as error:
        raise typer.BadParameter(
            "--expires-in-minutes must be a positive integer or 'never'"
        ) from error
    if minutes <= 0:
        raise typer.BadParameter(
            "--expires-in-minutes must be a positive integer or 'never'"
        )
    return now + timedelta(minutes=minutes)


async def _preflight_summary(
    database_url: str,
    account_label: str,
    strategy_name: str,
    *,
    expected_git_commit: str | None = None,
    expected_migration_revision: str | None = None,
    expected_lease_owner: str | None = None,
    expected_strategy_config_hash: str | None = None,
) -> dict[str, object]:
    now = datetime.now(tz=UTC)
    engine = create_execution_database_engine(database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        approval = await PostgresLiveRolloutRepository(factory).load_active_approval(
            account_label=account_label,
            strategy_name=strategy_name,
            now=now,
        )
        lease = await PostgresRiskRepository(factory).load_active_lease(
            "live", account_label, now
        )
        unresolved = await PostgresOrderRepository(factory).load_unresolved_orders()
        risk_config = await _latest_risk_config(factory, account_label)
        runtime_config = _preflight_runtime_strategy_config()
        runtime_strategy_config_hash = _live_strategy_config_hash(
            strategy_name,
            profile=runtime_config.profile,
            entry_positive_gainer_top_count=(
                runtime_config.entry_positive_gainer_top_count
            ),
            require_price_above_ema5=runtime_config.require_price_above_ema5,
            require_price_above_ema10=runtime_config.require_price_above_ema10,
            entry_policy_enforce=runtime_config.entry_policy_enforce,
            entry_order_type=runtime_config.entry_order_type,
            entry_limit_ttl_seconds=runtime_config.entry_limit_ttl_seconds,
        )
        configured_strategy_config_hash = (
            os.environ.get("CML_LIVE_STRATEGY_CONFIG_HASH", "").strip().lower()
            or None
        )
        if configured_strategy_config_hash == "unset":
            configured_strategy_config_hash = None
        approved_strategy_config_hash = (
            None if approval is None else approval.strategy_config_hash
        )
        account_state = await _latest_account_state(factory, account_label)
        approval_risk_config_hash = (
            None if approval is None else approval.risk_config_hash
        )
        approval_git_commit_hash = (
            None if approval is None else approval.git_commit_hash
        )
        approval_migration_revision = (
            None
            if approval is None
            else approval.database_migration_revision
        )
        checks: dict[str, bool] = {
            "approval_present": approval is not None,
            "lease_present": lease is not None,
            "account_ready": account_state.value == "ready_readonly",
            "runtime_strategy_config_matches_approval": (
                approval is not None
                and runtime_strategy_config_hash == approved_strategy_config_hash
            ),
            "risk_config_matches_approval": (
                approval is not None
                and risk_config.config_hash == approval_risk_config_hash
            ),
        }
        if configured_strategy_config_hash is not None:
            checks["runtime_strategy_config_matches_configured"] = (
                runtime_strategy_config_hash == configured_strategy_config_hash
            )
        if expected_git_commit is not None:
            checks["approval_git_commit_matches_expected"] = (
                approval_git_commit_hash == expected_git_commit.strip().lower()
            )
        if expected_migration_revision is not None:
            checks["approval_migration_matches_expected"] = (
                approval_migration_revision == expected_migration_revision.strip()
            )
        if expected_lease_owner is not None:
            checks["lease_owner_matches_expected"] = (
                lease is not None and lease.owner == expected_lease_owner
            )
        if expected_strategy_config_hash is not None:
            checks["runtime_strategy_config_matches_manifest"] = (
                runtime_strategy_config_hash == expected_strategy_config_hash
            )
        preflight_errors = [name for name, passed in checks.items() if not passed]
        return {
            "approval_present": approval is not None,
            "lease_present": lease is not None,
            "account_state": account_state.value,
            "unresolved_order_count": len(unresolved),
            "risk_config_hash": risk_config.config_hash,
            "approved_risk_config_hash": approval_risk_config_hash,
            "runtime_strategy_config_hash": runtime_strategy_config_hash,
            "configured_strategy_config_hash": configured_strategy_config_hash,
            "approved_strategy_config_hash": approved_strategy_config_hash,
            "approved_git_commit_hash": approval_git_commit_hash,
            "approved_migration_revision": approval_migration_revision,
            "expected_lease_owner": expected_lease_owner,
            "expected_strategy_config_hash": expected_strategy_config_hash,
            "preflight_checks": checks,
            "preflight_errors": preflight_errors,
            "preflight_ok": not preflight_errors,
            "runtime_strategy_config_matches_configured": (
                None
                if configured_strategy_config_hash is None
                else runtime_strategy_config_hash == configured_strategy_config_hash
            ),
            "runtime_strategy_config_matches_approval": (
                None
                if approved_strategy_config_hash is None
                else runtime_strategy_config_hash == approved_strategy_config_hash
            ),
            "runtime_strategy_config_inputs": {
                **runtime_config.profile.as_dict(),
                "entry_positive_gainer_top_count": (
                    runtime_config.entry_positive_gainer_top_count
                ),
                "require_price_above_ema5": runtime_config.require_price_above_ema5,
                "require_price_above_ema10": runtime_config.require_price_above_ema10,
                "entry_policy_mode": runtime_config.entry_policy_mode,
                "entry_order_type": runtime_config.entry_order_type.value,
                "entry_limit_ttl_seconds": runtime_config.entry_limit_ttl_seconds,
            },
        }
    finally:
        await engine.dispose()


def _preflight_runtime_strategy_config() -> _PreflightRuntimeStrategyConfig:
    """Resolve the Live hash inputs exposed to one-off preflight containers.

    Compose passes the account-scoped profile, top-N, and policy-mode values
    into the long-running Live service environment. Keeping this resolver
    beside the diagnostic makes the reported hash explainable instead of
    silently using the library defaults (which may describe a different lane).
    """

    raw_top_count = os.environ.get(
        "CML_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT",
        str(_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT),
    ).strip()
    try:
        top_count = int(raw_top_count)
    except ValueError as error:
        raise ValueError(
            "CML_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT must be an integer"
        ) from error
    if top_count <= 0:
        raise ValueError("CML_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT must be positive")

    policy_mode = (
        os.environ.get("CML_LIVE_ENTRY_POLICY_MODE", "enforce").strip().lower()
        or "enforce"
    )
    if policy_mode not in _LIVE_ENTRY_POLICY_MODES:
        raise ValueError(
            "CML_LIVE_ENTRY_POLICY_MODE must be one of: "
            + ", ".join(sorted(_LIVE_ENTRY_POLICY_MODES))
        )
    try:
        profile = LiveOrderFlowImpulseProfile.from_environment()
    except ValueError as error:
        raise ValueError(str(error)) from error
    return _PreflightRuntimeStrategyConfig(
        profile=profile,
        entry_positive_gainer_top_count=top_count,
        require_price_above_ema5=_LIVE_ENTRY_PRICE_ABOVE_EMA5,
        require_price_above_ema10=_LIVE_ENTRY_PRICE_ABOVE_EMA10,
        entry_policy_mode=policy_mode,
        entry_order_type=_LIVE_ENTRY_ORDER_TYPE,
        entry_limit_ttl_seconds=_LIVE_ENTRY_LIMIT_TTL_SECONDS,
    )


async def _approved_intent_notional(
    factory: async_sessionmaker[AsyncSession],
    intent_id: str,
) -> Decimal | None:
    async with factory() as session:
        details = await session.scalar(
            select(OrderIntentExecutionRow.details).where(
                OrderIntentExecutionRow.intent_id == intent_id
            )
        )
    if not isinstance(details, dict):
        return None
    value = details.get("desired_notional")
    return None if value is None else Decimal(str(value))


async def _load_transition(
    database_url: str,
    session_id: str,
) -> LiveSessionTransition | None:
    engine = create_execution_database_engine(database_url)
    try:
        return await PostgresLiveRolloutRepository(
            async_sessionmaker(engine, expire_on_commit=False)
        ).load_latest_transition(session_id)
    finally:
        await engine.dispose()


async def _save_transition(
    database_url: str,
    session_id: str,
    operator: str,
    strategy_config_hash: str,
    risk_config_hash: str,
    state: LiveSessionState,
    reason: str,
) -> LiveSessionTransition:
    now = datetime.now(tz=UTC)
    transition = LiveSessionTransition(
        transition_id=f"transition-{uuid4()}",
        session_id=session_id,
        state=state,
        occurred_at=now,
        operator=operator,
        strategy_config_hash=strategy_config_hash,
        risk_config_hash=risk_config_hash,
        reason=reason,
        details={},
    )
    engine = create_execution_database_engine(database_url)
    try:
        await PostgresLiveRolloutRepository(
            async_sessionmaker(engine, expire_on_commit=False)
        ).save_transition(transition)
    finally:
        await engine.dispose()
    return transition


async def _publish_risk_control_event(
    *,
    url: str,
    token: str | None,
    event: RiskControlEvent,
) -> RiskControlEvent:
    publisher = WebSocketRiskControlPublisher(
        url=url,
        token=token or os.environ.get("CML_RISK_CONTROL_HUB_TOKEN") or None,
    )
    return await publisher.publish(event)


def _issue_one_shot_risk_control_command(
    *,
    action: RiskControlAction,
    command_type: str,
    confirmation_text: str,
    reason: str,
    session_id: str,
    operator: str,
    idempotency_key: str,
    confirmation: str,
    account_label: str,
    strategy: str,
    risk_control_hub_url: str,
    risk_control_hub_token: str | None,
    database_url: str | None,
) -> None:
    if confirmation != confirmation_text:
        raise typer.BadParameter(
            f"--confirmation must equal '{confirmation_text}'"
        )
    if not session_id.strip():
        raise typer.BadParameter("--session-id must not be empty")
    if not operator.strip():
        raise typer.BadParameter("--operator must not be empty")
    if not idempotency_key.strip():
        raise typer.BadParameter("--idempotency-key must not be empty")
    resolved_url = (
        risk_control_hub_url.strip()
        or os.environ.get("CML_RISK_CONTROL_HUB_URL", "").strip()
    )
    if not resolved_url:
        raise typer.BadParameter(
            "--risk-control-hub-url or CML_RISK_CONTROL_HUB_URL is required"
        )

    command = asyncio.run(
        _load_or_save_risk_control_command(
            database_url=_database_url(database_url),
            command_type=command_type,
            requested_by=operator,
            confirmation_text=confirmation,
            idempotency_key=idempotency_key,
            account_label=account_label,
            strategy_name=strategy,
            session_id=session_id,
        )
    )
    if command.status != "requested":
        typer.echo(
            json.dumps(
                {
                    "command_id": command.command_id,
                    "status": command.status,
                    "idempotency_key": command.idempotency_key,
                },
                sort_keys=True,
            )
        )
        return

    event = RiskControlEvent(
        environment="live",
        account_label=account_label,
        strategy_name=strategy,
        session_id=session_id,
        action=action,
        event_id=command.command_id,
        command_id=command.command_id,
        reason=reason,
        issued_at=command.requested_at,
        details={
            "command_type": command_type,
            "idempotency_key": command.idempotency_key,
        },
    )
    try:
        published = asyncio.run(
            _publish_risk_control_event(
                url=resolved_url,
                token=risk_control_hub_token,
                event=event,
            )
        )
    except Exception as error:
        typer.echo(
            json.dumps(
                {
                    "command_id": command.command_id,
                    "status": "requested",
                    "publish_error": type(error).__name__,
                    "retry_with_same_idempotency_key": True,
                },
                sort_keys=True,
            )
        )
        raise typer.Exit(code=1) from error
    typer.echo(
        json.dumps(
            {
                "action": action.value,
                "command_id": command.command_id,
                "sequence": published.sequence,
                "status": "published",
                "stream_epoch": published.stream_epoch,
            },
            sort_keys=True,
        )
    )


async def _load_or_save_risk_control_command(
    *,
    database_url: str,
    command_type: str,
    requested_by: str,
    confirmation_text: str,
    idempotency_key: str,
    account_label: str,
    strategy_name: str,
    session_id: str,
) -> RollbackCommand:
    now = datetime.now(tz=UTC)
    engine = create_execution_database_engine(database_url)
    repository = PostgresLiveRolloutRepository(
        async_sessionmaker(engine, expire_on_commit=False)
    )
    try:
        existing = await repository.load_command_by_idempotency(idempotency_key)
        if existing is not None:
            _require_matching_risk_control_command(
                existing,
                command_type=command_type,
                account_label=account_label,
                strategy_name=strategy_name,
                session_id=session_id,
            )
            return existing
        command = RollbackCommand(
            command_id=f"command-{uuid4()}",
            command_type=command_type,
            requested_by=requested_by,
            confirmation_text=confirmation_text,
            requested_at=now,
            idempotency_key=idempotency_key,
            account_label=account_label,
            strategy_name=strategy_name,
            session_id=session_id,
            status="requested",
            completed_at=None,
            failure_reason=None,
        )
        if await repository.save_command(command):
            return command
        existing = await repository.load_command_by_idempotency(idempotency_key)
        if existing is None:
            raise RuntimeError("risk-control command insert was not observable")
        _require_matching_risk_control_command(
            existing,
            command_type=command_type,
            account_label=account_label,
            strategy_name=strategy_name,
            session_id=session_id,
        )
        return existing
    finally:
        await engine.dispose()


def _require_matching_risk_control_command(
    command: RollbackCommand,
    *,
    command_type: str,
    account_label: str,
    strategy_name: str,
    session_id: str,
) -> None:
    if (
        command.command_type != command_type
        or command.account_label != account_label
        or command.strategy_name != strategy_name
        or command.session_id != session_id
    ):
        raise ValueError(
            "idempotency key is already bound to a different risk-control command"
        )


def _execution_database_url(value: str | None) -> str:
    return _resolve_database_url(value, "CML_EXECUTION_DATABASE_URL")


def _market_database_url(value: str | None) -> str:
    return _resolve_database_url(value, "CML_MARKET_DATABASE_URL")


def _observability_database_url(value: str | None) -> str:
    return _resolve_database_url(value, "CML_OBSERVABILITY_DATABASE_URL")


def _resolve_database_url(value: str | None, plane_env_var: str) -> str:
    resolved = resolve_database_url(
        value,
        plane_env_var,
        "CML_DATABASE_URL",
    )
    if not resolved:
        raise typer.BadParameter(
            f"--database-url or {plane_env_var} or CML_DATABASE_URL is required"
        )
    return resolved


def _resolve_live_cli_credentials(
    *,
    api_key_env: str | None,
    api_secret_env: str | None,
    allow_legacy_fallback: bool,
) -> ResolvedBinanceCredentials:
    try:
        return resolve_role_credentials(
            BinanceCredentialRole.TRADE,
            api_key_env=api_key_env,
            api_secret_env=api_secret_env,
            allow_legacy_fallback=allow_legacy_fallback,
        )
    except CredentialResolutionError as error:
        raise typer.BadParameter(str(error)) from error


def _database_url(value: str | None) -> str:
    """Resolve the execution plane for legacy live CLI commands."""

    return _execution_database_url(value)
