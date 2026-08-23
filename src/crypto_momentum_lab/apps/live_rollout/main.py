import asyncio
import json
import os
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable
from dataclasses import asdict, replace
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

from crypto_momentum_lab.apps.shadow_operation.main import (
    _latest_account_state,
    _latest_risk_config,
)
from crypto_momentum_lab.domain.execution import FuturesPositionSide, OrderExecutionPlan
from crypto_momentum_lab.domain.live_rollout import (
    LiveOperatorApproval,
    LiveSessionState,
    LiveSessionTransition,
)
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.risk import (
    RiskConfigSnapshot,
    RiskEvaluation,
    TradingLease,
    TradingLeaseState,
)
from crypto_momentum_lab.domain.strategy import (
    OrderIntentCandidate,
    RunMode,
    StrategyCheckpoint,
    StrategyRunIdentity,
    deterministic_config_hash,
)
from crypto_momentum_lab.execution_account.binance import (
    BinanceRateLimitError,
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
from crypto_momentum_lab.live_rollout.daemon import (
    LiveDaemonConfig,
    LiveDaemonResult,
    LiveEntryFilterContext,
    LiveRuntimeStrategy,
    LiveStrategyDaemon,
)
from crypto_momentum_lab.live_rollout.entry_cache import (
    EntryFilterCacheConfig,
    LiveEntryFilterCache,
)
from crypto_momentum_lab.live_rollout.exits import (
    LiveExitConfig,
    LiveExitManager,
    ThreadedClosedCandle15mLoader,
)
from crypto_momentum_lab.live_rollout.gates import LiveGateContext, evaluate_live_gate
from crypto_momentum_lab.live_rollout.lease import (
    LeaseHeartbeatConfig,
    LiveLeaseHeartbeat,
)
from crypto_momentum_lab.live_rollout.limits import FixedLiveLimits
from crypto_momentum_lab.live_rollout.postgres_runtime import (
    PostgresLiveContextProvider,
    live_limits_from_approval,
    poll_live_market_states,
)
from crypto_momentum_lab.live_rollout.session import (
    LiveRolloutSession,
    LiveSessionConfig,
    LiveSessionResult,
)
from crypto_momentum_lab.live_rollout.telemetry import (
    PERSISTED_ORDER_TELEMETRY_EVENTS,
    LiveRuntimeTelemetry,
    LiveTelemetrySink,
)
from crypto_momentum_lab.market_data.hub import (
    MarketStateHubError,
    WebSocketMarketStateSource,
)
from crypto_momentum_lab.persistence.postgres.live_rollout_repository import (
    PostgresLiveRolloutRepository,
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
from crypto_momentum_lab.persistence.postgres.repository import (
    PostgresUniverseRepository,
)
from crypto_momentum_lab.persistence.postgres.risk_repository import (
    PostgresRiskRepository,
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
# These two columns are retained by the existing risk-config schema for paper
# and shadow sessions. Live execution no longer enforces state-age limits; the
# large compatibility value makes that explicit without a destructive schema
# migration.
_LIVE_UNENFORCED_STATE_AGE_SECONDS = 1_000_000_000.0
_LIVE_MIN_WARMUP_SECONDS = 60
_LIVE_WARMUP_STATE_LIMIT = 100_000
_LIVE_WARMUP_BATCH_SIZE = 100
_LIVE_STARTUP_RETRY_INITIAL_SECONDS = 15
_LIVE_STARTUP_RETRY_MAX_SECONDS = 300
_LIVE_AUTO_REACQUIRE_LEASE_TTL_SECONDS = 300
_LIVE_LEASE_RENEW_BEFORE_SECONDS = 120
_LIVE_LEASE_HEARTBEAT_INTERVAL_SECONDS = 15.0
_LIVE_RECONCILE_INTERVAL_SECONDS = 60.0
_LIVE_ENTRY_FILTER_PREFETCH_CONCURRENCY = 4
_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT = 100
_LIVE_ENTRY_PRICE_ABOVE_EMA5 = True
_LIVE_ENTRY_PRICE_ABOVE_EMA10 = True


class _LiveStartupRetryableError(RuntimeError):
    def __init__(self, cause: Exception) -> None:
        super().__init__(str(cause))
        self.retry_after_seconds = getattr(cause, "retry_after_seconds", None)


@app.callback()
def live_rollout_app() -> None:
    """Operate explicitly approved small-capital live sessions."""


@app.command("strategy-config-hash")
def strategy_config_hash_command(
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
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
) -> None:
    typer.echo(
        _live_strategy_config_hash(
            strategy,
            entry_positive_gainer_top_count=entry_positive_gainer_top_count,
            require_price_above_ema5=entry_price_above_ema5,
            require_price_above_ema10=entry_price_above_ema10,
        )
    )


@app.command("prepare")
def prepare_command(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    lease_owner: Annotated[str, typer.Option("--lease-owner")] = "live-worker",
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
    confirmation: Annotated[str, typer.Option("--confirmation")] = "",
) -> None:
    if confirmation != _PREPARE_CONFIRMATION:
        raise typer.BadParameter(f"--confirmation must equal '{_PREPARE_CONFIRMATION}'")
    payload = asyncio.run(
        _prepare_live_risk_gates(
            database_url=_database_url(database_url),
            account_label=account_label,
            strategy_name=strategy,
            lease_owner=lease_owner,
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
            entry_positive_gainer_top_count=entry_positive_gainer_top_count,
            require_price_above_ema5=entry_price_above_ema5,
            require_price_above_ema10=entry_price_above_ema10,
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
    asyncio.run(_save_approval(_database_url(database_url), approval))
    typer.echo(f"Live approval recorded: {approval.approval_id}")


@app.command("preflight")
def preflight_command(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
) -> None:
    payload = asyncio.run(
        _preflight_summary(_database_url(database_url), account_label, strategy)
    )
    typer.echo(json.dumps(payload, sort_keys=True))


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
    base_url: Annotated[str, typer.Option("--base-url")] = "https://fapi.binance.com",
    api_key_env: Annotated[str, typer.Option("--api-key-env")] = "BINANCE_API_KEY",
    api_secret_env: Annotated[
        str, typer.Option("--api-secret-env")
    ] = "BINANCE_API_SECRET",
    entry_leverage: Annotated[
        int, typer.Option("--entry-leverage", min=1, max=125)
    ] = 1,
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
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
            entry_leverage=entry_leverage,
        )
    )
    typer.echo(json.dumps(asdict(result), default=str, sort_keys=True))


@app.command("run")
def run_command(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
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
    account_event_hub_url: Annotated[
        str,
        typer.Option("--account-event-hub-url"),
    ] = "ws://execution-account-live:8767",
    session_id: Annotated[str, typer.Option("--session-id")] = "live-manual",
    operator: Annotated[str, typer.Option("--operator")] = "",
    lease_owner: Annotated[str, typer.Option("--lease-owner")] = "live-worker",
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
    candle_grace_bars: Annotated[
        int,
        typer.Option("--candle-grace-bars", min=0),
    ] = 1,
    candle_grace_profit_pct: Annotated[
        str,
        typer.Option("--candle-grace-profit-pct"),
    ] = "0.0088",
    base_url: Annotated[str, typer.Option("--base-url")] = "https://fapi.binance.com",
    api_key_env: Annotated[str, typer.Option("--api-key-env")] = "BINANCE_API_KEY",
    api_secret_env: Annotated[
        str, typer.Option("--api-secret-env")
    ] = "BINANCE_API_SECRET",
    entry_leverage: Annotated[
        int, typer.Option("--entry-leverage", min=1, max=125)
    ] = 1,
    confirmation: Annotated[
        bool, typer.Option("--i-understand-this-places-real-orders")
    ] = False,
) -> None:
    if not confirmation:
        raise typer.BadParameter("--i-understand-this-places-real-orders is required")
    api_key = os.environ.get(api_key_env)
    api_secret = os.environ.get(api_secret_env)
    if not api_key or not api_secret:
        raise typer.BadParameter(f"{api_key_env} and {api_secret_env} are required")

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
            account_event_hub_url=account_event_hub_url,
            session_id=session_id,
            operator=operator,
            lease_owner=lease_owner,
            strategy_config_hash=strategy_config_hash,
            git_commit_hash=git_commit_hash,
            migration_revision=migration_revision,
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
            candle_grace_bars=candle_grace_bars,
            candle_grace_profit_pct=Decimal(candle_grace_profit_pct),
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
            entry_leverage=entry_leverage,
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
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
) -> None:
    asyncio.run(
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
    typer.echo("Live session is draining")


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
    base_url: str,
    api_key: str,
    api_secret: str,
    entry_leverage: int,
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
        )
        account_config = await client.fetch_account_config()
        plan_uses_hedge_mode = plan.position_side is not FuturesPositionSide.BOTH
        if account_config.hedge_mode != plan_uses_hedge_mode:
            raise RuntimeError("order plan position mode does not match Binance")
        machine = OrderExecutionStateMachine(
            exchange=client,
            repository=order_repository,
            submit_policy=SubmitPolicy.LIVE_SUBMIT,
            live_submit_enabled=True,
            clock=lambda: datetime.now(tz=UTC),
            serialize_commands=False,
        )
        execution_coordinator = OrderExecutionCoordinator(
            backend=machine,
            account_label=account_label,
        )
        session = LiveRolloutSession(
            repository=live_repository,
            state_machine=execution_coordinator,
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


async def _run_with_live_startup_backoff(
    run_once: Callable[[], Awaitable[LiveDaemonResult]],
) -> LiveDaemonResult:
    consecutive_failures = 0
    while True:
        try:
            return await run_once()
        except _LiveStartupRetryableError as error:
            consecutive_failures += 1
            delay = _live_startup_retry_delay(
                consecutive_failures,
                retry_after_seconds=error.retry_after_seconds,
            )
            log.warning(
                "live_startup_retry_scheduled",
                attempt=consecutive_failures,
                delay_seconds=delay,
                error_type=type(error.__cause__ or error).__name__,
                error=str(error),
            )
            await asyncio.sleep(delay)


def _is_retryable_live_startup_error(error: Exception) -> bool:
    return isinstance(error, BinanceRateLimitError) or (
        isinstance(error, RuntimeError) and str(error).startswith("live gate blocked:")
    )


def _should_auto_reacquire_live_lease(
    *,
    lease_present: bool,
    session_was_live_enabled: bool,
    draining: bool,
    gate_reasons: tuple[str, ...],
) -> bool:
    """Allow recovery only for an already-enabled, non-draining session."""
    return (
        not lease_present
        and session_was_live_enabled
        and not draining
        and gate_reasons == ("missing_active_lease",)
    )


async def _session_was_live_enabled(
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
    return latest_state == LiveSessionState.LIVE_ENABLED.value


async def _maybe_auto_reacquire_live_lease(
    *,
    factory: async_sessionmaker[AsyncSession],
    risk_repository: PostgresRiskRepository,
    gate_context: LiveGateContext,
    session_id: str,
    draining: bool,
) -> TradingLease | None:
    """Recover a lease lost during a worker/feed restart without bypassing gates."""
    gate = evaluate_live_gate(gate_context)
    if gate.approved or gate_context.active_lease is not None:
        return gate_context.active_lease
    if not await _session_was_live_enabled(factory, session_id):
        return None
    if not _should_auto_reacquire_live_lease(
        lease_present=False,
        session_was_live_enabled=True,
        draining=draining,
        gate_reasons=gate.reasons,
    ):
        return None
    now = gate_context.now
    lease = TradingLease(
        lease_id=f"lease-{uuid4()}",
        environment="live",
        account_label=gate_context.account_label,
        strategy_name=gate_context.strategy_name,
        owner=gate_context.required_lease_owner,
        state=TradingLeaseState.ACTIVE,
        acquired_at=now,
        expires_at=now + timedelta(seconds=_LIVE_AUTO_REACQUIRE_LEASE_TTL_SECONDS),
    )
    await risk_repository.acquire_lease(lease)
    log.info(
        "live_lease_auto_reacquired",
        account_label=lease.account_label,
        session_id=session_id,
        lease_id=lease.lease_id,
        lease_expires_at=lease.expires_at.isoformat(),
    )
    return lease


def _live_startup_retry_delay(
    attempt: int,
    *,
    retry_after_seconds: object,
) -> float:
    if attempt <= 0:
        raise ValueError("attempt must be positive")
    exponent = min(attempt - 1, 30)
    delay = min(
        _LIVE_STARTUP_RETRY_INITIAL_SECONDS * (2**exponent),
        _LIVE_STARTUP_RETRY_MAX_SECONDS,
    )
    if isinstance(retry_after_seconds, int | float) and not isinstance(
        retry_after_seconds, bool
    ):
        if retry_after_seconds >= 0:
            delay = max(delay, float(retry_after_seconds))
    return float(min(delay, _LIVE_STARTUP_RETRY_MAX_SECONDS))


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
    account_event_hub_url: str,
    session_id: str,
    operator: str,
    lease_owner: str,
    strategy_config_hash: str,
    git_commit_hash: str,
    migration_revision: str,
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
    candle_grace_bars: int,
    candle_grace_profit_pct: Decimal,
    base_url: str,
    api_key: str,
    api_secret: str,
    entry_leverage: int,
) -> LiveDaemonResult:
    if market_state_source not in {"hub", "postgres"}:
        raise ValueError("market_state_source must be 'hub' or 'postgres'")
    if market_state_source == "hub" and not market_state_hub_url.strip():
        raise ValueError("market_state_hub_url must not be empty in hub mode")
    if not account_event_hub_url.strip():
        raise ValueError("account_event_hub_url must not be empty")
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
    ema_candle_source: BinanceRestClosedCandle15mSource | None = None
    entry_filter_cache: LiveEntryFilterCache | None = None
    entry_filter_cache_task: asyncio.Task[None] | None = None
    live_repository: PostgresLiveRolloutRepository | None = None
    telemetry: LiveRuntimeTelemetry | None = None
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
            persist_event_types=PERSISTED_ORDER_TELEMETRY_EVENTS,
        )
        await telemetry.start()
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
        )
        account_config = await client.fetch_account_config()
        if account_config.hedge_mode != hedge_mode:
            expected = "hedge" if hedge_mode else "one-way"
            actual = "hedge" if account_config.hedge_mode else "one-way"
            raise RuntimeError(
                f"position mode mismatch: expected {expected}, got {actual}"
            )
        state_machine = OrderExecutionStateMachine(
            exchange=client,
            repository=order_repository,
            submit_policy=SubmitPolicy.LIVE_SUBMIT,
            live_submit_enabled=True,
            clock=lambda: datetime.now(tz=UTC),
            on_event=telemetry.order_event,
            on_exchange_request=telemetry.exchange_request_started,
            on_exchange_response=telemetry.exchange_response_received,
            serialize_commands=False,
        )
        execution_coordinator = OrderExecutionCoordinator(
            backend=state_machine,
            account_label=account_label,
        )
        await _reconcile_run_orders(
            order_repository=order_repository,
            state_machine=execution_coordinator,
            run_id=session_id,
        )
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
        )
        gate_context = replace(gate_context, active_lease=active_lease)
        gate = evaluate_live_gate(gate_context)
        if not gate.approved:
            raise RuntimeError(f"live gate blocked: {','.join(gate.reasons)}")
        if approval is None:
            raise RuntimeError("live approval is required")
        if active_lease is None:
            raise RuntimeError("live lease is required")

        strategy_config = _live_strategy_config()
        computed_hash = _live_strategy_config_hash(
            strategy_name,
            entry_positive_gainer_top_count=entry_positive_gainer_top_count,
            require_price_above_ema5=require_price_above_ema5,
            require_price_above_ema10=require_price_above_ema10,
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
        market_cursor: RuntimeStateCursor | None = None
        if checkpoint is not None:
            if _checkpoint_needs_market_recovery(checkpoint):
                await _restore_live_strategy_from_checkpoint(
                    strategy=strategy,
                    checkpoint=checkpoint,
                    repository=state_repository,
                    environment=market_environment,
                )
            market_cursor = RuntimeStateCursor(bucket_start=now, symbol="")
        elif market_state_source == "postgres":
            market_cursor = await _warm_live_strategy_then_start_fresh(
                strategy=strategy,
                repository=state_repository,
                environment=market_environment,
                now=now,
            )
        else:
            await _warm_live_strategy(
                strategy=strategy,
                repository=state_repository,
                environment=market_environment,
                now=now,
            )

        notional_cap, max_positions, max_loss, max_gross = live_limits_from_approval(
            approval=approval,
            risk_config=risk_config,
        )
        candle_loader = None
        ema_provider: ClosedCandleEmaProvider | None = None
        if exit_mode is PositionExitMode.CANDLE_15M:
            candle_source = BinanceRestClosedCandle15mSource(base_url)
            candle_loader = ThreadedClosedCandle15mLoader(candle_source)
        if require_price_above_ema5 or require_price_above_ema10:
            ema_candle_source = BinanceRestClosedCandle15mSource(base_url)
            ema_provider = ClosedCandleEmaProvider(ema_candle_source)

        entry_symbol_loader: Callable[[datetime], Awaitable[frozenset[str]]] | None = (
            None
        )
        if entry_positive_gainer_top_count is not None:
            universe_repository = PostgresUniverseRepository(market_factory)

            async def load_entry_symbols_from_database(
                observed_at: datetime,
            ) -> frozenset[str]:
                return await universe_repository.load_positive_gainer_symbols_at(
                    observed_at,
                    top_count=entry_positive_gainer_top_count,
                )

            entry_symbol_loader = load_entry_symbols_from_database
        if entry_symbol_loader is not None and entry_leverage is not None:
            assert client is not None
            try:
                initial_entry_symbols = await entry_symbol_loader(now)
                await client.warm_entry_leverage(initial_entry_symbols)
                log.info(
                    "live_entry_leverage_warmed",
                    symbol_count=len(initial_entry_symbols),
                    leverage=entry_leverage,
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                # A failed warmup keeps the existing per-symbol confirmation
                # fallback in place. It must be visible, but it must not make
                # exits unavailable during startup.
                log.warning(
                    "live_entry_leverage_warmup_failed",
                    error_type=type(error).__name__,
                )
        entry_filter_cache_required = (
            ema_provider is not None and entry_symbol_loader is not None
        )
        daemon_entry_symbol_loader = entry_symbol_loader
        if entry_filter_cache_required:

            async def load_entry_symbols_from_cache(
                observed_at: datetime,
            ) -> frozenset[str]:
                if entry_filter_cache is None:
                    return frozenset()
                return entry_filter_cache.symbols_for(observed_at)

            daemon_entry_symbol_loader = load_entry_symbols_from_cache

        entry_filter_context_loader: (
            Callable[
                [MarketState15s],
                Awaitable[LiveEntryFilterContext | None],
            ]
            | None
        ) = None
        if ema_provider is not None:

            async def load_entry_filter_context(
                state: MarketState15s,
            ) -> LiveEntryFilterContext | None:
                entry_price = (
                    state.last_ask_price
                    or state.midpoint
                    or state.close_price
                    or state.mark_price
                )
                if entry_price is None:
                    return None
                if entry_filter_cache is not None:
                    snapshot = entry_filter_cache.snapshot_for(
                        symbol=state.symbol,
                        observed_at=state.bucket_start,
                    )
                else:
                    # Explicit no-pool configurations retain the old
                    # behaviour. The production Top100 path always uses the
                    # background cache above.
                    snapshot = await asyncio.to_thread(
                        ema_provider.load,
                        symbol=state.symbol,
                        observed_at=state.bucket_start,
                    )
                if snapshot is None:
                    return None
                return LiveEntryFilterContext(
                    entry_price=entry_price,
                    ema5=snapshot.ema5,
                    ema10=snapshot.ema10,
                )

            entry_filter_context_loader = load_entry_filter_context
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
        latest_market_states = _LatestMarketStateCache()

        async def recover_live_lease() -> TradingLease | None:
            states = latest_market_states.for_symbols(())
            if not states:
                return None
            latest_state = states[-1]
            heartbeat_context_provider.invalidate_cache()
            recovery_context = await heartbeat_context_provider(latest_state)
            return await _maybe_auto_reacquire_live_lease(
                factory=heartbeat_factory,
                risk_repository=heartbeat_risk_repository,
                gate_context=recovery_context.gate_context,
                session_id=session_id,
                draining=await _session_is_draining(
                    heartbeat_factory,
                    session_id,
                ),
            )
        def on_lease_renewed(lease: TradingLease) -> None:
            context_provider.update_lease(lease)
            log.info(
                "live_lease_renewed",
                session_id=session_id,
                lease_id=lease.lease_id,
                lease_expires_at=lease.expires_at.isoformat(),
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
            on_renewed=on_lease_renewed,
            on_error=lambda error: log.warning(
                "live_lease_renewal_failed",
                session_id=session_id,
                error_type=type(error).__name__,
            ),
            recover=recover_live_lease,
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
            ),
            state_machine=execution_coordinator,
            context_provider=context_provider,
            telemetry=telemetry,
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
                    candle_grace_profit_pct=candle_grace_profit_pct,
                ),
                candle_loader=candle_loader,
            ),
        )
        entry_filter_cache_ready = not entry_filter_cache_required
        market_state_available = market_state_source != "hub"

        def refresh_entry_enabled() -> None:
            if draining:
                daemon.set_entry_enabled(False, reason="session_draining")
            elif not market_state_available:
                daemon.set_entry_enabled(
                    False,
                    reason="market_state_hub_connecting",
                )
            elif not entry_filter_cache_ready:
                daemon.set_entry_enabled(
                    False,
                    reason="entry_filter_cache_warming",
                )
            else:
                daemon.set_entry_enabled(
                    True,
                    reason="live_entry_prerequisites_ready",
                )

        def on_entry_filter_cache_ready(ready: bool) -> None:
            nonlocal entry_filter_cache_ready
            entry_filter_cache_ready = ready
            refresh_entry_enabled()

        if entry_filter_cache_required:
            assert ema_provider is not None
            assert entry_symbol_loader is not None
            entry_filter_cache = LiveEntryFilterCache(
                ema_provider=ema_provider,
                symbol_loader=entry_symbol_loader,
                config=EntryFilterCacheConfig(
                    refresh_interval_seconds=15.0,
                    prefetch_concurrency=_LIVE_ENTRY_FILTER_PREFETCH_CONCURRENCY,
                ),
            )
            entry_filter_cache.set_ready_callback(
                on_entry_filter_cache_ready
            )
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
        startup_phase = False
        hub_source: WebSocketMarketStateSource | None = None
        state_stream: AsyncIterable[MarketState15s]
        if market_state_source == "hub":

            def on_market_connection_change(
                available: bool,
                reason: str | None,
            ) -> None:
                nonlocal market_state_available
                market_state_available = available
                refresh_entry_enabled()

            hub_source = WebSocketMarketStateSource(
                url=market_state_hub_url,
                environment=market_environment,
                consumer_id=f"live-strategy:{session_id}",
                on_connection_change=on_market_connection_change,
            )
            state_stream = _resilient_market_state_stream(hub_source)
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
        )
        market_task = asyncio.create_task(
            daemon.run(_observe_market_states(state_stream, latest_market_states))
        )
        account_task = asyncio.create_task(
            _run_account_event_channel(
                source=account_source,
                daemon=daemon,
                latest_market_states=latest_market_states,
                order_repository=order_repository,
                state_machine=execution_coordinator,
                run_id=session_id,
                telemetry=telemetry,
            )
        )
        if entry_filter_cache is not None:
            entry_filter_cache_task = asyncio.create_task(
                entry_filter_cache.run()
            )
        lease_task = asyncio.create_task(lease_heartbeat.run())
        reconcile_task = asyncio.create_task(
            _periodic_reconcile_run_orders(
                order_repository=order_repository,
                state_machine=execution_coordinator,
                run_id=session_id,
            )
        )
        try:
            monitored_tasks: set[asyncio.Task[object]] = {
                market_task,
                account_task,
                lease_task,
                reconcile_task,
            }
            if entry_filter_cache_task is not None:
                monitored_tasks.add(entry_filter_cache_task)
            await asyncio.wait(
                monitored_tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if account_task.done():
                await account_task
                raise RuntimeError("account event channel stopped unexpectedly")
            if lease_task.done():
                await lease_task
                raise RuntimeError("live lease heartbeat stopped unexpectedly")
            if reconcile_task.done():
                await reconcile_task
                raise RuntimeError("live order reconcile task stopped unexpectedly")
            if (
                entry_filter_cache_task is not None
                and entry_filter_cache_task.done()
            ):
                await entry_filter_cache_task
                raise RuntimeError(
                    "live entry filter cache task stopped unexpectedly"
                )
            result = await market_task
        finally:
            if hub_source is not None:
                hub_source.stop()
            account_source.stop()
            if not market_task.done():
                market_task.cancel()
            if not account_task.done():
                account_task.cancel()
            if not lease_task.done():
                lease_task.cancel()
            if not reconcile_task.done():
                reconcile_task.cancel()
            if entry_filter_cache is not None:
                await entry_filter_cache.stop()
            await asyncio.gather(
                market_task,
                account_task,
                lease_task,
                reconcile_task,
                *(
                    (entry_filter_cache_task,)
                    if entry_filter_cache_task is not None
                    else ()
                ),
                return_exceptions=True,
            )
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
        if execution_coordinator is not None:
            await execution_coordinator.aclose()
        if client is not None:
            await client.aclose()
        if candle_source is not None:
            candle_source.close()
        if ema_candle_source is not None:
            ema_candle_source.close()
        if telemetry is not None:
            await telemetry.stop()
        await execution_engine.dispose()
        await market_engine.dispose()
        await observability_engine.dispose()
        await checkpoint_engine.dispose()
        if heartbeat_engine is not None:
            await heartbeat_engine.dispose()


class _LatestMarketStateCache:
    def __init__(self) -> None:
        self._states: dict[str, MarketState15s] = {}

    def observe(self, state: MarketState15s) -> None:
        previous = self._states.get(state.symbol)
        if previous is None or state.bucket_start >= previous.bucket_start:
            self._states[state.symbol] = state

    def for_symbols(self, symbols: tuple[str, ...]) -> tuple[MarketState15s, ...]:
        if symbols:
            selected = [
                self._states[symbol]
                for symbol in symbols
                if symbol in self._states
            ]
        else:
            selected = list(self._states.values())
        return tuple(
            sorted(selected, key=lambda state: (state.bucket_start, state.symbol))
        )


async def _observe_market_states(
    states: AsyncIterable[MarketState15s],
    cache: _LatestMarketStateCache,
) -> AsyncIterator[MarketState15s]:
    async for state in states:
        cache.observe(state)
        yield state


async def _resilient_market_state_stream(
    states: AsyncIterable[MarketState15s],
    *,
    retry_delay_seconds: float = 1.0,
) -> AsyncIterator[MarketState15s]:
    """Keep the live process alive while the market transport reconnects.

    The source owns connection-level retries and reports availability changes.
    This outer loop handles the longer outage threshold without terminating the
    live daemon, so the account-event exit lane can continue independently.
    """
    if retry_delay_seconds < 0:
        raise ValueError("retry_delay_seconds must not be negative")
    while True:
        try:
            async for state in states:
                yield state
        except asyncio.CancelledError:
            raise
        except MarketStateHubError as error:
            log.warning(
                "live_market_state_stream_retry",
                error_type=type(error).__name__,
                error=str(error),
                retry_delay_seconds=retry_delay_seconds,
            )
            if retry_delay_seconds > 0:
                await asyncio.sleep(retry_delay_seconds)
        else:
            return


async def _run_account_event_channel(
    *,
    source: WebSocketAccountEventSource,
    daemon: LiveStrategyDaemon,
    latest_market_states: _LatestMarketStateCache,
    order_repository: PostgresOrderRepository,
    state_machine: OrderExecutionPort,
    run_id: str,
    telemetry: LiveTelemetrySink | None = None,
) -> None:
    async for event in source:
        try:
            if telemetry is not None and event.has_fill:
                await telemetry.account_fill(
                    event,
                    occurred_at=event.received_at,
                )
            if event.event_type == "ORDER_TRADE_UPDATE" and event.client_order_id:
                await _reconcile_account_event_order(
                    event=event,
                    order_repository=order_repository,
                    state_machine=state_machine,
                    run_id=run_id,
                )
            for state in latest_market_states.for_symbols(event.symbols):
                failure = await daemon.process_account_event(state)
                if failure is not None:
                    raise RuntimeError(f"account_event_exit_failed:{failure}")
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if not _is_transient_live_runtime_error(error):
                raise
            # The account stream itself is still healthy.  Do not kill the
            # process because persistence is briefly unavailable; periodic
            # reconciliation and the next market state provide retry paths.
            log.warning(
                "live_account_event_processing_degraded",
                run_id=run_id,
                event_type=event.event_type,
                error_type=type(error).__name__,
            )


async def _reconcile_account_event_order(
    *,
    event: AccountEvent,
    order_repository: PostgresOrderRepository,
    state_machine: OrderExecutionPort,
    run_id: str,
) -> None:
    unresolved = await order_repository.load_unresolved_orders(run_id)
    for order in unresolved:
        if order.plan.client_order_id == event.client_order_id:
            await state_machine.reconcile_order(order.plan)
            return


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
    ) -> None:
        self._orders = order_repository
        self._checkpoints = checkpoint_repository

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
    ) -> PreparedOrderSubmission:
        return await self._orders.prepare_submission(
            intent=intent,
            evaluation=evaluation,
            plan=plan,
            prepared_at=prepared_at,
        )

    async def save_checkpoint(
        self,
        run_id: str,
        checkpoint: StrategyCheckpoint,
        saved_at: datetime,
    ) -> None:
        await self._checkpoints.save_checkpoint(run_id, checkpoint, saved_at)


async def _reconcile_run_orders(
    *,
    order_repository: PostgresOrderRepository,
    state_machine: OrderExecutionPort,
    run_id: str,
) -> None:
    for order in await order_repository.load_unresolved_orders(run_id):
        await state_machine.reconcile_order(order.plan)


async def _periodic_reconcile_run_orders(
    *,
    order_repository: PostgresOrderRepository,
    state_machine: OrderExecutionPort,
    run_id: str,
    interval_seconds: float = _LIVE_RECONCILE_INTERVAL_SECONDS,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    """Reconcile ordinary unresolved orders off the market-state hot path.

    Account WebSocket events still trigger an immediate reconcile for the
    affected client order.  This loop is the eventual-consistency safety net
    for orders without a recent account event; a transient REST/DB failure is
    logged and retried on the next interval rather than stopping market
    processing.
    """

    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    while True:
        await sleep(interval_seconds)
        try:
            await _reconcile_run_orders(
                order_repository=order_repository,
                state_machine=state_machine,
                run_id=run_id,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception(
                "live_periodic_order_reconcile_failed",
                run_id=run_id,
                interval_seconds=interval_seconds,
            )


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
) -> None:
    if await _has_matching_shadow_session(
        factory,
        strategy_name=strategy_name,
        strategy_config_hash=strategy_config_hash,
    ):
        return
    log.warning(
        "live_shadow_preflight_missing",
        account_label=account_label,
        session_id=session_id,
        strategy_name=strategy_name,
        strategy_config_hash=strategy_config_hash,
    )


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


async def _warm_live_strategy(
    *,
    strategy: LiveRuntimeStrategy,
    repository: PostgresRuntimeMarketStateRepository,
    environment: str,
    now: datetime,
) -> RuntimeStateCursor:
    warmup_seconds = _live_warmup_seconds(strategy)
    cursor = RuntimeStateCursor(
        bucket_start=now - timedelta(seconds=warmup_seconds),
        symbol="",
    )
    warmed_state_count = 0
    while warmed_state_count < _LIVE_WARMUP_STATE_LIMIT:
        batch = await repository.load_after(
            environment=environment,
            cursor=cursor,
            limit=min(
                _LIVE_WARMUP_BATCH_SIZE,
                _LIVE_WARMUP_STATE_LIMIT - warmed_state_count,
            ),
        )
        if not batch:
            break
        for state in batch:
            strategy.on_market_state(state)
            cursor = RuntimeStateCursor(
                bucket_start=state.bucket_start,
                symbol=state.symbol,
            )
            warmed_state_count += 1
        if len(batch) < _LIVE_WARMUP_BATCH_SIZE:
            break
    return cursor


async def _restore_live_strategy_from_checkpoint(
    *,
    strategy: LiveRuntimeStrategy,
    checkpoint: StrategyCheckpoint,
    repository: PostgresRuntimeMarketStateRepository,
    environment: str,
) -> None:
    warm_market_state = getattr(strategy, "warm_market_state", None)
    if not callable(warm_market_state):
        raise RuntimeError(
            "strategy does not support compact checkpoint recovery"
        )
    states = await repository.load_recovery_window(
        environment=environment,
        last_processed_at_by_symbol=checkpoint.last_processed_at_by_symbol,
        lookback_seconds=_live_warmup_seconds(strategy),
        limit=_LIVE_WARMUP_STATE_LIMIT,
    )
    for state in states:
        warm_market_state(state)
    compact_checkpoint = strategy.checkpoint(
        include_market_state_buffers=False
    )
    log.info(
        "live_strategy_checkpoint_recovered",
        environment=environment,
        state_count=len(states),
        symbol_count=len(compact_checkpoint.warmup_buckets_by_symbol),
        expected_symbol_count=len(checkpoint.last_processed_at_by_symbol),
    )


def _checkpoint_needs_market_recovery(checkpoint: StrategyCheckpoint) -> bool:
    return not any(
        key in checkpoint.payload
        for key in ("market_state_buffers", "signal_buffers")
    )


def _live_warmup_seconds(strategy: LiveRuntimeStrategy) -> int:
    """Return the minimum history window sufficient for strategy buffers."""
    required_data = getattr(strategy, "required_data", None)
    warmup_buckets = 0
    if callable(required_data):
        warmup_buckets = int(getattr(required_data(), "warmup_buckets", 0))
    buffer_seconds = (warmup_buckets + 16) * 15
    return max(_LIVE_MIN_WARMUP_SECONDS, buffer_seconds)


async def _warm_live_strategy_then_start_fresh(
    *,
    strategy: LiveRuntimeStrategy,
    repository: PostgresRuntimeMarketStateRepository,
    environment: str,
    now: datetime,
) -> RuntimeStateCursor:
    """Warm historical state, then continue from the current live boundary."""
    await _warm_live_strategy(
        strategy=strategy,
        repository=repository,
        environment=environment,
        now=now,
    )
    return RuntimeStateCursor(bucket_start=datetime.now(tz=UTC), symbol="")


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
    )
    if not plan.quantized:
        raise typer.BadParameter("order plan must be quantized")
    return plan


def _live_strategy_config() -> dict[str, object]:
    return {
        "candidate_notional": Decimal("100"),
        "candidate_ttl_buckets": 4,
        # B1 must not suppress a same-symbol signal for two 15-second buckets.
        "cooldown_buckets": 0,
    }


def _live_strategy_config_hash(
    strategy_name: str,
    *,
    entry_positive_gainer_top_count: int | None = _LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT,
    require_price_above_ema5: bool = _LIVE_ENTRY_PRICE_ABOVE_EMA5,
    require_price_above_ema10: bool = _LIVE_ENTRY_PRICE_ABOVE_EMA10,
) -> str:
    if (
        entry_positive_gainer_top_count is not None
        and entry_positive_gainer_top_count <= 0
    ):
        raise ValueError("entry_positive_gainer_top_count must be positive")
    return deterministic_config_hash(
        {
            "strategy": build_runtime_config(
                strategy_name,
                config=_live_strategy_config(),
            ),
            "entry_filter": {
                "entry_positive_gainer_top_count": entry_positive_gainer_top_count,
                "require_price_above_ema5": require_price_above_ema5,
                "require_price_above_ema10": require_price_above_ema10,
            },
        }
    )


async def _prepare_live_risk_gates(
    *,
    database_url: str,
    account_label: str,
    strategy_name: str,
    lease_owner: str,
    lease_ttl_seconds: int,
    max_order_notional: Decimal | None,
    max_gross_notional: Decimal | None,
    max_daily_loss: Decimal | None,
    max_open_positions: int | None,
    entry_positive_gainer_top_count: int | None,
    require_price_above_ema5: bool,
    require_price_above_ema10: bool,
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
            entry_positive_gainer_top_count=entry_positive_gainer_top_count,
            require_price_above_ema5=require_price_above_ema5,
            require_price_above_ema10=require_price_above_ema10,
        ),
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
        runtime_strategy_config_hash = _live_strategy_config_hash(strategy_name)
        return {
            "approval_present": approval is not None,
            "lease_present": lease is not None,
            "account_state": (
                await _latest_account_state(factory, account_label)
            ).value,
            "unresolved_order_count": len(unresolved),
            "risk_config_hash": risk_config.config_hash,
            "runtime_strategy_config_hash": runtime_strategy_config_hash,
        }
    finally:
        await engine.dispose()


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
) -> None:
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


def _execution_database_url(value: str | None) -> str:
    return _resolve_database_url(value, "CML_EXECUTION_DATABASE_URL")


def _market_database_url(value: str | None) -> str:
    return _resolve_database_url(value, "CML_MARKET_DATABASE_URL")


def _observability_database_url(value: str | None) -> str:
    return _resolve_database_url(value, "CML_OBSERVABILITY_DATABASE_URL")


def _resolve_database_url(value: str | None, plane_env_var: str) -> str:
    resolved = value or os.environ.get(plane_env_var) or os.environ.get(
        "CML_DATABASE_URL"
    )
    if not resolved:
        raise typer.BadParameter(
            f"--database-url or {plane_env_var} or CML_DATABASE_URL is required"
        )
    return resolved


def _database_url(value: str | None) -> str:
    """Resolve the execution plane for legacy live CLI commands."""

    return _execution_database_url(value)
