import asyncio
import json
import os
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Annotated
from uuid import uuid4

import typer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

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
from crypto_momentum_lab.execution_account.binance import BinanceUsdMTradeClient
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionStateMachine,
    SubmitPolicy,
)
from crypto_momentum_lab.live_rollout.daemon import (
    LiveDaemonConfig,
    LiveDaemonResult,
    LiveRuntimeStrategy,
    LiveStrategyDaemon,
)
from crypto_momentum_lab.live_rollout.exits import (
    LiveExitConfig,
    LiveExitManager,
    ThreadedClosedCandle15mLoader,
)
from crypto_momentum_lab.live_rollout.gates import LiveGateContext, evaluate_live_gate
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
from crypto_momentum_lab.persistence.postgres.risk_repository import (
    PostgresRiskRepository,
)
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    PostgresRuntimeMarketStateRepository,
    RuntimeStateCursor,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)
from crypto_momentum_lab.risk.gateway import RiskGateway
from crypto_momentum_lab.strategy_runner.candle_source import (
    BinanceRestClosedCandle15mSource,
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
_PREPARE_CONFIRMATION = "PREPARE LIVE RISK GATES"
_LIVE_MIN_WARMUP_SECONDS = 60
_LIVE_WARMUP_STATE_LIMIT = 100_000
_LIVE_WARMUP_BATCH_SIZE = 100


@app.callback()
def live_rollout_app() -> None:
    """Operate explicitly approved small-capital live sessions."""


@app.command("strategy-config-hash")
def strategy_config_hash_command(
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
) -> None:
    typer.echo(_live_strategy_config_hash(strategy))


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
    ] = "100",
    max_gross_notional: Annotated[
        str,
        typer.Option("--max-gross-notional"),
    ] = "300",
    max_daily_loss: Annotated[
        str,
        typer.Option("--max-daily-loss"),
    ] = "25",
    max_open_positions: Annotated[
        int,
        typer.Option("--max-open-positions", min=1),
    ] = 3,
    state_stale_after_seconds: Annotated[
        float,
        typer.Option("--state-stale-after-seconds", min=1),
    ] = 30.0,
    confirmation: Annotated[str, typer.Option("--confirmation")] = "",
) -> None:
    if confirmation != _PREPARE_CONFIRMATION:
        raise typer.BadParameter(
            f"--confirmation must equal '{_PREPARE_CONFIRMATION}'"
        )
    payload = asyncio.run(
        _prepare_live_risk_gates(
            database_url=_database_url(database_url),
            account_label=account_label,
            strategy_name=strategy,
            lease_owner=lease_owner,
            lease_ttl_seconds=lease_ttl_seconds,
            max_order_notional=Decimal(max_order_notional),
            max_gross_notional=Decimal(max_gross_notional),
            max_daily_loss=Decimal(max_daily_loss),
            max_open_positions=max_open_positions,
            state_stale_after_seconds=state_stale_after_seconds,
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
    notional_cap: Annotated[str, typer.Option("--notional-cap")] = "25",
    max_open_positions: Annotated[
        int, typer.Option("--max-open-positions", min=1)
    ] = 1,
    max_daily_loss: Annotated[str, typer.Option("--max-daily-loss")] = "10",
    approver: Annotated[str, typer.Option("--approver")] = "",
    confirmation: Annotated[str, typer.Option("--confirmation")] = "",
    expires_in_minutes: Annotated[
        int, typer.Option("--expires-in-minutes", min=1)
    ] = 60,
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
        approved_notional_cap=Decimal(notional_cap),
        approved_max_open_positions=max_open_positions,
        approved_max_daily_loss=Decimal(max_daily_loss),
        approver_name=approver,
        approval_text=confirmation,
        expires_at=now + timedelta(minutes=expires_in_minutes),
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
        raise typer.BadParameter(
            "--i-understand-this-places-real-orders is required"
        )
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
    ] = 1.0,
    state_stale_after_seconds: Annotated[
        float, typer.Option("--state-stale-after-seconds", min=1)
    ] = 30.0,
    checkpoint_every_states: Annotated[
        int, typer.Option("--checkpoint-every-states", min=1)
    ] = 100,
    max_spread: Annotated[str, typer.Option("--max-spread")] = "5",
    cooldown_seconds: Annotated[
        int, typer.Option("--cooldown-seconds", min=0)
    ] = 300,
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
    candle_grace_bars: Annotated[
        int,
        typer.Option("--candle-grace-bars", min=0),
    ] = 1,
    candle_grace_profit_pct: Annotated[
        str,
        typer.Option("--candle-grace-profit-pct"),
    ] = "0.0088",
    max_holding_seconds: Annotated[
        int,
        typer.Option("--max-holding-seconds", min=1),
    ] = 1200,
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
        raise typer.BadParameter(
            "--i-understand-this-places-real-orders is required"
        )
    api_key = os.environ.get(api_key_env)
    api_secret = os.environ.get(api_secret_env)
    if not api_key or not api_secret:
        raise typer.BadParameter(f"{api_key_env} and {api_secret_env} are required")
    result = asyncio.run(
        _run_live_daemon(
            database_url=_database_url(database_url),
            account_label=account_label,
            strategy_name=strategy,
            market_environment=market_environment,
            session_id=session_id,
            operator=operator,
            lease_owner=lease_owner,
            strategy_config_hash=strategy_config_hash,
            git_commit_hash=git_commit_hash,
            migration_revision=migration_revision,
            max_runtime_seconds=max_runtime_seconds,
            poll_interval_seconds=poll_interval_seconds,
            state_stale_after_seconds=state_stale_after_seconds,
            checkpoint_every_states=checkpoint_every_states,
            max_spread=Decimal(max_spread),
            cooldown_seconds=cooldown_seconds,
            hedge_mode=hedge_mode,
            exit_mode=exit_mode,
            take_profit_pct=Decimal(take_profit_pct),
            stop_loss_pct=Decimal(stop_loss_pct),
            max_holding_seconds=max_holding_seconds,
            entry_long_only=entry_long_only,
            candle_grace_bars=candle_grace_bars,
            candle_grace_profit_pct=Decimal(candle_grace_profit_pct),
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
            entry_leverage=entry_leverage,
        )
    )
    typer.echo(json.dumps(asdict(result), default=str, sort_keys=True))


@app.command("status")
def status_command(
    session_id: Annotated[str, typer.Option("--session-id")],
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
) -> None:
    transition = asyncio.run(
        _load_transition(_database_url(database_url), session_id)
    )
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
    transition = asyncio.run(
        _load_transition(_database_url(database_url), session_id)
    )
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
    engine = create_async_database_engine(database_url)
    client: BinanceUsdMTradeClient | None = None
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
            active_halts=await risk_repository.load_active_halts(
                "live", account_label
            ),
            unresolved_order_states=tuple(item.state for item in unresolved),
        )
        gate = evaluate_live_gate(context)
        if not gate.approved:
            raise RuntimeError(f"live gate blocked: {','.join(gate.reasons)}")
        desired_notional = await _approved_intent_notional(factory, plan.intent_id)
        if desired_notional is None:
            raise RuntimeError("live plan has no persisted approved intent notional")
        if desired_notional > risk_config.max_order_notional:
            raise RuntimeError("live plan exceeds current risk notional cap")
        if approval is None or desired_notional > approval.approved_notional_cap:
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
        )
        session = LiveRolloutSession(
            repository=live_repository,
            state_machine=machine,
            config=LiveSessionConfig(
                session_id=session_id,
                operator=operator,
                strategy_config_hash=strategy_config_hash,
                risk_config_hash=risk_config.config_hash,
            ),
            clock=lambda: datetime.now(tz=UTC),
        )

        async def shadow_preflight() -> bool:
            async with factory() as database_session:
                completed_shadow = await database_session.scalar(
                    select(ShadowSessionRow.run_id)
                    .where(
                        ShadowSessionRow.strategy_name == strategy_name,
                        ShadowSessionRow.strategy_config_hash
                        == strategy_config_hash,
                        ShadowSessionRow.state == "completed",
                        ShadowSessionRow.ended_at
                        >= now - timedelta(hours=24),
                    )
                    .order_by(ShadowSessionRow.ended_at.desc())
                    .limit(1)
                )
            return completed_shadow is not None

        return await session.run_one(
            gate_context=context,
            shadow_preflight=shadow_preflight,
            plan=plan,
        )
    finally:
        if client is not None:
            await client.aclose()
        await engine.dispose()


async def _run_live_daemon(
    *,
    database_url: str,
    account_label: str,
    strategy_name: str,
    market_environment: str,
    session_id: str,
    operator: str,
    lease_owner: str,
    strategy_config_hash: str,
    git_commit_hash: str,
    migration_revision: str,
    max_runtime_seconds: int,
    poll_interval_seconds: float,
    state_stale_after_seconds: float,
    checkpoint_every_states: int,
    max_spread: Decimal,
    cooldown_seconds: int,
    hedge_mode: bool,
    exit_mode: PositionExitMode,
    take_profit_pct: Decimal,
    stop_loss_pct: Decimal,
    max_holding_seconds: int,
    entry_long_only: bool,
    candle_grace_bars: int,
    candle_grace_profit_pct: Decimal,
    base_url: str,
    api_key: str,
    api_secret: str,
    entry_leverage: int,
) -> LiveDaemonResult:
    now = datetime.now(tz=UTC)
    engine = create_async_database_engine(database_url)
    client: BinanceUsdMTradeClient | None = None
    candle_source: BinanceRestClosedCandle15mSource | None = None
    live_repository: PostgresLiveRolloutRepository | None = None
    risk_config_hash = ""
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        live_repository = PostgresLiveRolloutRepository(factory)
        risk_repository = PostgresRiskRepository(factory)
        order_repository = PostgresOrderRepository(factory)
        checkpoint_repository = PostgresPaperDaemonRepository(factory)
        risk_config = await _latest_risk_config(factory, account_label)
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
        )
        await _reconcile_run_orders(
            order_repository=order_repository,
            state_machine=state_machine,
            run_id=session_id,
        )
        draining = await _session_is_draining(factory, session_id)
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
            active_lease=await risk_repository.load_active_lease(
                "live", account_label, now
            ),
            risk_config=risk_config,
            approval=approval,
            account_state=await _latest_account_state(factory, account_label),
            active_halts=await risk_repository.load_active_halts(
                "live", account_label
            ),
            unresolved_order_states=tuple(item.state for item in unresolved),
        )
        gate = evaluate_live_gate(gate_context)
        if not gate.approved:
            raise RuntimeError(f"live gate blocked: {','.join(gate.reasons)}")
        if approval is None:
            raise RuntimeError("live approval is required")

        strategy_config = _live_strategy_config()
        runtime_config = build_runtime_config(strategy_name, config=strategy_config)
        computed_hash = deterministic_config_hash(runtime_config)
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
        if not await _has_recent_shadow_session(
            factory,
            strategy_name=strategy_name,
            strategy_config_hash=strategy_config_hash,
            now=now,
        ):
            raise RuntimeError("shadow preflight failed")

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
                source_paths=(f"postgres-runtime-states:{market_environment}",),
            ),
        )
        checkpoint = await checkpoint_repository.load_checkpoint(session_id)
        if checkpoint is not None:
            strategy.restore_checkpoint(checkpoint)
        state_repository = PostgresRuntimeMarketStateRepository(factory)
        market_cursor = (
            RuntimeStateCursor(bucket_start=now, symbol="")
            if checkpoint is not None
            else await _warm_live_strategy_then_start_fresh(
                strategy=strategy,
                repository=state_repository,
                environment=market_environment,
                now=now,
                stale_after_seconds=state_stale_after_seconds,
            )
        )

        notional_cap, max_positions, max_loss, max_gross = (
            live_limits_from_approval(
                approval=approval,
                risk_config=risk_config,
            )
        )
        candle_loader = None
        if exit_mode is PositionExitMode.CANDLE_15M:
            candle_source = BinanceRestClosedCandle15mSource(base_url)
            candle_loader = ThreadedClosedCandle15mLoader(candle_source)
        daemon = LiveStrategyDaemon(
            strategy=strategy,
            risk_gateway=RiskGateway(),
            limits=FixedLiveLimits(
                notional_cap=notional_cap,
                max_open_positions=max_positions,
                max_daily_loss=max_loss,
                max_gross_exposure=max_gross,
                max_spread=max_spread,
                cooldown_seconds=cooldown_seconds,
                max_account_age_seconds=state_stale_after_seconds,
                max_market_age_seconds=state_stale_after_seconds,
            ),
            repository=_LiveDaemonRepositoryAdapter(
                order_repository,
                checkpoint_repository,
            ),
            state_machine=state_machine,
            context_provider=PostgresLiveContextProvider(
                session_factory=factory,
                account_label=account_label,
                run_id=session_id,
                strategy_name=strategy_name,
                strategy_config_hash=strategy_config_hash,
                git_commit_hash=git_commit_hash,
                migration_revision=migration_revision,
                lease_owner=lease_owner,
                approval_id=approval.approval_id,
            ),
            config=LiveDaemonConfig(
                run_id=session_id,
                max_market_state_age_seconds=state_stale_after_seconds,
                resize_tolerance=Decimal("0.10"),
                checkpoint_every_states=checkpoint_every_states,
                hedge_mode=hedge_mode,
                entry_long_only=entry_long_only,
                skip_stale_until_fresh=True,
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
                        max_holding_seconds=max_holding_seconds,
                        mode=exit_mode,
                    ),
                    candle_grace_bars=candle_grace_bars,
                    candle_grace_profit_pct=candle_grace_profit_pct,
                ),
                candle_loader=candle_loader,
            ),
            reconcile_orders=lambda: _reconcile_run_orders(
                order_repository=order_repository,
                state_machine=state_machine,
                run_id=session_id,
            ),
        )
        if not draining:
            await _record_transition(
                live_repository,
                session_id=session_id,
                operator=operator,
                strategy_config_hash=strategy_config_hash,
                risk_config_hash=risk_config_hash,
                state=LiveSessionState.LIVE_ENABLED,
            )
        result = await daemon.run(
            poll_live_market_states(
                repository=state_repository,
                environment=market_environment,
                max_runtime_seconds=max_runtime_seconds,
                poll_interval_seconds=poll_interval_seconds,
                cursor=market_cursor,
            )
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
        if client is not None:
            await client.aclose()
        if candle_source is not None:
            candle_source.close()
        await engine.dispose()


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
    state_machine: OrderExecutionStateMachine,
    run_id: str,
) -> None:
    for order in await order_repository.load_unresolved_orders(run_id):
        await state_machine.reconcile_order(order.plan)


async def _has_recent_shadow_session(
    factory: async_sessionmaker[AsyncSession],
    *,
    strategy_name: str,
    strategy_config_hash: str,
    now: datetime,
) -> bool:
    async with factory() as database_session:
        completed_shadow = await database_session.scalar(
            select(ShadowSessionRow.run_id)
            .where(
                ShadowSessionRow.strategy_name == strategy_name,
                ShadowSessionRow.strategy_config_hash == strategy_config_hash,
                ShadowSessionRow.state == "completed",
                ShadowSessionRow.ended_at >= now - timedelta(hours=24),
            )
            .order_by(ShadowSessionRow.ended_at.desc())
            .limit(1)
        )
    return completed_shadow is not None


async def _session_is_draining(
    factory: async_sessionmaker[AsyncSession],
    session_id: str,
) -> bool:
    async with factory() as database_session:
        control_state = await database_session.scalar(
            select(LiveSessionTransitionRow.state)
            .where(
                LiveSessionTransitionRow.session_id == session_id,
                LiveSessionTransitionRow.state.in_(
                    (
                        LiveSessionState.LIVE_ENABLED.value,
                        LiveSessionState.DRAINING.value,
                    )
                ),
            )
            .order_by(LiveSessionTransitionRow.occurred_at.desc())
            .limit(1)
        )
    return control_state == LiveSessionState.DRAINING.value


async def _warm_live_strategy(
    *,
    strategy: LiveRuntimeStrategy,
    repository: PostgresRuntimeMarketStateRepository,
    environment: str,
    now: datetime,
    stale_after_seconds: float,
) -> RuntimeStateCursor:
    warmup_seconds = _live_warmup_seconds(strategy)
    cursor = RuntimeStateCursor(
        bucket_start=now - timedelta(seconds=warmup_seconds),
        symbol="",
    )
    fresh_after = now - timedelta(seconds=stale_after_seconds)
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
        reached_fresh = False
        for state in batch:
            if state.bucket_end >= fresh_after:
                reached_fresh = True
                break
            strategy.on_market_state(state)
            cursor = RuntimeStateCursor(
                bucket_start=state.bucket_start,
                symbol=state.symbol,
            )
            warmed_state_count += 1
        if reached_fresh or len(batch) < _LIVE_WARMUP_BATCH_SIZE:
            break
    return cursor


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
    stale_after_seconds: float,
) -> RuntimeStateCursor:
    """Warm historical state, then wait for a state produced after warmup.

    Warmup can take longer than the stale-data budget on a cold container.  A
    cursor at the last historical row would therefore replay a row that is
    already stale by the time the daemon starts and cause an immediate halt.
    Historical rows older than the cutoff are still applied to the strategy;
    rows at or after the cutoff are intentionally left for the live stream.
    Starting the poll cursor at the post-warmup wall clock avoids replaying
    that moving boundary while retaining the daemon's stale-data safety gate.
    """
    await _warm_live_strategy(
        strategy=strategy,
        repository=repository,
        environment=environment,
        now=now,
        stale_after_seconds=stale_after_seconds,
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
    }


def _live_strategy_config_hash(strategy_name: str) -> str:
    return deterministic_config_hash(
        build_runtime_config(
            strategy_name,
            config=_live_strategy_config(),
        )
    )


async def _prepare_live_risk_gates(
    *,
    database_url: str,
    account_label: str,
    strategy_name: str,
    lease_owner: str,
    lease_ttl_seconds: int,
    max_order_notional: Decimal,
    max_gross_notional: Decimal,
    max_daily_loss: Decimal,
    max_open_positions: int,
    state_stale_after_seconds: float,
) -> dict[str, str]:
    now = datetime.now(tz=UTC)
    risk_config = RiskConfigSnapshot(
        environment="live",
        account_label=account_label,
        max_order_notional=max_order_notional,
        max_gross_notional=max_gross_notional,
        max_daily_loss=max_daily_loss,
        max_open_positions=max_open_positions,
        max_market_state_age_seconds=state_stale_after_seconds,
        max_account_state_age_seconds=state_stale_after_seconds,
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
    engine = create_async_database_engine(database_url)
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
        "strategy_config_hash": _live_strategy_config_hash(strategy_name),
    }


async def _save_approval(
    database_url: str,
    approval: LiveOperatorApproval,
) -> None:
    engine = create_async_database_engine(database_url)
    try:
        repository = PostgresLiveRolloutRepository(
            async_sessionmaker(engine, expire_on_commit=False)
        )
        await repository.save_approval(approval)
    finally:
        await engine.dispose()


async def _preflight_summary(
    database_url: str,
    account_label: str,
    strategy_name: str,
) -> dict[str, object]:
    now = datetime.now(tz=UTC)
    engine = create_async_database_engine(database_url)
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
    engine = create_async_database_engine(database_url)
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
    engine = create_async_database_engine(database_url)
    try:
        await PostgresLiveRolloutRepository(
            async_sessionmaker(engine, expire_on_commit=False)
        ).save_transition(transition)
    finally:
        await engine.dispose()


def _database_url(value: str | None) -> str:
    resolved = value or os.environ.get("CML_DATABASE_URL")
    if not resolved:
        raise typer.BadParameter("--database-url or CML_DATABASE_URL is required")
    return resolved
