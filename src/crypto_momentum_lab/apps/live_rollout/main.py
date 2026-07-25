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
from crypto_momentum_lab.domain.execution import OrderExecutionPlan
from crypto_momentum_lab.domain.live_rollout import (
    LiveOperatorApproval,
    LiveSessionState,
    LiveSessionTransition,
)
from crypto_momentum_lab.domain.risk import RiskEvaluation
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
    LiveStrategyDaemon,
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
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)
from crypto_momentum_lab.risk.gateway import RiskGateway
from crypto_momentum_lab.strategy_runner.registry import build_runtime_strategy

app = typer.Typer(no_args_is_help=True)


@app.callback()
def live_rollout_app() -> None:
    """Operate explicitly approved small-capital live sessions."""


@app.command("approve")
def approve_command(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "compression_breakout",
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
    strategy: Annotated[str, typer.Option("--strategy")] = "compression_breakout",
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
        )
    )
    typer.echo(json.dumps(asdict(result), default=str, sort_keys=True))


@app.command("run")
def run_command(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "compression_breakout",
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
    base_url: Annotated[str, typer.Option("--base-url")] = "https://fapi.binance.com",
    api_key_env: Annotated[str, typer.Option("--api-key-env")] = "BINANCE_API_KEY",
    api_secret_env: Annotated[
        str, typer.Option("--api-secret-env")
    ] = "BINANCE_API_SECRET",
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
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
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
) -> LiveSessionResult:
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
        unresolved = await order_repository.load_unresolved_orders()
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
        )
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
    base_url: str,
    api_key: str,
    api_secret: str,
) -> LiveDaemonResult:
    now = datetime.now(tz=UTC)
    engine = create_async_database_engine(database_url)
    client: BinanceUsdMTradeClient | None = None
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
        unresolved = await order_repository.load_unresolved_orders()
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

        strategy_config: dict[str, object] = {
            "candidate_notional": min(
                Decimal("100"),
                risk_config.max_order_notional,
            ),
            "candidate_ttl_buckets": 4,
        }
        computed_hash = deterministic_config_hash(strategy_config)
        if computed_hash != strategy_config_hash:
            raise RuntimeError(
                "strategy config hash does not match the live runtime configuration"
            )
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
                run_mode=RunMode.PAPER,
                code_commit=git_commit_hash,
                created_at=now,
                source_paths=("postgres-runtime-states:live",),
            ),
        )
        checkpoint = await checkpoint_repository.load_checkpoint(session_id)
        if checkpoint is not None:
            strategy.restore_checkpoint(checkpoint)

        client = BinanceUsdMTradeClient(
            api_key=api_key,
            api_secret=api_secret,
            environment="live",
            account_label=account_label,
            live_submit_enabled=True,
            base_url=base_url,
        )
        state_machine = OrderExecutionStateMachine(
            exchange=client,
            repository=order_repository,
            submit_policy=SubmitPolicy.LIVE_SUBMIT,
            live_submit_enabled=True,
            clock=lambda: datetime.now(tz=UTC),
        )
        notional_cap, max_positions, max_loss, max_gross = (
            live_limits_from_approval(
                approval=approval,
                risk_config=risk_config,
            )
        )
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
            ),
        )
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
                repository=PostgresRuntimeMarketStateRepository(factory),
                environment="live",
                max_runtime_seconds=max_runtime_seconds,
                poll_interval_seconds=poll_interval_seconds,
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
        quantized=bool(payload.get("quantized", False)),
    )
    if not plan.quantized:
        raise typer.BadParameter("order plan must be quantized")
    return plan


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
        runtime_strategy_config_hash = deterministic_config_hash(
            {
                "candidate_notional": min(
                    Decimal("100"), risk_config.max_order_notional
                ),
                "candidate_ttl_buckets": 4,
            }
        )
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
