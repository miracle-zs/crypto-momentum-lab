"""One-shot live plan execution around the shared session and order seams."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution import (
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.execution_account.binance import BinanceUsdMTradeClient
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionStateMachine,
    SubmitPolicy,
)
from crypto_momentum_lab.live_rollout.entry_expectations import (
    LiveEntryExpectationRegistrar,
)
from crypto_momentum_lab.live_rollout.gates import LiveGateContext, evaluate_live_gate
from crypto_momentum_lab.live_rollout.resource_lifecycle import (
    LiveResourceLifecycle,
)
from crypto_momentum_lab.live_rollout.session import (
    LiveRolloutSession,
    LiveSessionConfig,
    LiveSessionResult,
)
from crypto_momentum_lab.live_rollout.submission_fence import LiveSubmissionFence
from crypto_momentum_lab.persistence.postgres.live_rollout_repository import (
    PostgresLiveRolloutRepository,
)
from crypto_momentum_lab.persistence.postgres.order_repository import (
    PostgresOrderRepository,
)
from crypto_momentum_lab.persistence.postgres.risk_repository import (
    PostgresRiskRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_execution_database_engine,
)

LatestRiskConfigLoader = Callable[
    [async_sessionmaker[AsyncSession], str],
    Awaitable[Any],
]
LatestAccountStateLoader = Callable[
    [async_sessionmaker[AsyncSession], str],
    Awaitable[Any],
]
ApprovedIntentNotionalLoader = Callable[
    [async_sessionmaker[AsyncSession], str],
    Awaitable[Decimal | None],
]
SessionDrainingLoader = Callable[
    [async_sessionmaker[AsyncSession], str],
    Awaitable[bool],
]
ShadowPreflightWarning = Callable[..., Awaitable[None]]


async def run_live_plan(
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
    load_latest_risk_config: LatestRiskConfigLoader,
    load_latest_account_state: LatestAccountStateLoader,
    load_approved_intent_notional: ApprovedIntentNotionalLoader,
    session_is_draining: SessionDrainingLoader,
    warn_if_shadow_preflight_missing: ShadowPreflightWarning,
) -> LiveSessionResult:
    """Validate and execute one approved plan with live safety barriers."""

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
        risk_config = await load_latest_risk_config(factory, account_label)
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
            account_state=await load_latest_account_state(factory, account_label),
            active_halts=await risk_repository.load_active_halts("live", account_label),
            unresolved_order_states=tuple(item.state for item in unresolved),
        )
        gate = evaluate_live_gate(context)
        if not gate.approved:
            raise RuntimeError(f"live gate blocked: {','.join(gate.reasons)}")
        desired_notional = await load_approved_intent_notional(
            factory,
            plan.intent_id,
        )
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
            is_draining=lambda: session_is_draining(factory, session_id),
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
            await warn_if_shadow_preflight_missing(
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
        await LiveResourceLifecycle(
            entry_runtime=None,
            entry_order_lifecycle=None,
            execution_coordinator=execution_coordinator,
            client=client,
            closed_candle_feed=None,
            candle_source=None,
            ema_candle_source=None,
            signal_recorder=None,
            telemetry=None,
            volume_cache=None,
            volume_rest_client=None,
            execution_engine=engine,
            market_engine=None,
            observability_engine=None,
            checkpoint_engine=None,
            heartbeat_engine=None,
            health=None,
        ).close()


__all__ = ["run_live_plan"]
