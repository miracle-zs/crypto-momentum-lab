"""One-shot live plan execution around the shared session and order seams."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import async_sessionmaker

import crypto_momentum_lab.persistence.postgres.runtime_context as runtime_context
from crypto_momentum_lab.domain.execution.order_state import (
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.execution_account.binance.client import (
    BinanceUsdMTradeClient,
)
from crypto_momentum_lab.execution_account.hub import (
    WebSocketAccountPositionExpectationPublisher,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionStateMachine,
)
from crypto_momentum_lab.live_rollout.entry_expectations import (
    LiveEntryExpectationRegistrar,
)
from crypto_momentum_lab.live_rollout.gates import (
    LiveGateContext,
    evaluate_live_gate,
    has_entry_order_conflict,
)
from crypto_momentum_lab.live_rollout.resource_lifecycle import (
    LiveResourceLifecycle,
)
from crypto_momentum_lab.live_rollout.session import (
    LiveRolloutSession,
    LiveSessionConfig,
    LiveSessionResult,
)
from crypto_momentum_lab.persistence.postgres.live_rollout_repository import (
    PostgresLiveRolloutRepository,
)
from crypto_momentum_lab.persistence.postgres.order_event_repository import (
    PostgresOrderEventRepository,
)
from crypto_momentum_lab.persistence.postgres.order_plan_repository import (
    PostgresOrderPlanRepository,
)
from crypto_momentum_lab.persistence.postgres.order_read_repository import (
    PostgresOrderReadRepository,
)
from crypto_momentum_lab.persistence.postgres.position_reservation_repository import (
    AsyncPostgresPositionReservationRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_execution_database_engine,
)


async def run_live_plan(
    *,
    database_url: str,
    account_label: str,
    strategy_name: str,
    session_id: str,
    operator: str,
    strategy_config_hash: str,
    plan: OrderExecutionPlan,
    account_event_hub_url: str,
    base_url: str,
    api_key: str,
    api_secret: str,
    entry_leverage: int,
    margin_type: str = "CROSSED",
) -> LiveSessionResult:
    """Validate and execute one approved plan with live safety barriers."""

    if plan.run_id != session_id:
        raise ValueError("order plan run_id must match session_id")
    engine = create_execution_database_engine(database_url)
    client: BinanceUsdMTradeClient | None = None
    execution_coordinator: OrderExecutionCoordinator | None = None
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        live_repository = PostgresLiveRolloutRepository(factory)
        order_repository = PostgresOrderPlanRepository(factory)
        order_read_repository = PostgresOrderReadRepository(factory)
        order_event_repository = PostgresOrderEventRepository(factory)
        risk_config = await runtime_context.load_latest_risk_config(factory, account_label)
        unresolved = await order_read_repository.load_unresolved_orders(session_id)
        context = LiveGateContext(
            live_submit_enabled=True,
            account_label=account_label,
            strategy_name=strategy_name,
            strategy_config_hash=strategy_config_hash,
        )
        gate = evaluate_live_gate(context)
        if not gate.approved:
            raise RuntimeError(f"live gate blocked: {','.join(gate.reasons)}")
        if not plan.reduce_only and has_entry_order_conflict(plan.symbol, unresolved):
            raise RuntimeError("live plan blocked: unresolved_order_uncertainty")
        desired_notional = await order_read_repository.load_approved_intent_notional(
            plan.intent_id,
        )
        if desired_notional is None:
            raise RuntimeError("live plan has no persisted approved intent notional")
        if (
            risk_config.max_order_notional is not None
            and desired_notional > risk_config.max_order_notional
        ):
            raise RuntimeError("live plan exceeds current risk notional cap")
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

        if not account_event_hub_url.strip():
            raise ValueError("account_event_hub_url must not be empty")
        register_expected_entry = LiveEntryExpectationRegistrar(
            account_label=account_label,
            publisher=WebSocketAccountPositionExpectationPublisher(
                url=account_event_hub_url,
                environment="live",
                account_label=account_label,
            ),
        )
        machine = OrderExecutionStateMachine(
            event_repository=order_event_repository,
            exchange=client,
            repository=order_repository,
            live_submit_enabled=True,
            clock=lambda: datetime.now(tz=UTC),
            on_before_submit=register_expected_entry,
            serialize_commands=False,
        )
        reservation_repo = AsyncPostgresPositionReservationRepository(
            factory, strategy_name=strategy_name
        )
        execution_coordinator = OrderExecutionCoordinator(
            backend=machine,
            account_label=account_label,
            environment="live",
            reservation_repository=reservation_repo,
        )
        session = LiveRolloutSession(
            repository=live_repository,
            execute_plan=execution_coordinator.submit,
            config=LiveSessionConfig(
                session_id=session_id,
                operator=operator,
                strategy_config_hash=strategy_config_hash,
                risk_config_hash=risk_config.config_hash,
            ),
            clock=lambda: datetime.now(tz=UTC),
        )

        return await session.run_one(
            gate=gate,
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
