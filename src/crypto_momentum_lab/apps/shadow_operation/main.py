import asyncio
import json
import os
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Annotated

import typer
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    ExchangeOrderFill,
    ExchangeOrderSnapshot,
    OrderExecutionPlan,
    ShadowSuppressionEvent,
)
from crypto_momentum_lab.domain.risk import (
    RiskConfigSnapshot,
    StrategyLiveState,
)
from crypto_momentum_lab.domain.strategy import (
    RunMode,
    StrategyRunIdentity,
    deterministic_config_hash,
)
from crypto_momentum_lab.execution_account.orders.quantization import (
    SymbolTradingRules,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionStateMachine,
    SubmitPolicy,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ContractMetadataRow,
    ExecutionAccountProcessStateRow,
    RiskConfigSnapshotRow,
)
from crypto_momentum_lab.persistence.postgres.order_repository import (
    PostgresOrderRepository,
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
from crypto_momentum_lab.persistence.postgres.shadow_repository import (
    PostgresShadowRepository,
)
from crypto_momentum_lab.risk.gateway import RiskGateway
from crypto_momentum_lab.shadow_operation.drills import (
    SUPPORTED_SHADOW_DRILLS,
    run_shadow_drill,
)
from crypto_momentum_lab.shadow_operation.reports import (
    ShadowReport,
    build_shadow_report,
)
from crypto_momentum_lab.shadow_operation.service import (
    ShadowOperationConfig,
    ShadowOperationContext,
    ShadowOperationResult,
    ShadowOperationService,
)
from crypto_momentum_lab.strategy_runner.registry import (
    build_runtime_config,
    build_runtime_strategy,
)

app = typer.Typer(no_args_is_help=True)


@app.callback()
def shadow_operation_app() -> None:
    """Run and inspect write-suppressed live strategy sessions."""


@app.command("run")
def run_command(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "compression_breakout",
    market_environment: Annotated[
        str,
        typer.Option("--market-environment"),
    ] = "research",
    run_id: Annotated[str, typer.Option("--run-id")] = "shadow-manual",
    max_runtime_seconds: Annotated[
        int, typer.Option("--max-runtime-seconds", min=1)
    ] = 3600,
    state_stale_after_seconds: Annotated[
        float, typer.Option("--state-stale-after-seconds", min=1)
    ] = 120.0,
    checkpoint_every_states: Annotated[
        int, typer.Option("--checkpoint-every-states", min=1)
    ] = 100,
    require_lease_owner: Annotated[
        str, typer.Option("--require-lease-owner")
    ] = "shadow-preflight",
    hedge_mode: Annotated[
        bool,
        typer.Option("--hedge-mode/--one-way-mode"),
    ] = True,
    entry_long_only: Annotated[
        bool,
        typer.Option("--entry-long-only/--entry-all-sides"),
    ] = True,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    del checkpoint_every_states
    resolved_url = _database_url(database_url)
    result = asyncio.run(
        _run_from_database(
            database_url=resolved_url,
            account_label=account_label,
            strategy_name=strategy,
            market_environment=market_environment,
            run_id=run_id,
            max_runtime_seconds=max_runtime_seconds,
            state_stale_after_seconds=state_stale_after_seconds,
            lease_owner=require_lease_owner,
            hedge_mode=hedge_mode,
            entry_long_only=entry_long_only,
        )
    )
    payload = asdict(result)
    typer.echo(json.dumps(payload, sort_keys=True) if json_output else str(payload))


@app.command("report")
def report_command(
    run_id: Annotated[str, typer.Option("--run-id")],
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    report = asyncio.run(_load_report(_database_url(database_url), run_id))
    payload = asdict(report)
    typer.echo(json.dumps(payload, sort_keys=True) if json_output else str(payload))


@app.command("drill")
def drill_command(
    run_id: Annotated[str, typer.Option("--run-id")],
    drill_name: Annotated[str, typer.Option("--drill")],
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
) -> None:
    if drill_name not in SUPPORTED_SHADOW_DRILLS:
        raise typer.BadParameter(f"unsupported drill: {drill_name}")
    asyncio.run(_record_drill(_database_url(database_url), run_id, drill_name))
    typer.echo(f"Shadow drill recorded: {drill_name}")


async def _run_from_database(
    *,
    database_url: str,
    account_label: str,
    strategy_name: str,
    market_environment: str,
    run_id: str,
    max_runtime_seconds: int,
    state_stale_after_seconds: float,
    lease_owner: str,
    hedge_mode: bool,
    entry_long_only: bool,
) -> ShadowOperationResult:
    now = datetime.now(tz=UTC)
    engine = create_async_database_engine(database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        risk_repository = PostgresRiskRepository(factory)
        shadow_repository = PostgresShadowRepository(factory)
        order_repository = PostgresOrderRepository(factory)
        lease = await risk_repository.load_active_lease("live", account_label, now)
        account_state = await _latest_account_state(factory, account_label)
        risk_config = await _latest_risk_config(factory, account_label)
        halts = await risk_repository.load_active_halts("live", account_label)
        state_repository = PostgresRuntimeMarketStateRepository(factory)
        states = await state_repository.load_after(
            environment=market_environment,
            cursor=RuntimeStateCursor(
                bucket_start=now - timedelta(seconds=max_runtime_seconds),
                symbol="",
            ),
            limit=min(100000, max(10000, max_runtime_seconds * 8)),
        )
        rules = await _load_trading_rules(factory, {state.symbol for state in states})
        strategy_config: dict[str, object] = {
            "candidate_notional": Decimal("100"),
            "candidate_ttl_buckets": 4,
        }
        config_hash = deterministic_config_hash(
            build_runtime_config(strategy_name, config=strategy_config)
        )
        strategy = build_runtime_strategy(
            strategy_name,
            config=strategy_config,
            identity=StrategyRunIdentity(
                run_id=run_id,
                strategy_name=strategy_name,
                strategy_version="v0",
                config_hash=config_hash,
                run_mode=RunMode.SHADOW,
                code_commit="operator-shadow",
                created_at=now,
                source_paths=(
                    f"postgres-runtime-states:{market_environment}",
                ),
            ),
        )
        guarded_exchange = _WriteRejectingExchange()
        state_machine = OrderExecutionStateMachine(
            exchange=guarded_exchange,
            repository=_ShadowOrderRepositoryAdapter(
                order_repository,
                shadow_repository,
            ),
            submit_policy=SubmitPolicy.SHADOW_SUPPRESS,
            live_submit_enabled=False,
            clock=lambda: now,
        )
        service = ShadowOperationService(
            strategy=strategy,
            risk_gateway=RiskGateway(),
            shadow_repository=shadow_repository,
            approved_intent_repository=order_repository,
            state_machine=state_machine,
            config=ShadowOperationConfig(
                run_id=run_id,
                account_label=account_label,
                strategy_name=strategy_name,
                strategy_config_hash=config_hash,
                lease_owner=lease_owner,
                max_market_state_age_seconds=state_stale_after_seconds,
                resize_tolerance=Decimal("0.10"),
                hedge_mode=hedge_mode,
                entry_long_only=entry_long_only,
                warm_stale_states=True,
            ),
        )
        return await service.run(
            states,
            ShadowOperationContext(
                now=now,
                active_lease=lease,
                account_state=account_state,
                open_position_symbols=frozenset(),
                active_halts=halts,
                risk_config=risk_config,
                strategy_state=StrategyLiveState.ACTIVE,
                trading_rules=rules,
            ),
        )
    finally:
        await engine.dispose()


class _ShadowOrderRepositoryAdapter:
    def __init__(
        self,
        order_repository: PostgresOrderRepository,
        shadow_repository: PostgresShadowRepository,
    ) -> None:
        self._orders = order_repository
        self._shadow = shadow_repository

    async def save_planned_order(self, plan: OrderExecutionPlan) -> None:
        await self._orders.save_planned_order(plan)

    async def append_order_event(self, event: ExchangeOrderEvent) -> bool:
        return await self._orders.append_order_event(event)

    async def save_fill(self, fill: ExchangeOrderFill) -> bool:
        return await self._orders.save_fill(fill)

    async def save_shadow_suppression(
        self,
        event: ShadowSuppressionEvent,
    ) -> None:
        await self._shadow.save_shadow_suppression(event)


class _WriteRejectingExchange:
    async def submit_order(self, plan: OrderExecutionPlan) -> ExchangeOrderSnapshot:
        raise AssertionError("Binance write boundary reached in shadow mode")

    async def query_order_by_client_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> ExchangeOrderSnapshot | None:
        raise AssertionError("Binance order query reached in shadow mode")

    async def cancel_order_by_client_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> ExchangeOrderSnapshot:
        raise AssertionError("Binance order cancellation reached in shadow mode")


async def _latest_account_state(
    factory: async_sessionmaker[AsyncSession],
    account_label: str,
) -> ExecutionAccountStatus:
    async with factory() as session:
        state = await session.scalar(
            select(ExecutionAccountProcessStateRow.state)
            .where(
                ExecutionAccountProcessStateRow.environment == "live",
                ExecutionAccountProcessStateRow.account_label == account_label,
            )
            .order_by(ExecutionAccountProcessStateRow.occurred_at.desc())
            .limit(1)
        )
    return ExecutionAccountStatus(state) if state else ExecutionAccountStatus.DEGRADED


async def _latest_risk_config(
    factory: async_sessionmaker[AsyncSession],
    account_label: str,
) -> RiskConfigSnapshot:
    async with factory() as session:
        row = await session.scalar(
            select(RiskConfigSnapshotRow)
            .where(
                RiskConfigSnapshotRow.environment == "live",
                RiskConfigSnapshotRow.account_label == account_label,
            )
            .order_by(RiskConfigSnapshotRow.created_at.desc())
            .limit(1)
        )
    if row is None:
        raise RuntimeError("no persisted risk config for account")
    return RiskConfigSnapshot(
        environment=row.environment,
        account_label=row.account_label,
        max_order_notional=row.max_order_notional,
        max_gross_notional=row.max_gross_notional,
        max_daily_loss=row.max_daily_loss,
        max_open_positions=row.max_open_positions,
        max_market_state_age_seconds=float(row.max_market_state_age_seconds),
        max_account_state_age_seconds=float(row.max_account_state_age_seconds),
        allow_reduce_only_while_draining=row.allow_reduce_only_while_draining,
        created_at=row.created_at,
    )


async def _load_trading_rules(
    factory: async_sessionmaker[AsyncSession],
    symbols: set[str] | None,
) -> dict[str, SymbolTradingRules]:
    async with factory() as session:
        latest_query = select(
            ContractMetadataRow.symbol,
            func.max(ContractMetadataRow.effective_at).label("effective_at"),
        ).group_by(ContractMetadataRow.symbol)
        if symbols is not None:
            latest_query = latest_query.where(
                ContractMetadataRow.symbol.in_(symbols)
            )
        latest_effective_at = latest_query.subquery()
        statement = (
            select(ContractMetadataRow)
            .join(
                latest_effective_at,
                and_(
                    ContractMetadataRow.symbol == latest_effective_at.c.symbol,
                    ContractMetadataRow.effective_at
                    == latest_effective_at.c.effective_at,
                ),
            )
            .order_by(ContractMetadataRow.symbol)
        )
        rows = (await session.scalars(statement)).all()
    rules: dict[str, SymbolTradingRules] = {}
    for row in rows:
        if row.symbol in rules:
            continue
        parsed = _rules_from_exchange_info(row.symbol, row.raw_payload)
        if parsed is not None:
            rules[row.symbol] = parsed
    return rules


def _rules_from_exchange_info(
    symbol: str,
    payload: dict[str, object],
) -> SymbolTradingRules | None:
    filters_value = payload.get("filters")
    if not isinstance(filters_value, list):
        return None
    filters = {
        str(item.get("filterType")): item
        for item in filters_value
        if isinstance(item, dict)
    }
    price_filter = filters.get("PRICE_FILTER")
    lot_filter = filters.get("MARKET_LOT_SIZE") or filters.get("LOT_SIZE")
    notional_filter = filters.get("MIN_NOTIONAL")
    if not price_filter or not lot_filter or not notional_filter:
        return None
    return SymbolTradingRules(
        symbol=symbol,
        tick_size=Decimal(str(price_filter["tickSize"])),
        step_size=Decimal(str(lot_filter["stepSize"])),
        min_quantity=Decimal(str(lot_filter["minQty"])),
        max_quantity=Decimal(str(lot_filter["maxQty"])),
        min_notional=Decimal(
            str(notional_filter.get("notional", notional_filter.get("minNotional")))
        ),
    )


async def _load_report(database_url: str, run_id: str) -> ShadowReport:
    engine = create_async_database_engine(database_url)
    try:
        repository = PostgresShadowRepository(
            async_sessionmaker(engine, expire_on_commit=False)
        )
        plans, suppressions, metrics, drills = await repository.load_report_rows(run_id)
        return build_shadow_report(
            plans=plans,
            suppressions=suppressions,
            metrics=metrics,
            drills=drills,
        )
    finally:
        await engine.dispose()


async def _record_drill(database_url: str, run_id: str, drill_name: str) -> None:
    async def probe() -> None:
        return None

    engine = create_async_database_engine(database_url)
    try:
        repository = PostgresShadowRepository(
            async_sessionmaker(engine, expire_on_commit=False)
        )
        result = await run_shadow_drill(
            run_id=run_id,
            drill_name=drill_name,
            occurred_at=datetime.now(tz=UTC),
            probe=probe,
        )
        await repository.save_drill_result(result)
    finally:
        await engine.dispose()


def _database_url(value: str | None) -> str:
    resolved = value or os.environ.get("CML_DATABASE_URL")
    if not resolved:
        raise typer.BadParameter("--database-url or CML_DATABASE_URL is required")
    return resolved
