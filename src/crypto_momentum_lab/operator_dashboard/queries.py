from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution import ExchangeOrderState
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.operator_dashboard.schemas import (
    AccountOverviewResponse,
    RiskExecutionResponse,
    RunReportSummaryResponse,
    ServiceStatusResponse,
    StrategyRunResponse,
    SystemOverviewResponse,
    UniverseStatusResponse,
)
from crypto_momentum_lab.operator_dashboard.status import (
    OperationalStatus,
    freshness_status,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountBalanceSnapshotRow,
    AccountFillEventRow,
    AccountOpenOrderRow,
    AccountPositionSnapshotRow,
    ExchangeOrderRow,
    ExecutionAccountProcessStateRow,
    LiveSessionTransitionRow,
    MonitoringMembershipRow,
    PaperFillRow,
    RiskEvaluationRow,
    RiskHaltRow,
    RuntimeMarketState15sRow,
    ShadowSessionRow,
    StrategyRunRow,
    StrategyRuntimeEventRow,
    StrategySignalRow,
    TradingLeaseRow,
    UniverseEntryRow,
    UniverseSnapshotRow,
)


class DashboardQueries:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        clock: Callable[[], datetime] | None = None,
        stale_after_seconds: float = 120.0,
    ) -> None:
        self._session_factory = session_factory
        self._clock = clock or (lambda: datetime.now(tz=UTC))
        self._stale_after_seconds = stale_after_seconds

    async def health(self) -> dict[str, str]:
        async with self._session_factory() as session:
            await session.execute(text("SELECT 1"))
        return {"app_status": "UP", "database_status": "UP"}

    async def overview(self) -> SystemOverviewResponse:
        now = self._clock()
        async with self._session_factory() as session:
            market_at = await session.scalar(
                select(func.max(RuntimeMarketState15sRow.bucket_end))
            )
            account = await session.scalar(
                select(ExecutionAccountProcessStateRow)
                .order_by(ExecutionAccountProcessStateRow.occurred_at.desc())
                .limit(1)
            )
            strategy_event = await session.scalar(
                select(StrategyRuntimeEventRow)
                .order_by(StrategyRuntimeEventRow.occurred_at.desc())
                .limit(1)
            )
            halt_count = await session.scalar(
                select(func.count(RiskHaltRow.halt_id)).where(
                    RiskHaltRow.active.is_(True)
                )
            )
            lease = await session.scalar(
                select(TradingLeaseRow)
                .where(
                    TradingLeaseRow.state == "active",
                    TradingLeaseRow.expires_at > now,
                )
                .order_by(TradingLeaseRow.expires_at.desc())
                .limit(1)
            )
            live = await session.scalar(
                select(LiveSessionTransitionRow)
                .order_by(LiveSessionTransitionRow.occurred_at.desc())
                .limit(1)
            )
        account_at = None if account is None else account.occurred_at
        strategy_at = None if strategy_event is None else strategy_event.occurred_at
        services = [
            _service("market-data", now, market_at, self._stale_after_seconds),
            _service("execution-account", now, account_at, self._stale_after_seconds),
            _service("strategy-runner", now, strategy_at, self._stale_after_seconds),
            ServiceStatusResponse(
                name="database",
                status=OperationalStatus.READY,
                observed_at=now,
                age_seconds=0,
            ),
        ]
        if live is not None:
            live_status = (
                OperationalStatus.LIVE
                if live.state == "live_enabled"
                else OperationalStatus.HALTED
                if live.state == "halted"
                else OperationalStatus.SHADOW
            )
            services.append(
                ServiceStatusResponse(
                    name="live-rollout",
                    status=live_status,
                    observed_at=live.occurred_at,
                    age_seconds=_age(now, live.occurred_at),
                    details={"state": live.state, "session_id": live.session_id},
                )
            )
        return SystemOverviewResponse(
            generated_at=now,
            database_status=OperationalStatus.READY,
            services=services,
            active_halt_count=int(halt_count or 0),
            active_lease=None
            if lease is None
            else {
                "account_label": lease.account_label,
                "strategy_name": lease.strategy_name,
                "owner": lease.owner,
                "expires_at": lease.expires_at.isoformat(),
            },
        )

    async def universe(self) -> UniverseStatusResponse:
        async with self._session_factory() as session:
            snapshot = await session.scalar(
                select(UniverseSnapshotRow)
                .where(UniverseSnapshotRow.activated.is_(True))
                .order_by(UniverseSnapshotRow.observed_at.desc())
                .limit(1)
            )
            if snapshot is None:
                return UniverseStatusResponse(
                    status=OperationalStatus.NO_DATA,
                    observed_at=None,
                    gainers=[],
                    losers=[],
                    monitored_symbols=[],
                )
            entries = (
                await session.scalars(
                    select(UniverseEntryRow).where(
                        UniverseEntryRow.snapshot_id == snapshot.snapshot_id
                    )
                )
            ).all()
            memberships = (
                await session.scalars(
                    select(MonitoringMembershipRow).where(
                        MonitoringMembershipRow.snapshot_id == snapshot.snapshot_id
                    )
                )
            ).all()
        gainers = sorted(
            (entry for entry in entries if entry.gainer_rank is not None),
            key=lambda item: item.gainer_rank or 999,
        )[:20]
        losers = sorted(
            (entry for entry in entries if entry.loser_rank is not None),
            key=lambda item: item.loser_rank or 999,
        )[:20]
        return UniverseStatusResponse(
            status=freshness_status(
                now=self._clock(),
                observed_at=snapshot.observed_at,
                stale_after_seconds=3600,
            ),
            observed_at=snapshot.observed_at,
            gainers=[_universe_entry(row, "gainer") for row in gainers],
            losers=[_universe_entry(row, "loser") for row in losers],
            monitored_symbols=[
                {
                    "symbol": row.symbol,
                    "status": row.status,
                    "side": row.side,
                }
                for row in memberships
            ],
        )

    async def strategy_run(self) -> StrategyRunResponse:
        async with self._session_factory() as session:
            run = await session.scalar(
                select(StrategyRunRow)
                .order_by(StrategyRunRow.created_at.desc())
                .limit(1)
            )
            if run is None:
                return StrategyRunResponse(
                    status=OperationalStatus.NO_DATA,
                    run_id=None,
                    strategy_name=None,
                    config_hash=None,
                    checkpoint_at=None,
                    latest_signals=[],
                    latest_paper_fills=[],
                    rejection_summary={},
                )
            signals = (
                await session.scalars(
                    select(StrategySignalRow)
                    .where(StrategySignalRow.run_id == run.run_id)
                    .order_by(StrategySignalRow.detected_at.desc())
                    .limit(20)
                )
            ).all()
            checkpoint_at = await session.scalar(
                select(func.max(StrategyRuntimeEventRow.occurred_at)).where(
                    StrategyRuntimeEventRow.run_id == run.run_id,
                    StrategyRuntimeEventRow.event_type == "checkpoint_saved",
                )
            )
            fills = (
                await session.scalars(
                    select(PaperFillRow)
                    .where(PaperFillRow.run_id == run.run_id)
                    .order_by(
                        PaperFillRow.filled_at.desc().nulls_last(),
                        PaperFillRow.target_fill_at.desc(),
                    )
                    .limit(20)
                )
            ).all()
        return StrategyRunResponse(
            status=OperationalStatus.READY,
            run_id=run.run_id,
            strategy_name=run.strategy_name,
            config_hash=run.config_hash,
            checkpoint_at=checkpoint_at,
            latest_signals=[
                {
                    "symbol": row.symbol,
                    "side": row.side,
                    "detected_at": row.detected_at.isoformat(),
                    "reason": row.reason,
                }
                for row in signals
            ],
            latest_paper_fills=[
                {
                    "filled_at": None
                    if row.filled_at is None
                    else row.filled_at.isoformat(),
                    "symbol": row.symbol,
                    "action": "BUY" if row.side == "long" else "SELL",
                    "side": row.side,
                    "status": row.status,
                    "fill_price": None
                    if row.fill_price is None
                    else str(row.fill_price),
                    "quantity": None
                    if row.quantity is None
                    else str(row.quantity),
                    "filled_notional": None
                    if row.filled_notional is None
                    else str(row.filled_notional),
                    "fee": str(row.fee),
                }
                for row in fills
            ],
            rejection_summary=_json_mapping(run.rejection_summary),
        )

    async def account(self) -> AccountOverviewResponse:
        async with self._session_factory() as session:
            process = await session.scalar(
                select(ExecutionAccountProcessStateRow)
                .order_by(ExecutionAccountProcessStateRow.occurred_at.desc())
                .limit(1)
            )
            balances = (
                await session.scalars(
                    select(AccountBalanceSnapshotRow)
                    .order_by(AccountBalanceSnapshotRow.observed_at.desc())
                    .limit(20)
                )
            ).all()
            positions = (
                await session.scalars(
                    select(AccountPositionSnapshotRow)
                    .where(AccountPositionSnapshotRow.position_amt != 0)
                    .order_by(AccountPositionSnapshotRow.observed_at.desc())
                    .limit(20)
                )
            ).all()
            orders = (
                await session.scalars(
                    select(AccountOpenOrderRow)
                    .order_by(AccountOpenOrderRow.observed_at.desc())
                    .limit(20)
                )
            ).all()
            fills = (
                await session.scalars(
                    select(AccountFillEventRow)
                    .order_by(AccountFillEventRow.trade_at.desc())
                    .limit(20)
                )
            ).all()
        observed_at = None if process is None else process.occurred_at
        return AccountOverviewResponse(
            status=OperationalStatus.UNKNOWN
            if process is None
            else OperationalStatus.READY
            if process.state == "ready_readonly"
            else OperationalStatus.HALTED,
            observed_at=observed_at,
            balances=[
                {
                    "asset": row.asset,
                    "wallet_balance": str(row.wallet_balance),
                    "available_balance": str(row.available_balance),
                    "unrealized_pnl": str(row.unrealized_pnl),
                }
                for row in balances
            ],
            positions=[
                {
                    "symbol": row.symbol,
                    "position_amt": str(row.position_amt),
                    "entry_price": str(row.entry_price),
                    "notional": str(row.notional),
                    "unrealized_pnl": str(row.unrealized_pnl),
                }
                for row in positions
            ],
            open_orders=[
                {
                    "symbol": row.symbol,
                    "client_order_id": row.client_order_id,
                    "side": row.side,
                    "status": row.status,
                    "reduce_only": row.reduce_only,
                }
                for row in orders
            ],
            fills=[
                {
                    "symbol": row.symbol,
                    "trade_id": row.trade_id,
                    "price": str(row.price),
                    "quantity": str(row.quantity),
                    "realized_pnl": str(row.realized_pnl),
                    "fee": str(row.fee),
                }
                for row in fills
            ],
        )

    async def risk_execution(self) -> RiskExecutionResponse:
        terminal = tuple(state.value for state in ExchangeOrderState if state.terminal)
        async with self._session_factory() as session:
            halts = (
                await session.scalars(
                    select(RiskHaltRow)
                    .where(RiskHaltRow.active.is_(True))
                    .order_by(RiskHaltRow.created_at.desc())
                )
            ).all()
            decisions = (
                await session.scalars(
                    select(RiskEvaluationRow)
                    .order_by(RiskEvaluationRow.evaluated_at.desc())
                    .limit(30)
                )
            ).all()
            orders = (
                await session.scalars(
                    select(ExchangeOrderRow)
                    .order_by(ExchangeOrderRow.updated_at.desc())
                    .limit(30)
                )
            ).all()
        ambiguous = [row for row in orders if row.state not in terminal]
        return RiskExecutionResponse(
            status=OperationalStatus.HALTED
            if halts or ambiguous
            else OperationalStatus.READY,
            active_halts=[
                {"reason": row.reason, "created_at": row.created_at.isoformat()}
                for row in halts
            ],
            latest_risk_decisions=[
                {
                    "candidate_id": row.candidate_id,
                    "decision": row.decision,
                    "reason": row.reason,
                    "evaluated_at": row.evaluated_at.isoformat(),
                }
                for row in decisions
            ],
            exchange_orders=[_exchange_order(row) for row in orders],
            ambiguous_orders=[_exchange_order(row) for row in ambiguous],
        )

    async def reports(self) -> RunReportSummaryResponse:
        async with self._session_factory() as session:
            paper = (
                await session.scalars(
                    select(StrategyRunRow)
                    .order_by(StrategyRunRow.created_at.desc())
                    .limit(10)
                )
            ).all()
            shadow = (
                await session.scalars(
                    select(ShadowSessionRow)
                    .order_by(ShadowSessionRow.started_at.desc())
                    .limit(10)
                )
            ).all()
            live = (
                await session.scalars(
                    select(LiveSessionTransitionRow)
                    .order_by(LiveSessionTransitionRow.occurred_at.desc())
                    .limit(10)
                )
            ).all()
        return RunReportSummaryResponse(
            status=OperationalStatus.READY
            if paper or shadow or live
            else OperationalStatus.NO_DATA,
            paper_runs=[
                {
                    "run_id": row.run_id,
                    "strategy_name": row.strategy_name,
                    "created_at": row.created_at.isoformat(),
                    "signal_count": row.signal_count,
                    "fill_count": row.fill_count,
                }
                for row in paper
            ],
            shadow_sessions=[
                {
                    "run_id": row.run_id,
                    "strategy_name": row.strategy_name,
                    "state": row.state,
                    "started_at": row.started_at.isoformat(),
                }
                for row in shadow
            ],
            live_sessions=[
                {
                    "session_id": row.session_id,
                    "state": row.state,
                    "occurred_at": row.occurred_at.isoformat(),
                }
                for row in live
            ],
        )


def _service(
    name: str,
    now: datetime,
    observed_at: datetime | None,
    stale_after_seconds: float,
) -> ServiceStatusResponse:
    return ServiceStatusResponse(
        name=name,
        status=freshness_status(
            now=now,
            observed_at=observed_at,
            stale_after_seconds=stale_after_seconds,
        ),
        observed_at=observed_at,
        age_seconds=None if observed_at is None else _age(now, observed_at),
    )


def _age(now: datetime, observed_at: datetime) -> float:
    return max(0.0, (now - observed_at).total_seconds())


def _universe_entry(row: UniverseEntryRow, side: str) -> dict[str, JsonValue]:
    rank = row.gainer_rank if side == "gainer" else row.loser_rank
    return {
        "symbol": row.symbol,
        "rank": rank,
        "utc_day_return": None
        if row.utc_day_return is None
        else str(row.utc_day_return),
        "current_price": None
        if row.current_price is None
        else str(row.current_price),
    }


def _exchange_order(row: ExchangeOrderRow) -> dict[str, JsonValue]:
    return {
        "client_order_id": row.client_order_id,
        "exchange_order_id": row.exchange_order_id,
        "symbol": row.symbol,
        "side": row.side,
        "state": row.state,
        "quantity": str(row.quantity),
        "updated_at": row.updated_at.isoformat(),
    }


def _json_mapping(value: dict[str, object]) -> dict[str, JsonValue]:
    return {key: _json_value(item) for key, item in value.items()}


def _json_value(value: object) -> JsonValue:
    if isinstance(value, Decimal | datetime):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    return str(value)
