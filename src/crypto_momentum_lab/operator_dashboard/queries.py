from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import and_, case, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution import ExchangeOrderState
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.operator_dashboard.schemas import (
    AccountOverviewResponse,
    PaperAccountEquityResponse,
    PaperAccountHistoryResponse,
    PaperAccountsEquityResponse,
    PaperAccountsResponse,
    PaperAccountSummaryResponse,
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
    AccountConfigSnapshotRow,
    AccountFillEventRow,
    AccountOpenOrderRow,
    AccountPositionSnapshotRow,
    AccountReconciliationRunRow,
    ExchangeOrderRow,
    ExecutionAccountProcessStateRow,
    LiveSessionTransitionRow,
    MonitoringMembershipRow,
    OrderIntentCandidateRow,
    OrderIntentExecutionRow,
    PaperEquitySnapshotRow,
    PaperFillRow,
    PaperPositionRow,
    RiskEvaluationRow,
    RiskHaltRow,
    RuntimeMarketState15sRow,
    ShadowSessionRow,
    StrategyRunRow,
    StrategyRuntimeCheckpointRow,
    StrategySignalRow,
    TradingLeaseRow,
    UniverseEntryRow,
    UniverseSnapshotRow,
)

_EQUITY_WINDOW = timedelta(hours=24)
_EQUITY_BUCKET_SECONDS = 6 * 60
_EQUITY_MAX_POINTS = 240


@dataclass(slots=True)
class _AccountFillAggregate:
    symbol: str
    order_id: str
    side: str
    strategy_name: str | None
    quantity: Decimal = Decimal("0")
    notional: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    fee: Decimal = Decimal("0")
    trade_at: datetime | None = None
    fill_count: int = 0
    fee_assets: set[str] = field(default_factory=set)


def _aggregate_account_fills(
    rows: Sequence[AccountFillEventRow],
    strategy_by_order: dict[str, str],
    *,
    limit: int = 20,
) -> list[dict[str, JsonValue]]:
    """Collapse exchange partial fills into one order-level dashboard row."""
    grouped: dict[tuple[str, str, str], _AccountFillAggregate] = {}
    for row in rows:
        key = (row.order_id, row.symbol, row.side)
        aggregate = grouped.get(key)
        if aggregate is None:
            aggregate = _AccountFillAggregate(
                symbol=row.symbol,
                order_id=row.order_id,
                side=row.side,
                strategy_name=strategy_by_order.get(row.order_id),
            )
            grouped[key] = aggregate
        aggregate.fee_assets.add(row.fee_asset)
        aggregate.quantity += row.quantity
        aggregate.notional += row.price * row.quantity
        aggregate.realized_pnl += row.realized_pnl
        aggregate.fee += row.fee
        aggregate.trade_at = (
            row.trade_at
            if aggregate.trade_at is None
            else max(aggregate.trade_at, row.trade_at)
        )
        aggregate.fill_count += 1

    ordered = sorted(
        grouped.values(),
        key=lambda aggregate: aggregate.trade_at or datetime.min.replace(tzinfo=UTC),
        reverse=True,
    )[:limit]
    return [
        {
            "symbol": aggregate.symbol,
            "order_id": aggregate.order_id,
            "side": aggregate.side,
            "price": str(
                aggregate.notional / aggregate.quantity
                if aggregate.quantity
                else Decimal("0")
            ),
            "quantity": str(aggregate.quantity),
            "realized_pnl": str(aggregate.realized_pnl),
            "fee": str(aggregate.fee),
            "fee_asset": " / ".join(sorted(aggregate.fee_assets)),
            "trade_at": (
                None
                if aggregate.trade_at is None
                else aggregate.trade_at.isoformat()
            ),
            "fill_count": aggregate.fill_count,
            "strategy_name": aggregate.strategy_name,
        }
        for aggregate in ordered
    ]


@asynccontextmanager
async def _session_scope(
    session_factory: async_sessionmaker[AsyncSession],
    existing: AsyncSession | None,
) -> AsyncIterator[AsyncSession]:
    if existing is not None:
        yield existing
        return
    async with session_factory() as session:
        yield session


class DashboardQueries:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        clock: Callable[[], datetime] | None = None,
        stale_after_seconds: float = 120.0,
        paper_run_ids: frozenset[str] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._clock = clock or (lambda: datetime.now(tz=UTC))
        self._stale_after_seconds = stale_after_seconds
        self._paper_run_ids = paper_run_ids

    async def health(self) -> dict[str, str]:
        async with self._session_factory() as session:
            await session.execute(text("SELECT 1"))
        return {"app_status": "UP", "database_status": "UP"}

    async def overview(self) -> SystemOverviewResponse:
        now = self._clock()
        async with self._session_factory() as session:
            market_at = await session.scalar(
                select(RuntimeMarketState15sRow.bucket_end)
                .order_by(RuntimeMarketState15sRow.bucket_start.desc())
                .limit(1)
            )
            account = await session.scalar(
                select(ExecutionAccountProcessStateRow)
                .order_by(ExecutionAccountProcessStateRow.occurred_at.desc())
                .limit(1)
            )
            strategy_checkpoint = await session.scalar(
                select(StrategyRuntimeCheckpointRow)
                .order_by(StrategyRuntimeCheckpointRow.saved_at.desc())
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
            live_heartbeat_at = None
            live_started_at = None
            if live is not None:
                # A transition records the lifecycle state, not a process
                # heartbeat.  The live daemon's runtime checkpoint is the
                # freshest durable signal that its loop is still progressing.
                live_heartbeat_at = await session.scalar(
                    select(StrategyRuntimeCheckpointRow.saved_at).where(
                        StrategyRuntimeCheckpointRow.run_id == live.session_id
                    )
                )
                live_started_at = await session.scalar(
                    select(LiveSessionTransitionRow.occurred_at)
                    .where(
                        LiveSessionTransitionRow.session_id == live.session_id,
                        LiveSessionTransitionRow.state == "live_enabled",
                    )
                    .order_by(LiveSessionTransitionRow.occurred_at.desc())
                    .limit(1)
                )
        account_at = None if account is None else account.occurred_at
        strategy_at = (
            None if strategy_checkpoint is None else strategy_checkpoint.saved_at
        )
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
            live_observed_at = live_heartbeat_at or live.occurred_at
            live_details: dict[str, JsonValue] = {
                "state": live.state,
                "session_id": live.session_id,
                "heartbeat_source": (
                    "runtime_checkpoint"
                    if live_heartbeat_at is not None
                    else "state_transition"
                ),
            }
            if live_started_at is not None:
                live_details["started_at"] = live_started_at.isoformat()
            services.append(
                ServiceStatusResponse(
                    name="live-rollout",
                    status=live_status,
                    observed_at=live_observed_at,
                    age_seconds=_age(now, live_observed_at),
                    details=live_details,
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

    async def paper_accounts(self) -> PaperAccountsResponse:
        async with self._session_factory() as session:
            selected_runs = await self._selected_paper_runs(session)
            accounts = await self._paper_account_summaries(session, selected_runs)
        return PaperAccountsResponse(
            status=(OperationalStatus.READY if accounts else OperationalStatus.NO_DATA),
            accounts=accounts,
        )

    async def paper_account_equity(self) -> PaperAccountsEquityResponse:
        window_end = self._clock()
        window_start = window_end - _EQUITY_WINDOW
        async with self._session_factory() as session:
            selected_runs = await self._selected_paper_runs(session)
            if not selected_runs:
                return PaperAccountsEquityResponse(
                    status=OperationalStatus.NO_DATA,
                    accounts=[],
                )
            run_ids = [run.run_id for run in selected_runs]
            rows = (
                await session.scalars(
                    select(PaperEquitySnapshotRow)
                    .where(
                        PaperEquitySnapshotRow.run_id.in_(run_ids),
                        PaperEquitySnapshotRow.observed_at >= window_start,
                        PaperEquitySnapshotRow.observed_at <= window_end,
                    )
                    .order_by(
                        PaperEquitySnapshotRow.run_id,
                        PaperEquitySnapshotRow.observed_at,
                    )
                )
            ).all()
        rows_by_run: dict[str, list[PaperEquitySnapshotRow]] = {}
        for row in rows:
            rows_by_run.setdefault(row.run_id, []).append(row)
        accounts = []
        for run in selected_runs:
            equity = _downsample_equity_snapshots(rows_by_run.get(run.run_id, []))
            exit_mode, exit_label = _paper_exit_details(run)
            accounts.append(
                PaperAccountEquityResponse(
                    run_id=run.run_id,
                    strategy_name=run.strategy_name,
                    exit_mode=exit_mode,
                    exit_label=exit_label,
                    equity_window_start=window_start,
                    equity_window_end=window_end,
                    equity_sample_interval_seconds=_EQUITY_BUCKET_SECONDS,
                    equity_curve=[
                        {
                            "observed_at": row.observed_at.isoformat(),
                            "balance": str(row.balance),
                            "equity": str(row.equity),
                            "realized_pnl": str(row.realized_pnl),
                            "unrealized_pnl": str(row.unrealized_pnl),
                        }
                        for row in equity
                    ],
                )
            )
        return PaperAccountsEquityResponse(
            status=OperationalStatus.READY,
            accounts=accounts,
        )

    async def paper_account(self, run_id: str) -> StrategyRunResponse:
        return await self.strategy_run(run_id=run_id)

    async def _selected_paper_runs(
        self,
        session: AsyncSession,
    ) -> list[StrategyRunRow]:
        runs = (
            await session.scalars(
                select(StrategyRunRow)
                .where(StrategyRunRow.run_mode == "paper")
                .order_by(StrategyRunRow.created_at.desc())
                .limit(50)
            )
        ).all()
        if self._paper_run_ids is not None:
            selected_runs = [
                run for run in runs if run.run_id in self._paper_run_ids
            ]
        else:
            current_runs = [
                run for run in runs if run.run_id.startswith("paper-account-")
            ]
            selected_runs = current_runs or list(runs)

        selected: list[StrategyRunRow] = []
        for strategy_name in (
            "compression_breakout",
            "orderflow_impulse",
            "liquidation_cascade",
        ):
            strategy_runs = sorted(
                (
                    run
                    for run in selected_runs
                    if run.strategy_name == strategy_name
                ),
                key=lambda run: (run.created_at, run.run_id),
            )
            selected.extend(
                strategy_runs
                if self._paper_run_ids is not None
                else strategy_runs[-2:]
            )
        return selected

    async def _paper_account_summaries(
        self,
        session: AsyncSession,
        runs: Sequence[StrategyRunRow],
    ) -> list[PaperAccountSummaryResponse]:
        if not runs:
            return []
        run_ids = [run.run_id for run in runs]
        checkpoints = (
            await session.scalars(
                select(StrategyRuntimeCheckpointRow).where(
                    StrategyRuntimeCheckpointRow.run_id.in_(run_ids)
                )
            )
        ).all()
        open_positions = (
            await session.scalars(
                select(PaperPositionRow.run_id).where(
                    PaperPositionRow.run_id.in_(run_ids),
                    PaperPositionRow.status == "open",
                )
            )
        ).all()
        closed_stats = (
            await session.execute(
                select(
                    PaperPositionRow.run_id,
                    func.count(PaperPositionRow.position_id).label("closed_count"),
                    func.sum(
                        case(
                            (PaperPositionRow.realized_pnl > 0, 1),
                            else_=0,
                        )
                    ).label("winning_count"),
                )
                .where(
                    PaperPositionRow.run_id.in_(run_ids),
                    PaperPositionRow.status == "closed",
                )
                .group_by(PaperPositionRow.run_id)
            )
        ).all()
        latest_equity_times = (
            select(
                PaperEquitySnapshotRow.run_id.label("run_id"),
                func.max(PaperEquitySnapshotRow.observed_at).label("observed_at"),
            )
            .where(PaperEquitySnapshotRow.run_id.in_(run_ids))
            .group_by(PaperEquitySnapshotRow.run_id)
            .subquery()
        )
        latest_equities = (
            await session.scalars(
                select(PaperEquitySnapshotRow).join(
                    latest_equity_times,
                    and_(
                        PaperEquitySnapshotRow.run_id
                        == latest_equity_times.c.run_id,
                        PaperEquitySnapshotRow.observed_at
                        == latest_equity_times.c.observed_at,
                    ),
                )
            )
        ).all()
        checkpoint_by_run = {row.run_id: row.saved_at for row in checkpoints}
        open_count_by_run: dict[str, int] = {}
        for run_id in open_positions:
            open_count_by_run[run_id] = open_count_by_run.get(run_id, 0) + 1
        closed_stats_by_run = {
            row.run_id: (int(row.closed_count), int(row.winning_count or 0))
            for row in closed_stats
        }
        latest_equity_by_run = {row.run_id: row for row in latest_equities}
        return [
            _paper_account_summary(
                run,
                checkpoint_at=checkpoint_by_run.get(run.run_id),
                open_position_count=open_count_by_run.get(run.run_id, 0),
                closed_trade_count=closed_stats_by_run.get(run.run_id, (0, 0))[0],
                winning_trade_count=closed_stats_by_run.get(run.run_id, (0, 0))[1],
                latest_equity=latest_equity_by_run.get(run.run_id),
            )
            for run in runs
        ]

    async def paper_history(self, run_id: str) -> PaperAccountHistoryResponse:
        async with self._session_factory() as session:
            run_exists = await session.scalar(
                select(StrategyRunRow.run_id).where(StrategyRunRow.run_id == run_id)
            )
            positions = (
                await session.scalars(
                    select(PaperPositionRow)
                    .where(PaperPositionRow.run_id == run_id)
                    .order_by(
                        PaperPositionRow.opened_at.desc(),
                        PaperPositionRow.position_id,
                    )
                )
            ).all()
        closed_positions = sorted(
            (row for row in positions if row.status == "closed"),
            key=lambda row: (row.closed_at or row.opened_at, row.position_id),
            reverse=True,
        )
        trade_events = sorted(
            (
                *(_position_open_event(row) for row in positions),
                *(_position_close_event(row) for row in closed_positions),
            ),
            key=lambda item: str(item["occurred_at"]),
            reverse=True,
        )
        return PaperAccountHistoryResponse(
            status=(
                OperationalStatus.READY
                if run_exists is not None
                else OperationalStatus.NO_DATA
            ),
            run_id=run_id,
            closed_trade_count=len(closed_positions),
            closed_trades=[_paper_position(row) for row in closed_positions],
            trade_events=trade_events,
        )

    async def strategy_run(
        self,
        run_id: str | None = None,
        *,
        equity_window_end: datetime | None = None,
        _session: AsyncSession | None = None,
    ) -> StrategyRunResponse:
        window_end = equity_window_end or self._clock()
        window_start = window_end - _EQUITY_WINDOW
        async with _session_scope(self._session_factory, _session) as session:
            statement = select(StrategyRunRow)
            if run_id is not None:
                statement = statement.where(StrategyRunRow.run_id == run_id)
            run = await session.scalar(
                statement.order_by(StrategyRunRow.created_at.desc()).limit(1)
            )
            if run is None:
                return StrategyRunResponse(
                    status=OperationalStatus.NO_DATA,
                    run_id=None,
                    strategy_name=None,
                    exit_mode=None,
                    config_hash=None,
                    checkpoint_at=None,
                    equity_window_start=window_start,
                    equity_window_end=window_end,
                    equity_sample_interval_seconds=_EQUITY_BUCKET_SECONDS,
                    portfolio_summary={},
                    equity_curve=[],
                    open_positions=[],
                    closed_trades=[],
                    trade_events=[],
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
            candidates_by_signal_id = {
                row.signal_id: row
                for row in (
                    await session.scalars(
                        select(OrderIntentCandidateRow).where(
                            OrderIntentCandidateRow.signal_id.in_(
                                [signal.signal_id for signal in signals]
                            )
                        )
                    )
                ).all()
            }
            checkpoint_at = await session.scalar(
                select(StrategyRuntimeCheckpointRow.saved_at).where(
                    StrategyRuntimeCheckpointRow.run_id == run.run_id,
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
            open_positions = (
                await session.scalars(
                    select(PaperPositionRow)
                    .where(
                        PaperPositionRow.run_id == run.run_id,
                        PaperPositionRow.status == "open",
                    )
                    .order_by(PaperPositionRow.opened_at.desc())
                )
            ).all()
            closed_positions = (
                await session.scalars(
                    select(PaperPositionRow)
                    .where(
                        PaperPositionRow.run_id == run.run_id,
                        PaperPositionRow.status == "closed",
                    )
                    .order_by(PaperPositionRow.closed_at.desc())
                    .limit(30)
                )
            ).all()
            closed_trade_count = await session.scalar(
                select(func.count(PaperPositionRow.position_id)).where(
                    PaperPositionRow.run_id == run.run_id,
                    PaperPositionRow.status == "closed",
                )
            )
            winning_trade_count = await session.scalar(
                select(func.count(PaperPositionRow.position_id)).where(
                    PaperPositionRow.run_id == run.run_id,
                    PaperPositionRow.status == "closed",
                    PaperPositionRow.realized_pnl > 0,
                )
            )
            latest_equity = await session.scalar(
                select(PaperEquitySnapshotRow)
                .where(PaperEquitySnapshotRow.run_id == run.run_id)
                .order_by(PaperEquitySnapshotRow.observed_at.desc())
                .limit(1)
            )
            equity_bucket = func.floor(
                func.extract("epoch", PaperEquitySnapshotRow.observed_at)
                / _EQUITY_BUCKET_SECONDS
            )
            equity_rows = (
                await session.scalars(
                    select(PaperEquitySnapshotRow)
                    .where(
                        PaperEquitySnapshotRow.run_id == run.run_id,
                        PaperEquitySnapshotRow.observed_at >= window_start,
                        PaperEquitySnapshotRow.observed_at <= window_end,
                    )
                    .distinct(equity_bucket)
                    .order_by(
                        equity_bucket.desc(),
                        PaperEquitySnapshotRow.observed_at.desc(),
                    )
                    .limit(_EQUITY_MAX_POINTS)
                )
            ).all()
        equity = _downsample_equity_snapshots(equity_rows)
        exit_mode, exit_label = _paper_exit_details(run)
        total_closed_trades = int(closed_trade_count or 0)
        total_winning_trades = int(winning_trade_count or 0)
        trade_events = sorted(
            (
                *(_position_open_event(row) for row in open_positions),
                *(_position_open_event(row) for row in closed_positions),
                *(_position_close_event(row) for row in closed_positions),
            ),
            key=lambda item: str(item["occurred_at"]),
            reverse=True,
        )[:40]
        return StrategyRunResponse(
            status=OperationalStatus.READY,
            run_id=run.run_id,
            strategy_name=run.strategy_name,
            exit_mode=exit_mode,
            exit_label=exit_label,
            config_hash=run.config_hash,
            checkpoint_at=checkpoint_at,
            equity_window_start=window_start,
            equity_window_end=window_end,
            equity_sample_interval_seconds=_EQUITY_BUCKET_SECONDS,
            portfolio_summary={
                "balance": None
                if latest_equity is None
                else str(latest_equity.balance),
                "equity": None if latest_equity is None else str(latest_equity.equity),
                "realized_pnl": None
                if latest_equity is None
                else str(latest_equity.realized_pnl),
                "unrealized_pnl": None
                if latest_equity is None
                else str(latest_equity.unrealized_pnl),
                "total_fees": None
                if latest_equity is None
                else str(latest_equity.total_fees),
                "open_position_count": len(open_positions),
                "closed_trade_count": total_closed_trades,
                "win_rate": None
                if total_closed_trades == 0
                else str(total_winning_trades / total_closed_trades),
            },
            equity_curve=[
                {
                    "observed_at": row.observed_at.isoformat(),
                    "balance": str(row.balance),
                    "equity": str(row.equity),
                    "realized_pnl": str(row.realized_pnl),
                    "unrealized_pnl": str(row.unrealized_pnl),
                }
                for row in equity
            ],
            open_positions=[_paper_position(row) for row in open_positions],
            closed_trades=[_paper_position(row) for row in closed_positions],
            trade_events=trade_events,
            latest_signals=[
                {
                    "signal_id": row.signal_id,
                    "strategy_name": row.strategy_name,
                    "symbol": row.symbol,
                    "side": row.side,
                    "detected_at": row.detected_at.isoformat(),
                    "reason": row.reason,
                    "candidate_id": (
                        None
                        if (candidate := candidates_by_signal_id.get(row.signal_id))
                        is None
                        else candidate.candidate_id
                    ),
                    "requested_notional": (
                        None
                        if candidate is None or candidate.desired_notional is None
                        else str(candidate.desired_notional)
                    ),
                    "features": _json_mapping(row.features),
                    "reference_prices": _json_mapping(row.reference_prices),
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
                    "quantity": None if row.quantity is None else str(row.quantity),
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
        process: ExecutionAccountProcessStateRow | None = None
        reconciliation: AccountReconciliationRunRow | None = None
        account_config: AccountConfigSnapshotRow | None = None
        balances: Sequence[AccountBalanceSnapshotRow] = ()
        positions: Sequence[AccountPositionSnapshotRow] = ()
        orders: Sequence[AccountOpenOrderRow] = ()
        fills: Sequence[AccountFillEventRow] = ()
        execution_orders: Sequence[ExchangeOrderRow] = ()
        intent_rows: Sequence[OrderIntentExecutionRow] = ()
        async with self._session_factory() as session:
            process = await session.scalar(
                select(ExecutionAccountProcessStateRow)
                .order_by(ExecutionAccountProcessStateRow.occurred_at.desc())
                .limit(1)
            )
            if process is not None:
                environment = process.environment
                account_label = process.account_label
                account_config = await session.scalar(
                    select(AccountConfigSnapshotRow)
                    .where(
                        AccountConfigSnapshotRow.environment == environment,
                        AccountConfigSnapshotRow.account_label == account_label,
                    )
                    .order_by(AccountConfigSnapshotRow.observed_at.desc())
                    .limit(1)
                )
                reconciliation = await session.scalar(
                    select(AccountReconciliationRunRow)
                    .where(
                        AccountReconciliationRunRow.environment == environment,
                        AccountReconciliationRunRow.account_label == account_label,
                        AccountReconciliationRunRow.status == "ready",
                    )
                    .order_by(AccountReconciliationRunRow.observed_at.desc())
                    .limit(1)
                )
                balance_at = await session.scalar(
                    select(func.max(AccountBalanceSnapshotRow.observed_at)).where(
                        AccountBalanceSnapshotRow.environment == environment,
                        AccountBalanceSnapshotRow.account_label == account_label,
                    )
                )
                if balance_at is not None:
                    balances = (
                        await session.scalars(
                            select(AccountBalanceSnapshotRow).where(
                                AccountBalanceSnapshotRow.environment == environment,
                                AccountBalanceSnapshotRow.account_label
                                == account_label,
                                AccountBalanceSnapshotRow.observed_at == balance_at,
                            )
                        )
                    ).all()
                if reconciliation is not None and reconciliation.position_count > 0:
                    position_at = await session.scalar(
                        select(
                            func.max(AccountPositionSnapshotRow.observed_at)
                        ).where(
                            AccountPositionSnapshotRow.environment == environment,
                            AccountPositionSnapshotRow.account_label == account_label,
                        )
                    )
                    if position_at is not None:
                        positions = (
                            await session.scalars(
                                select(AccountPositionSnapshotRow).where(
                                    AccountPositionSnapshotRow.environment
                                    == environment,
                                    AccountPositionSnapshotRow.account_label
                                    == account_label,
                                    AccountPositionSnapshotRow.observed_at
                                    == position_at,
                                    AccountPositionSnapshotRow.position_amt != 0,
                                )
                            )
                        ).all()
                orders = (
                    await session.scalars(
                        select(AccountOpenOrderRow)
                        .where(
                            AccountOpenOrderRow.environment == environment,
                            AccountOpenOrderRow.account_label == account_label,
                        )
                        .order_by(AccountOpenOrderRow.observed_at.desc())
                        .limit(20)
                    )
                ).all()
                fills = (
                    await session.scalars(
                        select(AccountFillEventRow)
                        .where(
                            AccountFillEventRow.environment == environment,
                            AccountFillEventRow.account_label == account_label,
                        )
                        .order_by(AccountFillEventRow.trade_at.desc())
                        .limit(200)
                    )
                ).all()
                execution_orders = (
                    await session.scalars(
                        select(ExchangeOrderRow)
                        .order_by(ExchangeOrderRow.updated_at.desc())
                        .limit(200)
                    )
                ).all()
                intent_ids = {row.intent_id for row in execution_orders}
                if intent_ids:
                    intent_rows = (
                        await session.scalars(
                            select(OrderIntentExecutionRow).where(
                                OrderIntentExecutionRow.intent_id.in_(intent_ids)
                            )
                        )
                    ).all()

        execution_by_client = {
            row.client_order_id: row for row in execution_orders
        }
        intent_by_id = {row.intent_id: row for row in intent_rows}
        strategy_by_order = {
            row.exchange_order_id: intent_by_id[row.intent_id].strategy_name
            for row in execution_orders
            if row.exchange_order_id is not None
            and row.intent_id in intent_by_id
        }
        strategy_by_symbol: dict[str, str] = {}
        for order in execution_orders:
            intent = intent_by_id.get(order.intent_id)
            if (
                intent is not None
                and order.state == ExchangeOrderState.FILLED.value
                and not order.reduce_only
            ):
                strategy_by_symbol.setdefault(order.symbol, intent.strategy_name)

        recent_trades = _aggregate_account_fills(
            fills,
            strategy_by_order,
        )

        reconciliation_payload: dict[str, JsonValue] = (
            {}
            if reconciliation is None
            else {
                "status": reconciliation.status,
                "observed_at": reconciliation.observed_at.isoformat(),
                "balance_count": reconciliation.balance_count,
                "position_count": reconciliation.position_count,
                "open_order_count": reconciliation.open_order_count,
                "fill_count": reconciliation.fill_count,
                "mismatch_count": reconciliation.mismatch_count,
            }
        )
        account_config_payload: dict[str, JsonValue] = (
            {}
            if account_config is None
            else {
                "multi_assets_mode": account_config.multi_assets_mode,
                "hedge_mode": account_config.hedge_mode,
                "can_trade": account_config.can_trade,
                "fee_tier": account_config.fee_tier,
                "observed_at": account_config.observed_at.isoformat(),
            }
        )
        usdt = next((row for row in balances if row.asset == "USDT"), None)
        total_unrealized = sum(
            (row.unrealized_pnl for row in balances),
            start=Decimal("0"),
        )
        total_notional = sum(
            (abs(row.notional) for row in positions),
            start=Decimal("0"),
        )
        observed_at = None if process is None else process.occurred_at
        return AccountOverviewResponse(
            status=OperationalStatus.UNKNOWN
            if process is None
            else OperationalStatus.READY
            if process.state == "ready_readonly"
            else OperationalStatus.HALTED,
            observed_at=observed_at,
            environment=None if process is None else process.environment,
            account_label=None if process is None else process.account_label,
            account_config=account_config_payload,
            reconciliation=reconciliation_payload,
            summary={
                "usdt_wallet_balance": (
                    None if usdt is None else str(usdt.wallet_balance)
                ),
                "usdt_available_balance": (
                    None if usdt is None else str(usdt.available_balance)
                ),
                "total_unrealized_pnl": str(total_unrealized),
                "gross_position_notional": str(total_notional),
                "position_count": len(positions),
                "open_order_count": len(orders),
                "recent_trade_count": len(recent_trades),
                "recent_fill_count": len(fills),
            },
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
                    "position_side": row.position_side,
                    "position_amt": str(row.position_amt),
                    "entry_price": str(row.entry_price),
                    "notional": str(row.notional),
                    "unrealized_pnl": str(row.unrealized_pnl),
                    "leverage": row.leverage,
                    "mark_price": str(row.mark_price),
                    "margin_type": row.margin_type,
                    "strategy_name": strategy_by_symbol.get(row.symbol),
                    "entry_notional": str(
                        abs(row.position_amt * row.entry_price)
                    ),
                }
                for row in positions
            ],
            open_orders=[
                {
                    "symbol": row.symbol,
                    "client_order_id": row.client_order_id,
                    "side": row.side,
                    "order_type": row.order_type,
                    "price": str(row.price),
                    "original_quantity": str(row.original_quantity),
                    "executed_quantity": str(row.executed_quantity),
                    "remaining_quantity": str(
                        max(
                            Decimal("0"),
                            row.original_quantity - row.executed_quantity,
                        )
                    ),
                    "status": row.status,
                    "reduce_only": row.reduce_only,
                    "observed_at": row.observed_at.isoformat(),
                    "strategy_name": (
                        None
                        if (internal := execution_by_client.get(row.client_order_id))
                        is None
                        or (intent := intent_by_id.get(internal.intent_id)) is None
                        else intent.strategy_name
                    ),
                }
                for row in orders
            ],
            fills=recent_trades,
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
            if shadow or live
            else OperationalStatus.NO_DATA,
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


def _downsample_equity_snapshots(
    rows: Sequence[PaperEquitySnapshotRow],
    *,
    interval_seconds: int = _EQUITY_BUCKET_SECONDS,
    max_points: int = _EQUITY_MAX_POINTS,
) -> list[PaperEquitySnapshotRow]:
    latest_by_bucket: dict[int, PaperEquitySnapshotRow] = {}
    for row in rows:
        observed_at = row.observed_at
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=UTC)
        bucket = int(observed_at.timestamp()) // interval_seconds
        latest_by_bucket[bucket] = row
    ordered = [latest_by_bucket[key] for key in sorted(latest_by_bucket)]
    return ordered[-max_points:]


def _paper_exit_details(run: StrategyRunRow) -> tuple[str, str]:
    portfolio_config = run.execution_config.get("portfolio")
    exit_mode = (
        str(portfolio_config.get("exit_mode"))
        if isinstance(portfolio_config, dict)
        and portfolio_config.get("exit_mode") is not None
        else "fixed"
    )
    entry_filter = run.execution_config.get("entry_filter")
    return exit_mode, _paper_exit_label(exit_mode, portfolio_config, entry_filter)


def _paper_account_summary(
    run: StrategyRunRow,
    *,
    checkpoint_at: datetime | None,
    open_position_count: int,
    closed_trade_count: int,
    winning_trade_count: int,
    latest_equity: PaperEquitySnapshotRow | None,
) -> PaperAccountSummaryResponse:
    exit_mode, exit_label = _paper_exit_details(run)
    return PaperAccountSummaryResponse(
        status=OperationalStatus.READY,
        run_id=run.run_id,
        strategy_name=run.strategy_name,
        exit_mode=exit_mode,
        exit_label=exit_label,
        config_hash=run.config_hash,
        checkpoint_at=checkpoint_at,
        portfolio_summary={
            "balance": None if latest_equity is None else str(latest_equity.balance),
            "equity": None if latest_equity is None else str(latest_equity.equity),
            "realized_pnl": (
                None if latest_equity is None else str(latest_equity.realized_pnl)
            ),
            "unrealized_pnl": (
                None
                if latest_equity is None
                else str(latest_equity.unrealized_pnl)
            ),
            "total_fees": (
                None if latest_equity is None else str(latest_equity.total_fees)
            ),
            "open_position_count": open_position_count,
            "closed_trade_count": closed_trade_count,
            "win_rate": (
                None
                if closed_trade_count == 0
                else str(winning_trade_count / closed_trade_count)
            ),
        },
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
        "current_price": None if row.current_price is None else str(row.current_price),
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


def _paper_position(row: PaperPositionRow) -> dict[str, JsonValue]:
    return {
        "position_id": row.position_id,
        "symbol": row.symbol,
        "side": row.side,
        "status": row.status,
        "opened_at": row.opened_at.isoformat(),
        "closed_at": None if row.closed_at is None else row.closed_at.isoformat(),
        "entry_price": str(row.entry_price),
        "exit_price": None if row.exit_price is None else str(row.exit_price),
        "last_mark_price": str(row.last_mark_price),
        "quantity": str(row.quantity),
        "entry_notional": str(row.entry_notional),
        "unrealized_pnl": str(row.unrealized_pnl),
        "realized_pnl": None if row.realized_pnl is None else str(row.realized_pnl),
        "return_pct": None if row.return_pct is None else str(row.return_pct),
        "fees": str(row.entry_fee + row.exit_fee),
        "close_reason": row.close_reason,
    }


def _position_open_event(row: PaperPositionRow) -> dict[str, JsonValue]:
    is_long = row.side == "long"
    return {
        "occurred_at": row.opened_at.isoformat(),
        "symbol": row.symbol,
        "event": "OPEN_LONG" if is_long else "OPEN_SHORT",
        "label": "开多" if is_long else "开空",
        "order_action": "BUY" if is_long else "SELL",
        "price": str(row.entry_price),
        "quantity": str(row.quantity),
        "pnl": None,
        "reason": "strategy_signal",
    }


def _position_close_event(row: PaperPositionRow) -> dict[str, JsonValue]:
    is_long = row.side == "long"
    return {
        "occurred_at": None if row.closed_at is None else row.closed_at.isoformat(),
        "symbol": row.symbol,
        "event": "CLOSE_LONG" if is_long else "CLOSE_SHORT",
        "label": "平多" if is_long else "平空",
        "order_action": "SELL" if is_long else "BUY",
        "price": None if row.exit_price is None else str(row.exit_price),
        "quantity": str(row.quantity),
        "pnl": None if row.realized_pnl is None else str(row.realized_pnl),
        "reason": row.close_reason,
    }


def _paper_exit_label(
    exit_mode: str,
    portfolio_config: object,
    entry_filter: object = None,
) -> str:
    if exit_mode != "candle_15m":
        return "固定 TP / SL"
    if not isinstance(portfolio_config, dict):
        return "15M 收线退出"
    confirmation_count = _config_int(
        portfolio_config.get("candle_confirmation_count"),
        default=1,
    )
    grace_bars = _config_int(
        portfolio_config.get("candle_grace_bars"),
        default=0,
    )
    grace_profit_pct = _config_decimal(
        portfolio_config.get("candle_grace_profit_pct"),
        default=Decimal("0"),
    )
    minimum_buckets = _config_int(
        portfolio_config.get("candle_minimum_holding_buckets"),
        default=0,
    )
    label = "15M 收线退出"
    if grace_bars > 0:
        label = f"反向后宽限 {grace_bars} 根 15M"
        if grace_profit_pct > 0:
            percentage = (grace_profit_pct * 100).normalize()
            label += f" · 回收 +{percentage:f}%"
    elif confirmation_count > 1:
        label = f"{confirmation_count} 根反向 15M 收线"
    elif minimum_buckets > 0:
        minutes = minimum_buckets * 15 // 60
        label = f"持仓 {minutes} 分钟后反向 15M 收线"
    filter_label = _paper_entry_filter_label(entry_filter)
    return f"{label} · {filter_label}" if filter_label else label


def _paper_entry_filter_label(entry_filter: object) -> str:
    if not isinstance(entry_filter, dict):
        return ""
    allow_long = entry_filter.get("allow_long", True)
    allow_short = entry_filter.get("allow_short", True)
    parts: list[str] = []
    if allow_long is True and allow_short is False:
        parts.append("仅多头")
    elif allow_long is False and allow_short is True:
        parts.append("仅空头")
    elif allow_long is False and allow_short is False:
        parts.append("无方向")

    max_imbalance = entry_filter.get("max_abs_aggressive_imbalance")
    if max_imbalance is not None:
        try:
            percentage = Decimal(str(max_imbalance)) * 100
            parts.append(f"主动不平衡 ≤ {percentage:.2f}%")
        except (ArithmeticError, ValueError):
            parts.append(f"主动不平衡 ≤ {max_imbalance}")

    max_cluster_trade_count = entry_filter.get("max_cluster_trade_count")
    if max_cluster_trade_count is not None:
        parts.append(f"成交簇 ≤ {max_cluster_trade_count} 笔")
    return " · ".join(parts)


def _config_int(value: object, *, default: int) -> int:
    try:
        if isinstance(value, int | str):
            return int(value)
        return default
    except (TypeError, ValueError):
        return default


def _config_decimal(value: object, *, default: Decimal) -> Decimal:
    try:
        if isinstance(value, Decimal | int | float | str):
            return Decimal(str(value))
        return default
    except (ArithmeticError, TypeError, ValueError):
        return default


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
