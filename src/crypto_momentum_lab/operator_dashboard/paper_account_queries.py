"""Paper-account read models for the operator dashboard.

This module owns paper-run selection, account summaries, position history, and
strategy-run detail.  It keeps paper-specific policy (which runs are visible,
how exits are labeled, and how positions become response events) together
while leaving equity time-series SQL to ``paper_equity_queries``.
"""

from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.operator_dashboard.paper_equity_queries import (
    _paper_latest_equity_statement,
)
from crypto_momentum_lab.operator_dashboard.schemas import (
    PaperAccountHistoryResponse,
    PaperAccountsResponse,
    PaperAccountSummaryResponse,
    StrategyRunResponse,
)
from crypto_momentum_lab.operator_dashboard.status import (
    OperationalStatus,
    freshness_status,
)
from crypto_momentum_lab.persistence.postgres.models import (
    OrderIntentCandidateRow,
    PaperEquitySnapshotRow,
    PaperFillRow,
    PaperPositionRow,
    StrategyRunRow,
    StrategyRuntimeCheckpointRow,
    StrategySignalRow,
)

_EQUITY_WINDOW = timedelta(hours=24)
_EQUITY_BUCKET_SECONDS = 6 * 60
_EQUITY_MAX_POINTS = 240
_PAPER_HISTORY_RECENT_LIMIT = 500


@dataclass(frozen=True, slots=True)
class _PaperEquitySummaryPoint:
    """Narrow latest paper-equity projection used by account summaries."""

    balance: Decimal
    equity: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    total_fees: Decimal


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


def _is_dashboard_paper_run(run: StrategyRunRow) -> bool:
    return _paper_exit_details(run)[0] != "fixed"


def _paper_account_summary(
    run: StrategyRunRow,
    *,
    now: datetime,
    stale_after_seconds: float,
    checkpoint_at: datetime | None,
    open_position_count: int,
    closed_trade_count: int,
    winning_trade_count: int,
    latest_equity: PaperEquitySnapshotRow | _PaperEquitySummaryPoint | None,
) -> PaperAccountSummaryResponse:
    exit_mode, exit_label = _paper_exit_details(run)
    return PaperAccountSummaryResponse(
        status=freshness_status(
            now=now,
            observed_at=checkpoint_at,
            stale_after_seconds=stale_after_seconds,
        ),
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


class PaperAccountQueries:
    """Read and assemble paper-account responses for the dashboard."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        clock: Callable[[], datetime],
        stale_after_seconds: float,
        paper_run_ids: frozenset[str] | None,
    ) -> None:
        self._session_factory = session_factory
        self._clock = clock
        self._stale_after_seconds = stale_after_seconds
        self._paper_run_ids = paper_run_ids

    async def paper_accounts(self) -> PaperAccountsResponse:
        async with self._session_factory() as session:
            selected_runs = await self.selected_paper_runs(session)
            accounts = await self._paper_account_summaries(session, selected_runs)
        return PaperAccountsResponse(
            status=(OperationalStatus.READY if accounts else OperationalStatus.NO_DATA),
            accounts=accounts,
        )

    async def selected_paper_runs(
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

        selected_runs = [run for run in selected_runs if _is_dashboard_paper_run(run)]

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
            selected.extend(strategy_runs)
        return selected

    async def _paper_account_summaries(
        self,
        session: AsyncSession,
        runs: Sequence[StrategyRunRow],
    ) -> list[PaperAccountSummaryResponse]:
        if not runs:
            return []
        now = self._clock()
        run_ids = [run.run_id for run in runs]
        checkpoints = (
            await session.execute(
                select(
                    StrategyRuntimeCheckpointRow.run_id,
                    StrategyRuntimeCheckpointRow.saved_at,
                ).where(StrategyRuntimeCheckpointRow.run_id.in_(run_ids))
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
        latest_equity_rows = (
            await session.execute(_paper_latest_equity_statement(run_ids))
        ).all()
        checkpoint_by_run = {row.run_id: row.saved_at for row in checkpoints}
        open_count_by_run: dict[str, int] = {}
        for run_id in open_positions:
            open_count_by_run[run_id] = open_count_by_run.get(run_id, 0) + 1
        closed_stats_by_run = {
            row.run_id: (int(row.closed_count), int(row.winning_count or 0))
            for row in closed_stats
        }
        latest_equity_by_run = {
            row.run_id: _PaperEquitySummaryPoint(
                balance=row.balance,
                equity=row.equity,
                realized_pnl=row.realized_pnl,
                unrealized_pnl=row.unrealized_pnl,
                total_fees=row.total_fees,
            )
            for row in latest_equity_rows
        }
        return [
            _paper_account_summary(
                run,
                now=now,
                stale_after_seconds=self._stale_after_seconds,
                checkpoint_at=checkpoint_by_run.get(run.run_id),
                open_position_count=open_count_by_run.get(run.run_id, 0),
                closed_trade_count=closed_stats_by_run.get(run.run_id, (0, 0))[0],
                winning_trade_count=closed_stats_by_run.get(run.run_id, (0, 0))[1],
                latest_equity=latest_equity_by_run.get(run.run_id),
            )
            for run in runs
        ]

    async def paper_history(
        self,
        run_id: str,
        *,
        full: bool = False,
    ) -> PaperAccountHistoryResponse:
        async with self._session_factory() as session:
            run_exists = await session.scalar(
                select(StrategyRunRow.run_id).where(StrategyRunRow.run_id == run_id)
            )
            closed_trade_count = await session.scalar(
                select(func.count(PaperPositionRow.position_id)).where(
                    PaperPositionRow.run_id == run_id,
                    PaperPositionRow.status == "closed",
                )
            )
            if full:
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
            else:
                open_positions = (
                    await session.scalars(
                        select(PaperPositionRow)
                        .where(
                            PaperPositionRow.run_id == run_id,
                            PaperPositionRow.status == "open",
                        )
                        .order_by(
                            PaperPositionRow.opened_at.desc(),
                            PaperPositionRow.position_id,
                        )
                    )
                ).all()
                recent_closed_positions = (
                    await session.scalars(
                        select(PaperPositionRow)
                        .where(
                            PaperPositionRow.run_id == run_id,
                            PaperPositionRow.status == "closed",
                        )
                        .order_by(
                            PaperPositionRow.closed_at.desc().nullslast(),
                            PaperPositionRow.position_id,
                        )
                        .limit(_PAPER_HISTORY_RECENT_LIMIT)
                    )
                ).all()
                positions = [*open_positions, *recent_closed_positions]
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
            closed_trade_count=int(closed_trade_count or 0),
            history_complete=(
                full
                or int(closed_trade_count or 0) <= _PAPER_HISTORY_RECENT_LIMIT
            ),
            closed_trades=[_paper_position(row) for row in closed_positions],
            trade_events=trade_events,
        )

    async def strategy_run(
        self,
        run_id: str | None = None,
        *,
        equity_window_end: datetime | None = None,
        session: AsyncSession | None = None,
    ) -> StrategyRunResponse:
        window_end = equity_window_end or self._clock()
        window_start = window_end - _EQUITY_WINDOW
        async with _session_scope(self._session_factory, session) as db_session:
            statement = select(StrategyRunRow)
            if run_id is not None:
                statement = statement.where(StrategyRunRow.run_id == run_id)
            run = await db_session.scalar(
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
                await db_session.scalars(
                    select(StrategySignalRow)
                    .where(StrategySignalRow.run_id == run.run_id)
                    .order_by(StrategySignalRow.detected_at.desc())
                    .limit(20)
                )
            ).all()
            candidates_by_signal_id = {
                row.signal_id: row
                for row in (
                    await db_session.scalars(
                        select(OrderIntentCandidateRow).where(
                            OrderIntentCandidateRow.signal_id.in_(
                                [signal.signal_id for signal in signals]
                            )
                        )
                    )
                ).all()
            }
            checkpoint_at = await db_session.scalar(
                select(StrategyRuntimeCheckpointRow.saved_at).where(
                    StrategyRuntimeCheckpointRow.run_id == run.run_id,
                )
            )
            fills = (
                await db_session.scalars(
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
                await db_session.scalars(
                    select(PaperPositionRow)
                    .where(
                        PaperPositionRow.run_id == run.run_id,
                        PaperPositionRow.status == "open",
                    )
                    .order_by(PaperPositionRow.opened_at.desc())
                )
            ).all()
            closed_positions = (
                await db_session.scalars(
                    select(PaperPositionRow)
                    .where(
                        PaperPositionRow.run_id == run.run_id,
                        PaperPositionRow.status == "closed",
                    )
                    .order_by(PaperPositionRow.closed_at.desc())
                    .limit(30)
                )
            ).all()
            closed_trade_count = await db_session.scalar(
                select(func.count(PaperPositionRow.position_id)).where(
                    PaperPositionRow.run_id == run.run_id,
                    PaperPositionRow.status == "closed",
                )
            )
            winning_trade_count = await db_session.scalar(
                select(func.count(PaperPositionRow.position_id)).where(
                    PaperPositionRow.run_id == run.run_id,
                    PaperPositionRow.status == "closed",
                    PaperPositionRow.realized_pnl > 0,
                )
            )
            latest_equity = await db_session.scalar(
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
                await db_session.scalars(
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
                "equity": None
                if latest_equity is None
                else str(latest_equity.equity),
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
    if entry_filter.get("require_price_above_ema5") is True:
        parts.append("价格 > 15M EMA5")
    if entry_filter.get("require_price_above_ema10") is True:
        parts.append("价格 > 15M EMA10")
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


__all__ = [
    "PaperAccountQueries",
    "_PaperEquitySummaryPoint",
    "_downsample_equity_snapshots",
    "_is_dashboard_paper_run",
    "_paper_account_summary",
    "_paper_exit_details",
    "_paper_exit_label",
    "_paper_latest_equity_statement",
]
