"""Paper-account equity curves for the operator dashboard.

This module owns the bounded time-series read model for paper accounts and its
live-account comparison curves.  Run selection and paper exit labeling remain
facade policies and are injected as narrow callbacks so this module can focus
on the SQL projections and response assembly.
"""

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy import Select, String, column, func, select, text, true, values
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import aliased
from sqlalchemy.sql.selectable import Values

from crypto_momentum_lab.operator_dashboard.common_equity import (
    LiveCashFlowAdjustment,
    build_common_equity_result,
    live_account_equity_point,
)
from crypto_momentum_lab.operator_dashboard.common_equity import (
    as_utc as _as_utc,
)
from crypto_momentum_lab.operator_dashboard.common_equity import (
    bucket_start as _bucket_start,
)
from crypto_momentum_lab.operator_dashboard.common_equity import (
    common_equity_interval_seconds as _common_equity_interval_seconds,
)
from crypto_momentum_lab.operator_dashboard.common_equity import (
    relative_bucket_end as _relative_bucket_end,
)
from crypto_momentum_lab.operator_dashboard.live_account_metrics_queries import (
    AccountEquityPoint,
    account_equity_statement,
)
from crypto_momentum_lab.operator_dashboard.overview_queries import (
    account_label_sort_key,
    latest_live_account_process_statement,
)
from crypto_momentum_lab.operator_dashboard.schemas import (
    PaperAccountEquityResponse,
    PaperAccountsEquityResponse,
)
from crypto_momentum_lab.operator_dashboard.status import OperationalStatus
from crypto_momentum_lab.persistence.postgres.models import (
    AccountBalanceSnapshotRow,
    ExecutionAccountProcessStateRow,
    PaperEquitySnapshotRow,
    StrategyLiveStateRow,
    StrategyRunRow,
)

_EQUITY_WINDOW = timedelta(hours=24)
_EQUITY_BUCKET_SECONDS = 6 * 60
_EQUITY_MAX_POINTS = 240
_COMMON_EQUITY_BUCKET_SECONDS = 15 * 60


@dataclass(frozen=True, slots=True)
class _PaperEquityPoint:
    """Scalar paper-equity projection used by the dashboard curve."""

    run_id: str
    observed_at: datetime
    balance: Decimal
    equity: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal


PaperRunSelector = Callable[
    [AsyncSession], Awaitable[list[StrategyRunRow]]
]
PaperExitDetails = Callable[[StrategyRunRow], tuple[str, str]]


def _paper_run_values(run_ids: Sequence[str]) -> Values:
    if not run_ids:
        raise ValueError("run_ids must not be empty")
    return values(
        column("run_id", String(128)),
        name="paper_run_ids",
    ).data([(run_id,) for run_id in run_ids])


def _paper_first_equity_statement(
    run_ids: Sequence[str],
) -> Select[tuple[str, datetime]]:
    """Find each run's first valid equity with one index probe per run."""
    run_values = _paper_run_values(run_ids)
    snapshot = aliased(PaperEquitySnapshotRow)
    first_equity = (
        select(snapshot.observed_at.label("first_at"))
        .where(
            snapshot.run_id == run_values.c.run_id,
            snapshot.equity > 0,
        )
        .order_by(snapshot.observed_at)
        .limit(1)
        .lateral("first_equity")
    )
    return (
        select(run_values.c.run_id, first_equity.c.first_at)
        .select_from(run_values.join(first_equity, true()))
        .order_by(run_values.c.run_id)
    )


def _paper_latest_equity_statement(
    run_ids: Sequence[str],
) -> Select[tuple[str, Decimal, Decimal, Decimal, Decimal, Decimal]]:
    """Fetch one narrow latest-equity row per run with an index probe."""
    run_values = _paper_run_values(run_ids)
    snapshot = aliased(PaperEquitySnapshotRow)
    latest_equity = (
        select(
            snapshot.balance.label("balance"),
            snapshot.equity.label("equity"),
            snapshot.realized_pnl.label("realized_pnl"),
            snapshot.unrealized_pnl.label("unrealized_pnl"),
            snapshot.total_fees.label("total_fees"),
        )
        .where(snapshot.run_id == run_values.c.run_id)
        .order_by(snapshot.observed_at.desc(), snapshot.snapshot_id.desc())
        .limit(1)
        .lateral("latest_equity")
    )
    return (
        select(
            run_values.c.run_id,
            latest_equity.c.balance,
            latest_equity.c.equity,
            latest_equity.c.realized_pnl,
            latest_equity.c.unrealized_pnl,
            latest_equity.c.total_fees,
        )
        .select_from(run_values.join(latest_equity, true()))
        .order_by(run_values.c.run_id)
    )


def _paper_common_equity_statement(
    run_ids: Sequence[str],
    common_start_at: datetime,
    window_end: datetime,
    *,
    interval_seconds: int | None = None,
    max_points: int = _EQUITY_MAX_POINTS,
) -> Select[tuple[str, datetime, Decimal]]:
    """Fetch the latest valid snapshot in each run/bucket.

    The sampling interval expands with the requested history so the generated
    bucket series stays bounded even as the accounts run indefinitely.
    """
    if max_points <= 0:
        raise ValueError("max_points must be positive")
    if common_start_at > window_end:
        raise ValueError("common_start_at must not be later than window_end")
    if interval_seconds is not None and interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    minimum_interval_seconds = _common_equity_interval_seconds(
        common_start_at,
        window_end,
        max_points=max_points,
    )
    resolved_interval_seconds = max(
        minimum_interval_seconds,
        _COMMON_EQUITY_BUCKET_SECONDS
        if interval_seconds is None
        else interval_seconds,
    )
    if resolved_interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    bucket_interval = text(f"interval '{resolved_interval_seconds} seconds'")
    run_values = _paper_run_values(run_ids)
    bucket_series = func.generate_series(
        common_start_at,
        _relative_bucket_end(
            common_start_at,
            window_end,
            resolved_interval_seconds,
        ),
        bucket_interval,
    ).table_valued("bucket").render_derived(name="equity_buckets")
    snapshot = aliased(PaperEquitySnapshotRow)
    bucket_start_at = bucket_series.c.bucket
    latest_equity = (
        select(
            snapshot.run_id.label("run_id"),
            snapshot.observed_at.label("observed_at"),
            snapshot.equity.label("equity"),
        )
        .where(
            snapshot.run_id == run_values.c.run_id,
            snapshot.equity > 0,
            snapshot.observed_at >= bucket_start_at,
            snapshot.observed_at < bucket_start_at + bucket_interval,
            snapshot.observed_at <= window_end,
        )
        .order_by(snapshot.observed_at.desc())
        .limit(1)
        .lateral("latest_equity")
    )
    return (
        select(
            latest_equity.c.run_id,
            latest_equity.c.observed_at,
            latest_equity.c.equity,
        )
        .select_from(
            run_values.join(bucket_series, true()).join(latest_equity, true())
        )
        .order_by(run_values.c.run_id, bucket_start_at)
    )


def _paper_equity_statement(
    run_ids: Sequence[str],
    window_start: datetime,
    window_end: datetime,
    interval_seconds: int = _EQUITY_BUCKET_SECONDS,
    max_points: int = _EQUITY_MAX_POINTS,
) -> Select[tuple[str, datetime, Decimal, Decimal, Decimal, Decimal]]:
    """Fetch one narrow, latest paper snapshot per run and time bucket."""
    if not run_ids:
        raise ValueError("run_ids must not be empty")
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    if max_points <= 0:
        raise ValueError("max_points must be positive")
    if window_start > window_end:
        raise ValueError("window_start must not be later than window_end")

    bucket_interval = text(f"interval '{interval_seconds} seconds'")
    end_bucket = _bucket_start(window_end, interval_seconds)
    earliest_bucket = _bucket_start(window_start, interval_seconds)
    latest_window_start = end_bucket - timedelta(
        seconds=interval_seconds * (max_points - 1)
    )
    series_start = max(earliest_bucket, latest_window_start)
    run_values = _paper_run_values(run_ids)
    bucket_series = func.generate_series(
        series_start,
        end_bucket,
        bucket_interval,
    ).table_valued("bucket").render_derived(name="equity_buckets")
    snapshot = aliased(PaperEquitySnapshotRow)
    bucket_start_at = bucket_series.c.bucket
    latest_equity = (
        select(
            snapshot.run_id.label("run_id"),
            snapshot.observed_at.label("observed_at"),
            snapshot.balance.label("balance"),
            snapshot.equity.label("equity"),
            snapshot.realized_pnl.label("realized_pnl"),
            snapshot.unrealized_pnl.label("unrealized_pnl"),
        )
        .where(
            snapshot.run_id == run_values.c.run_id,
            snapshot.observed_at >= window_start,
            snapshot.observed_at <= window_end,
            snapshot.observed_at >= bucket_start_at,
            snapshot.observed_at < bucket_start_at + bucket_interval,
        )
        .order_by(snapshot.observed_at.desc())
        .limit(1)
        .lateral("latest_equity")
    )
    return (
        select(
            latest_equity.c.run_id,
            latest_equity.c.observed_at,
            latest_equity.c.balance,
            latest_equity.c.equity,
            latest_equity.c.realized_pnl,
            latest_equity.c.unrealized_pnl,
        )
        .select_from(
            run_values.join(bucket_series, true()).join(latest_equity, true())
        )
        .order_by(latest_equity.c.run_id, bucket_start_at)
    )


def _live_common_equity_statement(
    *,
    environment: str,
    account_label: str,
    window_start: datetime,
    window_end: datetime,
    interval_seconds: int = _COMMON_EQUITY_BUCKET_SECONDS,
    max_points: int = _EQUITY_MAX_POINTS,
) -> Select[tuple[datetime, Decimal]]:
    """Aggregate live assets at the latest timestamp in each common bucket."""
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    if max_points <= 0:
        raise ValueError("max_points must be positive")
    if window_start > window_end:
        raise ValueError("window_start must not be later than window_end")

    minimum_interval_seconds = _common_equity_interval_seconds(
        window_start,
        window_end,
        max_points=max_points,
    )
    resolved_interval_seconds = max(interval_seconds, minimum_interval_seconds)
    if resolved_interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    bucket_interval = text(f"interval '{resolved_interval_seconds} seconds'")
    end_bucket = _relative_bucket_end(
        window_start,
        window_end,
        resolved_interval_seconds,
    )
    earliest_bucket = _as_utc(window_start)
    latest_window_start = end_bucket - timedelta(
        seconds=resolved_interval_seconds * (max_points - 1)
    )
    series_start = max(earliest_bucket, latest_window_start)
    bucket_series = func.generate_series(
        series_start,
        end_bucket,
        bucket_interval,
    ).table_valued("bucket").render_derived(name="equity_buckets")
    snapshot = aliased(AccountBalanceSnapshotRow)
    bucket_start_at = bucket_series.c.bucket
    total_equity = snapshot.wallet_balance + snapshot.unrealized_pnl
    latest_equity = (
        select(
            snapshot.observed_at.label("observed_at"),
            func.sum(total_equity).label("equity"),
        )
        .where(
            snapshot.environment == environment,
            snapshot.account_label == account_label,
            snapshot.observed_at >= window_start,
            snapshot.observed_at <= window_end,
            snapshot.observed_at >= bucket_start_at,
            snapshot.observed_at < bucket_start_at + bucket_interval,
        )
        .group_by(snapshot.observed_at)
        .having(func.sum(total_equity) > 0)
        .order_by(snapshot.observed_at.desc())
        .limit(1)
        .lateral("latest_equity")
    )
    return (
        select(latest_equity.c.observed_at, latest_equity.c.equity)
        .select_from(bucket_series.join(latest_equity, true()))
        .order_by(bucket_start_at)
    )


class PaperEquityQueries:
    """Read and assemble paper/live equity curves for dashboard responses."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        clock: Callable[[], datetime],
        select_paper_runs: PaperRunSelector,
        paper_exit_details: PaperExitDetails,
        live_cash_flow_adjustments: Sequence[LiveCashFlowAdjustment],
        common_equity_start_at: datetime | None,
    ) -> None:
        self._session_factory = session_factory
        self._clock = clock
        self._select_paper_runs = select_paper_runs
        self._paper_exit_details = paper_exit_details
        self._live_cash_flow_adjustments = tuple(live_cash_flow_adjustments)
        self._common_equity_start_at = common_equity_start_at

    async def paper_account_equity(self) -> PaperAccountsEquityResponse:
        window_end = self._clock()
        window_start = window_end - _EQUITY_WINDOW
        live_processes: Sequence[ExecutionAccountProcessStateRow] = ()
        live_balance_rows_by_account: dict[str, Sequence[AccountEquityPoint]] = {}
        common_paper_rows: Sequence[tuple[str, datetime, Decimal]] = ()
        common_live_equity_rows_by_account: dict[
            str,
            list[tuple[datetime, Decimal]],
        ] = {}
        live_strategy_names_by_account: dict[str, str | None] = {}
        paper_first_at_by_run: dict[str, datetime] = {}
        live_first_at_by_account: dict[str, datetime | None] = {}
        common_start_at: datetime | None = None
        common_equity_interval_seconds: int | None = None
        async with self._session_factory() as session:
            selected_runs = await self._select_paper_runs(session)
            run_ids = [run.run_id for run in selected_runs]
            rows: list[_PaperEquityPoint] = []
            if run_ids:
                rows = [
                    _PaperEquityPoint(
                        run_id=row.run_id,
                        observed_at=row.observed_at,
                        balance=row.balance,
                        equity=row.equity,
                        realized_pnl=row.realized_pnl,
                        unrealized_pnl=row.unrealized_pnl,
                    )
                    for row in (
                        await session.execute(
                            _paper_equity_statement(
                                run_ids,
                                window_start,
                                window_end,
                            )
                        )
                    ).all()
                ]
            live_processes = (
                await session.scalars(latest_live_account_process_statement())
            ).all()
            strategy_states = (
                await session.scalars(
                    select(StrategyLiveStateRow).where(
                        StrategyLiveStateRow.environment == "live"
                    )
                )
            ).all()
            strategy_by_account: dict[str, StrategyLiveStateRow] = {}
            for state in sorted(
                strategy_states,
                key=lambda item: item.changed_at,
                reverse=True,
            ):
                strategy_by_account.setdefault(state.account_label, state)
            for process in live_processes:
                account_label = process.account_label or "primary"
                live_strategy_names_by_account[account_label] = (
                    None
                    if account_label not in strategy_by_account
                    else strategy_by_account[account_label].strategy_name
                )
                live_balance_rows_by_account[account_label] = [
                    AccountEquityPoint(
                        observed_at=row.observed_at,
                        wallet_balance=row.wallet_balance,
                        unrealized_pnl=row.unrealized_pnl,
                    )
                    for row in (
                        await session.execute(
                            account_equity_statement(
                                environment="live",
                                account_label=process.account_label,
                                asset="USDT",
                                window_start=window_start,
                                window_end=window_end,
                                interval_seconds=_EQUITY_BUCKET_SECONDS,
                            )
                        )
                    ).all()
                ]
            if run_ids:
                paper_first_rows = (
                    await session.execute(_paper_first_equity_statement(run_ids))
                ).all()
                paper_first_at_by_run = {
                    run_id: first_at for run_id, first_at in paper_first_rows
                }

            for process in live_processes:
                account_label = process.account_label or "primary"
                live_first_at_by_account[account_label] = await session.scalar(
                    select(func.min(AccountBalanceSnapshotRow.observed_at)).where(
                        AccountBalanceSnapshotRow.environment == "live",
                        AccountBalanceSnapshotRow.account_label
                        == process.account_label,
                    )
                )

            first_buckets = {
                run_id: _bucket_start(first_at, _COMMON_EQUITY_BUCKET_SECONDS)
                for run_id, first_at in paper_first_at_by_run.items()
            }
            for account_label, live_first_at in live_first_at_by_account.items():
                if live_first_at is None:
                    continue
                live_run_id = f"live-{account_label}-b1"
                first_buckets[live_run_id] = _bucket_start(
                    live_first_at,
                    _COMMON_EQUITY_BUCKET_SECONDS,
                )
            if (
                self._common_equity_start_at is not None
                and self._common_equity_start_at <= window_end
            ):
                common_start_at = self._common_equity_start_at
                common_equity_interval_seconds = _common_equity_interval_seconds(
                    common_start_at,
                    window_end,
                )
                if run_ids:
                    common_paper_rows = [
                        (run_id, observed_at, equity)
                        for run_id, observed_at, equity in (
                            await session.execute(
                                _paper_common_equity_statement(
                                    run_ids,
                                    common_start_at,
                                    window_end,
                                    interval_seconds=common_equity_interval_seconds,
                                )
                            )
                        ).all()
                    ]
                for process in live_processes:
                    account_label = process.account_label or "primary"
                    common_live_equity_rows_by_account[account_label] = [
                        (observed_at, equity)
                        for observed_at, equity in (
                            await session.execute(
                                _live_common_equity_statement(
                                    environment="live",
                                    account_label=process.account_label,
                                    window_start=common_start_at,
                                    window_end=window_end,
                                    interval_seconds=common_equity_interval_seconds,
                                )
                            )
                        ).all()
                    ]

        common_equity_result = build_common_equity_result(
            paper_rows=common_paper_rows,
            live_rows_by_account=common_live_equity_rows_by_account,
            run_ids=run_ids,
            common_start_at=common_start_at,
            window_end=window_end,
            first_buckets=first_buckets,
            live_account_labels=live_balance_rows_by_account,
            cash_flow_adjustments=self._live_cash_flow_adjustments,
        )
        common_equity_by_run = common_equity_result.curves_by_run
        common_baseline_by_run = common_equity_result.baselines_by_run
        common_end_at = common_equity_result.end_at
        common_anchor_accounts = common_equity_result.anchor_accounts
        common_cash_flows = common_equity_result.cash_flows
        common_note = common_equity_result.note
        common_start_at = common_equity_result.start_at
        common_equity_interval_seconds = common_equity_result.interval_seconds

        rows_by_run: dict[str, list[_PaperEquityPoint]] = {}
        for row in rows:
            rows_by_run.setdefault(row.run_id, []).append(row)
        accounts = []
        for run in selected_runs:
            equity = sorted(
                rows_by_run.get(run.run_id, []),
                key=lambda row: row.observed_at,
            )
            exit_mode, exit_label = self._paper_exit_details(run)
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
                    common_equity_baseline=(
                        None
                        if run.run_id not in common_baseline_by_run
                        else str(common_baseline_by_run[run.run_id])
                    ),
                    common_equity_curve=common_equity_by_run.get(
                        run.run_id,
                        [],
                    ),
                )
            )
        for account_label in sorted(
            live_balance_rows_by_account,
            key=account_label_sort_key,
        ):
            live_balance_rows = live_balance_rows_by_account[account_label]
            if len(live_balance_rows) < 2:
                continue
            live_run_id = f"live-{account_label}-b1"
            accounts.append(
                PaperAccountEquityResponse(
                    run_id=live_run_id,
                    strategy_name=(
                        live_strategy_names_by_account.get(account_label)
                        or "orderflow_impulse"
                    ),
                    exit_mode="candle_15m",
                    exit_label=(
                        "实盘 Top10 · 反向后宽限 8 根 15M · 回收 +0.88% · 仅多头"
                    ),
                    equity_window_start=window_start,
                    equity_window_end=window_end,
                    equity_sample_interval_seconds=_EQUITY_BUCKET_SECONDS,
                    source="live",
                    account_label=account_label,
                    equity_curve=[
                        live_account_equity_point(row)
                        for row in sorted(
                            live_balance_rows,
                            key=lambda row: row.observed_at,
                        )
                    ],
                    common_equity_baseline=(
                        None
                        if live_run_id not in common_baseline_by_run
                        else str(common_baseline_by_run[live_run_id])
                    ),
                    common_equity_curve=common_equity_by_run.get(
                        live_run_id,
                        [],
                    ),
                )
            )
        return PaperAccountsEquityResponse(
            status=(
                OperationalStatus.READY if accounts else OperationalStatus.NO_DATA
            ),
            accounts=accounts,
            common_equity_start_at=(
                common_start_at if common_equity_by_run else None
            ),
            common_equity_end_at=common_end_at,
            common_equity_sample_interval_seconds=(
                common_equity_interval_seconds
                if common_equity_by_run
                else None
            ),
            common_equity_anchor=(
                "fixed_2026-08-21T02:45:00Z"
                if common_equity_by_run
                else None
            ),
            common_equity_anchor_accounts=common_anchor_accounts,
            common_equity_account_count=len(common_equity_by_run),
            common_equity_cash_flows=common_cash_flows,
            common_equity_note=common_note,
        )


__all__ = [
    "PaperEquityQueries",
    "_PaperEquityPoint",
    "_live_common_equity_statement",
    "_paper_common_equity_statement",
    "_paper_equity_statement",
    "_paper_first_equity_statement",
    "_paper_latest_equity_statement",
    "_paper_run_values",
]
