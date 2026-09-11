"""Live account equity and margin metrics for the operator dashboard.

This module owns the bounded time-series read model for live accounts.  Its
small external interface accepts only the requested history range; account
selection, PostgreSQL bucket probes, margin-source filtering, and derived
drawdown ratios remain local to this module.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal, cast
from zoneinfo import ZoneInfo

from sqlalchemy import (
    Numeric,
    Select,
    and_,
    func,
    or_,
    select,
    text,
    true,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import aliased

from crypto_momentum_lab.operator_dashboard.overview_queries import (
    latest_live_account_process_statement,
    live_account_fleet_status,
    live_account_summaries,
)
from crypto_momentum_lab.operator_dashboard.schemas import (
    LiveAccountMetricPointResponse,
    LiveAccountMetricsAccountResponse,
    LiveAccountMetricsResponse,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountBalanceSnapshotRow,
    AccountConfigSnapshotRow,
    AccountReconciliationRunRow,
    StrategyLiveStateRow,
    TradingLeaseRow,
)

_EQUITY_MAX_POINTS = 240
_LIVE_ACCOUNT_METRIC_MAX_POINTS = _EQUITY_MAX_POINTS + 1
_LIVE_ACCOUNT_METRIC_TIME_ZONE = ZoneInfo("Asia/Shanghai")
_LIVE_ACCOUNT_METRIC_ANCHOR_HOUR = 8
_ACCOUNT_EQUITY_RANGES: dict[str, tuple[timedelta, int]] = {
    "24h": (timedelta(hours=24), 6 * 60),
    "7d": (timedelta(days=7), 60 * 60),
    "30d": (timedelta(days=30), 3 * 60 * 60),
    "1y": (timedelta(days=365), 2 * 24 * 60 * 60),
}


@dataclass(frozen=True, slots=True)
class AccountEquityPoint:
    """Narrow account-equity projection used by the dashboard curve."""

    observed_at: datetime
    wallet_balance: Decimal
    unrealized_pnl: Decimal


def account_equity_range(value: str) -> tuple[timedelta, int]:
    try:
        return _ACCOUNT_EQUITY_RANGES[value]
    except KeyError as error:
        raise ValueError(f"unsupported account equity range: {value}") from error


def live_account_metrics_window_start(
    window_end: datetime,
    window: timedelta,
) -> datetime:
    """Return the first daily 08:00 (UTC+8) anchor inside the requested window."""
    if window <= timedelta(0):
        raise ValueError("window must be positive")
    requested_start = as_utc(window_end) - window
    local_start = requested_start.astimezone(_LIVE_ACCOUNT_METRIC_TIME_ZONE)
    anchor = local_start.replace(
        hour=_LIVE_ACCOUNT_METRIC_ANCHOR_HOUR,
        minute=0,
        second=0,
        microsecond=0,
    )
    if anchor < local_start:
        anchor += timedelta(days=1)
    return anchor.astimezone(UTC)


def account_equity_statement(
    *,
    environment: str,
    account_label: str,
    asset: str,
    window_start: datetime,
    window_end: datetime,
    interval_seconds: int,
    max_points: int = _EQUITY_MAX_POINTS,
) -> Select[tuple[datetime, Decimal, Decimal]]:
    """Fetch one narrow, latest balance row per UTC equity bucket.

    A bounded bucket series with a lateral index lookup keeps the work
    proportional to the number of points rendered by the dashboard.
    """
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    if max_points <= 0:
        raise ValueError("max_points must be positive")
    if window_start > window_end:
        raise ValueError("window_start must not be later than window_end")

    bucket_interval = text(f"interval '{interval_seconds} seconds'")
    end_bucket = bucket_start(window_end, interval_seconds)
    earliest_bucket = bucket_start(window_start, interval_seconds)
    latest_window_start = end_bucket - timedelta(
        seconds=interval_seconds * (max_points - 1)
    )
    series_start = max(earliest_bucket, latest_window_start)
    bucket_series = func.generate_series(
        series_start,
        end_bucket,
        bucket_interval,
    ).table_valued("bucket").render_derived(name="equity_buckets")
    snapshot = aliased(AccountBalanceSnapshotRow)
    bucket_start_at = bucket_series.c.bucket
    latest_equity = (
        select(
            snapshot.observed_at.label("observed_at"),
            snapshot.wallet_balance.label("wallet_balance"),
            snapshot.unrealized_pnl.label("unrealized_pnl"),
        )
        .where(
            snapshot.environment == environment,
            snapshot.account_label == account_label,
            snapshot.asset == asset,
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
            latest_equity.c.observed_at,
            latest_equity.c.wallet_balance,
            latest_equity.c.unrealized_pnl,
        )
        .select_from(bucket_series.join(latest_equity, true()))
        .order_by(bucket_start_at)
    )


def account_margin_statement(
    *,
    environment: str,
    account_label: str,
    window_start: datetime,
    window_end: datetime,
    interval_seconds: int,
    max_points: int = _EQUITY_MAX_POINTS,
) -> Select[tuple[datetime, Decimal]]:
    """Fetch one latest aggregate initial-margin observation per bucket.

    Binance's account-level REST response is the source of truth. WebSocket
    position projections are excluded because they are per-symbol and may be
    stale while an account event is being merged.
    """
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    if max_points <= 0:
        raise ValueError("max_points must be positive")
    if window_start > window_end:
        raise ValueError("window_start must not be later than window_end")

    bucket_interval = text(f"interval '{interval_seconds} seconds'")
    end_bucket = bucket_start(window_end, interval_seconds)
    earliest_bucket = bucket_start(window_start, interval_seconds)
    latest_window_start = end_bucket - timedelta(
        seconds=interval_seconds * (max_points - 1)
    )
    series_start = max(earliest_bucket, latest_window_start)
    bucket_series = func.generate_series(
        series_start,
        end_bucket,
        bucket_interval,
    ).table_valued("bucket").render_derived(name="margin_buckets")
    config = aliased(AccountConfigSnapshotRow)
    reconciliation = aliased(AccountReconciliationRunRow)
    bucket_start_at = bucket_series.c.bucket
    raw_total_initial_margin = func.nullif(
        config.raw_payload["totalInitialMargin"].astext,
        "",
    ).cast(Numeric(38, 18))
    rest_source = reconciliation.details["source"].astext
    latest_margin = (
        select(
            config.observed_at.label("observed_at"),
            raw_total_initial_margin.label("margin_used"),
        )
        .select_from(config)
        .join(
            reconciliation,
            and_(
                reconciliation.environment == config.environment,
                reconciliation.account_label == config.account_label,
                reconciliation.observed_at == config.observed_at,
                reconciliation.status == "ready",
            ),
        )
        .where(
            config.environment == environment,
            config.account_label == account_label,
            config.observed_at >= window_start,
            config.observed_at <= window_end,
            config.observed_at >= bucket_start_at,
            config.observed_at < bucket_start_at + bucket_interval,
            raw_total_initial_margin.is_not(None),
            raw_total_initial_margin >= 0,
            or_(
                rest_source == "rest_reconciliation",
                rest_source.is_(None),
            ),
        )
        .order_by(config.observed_at.desc())
        .limit(1)
        .lateral("latest_margin")
    )
    return (
        select(latest_margin.c.observed_at, latest_margin.c.margin_used)
        .select_from(bucket_series.join(latest_margin, true()))
        .order_by(bucket_start_at)
    )


def live_account_metric_points(
    equity_rows: Sequence[AccountEquityPoint],
    margin_rows: Sequence[tuple[datetime, Decimal]],
    *,
    interval_seconds: int,
) -> list[LiveAccountMetricPointResponse]:
    """Derive comparable equity, margin, and drawdown metrics per bucket."""
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    margin_by_bucket = {
        bucket_start(observed_at, interval_seconds): margin_used
        for observed_at, margin_used in margin_rows
    }
    baseline: Decimal | None = None
    peak: Decimal | None = None
    latest_margin = Decimal("0")
    points: list[LiveAccountMetricPointResponse] = []
    for row in sorted(equity_rows, key=lambda item: item.observed_at):
        equity = row.wallet_balance + row.unrealized_pnl
        bucket = bucket_start(row.observed_at, interval_seconds)
        if bucket in margin_by_bucket:
            latest_margin = max(Decimal("0"), margin_by_bucket[bucket])
        if baseline is None:
            baseline = equity
        peak = equity if peak is None else max(peak, equity)
        equity_change_ratio = (
            None if baseline == 0 else (equity - baseline) / baseline
        )
        margin_occupancy_ratio = None if equity <= 0 else latest_margin / equity
        drawdown = equity - peak
        drawdown_ratio = None if peak <= 0 else drawdown / peak
        points.append(
            LiveAccountMetricPointResponse(
                observed_at=row.observed_at,
                equity=str(equity),
                equity_change_ratio=(
                    None
                    if equity_change_ratio is None
                    else str(equity_change_ratio)
                ),
                margin_used=str(latest_margin),
                margin_occupancy_ratio=(
                    None
                    if margin_occupancy_ratio is None
                    else str(margin_occupancy_ratio)
                ),
                drawdown=str(drawdown),
                drawdown_ratio=(
                    None if drawdown_ratio is None else str(drawdown_ratio)
                ),
            )
        )
    return points


class LiveAccountMetricsQueries:
    """Deep query module for bounded live-account metrics."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        clock: Callable[[], datetime],
    ) -> None:
        self._session_factory = session_factory
        self._clock = clock

    async def live_account_metrics(
        self,
        equity_range: str = "24h",
    ) -> LiveAccountMetricsResponse:
        """Return comparable equity, margin, and drawdown curves."""
        equity_window, equity_bucket_seconds = account_equity_range(equity_range)
        equity_window_end = self._clock()
        equity_window_start = live_account_metrics_window_start(
            equity_window_end,
            equity_window,
        )
        async with self._session_factory() as session:
            processes = (
                await session.scalars(latest_live_account_process_statement())
            ).all()
            strategy_states = (
                await session.scalars(
                    select(StrategyLiveStateRow).where(
                        StrategyLiveStateRow.environment == "live"
                    )
                )
            ).all()
            leases = (
                await session.scalars(
                    select(TradingLeaseRow)
                    .where(
                        TradingLeaseRow.environment == "live",
                        TradingLeaseRow.state == "active",
                        TradingLeaseRow.expires_at > equity_window_end,
                    )
                    .order_by(TradingLeaseRow.expires_at.desc())
                )
            ).all()
            accounts = live_account_summaries(
                processes,
                strategy_states,
                leases,
            )
            metric_accounts: list[LiveAccountMetricsAccountResponse] = []
            for account in accounts:
                equity_rows = [
                    AccountEquityPoint(
                        observed_at=row.observed_at,
                        wallet_balance=row.wallet_balance,
                        unrealized_pnl=row.unrealized_pnl,
                    )
                    for row in (
                        await session.execute(
                            account_equity_statement(
                                environment=account.environment,
                                account_label=account.account_label,
                                asset="USDT",
                                window_start=equity_window_start,
                                window_end=equity_window_end,
                                interval_seconds=equity_bucket_seconds,
                                max_points=_LIVE_ACCOUNT_METRIC_MAX_POINTS,
                            )
                        )
                    ).all()
                ]
                margin_rows = [
                    (row.observed_at, row.margin_used)
                    for row in (
                        await session.execute(
                            account_margin_statement(
                                environment=account.environment,
                                account_label=account.account_label,
                                window_start=equity_window_start,
                                window_end=equity_window_end,
                                interval_seconds=equity_bucket_seconds,
                                max_points=_LIVE_ACCOUNT_METRIC_MAX_POINTS,
                            )
                        )
                    ).all()
                ]
                metric_accounts.append(
                    LiveAccountMetricsAccountResponse(
                        account_label=account.account_label,
                        environment=account.environment,
                        status=account.status,
                        metrics_curve=live_account_metric_points(
                            equity_rows,
                            margin_rows,
                            interval_seconds=equity_bucket_seconds,
                        ),
                    )
                )
        return LiveAccountMetricsResponse(
            status=live_account_fleet_status(accounts),
            equity_range=cast(
                Literal["24h", "7d", "30d", "1y"],
                equity_range,
            ),
            equity_window_start=equity_window_start,
            equity_window_end=equity_window_end,
            equity_sample_interval_seconds=equity_bucket_seconds,
            accounts=metric_accounts,
        )


def as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def bucket_start(value: datetime, interval_seconds: int) -> datetime:
    observed_at = as_utc(value)
    epoch_seconds = int(observed_at.timestamp())
    bucket_epoch = epoch_seconds // interval_seconds * interval_seconds
    return datetime.fromtimestamp(bucket_epoch, tz=UTC)


__all__ = [
    "AccountEquityPoint",
    "LiveAccountMetricsQueries",
    "account_equity_range",
    "account_equity_statement",
    "account_margin_statement",
    "bucket_start",
    "live_account_metric_points",
    "live_account_metrics_window_start",
]
