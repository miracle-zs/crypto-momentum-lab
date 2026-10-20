"""Shared SQL helpers for current and paper account equity history."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import (
    Select,
    and_,
    case,
    func,
    or_,
    select,
    text,
    true,
)
from sqlalchemy.orm import aliased

from crypto_momentum_lab.persistence.postgres.models import (
    AccountBalanceSnapshotRow,
    AccountPositionSnapshotRow,
    AccountReconciliationRunRow,
    RuntimeMarketState15sRow,
)

_EQUITY_MAX_POINTS = 240


@dataclass(frozen=True, slots=True)
class AccountEquityPoint:
    """Narrow account-equity projection used by the dashboard curve."""

    observed_at: datetime
    wallet_balance: Decimal
    unrealized_pnl: Decimal


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
    bucket_series = (
        func.generate_series(
            series_start,
            end_bucket,
            bucket_interval,
        )
        .table_valued("bucket")
        .render_derived(name="equity_buckets")
    )
    snapshot = aliased(AccountBalanceSnapshotRow)
    bucket_start_at = bucket_series.c.bucket
    sample_at = func.least(bucket_start_at + bucket_interval, window_end)
    latest_equity = (
        select(
            snapshot.observed_at.label("observed_at"),
            snapshot.wallet_balance.label("wallet_balance"),
            snapshot.unrealized_pnl.label("unrealized_pnl"),
            snapshot.raw_payload.has_key("crossUnPnl").label("rest_valuation"),
        )
        .where(
            snapshot.environment == environment,
            snapshot.account_label == account_label,
            snapshot.asset == asset,
            snapshot.observed_at <= sample_at,
        )
        .order_by(snapshot.observed_at.desc())
        .limit(1)
        .lateral("latest_equity")
    )
    head = aliased(AccountReconciliationRunRow)
    account_head = (
        select(head.position_count, head.details)
        .where(
            head.environment == environment,
            head.account_label == account_label,
            head.status == "ready",
            head.observed_at <= sample_at,
        )
        .order_by(head.observed_at.desc(), head.reconciliation_id.desc())
        .limit(1)
        .correlate(bucket_series)
        .lateral("equity_head")
    )
    position = aliased(AccountPositionSnapshotRow)
    columns = (
        position.symbol,
        position.position_side,
        position.position_amt,
        position.entry_price,
        position.observed_at,
    )
    scope = (
        position.environment == environment,
        position.account_label == account_label,
    )
    baseline = (
        select(*columns)
        .distinct(position.symbol, position.position_side)
        .where(*scope, position.observed_at < window_start)
        .order_by(position.symbol, position.position_side, position.observed_at.desc())
        .subquery("position_baseline")
    )
    history = (
        select(*baseline.c)
        .union_all(
            select(*columns).where(
                *scope,
                position.observed_at >= window_start,
                position.observed_at <= window_end,
            )
        )
        .cte("position_history")
    )
    intervals = (
        select(
            *history.c,
            func.lead(history.c.observed_at)
            .over(
                partition_by=(history.c.symbol, history.c.position_side),
                order_by=history.c.observed_at,
            )
            .label("valid_until"),
        )
        .cte("position_intervals")
        .prefix_with("MATERIALIZED", dialect="postgresql")
    )
    latest_positions = (
        select(
            intervals.c.symbol,
            intervals.c.position_side,
            intervals.c.position_amt,
            intervals.c.entry_price,
        )
        .where(
            intervals.c.observed_at <= sample_at,
            or_(intervals.c.valid_until.is_(None), intervals.c.valid_until > sample_at),
        )
        .correlate(bucket_series)
        .lateral("equity_positions")
    )
    market = aliased(RuntimeMarketState15sRow)
    latest_mark = (
        select(market.mark_price)
        .where(
            market.environment == "research",
            market.symbol == latest_positions.c.symbol,
            market.bucket_start <= sample_at,
            market.bucket_start >= sample_at - text("interval '75 seconds'"),
            market.bucket_end <= sample_at,
            market.bucket_end >= sample_at - text("interval '60 seconds'"),
            market.mark_price.is_not(None),
        )
        .order_by(market.bucket_start.desc())
        .limit(1)
        .correlate(latest_positions, bucket_series)
        .lateral("equity_mark")
    )
    valuation = (
        select(
            func.coalesce(
                func.sum(
                    latest_positions.c.position_amt
                    * (latest_mark.c.mark_price - latest_positions.c.entry_price)
                ),
                0,
            ).label("unrealized_pnl"),
            (
                func.count(latest_positions.c.symbol)
                == func.count(latest_mark.c.mark_price)
            ).label("complete"),
            func.count(latest_positions.c.symbol).label("position_count"),
        )
        .select_from(latest_positions.outerjoin(latest_mark, true()))
        .where(
            latest_positions.c.position_amt != 0,
            account_head.c.position_count > 0,
            or_(
                account_head.c.details["position_keys"].is_(None),
                account_head.c.details["position_keys"].op("@>")(
                    func.jsonb_build_array(
                        func.jsonb_build_object(
                            "symbol",
                            latest_positions.c.symbol,
                            "position_side",
                            latest_positions.c.position_side,
                        )
                    )
                ),
            ),
        )
        .correlate(bucket_series, account_head)
        .lateral("equity_valuation")
    )
    complete = and_(
        valuation.c.complete,
        valuation.c.position_count == account_head.c.position_count,
    )
    historical_rest_point = and_(
        latest_equity.c.rest_valuation, latest_equity.c.observed_at >= bucket_start_at
    )
    return (
        select(
            case((complete, sample_at), else_=latest_equity.c.observed_at).label(
                "observed_at"
            ),
            latest_equity.c.wallet_balance,
            case(
                (complete, valuation.c.unrealized_pnl),
                else_=latest_equity.c.unrealized_pnl,
            ).label("unrealized_pnl"),
        )
        .select_from(
            bucket_series.join(latest_equity, true())
            .join(account_head, true())
            .join(valuation, true())
        )
        .where(
            or_(complete, historical_rest_point),
            bucket_start_at < window_end,
        )
        .order_by(bucket_start_at)
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
    "account_equity_statement",
    "bucket_start",
]
