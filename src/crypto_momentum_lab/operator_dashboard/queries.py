import json
import os
from collections.abc import AsyncIterator, Callable, Iterable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal, cast

from sqlalchemy import (
    Select,
    String,
    and_,
    case,
    column,
    func,
    select,
    text,
    true,
    values,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import aliased
from sqlalchemy.sql.selectable import Values

from crypto_momentum_lab.domain.execution import ExchangeOrderState
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.operator_dashboard.schemas import (
    AccountOverviewResponse,
    LiveAccountsResponse,
    LiveAccountSummaryResponse,
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
    LiveStrategySignalRow,
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
    StrategyLiveStateRow,
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
_LIVE_SIGNAL_MAX_ROWS = 30
_PAPER_HISTORY_RECENT_LIMIT = 500
_COMMON_EQUITY_BUCKET_SECONDS = 15 * 60
FIXED_COMMON_EQUITY_START_AT = datetime(2026, 8, 21, 2, 45, tzinfo=UTC)
_ACCOUNT_EQUITY_RANGES: dict[str, tuple[timedelta, int]] = {
    "24h": (timedelta(hours=24), 6 * 60),
    "7d": (timedelta(days=7), 60 * 60),
    "30d": (timedelta(days=30), 3 * 60 * 60),
    "1y": (timedelta(days=365), 2 * 24 * 60 * 60),
}
_CONFIRMED_OPEN_ORDER_STATES = frozenset(
    {
        ExchangeOrderState.ACKNOWLEDGED.value,
        ExchangeOrderState.PARTIALLY_FILLED.value,
    }
)


def _latest_live_account_process_statement() -> Select[Any]:
    latest = (
        select(
            ExecutionAccountProcessStateRow.account_label,
            func.max(ExecutionAccountProcessStateRow.occurred_at).label(
                "latest_occurred_at"
            ),
        )
        .where(ExecutionAccountProcessStateRow.environment == "live")
        .group_by(ExecutionAccountProcessStateRow.account_label)
        .subquery("latest_live_account_process")
    )
    return (
        select(ExecutionAccountProcessStateRow)
        .join(
            latest,
            and_(
                ExecutionAccountProcessStateRow.account_label
                == latest.c.account_label,
                ExecutionAccountProcessStateRow.occurred_at
                == latest.c.latest_occurred_at,
                ExecutionAccountProcessStateRow.environment == "live",
            ),
        )
        .order_by(ExecutionAccountProcessStateRow.account_label)
    )


def _account_label_sort_key(account_label: str) -> tuple[int, int | str]:
    if account_label == "primary":
        return (0, 0)
    suffix = account_label.removeprefix("account-")
    return (1, int(suffix)) if suffix.isdigit() else (2, account_label)


def _live_account_status(state: str | None) -> OperationalStatus:
    if state is None:
        return OperationalStatus.UNKNOWN
    return (
        OperationalStatus.READY
        if state == "ready_readonly"
        else OperationalStatus.HALTED
    )


@dataclass(frozen=True, slots=True)
class LiveCashFlowAdjustment:
    account_label: str
    effective_at: datetime
    amount: Decimal
    cash_flow_type: str = "deposit"


@dataclass(frozen=True, slots=True)
class _AccountEquityPoint:
    """Narrow account-equity projection used by the dashboard curve."""

    observed_at: datetime
    wallet_balance: Decimal
    unrealized_pnl: Decimal


@dataclass(frozen=True, slots=True)
class _PaperEquityPoint:
    """Scalar paper-equity projection used by the dashboard curve."""

    run_id: str
    observed_at: datetime
    balance: Decimal
    equity: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal


@dataclass(frozen=True, slots=True)
class _PaperEquitySummaryPoint:
    """Narrow latest paper-equity projection used by account summaries."""

    balance: Decimal
    equity: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    total_fees: Decimal


DEFAULT_LIVE_CASH_FLOW_ADJUSTMENTS = (
    LiveCashFlowAdjustment(
        account_label="primary",
        effective_at=datetime(
            2026,
            8,
            21,
            9,
            41,
            19,
            895915,
            tzinfo=UTC,
        ),
        amount=Decimal("200"),
        cash_flow_type="deposit",
    ),
)


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
    """Fetch one narrow latest-equity row per run with an index probe.

    The account-card summary only needs five scalar values. Avoid selecting the
    full ORM row and a grouped ``MAX(observed_at)`` subquery, which otherwise
    scans every historical snapshot on each dashboard refresh.
    """
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
    """Fetch the latest valid snapshot in each run/bucket using the run index.

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
    bucket_start = bucket_series.c.bucket
    latest_equity = (
        select(
            snapshot.run_id.label("run_id"),
            snapshot.observed_at.label("observed_at"),
            snapshot.equity.label("equity"),
        )
        .where(
            snapshot.run_id == run_values.c.run_id,
            snapshot.equity > 0,
            snapshot.observed_at >= bucket_start,
            snapshot.observed_at
            < bucket_start + bucket_interval,
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
        .order_by(run_values.c.run_id, bucket_start)
    )


def _account_equity_statement(
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

    The previous dashboard query selected the full balance row (including the
    JSON payload), sorted every row in the requested window, and then applied
    ``DISTINCT ON``. A bounded bucket series with a lateral index lookup keeps
    the work proportional to the number of points rendered by the dashboard.
    """
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
    bucket_series = func.generate_series(
        series_start,
        end_bucket,
        bucket_interval,
    ).table_valued("bucket").render_derived(name="equity_buckets")
    snapshot = aliased(AccountBalanceSnapshotRow)
    bucket_start = bucket_series.c.bucket
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
            snapshot.observed_at >= bucket_start,
            snapshot.observed_at < bucket_start + bucket_interval,
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
        .order_by(bucket_start)
    )


def _paper_equity_statement(
    run_ids: Sequence[str],
    window_start: datetime,
    window_end: datetime,
    interval_seconds: int = _EQUITY_BUCKET_SECONDS,
    max_points: int = _EQUITY_MAX_POINTS,
) -> Select[
    tuple[str, datetime, Decimal, Decimal, Decimal, Decimal]
]:
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
    bucket_start = bucket_series.c.bucket
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
            snapshot.observed_at >= bucket_start,
            snapshot.observed_at < bucket_start + bucket_interval,
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
        .order_by(latest_equity.c.run_id, bucket_start)
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
    bucket_start = bucket_series.c.bucket
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
            snapshot.observed_at >= bucket_start,
            snapshot.observed_at < bucket_start + bucket_interval,
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
        .order_by(bucket_start)
    )


def _latest_checkpoint_at_statement() -> Select[tuple[datetime]]:
    return (
        select(StrategyRuntimeCheckpointRow.saved_at)
        .order_by(StrategyRuntimeCheckpointRow.saved_at.desc())
        .limit(1)
    )


def _checkpoint_times_statement(
    run_ids: Sequence[str],
) -> Select[tuple[str, datetime]]:
    return select(
        StrategyRuntimeCheckpointRow.run_id,
        StrategyRuntimeCheckpointRow.saved_at,
    ).where(StrategyRuntimeCheckpointRow.run_id.in_(run_ids))


def parse_live_cash_flow_adjustments(
    value: str | None = None,
) -> tuple[LiveCashFlowAdjustment, ...]:
    """Parse dashboard-only cash-flow corrections without exposing credentials."""
    raw_value = (
        os.environ.get("CML_DASHBOARD_LIVE_CASH_FLOWS_JSON", "")
        if value is None
        else value
    )
    if not raw_value.strip():
        return DEFAULT_LIVE_CASH_FLOW_ADJUSTMENTS
    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError as error:
        raise ValueError(
            "CML_DASHBOARD_LIVE_CASH_FLOWS_JSON must be valid JSON"
        ) from error
    if not isinstance(payload, list):
        raise ValueError(
            "CML_DASHBOARD_LIVE_CASH_FLOWS_JSON must be a JSON list"
        )

    adjustments: list[LiveCashFlowAdjustment] = []
    for index, item in enumerate(payload):
        if not isinstance(item, Mapping):
            raise ValueError(f"cash-flow entry {index} must be a JSON object")
        account_label = item.get("account_label")
        effective_at = item.get("effective_at")
        amount = item.get("amount")
        cash_flow_type = item.get("cash_flow_type", "deposit")
        if not isinstance(account_label, str) or not account_label.strip():
            raise ValueError(f"cash-flow entry {index} has no account_label")
        if not isinstance(effective_at, str) or not effective_at.strip():
            raise ValueError(f"cash-flow entry {index} has no effective_at")
        if not isinstance(cash_flow_type, str) or not cash_flow_type.strip():
            raise ValueError(f"cash-flow entry {index} has no cash_flow_type")
        try:
            parsed_at = datetime.fromisoformat(
                effective_at.strip().replace("Z", "+00:00")
            )
            if parsed_at.tzinfo is None:
                raise ValueError("effective_at must include a timezone")
            parsed_amount = Decimal(str(amount))
        except (TypeError, ValueError, ArithmeticError) as error:
            raise ValueError(f"invalid cash-flow entry {index}") from error
        if not parsed_amount.is_finite():
            raise ValueError(f"cash-flow entry {index} amount must be finite")
        adjustments.append(
            LiveCashFlowAdjustment(
                account_label=account_label.strip(),
                effective_at=parsed_at.astimezone(UTC),
                amount=parsed_amount,
                cash_flow_type=cash_flow_type.strip(),
            )
        )
    return tuple(sorted(adjustments, key=lambda item: item.effective_at))


def parse_common_equity_start_at(value: str | None = None) -> datetime:
    """Return the production comparison origin, which is intentionally fixed."""
    raw_value = (
        os.environ.get("CML_DASHBOARD_COMMON_EQUITY_START_AT", "")
        if value is None
        else value
    )
    if not raw_value.strip():
        return FIXED_COMMON_EQUITY_START_AT
    try:
        parsed_at = datetime.fromisoformat(
            raw_value.strip().replace("Z", "+00:00")
        )
    except ValueError as error:
        raise ValueError(
            "CML_DASHBOARD_COMMON_EQUITY_START_AT must be an ISO-8601 timestamp"
        ) from error
    if parsed_at.tzinfo is None:
        raise ValueError(
            "CML_DASHBOARD_COMMON_EQUITY_START_AT must include a timezone"
        )
    parsed_at = parsed_at.astimezone(UTC)
    if parsed_at != FIXED_COMMON_EQUITY_START_AT:
        raise ValueError(
            "CML_DASHBOARD_COMMON_EQUITY_START_AT is fixed at "
            "2026-08-21T02:45:00Z"
        )
    return FIXED_COMMON_EQUITY_START_AT


def _account_equity_range(value: str) -> tuple[timedelta, int]:
    try:
        return _ACCOUNT_EQUITY_RANGES[value]
    except KeyError as error:
        raise ValueError(f"unsupported account equity range: {value}") from error


def _split_exchange_orders(
    rows: Sequence[ExchangeOrderRow],
) -> tuple[list[ExchangeOrderRow], list[ExchangeOrderRow]]:
    """Separate confirmed resting orders from genuinely uncertain orders."""
    terminal_states = {
        state.value for state in ExchangeOrderState if state.terminal
    }
    pending: list[ExchangeOrderRow] = []
    ambiguous: list[ExchangeOrderRow] = []
    for row in rows:
        if row.state in terminal_states:
            continue
        if row.state in _CONFIRMED_OPEN_ORDER_STATES:
            pending.append(row)
        else:
            # Unknown/non-terminal states must remain fail-closed.
            ambiguous.append(row)
    return pending, ambiguous


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
    reduce_only: bool = False
    close_reason: str | None = None


def _aggregate_account_fills(
    rows: Sequence[AccountFillEventRow],
    strategy_by_order: dict[str, str],
    order_metadata_by_order: Mapping[str, Mapping[str, JsonValue]] | None = None,
    *,
    limit: int = 20,
) -> list[dict[str, JsonValue]]:
    """Collapse exchange partial fills into one order-level dashboard row."""
    grouped: dict[tuple[str, str, str], _AccountFillAggregate] = {}
    for row in rows:
        key = (row.order_id, row.symbol, row.side)
        aggregate = grouped.get(key)
        if aggregate is None:
            metadata = (order_metadata_by_order or {}).get(row.order_id, {})
            close_reason = metadata.get("close_reason")
            aggregate = _AccountFillAggregate(
                symbol=row.symbol,
                order_id=row.order_id,
                side=row.side,
                strategy_name=strategy_by_order.get(row.order_id),
                reduce_only=bool(metadata.get("reduce_only", False)),
                close_reason=(
                    close_reason if isinstance(close_reason, str) else None
                ),
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
            "reduce_only": aggregate.reduce_only,
            "close_reason": aggregate.close_reason,
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
        live_cash_flow_adjustments: Sequence[LiveCashFlowAdjustment]
        | None = None,
        common_equity_start_at: datetime | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._clock = clock or (lambda: datetime.now(tz=UTC))
        self._stale_after_seconds = stale_after_seconds
        self._paper_run_ids = paper_run_ids
        self._live_cash_flow_adjustments = tuple(
            DEFAULT_LIVE_CASH_FLOW_ADJUSTMENTS
            if live_cash_flow_adjustments is None
            else live_cash_flow_adjustments
        )
        self._common_equity_start_at = (
            FIXED_COMMON_EQUITY_START_AT
            if common_equity_start_at is None
            else _as_utc(common_equity_start_at)
        )

    async def health(self) -> dict[str, str]:
        async with self._session_factory() as session:
            await session.execute(text("SELECT 1"))
        return {"app_status": "UP", "database_status": "UP"}

    @staticmethod
    def _live_account_summaries(
        processes: Sequence[ExecutionAccountProcessStateRow],
        strategy_states: Sequence[StrategyLiveStateRow],
        leases: Sequence[TradingLeaseRow],
    ) -> list[LiveAccountSummaryResponse]:
        process_by_account = {row.account_label: row for row in processes}
        strategy_by_account: dict[str, StrategyLiveStateRow] = {}
        for row in sorted(
            strategy_states,
            key=lambda item: item.changed_at,
            reverse=True,
        ):
            strategy_by_account.setdefault(row.account_label, row)
        lease_by_account = {row.account_label: row for row in leases}
        account_labels = sorted(
            set(process_by_account)
            | set(strategy_by_account)
            | set(lease_by_account),
            key=_account_label_sort_key,
        )
        summaries: list[LiveAccountSummaryResponse] = []
        for account_label in account_labels:
            process = process_by_account.get(account_label)
            strategy = strategy_by_account.get(account_label)
            lease = lease_by_account.get(account_label)
            readiness = process.state if process is not None else "missing"
            summaries.append(
                LiveAccountSummaryResponse(
                    account_label=account_label,
                    environment=(
                        process.environment if process is not None else "live"
                    ),
                    status=_live_account_status(
                        None if process is None else process.state
                    ),
                    readiness=readiness,
                    observed_at=(
                        None if process is None else process.occurred_at
                    ),
                    strategy_name=(
                        None if strategy is None else strategy.strategy_name
                    ),
                    strategy_state=(
                        None if strategy is None else strategy.state
                    ),
                    lease_expires_at=(
                        None if lease is None else lease.expires_at
                    ),
                )
            )
        return summaries

    async def live_accounts(self) -> LiveAccountsResponse:
        now = self._clock()
        async with self._session_factory() as session:
            processes = (
                await session.scalars(_latest_live_account_process_statement())
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
                    select(TradingLeaseRow).where(
                        TradingLeaseRow.environment == "live",
                        TradingLeaseRow.state == "active",
                        TradingLeaseRow.expires_at > now,
                    )
                )
            ).all()
        accounts = self._live_account_summaries(
            processes,
            strategy_states,
            leases,
        )
        if not accounts:
            status = OperationalStatus.NO_DATA
        elif all(account.status is OperationalStatus.READY for account in accounts):
            status = OperationalStatus.READY
        else:
            status = OperationalStatus.HALTED
        return LiveAccountsResponse(status=status, accounts=accounts)

    async def overview(self) -> SystemOverviewResponse:
        now = self._clock()
        async with self._session_factory() as session:
            market_at = await session.scalar(
                select(RuntimeMarketState15sRow.bucket_end)
                .order_by(RuntimeMarketState15sRow.bucket_start.desc())
                .limit(1)
            )
            account_rows = (
                await session.scalars(_latest_live_account_process_statement())
            ).all()
            account = max(
                account_rows,
                key=lambda row: row.occurred_at,
                default=None,
            )
            strategy_at = await session.scalar(_latest_checkpoint_at_statement())
            halt_count = await session.scalar(
                select(func.count(RiskHaltRow.halt_id)).where(
                    RiskHaltRow.active.is_(True)
                )
            )
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
                        TradingLeaseRow.expires_at > now,
                    )
                    .order_by(TradingLeaseRow.expires_at.desc())
                )
            ).all()
            lease = leases[0] if leases else None
            live = await session.scalar(
                select(LiveSessionTransitionRow)
                .order_by(LiveSessionTransitionRow.occurred_at.desc())
                .limit(1)
            )
            live_heartbeat_at = None
            live_started_at = None
            if live is not None:
                # A runtime checkpoint is meaningful for the current session
                # only after the daemon has entered live_enabled. During
                # preflight retries, an older checkpoint would make a healthy
                # retry loop look dead in the dashboard.
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
        account_statuses = self._live_account_summaries(
            account_rows,
            strategy_states=strategy_states,
            leases=leases,
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
            live_observed_at, heartbeat_source = _live_observation(
                state=live.state,
                runtime_checkpoint_at=live_heartbeat_at,
                transition_at=live.occurred_at,
            )
            live_details: dict[str, JsonValue] = {
                "state": live.state,
                "session_id": live.session_id,
                "heartbeat_source": heartbeat_source,
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
            active_leases=[
                {
                    "account_label": item.account_label,
                    "strategy_name": item.strategy_name,
                    "owner": item.owner,
                    "expires_at": item.expires_at.isoformat(),
                }
                for item in leases
            ],
            account_statuses=account_statuses,
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
            entries_by_symbol = {entry.symbol: entry for entry in entries}
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
                _universe_membership(row, entries_by_symbol.get(row.symbol))
                for row in sorted(
                    memberships,
                    key=lambda row: (
                        {"target": 0, "retained": 1, "forced": 2}.get(
                            row.status,
                            99,
                        ),
                        {"gainer": 0, "loser": 1}.get(row.side or "", 2),
                        row.symbol,
                    ),
                )
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
        live_processes: Sequence[ExecutionAccountProcessStateRow] = ()
        live_balance_rows_by_account: dict[
            str, Sequence[_AccountEquityPoint]
        ] = {}
        common_paper_rows: Sequence[tuple[str, datetime, Decimal]] = ()
        common_live_equity_rows_by_account: dict[
            str, list[tuple[datetime, Decimal]]
        ] = {}
        live_strategy_names: dict[str, str] = {}
        paper_first_at_by_run: dict[str, datetime] = {}
        live_first_at_by_account: dict[str, datetime] = {}
        common_start_at: datetime | None = None
        common_source_end_at: datetime | None = None
        common_equity_interval_seconds: int | None = None
        async with self._session_factory() as session:
            selected_runs = await self._selected_paper_runs(session)
            if not selected_runs:
                run_ids: list[str] = []
            else:
                run_ids = [run.run_id for run in selected_runs]
            rows = []
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
                await session.scalars(_latest_live_account_process_statement())
            ).all()
            strategy_states = (
                await session.scalars(
                    select(StrategyLiveStateRow).where(
                        StrategyLiveStateRow.environment == "live"
                    )
                )
            ).all()
            for strategy_state in sorted(
                strategy_states,
                key=lambda row: row.changed_at,
                reverse=True,
            ):
                live_strategy_names.setdefault(
                    strategy_state.account_label,
                    strategy_state.strategy_name,
                )
            for live_process in live_processes:
                account_label = live_process.account_label
                live_balance_rows_by_account[account_label] = [
                    _AccountEquityPoint(
                        observed_at=row.observed_at,
                        wallet_balance=row.wallet_balance,
                        unrealized_pnl=row.unrealized_pnl,
                    )
                    for row in (
                        await session.execute(
                            _account_equity_statement(
                                environment="live",
                                account_label=account_label,
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
                    await session.execute(
                        _paper_first_equity_statement(run_ids)
                    )
                ).all()
                paper_first_at_by_run = {
                    run_id: first_at
                    for run_id, first_at in paper_first_rows
                }

            for live_process in live_processes:
                first_at = await session.scalar(
                    select(func.min(AccountBalanceSnapshotRow.observed_at)).where(
                        AccountBalanceSnapshotRow.environment == "live",
                        AccountBalanceSnapshotRow.account_label
                        == live_process.account_label,
                    )
                )
                if first_at is not None:
                    live_first_at_by_account[live_process.account_label] = first_at

            first_buckets = {
                run_id: _bucket_start(first_at, _COMMON_EQUITY_BUCKET_SECONDS)
                for run_id, first_at in paper_first_at_by_run.items()
            }
            for account_label, first_at in live_first_at_by_account.items():
                live_run_id = f"live-{account_label}-b1"
                first_buckets[live_run_id] = _bucket_start(
                    first_at,
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
                for live_process in live_processes:
                    account_label = live_process.account_label
                    common_live_equity_rows_by_account[account_label] = [
                        (observed_at, equity)
                        for observed_at, equity in (
                            await session.execute(
                                _live_common_equity_statement(
                                    environment="live",
                                    account_label=account_label,
                                    window_start=common_start_at,
                                    window_end=window_end,
                                    interval_seconds=common_equity_interval_seconds,
                                )
                            )
                        ).all()
                    ]

        common_equity_by_run: dict[str, list[dict[str, JsonValue]]] = {}
        common_baseline_by_run: dict[str, Decimal] = {}
        common_end_at: datetime | None = None
        common_anchor_accounts: list[str] = []
        common_cash_flows: list[dict[str, JsonValue]] = []
        common_note: str | None = None
        if common_start_at is not None:
            common_observations: dict[str, list[_EquityObservation]] = {
                run_id: _paper_equity_observations_from_values(
                    (
                        row_run_id,
                        observed_at,
                        equity,
                    )
                    for row_run_id, observed_at, equity in common_paper_rows
                    if row_run_id == run_id
                )
                for run_id in run_ids
            }
            for account_label, equity_rows in (
                common_live_equity_rows_by_account.items()
            ):
                live_run_id = f"live-{account_label}-b1"
                common_observations[live_run_id] = (
                    _live_aggregated_equity_observations(
                        equity_rows,
                        account_label=account_label,
                        cash_flow_adjustments=self._live_cash_flow_adjustments,
                    )
                )
            available_observations = {
                run_id: observations
                for run_id, observations in common_observations.items()
                if observations
            }
            if len(available_observations) >= 2:
                common_source_end_at = min(
                    max(
                        observation.source_observed_at
                        for observation in observations
                    )
                    for observations in available_observations.values()
                )
                assert common_equity_interval_seconds is not None
                curve_end_at = _relative_bucket_end(
                    common_start_at,
                    common_source_end_at,
                    common_equity_interval_seconds,
                )
                for run_id, observations in available_observations.items():
                    curve, baseline = _build_common_equity_curve(
                        observations,
                        common_start_at=common_start_at,
                        end_at=curve_end_at,
                        interval_seconds=common_equity_interval_seconds,
                        source_end_at=common_source_end_at,
                        max_points=_EQUITY_MAX_POINTS,
                    )
                    if len(curve) >= 2 and baseline is not None:
                        common_equity_by_run[run_id] = curve
                        common_baseline_by_run[run_id] = baseline
                if common_equity_by_run:
                    common_end_at = common_source_end_at
                common_anchor_accounts = [
                    run_id
                    for run_id, first_at in first_buckets.items()
                    if run_id in common_equity_by_run
                    and first_at == common_start_at
                ]
                common_cash_flows = [
                    _live_cash_flow_payload(adjustment)
                    for adjustment in self._live_cash_flow_adjustments
                    if (
                        common_end_at is not None
                        and adjustment.effective_at <= common_end_at
                    )
                ]
                common_note = _common_equity_note(
                    common_cash_flows,
                    interval_seconds=common_equity_interval_seconds,
                )

        rows_by_run: dict[str, list[_PaperEquityPoint]] = {}
        for row in rows:
            rows_by_run.setdefault(row.run_id, []).append(row)
        accounts = []
        for run in selected_runs:
            equity = sorted(
                rows_by_run.get(run.run_id, []),
                key=lambda row: row.observed_at,
            )
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
        for live_process in live_processes:
            live_account_label = live_process.account_label
            live_balance_rows = live_balance_rows_by_account.get(
                live_account_label,
                (),
            )
            if len(live_balance_rows) < 2:
                continue
            live_run_id = f"live-{live_account_label}-b1"
            accounts.append(
                PaperAccountEquityResponse(
                    run_id=live_run_id,
                    strategy_name=(
                        live_strategy_names.get(live_account_label)
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
                    account_label=live_account_label,
                    equity_curve=[
                        _live_account_equity_point(row)
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
                OperationalStatus.READY
                if accounts
                else OperationalStatus.NO_DATA
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
            await session.execute(_checkpoint_times_statement(run_ids))
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

    async def account(
        self,
        equity_range: str = "24h",
        account_label: str | None = None,
    ) -> AccountOverviewResponse:
        equity_window, equity_bucket_seconds = _account_equity_range(equity_range)
        equity_window_end = self._clock()
        equity_window_start = equity_window_end - equity_window
        process: ExecutionAccountProcessStateRow | None = None
        reconciliation: AccountReconciliationRunRow | None = None
        account_config: AccountConfigSnapshotRow | None = None
        balances: Sequence[AccountBalanceSnapshotRow] = ()
        equity_rows: Sequence[_AccountEquityPoint] = ()
        positions: Sequence[AccountPositionSnapshotRow] = ()
        orders: Sequence[AccountOpenOrderRow] = ()
        fills: Sequence[AccountFillEventRow] = ()
        live_signals: Sequence[LiveStrategySignalRow] = ()
        execution_orders: Sequence[ExchangeOrderRow] = ()
        intent_rows: Sequence[OrderIntentExecutionRow] = ()
        available_accounts: list[LiveAccountSummaryResponse] = []
        async with self._session_factory() as session:
            live_processes = (
                await session.scalars(_latest_live_account_process_statement())
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
                    select(TradingLeaseRow).where(
                        TradingLeaseRow.environment == "live",
                        TradingLeaseRow.state == "active",
                        TradingLeaseRow.expires_at > equity_window_end,
                    )
                )
            ).all()
            available_accounts = self._live_account_summaries(
                live_processes,
                strategy_states,
                leases,
            )
            process_query = select(ExecutionAccountProcessStateRow).where(
                ExecutionAccountProcessStateRow.environment == "live"
            )
            if account_label is not None:
                process_query = process_query.where(
                    ExecutionAccountProcessStateRow.account_label
                    == account_label
                )
            process_query = process_query.order_by(
                ExecutionAccountProcessStateRow.occurred_at.desc()
            ).limit(1)
            process = await session.scalar(
                process_query
            )
            if process is not None:
                environment = process.environment
                account_label = process.account_label
                live_signals = (
                    await session.scalars(
                        select(LiveStrategySignalRow)
                        .where(
                            LiveStrategySignalRow.account_label == account_label
                        )
                        .order_by(
                            LiveStrategySignalRow.detected_at.desc(),
                            LiveStrategySignalRow.recorded_at.desc(),
                        )
                        .limit(_LIVE_SIGNAL_MAX_ROWS)
                    )
                ).all()
                equity_rows = [
                    _AccountEquityPoint(
                        observed_at=row.observed_at,
                        wallet_balance=row.wallet_balance,
                        unrealized_pnl=row.unrealized_pnl,
                    )
                    for row in (
                        await session.execute(
                            _account_equity_statement(
                                environment=environment,
                                account_label=account_label,
                                asset="USDT",
                                window_start=equity_window_start,
                                window_end=equity_window_end,
                                interval_seconds=equity_bucket_seconds,
                            )
                        )
                    ).all()
                ]
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
        order_metadata_by_order = {
            row.exchange_order_id: {
                "reduce_only": row.reduce_only,
                "close_reason": (
                    _order_intent_reason(intent_by_id[row.intent_id].details)
                    if row.reduce_only
                    else None
                ),
            }
            for row in execution_orders
            if row.exchange_order_id is not None and row.intent_id in intent_by_id
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
            order_metadata_by_order,
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
            equity_range=cast(
                Literal["24h", "7d", "30d", "1y"],
                equity_range,
            ),
            equity_window_start=equity_window_start,
            equity_window_end=equity_window_end,
            equity_sample_interval_seconds=equity_bucket_seconds,
            equity_curve=[
                _live_account_equity_point(row)
                for row in sorted(
                    equity_rows,
                    key=lambda row: row.observed_at,
                )
            ],
            available_accounts=available_accounts,
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
            live_signals=[_live_strategy_signal(row) for row in live_signals],
        )

    async def risk_execution(self) -> RiskExecutionResponse:
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
        pending, ambiguous = _split_exchange_orders(orders)
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
            pending_orders=[_exchange_order(row) for row in pending],
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


@dataclass(frozen=True, slots=True)
class _EquityObservation:
    observed_at: datetime
    equity: Decimal
    source_observed_at: datetime


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _bucket_start(value: datetime, interval_seconds: int) -> datetime:
    observed_at = _as_utc(value)
    epoch_seconds = int(observed_at.timestamp())
    bucket_epoch = epoch_seconds // interval_seconds * interval_seconds
    return datetime.fromtimestamp(bucket_epoch, tz=UTC)


def _relative_bucket_start(
    value: datetime,
    origin: datetime,
    interval_seconds: int,
) -> datetime:
    """Return a bucket boundary measured from a caller-provided origin."""
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    observed_at = _as_utc(value)
    bucket_origin = _as_utc(origin)
    elapsed_seconds = int((observed_at - bucket_origin).total_seconds())
    bucket_offset = elapsed_seconds // interval_seconds * interval_seconds
    return bucket_origin + timedelta(seconds=bucket_offset)


def _relative_bucket_end(
    origin: datetime,
    value: datetime,
    interval_seconds: int,
) -> datetime:
    """Return the last complete relative bucket at or before ``value``."""
    return _relative_bucket_start(value, origin, interval_seconds)


def _common_equity_interval_seconds(
    common_start_at: datetime,
    window_end: datetime,
    *,
    max_points: int = _EQUITY_MAX_POINTS,
) -> int:
    """Choose a bounded interval while retaining the common equity history."""
    if max_points <= 0:
        raise ValueError("max_points must be positive")
    if common_start_at > window_end:
        raise ValueError("common_start_at must not be later than window_end")

    base_interval = _COMMON_EQUITY_BUCKET_SECONDS
    span_seconds = max(
        0,
        int((_as_utc(window_end) - _as_utc(common_start_at)).total_seconds()),
    )
    base_intervals = span_seconds // base_interval
    if base_intervals + 1 <= max_points:
        return base_interval

    if max_points == 1:
        return base_interval * (base_intervals + 1)

    # Round up to a multiple of the native 15-minute resolution.  The number
    # of generated buckets is then at most ``max_points`` for any history.
    multiplier = (base_intervals + max_points - 2) // (max_points - 1)
    return base_interval * max(multiplier, 1)


def _bucket_equity_observations(
    observations: Iterable[_EquityObservation],
    *,
    interval_seconds: int,
    bucket_origin: datetime | None = None,
) -> dict[datetime, _EquityObservation]:
    ordered = sorted(
        (
            _EquityObservation(
                observed_at=_as_utc(observation.observed_at),
                equity=observation.equity,
                source_observed_at=_as_utc(observation.source_observed_at),
            )
            for observation in observations
        ),
        key=lambda observation: observation.observed_at,
    )
    latest_by_bucket: dict[datetime, _EquityObservation] = {}
    for observation in ordered:
        bucket = (
            _bucket_start(observation.observed_at, interval_seconds)
            if bucket_origin is None
            else _relative_bucket_start(
                observation.observed_at,
                bucket_origin,
                interval_seconds,
            )
        )
        latest_by_bucket[bucket] = _EquityObservation(
            observed_at=bucket,
            equity=observation.equity,
            source_observed_at=observation.source_observed_at,
        )
    if ordered:
        first = ordered[0]
        first_bucket = _bucket_start(first.observed_at, interval_seconds)
        latest_by_bucket[first_bucket] = _EquityObservation(
            observed_at=first_bucket,
            equity=first.equity,
            source_observed_at=first.source_observed_at,
        )
    return latest_by_bucket


def _paper_equity_observations(
    rows: Iterable[PaperEquitySnapshotRow],
) -> list[_EquityObservation]:
    return [
        _EquityObservation(
            observed_at=_as_utc(row.observed_at),
            equity=row.equity,
            source_observed_at=_as_utc(row.observed_at),
        )
        for row in rows
        if row.equity is not None and row.equity > 0
    ]


def _paper_equity_observations_from_values(
    rows: Iterable[tuple[str, datetime, Decimal]],
) -> list[_EquityObservation]:
    return [
        _EquityObservation(
            observed_at=_as_utc(observed_at),
            equity=equity,
            source_observed_at=_as_utc(observed_at),
        )
        for _, observed_at, equity in rows
        if equity > 0
    ]


def _live_equity_observations(
    rows: Iterable[AccountBalanceSnapshotRow],
    *,
    account_label: str,
    cash_flow_adjustments: Sequence[LiveCashFlowAdjustment],
) -> list[_EquityObservation]:
    raw_by_timestamp: dict[datetime, Decimal] = {}
    for row in rows:
        if row.account_label != account_label:
            continue
        observed_at = _as_utc(row.observed_at)
        raw_by_timestamp[observed_at] = raw_by_timestamp.get(
            observed_at,
            Decimal("0"),
        ) + row.wallet_balance + (row.unrealized_pnl or Decimal("0"))

    return _apply_live_cash_flow_adjustments(
        (
            _EquityObservation(
                observed_at=observed_at,
                equity=equity,
                source_observed_at=observed_at,
            )
            for observed_at, equity in raw_by_timestamp.items()
        ),
        account_label=account_label,
        cash_flow_adjustments=cash_flow_adjustments,
    )


def _live_aggregated_equity_observations(
    rows: Iterable[tuple[datetime, Decimal]],
    *,
    account_label: str,
    cash_flow_adjustments: Sequence[LiveCashFlowAdjustment],
) -> list[_EquityObservation]:
    return _apply_live_cash_flow_adjustments(
        (
            _EquityObservation(
                observed_at=_as_utc(observed_at),
                equity=equity,
                source_observed_at=_as_utc(observed_at),
            )
            for observed_at, equity in rows
        ),
        account_label=account_label,
        cash_flow_adjustments=cash_flow_adjustments,
    )


def _apply_live_cash_flow_adjustments(
    observations: Iterable[_EquityObservation],
    *,
    account_label: str,
    cash_flow_adjustments: Sequence[LiveCashFlowAdjustment],
) -> list[_EquityObservation]:
    ordered_observations = sorted(
        observations,
        key=lambda observation: observation.source_observed_at,
    )

    adjustments = sorted(
        (
            adjustment
            for adjustment in cash_flow_adjustments
            if adjustment.account_label == account_label
        ),
        key=lambda adjustment: adjustment.effective_at,
    )
    adjusted_observations: list[_EquityObservation] = []
    cumulative_cash_flow = Decimal("0")
    adjustment_index = 0
    for observation in ordered_observations:
        source_observed_at = observation.source_observed_at
        while (
            adjustment_index < len(adjustments)
            and adjustments[adjustment_index].effective_at <= source_observed_at
        ):
            cumulative_cash_flow += adjustments[adjustment_index].amount
            adjustment_index += 1
        equity = observation.equity - cumulative_cash_flow
        if equity <= 0:
            continue
        adjusted_observations.append(
            _EquityObservation(
                observed_at=observation.observed_at,
                equity=equity,
                source_observed_at=source_observed_at,
            )
        )
    return adjusted_observations


def _build_common_equity_curve(
    observations: Iterable[_EquityObservation],
    *,
    common_start_at: datetime,
    end_at: datetime,
    interval_seconds: int = _COMMON_EQUITY_BUCKET_SECONDS,
    source_end_at: datetime | None = None,
    max_points: int = _EQUITY_MAX_POINTS,
) -> tuple[list[dict[str, JsonValue]], Decimal | None]:
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    if max_points <= 0:
        raise ValueError("max_points must be positive")
    resolved_source_end_at = (
        None if source_end_at is None else _as_utc(source_end_at)
    )
    bucket_origin = _as_utc(common_start_at)
    buckets = _bucket_equity_observations(
        (
            observation
            for observation in observations
            if (
                resolved_source_end_at is None
                or _as_utc(observation.source_observed_at)
                <= resolved_source_end_at
            )
        ),
        interval_seconds=interval_seconds,
        bucket_origin=bucket_origin,
    )
    if not buckets:
        return [], None
    start_at = bucket_origin
    end_bucket = _relative_bucket_end(
        bucket_origin,
        end_at,
        interval_seconds,
    )
    if end_bucket < start_at:
        return [], None

    # Keep this seam safe even if a future caller passes an unbounded
    # observation source.  The normal query path chooses an adaptive interval
    # first, so this is a defensive final cap rather than the hot path.
    latest_start_at = end_bucket - timedelta(
        seconds=interval_seconds * (max_points - 1)
    )
    if latest_start_at > start_at:
        start_at = latest_start_at

    baseline_observation = buckets.get(start_at)
    if baseline_observation is None:
        prior_buckets = [bucket for bucket in buckets if bucket <= start_at]
        if prior_buckets:
            baseline_observation = buckets[max(prior_buckets)]
        else:
            future_buckets = [bucket for bucket in buckets if bucket > start_at]
            if not future_buckets:
                return [], None
            baseline_observation = buckets[min(future_buckets)]
    baseline = baseline_observation.equity
    current = baseline_observation
    points: list[dict[str, JsonValue]] = []
    cursor = start_at
    while cursor <= end_bucket:
        observation = buckets.get(cursor)
        if observation is not None:
            current = observation
        delta = current.equity - baseline
        return_pct = None if baseline == 0 else delta / baseline * 100
        points.append(
            {
                "observed_at": cursor.isoformat(),
                "equity": str(current.equity),
                "delta": str(delta),
                "return_pct": None if return_pct is None else str(return_pct),
                "source_observed_at": current.source_observed_at.isoformat(),
            }
        )
        cursor += timedelta(seconds=interval_seconds)
    return points, baseline


def _live_cash_flow_payload(
    adjustment: LiveCashFlowAdjustment,
) -> dict[str, JsonValue]:
    return {
        "account_label": adjustment.account_label,
        "effective_at": _as_utc(adjustment.effective_at).isoformat(),
        "amount": str(adjustment.amount),
        "cash_flow_type": adjustment.cash_flow_type,
    }


def _common_equity_note(
    cash_flows: Sequence[dict[str, JsonValue]],
    *,
    interval_seconds: int | None = None,
) -> str:
    note = (
        "统一起点固定为 2026-08-21 02:45 UTC（北京时间 10:45），"
        "共同曲线按历史跨度自适应采样并限制点数；"
        "曲线展示现金流校正后的权益金额变化（USDT），该时点各账号均归零。"
    )
    if interval_seconds is not None:
        note = (
            f"{note} 当前采样间隔为 {interval_seconds // 60} 分钟。"
        )
    if not cash_flows:
        return f"{note} 当前未配置外部现金流校正。"
    details = "、".join(
        f"{flow.get('cash_flow_type', '现金流')} {flow.get('amount')} USDT "
        f"@ {flow.get('effective_at')}"
        for flow in cash_flows
    )
    return f"{note} 实盘已扣除：{details}。"


def _live_account_equity_point(
    row: AccountBalanceSnapshotRow | _AccountEquityPoint,
) -> dict[str, JsonValue]:
    equity = row.wallet_balance + row.unrealized_pnl
    return {
        "observed_at": row.observed_at.isoformat(),
        "balance": str(row.wallet_balance),
        "equity": str(equity),
        "realized_pnl": None,
        "unrealized_pnl": str(row.unrealized_pnl),
    }


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


def _live_observation(
    *,
    state: str,
    runtime_checkpoint_at: datetime | None,
    transition_at: datetime,
) -> tuple[datetime, str]:
    """Choose a heartbeat that belongs to the live session's current state."""
    if state == "live_enabled" and runtime_checkpoint_at is not None:
        return runtime_checkpoint_at, "runtime_checkpoint"
    return transition_at, "state_transition"


def _age(now: datetime, observed_at: datetime) -> float:
    return max(0.0, (now - observed_at).total_seconds())


def _order_intent_reason(details: object) -> str | None:
    if not isinstance(details, dict):
        return None
    reason = details.get("reason")
    return reason if isinstance(reason, str) and reason else None


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


def _universe_membership(
    row: MonitoringMembershipRow,
    entry: UniverseEntryRow | None,
) -> dict[str, JsonValue]:
    rank = None
    utc_day_return = None
    current_price = None
    if entry is not None:
        rank = (
            entry.gainer_rank
            if row.side == "gainer"
            else entry.loser_rank
            if row.side == "loser"
            else None
        )
        utc_day_return = (
            None
            if entry.utc_day_return is None
            else str(entry.utc_day_return)
        )
        current_price = (
            None if entry.current_price is None else str(entry.current_price)
        )
    return {
        "symbol": row.symbol,
        "status": row.status,
        "side": row.side,
        "rank": rank,
        "utc_day_return": utc_day_return,
        "current_price": current_price,
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


def _live_strategy_signal(row: LiveStrategySignalRow) -> dict[str, JsonValue]:
    return {
        "observation_id": row.observation_id,
        "signal_id": row.signal_id,
        "candidate_id": row.candidate_id,
        "run_id": row.run_id,
        "account_label": row.account_label,
        "strategy_name": row.strategy_name,
        "strategy_version": row.strategy_version,
        "config_hash": row.config_hash,
        "code_commit": row.code_commit,
        "signal_kind": row.signal_kind,
        "symbol": row.symbol,
        "side": row.side,
        "detected_at": row.detected_at.isoformat(),
        "source_state_at": row.source_state_at.isoformat(),
        "recorded_at": row.recorded_at.isoformat(),
        "reason": row.reason,
        "schema_version": row.schema_version,
        "quote_volume_24h": (
            None if row.quote_volume_24h is None else str(row.quote_volume_24h)
        ),
        "quote_volume_24h_quote_asset": row.quote_volume_24h_quote_asset,
        "quote_volume_24h_source": row.quote_volume_24h_source,
        "quote_volume_24h_source_at": (
            None
            if row.quote_volume_24h_source_at is None
            else row.quote_volume_24h_source_at.isoformat()
        ),
        "quote_volume_24h_fetched_at": (
            None
            if row.quote_volume_24h_fetched_at is None
            else row.quote_volume_24h_fetched_at.isoformat()
        ),
        "quote_volume_24h_age_ms": row.quote_volume_24h_age_ms,
        "features": _json_mapping(row.features),
        "reference_prices": _json_mapping(row.reference_prices),
        "market_context": _json_mapping(row.market_context),
        "filter_context": _json_mapping(row.filter_context),
        "candidate_context": _json_mapping(row.candidate_context),
        "account_context": _json_mapping(row.account_context),
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
