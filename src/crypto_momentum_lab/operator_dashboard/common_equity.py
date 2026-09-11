"""Common-equity normalization and curve construction.

The dashboard compares paper runs with live accounts on one fixed origin. This
module owns the pure read-model work behind that seam: timezone normalization,
cash-flow adjustment, adaptive bucketing, baseline selection, and bounded
curve construction. Database query orchestration stays outside this module.
"""

from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.operator_dashboard.live_account_metrics_queries import (
    AccountEquityPoint,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountBalanceSnapshotRow,
    PaperEquitySnapshotRow,
)

_COMMON_EQUITY_BUCKET_SECONDS = 15 * 60
_EQUITY_MAX_POINTS = 240


@dataclass(frozen=True, slots=True)
class LiveCashFlowAdjustment:
    account_label: str
    effective_at: datetime
    amount: Decimal
    cash_flow_type: str = "deposit"


@dataclass(frozen=True, slots=True)
class EquityObservation:
    observed_at: datetime
    equity: Decimal
    source_observed_at: datetime


@dataclass(frozen=True, slots=True)
class CommonEquityResult:
    """Bounded common-equity result consumed by the dashboard response."""

    curves_by_run: dict[str, list[dict[str, JsonValue]]] = field(
        default_factory=dict
    )
    baselines_by_run: dict[str, Decimal] = field(default_factory=dict)
    end_at: datetime | None = None
    anchor_accounts: list[str] = field(default_factory=list)
    cash_flows: list[dict[str, JsonValue]] = field(default_factory=list)
    note: str | None = None
    start_at: datetime | None = None
    interval_seconds: int | None = None


def as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def bucket_start(value: datetime, interval_seconds: int) -> datetime:
    observed_at = as_utc(value)
    epoch_seconds = int(observed_at.timestamp())
    bucket_epoch = epoch_seconds // interval_seconds * interval_seconds
    return datetime.fromtimestamp(bucket_epoch, tz=UTC)


def relative_bucket_start(
    value: datetime,
    origin: datetime,
    interval_seconds: int,
) -> datetime:
    """Return a bucket boundary measured from a caller-provided origin."""
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    observed_at = as_utc(value)
    bucket_origin = as_utc(origin)
    elapsed_seconds = int((observed_at - bucket_origin).total_seconds())
    bucket_offset = elapsed_seconds // interval_seconds * interval_seconds
    return bucket_origin + timedelta(seconds=bucket_offset)


def relative_bucket_end(
    origin: datetime,
    value: datetime,
    interval_seconds: int,
) -> datetime:
    """Return the last complete relative bucket at or before ``value``."""
    return relative_bucket_start(value, origin, interval_seconds)


def common_equity_interval_seconds(
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
        int((as_utc(window_end) - as_utc(common_start_at)).total_seconds()),
    )
    base_intervals = span_seconds // base_interval
    if base_intervals + 1 <= max_points:
        return base_interval

    if max_points == 1:
        return base_interval * (base_intervals + 1)

    multiplier = (base_intervals + max_points - 2) // (max_points - 1)
    return base_interval * max(multiplier, 1)


def bucket_equity_observations(
    observations: Iterable[EquityObservation],
    *,
    interval_seconds: int,
    bucket_origin: datetime | None = None,
) -> dict[datetime, EquityObservation]:
    ordered = sorted(
        (
            EquityObservation(
                observed_at=as_utc(observation.observed_at),
                equity=observation.equity,
                source_observed_at=as_utc(observation.source_observed_at),
            )
            for observation in observations
        ),
        key=lambda observation: observation.observed_at,
    )
    latest_by_bucket: dict[datetime, EquityObservation] = {}
    for observation in ordered:
        bucket = (
            bucket_start(observation.observed_at, interval_seconds)
            if bucket_origin is None
            else relative_bucket_start(
                observation.observed_at,
                bucket_origin,
                interval_seconds,
            )
        )
        latest_by_bucket[bucket] = EquityObservation(
            observed_at=bucket,
            equity=observation.equity,
            source_observed_at=observation.source_observed_at,
        )
    if ordered:
        first = ordered[0]
        first_bucket = bucket_start(first.observed_at, interval_seconds)
        latest_by_bucket[first_bucket] = EquityObservation(
            observed_at=first_bucket,
            equity=first.equity,
            source_observed_at=first.source_observed_at,
        )
    return latest_by_bucket


def paper_equity_observations(
    rows: Iterable[PaperEquitySnapshotRow],
) -> list[EquityObservation]:
    return [
        EquityObservation(
            observed_at=as_utc(row.observed_at),
            equity=row.equity,
            source_observed_at=as_utc(row.observed_at),
        )
        for row in rows
        if row.equity is not None and row.equity > 0
    ]


def paper_equity_observations_from_values(
    rows: Iterable[tuple[str, datetime, Decimal]],
) -> list[EquityObservation]:
    return [
        EquityObservation(
            observed_at=as_utc(observed_at),
            equity=equity,
            source_observed_at=as_utc(observed_at),
        )
        for _, observed_at, equity in rows
        if equity > 0
    ]


def live_equity_observations(
    rows: Iterable[AccountBalanceSnapshotRow],
    *,
    account_label: str,
    cash_flow_adjustments: Sequence[LiveCashFlowAdjustment],
) -> list[EquityObservation]:
    raw_by_timestamp: dict[datetime, Decimal] = {}
    for row in rows:
        if row.account_label != account_label:
            continue
        observed_at = as_utc(row.observed_at)
        raw_by_timestamp[observed_at] = raw_by_timestamp.get(
            observed_at,
            Decimal("0"),
        ) + row.wallet_balance + (row.unrealized_pnl or Decimal("0"))

    return apply_live_cash_flow_adjustments(
        (
            EquityObservation(
                observed_at=observed_at,
                equity=equity,
                source_observed_at=observed_at,
            )
            for observed_at, equity in raw_by_timestamp.items()
        ),
        account_label=account_label,
        cash_flow_adjustments=cash_flow_adjustments,
    )


def live_aggregated_equity_observations(
    rows: Iterable[tuple[datetime, Decimal]],
    *,
    account_label: str,
    cash_flow_adjustments: Sequence[LiveCashFlowAdjustment],
) -> list[EquityObservation]:
    return apply_live_cash_flow_adjustments(
        (
            EquityObservation(
                observed_at=as_utc(observed_at),
                equity=equity,
                source_observed_at=as_utc(observed_at),
            )
            for observed_at, equity in rows
        ),
        account_label=account_label,
        cash_flow_adjustments=cash_flow_adjustments,
    )


def apply_live_cash_flow_adjustments(
    observations: Iterable[EquityObservation],
    *,
    account_label: str,
    cash_flow_adjustments: Sequence[LiveCashFlowAdjustment],
) -> list[EquityObservation]:
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
    adjusted_observations: list[EquityObservation] = []
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
            EquityObservation(
                observed_at=observation.observed_at,
                equity=equity,
                source_observed_at=source_observed_at,
            )
        )
    return adjusted_observations


def build_common_equity_curve(
    observations: Iterable[EquityObservation],
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
        None if source_end_at is None else as_utc(source_end_at)
    )
    bucket_origin = as_utc(common_start_at)
    buckets = bucket_equity_observations(
        (
            observation
            for observation in observations
            if (
                resolved_source_end_at is None
                or as_utc(observation.source_observed_at)
                <= resolved_source_end_at
            )
        ),
        interval_seconds=interval_seconds,
        bucket_origin=bucket_origin,
    )
    if not buckets:
        return [], None
    start_at = bucket_origin
    end_bucket = relative_bucket_end(
        bucket_origin,
        end_at,
        interval_seconds,
    )
    if end_bucket < start_at:
        return [], None

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


def live_cash_flow_payload(
    adjustment: LiveCashFlowAdjustment,
) -> dict[str, JsonValue]:
    return {
        "account_label": adjustment.account_label,
        "effective_at": as_utc(adjustment.effective_at).isoformat(),
        "amount": str(adjustment.amount),
        "cash_flow_type": adjustment.cash_flow_type,
    }


def common_equity_note(
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
        note = f"{note} 当前采样间隔为 {interval_seconds // 60} 分钟。"
    if not cash_flows:
        return f"{note} 当前未配置外部现金流校正。"
    details = "、".join(
        f"{flow.get('cash_flow_type', '现金流')} {flow.get('amount')} USDT "
        f"@ {flow.get('effective_at')}"
        for flow in cash_flows
    )
    return f"{note} 实盘已扣除：{details}。"


def live_account_equity_point(
    row: AccountBalanceSnapshotRow | AccountEquityPoint,
) -> dict[str, JsonValue]:
    equity = row.wallet_balance + row.unrealized_pnl
    return {
        "observed_at": row.observed_at.isoformat(),
        "balance": str(row.wallet_balance),
        "equity": str(equity),
        "realized_pnl": None,
        "unrealized_pnl": str(row.unrealized_pnl),
    }


def build_common_equity_result(
    *,
    paper_rows: Sequence[tuple[str, datetime, Decimal]],
    live_rows_by_account: Mapping[str, Sequence[tuple[datetime, Decimal]]],
    run_ids: Sequence[str],
    common_start_at: datetime | None,
    window_end: datetime,
    first_buckets: Mapping[str, datetime],
    live_account_labels: Collection[str],
    cash_flow_adjustments: Sequence[LiveCashFlowAdjustment],
) -> CommonEquityResult:
    """Build all common-equity response fields from normalized observations."""
    if common_start_at is None:
        return CommonEquityResult()

    interval_seconds = common_equity_interval_seconds(
        common_start_at,
        window_end,
    )
    common_observations: dict[str, list[EquityObservation]] = {
        run_id: paper_equity_observations_from_values(
            (
                row_run_id,
                observed_at,
                equity,
            )
            for row_run_id, observed_at, equity in paper_rows
            if row_run_id == run_id
        )
        for run_id in run_ids
    }
    for account_label, equity_rows in live_rows_by_account.items():
        live_run_id = f"live-{account_label}-b1"
        common_observations[live_run_id] = live_aggregated_equity_observations(
            equity_rows,
            account_label=account_label,
            cash_flow_adjustments=cash_flow_adjustments,
        )

    available_observations = {
        run_id: observations
        for run_id, observations in common_observations.items()
        if observations
    }
    if len(available_observations) < 2:
        return CommonEquityResult()

    source_end_at = min(
        max(observation.source_observed_at for observation in observations)
        for observations in available_observations.values()
    )
    curve_end_at = relative_bucket_end(
        common_start_at,
        source_end_at,
        interval_seconds,
    )
    curves_by_run: dict[str, list[dict[str, JsonValue]]] = {}
    baselines_by_run: dict[str, Decimal] = {}
    for run_id, observations in available_observations.items():
        curve, baseline = build_common_equity_curve(
            observations,
            common_start_at=common_start_at,
            end_at=curve_end_at,
            interval_seconds=interval_seconds,
            source_end_at=source_end_at,
            max_points=_EQUITY_MAX_POINTS,
        )
        if len(curve) >= 2 and baseline is not None:
            curves_by_run[run_id] = curve
            baselines_by_run[run_id] = baseline

    if not curves_by_run:
        return CommonEquityResult()
    cash_flows = [
        live_cash_flow_payload(adjustment)
        for adjustment in cash_flow_adjustments
        if (
            adjustment.account_label in live_account_labels
            and adjustment.effective_at <= source_end_at
        )
    ]
    return CommonEquityResult(
        curves_by_run=curves_by_run,
        baselines_by_run=baselines_by_run,
        end_at=source_end_at,
        anchor_accounts=[
            run_id
            for run_id, first_at in first_buckets.items()
            if run_id in curves_by_run and first_at == common_start_at
        ],
        cash_flows=cash_flows,
        note=common_equity_note(
            cash_flows,
            interval_seconds=interval_seconds,
        ),
        start_at=common_start_at,
        interval_seconds=interval_seconds,
    )


__all__ = [
    "CommonEquityResult",
    "EquityObservation",
    "LiveCashFlowAdjustment",
    "build_common_equity_curve",
    "build_common_equity_result",
    "common_equity_interval_seconds",
    "live_account_equity_point",
]
