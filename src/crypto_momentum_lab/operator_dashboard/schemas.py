from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.operator_dashboard.status import OperationalStatus


class DashboardSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ServiceStatusResponse(DashboardSchema):
    name: str
    status: OperationalStatus
    observed_at: datetime | None
    age_seconds: float | None
    details: dict[str, JsonValue] = Field(default_factory=dict)


class DecisionSLOLatencyResponse(DashboardSchema):
    sample_count: int
    p50_ms: float
    p95_ms: float
    max_ms: float


class DecisionSLOConsumerResponse(DashboardSchema):
    consumer: str
    observed_event_count: int
    recovery_count: int
    unavailable_event_count: int
    lag_event_count: int
    last_available: bool | None = None
    last_recovery_reason: str | None = None
    last_observed_at: datetime | None = None


class DecisionSLOResponse(DashboardSchema):
    """Bounded historical decision-path SLO aggregates."""

    status: OperationalStatus
    window: Literal["1h", "6h", "24h", "7d"]
    window_start: datetime
    window_end: datetime
    persisted_event_count: int
    truncated: bool = False
    phase_latency: dict[str, DecisionSLOLatencyResponse] = Field(
        default_factory=dict
    )
    terminal_reasons: dict[str, dict[str, dict[str, int]]] = Field(
        default_factory=dict
    )
    consumers: list[DecisionSLOConsumerResponse] = Field(default_factory=list)


class LiveAccountSummaryResponse(DashboardSchema):
    """Operational state for one configured live account."""

    account_label: str
    environment: str
    status: OperationalStatus
    readiness: str
    observed_at: datetime | None = None
    strategy_name: str | None = None
    strategy_state: str | None = None
    lease_expires_at: datetime | None = None


class LiveAccountMetricPointResponse(DashboardSchema):
    """One aligned live-account equity and margin observation.

    Ratio fields are decimal ratios (``0.01`` means ``1%``) so callers can
    render them without losing precision while keeping the API numeric shape
    consistent with the existing dashboard payloads.
    """

    observed_at: datetime
    equity: str
    equity_change_ratio: str | None = None
    margin_used: str
    margin_occupancy_ratio: str | None = None
    drawdown: str
    drawdown_ratio: str | None = None


class LiveAccountMetricsAccountResponse(DashboardSchema):
    """Time-series metrics for one live account in the comparison fleet."""

    account_label: str
    environment: str
    status: OperationalStatus
    metrics_curve: list[LiveAccountMetricPointResponse] = Field(
        default_factory=list
    )


class LiveAccountMetricsResponse(DashboardSchema):
    """Comparable equity, margin, and drawdown curves for live accounts."""

    status: OperationalStatus
    equity_range: Literal["24h", "7d", "30d", "1y"] = "24h"
    equity_window_start: datetime | None = None
    equity_window_end: datetime | None = None
    equity_sample_interval_seconds: int | None = None
    accounts: list[LiveAccountMetricsAccountResponse] = Field(
        default_factory=list
    )


class SystemOverviewResponse(DashboardSchema):
    generated_at: datetime
    database_status: OperationalStatus
    services: list[ServiceStatusResponse]
    active_halt_count: int
    active_lease: dict[str, JsonValue] | None
    active_leases: list[dict[str, JsonValue]] = Field(default_factory=list)
    account_statuses: list[LiveAccountSummaryResponse] = Field(default_factory=list)


class ResearchCollectorResponse(DashboardSchema):
    status: OperationalStatus
    status_detail: str
    generated_at: datetime
    environment: str
    checkpoint_at: datetime | None
    checkpoint_age_seconds: float | None
    last_bucket_start: datetime | None
    last_sequence: int | None
    last_symbol: str | None
    stream_id: str | None
    stale: bool
    capacity_state: str
    collector_bytes: int
    collector_soft_limit_bytes: int
    collector_hard_limit_bytes: int
    disk_free_bytes: int
    disk_warning_free_bytes: int
    disk_pause_free_bytes: int
    pending_spool_files: int
    pending_spool_bytes: int
    parquet_file_count: int
    parquet_first_window_start: datetime | None
    parquet_latest_window_start: datetime | None
    parquet_latest_written_at: datetime | None
    parquet_latest_age_seconds: float | None
    parquet_window_seconds: int
    parquet_gap_count: int
    top_count: int
    late_tolerance_seconds: int
    max_spool_bytes: int
    pending_spool_overdue_files: int = 0
    pending_spool_oldest_age_seconds: float | None = None
    alerts: list[str] = Field(default_factory=list)
    recent_windows: list[dict[str, JsonValue]] = Field(default_factory=list)


class UniverseStatusResponse(DashboardSchema):
    status: OperationalStatus
    observed_at: datetime | None
    gainers: list[dict[str, JsonValue]]
    losers: list[dict[str, JsonValue]]
    monitored_symbols: list[dict[str, JsonValue]]


class StrategyRunResponse(DashboardSchema):
    status: OperationalStatus
    run_id: str | None
    strategy_name: str | None
    exit_mode: str | None = None
    exit_label: str | None = None
    config_hash: str | None
    checkpoint_at: datetime | None
    equity_window_start: datetime | None = None
    equity_window_end: datetime | None = None
    equity_sample_interval_seconds: int | None = None
    portfolio_summary: dict[str, JsonValue] = Field(default_factory=dict)
    equity_curve: list[dict[str, JsonValue]] = Field(default_factory=list)
    open_positions: list[dict[str, JsonValue]] = Field(default_factory=list)
    closed_trades: list[dict[str, JsonValue]] = Field(default_factory=list)
    trade_events: list[dict[str, JsonValue]] = Field(default_factory=list)
    latest_signals: list[dict[str, JsonValue]]
    latest_paper_fills: list[dict[str, JsonValue]] = Field(default_factory=list)
    rejection_summary: dict[str, JsonValue]


class PaperAccountSummaryResponse(DashboardSchema):
    status: OperationalStatus
    run_id: str | None
    strategy_name: str | None
    exit_mode: str | None = None
    exit_label: str | None = None
    config_hash: str | None
    checkpoint_at: datetime | None
    portfolio_summary: dict[str, JsonValue] = Field(default_factory=dict)


class PaperAccountEquityResponse(DashboardSchema):
    run_id: str
    strategy_name: str
    exit_mode: str
    exit_label: str
    equity_window_start: datetime
    equity_window_end: datetime
    equity_sample_interval_seconds: int
    source: str = "paper"
    account_label: str | None = None
    equity_curve: list[dict[str, JsonValue]] = Field(default_factory=list)
    common_equity_baseline: str | None = None
    common_equity_curve: list[dict[str, JsonValue]] = Field(default_factory=list)


class PaperAccountsEquityResponse(DashboardSchema):
    status: OperationalStatus
    accounts: list[PaperAccountEquityResponse] = Field(default_factory=list)
    common_equity_start_at: datetime | None = None
    common_equity_end_at: datetime | None = None
    common_equity_sample_interval_seconds: int | None = None
    common_equity_anchor: str | None = None
    common_equity_anchor_accounts: list[str] = Field(default_factory=list)
    common_equity_account_count: int = 0
    common_equity_cash_flows: list[dict[str, JsonValue]] = Field(default_factory=list)
    common_equity_note: str | None = None


class PaperAccountsResponse(DashboardSchema):
    status: OperationalStatus
    accounts: list[PaperAccountSummaryResponse] = Field(default_factory=list)


class PaperAccountHistoryResponse(DashboardSchema):
    status: OperationalStatus
    run_id: str
    closed_trade_count: int
    history_complete: bool = True
    closed_trades: list[dict[str, JsonValue]] = Field(default_factory=list)
    trade_events: list[dict[str, JsonValue]] = Field(default_factory=list)


class AccountOverviewResponse(DashboardSchema):
    status: OperationalStatus
    observed_at: datetime | None
    balances: list[dict[str, JsonValue]]
    positions: list[dict[str, JsonValue]]
    open_orders: list[dict[str, JsonValue]]
    fills: list[dict[str, JsonValue]]
    live_signals: list[dict[str, JsonValue]] = Field(default_factory=list)
    environment: str | None = None
    account_label: str | None = None
    account_config: dict[str, JsonValue] = Field(default_factory=dict)
    reconciliation: dict[str, JsonValue] = Field(default_factory=dict)
    summary: dict[str, JsonValue] = Field(default_factory=dict)
    equity_range: Literal["24h", "7d", "30d", "1y"] = "24h"
    equity_window_start: datetime | None = None
    equity_window_end: datetime | None = None
    equity_sample_interval_seconds: int | None = None
    equity_curve: list[dict[str, JsonValue]] = Field(default_factory=list)
    available_accounts: list[LiveAccountSummaryResponse] = Field(default_factory=list)


class LiveAccountsResponse(DashboardSchema):
    status: OperationalStatus
    accounts: list[LiveAccountSummaryResponse] = Field(default_factory=list)


class RiskExecutionResponse(DashboardSchema):
    status: OperationalStatus
    active_halts: list[dict[str, JsonValue]]
    latest_risk_decisions: list[dict[str, JsonValue]]
    exchange_orders: list[dict[str, JsonValue]]
    pending_orders: list[dict[str, JsonValue]] = Field(default_factory=list)
    ambiguous_orders: list[dict[str, JsonValue]]


class RunReportSummaryResponse(DashboardSchema):
    status: OperationalStatus
    shadow_sessions: list[dict[str, JsonValue]]
    live_sessions: list[dict[str, JsonValue]]
