from datetime import datetime

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


class SystemOverviewResponse(DashboardSchema):
    generated_at: datetime
    database_status: OperationalStatus
    services: list[ServiceStatusResponse]
    active_halt_count: int
    active_lease: dict[str, JsonValue] | None


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
    equity_curve: list[dict[str, JsonValue]] = Field(default_factory=list)


class PaperAccountsEquityResponse(DashboardSchema):
    status: OperationalStatus
    accounts: list[PaperAccountEquityResponse] = Field(default_factory=list)


class PaperAccountsResponse(DashboardSchema):
    status: OperationalStatus
    accounts: list[PaperAccountSummaryResponse] = Field(default_factory=list)


class PaperAccountHistoryResponse(DashboardSchema):
    status: OperationalStatus
    run_id: str
    closed_trade_count: int
    closed_trades: list[dict[str, JsonValue]] = Field(default_factory=list)
    trade_events: list[dict[str, JsonValue]] = Field(default_factory=list)


class AccountOverviewResponse(DashboardSchema):
    status: OperationalStatus
    observed_at: datetime | None
    balances: list[dict[str, JsonValue]]
    positions: list[dict[str, JsonValue]]
    open_orders: list[dict[str, JsonValue]]
    fills: list[dict[str, JsonValue]]
    environment: str | None = None
    account_label: str | None = None
    account_config: dict[str, JsonValue] = Field(default_factory=dict)
    reconciliation: dict[str, JsonValue] = Field(default_factory=dict)
    summary: dict[str, JsonValue] = Field(default_factory=dict)


class RiskExecutionResponse(DashboardSchema):
    status: OperationalStatus
    active_halts: list[dict[str, JsonValue]]
    latest_risk_decisions: list[dict[str, JsonValue]]
    exchange_orders: list[dict[str, JsonValue]]
    ambiguous_orders: list[dict[str, JsonValue]]


class RunReportSummaryResponse(DashboardSchema):
    status: OperationalStatus
    shadow_sessions: list[dict[str, JsonValue]]
    live_sessions: list[dict[str, JsonValue]]
