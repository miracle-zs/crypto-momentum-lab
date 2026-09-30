"""Query interface consumed by the dashboard HTTP application."""

from datetime import datetime
from typing import Any, Protocol

from crypto_momentum_lab.operator_dashboard.schemas import (
    AccountOverviewResponse,
    DecisionSLOResponse,
    LiveAccountMetricsResponse,
    LiveAccountsResponse,
    PaperAccountHistoryResponse,
    PaperAccountsEquityResponse,
    PaperAccountsResponse,
    ResearchCollectorResponse,
    RiskExecutionResponse,
    RunReportSummaryResponse,
    StrategyRunResponse,
    SystemOverviewResponse,
    SystemPerformanceResponse,
    SystemReadinessResponse,
    UniverseStatusResponse,
)


class DashboardQueryProtocol(Protocol):
    async def health(self) -> dict[str, str]: ...

    async def operational_health(self) -> dict[str, Any]: ...

    async def readiness(self) -> SystemReadinessResponse: ...

    async def decision_slo(
        self,
        window: str = "24h",
    ) -> DecisionSLOResponse: ...

    async def overview(self) -> SystemOverviewResponse: ...

    async def research_collector(self) -> ResearchCollectorResponse: ...

    async def universe(self) -> UniverseStatusResponse: ...

    async def strategy_run(self) -> StrategyRunResponse: ...

    async def paper_accounts(self) -> PaperAccountsResponse: ...

    async def paper_account_equity(self) -> PaperAccountsEquityResponse: ...

    async def paper_account(self, run_id: str) -> StrategyRunResponse: ...

    async def paper_history(
        self,
        run_id: str,
        *,
        full: bool = False,
    ) -> PaperAccountHistoryResponse: ...

    async def account(
        self,
        equity_range: str = "24h",
        account_label: str | None = None,
    ) -> AccountOverviewResponse: ...

    async def live_accounts(self) -> LiveAccountsResponse: ...

    async def live_account_metrics(
        self,
        equity_range: str = "24h",
    ) -> LiveAccountMetricsResponse: ...

    async def risk_execution(self) -> RiskExecutionResponse: ...

    async def reports(self) -> RunReportSummaryResponse: ...

    async def performance(
        self,
        window: str = "6h",
    ) -> SystemPerformanceResponse: ...

    async def account_performance(
        self,
        account_label: str = "primary",
        window_hours: int = 24,
        environment: str = "live",
        asset: str = "USDT",
        end_time: datetime | None = None,
    ) -> dict[str, object]: ...
