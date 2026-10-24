"""Query interface consumed by the dashboard HTTP application."""

from typing import Any, Protocol

from crypto_momentum_lab.operator_dashboard.schemas import (
    AccountOverviewResponse,
    LiveAccountMetricsResponse,
    LiveAccountsResponse,
    ResearchCollectorResponse,
    RiskExecutionResponse,
    RunReportSummaryResponse,
    SystemOverviewResponse,
    SystemPerformanceResponse,
    SystemReadinessResponse,
    UniverseStatusResponse,
)


class DashboardQueryProtocol(Protocol):
    async def health(self) -> dict[str, str]: ...

    async def operational_health(self) -> dict[str, Any]: ...

    async def readiness(self) -> SystemReadinessResponse: ...

    async def overview(self) -> SystemOverviewResponse: ...

    async def research_collector(self) -> ResearchCollectorResponse: ...

    async def universe(self) -> UniverseStatusResponse: ...

    async def account(
        self,
        equity_range: str = "24h",
        account_label: str | None = None,
        environment: str | None = None,
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
        window: str = "24h",
    ) -> SystemPerformanceResponse: ...
