from datetime import UTC, datetime

from fastapi.testclient import TestClient

from crypto_momentum_lab.operator_dashboard.api import create_dashboard_app
from crypto_momentum_lab.operator_dashboard.schemas import (
    AccountOverviewResponse,
    PaperAccountsResponse,
    RiskExecutionResponse,
    RunReportSummaryResponse,
    StrategyRunResponse,
    SystemOverviewResponse,
    UniverseStatusResponse,
)
from crypto_momentum_lab.operator_dashboard.status import OperationalStatus

NOW = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
DASHBOARD_AUTH_KWARGS = {
    "auth_username": "operator",
    "auth_password": "test-password",
}
DASHBOARD_BASIC_AUTH = ("operator", "test-password")


def test_dashboard_app_serves_health_endpoint() -> None:
    with TestClient(
        create_dashboard_app(queries=FakeQueries(), **DASHBOARD_AUTH_KWARGS)
    ) as client:
        assert client.get("/api/health").status_code == 401
        response = client.get("/api/health", auth=DASHBOARD_BASIC_AUTH)

    assert response.status_code == 200
    assert response.json() == {"app_status": "UP", "database_status": "UP"}


def test_dashboard_app_allows_anonymous_api_access_without_credentials() -> None:
    with TestClient(create_dashboard_app(queries=FakeQueries())) as client:
        response = client.get("/api/health")

    assert response.status_code == 200
    assert response.json() == {"app_status": "UP", "database_status": "UP"}


def test_dashboard_app_mounts_static_index() -> None:
    with TestClient(
        create_dashboard_app(queries=FakeQueries(), **DASHBOARD_AUTH_KWARGS)
    ) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert "Execution" in response.text
    assert "Control Room" in response.text


class FakeQueries:
    async def health(self) -> dict[str, str]:
        return {"app_status": "UP", "database_status": "UP"}

    async def overview(self) -> SystemOverviewResponse:
        return SystemOverviewResponse(
            generated_at=NOW,
            database_status=OperationalStatus.READY,
            services=[],
            active_halt_count=0,
            active_lease=None,
        )

    async def universe(self) -> UniverseStatusResponse:
        return UniverseStatusResponse(
            status=OperationalStatus.NO_DATA,
            observed_at=None,
            gainers=[],
            losers=[],
            monitored_symbols=[],
        )

    async def strategy_run(self) -> StrategyRunResponse:
        return StrategyRunResponse(
            status=OperationalStatus.NO_DATA,
            run_id=None,
            strategy_name=None,
            config_hash=None,
            checkpoint_at=None,
            latest_signals=[],
            rejection_summary={},
        )

    async def paper_accounts(self) -> PaperAccountsResponse:
        return PaperAccountsResponse(
            status=OperationalStatus.READY,
            accounts=[await self.strategy_run()],
        )

    async def account(self) -> AccountOverviewResponse:
        return AccountOverviewResponse(
            status=OperationalStatus.UNKNOWN,
            observed_at=None,
            balances=[],
            positions=[],
            open_orders=[],
            fills=[],
        )

    async def risk_execution(self) -> RiskExecutionResponse:
        return RiskExecutionResponse(
            status=OperationalStatus.READY,
            active_halts=[],
            latest_risk_decisions=[],
            exchange_orders=[],
            ambiguous_orders=[],
        )

    async def reports(self) -> RunReportSummaryResponse:
        return RunReportSummaryResponse(
            status=OperationalStatus.NO_DATA,
            paper_runs=[],
            shadow_sessions=[],
            live_sessions=[],
        )
