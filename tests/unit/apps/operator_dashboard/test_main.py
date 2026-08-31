from datetime import UTC, datetime

from fastapi.testclient import TestClient

from crypto_momentum_lab.operator_dashboard.api import create_dashboard_app
from crypto_momentum_lab.operator_dashboard.schemas import (
    AccountOverviewResponse,
    PaperAccountHistoryResponse,
    PaperAccountsEquityResponse,
    PaperAccountsResponse,
    PaperAccountSummaryResponse,
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
            accounts=[
                PaperAccountSummaryResponse(
                    status=OperationalStatus.READY,
                    run_id="paper-account-test",
                    strategy_name="compression_breakout",
                    config_hash="config-hash",
                    checkpoint_at=NOW,
                    portfolio_summary={"equity": "1000"},
                )
            ],
        )

    async def paper_account_equity(self) -> PaperAccountsEquityResponse:
        return PaperAccountsEquityResponse(
            status=OperationalStatus.READY,
            accounts=[],
        )

    async def paper_account(self, run_id: str) -> StrategyRunResponse:
        response = await self.strategy_run()
        response.run_id = run_id
        return response

    async def paper_history(
        self,
        run_id: str,
        *,
        full: bool = False,
    ) -> PaperAccountHistoryResponse:
        del full
        return PaperAccountHistoryResponse(
            status=OperationalStatus.READY,
            run_id=run_id,
            closed_trade_count=0,
            closed_trades=[],
            trade_events=[],
        )

    async def account(self, equity_range: str = "24h") -> AccountOverviewResponse:
        return AccountOverviewResponse(
            status=OperationalStatus.UNKNOWN,
            observed_at=None,
            equity_range=equity_range,
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
            pending_orders=[],
            ambiguous_orders=[],
        )

    async def reports(self) -> RunReportSummaryResponse:
        return RunReportSummaryResponse(
            status=OperationalStatus.NO_DATA,
            shadow_sessions=[],
            live_sessions=[],
        )
