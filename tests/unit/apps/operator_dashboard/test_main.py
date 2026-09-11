from datetime import UTC, datetime
from typing import Literal

from fastapi.testclient import TestClient

from crypto_momentum_lab.operator_dashboard.api import create_dashboard_app
from crypto_momentum_lab.operator_dashboard.schemas import (
    AccountOverviewResponse,
    DecisionSLOResponse,
    LiveAccountMetricsResponse,
    LiveAccountsResponse,
    LiveAccountSummaryResponse,
    PaperAccountHistoryResponse,
    PaperAccountsEquityResponse,
    PaperAccountsResponse,
    PaperAccountSummaryResponse,
    ResearchCollectorResponse,
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

    async def decision_slo(
        self,
        window: Literal["1h", "6h", "24h", "7d"] = "24h",
    ) -> DecisionSLOResponse:
        return DecisionSLOResponse(
            status=OperationalStatus.NO_DATA,
            window=window,
            window_start=NOW,
            window_end=NOW,
            persisted_event_count=0,
        )

    async def overview(self) -> SystemOverviewResponse:
        return SystemOverviewResponse(
            generated_at=NOW,
            database_status=OperationalStatus.READY,
            services=[],
            active_halt_count=0,
            active_lease=None,
        )

    async def research_collector(self) -> ResearchCollectorResponse:
        return ResearchCollectorResponse(
            status=OperationalStatus.FRESH,
            status_detail="checkpoint 与 Parquet 窗口持续更新",
            generated_at=NOW,
            environment="research",
            checkpoint_at=NOW,
            checkpoint_age_seconds=0,
            last_bucket_start=NOW,
            last_sequence=1,
            last_symbol="BTCUSDT",
            stream_id="stream-id",
            stale=False,
            capacity_state="healthy",
            collector_bytes=1024,
            collector_soft_limit_bytes=6 * 1024**3,
            collector_hard_limit_bytes=8 * 1024**3,
            disk_free_bytes=40 * 1024**3,
            disk_warning_free_bytes=15 * 1024**3,
            disk_pause_free_bytes=10 * 1024**3,
            pending_spool_files=0,
            pending_spool_bytes=0,
            parquet_file_count=1,
            parquet_first_window_start=NOW,
            parquet_latest_window_start=NOW,
            parquet_latest_written_at=NOW,
            parquet_latest_age_seconds=0,
            parquet_window_seconds=900,
            parquet_gap_count=0,
            top_count=30,
            late_tolerance_seconds=30,
            max_spool_bytes=1024**3,
            alerts=[],
            recent_windows=[],
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

    async def account(
        self,
        equity_range: str = "24h",
        account_label: str | None = None,
    ) -> AccountOverviewResponse:
        return AccountOverviewResponse(
            status=OperationalStatus.UNKNOWN,
            observed_at=None,
            account_label=account_label,
            equity_range=equity_range,
            balances=[],
            positions=[],
            open_orders=[],
            fills=[],
        )

    async def live_accounts(self) -> LiveAccountsResponse:
        return LiveAccountsResponse(
            status=OperationalStatus.READY,
            accounts=[
                LiveAccountSummaryResponse(
                    account_label="primary",
                    environment="live",
                    status=OperationalStatus.READY,
                    readiness="ready_readonly",
                )
            ],
        )

    async def live_account_metrics(
        self,
        equity_range: str = "24h",
    ) -> LiveAccountMetricsResponse:
        return LiveAccountMetricsResponse(
            status=OperationalStatus.NO_DATA,
            equity_range=equity_range,
            accounts=[],
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
