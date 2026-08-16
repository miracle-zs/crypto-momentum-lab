from datetime import UTC, datetime

from crypto_momentum_lab.operator_dashboard.schemas import (
    AccountOverviewResponse,
    RunReportSummaryResponse,
    SystemOverviewResponse,
)
from crypto_momentum_lab.operator_dashboard.status import OperationalStatus


def test_dashboard_overview_schema_excludes_secret_fields() -> None:
    schema_text = str(SystemOverviewResponse.model_json_schema()).lower()

    assert "api_key" not in schema_text
    assert "api_secret" not in schema_text
    assert "credential" not in schema_text


def test_report_schema_does_not_repeat_paper_accounts() -> None:
    assert "paper_runs" not in RunReportSummaryResponse.model_fields


def test_account_schema_exposes_live_equity_curve() -> None:
    observed_at = datetime(2026, 8, 16, 2, 0, tzinfo=UTC)
    response = AccountOverviewResponse(
        status=OperationalStatus.READY,
        observed_at=observed_at,
        balances=[],
        positions=[],
        open_orders=[],
        fills=[],
        equity_window_start=observed_at,
        equity_window_end=observed_at,
        equity_sample_interval_seconds=360,
        equity_curve=[
            {
                "observed_at": observed_at.isoformat(),
                "balance": "282.28",
                "equity": "276.80",
                "unrealized_pnl": "-5.48",
            }
        ],
    )

    assert response.equity_sample_interval_seconds == 360
    assert response.equity_curve[0]["equity"] == "276.80"
