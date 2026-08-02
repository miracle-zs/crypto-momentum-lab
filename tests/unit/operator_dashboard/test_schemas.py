from crypto_momentum_lab.operator_dashboard.schemas import (
    RunReportSummaryResponse,
    SystemOverviewResponse,
)


def test_dashboard_overview_schema_excludes_secret_fields() -> None:
    schema_text = str(SystemOverviewResponse.model_json_schema()).lower()

    assert "api_key" not in schema_text
    assert "api_secret" not in schema_text
    assert "credential" not in schema_text


def test_report_schema_does_not_repeat_paper_accounts() -> None:
    assert "paper_runs" not in RunReportSummaryResponse.model_fields
