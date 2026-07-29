from fastapi.testclient import TestClient

from crypto_momentum_lab.operator_dashboard.api import create_dashboard_app


def test_operator_dashboard_reads_local_postgres(
    async_database_url: str,
) -> None:
    app = create_dashboard_app(
        database_url=async_database_url,
        auth_username="operator",
        auth_password="test-password",
    )

    with TestClient(app) as client:
        auth = ("operator", "test-password")
        assert client.get("/api/health", auth=auth).status_code == 200
        for route in (
            "/api/overview",
            "/api/universe",
            "/api/strategy-runs/current",
            "/api/paper-accounts",
            "/api/account",
            "/api/risk-execution",
            "/api/reports",
        ):
            response = client.get(route, auth=auth)
            assert response.status_code == 200, response.text
