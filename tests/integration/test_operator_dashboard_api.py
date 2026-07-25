from fastapi.testclient import TestClient

from crypto_momentum_lab.operator_dashboard.api import create_dashboard_app


def test_operator_dashboard_reads_local_postgres(
    async_database_url: str,
) -> None:
    app = create_dashboard_app(database_url=async_database_url)

    with TestClient(app) as client:
        assert client.get("/api/health").status_code == 200
        for route in (
            "/api/overview",
            "/api/universe",
            "/api/strategy-runs/current",
            "/api/account",
            "/api/risk-execution",
            "/api/reports",
        ):
            response = client.get(route)
            assert response.status_code == 200, response.text
