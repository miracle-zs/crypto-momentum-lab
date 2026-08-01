from fastapi.testclient import TestClient

from crypto_momentum_lab.operator_dashboard.api import create_dashboard_app
from tests.unit.apps.operator_dashboard.test_main import (
    DASHBOARD_AUTH_KWARGS,
    DASHBOARD_BASIC_AUTH,
    FakeQueries,
)


def test_overview_endpoint_aggregates_service_status() -> None:
    with TestClient(
        create_dashboard_app(queries=FakeQueries(), **DASHBOARD_AUTH_KWARGS)
    ) as client:
        response = client.get("/api/overview", auth=DASHBOARD_BASIC_AUTH)

    assert response.status_code == 200
    assert response.json()["database_status"] == "READY"


def test_all_read_only_dashboard_routes_are_available() -> None:
    with TestClient(
        create_dashboard_app(queries=FakeQueries(), **DASHBOARD_AUTH_KWARGS)
    ) as client:
        for route in (
            "/api/universe",
            "/api/strategy-runs/current",
            "/api/paper-accounts",
            "/api/paper-accounts/paper-account-test/history",
            "/api/account",
            "/api/risk-execution",
            "/api/reports",
        ):
            assert client.get(route, auth=DASHBOARD_BASIC_AUTH).status_code == 200
