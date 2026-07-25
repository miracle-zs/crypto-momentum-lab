from fastapi.testclient import TestClient

from crypto_momentum_lab.operator_dashboard.api import create_dashboard_app
from tests.unit.apps.operator_dashboard.test_main import FakeQueries


def test_future_action_routes_return_not_implemented() -> None:
    with TestClient(create_dashboard_app(queries=FakeQueries())) as client:
        for route in (
            "/api/actions/halt",
            "/api/actions/drain",
            "/api/actions/cancel-all",
            "/api/actions/flatten",
            "/api/actions/release-lease",
        ):
            assert client.post(route).status_code == 404
