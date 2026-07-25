from fastapi.testclient import TestClient

from crypto_momentum_lab.operator_dashboard.api import create_dashboard_app
from tests.unit.apps.operator_dashboard.test_main import FakeQueries


def test_dashboard_local_server_renders_overview() -> None:
    with TestClient(create_dashboard_app(queries=FakeQueries())) as client:
        assert client.get("/").status_code == 200
        assert client.get("/api/overview").status_code == 200
