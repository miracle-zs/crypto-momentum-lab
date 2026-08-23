from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from crypto_momentum_lab.operator_dashboard.api import create_dashboard_app
from crypto_momentum_lab.operator_dashboard.queries import _account_equity_range
from tests.unit.apps.operator_dashboard.test_main import FakeQueries


def test_live_account_equity_ranges_bound_chart_density() -> None:
    assert _account_equity_range("24h") == (timedelta(hours=24), 6 * 60)
    assert _account_equity_range("7d") == (timedelta(days=7), 60 * 60)
    assert _account_equity_range("30d") == (timedelta(days=30), 3 * 60 * 60)
    assert _account_equity_range("1y") == (
        timedelta(days=365),
        2 * 24 * 60 * 60,
    )
    with pytest.raises(ValueError, match="unsupported account equity range"):
        _account_equity_range("all")


def test_account_equity_range_is_validated_and_returned() -> None:
    with TestClient(create_dashboard_app(queries=FakeQueries())) as client:
        for equity_range in ("24h", "7d", "30d", "1y"):
            response = client.get(
                "/api/account",
                params={"equity_range": equity_range},
            )
            assert response.status_code == 200
            assert response.json()["equity_range"] == equity_range

        invalid = client.get(
            "/api/account",
            params={"equity_range": "all"},
        )

    assert invalid.status_code == 422
