from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from crypto_momentum_lab.operator_dashboard.api import create_dashboard_app
from crypto_momentum_lab.next_gen.dashboard import (
    account_equity_range as _account_equity_range,
)
from tests.unit.apps.operator_dashboard.test_main import FakeQueries


def test_live_account_equity_ranges_bound_chart_density() -> None:
    assert _account_equity_range("24h") == (timedelta(hours=24), 6 * 60)
    for equity_range in ("7d", "30d", "1y", "all"):
        with pytest.raises(ValueError, match="unsupported account equity range"):
            _account_equity_range(equity_range)


def test_account_equity_range_is_validated_and_returned() -> None:
    with TestClient(create_dashboard_app(queries=FakeQueries())) as client:
        response = client.get("/api/account", params={"equity_range": "24h"})
        assert response.status_code == 200
        assert response.json()["equity_range"] == "24h"

        invalid_responses = [
            client.get("/api/account", params={"equity_range": value})
            for value in ("7d", "30d", "1y", "all")
        ]

    assert [response.status_code for response in invalid_responses] == [422] * 4
