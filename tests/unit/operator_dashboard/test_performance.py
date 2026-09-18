from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from crypto_momentum_lab.operator_dashboard import api as dashboard_api
from crypto_momentum_lab.operator_dashboard.api import create_dashboard_app
from crypto_momentum_lab.operator_dashboard.performance_queries import (
    PerformanceQueries,
    _account_label_and_phase,
    _percentile,
)
from crypto_momentum_lab.operator_dashboard.schemas import (
    SystemPerformanceResponse,
)
from crypto_momentum_lab.operator_dashboard.status import OperationalStatus
from tests.unit.apps.operator_dashboard.test_main import (
    DASHBOARD_AUTH_KWARGS,
    DASHBOARD_BASIC_AUTH,
    FakeQueries,
)

NOW = datetime(2026, 9, 18, 0, 0, tzinfo=UTC)


def test_percentile_calculation() -> None:
    values = [10.0, 20.0, 30.0, 40.0, 50.0]
    assert _percentile(values, 0.50) == 30.0
    assert _percentile(values, 0.95) == 50.0
    assert _percentile([], 0.50) == 0.0


def test_account_label_and_phase_mapping() -> None:
    assert _account_label_and_phase("cml-live-primary") == ("primary", 0.0)
    assert _account_label_and_phase("cml-live-account-2") == ("account-2", 15.0)
    assert _account_label_and_phase("cml-live-account-3") == ("account-3", 30.0)
    assert _account_label_and_phase("cml-live-account-4") == ("account-4", 45.0)
    assert _account_label_and_phase("other-run") == ("other-run", 0.0)


def test_performance_endpoint_returns_200_and_matches_schema() -> None:
    with TestClient(
        create_dashboard_app(queries=FakeQueries(), **DASHBOARD_AUTH_KWARGS)
    ) as client:
        response = client.get("/api/performance", auth=DASHBOARD_BASIC_AUTH)

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "READY"
    assert "decision_slo" in data
    assert "persistence" in data
    assert "market_data" in data
    assert "host_resources" in data


@pytest.mark.asyncio
async def test_performance_queries_aggregates_empty_database() -> None:
    mock_session = AsyncMock()
    mock_scalars_result = MagicMock()
    mock_scalars_result.all.return_value = []
    mock_session.scalars.return_value = mock_scalars_result
    mock_session.scalar.return_value = None
    mock_session.execute.return_value = MagicMock(
        first=lambda: (2, 45),
        scalar=lambda: 1024 * 1024 * 50,
    )

    class MockSessionFactory:
        def __call__(self) -> Any:
            cm = MagicMock()
            cm.__aenter__ = AsyncMock(return_value=mock_session)
            cm.__aexit__ = AsyncMock(return_value=None)
            return cm

    queries = PerformanceQueries(
        MockSessionFactory(),
        clock=lambda: NOW,
    )
    result = await queries.performance()
    assert isinstance(result, SystemPerformanceResponse)
    assert result.status == OperationalStatus.NO_DATA
    assert result.persistence.sample_count == 0
    assert result.market_data.missing_agg_trade_count == 0


def test_performance_endpoint_caches_response() -> None:
    calls = 0

    class CountingQueries(FakeQueries):
        async def performance(self, window: str = "6h"):
            nonlocal calls
            calls += 1
            return await super().performance(window)

    with TestClient(
        create_dashboard_app(queries=CountingQueries(), **DASHBOARD_AUTH_KWARGS)
    ) as client:
        first = client.get("/api/performance", auth=DASHBOARD_BASIC_AUTH)
        second = client.get("/api/performance", auth=DASHBOARD_BASIC_AUTH)

    assert first.status_code == 200
    assert second.status_code == 200
    assert calls == 1


def test_performance_endpoint_returns_504_when_query_exceeds_timeout() -> None:
    async def slow_performance(self, window: str = "6h"):
        del self, window
        raise TimeoutError("slow performance query")

    class SlowQueries(FakeQueries):
        performance = slow_performance

    with (
        patch.object(dashboard_api, "_PERFORMANCE_QUERY_TIMEOUT_SECONDS", 0.05),
        TestClient(
            create_dashboard_app(queries=SlowQueries(), **DASHBOARD_AUTH_KWARGS)
        ) as client,
    ):
        response = client.get("/api/performance", auth=DASHBOARD_BASIC_AUTH)

    assert response.status_code == 504
    assert "timed out" in response.json()["detail"]


def test_dashboard_js_uses_fetch_timeout_and_section_inflight() -> None:
    from pathlib import Path

    javascript = (
        Path(__file__).resolve().parents[3]
        / "src/crypto_momentum_lab/operator_dashboard/static/dashboard.js"
    ).read_text(encoding="utf-8")

    assert "AbortSignal.timeout" in javascript
    assert "sectionInFlight" in javascript
    assert "pollInFlight" not in javascript
