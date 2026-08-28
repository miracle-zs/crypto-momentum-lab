import asyncio

from fastapi.testclient import TestClient

from crypto_momentum_lab.operator_dashboard.api import (
    _ResponseCache,
    create_dashboard_app,
)
from crypto_momentum_lab.operator_dashboard.schemas import SystemOverviewResponse
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
            "/api/paper-accounts/equity",
            "/api/paper-accounts/paper-account-test",
            "/api/paper-accounts/paper-account-test/history",
            "/api/account",
            "/api/risk-execution",
            "/api/reports",
        ):
            assert client.get(route, auth=DASHBOARD_BASIC_AUTH).status_code == 200


def test_equity_endpoint_exposes_unified_start_comparison_metadata() -> None:
    with TestClient(create_dashboard_app(queries=FakeQueries())) as client:
        response = client.get("/api/paper-accounts/equity")

    assert response.status_code == 200
    payload = response.json()
    assert payload["common_equity_start_at"] is None
    assert payload["common_equity_account_count"] == 0
    assert payload["common_equity_cash_flows"] == []


def test_paper_accounts_starts_with_summary_and_loads_detail_separately() -> None:
    with TestClient(create_dashboard_app(queries=FakeQueries())) as client:
        summary = client.get("/api/paper-accounts")
        detail = client.get("/api/paper-accounts/paper-account-test")

    assert summary.status_code == 200
    assert "equity_curve" not in summary.json()["accounts"][0]
    assert "open_positions" not in summary.json()["accounts"][0]
    assert detail.status_code == 200
    assert "equity_curve" in detail.json()


def test_dashboard_enables_gzip_for_large_static_responses() -> None:
    with TestClient(create_dashboard_app(queries=FakeQueries())) as client:
        response = client.get(
            "/static/dashboard.js",
            headers={"Accept-Encoding": "gzip"},
        )

    assert response.status_code == 200
    assert response.headers["content-encoding"] == "gzip"


async def test_response_cache_deduplicates_concurrent_loads_and_expires() -> None:
    cache = _ResponseCache(ttl_seconds=0.02)
    calls = 0

    async def loader() -> int:
        nonlocal calls
        await asyncio.sleep(0.005)
        calls += 1
        return calls

    values = await asyncio.gather(
        *(cache.get("paper-accounts", loader) for _ in range(4))
    )
    assert values == [1, 1, 1, 1]
    assert await cache.get("paper-accounts", loader) == 1

    await asyncio.sleep(0.025)
    assert await cache.get("paper-accounts", loader) == 2


def test_overview_timeout_returns_gateway_timeout() -> None:
    class SlowQueries(FakeQueries):
        async def overview(self) -> SystemOverviewResponse:
            await asyncio.sleep(0.05)
            return await super().overview()

    with TestClient(
        create_dashboard_app(
            queries=SlowQueries(),
            overview_query_timeout_seconds=0.01,
        )
    ) as client:
        response = client.get("/api/overview")

    assert response.status_code == 504
    assert response.json()["detail"] == "dashboard overview query timed out"
