import asyncio

from fastapi.testclient import TestClient

from crypto_momentum_lab.operator_dashboard.api import (
    _ResponseCache,
    create_dashboard_app,
)
from crypto_momentum_lab.operator_dashboard.schemas import (
    DecisionSLOResponse,
    PaperAccountHistoryResponse,
    SystemOverviewResponse,
)
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
            "/api/decision-slo",
            "/api/research-collector",
            "/api/universe",
            "/api/strategy-runs/current",
            "/api/paper-accounts",
            "/api/paper-accounts/equity",
            "/api/paper-accounts/paper-account-test",
            "/api/paper-accounts/paper-account-test/history",
            "/api/account",
            "/api/live-accounts",
            "/api/live-account-metrics",
            "/api/risk-execution",
            "/api/reports",
            "/api/performance",
            "/api/readiness",
        ):
            assert client.get(route, auth=DASHBOARD_BASIC_AUTH).status_code == 200


def test_readiness_endpoint_returns_layered_state() -> None:
    with TestClient(
        create_dashboard_app(queries=FakeQueries(), **DASHBOARD_AUTH_KWARGS)
    ) as client:
        response = client.get("/api/readiness", auth=DASHBOARD_BASIC_AUTH)

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "READY"
    assert payload["liveness"] == {"app_status": "UP", "database_status": "UP"}
    assert payload["tradeability"]["mode"] == "FULLY_TRADEABLE"
    assert payload["tradeability"]["entry_gate_open"] is True
    assert payload["stream_readiness"]["overall"] == "READY"
    assert "account" in payload["stream_readiness"]["streams"]


def test_decision_slo_endpoint_accepts_a_bounded_window() -> None:
    with TestClient(create_dashboard_app(queries=FakeQueries())) as client:
        response = client.get("/api/decision-slo?window=7d")

    assert response.status_code == 200
    assert response.json()["window"] == "7d"


def test_account_endpoint_accepts_an_account_label() -> None:
    with TestClient(create_dashboard_app(queries=FakeQueries())) as client:
        response = client.get("/api/account?account_label=account-3&equity_range=7d")

    assert response.status_code == 200
    assert response.json()["account_label"] == "account-3"
    assert response.json()["equity_range"] == "7d"


def test_equity_endpoint_exposes_unified_start_comparison_metadata() -> None:
    with TestClient(create_dashboard_app(queries=FakeQueries())) as client:
        response = client.get("/api/paper-accounts/equity")

    assert response.status_code == 200
    payload = response.json()
    assert payload["common_equity_start_at"] is None
    assert payload["common_equity_account_count"] == 0
    assert payload["common_equity_cash_flows"] == []


def test_live_accounts_endpoint_returns_account_collection() -> None:
    with TestClient(create_dashboard_app(queries=FakeQueries())) as client:
        response = client.get("/api/live-accounts")

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["accounts"]) == 1
    assert payload["accounts"][0]["account_label"] == "primary"


def test_live_account_metrics_endpoint_accepts_equity_range() -> None:
    with TestClient(create_dashboard_app(queries=FakeQueries())) as client:
        response = client.get("/api/live-account-metrics?equity_range=7d")

    assert response.status_code == 200
    payload = response.json()
    assert payload["equity_range"] == "7d"
    assert payload["accounts"] == []


def test_paper_accounts_starts_with_summary_and_loads_detail_separately() -> None:
    with TestClient(create_dashboard_app(queries=FakeQueries())) as client:
        summary = client.get("/api/paper-accounts")
        detail = client.get("/api/paper-accounts/paper-account-test")

    assert summary.status_code == 200
    assert "equity_curve" not in summary.json()["accounts"][0]
    assert "open_positions" not in summary.json()["accounts"][0]
    assert detail.status_code == 200
    assert "equity_curve" in detail.json()


def test_paper_history_full_flag_uses_separate_cache_entry() -> None:
    class RecordingQueries(FakeQueries):
        def __init__(self) -> None:
            self.full_history_requests: list[bool] = []

        async def paper_history(
            self,
            run_id: str,
            *,
            full: bool = False,
        ) -> PaperAccountHistoryResponse:
            self.full_history_requests.append(full)
            return await super().paper_history(run_id)

    queries = RecordingQueries()
    with TestClient(create_dashboard_app(queries=queries)) as client:
        assert client.get(
            "/api/paper-accounts/paper-account-test/history"
        ).status_code == 200
        assert client.get(
            "/api/paper-accounts/paper-account-test/history?full=true"
        ).status_code == 200

    assert queries.full_history_requests == [False, True]


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


async def test_response_cache_serves_stale_equity_while_refreshing() -> None:
    cache = _ResponseCache(ttl_seconds=0.05)
    calls = 0

    async def loader() -> int:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.005)
        return calls

    try:
        assert await cache.get("paper-accounts-equity", loader) == 1
        await asyncio.sleep(0.06)

        # An expired response is immediately available while one refresh runs
        # in the background, so a cold database query does not block the UI.
        assert (
            await cache.get(
                "paper-accounts-equity",
                loader,
                stale_while_revalidate_seconds=0.2,
            )
            == 1
        )
        await asyncio.sleep(0.02)
        assert calls == 2
        assert await cache.get("paper-accounts-equity", loader) == 2
    finally:
        await cache.aclose()


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


def test_account_timeout_returns_gateway_timeout() -> None:
    class SlowAccountQueries(FakeQueries):
        async def account(self, equity_range: str = "24h") -> object:
            del equity_range
            raise TimeoutError("account query timed out")

    with TestClient(create_dashboard_app(queries=SlowAccountQueries())) as client:
        response = client.get("/api/account?equity_range=30d")

    assert response.status_code == 504
    assert response.json()["detail"] == "dashboard account query timed out"


def test_static_assets_cache_headers() -> None:
    with TestClient(create_dashboard_app(queries=FakeQueries())) as client:
        vendor_res = client.get("/static/vendor/echarts.min.js")
        assert vendor_res.status_code == 200
        assert "max-age=31536000" in vendor_res.headers.get("Cache-Control", "")

        css_res = client.get("/static/dashboard.css")
        assert css_res.status_code == 200
        assert "max-age=86400" in css_res.headers.get("Cache-Control", "")

        html_res = client.get("/static/index.html")
        assert html_res.status_code == 200
        assert "max-age" not in html_res.headers.get("Cache-Control", "")


def test_decision_slo_caches_response() -> None:
    calls = 0

    class CountingQueries(FakeQueries):
        async def decision_slo(
            self,
            window: str = "24h",
        ) -> DecisionSLOResponse:
            nonlocal calls
            calls += 1
            return await super().decision_slo(window)

    with TestClient(create_dashboard_app(queries=CountingQueries())) as client:
        res1 = client.get("/api/decision-slo?window=24h")
        assert res1.status_code == 200
        assert calls == 1

        res2 = client.get("/api/decision-slo?window=24h")
        assert res2.status_code == 200
        assert calls == 1


