from datetime import UTC, datetime, timedelta
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
from crypto_momentum_lab.persistence.postgres.models import (
    RuntimeMarketState15sRow,
    StrategyRuntimeEventRow,
)
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


@pytest.mark.asyncio
async def test_performance_queries_uses_market_state_progress_delay() -> None:
    mock_session = AsyncMock()
    mock_scalars_result = MagicMock()
    mock_scalars_result.all.return_value = []
    mock_session.scalars.return_value = mock_scalars_result

    market_state = MagicMock(spec=RuntimeMarketState15sRow)
    market_state.bucket_end = NOW - timedelta(seconds=12)
    market_state.created_at = NOW - timedelta(seconds=11, milliseconds=450)
    market_state.missing_agg_trade_count = 0

    market_progress = MagicMock(spec=StrategyRuntimeEventRow)
    market_progress.occurred_at = NOW - timedelta(seconds=11, milliseconds=450)
    market_progress.details = {"market_delay_ms": 550.0}

    mock_session.scalar.side_effect = [
        market_state,
        None,
        market_progress,
        0,
    ]
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
    assert result.market_data.status == OperationalStatus.READY
    assert result.market_data.market_delay_ms == 550.0
    assert result.market_data.realtime_closure_delay_seconds == 0.55


def test_assess_coverage_hardened_validation() -> None:
    from dataclasses import dataclass

    from crypto_momentum_lab.operator_dashboard.performance_builder import (
        _assess_coverage,
    )

    @dataclass
    class DummyCF:
        correction_id: str
        evidence_hash: str
        approval_ref: str
        effective_at: datetime

    start = datetime(2026, 9, 20, 0, 0, tzinfo=UTC)
    end = datetime(2026, 9, 21, 0, 0, tzinfo=UTC)
    valid_eff = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)

    # 1. Valid hex hash and interval
    valid_cf = DummyCF("cf1", "a" * 64, "appr_1", valid_eff)
    ok, status, proof = _assess_coverage([valid_cf], start, end)
    assert ok is True
    assert status == "confirmed"
    assert "audited_records_count_1" in proof

    # 2. Non-hex characters in 64-char string (e.g. 'z')
    invalid_hex_cf = DummyCF("cf2", "z" * 64, "appr_1", valid_eff)
    ok, status, proof = _assess_coverage([invalid_hex_cf], start, end)
    assert ok is False
    assert status == "uncertified"
    assert "invalid_evidence_hash_format" in proof

    # 3. All zeros dummy hash
    zero_cf = DummyCF("cf3", "0" * 64, "appr_1", valid_eff)
    ok, status, proof = _assess_coverage([zero_cf], start, end)
    assert ok is False
    assert status == "uncertified"
    assert "zero_placeholder_evidence" in proof

    # 4. Empty approval ref
    no_appr_cf = DummyCF("cf4", "f" * 64, "   ", valid_eff)
    ok, status, proof = _assess_coverage([no_appr_cf], start, end)
    assert ok is False
    assert status == "uncertified"
    assert "missing_approval_ref" in proof

    # 5. Out of bounds effective_at
    out_of_bounds_cf = DummyCF("cf5", "b" * 64, "appr_1", end + timedelta(seconds=1))
    ok, status, proof = _assess_coverage([out_of_bounds_cf], start, end)
    assert ok is False
    assert status == "uncertified"
    assert "cash_flow_out_of_interval" in proof
