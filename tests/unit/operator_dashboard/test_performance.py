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
    from decimal import Decimal

    from crypto_momentum_lab.operator_dashboard.performance_builder import (
        _assess_coverage,
        compute_cash_flow_evidence_hash,
    )

    @dataclass
    class DummyCF:
        correction_id: str
        evidence_hash: str
        approval_ref: str
        effective_at: datetime

    @dataclass
    class FullCF:
        correction_id: str
        account_label: str
        amount: Decimal
        cash_flow_type: str
        effective_at: datetime
        reason: str
        approval_ref: str
        evidence_hash: str

    start = datetime(2026, 9, 20, 0, 0, tzinfo=UTC)
    end = datetime(2026, 9, 21, 0, 0, tzinfo=UTC)
    valid_eff = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    good_hash = compute_cash_flow_evidence_hash(
        correction_id="cf1",
        account_label="primary",
        amount=Decimal("10"),
        cash_flow_type="deposit",
        effective_at=valid_eff,
        reason="wire",
        approval_ref="appr_1",
    )

    # 1. Content hash matches the canonical record
    valid_cf = FullCF(
        "cf1", "primary", Decimal("10"), "deposit", valid_eff, "wire", "appr_1",
        good_hash,
    )
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


def test_assess_coverage_requires_content_hash_match_and_effective_at() -> None:
    from dataclasses import dataclass
    from decimal import Decimal

    from crypto_momentum_lab.operator_dashboard.performance_builder import (
        _assess_coverage,
        compute_cash_flow_evidence_hash,
    )

    @dataclass
    class FullCF:
        correction_id: str
        account_label: str
        amount: Decimal
        cash_flow_type: str
        effective_at: datetime
        reason: str
        approval_ref: str
        evidence_hash: str

    start = datetime(2026, 9, 20, 0, 0, tzinfo=UTC)
    end = datetime(2026, 9, 21, 0, 0, tzinfo=UTC)
    eff = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    good_hash = compute_cash_flow_evidence_hash(
        correction_id="cf1",
        account_label="primary",
        amount=Decimal("10"),
        cash_flow_type="deposit",
        effective_at=eff,
        reason="wire",
        approval_ref="appr_42",
    )

    ok_row = FullCF(
        "cf1", "primary", Decimal("10"), "deposit", eff, "wire", "appr_42", good_hash
    )
    ok, status, proof = _assess_coverage([ok_row], start, end)
    assert ok is True
    assert status == "confirmed"

    bad_hash = FullCF(
        "cf1", "primary", Decimal("10"), "deposit", eff, "wire", "appr_42", "a" * 64
    )
    ok, _, proof = _assess_coverage([bad_hash], start, end)
    assert ok is False
    assert "content_mismatch" in proof

    missing_eff = FullCF(
        "cf1", "primary", Decimal("10"), "deposit", None, "wire", "appr_42", good_hash
    )
    ok, _, proof = _assess_coverage([missing_eff], start, end)
    assert ok is False
    assert "missing_effective_at" in proof

    placeholder = FullCF(
        "cf1", "primary", Decimal("10"), "deposit", eff, "wire", "system", good_hash
    )
    ok, _, proof = _assess_coverage([placeholder], start, end)
    assert ok is False
    assert "missing_approval_ref" in proof


def test_assess_coverage_requires_equity_window_bracket() -> None:
    from dataclasses import dataclass
    from decimal import Decimal

    from crypto_momentum_lab.operator_dashboard.performance_builder import (
        _assess_coverage,
        compute_cash_flow_evidence_hash,
    )

    @dataclass
    class FullCF:
        correction_id: str
        account_label: str
        amount: Decimal
        cash_flow_type: str
        effective_at: datetime
        reason: str
        approval_ref: str
        evidence_hash: str

    @dataclass
    class Eq:
        observed_at: datetime
        wallet_balance: Decimal

    start = datetime(2026, 9, 20, 0, 0, tzinfo=UTC)
    end = datetime(2026, 9, 21, 0, 0, tzinfo=UTC)
    eff = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    good_hash = compute_cash_flow_evidence_hash(
        correction_id="cf1",
        account_label="primary",
        amount=Decimal("10"),
        cash_flow_type="deposit",
        effective_at=eff,
        reason="wire",
        approval_ref="appr_42",
    )
    cf = FullCF(
        "cf1", "primary", Decimal("10"), "deposit", eff, "wire", "appr_42", good_hash
    )

    short = [
        Eq(datetime(2026, 9, 20, 6, 0, tzinfo=UTC), Decimal("1")),
        Eq(datetime(2026, 9, 20, 18, 0, tzinfo=UTC), Decimal("2")),
    ]
    ok, _, proof = _assess_coverage([cf], start, end, equity_rows=short)
    assert ok is False
    assert "equity_window_incomplete" in proof

    brackets = [
        Eq(start, Decimal("1")),
        Eq(end, Decimal("2")),
    ]
    ok, status, _ = _assess_coverage([cf], start, end, equity_rows=brackets)
    assert ok is True
    assert status == "confirmed"

    # Gap detection
    gap_rows = [
        Eq(start, Decimal("1")),
        Eq(start + timedelta(hours=1), Decimal("1.1")),
        Eq(end, Decimal("2")),  # 23-hour gap
    ]
    ok_gap, status_gap, proof_gap = _assess_coverage(
        [cf], start, end, equity_rows=gap_rows, max_equity_gap=timedelta(hours=2)
    )
    assert ok_gap is False
    assert status_gap == "uncertified"
    assert "equity_window_gap_detected" in proof_gap
