"""Unit tests for domain Progress Contract and Readiness Evaluator."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.execution.progress_contract import (
    ExecutionReadiness,
    ProgressFreshnessSLA,
    ReadinessAssessment,
    ReadinessEvaluator,
)

NOW = datetime(2026, 9, 20, 12, 0, 0, tzinfo=timezone.utc)


def test_progress_contract_fresh_data_is_independent_executable() -> None:
    """When watermark lag is within SLA (<= 90s) and gap is 0, readiness is INDEPENDENT_EXECUTABLE."""
    watermark = NOW - timedelta(seconds=75)
    assessment = ReadinessEvaluator.evaluate(
        current_time=NOW,
        watermark_time=watermark,
        reconciliation_gap=Decimal("0"),
    )
    assert assessment.readiness == ExecutionReadiness.INDEPENDENT_EXECUTABLE
    assert assessment.allows_entries is True
    assert assessment.allows_exits is True
    assert assessment.lag_seconds == 75.0


def test_progress_contract_lagging_suppresses_entries_retains_exits() -> None:
    """When watermark lag exceeds execution SLA (e.g. 105s > 90s), system degrades to PROGRESS_LAGGING."""
    watermark = NOW - timedelta(seconds=105)
    assessment = ReadinessEvaluator.evaluate(
        current_time=NOW,
        watermark_time=watermark,
        reconciliation_gap=Decimal("0"),
    )
    assert assessment.readiness == ExecutionReadiness.PROGRESS_LAGGING
    # Restricts new entries
    assert assessment.allows_entries is False
    # Retains exit rights for safe risk reduction
    assert assessment.allows_exits is True
    assert "lag:105.0s" in assessment.reason


def test_progress_contract_reconciliation_gap_causes_lagging() -> None:
    """Even if lag is small, an unexplained reconciliation gap triggers PROGRESS_LAGGING."""
    watermark = NOW - timedelta(seconds=5)
    assessment = ReadinessEvaluator.evaluate(
        current_time=NOW,
        watermark_time=watermark,
        reconciliation_gap=Decimal("0.05"),
        context_reason="unreconciled_manual_fill",
    )
    assert assessment.readiness == ExecutionReadiness.PROGRESS_LAGGING
    assert assessment.allows_entries is False
    assert assessment.allows_exits is True
    assert "reconciliation_gap:0.05" in assessment.reason


def test_progress_contract_stalled_on_severe_lag() -> None:
    """When watermark lag exceeds stall SLA (> 300s), system halts execution completely (STALLED)."""
    watermark = NOW - timedelta(seconds=350)
    assessment = ReadinessEvaluator.evaluate(
        current_time=NOW,
        watermark_time=watermark,
        reconciliation_gap=Decimal("0"),
    )
    assert assessment.readiness == ExecutionReadiness.STALLED
    assert assessment.allows_entries is False
    assert assessment.allows_exits is False
    assert "staleness_threshold_exceeded" in assessment.reason


def test_progress_contract_missing_watermark() -> None:
    """Missing watermark defaults to PROGRESS_LAGGING, allowing exits if configured."""
    assessment = ReadinessEvaluator.evaluate(
        current_time=NOW,
        watermark_time=None,
    )
    assert assessment.readiness == ExecutionReadiness.PROGRESS_LAGGING
    assert assessment.allows_entries is False
    assert assessment.allows_exits is True
    assert assessment.reason == "watermark_missing"
