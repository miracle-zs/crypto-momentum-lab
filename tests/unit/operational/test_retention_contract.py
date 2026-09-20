from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from crypto_momentum_lab.domain.operational.retention_contract import (
    RetentionConsumerRequirement,
    RetentionGatingEvaluation,
    RetentionWatermarkEvaluator,
)


def test_retention_consumer_requirement_validations() -> None:
    now = datetime.now(timezone.utc)
    req = RetentionConsumerRequirement(
        consumer_id="active_episodes",
        min_required_watermark=now,
        reason="Preserve open positions",
    )
    assert req.consumer_id == "active_episodes"
    assert req.min_required_watermark == now

    with pytest.raises(ValueError, match="consumer_id must not be empty"):
        RetentionConsumerRequirement(
            consumer_id="  ",
            min_required_watermark=now,
            reason="Empty id",
        )

    naive_time = datetime(2026, 9, 20, 12, 0, 0)
    with pytest.raises(ValueError, match="min_required_watermark must be timezone-aware"):
        RetentionConsumerRequirement(
            consumer_id="active_episodes",
            min_required_watermark=naive_time,
            reason="Naive datetime",
        )


def test_retention_gating_evaluation_validations() -> None:
    now = datetime.now(timezone.utc)
    older = now - timedelta(days=1)
    newer = now + timedelta(days=1)

    eval_result = RetentionGatingEvaluation(
        requested_cutoff=now,
        effective_cutoff=older,
        is_constrained=True,
        binding_constraint=None,
    )
    assert eval_result.is_constrained is True
    assert eval_result.effective_cutoff == older

    with pytest.raises(ValueError, match="effective_cutoff .* must never be newer than requested_cutoff"):
        RetentionGatingEvaluation(
            requested_cutoff=now,
            effective_cutoff=newer,
            is_constrained=False,
            binding_constraint=None,
        )


def test_retention_watermark_evaluator_unconstrained() -> None:
    requested = datetime(2026, 9, 1, 0, 0, tzinfo=timezone.utc)
    res = RetentionWatermarkEvaluator.evaluate_cutoff(
        requested_cutoff=requested,
        requirements=(),
    )
    assert res.requested_cutoff == requested
    assert res.effective_cutoff == requested
    assert res.is_constrained is False
    assert res.binding_constraint is None


def test_retention_watermark_evaluator_constrained_by_active_episode() -> None:
    requested = datetime(2026, 9, 20, 0, 0, tzinfo=timezone.utc)
    req_active = RetentionConsumerRequirement(
        consumer_id="active_episodes",
        min_required_watermark=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
        reason="Active trade episode opened on Sep 10",
    )

    res = RetentionWatermarkEvaluator.evaluate_cutoff(
        requested_cutoff=requested,
        requirements=(req_active,),
    )
    assert res.is_constrained is True
    assert res.effective_cutoff == datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)
    assert res.binding_constraint == req_active


def test_retention_watermark_evaluator_multiple_requirements_takes_safest_minimum() -> None:
    requested = datetime(2026, 9, 20, 0, 0, tzinfo=timezone.utc)
    req_journal = RetentionConsumerRequirement(
        consumer_id="uncommitted_journal",
        min_required_watermark=datetime(2026, 9, 15, 0, 0, tzinfo=timezone.utc),
        reason="Uncommitted journal buffer",
    )
    req_episode = RetentionConsumerRequirement(
        consumer_id="active_episodes",
        min_required_watermark=datetime(2026, 9, 5, 0, 0, tzinfo=timezone.utc),
        reason="Long running episode",
    )
    req_dashboard = RetentionConsumerRequirement(
        consumer_id="dashboard_metrics",
        min_required_watermark=datetime(2026, 9, 18, 0, 0, tzinfo=timezone.utc),
        reason="Dashboard 48h rolling window",
    )

    res = RetentionWatermarkEvaluator.evaluate_cutoff(
        requested_cutoff=requested,
        requirements=(req_journal, req_episode, req_dashboard),
    )
    assert res.is_constrained is True
    assert res.effective_cutoff == datetime(2026, 9, 5, 0, 0, tzinfo=timezone.utc)
    assert res.binding_constraint == req_episode


def test_retention_watermark_evaluator_ignores_future_requirements() -> None:
    requested = datetime(2026, 9, 10, 0, 0, tzinfo=timezone.utc)
    # A requirement that only needs data from Sep 15 onwards doesn't restrict pruning before Sep 10
    req_future = RetentionConsumerRequirement(
        consumer_id="future_worker",
        min_required_watermark=datetime(2026, 9, 15, 0, 0, tzinfo=timezone.utc),
        reason="Worker starts from Sep 15",
    )

    res = RetentionWatermarkEvaluator.evaluate_cutoff(
        requested_cutoff=requested,
        requirements=(req_future,),
    )
    assert res.is_constrained is False
    assert res.effective_cutoff == requested
    assert res.binding_constraint is None
