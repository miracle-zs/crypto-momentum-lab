"""Unit tests for 17-Day Walk-Forward Analysis and Alpha decay evaluation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from local_optimization.optimizer import CandidateEvaluation
from local_optimization.protocol import ParameterCandidate
from local_optimization.run_walk_forward_analysis import (
    SplitEvaluationResult,
    WindowSplit,
    filter_events_by_params,
    generate_rolling_splits,
    render_walk_forward_markdown_report,
)


def test_generate_rolling_splits_no_lookahead() -> None:
    """Verify that rolling window splits strictly enforce causal time boundaries."""
    t_start = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)
    t_end = datetime(2026, 9, 15, 0, 0, tzinfo=UTC)

    splits = generate_rolling_splits(
        start_date=t_start,
        end_date=t_end,
        is_days=7,
        oos_days=3,
        step_days=2,
    )

    assert len(splits) > 0

    for i, s in enumerate(splits):
        # 1. No internal overlap: IS ends exactly when OOS begins
        assert s.is_start < s.is_end
        assert s.is_end == s.oos_start
        assert s.oos_start < s.oos_end

        # 2. Step forward consistency
        if i > 0:
            prev = splits[i - 1]
            assert s.is_start == prev.is_start + timedelta(days=2)
            assert s.is_end == prev.is_end + timedelta(days=2)


def test_filter_events_by_params_accuracy() -> None:
    """Verify filter_events_by_params correctly filters based on 7D thresholds."""
    t0 = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
    events: list[dict[str, Any]] = [
        {
            "symbol": "BTCUSDT",
            "detected_at": t0,
            "detected_epoch": t0.timestamp(),
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "impulse_return_pct": 0.8,
            "min_imbalance": 0.35,
            "confirmation_min": 0.35,
            "min_intensity": 4.5,
            "min_volume_ratio": 1.8,
            "net_pnl_usdt": 2.5,
        },
        {
            "symbol": "ETHUSDT",
            "detected_at": t0 + timedelta(minutes=5),
            "detected_epoch": (t0 + timedelta(minutes=5)).timestamp(),
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "impulse_return_pct": 0.4,  # Below min_return
            "min_imbalance": 0.35,
            "confirmation_min": 0.35,
            "min_intensity": 4.5,
            "min_volume_ratio": 1.8,
            "net_pnl_usdt": -1.5,
        },
    ]

    params = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.5,
        "min_imbalance": 0.30,
        "min_intensity": 4.0,
        "min_volume_ratio": 1.5,
        "cooldown_buckets": 0,
    }

    sel, pnl, mdd = filter_events_by_params(events, params)
    assert len(sel) == 1
    assert sel[0]["symbol"] == "BTCUSDT"
    assert pnl == 2.5
    assert mdd == 0.0


def test_render_walk_forward_markdown_report_structure() -> None:
    """Verify markdown report generator renders all 5 required analytical sections."""
    t0 = datetime(2026, 9, 3, 0, 0, tzinfo=UTC)
    split = WindowSplit(
        split_id=1,
        name="Split 1 (IS: 09-03~09-10 | OOS: 09-10~09-13)",
        is_start=t0,
        is_end=t0 + timedelta(days=7),
        oos_start=t0 + timedelta(days=7),
        oos_end=t0 + timedelta(days=10),
    )

    p_dict = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.5,
        "min_imbalance": 0.3,
        "min_intensity": 4.0,
        "min_volume_ratio": 1.5,
        "cooldown_buckets": 0,
    }
    cand = ParameterCandidate.from_dict(p_dict)
    eval_item = CandidateEvaluation(
        candidate=cand,
        is_feasible=True,
        net_pnl=120.0,
        neighborhood_stability_score=0.75,
    )

    mock_result = SplitEvaluationResult(
        split=split,
        rec_candidate=cand,
        rec_is_eval=eval_item,
        rec_oos_pnl=35.5,
        rec_oos_mdd=8.2,
        rec_oos_trades=18,
        rec_oos_win_rate=72.2,
        rec_wfe=0.69,
        best_candidate=cand,
        best_is_eval=eval_item,
        best_oos_pnl=20.0,
        best_oos_mdd=15.0,
        best_oos_trades=25,
        best_wfe=0.35,
        p1_is_pnl=100.0,
        p1_oos_pnl=25.0,
        p2_is_pnl=140.0,
        p2_oos_pnl=15.0,
        oos_daily_breakdown=[18.0, 12.0, 5.5],
        decomposition={
            "data_extension_gain": 15.0,
            "reselection_gain": 20.5,
            "total_change": 35.5,
        },
    )

    md = render_walk_forward_markdown_report([mock_result], total_data_days=17)
    assert "# 17天全样本走步向前验证" in md
    assert "跨窗口走步向前总览表" in md
    assert "Alpha 衰减半衰期曲线分析" in md
    assert "稳定性状态机评估" in md
    assert "生产账户落地指导建议" in md
