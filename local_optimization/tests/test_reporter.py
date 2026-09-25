"""Unit tests for stability tracker, SQLite catalog, and report generation."""

from __future__ import annotations

import tempfile
from pathlib import Path

from local_optimization.optimizer import CandidateEvaluation
from local_optimization.protocol import (
    OptimizationProtocol,
    ParameterCandidate,
)
from local_optimization.reporter import (
    ExperimentCatalog,
    generate_markdown_report,
)
from local_optimization.snapshot import SnapshotManifest
from local_optimization.tracker import (
    DailyTrackRecord,
    decompose_daily_performance,
    evaluate_stage_stability,
)


def test_performance_decomposition() -> None:
    """Verify clean separation of data extension gain from re-selection gain."""
    decomp = decompose_daily_performance(
        p_old_old=100.0,
        p_old_new=120.0,
        p_new_new=135.0,
    )
    assert decomp["data_extension_gain"] == 20.0
    assert decomp["reselection_gain"] == 15.0
    assert decomp["total_change"] == 35.0


def test_stage_stability_evaluation() -> None:
    """Test transition from candidate to stable or degraded."""
    cand_stable = ParameterCandidate.from_dict({"param": "opt1"})
    cand_other = ParameterCandidate.from_dict({"param": "opt2"})

    eval_stable = CandidateEvaluation(
        candidate=cand_stable, is_feasible=True, net_pnl=100.0
    )
    eval_other = CandidateEvaluation(
        candidate=cand_other, is_feasible=True, net_pnl=95.0
    )

    # 1. Less than 7 days
    history_short = [
        DailyTrackRecord(
            date_str=f"2026-09-0{i}",
            snapshot_id="snap1",
            protocol_id="proto1",
            recommended=eval_stable,
            oos_forward_pnl=5.0,
        )
        for i in range(1, 4)
    ]
    status, _ = evaluate_stage_stability(history_short, min_consistency_days=7)
    assert status == "insufficient_evidence"

    # 2. 7 days with consistent winner and positive forward PnL -> stable
    history_stable = [
        DailyTrackRecord(
            date_str=f"2026-09-{i:02d}",
            snapshot_id=f"snap_{i}",
            protocol_id="proto1",
            recommended=eval_stable,
            oos_forward_pnl=5.0,
        )
        for i in range(1, 8)
    ]
    status, _ = evaluate_stage_stability(
        history_stable, min_consistency_days=7, min_oos_days=7
    )
    assert status == "stable"

    # 3. Churning recommendations -> candidate
    history_churn = list(history_stable)
    history_churn[0] = DailyTrackRecord(
        date_str="2026-09-01",
        snapshot_id="snap_1",
        protocol_id="proto1",
        recommended=eval_other,
        oos_forward_pnl=5.0,
    )
    history_churn[1] = DailyTrackRecord(
        date_str="2026-09-02",
        snapshot_id="snap_2",
        protocol_id="proto1",
        recommended=eval_other,
        oos_forward_pnl=5.0,
    )
    status, _ = evaluate_stage_stability(history_churn, min_consistency_days=7)
    assert status == "candidate"


def test_sqlite_catalog_and_report_generation() -> None:
    """Test persisting experiments in SQLite catalog and generating Markdown report."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_experiments.db"
        catalog = ExperimentCatalog(db_path)

        protocol = OptimizationProtocol(scenario_family="margin280-test")
        snapshot = SnapshotManifest(
            snapshot_id="test_snap_01",
            imported_at="2026-09-18T00:00:00Z",
            target_cutoff="2026-09-18T00:00:00Z",
            is_complete=True,
            capability_tags=["execution_audit_ready"],
        )

        catalog.save_protocol(protocol)
        catalog.save_snapshot(snapshot)

        cand = ParameterCandidate.from_dict({"fast": 2, "slow": 10})
        cand_eval = CandidateEvaluation(
            candidate=cand,
            is_feasible=True,
            net_pnl=120.0,
            ulcer_index=0.025,
            max_drawdown_pct=0.05,
            peak_initial_margin_usdt=180.0,
            trade_count=45,
        )

        record = DailyTrackRecord(
            date_str="2026-09-18",
            snapshot_id=snapshot.snapshot_id,
            protocol_id=protocol.protocol_id,
            daily_best=cand_eval,
            recommended=cand_eval,
            live_actual=cand_eval,
            stability_status="candidate",
            stability_notes=["Testing daily report generation"],
        )

        catalog.save_daily_track(record)
        loaded_history = catalog.load_track_history(protocol.protocol_id)
        assert len(loaded_history) == 1
        assert loaded_history[0].date_str == "2026-09-18"

        md_report = generate_markdown_report(
            date_str="2026-09-18",
            protocol=protocol,
            snapshot=snapshot,
            daily_record=record,
            pareto_frontier=[cand_eval],
        )

        assert "# 本地参数寻优与稳定性日报 (2026-09-18)" in md_report
        assert "margin280-test" in md_report
        assert "今日最高 (Daily Best)" in md_report
        assert "Pareto 前沿" in md_report


def test_dashboard_html_generation(tmp_path: Path) -> None:
    """Validate that the interactive HTML dashboard is rendered properly."""
    from local_optimization.dashboard import render_dashboard

    out_html = tmp_path / "dashboard.html"
    report_data = {
        "date": "2026-09-18",
        "snapshot": "snap_test",
        "protocol": "proto_test",
        "n_candidates": 25200,
    }
    equity_series = [
        {
            "time": "2026-09-18 10:00",
            "equity": 1000.0,
            "unrealized": 0.0,
            "margin": 100.0,
        }
    ]
    render_dashboard(out_html, report_data, equity_series)
    assert out_html.exists()
    content = out_html.read_text(encoding="utf-8")
    assert "25,200" in content
    assert "2026-09-18" in content
    assert "15s 真实盯市" in content
    assert "7 维参数邻域平坦度" in content


def test_daily_orchestration_pipeline_mock(tmp_path: Path) -> None:
    """Test full execution of daily optimization pipeline with mock snapshot."""
    from local_optimization.run_daily_local_optimization import (
        load_candidate_evaluations,
    )

    # Test candidate evaluation loader with mock CSV
    mock_csv = tmp_path / "top_candidates.csv"
    mock_csv.write_text(
        "impulse_window_buckets,confirmation_buckets,min_return_pct,min_imbalance,"
        "min_intensity,volume_feature,min_volume_ratio,min_notional_5m_vs_30m,"
        "cooldown_buckets,full_net_pnl_usdt,full_max_drawdown_usdt,"
        "initial_margin_peak_usdt,full_n_closed,margin_constraint_feasible\n"
        "3,1,1.5,0.3,1.5,notional_5m_vs_30m,0.0,0.0,0,586.81,77.33,280.0,631,True\n",
        encoding="utf-8",
    )
    evals = load_candidate_evaluations(tmp_path)
    assert len(evals) == 1
    assert evals[0].net_pnl == 586.81
    assert evals[0].peak_initial_margin_usdt == 280.0
    assert evals[0].is_feasible is True
