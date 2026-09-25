"""Unit and integration tests for daily optimization pipeline orchestrator."""

from __future__ import annotations

import os
from pathlib import Path

from local_optimization.generate_six_scenarios_dashboard import (
    Candidate8D,
    format_8d_param_str,
)
from local_optimization.run_daily_local_optimization import (
    candidate8d_to_evaluation,
    format_executive_markdown,
)

ROOT_DIR = Path(__file__).resolve().parent.parent.parent


def test_sync_script_exists_and_executable() -> None:
    """Verify sync_daily_data.sh exists and has execute permissions."""
    sync_script = ROOT_DIR / "local_optimization/sync_daily_data.sh"
    assert sync_script.exists()
    assert os.access(sync_script, os.X_OK)


def test_candidate8d_to_evaluation_conversion() -> None:
    """Verify Candidate8D converts accurately into CandidateEvaluation."""
    cand = Candidate8D(
        params={
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "min_return_pct": 0.50,
            "min_imbalance": 0.30,
            "min_intensity": 3.0,
            "min_volume_ratio": 1.5,
            "cooldown_buckets": 0,
            "max_open_positions": 2,
        },
        net_pnl=229.38,
        mdd=39.40,
        calmar=4.93,
        compounding_score=0.1852,
        compounding_mdd=0.0394,
        compounding_ui=0.0125,
        terminal_compounded_equity=1229.38,
        n_trades=112,
        peak_margin=40.0,
        stability=0.857,
    )

    evaluation = candidate8d_to_evaluation(cand)
    assert evaluation.is_feasible is True
    assert evaluation.net_pnl == 229.38
    assert evaluation.trade_count == 112
    assert evaluation.peak_initial_margin_usdt == 40.0
    assert evaluation.neighborhood_stability_score == 0.857
    assert evaluation.candidate.parameter_id is not None
    assert len(evaluation.candidate.parameter_id) > 0


def test_format_executive_markdown() -> None:
    """Verify the executive markdown generator produces correct sections."""
    cand_s1 = Candidate8D(
        params={
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "min_return_pct": 0.5,
            "min_imbalance": 0.3,
            "min_intensity": 1.5,
            "min_volume_ratio": 0.0,
            "cooldown_buckets": 0,
            "max_open_positions": 4,
        },
        net_pnl=315.43,
        mdd=109.1,
        calmar=2.89,
        compounding_score=0.15,
        compounding_mdd=0.1091,
        compounding_ui=0.0371,
        terminal_compounded_equity=1315.43,
        n_trades=172,
        peak_margin=80.0,
        stability=0.714,
    )
    cand_s3 = Candidate8D(
        params={
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "min_return_pct": 0.5,
            "min_imbalance": 0.3,
            "min_intensity": 3.0,
            "min_volume_ratio": 1.5,
            "cooldown_buckets": 0,
            "max_open_positions": 2,
        },
        net_pnl=229.38,
        mdd=39.4,
        calmar=4.93,
        compounding_score=0.185,
        compounding_mdd=0.0394,
        compounding_ui=0.0125,
        terminal_compounded_equity=1229.38,
        n_trades=112,
        peak_margin=40.0,
        stability=0.857,
    )

    scenario_results = {
        "curves_meta": {
            "s_m280_pnl_max": {
                "title": "场景 1",
                "short_label": "S1: 280U/纯收益",
                "param_str": format_8d_param_str(cand_s1.params),
                "net_pnl": 315.43,
                "mdd_pct": 10.91,
                "calmar": 2.89,
                "ulcer_index": 0.0371,
                "stability": 0.714,
                "peak_margin": 80.0,
                "total_trades": 172,
                "is_baseline": False,
            },
            "s_m280_compounding": {
                "title": "场景 3",
                "short_label": "S3: 280U/复利导向",
                "param_str": format_8d_param_str(cand_s3.params),
                "net_pnl": 229.38,
                "mdd_pct": 3.94,
                "calmar": 4.93,
                "ulcer_index": 0.0125,
                "stability": 0.857,
                "peak_margin": 40.0,
                "total_trades": 112,
                "is_baseline": False,
            },
        }
    }

    reconciliation_data = {
        "layers": {
            "L6": {
                "equity_divergence_usdt": 0.08,
                "live_net_pnl_usdt": 288.66,
                "replay_net_pnl_usdt": 288.58,
            }
        },
        "first_causal_divergence": None,
    }

    md = format_executive_markdown(
        date_str="2026-09-20",
        reconciliation_data=reconciliation_data,
        scenario_results=scenario_results,
        stage_status="candidate",
        stage_notes=["Accumulating OOS evidence"],
        recommendation_cand=cand_s3,
        daily_best_cand=cand_s1,
        html_dashboard_path=Path("six_scenarios_equity_comparison.html"),
    )

    assert "每日盘后本地寻优与对账综合决策简报 (2026-09-20)" in md
    assert "在途持仓冻结机制" in md
    assert "229.38" in md
    assert "315.43" in md
    assert "0.08" in md


def test_all_in_one_dashboard_payload_and_render(tmp_path: Path) -> None:
    """Verify that All-in-One dashboard renders governance, recon, and opt data."""
    from local_optimization.generate_six_scenarios_dashboard import (
        render_dashboard_html,
    )

    gov_data = {
        "stage_status": "candidate",
        "stage_status_display": "🔵 候选就绪期 (CANDIDATE_READY)",
        "gates": [
            {
                "name": "连续盯市稳定性",
                "target": "≥ 70.0%",
                "current": "85.7%",
                "status": "PASS",
            },
            {
                "name": "样本外验证超额",
                "target": "> 0.00 USDT",
                "current": "+18.52 U",
                "status": "PASS",
            },
            {
                "name": "观察期达标天数",
                "target": "≥ 14 天",
                "current": "7 / 14 天",
                "status": "ACCUM",
            },
        ],
        "verdict_title": "⏸️ 保持当前实盘参数不变 (候选就绪观察中)",
        "verdict_desc": "已进入候选就绪期，继续监控样本外表现。",
        "recommended_params": "2 / 1 / 0.50% / 0.30 / 3.0 / 1.5x / cd=0 / slots=2",
    }
    recon_data = {
        "accounts": {
            "primary": {
                "account_id": "primary",
                "title": "Primary",
                "config_str": "cfg",
                "phase_offset": "00m",
                "live_final_pnl": 288.66,
                "replay_final_pnl": 288.58,
                "divergence_usdt": -0.08,
                "divergence_pct": 0.028,
                "mean_slippage_bps": 0.038,
                "status_label": "PASS",
                "series": [
                    {
                        "time": "09-20 12:00",
                        "live_equity": 1288.66,
                        "replay_equity": 1288.58,
                    }
                ],
            }
        }
    }
    curves_meta = {
        "s_m280_compounding": {
            "title": "S3",
            "short_label": "S3",
            "param_str": "test",
            "net_pnl": 229.38,
            "mdd_pct": 3.94,
            "calmar": 4.93,
            "ulcer_index": 0.0125,
            "stability": 0.857,
            "peak_margin": 40.0,
            "total_trades": 112,
            "is_baseline": False,
        }
    }
    timeline = [
        {
            "time": "2026-09-20 12:00",
            "s_m280_compounding": {
                "equity": 1229.38,
                "drawdown": 0.0,
                "margin": 40.0,
                "active_pos": 1,
            },
        }
    ]

    out_file = tmp_path / "all_in_one_test.html"
    render_dashboard_html(
        curves_meta=curves_meta,
        timeline_series=timeline,
        governance_data=gov_data,
        reconciliation_data=recon_data,
        output_html=out_file,
    )

    assert out_file.exists()
    content = out_file.read_text(encoding="utf-8")
    assert "CANDIDATE_READY" in content
    assert "连续盯市稳定性" in content
    assert "288.66" in content
    assert "229.38" in content
