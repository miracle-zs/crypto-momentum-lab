"""Unit test for local optimization dashboard and opt_groups loader."""

from __future__ import annotations

from pathlib import Path

from local_optimization.dashboard import (
    generate_three_track_data,
    load_optimization_groups,
    render_dashboard,
)

ROOT_DIR = Path(__file__).resolve().parent.parent.parent


def test_load_optimization_groups_new_run() -> None:
    opt_dir = ROOT_DIR / "local_optimization/data/optimization_all_collected_20260919"
    groups = load_optimization_groups(opt_dir)

    assert "group_280" in groups
    assert "group_unconstrained" in groups

    grp_280 = groups["group_280"]
    assert grp_280["daily_best"]["net_pnl"] == 356.39
    assert grp_280["daily_best"]["margin"] == 280.0
    assert grp_280["recommended"]["net_pnl"] == 353.43
    assert grp_280["recommended"]["mdd"] == 103.23
    assert grp_280["base_primary"]["net_pnl"] == 217.02
    assert grp_280["base_acc34"]["net_pnl"] == 321.73

    grp_uncon = groups["group_unconstrained"]
    assert grp_uncon["daily_best"]["net_pnl"] == 456.61
    assert grp_uncon["daily_best"]["margin"] == 500.0


def test_generate_three_track_data() -> None:
    opt_dir = ROOT_DIR / "local_optimization/data/optimization_all_collected_20260919"
    profile_csv = opt_dir / "profile_equity_series.csv"
    pts = generate_three_track_data([], profile_equity_csv=profile_csv, max_points=100)

    assert len(pts) > 0
    assert pts[0]["baseline"] == 1000.0
    assert pts[-1]["baseline"] == round(1000.0 - 29.75, 2)
    assert pts[-1]["daily_best"] == round(1000.0 + 356.39, 2)
    assert pts[-1]["recommended"] == round(1000.0 + 353.43, 2)


def test_render_dashboard_no_stale_values(tmp_path: Path) -> None:
    out_html = tmp_path / "dashboard_test.html"
    report_data = {
        "date": "2026-09-03 ~ 2026-09-19",
        "snapshot": "all_collected_test",
        "protocol": "margin280-free-cooldown",
        "n_candidates": 25200,
    }
    opt_dir = ROOT_DIR / "local_optimization/data/optimization_all_collected_20260919"

    render_dashboard(
        output_path=out_html,
        report_data=report_data,
        equity_series=[{"time": "2026-09-03 08:00", "equity": 1000.0}],
        opt_dir=opt_dir,
    )

    content = out_html.read_text(encoding="utf-8")
    assert "586.81" not in content
    assert "356.39" in content
    assert "353.43" in content
