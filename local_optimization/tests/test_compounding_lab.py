from datetime import UTC, datetime, timedelta

from local_optimization.generate_six_scenarios_dashboard import (
    DEFAULT_GOLD_PROFILE,
    evaluate_compounding_comparison,
    render_dashboard_html,
)
from local_optimization.tests.test_opportunity_and_wfa_repair import (
    make_mock_opportunity,
)


def test_evaluate_compounding_comparison_mock_trades() -> None:
    """Verify that evaluate_compounding_comparison computes all 4 modes properly."""
    t0 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    t1 = t0 + timedelta(hours=2)
    t2 = datetime(2026, 9, 2, 2, 0, tzinfo=UTC)
    t3 = t2 + timedelta(hours=2)

    opp1 = make_mock_opportunity(
        opp_id="opp_1",
        symbol="BTCUSDT",
        detected_at=t0 - timedelta(seconds=15),
        entry_at=t0,
        entry_price=100.0,
        exit_at=t1,
        exit_price=105.0,
        net_pnl=5.0,
        w=2,
        c=1,
        r=0.8,
        imb=0.4,
        inten=3.5,
        vol=1.5,
    )
    opp2 = make_mock_opportunity(
        opp_id="opp_2",
        symbol="ETHUSDT",
        detected_at=t0 + timedelta(minutes=10),
        entry_at=t0 + timedelta(minutes=15),
        entry_price=50.0,
        exit_at=t1 + timedelta(minutes=30),
        exit_price=49.0,
        net_pnl=-2.0,
        w=2,
        c=1,
        r=0.8,
        imb=0.4,
        inten=3.5,
        vol=1.5,
    )
    opp3 = make_mock_opportunity(
        opp_id="opp_3",
        symbol="SOLUSDT",
        detected_at=t2 - timedelta(seconds=15),
        entry_at=t2,
        entry_price=20.0,
        exit_at=t3,
        exit_price=22.0,
        net_pnl=10.0,
        w=2,
        c=1,
        r=0.8,
        imb=0.4,
        inten=3.5,
        vol=1.5,
    )

    events = [opp1, opp2, opp3]

    result = evaluate_compounding_comparison(
        events=events,
        prices_by_symbol={},
        params=DEFAULT_GOLD_PROFILE,
        manifest={},
        scheduled_risk_window=None,
        initial_equity=1000.0,
        f=0.10,
        leverage=1.25,
    )

    assert "modes" in result
    modes = result["modes"]
    assert "mode1_simple" in modes
    assert "mode2_daily" in modes
    assert "mode3_per_trade" in modes
    assert "mode4_flat_reset" in modes
    assert "mode5_signal3_exit" in modes

    m1 = modes["mode1_simple"]
    assert m1["notional_stats"]["min"] == 100.0
    assert m1["notional_stats"]["max"] == 100.0
    assert m1["total_trades"] == 3

    timeline = result["timeline"]
    assert len(timeline) > 0
    first_pt = timeline[0]
    for k in ["mode1_simple", "mode2_daily", "mode3_per_trade", "mode4_flat_reset", "mode5_signal3_exit"]:
        assert k in first_pt
        assert "equity" in first_pt[k]
        assert "drawdown" in first_pt[k]
        assert "margin" in first_pt[k]
        assert "notional" in first_pt[k]
        assert "active_pos" in first_pt[k]

    assert result["cycle_resets"] >= 1
    assert result["total_trades"] == 3


def test_dashboard_template_renders_compounding_lab(tmp_path) -> None:
    """Verify that render_dashboard_html injects compounding_lab data."""
    sample_comp_lab = {
        "param_str": "2/1/0.75%/0.30/3.0/1.25x/cd=0/slots=2",
        "params": DEFAULT_GOLD_PROFILE,
        "initial_equity": 1000.0,
        "f": 0.10,
        "leverage": 1.25,
        "cycle_resets": 96,
        "total_trades": 573,
        "modes": {
            "mode1_simple": {
                "key": "mode1_simple",
                "label": "1. 单利基准 (固定名义价值 100U)",
                "short_label": "单利基准 (100U)",
                "color": "#3b82f6",
                "desc": "每笔固定名义价值 100 USDT",
                "final_equity": 1401.06,
                "net_pnl": 401.06,
                "return_pct": 40.11,
                "mdd_pct": 8.87,
                "calmar": 4.52,
                "ulcer_index": 0.0331,
                "peak_margin": 160.0,
                "leverage_util": 0.16,
                "total_trades": 573,
                "win_rate": 61.4,
                "notional_stats": {
                    "min": 100.0,
                    "max": 100.0,
                    "mean": 100.0,
                    "end": 100.0,
                },
            },
            "mode2_daily": {
                "key": "mode2_daily",
                "label": "2. 每日复利 (每日根据总权益 10% 调整)",
                "short_label": "每日复利 (日10%)",
                "color": "#10b981",
                "desc": "每日 00:00 按当下账户 MTM 总权益重新核算",
                "final_equity": 1471.89,
                "net_pnl": 471.89,
                "return_pct": 47.19,
                "mdd_pct": 12.43,
                "calmar": 3.80,
                "ulcer_index": 0.0445,
                "peak_margin": 236.4,
                "leverage_util": 0.24,
                "total_trades": 573,
                "win_rate": 61.4,
                "notional_stats": {
                    "min": 100.0,
                    "max": 153.92,
                    "mean": 125.11,
                    "end": 147.00,
                },
            },
            "mode3_per_trade": {
                "key": "mode3_per_trade",
                "label": "3. 逐笔实时复利 (每笔买入按当时总权益 10%)",
                "short_label": "逐笔实时复利 (实时10%)",
                "color": "#f59e0b",
                "desc": "每笔买入时按当时总权益设定",
                "final_equity": 1467.99,
                "net_pnl": 467.99,
                "return_pct": 46.80,
                "mdd_pct": 12.47,
                "calmar": 3.75,
                "ulcer_index": 0.0450,
                "peak_margin": 237.94,
                "leverage_util": 0.24,
                "total_trades": 573,
                "win_rate": 61.4,
                "notional_stats": {
                    "min": 99.13,
                    "max": 155.40,
                    "mean": 126.60,
                    "end": 147.82,
                },
            },
            "mode4_flat_reset": {
                "key": "mode4_flat_reset",
                "label": "4. 归零重置复利 (持仓为0时锁定总权益 10%)",
                "short_label": "归零重置复利 (空仓10%)",
                "color": "#8b5cf6",
                "desc": "持仓为0时锁定总权益",
                "final_equity": 1475.81,
                "net_pnl": 475.81,
                "return_pct": 47.58,
                "mdd_pct": 12.49,
                "calmar": 3.81,
                "ulcer_index": 0.0449,
                "peak_margin": 237.89,
                "leverage_util": 0.24,
                "total_trades": 573,
                "win_rate": 61.4,
                "notional_stats": {
                    "min": 100.0,
                    "max": 155.79,
                    "mean": 126.46,
                    "end": 147.70,
                },
            },
            "mode5_signal3_exit": {
                "key": "mode5_signal3_exit",
                "label": "5. 3次截断平仓单利 (固定100U)",
                "short_label": "3次截断 (100U)",
                "color": "#ec4899",
                "desc": "第3次买入信号触发持仓平仓并锁利",
                "final_equity": 1405.00,
                "net_pnl": 405.00,
                "return_pct": 40.50,
                "mdd_pct": 8.50,
                "calmar": 4.76,
                "ulcer_index": 0.0320,
                "peak_margin": 160.0,
                "leverage_util": 0.16,
                "total_trades": 573,
                "win_rate": 61.8,
                "notional_stats": {
                    "min": 100.0,
                    "max": 100.0,
                    "mean": 100.0,
                    "end": 100.0,
                },
            },
        },
        "timeline": [
            {
                "time": "2026-09-01 00:00:00",
                "short_time": "09-01 00:00",
                "timestamp": 1788220800,
                "mode1_simple": {
                    "equity": 1000.0,
                    "drawdown": 0.0,
                    "margin": 0.0,
                    "notional": 100.0,
                    "active_pos": 0,
                },
                "mode2_daily": {
                    "equity": 1000.0,
                    "drawdown": 0.0,
                    "margin": 0.0,
                    "notional": 100.0,
                    "active_pos": 0,
                },
                "mode3_per_trade": {
                    "equity": 1000.0,
                    "drawdown": 0.0,
                    "margin": 0.0,
                    "notional": 100.0,
                    "active_pos": 0,
                },
                "mode4_flat_reset": {
                    "equity": 1000.0,
                    "drawdown": 0.0,
                    "margin": 0.0,
                    "notional": 100.0,
                    "active_pos": 0,
                },
                "mode5_signal3_exit": {
                    "equity": 1000.0,
                    "drawdown": 0.0,
                    "margin": 0.0,
                    "notional": 100.0,
                    "active_pos": 0,
                },
            }
        ],
    }

    out_file = tmp_path / "dashboard.html"
    render_dashboard_html(
        curves_meta={},
        timeline_series=[],
        governance_data={
            "stage_status": "stable",
            "stage_status_display": "🟢 准入放行 (PASS)",
        },
        reconciliation_data={"status": "PASS", "accounts": {}},
        output_html=out_file,
        compounding_lab=sample_comp_lab,
    )

    html_text = out_file.read_text(encoding="utf-8")
    assert "nav-tab-optimization" in html_text
    assert "nav-tab-compounding" in html_text
    assert "main-tab-optimization-content" in html_text
    assert "main-tab-compounding-content" in html_text
    assert "switchMainTab" in html_text
    assert "compoundingChart" in html_text
    assert "comp-scorecard-body" in html_text
    assert "mode1_simple" in html_text
    assert "mode4_flat_reset" in html_text
    assert "mode5_signal3_exit" in html_text
    assert "compLabData" in html_text


def test_mode5_signal3_early_exit() -> None:
    """Verify that close_on_third_signal flattens positions and suppresses 4th/5th signals."""
    from local_optimization.simulation_ledger import SimulationLedger

    t0 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    opp1 = make_mock_opportunity(
        opp_id="opp_1",
        symbol="BTCUSDT",
        detected_at=t0 - timedelta(seconds=15),
        entry_at=t0,
        entry_price=100.0,
        exit_at=t0 + timedelta(hours=2),
        exit_price=105.0,
        net_pnl=5.0,
        w=2,
        c=1,
        r=0.8,
        imb=0.4,
        inten=3.5,
        vol=1.5,
    )
    opp2 = make_mock_opportunity(
        opp_id="opp_2",
        symbol="BTCUSDT",
        detected_at=t0 + timedelta(minutes=10) - timedelta(seconds=15),
        entry_at=t0 + timedelta(minutes=10),
        entry_price=102.0,
        exit_at=t0 + timedelta(hours=2, minutes=30),
        exit_price=106.0,
        net_pnl=4.0,
        w=2,
        c=1,
        r=0.8,
        imb=0.4,
        inten=3.5,
        vol=1.5,
    )
    # Signal 3 at 20 min - should trigger early close of opp1 & opp2!
    opp3 = make_mock_opportunity(
        opp_id="opp_3",
        symbol="BTCUSDT",
        detected_at=t0 + timedelta(minutes=20) - timedelta(seconds=15),
        entry_at=t0 + timedelta(minutes=20),
        entry_price=108.0,
        exit_at=t0 + timedelta(hours=3),
        exit_price=110.0,
        net_pnl=2.0,
        w=2,
        c=1,
        r=0.8,
        imb=0.4,
        inten=3.5,
        vol=1.5,
    )
    # Signal 4 at 30 min - should be suppressed (not entered)!
    opp4 = make_mock_opportunity(
        opp_id="opp_4",
        symbol="BTCUSDT",
        detected_at=t0 + timedelta(minutes=30) - timedelta(seconds=15),
        entry_at=t0 + timedelta(minutes=30),
        entry_price=107.0,
        exit_at=t0 + timedelta(hours=3),
        exit_price=109.0,
        net_pnl=2.0,
        w=2,
        c=1,
        r=0.8,
        imb=0.4,
        inten=3.5,
        vol=1.5,
    )

    ledger = SimulationLedger(initial_cash=1000.0, notional_usdt=100.0, leverage=1.25)
    res, _ = ledger.simulate_window(
        opportunities=[opp1, opp2, opp3, opp4],
        params=DEFAULT_GOLD_PROFILE,
        window_start=t0 - timedelta(hours=1),
        window_end=t0 + timedelta(hours=5),
        max_concurrency=2,
        fast_eval=True,
        close_on_third_signal=True,
    )

    # Exactly 2 trades admitted (opp1 and opp2)
    assert len(res.admitted_trades) == 2
    tr1, tr2 = res.admitted_trades

    # Both trades must have exit_time equal to opp3's entry time (t0 + 20 min)
    assert tr1.exit_time == opp3.entry_eligible_at
    assert tr2.exit_time == opp3.entry_eligible_at

    # Both trades must have exit_price equal to opp3's reference price (108.0)
    assert tr1.exit_price == 108.0
    assert tr2.exit_price == 108.0

