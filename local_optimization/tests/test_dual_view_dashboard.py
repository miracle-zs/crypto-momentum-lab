"""Unit tests for dual-view (Top 10 vs All-Market) dashboard and filtering."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from local_optimization.generate_six_scenarios_dashboard import (
    Candidate8D,
    build_governance_data_for_scenarios,
    render_dashboard_html,
)
from local_optimization.opportunity import (
    RawOpportunity,
    filter_opportunities_by_top10,
    load_top10_lookup,
)


def test_filter_opportunities_by_top10() -> None:
    """Verify filter_opportunities_by_top10 accurately filters by 15m bucket."""
    t0 = datetime(2026, 9, 20, 10, 0, tzinfo=UTC)
    t1 = t0 + timedelta(minutes=14)  # same 15m bucket (10:00)
    t2 = t0 + timedelta(minutes=16)  # next 15m bucket (10:15)

    opp1 = RawOpportunity(
        opportunity_id="opp1",
        symbol="BTCUSDT",
        direction="LONG",
        detected_at=t0,
        detected_epoch=t0.timestamp(),
        entry_eligible_at=t0,
        entry_reference_price=100.0,
        impulse_window_buckets=2,
        confirmation_buckets=1,
        impulse_return_pct=0.8,
        aggressive_imbalance=0.4,
        confirmation_min_imbalance=0.4,
        notional_intensity=3.0,
        volume_ratio=1.5,
    )
    opp2 = RawOpportunity(
        opportunity_id="opp2",
        symbol="ETHUSDT",
        direction="LONG",
        detected_at=t1,
        detected_epoch=t1.timestamp(),
        entry_eligible_at=t1,
        entry_reference_price=50.0,
        impulse_window_buckets=2,
        confirmation_buckets=1,
        impulse_return_pct=0.8,
        aggressive_imbalance=0.4,
        confirmation_min_imbalance=0.4,
        notional_intensity=3.0,
        volume_ratio=1.5,
    )
    opp3 = RawOpportunity(
        opportunity_id="opp3",
        symbol="SOLUSDT",
        direction="LONG",
        detected_at=t2,
        detected_epoch=t2.timestamp(),
        entry_eligible_at=t2,
        entry_reference_price=20.0,
        impulse_window_buckets=2,
        confirmation_buckets=1,
        impulse_return_pct=0.8,
        aggressive_imbalance=0.4,
        confirmation_min_imbalance=0.4,
        notional_intensity=3.0,
        volume_ratio=1.5,
    )

    # Lookup: at 10:00 BTCUSDT is in Top 10, ETH is not.
    # At 10:15 SOLUSDT is in Top 10.
    bucket_1000_str = t0.strftime("%Y-%m-%d %H:%M:%S+00:00")
    bucket_1015_str = (t0 + timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M:%S+00:00")
    lookup = {
        ("BTCUSDT", bucket_1000_str),
        ("SOLUSDT", bucket_1015_str),
    }

    filtered = filter_opportunities_by_top10([opp1, opp2, opp3], lookup)
    assert len(filtered) == 2
    ids = {op.opportunity_id for op in filtered}
    assert ids == {"opp1", "opp3"}


def test_filter_opportunities_dict_fallback() -> None:
    """Verify dict-based events can also be filtered by top10 lookup."""
    t0 = datetime(2026, 9, 20, 10, 5, tzinfo=UTC)
    dict_opp = {
        "opportunity_id": "d1",
        "symbol": "DOGEUSDT",
        "detected_at": t0,
    }
    bucket_1000_str = datetime(2026, 9, 20, 10, 0, tzinfo=UTC).strftime(
        "%Y-%m-%d %H:%M:%S+00:00"
    )
    lookup = {("DOGEUSDT", bucket_1000_str)}

    res = filter_opportunities_by_top10([dict_opp], lookup)
    assert len(res) == 1
    assert res[0]["opportunity_id"] == "d1"

    # With non-matching lookup, should return empty list
    mismatch_lookup = {("OTHERCOIN", bucket_1000_str)}
    empty_res = filter_opportunities_by_top10([dict_opp], mismatch_lookup)
    assert len(empty_res) == 0


def test_load_top10_lookup_caching(tmp_path: Path) -> None:
    """Verify load_top10_lookup saves to and loads from cache path."""
    cache_file = tmp_path / "cache_top10.pkl"
    # Non-existent parquet dir with non-existent cache should return empty set
    empty_res = load_top10_lookup(tmp_path / "no_such_parquet", cache_path=cache_file)
    assert empty_res == set()

    # If cache exists with data, it should load directly from cache
    import pickle

    dummy_data = {(datetime(2026, 9, 20, 10, 0, tzinfo=UTC), "TESTUSDT")}
    with cache_file.open("wb") as f:
        pickle.dump(dummy_data, f)

    loaded = load_top10_lookup(tmp_path / "no_such_parquet", cache_path=cache_file)
    assert loaded == dummy_data


def test_render_dashboard_html_dual_views(tmp_path: Path) -> None:
    """Verify render_dashboard_html embeds dual views and Master View Switcher."""
    out_html = tmp_path / "dashboard.html"

    dummy_curves = {
        "s_m280_compounding": {
            "title": "场景 3: ≤280U保证金 · 复利导向走势",
            "short_label": "S3: 280U/复利导向",
            "objective_desc": "复利导向",
            "param_str": "2/1/0.75%/0.30/3.0/1.25x/cd=0/slots=2",
            "color": "#10b981",
            "is_baseline": False,
            "final_equity": 1288.66,
            "net_pnl": 288.66,
            "mdd_pct": 3.94,
            "mdd_usdt": 39.4,
            "calmar": 4.93,
            "ulcer_index": 0.0125,
            "peak_margin": 40.0,
            "total_trades": 19,
            "win_rate": 73.7,
            "stability": 0.857,
        }
    }
    dummy_timeline = [
        {
            "time": "2026-09-20 10:00:00",
            "short_time": "09-20 10:00",
            "timestamp": 1790000000,
            "s_m280_compounding": {
                "equity": 1000.0,
                "drawdown": 0.0,
                "margin": 20.0,
                "active_pos": 1,
            },
        }
    ]
    gov_top10 = {
        "stage_status": "provisional",
        "stage_status_display": "🟡 暂行观察期 (PROVISIONAL)",
        "view_label": "top10",
        "gates": [
            {
                "name": "8D 拓扑稳定性 (生产 Top 10 受限)",
                "target": "≥ 70.0%",
                "current": "85.7%",
                "status": "PASS",
            },
        ],
        "verdict_title": "⏸️ 保持当前实盘金牌参数不变",
        "verdict_desc": "处于暂行观察期",
        "recommended_params": "2/1/0.75%/0.30/3.0/1.25x/cd=0/slots=2",
    }
    recon_top10 = {
        "accounts": {
            "primary": {
                "title": "Primary (实盘主账户)",
                "config_str": "2/1/0.75%/0.30/3.0/1.25x/cd=0/slots=2",
                "live_final_pnl": 288.66,
                "replay_final_pnl": 288.58,
                "divergence_usdt": -0.08,
                "divergence_pct": 0.028,
                "mean_slippage_bps": 0.038,
                "status_label": "✅ 因果保真放行 (PASS)",
                "series": [],
            }
        }
    }

    views = {
        "top10": {
            "view_id": "top10",
            "title": "生产 Top 10 受限寻优 (实盘镜像)",
            "short_title": "Top 10 实盘镜像",
            "badge": "生产实盘镜像 (positive_gainer_top10)",
            "desc": "严格约束仅在动态涨幅榜 Top 10 准入门禁内开仓",
            "opp_count": 28359,
            "optimization": {
                "curves": dummy_curves,
                "timeline": dummy_timeline,
            },
            "reconciliation": recon_top10,
            "governance": gov_top10,
        },
        "top20": {
            "view_id": "top20",
            "title": "涨幅榜 Top 20 寻优 (温和扩容)",
            "short_title": "Top 20 温和扩容",
            "badge": "涨幅榜 Top 20 适度扩展 (gainer_rank <= 20)",
            "desc": "将准入门禁适度放宽至动态涨幅榜 Top 20",
            "opp_count": 48951,
            "optimization": {
                "curves": dummy_curves,
                "timeline": dummy_timeline,
            },
            "reconciliation": recon_top10,
            "governance": gov_top10,
        },
        "top30": {
            "view_id": "top30",
            "title": "涨幅榜 Top 30 寻优 (采集全域)",
            "short_title": "Top 30 扩展池",
            "badge": "涨幅榜 Top 30 扩展全域 (gainer_rank <= 30)",
            "desc": "放宽准入门禁至动态涨幅榜 Top 30",
            "opp_count": 56180,
            "optimization": {
                "curves": dummy_curves,
                "timeline": dummy_timeline,
            },
            "reconciliation": recon_top10,
            "governance": gov_top10,
        },
        "all_market": {
            "view_id": "all_market",
            "title": "全市场全量寻优 (397 币种)",
            "short_title": "全市场全量 (397 币种)",
            "badge": "全市场全容量参数探索 (397 币种)",
            "desc": "跨 397 币种全空间探索参数的全局统计显著性",
            "opp_count": 56829,
            "optimization": {
                "curves": dummy_curves,
                "timeline": dummy_timeline,
            },
            "reconciliation": recon_top10,
            "governance": gov_top10,
        },
    }

    render_dashboard_html(
        curves_meta=dummy_curves,
        timeline_series=dummy_timeline,
        governance_data=gov_top10,
        reconciliation_data=recon_top10,
        output_html=out_html,
        version_tag="test_dual_v1",
        views=views,
        default_view="top10",
    )

    assert out_html.exists()
    content = out_html.read_text(encoding="utf-8")

    # Verify Master View Switcher HTML elements exist
    assert "master-view-switcher" in content
    assert "view-tab-top10" in content
    assert "view-tab-top20" in content
    assert "view-tab-top30" in content
    assert "switchGlobalView('top10')" in content
    assert "switchGlobalView('top20')" in content
    assert "switchGlobalView('top30')" in content
    assert "renderScorecardAndLegend" in content

    # Verify injected JSON contains views
    assert '"views": {' in content
    assert '"top10": {' in content
    assert '"top20": {' in content
    assert '"top30": {' in content
    assert '"all_market": {' in content
    assert '"opp_count": 28359' in content
    assert '"opp_count": 48951' in content
    assert '"opp_count": 56180' in content
    assert '"opp_count": 56829' in content


def test_build_governance_data_for_scenarios() -> None:
    """Verify build_governance_data_for_scenarios adapts to view labels."""
    cand = Candidate8D(
        params={
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "min_return_pct": 0.75,
            "min_imbalance": 0.3,
            "min_intensity": 3.0,
            "min_volume_ratio": 1.25,
            "cooldown_buckets": 0,
            "max_open_positions": 2,
        },
        net_pnl=200.0,
        mdd=20.0,
        calmar=10.0,
        compounding_score=0.2,
        compounding_mdd=0.04,
        compounding_ui=0.01,
        terminal_compounded_equity=1200.0,
        n_trades=19,
        peak_margin=40.0,
        stability=0.857,
    )
    scenarios = {"m280_compounding": cand}

    gov_top10 = build_governance_data_for_scenarios(scenarios, view_label="top10")
    assert gov_top10["view_label"] == "top10"
    assert "生产 Top 10 受限" in gov_top10["gates"][0]["name"]
    assert gov_top10["gates"][0]["status"] == "PASS"

    gov_top20 = build_governance_data_for_scenarios(scenarios, view_label="top20")
    assert gov_top20["view_label"] == "top20"
    assert "涨幅榜 Top 20" in gov_top20["gates"][0]["name"]

    gov_top30 = build_governance_data_for_scenarios(scenarios, view_label="top30")
    assert gov_top30["view_label"] == "top30"
    assert "涨幅榜 Top 30" in gov_top30["gates"][0]["name"]

    gov_all = build_governance_data_for_scenarios(scenarios, view_label="all_market")
    assert gov_all["view_label"] == "all_market"
    assert "全市场全量" in gov_all["gates"][0]["name"]
