from pathlib import Path

STATIC = Path("src/crypto_momentum_lab/operator_dashboard/static")


def test_static_index_contains_dashboard_mount() -> None:
    text = (STATIC / "index.html").read_text(encoding="utf-8")

    for section_id in (
        "overview",
        "universe",
        "strategy",
        "account",
        "risk",
        "reports",
    ):
        assert f'id="{section_id}"' in text


def test_static_javascript_uses_relative_api_paths() -> None:
    text = (STATIC / "dashboard.js").read_text(encoding="utf-8")
    index = (STATIC / "index.html").read_text(encoding="utf-8")

    assert 'data-endpoint="api/overview"' in index
    assert 'href="static/dashboard.css?v=20260802-paper-simple-details-state"' in index
    assert 'src="static/dashboard.js?v=20260802-paper-simple-details-state"' in index
    assert 'data-endpoint="/api/' not in index
    assert "fetch(section.dataset.endpoint" in text
    assert "binance.com" not in text.lower()


def test_degraded_status_labels_are_visible() -> None:
    text = (STATIC / "index.html").read_text(encoding="utf-8")

    for label in ("UNKNOWN", "STALE", "HALTED", "LIVE"):
        assert label in text


def test_strategy_panel_renders_portfolio_and_position_lifecycle() -> None:
    text = (STATIC / "dashboard.js").read_text(encoding="utf-8")

    for label in (
        "资产权益走势",
        "当前持仓",
        "已平仓交易",
        "开平仓流水",
    ):
        assert label in text
    for field in (
        "equity_curve",
        "equity_window_start",
        "equity_window_end",
        "equity_sample_interval_seconds",
        "open_positions",
        "closed_trades",
        "trade_events",
        "signalEvidence",
        "触发依据",
        "liquidation_notional",
        "shortHash",
    ):
        assert field in text


def test_strategy_panel_renders_pair_matched_equity_comparisons() -> None:
    text = (STATIC / "dashboard.js").read_text(encoding="utf-8")

    for marker in (
        "buildPairedEquityModels",
        "PAIR-MATCHED EQUITY",
        "COMMON START",
        "SHARED AXES",
        "ROLLING 24H",
        "CLOSED TRADES · LATEST 30",
        "wirePaperAccountTabs",
        "loadPaperAccountHistory",
        "查看全部历史",
    ):
        assert marker in text


def test_universe_panel_separates_target_and_retained_symbols() -> None:
    text = (STATIC / "dashboard.js").read_text(encoding="utf-8")

    for marker in (
        "目标池 · 涨幅 Top 20",
        "目标池 · 跌幅 Top 20",
        "保留与持仓保护",
        "RETAINED / POSITION PROTECTION",
        "监控池 ${monitored.length}",
    ):
        assert marker in text


def test_dashboard_polling_preserves_scroll_positions() -> None:
    text = (STATIC / "dashboard.js").read_text(encoding="utf-8")

    for marker in (
        "captureScrollState",
        "restoreScrollState",
        "window.scrollTo(state.pageX, state.pageY)",
        'querySelectorAll(".table-scroll")',
    ):
        assert marker in text


def test_dashboard_polling_preserves_open_strategy_signals() -> None:
    text = (STATIC / "dashboard.js").read_text(encoding="utf-8")

    capture_start = text.index("function captureScrollState")
    refresh_start = text.index("async function refreshSection")
    scroll_state_code = text[capture_start:refresh_start]

    assert 'querySelectorAll("details")' in scroll_state_code
    assert "details.open = saved.open" in scroll_state_code


def test_paper_account_cards_pair_same_strategy_vertically() -> None:
    text = (STATIC / "dashboard.css").read_text(encoding="utf-8")

    assert "grid-template-rows: repeat(2, auto)" in text
    assert "grid-auto-flow: column" in text
    assert "grid-template-rows: none" in text
    assert "grid-auto-flow: row" in text


def test_reports_panel_does_not_repeat_paper_accounts() -> None:
    text = (STATIC / "dashboard.js").read_text(encoding="utf-8")

    assert "纸面运行" not in text
    assert "PAPER RUNS" not in text
    assert "paper_runs" not in text
