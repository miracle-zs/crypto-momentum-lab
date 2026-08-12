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
    assert 'href="static/dashboard.css?v=20260812-exit-series"' in index
    assert 'src="static/dashboard.js?v=20260812-exit-series"' in index
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
        "buildStrategyEquityModels",
        "strategyEquityChart",
        "STRATEGY EXIT EQUITY",
        "COMMON START",
        "SHARED AXES",
        "ROLLING 24H",
        "CLOSED TRADES · LATEST 30",
        "wirePaperAccountTabs",
        "loadPaperAccountHistory",
        "查看全部历史",
        "paper-accounts/equity",
        "paperDetailsByRun",
    ):
        assert marker in text
    assert "SUMMARY FIRST · DETAIL ON DEMAND" in (
        STATIC / "index.html"
    ).read_text(encoding="utf-8")


def test_account_cards_use_each_account_equity_curve() -> None:
    text = (STATIC / "dashboard.js").read_text(encoding="utf-8")
    card_start = text.index("function accountCard")
    card_end = text.index("function accountDetail", card_start)
    card_code = text[card_start:card_end]

    assert "function accountWindowDelta(account)" in text
    assert "standaloneSparkline(account.equity_curve)" in card_code
    assert "滚动 24H 权益变化" in card_code
    assert "comparisonSparkline" not in card_code


def test_paper_accounts_group_by_strategy_columns() -> None:
    javascript = (STATIC / "dashboard.js").read_text(encoding="utf-8")
    stylesheet = (STATIC / "dashboard.css").read_text(encoding="utf-8")

    for marker in (
        "strategyAccountColumn",
        "acct-strategy-column",
        "acct-strategy-cards",
    ):
        assert marker in javascript
        assert marker in stylesheet or marker == "strategyAccountColumn"
    assert "每个策略一张图 · 多退出方式叠加" in javascript
    assert "grid-auto-flow: column" not in stylesheet


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


def test_paper_detail_replacement_preserves_interaction_state() -> None:
    text = (STATIC / "dashboard.js").read_text(encoding="utf-8")

    replacement_start = text.index("function replacePaperDetail")
    replacement_end = text.index("function mountPaperDetail", replacement_start)
    replacement_code = text[replacement_start:replacement_end]

    capture = replacement_code.index("captureScrollState(body)")
    replace = replacement_code.index("detail.outerHTML")
    restore = replacement_code.index("restoreScrollState(body, scrollState)")
    assert capture < replace < restore

    mount_start = text.index("function mountPaperDetail")
    mount_end = text.index("function wirePaperDetailButton", mount_start)
    assert "replacePaperDetail(body," in text[mount_start:mount_end]

    history_start = text.index("async function loadPaperAccountHistory")
    history_end = text.index("function captureScrollState", history_start)
    assert "replacePaperDetail(body," in text[history_start:history_end]


def test_scroll_state_restores_layout_before_page_position() -> None:
    text = (STATIC / "dashboard.js").read_text(encoding="utf-8")

    restore_start = text.index("function restoreScrollState")
    restore_end = text.index("async function refreshSection", restore_start)
    restore_code = text[restore_start:restore_end]

    disclosures = restore_code.index('querySelectorAll("details")')
    containers = restore_code.index('querySelectorAll(".table-scroll")')
    page = restore_code.index("window.scrollTo(state.pageX, state.pageY)")
    assert disclosures < containers < page


def test_dashboard_formats_display_times_in_fixed_utc_plus_8() -> None:
    text = (STATIC / "dashboard.js").read_text(encoding="utf-8")
    index = (STATIC / "index.html").read_text(encoding="utf-8")

    assert 'const DISPLAY_TIME_ZONE = "Asia/Shanghai"' in text
    assert 'timeZone: DISPLAY_TIME_ZONE' in text
    assert "DISPLAY_TIME_FORMATTER.formatToParts" in text
    assert "toISOString().slice" not in text
    assert "UTC+8" in text
    assert "<small>UTC+8</small>" in index
    assert "UTC DAY" in text


def test_paper_account_cards_use_three_strategy_columns() -> None:
    text = (STATIC / "dashboard.css").read_text(encoding="utf-8")

    assert "grid-template-columns: repeat(3, minmax(0, 1fr))" in text
    assert ".acct-strategy-column" in text
    assert "grid-auto-flow: column" not in text


def test_reports_panel_does_not_repeat_paper_accounts() -> None:
    text = (STATIC / "dashboard.js").read_text(encoding="utf-8")

    assert "纸面运行" not in text
    assert "PAPER RUNS" not in text
    assert "paper_runs" not in text
