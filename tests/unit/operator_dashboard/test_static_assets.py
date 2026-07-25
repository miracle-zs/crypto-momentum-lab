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
    assert 'href="static/dashboard.css"' in index
    assert 'src="static/dashboard.js"' in index
    assert 'data-endpoint="/api/' not in index
    assert "fetch(section.dataset.endpoint" in text
    assert "binance.com" not in text.lower()


def test_degraded_status_labels_are_visible() -> None:
    text = (STATIC / "index.html").read_text(encoding="utf-8")

    for label in ("UNKNOWN", "STALE", "HALTED", "LIVE"):
        assert label in text
