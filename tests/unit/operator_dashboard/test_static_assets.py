import re
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlsplit

STATIC = Path("src/crypto_momentum_lab/operator_dashboard/static")


class _DashboardMarkupParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.stylesheets: list[str] = []
        self.scripts: list[str] = []
        self.module_scripts: list[str] = []
        self.endpoints: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        endpoint = attributes.get("data-endpoint")
        if endpoint is not None:
            self.endpoints.append(endpoint)

        if tag == "link" and attributes.get("rel") == "stylesheet":
            href = attributes.get("href")
            if href is not None:
                self.stylesheets.append(href)
        elif tag == "script":
            src = attributes.get("src")
            if src is not None:
                self.scripts.append(src)
                if attributes.get("type") == "module":
                    self.module_scripts.append(src)


def test_static_index_contains_dashboard_mount() -> None:
    text = (STATIC / "index.html").read_text(encoding="utf-8")

    for section_id in (
        "overview",
        "universe",
        "strategy",
        "account",
        "risk",
        "reports",
        "collector",
        "performance",
    ):
        assert f'id="{section_id}"' in text


def test_static_javascript_uses_relative_api_paths() -> None:
    poller = (STATIC / "app" / "poller.js").read_text(encoding="utf-8")
    index = (STATIC / "index.html").read_text(encoding="utf-8")
    markup = _DashboardMarkupParser()
    markup.feed(index)

    assert "api/overview" in markup.endpoints
    assert all(not endpoint.startswith("/") for endpoint in markup.endpoints)
    assert "fetch(" in poller
    assert "binance.com" not in poller.lower()

    active_assets = markup.stylesheets + markup.scripts
    active_paths = {urlsplit(asset).path for asset in active_assets}
    assert len(active_paths) == 3
    assert all(path.startswith("static/assets/") for path in active_paths)
    assert any(
        re.fullmatch(r"static/assets/dashboard-[a-f0-9]{16}\.css", path)
        for path in active_paths
    )
    assert any(
        re.fullmatch(r"static/assets/echarts-[a-f0-9]{16}\.js", path)
        for path in active_paths
    )
    assert any(
        re.fullmatch(r"static/assets/dashboard-[a-f0-9]{16}\.js", path)
        for path in active_paths
    )
    for path in active_paths:
        assert (STATIC / path.removeprefix("static/")).is_file(), (
            f"missing built asset: {path}"
        )
    assert all(not urlsplit(asset).path.startswith("/") for asset in active_assets)
    assert all(not urlsplit(asset).query for asset in active_assets)


def test_dashboard_loads_stable_frontend_modules() -> None:
    javascript = (STATIC / "dashboard.js").read_text(encoding="utf-8")
    index = (STATIC / "index.html").read_text(encoding="utf-8")
    markup = _DashboardMarkupParser()
    markup.feed(index)

    assert any(
        re.fullmatch(
            r"static/assets/dashboard-[a-f0-9]{16}\.js",
            urlsplit(source).path,
        )
        for source in markup.module_scripts
    )
    assert "import" in javascript, "source entrypoint must remain modular before bundling"


def test_degraded_status_labels_are_visible() -> None:
    text = (STATIC / "index.html").read_text(encoding="utf-8")

    for label in ("UNKNOWN", "STALE", "HALTED", "LIVE"):
        assert label in text
