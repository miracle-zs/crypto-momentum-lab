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
    assert {
        "static/dashboard.css",
        "static/vendor/echarts.min.js",
        "static/dashboard.js",
    } <= active_paths
    assert all(not urlsplit(asset).path.startswith("/") for asset in active_assets)
    assert all(urlsplit(asset).query.startswith("v=") for asset in active_assets)


def test_dashboard_loads_stable_frontend_modules() -> None:
    javascript = (STATIC / "dashboard.js").read_text(encoding="utf-8")
    index = (STATIC / "index.html").read_text(encoding="utf-8")
    markup = _DashboardMarkupParser()
    markup.feed(index)

    assert any(
        urlsplit(source).path == "static/dashboard.js"
        for source in markup.module_scripts
    )

    uncommented_javascript = "\n".join(
        line for line in javascript.splitlines() if not line.lstrip().startswith("//")
    )
    imported_modules = re.findall(
        r"""(?ms)^[ \t]*import\b[\s\S]*?\bfrom\s*["']([^"']+)["']\s*;""",
        uncommented_javascript,
    )
    imported_paths = {urlsplit(module).path for module in imported_modules}
    assert imported_paths, "dashboard entrypoint must load executable modules"
    for module in imported_modules:
        path = STATIC / urlsplit(module).path.removeprefix("./")
        assert path.is_file(), f"missing imported dashboard module: {path}"


def test_degraded_status_labels_are_visible() -> None:
    text = (STATIC / "index.html").read_text(encoding="utf-8")

    for label in ("UNKNOWN", "STALE", "HALTED", "LIVE"):
        assert label in text
