from pathlib import Path

import yaml


def test_server_compose_exposes_complete_paper_stack() -> None:
    manifest = yaml.safe_load(
        Path("compose.server.yaml").read_text(encoding="utf-8")
    )

    services = manifest["services"]
    assert {
        "postgres",
        "migrate",
        "bootstrap-universe",
        "market-data",
        "paper-trader",
        "dashboard",
    } <= services.keys()
    assert services["dashboard"]["ports"] == ["127.0.0.1:8765:8765"]
    assert services["paper-trader"]["entrypoint"] == ["cml-strategy-runner"]
    assert "BINANCE_API_KEY" not in str(manifest)
    assert "BINANCE_API_SECRET" not in str(manifest)


def test_nginx_proxy_keeps_existing_site_and_mounts_console() -> None:
    config = Path("deploy/nginx/crypto-momentum-lab.conf").read_text(
        encoding="utf-8"
    )

    assert "location /momentum/" in config
    assert "proxy_pass http://127.0.0.1:8765/;" in config
