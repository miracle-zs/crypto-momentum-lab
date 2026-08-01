from pathlib import Path

import yaml


def test_server_compose_exposes_complete_paper_stack() -> None:
    manifest = yaml.safe_load(Path("compose.server.yaml").read_text(encoding="utf-8"))

    services = manifest["services"]
    assert {
        "postgres",
        "migrate",
        "bootstrap-universe",
        "market-data",
        "paper-compression-pair",
        "paper-orderflow-pair",
        "paper-liquidation-pair",
        "dashboard",
    } <= services.keys()
    assert services["dashboard"]["ports"] == ["127.0.0.1:8765:8765"]
    for service in (
        "paper-compression-pair",
        "paper-orderflow-pair",
        "paper-liquidation-pair",
    ):
        assert services[service]["entrypoint"] == ["cml-strategy-runner"]
        assert (
            _option_value(
                services[service]["command"],
                "--paper-initial-balance",
            )
            == "1000"
        )
    compression = services["paper-compression-pair"]["command"]
    assert _option_value(compression, "--strategy") == "compression_breakout"
    assert _option_value(compression, "--signal-interval-seconds") == "300"
    assert _option_value(compression, "--compression-window-buckets") == "20"
    assert _option_value(compression, "--max-range-width-pct") == "0.025"
    assert _option_value(compression, "--min-breakout-pct") == "0.003"
    assert _option_value(compression, "--cooldown-buckets") == "12"
    assert _option_value(compression, "--fixed-take-profit-pct") == "0.03"
    assert _option_value(compression, "--fixed-stop-loss-pct") == "0.015"
    assert _option_value(compression, "--fixed-max-holding-buckets") == "480"
    assert "--require-market-quote" in compression
    assert (
        _option_value(
            services["paper-orderflow-pair"]["command"],
            "--strategy",
        )
        == "orderflow_impulse"
    )
    assert (
        _option_value(
            services["paper-liquidation-pair"]["command"],
            "--strategy",
        )
        == "liquidation_cascade"
    )
    assert "BINANCE_API_KEY" not in str(manifest)
    assert "BINANCE_API_SECRET" not in str(manifest)


def test_server_paper_capture_only_subscribes_to_strategy_required_streams() -> None:
    capture = yaml.safe_load(
        Path("configs/capture/server_paper.yaml").read_text(encoding="utf-8")
    )

    assert capture["enabled_streams"] == [
        "aggTrade",
        "bookTicker",
        "forceOrder",
        "kline_1m",
    ]


def test_nginx_proxy_keeps_existing_site_and_mounts_console() -> None:
    config = Path("deploy/nginx/crypto-momentum-lab.conf").read_text(encoding="utf-8")

    assert "location /momentum/" in config
    assert "proxy_pass http://127.0.0.1:8765/;" in config


def _option_value(command: list[str], option: str) -> str:
    return command[command.index(option) + 1]
