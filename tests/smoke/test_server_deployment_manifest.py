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
    assert manifest["x-app"]["stop_grace_period"] == "60s"
    assert services["market-data"]["healthcheck"]["start_period"] == "15m"
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
        assert (
            _option_value(
                services[service]["command"],
                "--candidate-notional",
            )
            == "100"
        )
        assert "--replay-stale-states" not in services[service]["command"]
        assert "--continue-while-halted" not in services[service]["command"]
    configured_run_ids = {
        item.strip()
        for item in services["market-data"]["environment"][
            "CML_PAPER_EXIT_RUN_IDS"
        ].split(",")
        if item.strip()
    }
    assert configured_run_ids == {
        "paper-account-01-compression-original-fixed-v1",
        "paper-account-02-compression-original-candle15m-v1",
        "paper-account-02-orderflow-v1",
        "paper-account-05-orderflow-candle15m-v1",
        "paper-account-07-orderflow-candle45m-v1",
        "paper-account-03-liquidation-v1",
        "paper-account-06-liquidation-candle15m-v1",
        "paper-account-08-liquidation-candle2confirm-v1",
    }
    assert {
        item.strip()
        for item in services["dashboard"]["environment"][
            "CML_PAPER_ACCOUNT_RUN_IDS"
        ].split(",")
        if item.strip()
    } == configured_run_ids
    compression = services["paper-compression-pair"]["command"]
    assert _option_value(compression, "--strategy") == "compression_breakout"
    assert _option_value(compression, "--signal-interval-seconds") == "15"
    assert _option_value(compression, "--compression-window-buckets") == "20"
    assert _option_value(compression, "--max-range-width-pct") == "0.005"
    assert _option_value(compression, "--min-breakout-pct") == "0.001"
    assert _option_value(compression, "--cooldown-buckets") == "8"
    assert _option_value(compression, "--fixed-take-profit-pct") == "0.02"
    assert _option_value(compression, "--fixed-stop-loss-pct") == "0.01"
    assert _option_value(compression, "--fixed-max-holding-buckets") == "80"
    assert _option_value(compression, "--fixed-run-id") == (
        "paper-account-01-compression-original-fixed-v1"
    )
    assert _option_value(compression, "--candle-run-id") == (
        "paper-account-02-compression-original-candle15m-v1"
    )
    assert "--require-market-quote" in compression
    assert (
        _option_value(
            services["paper-orderflow-pair"]["command"],
            "--strategy",
        )
        == "orderflow_impulse"
    )
    orderflow = services["paper-orderflow-pair"]["command"]
    assert _option_value(orderflow, "--third-run-id") == (
        "paper-account-07-orderflow-candle45m-v1"
    )
    assert _option_value(orderflow, "--third-candle-minimum-holding-buckets") == (
        "180"
    )
    assert (
        _option_value(
            services["paper-liquidation-pair"]["command"],
            "--strategy",
        )
        == "liquidation_cascade"
    )
    liquidation = services["paper-liquidation-pair"]["command"]
    assert _option_value(liquidation, "--third-run-id") == (
        "paper-account-08-liquidation-candle2confirm-v1"
    )
    assert _option_value(liquidation, "--third-candle-confirmation-count") == "2"
    assert services["execution-account-live"]["profiles"] == ["live"]
    assert services["live-strategy"]["profiles"] == ["live"]
    assert services["execution-account-live"]["environment"][
        "BINANCE_API_KEY"
    ] == "${BINANCE_API_KEY:-}"
    assert "BINANCE_API_KEY" not in str(services["market-data"])
    assert "BINANCE_API_KEY" not in str(services["paper-compression-pair"])


def test_server_paper_capture_only_subscribes_to_strategy_required_streams() -> None:
    capture = yaml.safe_load(
        Path("configs/capture/server_paper.yaml").read_text(encoding="utf-8")
    )

    assert capture["enabled_streams"] == [
        "aggTrade",
        "bookTicker",
        "forceOrder",
    ]
    assert capture["archive"]["streams"] == ["forceOrder"]


def test_nginx_proxy_keeps_existing_site_and_mounts_console() -> None:
    config = Path("deploy/nginx/crypto-momentum-lab.conf").read_text(encoding="utf-8")

    assert "location /momentum/" in config
    assert "proxy_pass http://127.0.0.1:8765/;" in config


def test_repository_has_no_online_paper_gap_replay_script() -> None:
    assert not {
        "promote_paper_recovery.py",
        "rebuild_gap_states_from_raw.py",
        "recover_paper_gap.py",
        "replay_paper_gap.py",
    } & {path.name for path in Path("scripts").glob("*.py")}


def _option_value(command: list[str], option: str) -> str:
    return command[command.index(option) + 1]
