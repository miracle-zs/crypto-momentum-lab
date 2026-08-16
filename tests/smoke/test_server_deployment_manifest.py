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
        "paper-orderflow-pair",
        "dashboard",
    } <= services.keys()
    assert "paper-liquidation-optimized" not in services
    assert services["dashboard"]["ports"] == ["127.0.0.1:8765:8765"]
    assert manifest["x-app"]["stop_grace_period"] == "60s"
    assert services["market-data"]["healthcheck"]["start_period"] == "15m"
    for service in (
        "paper-orderflow-pair",
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
        "paper-account-02-orderflow-v1",
        "paper-account-05-orderflow-candle15m-v1",
        "paper-account-10-orderflow-b2-long-candle15m-v1",
        "paper-account-12-orderflow-b1-long-candle15m-v1",
        "paper-account-13-orderflow-b8-long-candle15m-v1",
    }
    assert {
        item.strip()
        for item in services["dashboard"]["environment"][
            "CML_PAPER_ACCOUNT_RUN_IDS"
        ].split(",")
        if item.strip()
    } == configured_run_ids - {"paper-account-02-orderflow-v1"}
    assert (
        _option_value(
            services["paper-orderflow-pair"]["command"],
            "--strategy",
        )
        == "orderflow_impulse"
    )
    orderflow = services["paper-orderflow-pair"]["command"]
    assert _option_value(orderflow, "--fourth-run-id") == (
        "paper-account-10-orderflow-b2-long-candle15m-v1"
    )
    assert "--fourth-entry-long-only" in orderflow
    assert _option_value(orderflow, "--sixth-run-id") == (
        "paper-account-12-orderflow-b1-long-candle15m-v1"
    )
    assert "--sixth-entry-long-only" in orderflow
    assert _option_value(orderflow, "--sixth-candle-grace-bars") == "1"
    assert _option_value(orderflow, "--sixth-candle-grace-profit-pct") == "0.0088"
    assert _option_value(orderflow, "--seventh-run-id") == (
        "paper-account-13-orderflow-b8-long-candle15m-v1"
    )
    assert "--seventh-entry-long-only" in orderflow
    assert _option_value(orderflow, "--seventh-candle-grace-bars") == "8"
    assert _option_value(orderflow, "--seventh-candle-grace-profit-pct") == "0.0088"
    assert services["execution-account-live"]["profiles"] == ["live"]
    assert services["live-strategy"]["profiles"] == ["live"]
    assert services["execution-account-live"]["environment"][
        "BINANCE_API_KEY"
    ] == "${BINANCE_API_KEY:-}"
    assert "BINANCE_API_KEY" not in str(services["market-data"])
    assert "BINANCE_API_KEY" not in str(services["paper-orderflow-pair"])


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
