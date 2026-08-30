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
        "paper-b1-gainer100",
        "paper-b1-gainer100-ema",
        "dashboard",
    } <= services.keys()
    assert "paper-liquidation-optimized" not in services
    assert services["dashboard"]["ports"] == ["127.0.0.1:8765:8765"]
    assert manifest["x-app"]["stop_grace_period"] == "60s"
    assert "build" not in manifest["x-app"]
    assert services["migrate"]["build"]["context"] == "."
    assert services["migrate"]["image"] == manifest["x-app"]["image"]
    assert services["market-data"]["healthcheck"]["start_period"] == "15m"
    assert services["postgres"]["mem_limit"] == "1g"
    assert services["postgres"]["memswap_limit"] == "1536m"
    assert services["execution-account-live"]["mem_limit"] == "160m"
    assert services["live-strategy"]["mem_limit"] == "512m"
    assert services["dashboard"]["mem_limit"] == "320m"
    assert services["execution-account-live"]["healthcheck"]["interval"] == "30s"
    assert services["execution-account-live"]["healthcheck"]["retries"] == 4
    assert services["dashboard"]["healthcheck"]["interval"] == "30s"
    assert services["dashboard"]["healthcheck"]["retries"] == 4
    for service in (
        "paper-orderflow-pair",
        "paper-b1-gainer100",
        "paper-b1-gainer100-ema",
    ):
        assert services[service]["healthcheck"]["interval"] == "45s"
        assert services[service]["healthcheck"]["retries"] == 2
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
        "paper-account-05-orderflow-candle15m-v1",
        "paper-account-10-orderflow-b2-long-candle15m-v1",
        "paper-account-12-orderflow-b1-long-candle15m-v1",
        "paper-account-13-orderflow-b8-long-candle15m-v1",
        "paper-account-14-orderflow-b1-gainer100-v1",
        "paper-account-15-orderflow-b1-gainer100-ema-v1",
    }
    assert {
        item.strip()
        for item in services["dashboard"]["environment"][
            "CML_PAPER_ACCOUNT_RUN_IDS"
        ].split(",")
        if item.strip()
    } == configured_run_ids
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
    assert "--fixed-run-id" not in orderflow
    for service in ("paper-b1-gainer100", "paper-b1-gainer100-ema"):
        command = services[service]["command"]
        assert _option_value(command, "--entry-positive-gainer-top-count") == "100"
        assert "--entry-long-only" in command
    account14_command = services["paper-b1-gainer100"]["command"]
    assert "--no-entry-price-above-ema5" in account14_command
    assert "--no-entry-price-above-ema10" in account14_command
    assert _option_value(
        account14_command,
        "--orderflow-min-aggressive-imbalance",
    ) == "0.40"
    assert "--entry-price-above-ema5" not in services["paper-b1-gainer100"]["command"]
    assert "--entry-price-above-ema10" not in services["paper-b1-gainer100"]["command"]
    assert "--entry-price-above-ema5" in services["paper-b1-gainer100-ema"]["command"]
    assert "--entry-price-above-ema10" in services["paper-b1-gainer100-ema"]["command"]
    assert services["execution-account-live"]["profiles"] == ["live"]
    assert services["live-strategy"]["profiles"] == ["live"]
    live_healthcheck = services["live-strategy"]["healthcheck"]["test"]
    assert _option_value(live_healthcheck, "--service") == "live"
    assert "--ignore-age" not in live_healthcheck
    assert _option_value(live_healthcheck, "--session-id") == (
        "${CML_LIVE_SESSION_ID:-live-primary-v1}"
    )
    live_command = services["live-strategy"]["command"]
    assert _option_value(live_command, "--entry-positive-gainer-top-count") == (
        "${CML_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT:-100}"
    )
    assert "--entry-long-only" in live_command
    assert "--no-entry-price-above-ema5" in live_command
    assert "--no-entry-price-above-ema10" in live_command
    assert "--entry-price-above-ema5" not in live_command
    assert "--entry-price-above-ema10" not in live_command
    assert _option_value(
        live_command,
        "--candle-grace-decision-profit-pct",
    ) == "${CML_LIVE_CANDLE_GRACE_DECISION_PROFIT_PCT:-0.001}"
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
