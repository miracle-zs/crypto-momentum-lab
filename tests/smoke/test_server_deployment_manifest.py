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
        "paper-orderflow-gainer10-pair",
        "paper-b1-gainer100",
        "paper-b1-gainer100-ema",
        "dashboard",
    } <= services.keys()
    assert "paper-liquidation-optimized" not in services
    assert services["dashboard"]["ports"] == ["127.0.0.1:8765:8765"]
    assert services["dashboard"]["volumes"] == [
        "research-data:/app/research-data:ro"
    ]
    assert manifest["x-app"]["stop_grace_period"] == "20s"
    assert manifest["x-app"]["environment"]["CML_LOCAL_HEALTH_DIR"] == (
        "/run/cml/health"
    )
    assert "build" not in manifest["x-app"]
    assert services["migrate"]["build"]["context"] == "."
    assert services["migrate"]["image"] == manifest["x-app"]["image"]
    assert services["market-data"]["healthcheck"]["start_period"] == "15m"
    assert services["market-data"]["healthcheck"]["start_interval"] == "15s"
    assert services["market-data"]["healthcheck"]["test"] == [
        "CMD",
        "/usr/local/bin/cml-local-healthcheck",
        "120",
    ]
    assert services["market-data"]["stop_grace_period"] == "60s"
    assert services["postgres"]["mem_limit"] == "1g"
    assert services["postgres"]["memswap_limit"] == "1536m"
    assert services["execution-account-live"]["mem_limit"] == "160m"
    assert services["live-strategy"]["mem_limit"] == "512m"
    assert services["dashboard"]["mem_limit"] == "320m"
    assert services["execution-account-live"]["healthcheck"]["interval"] == "60s"
    assert services["execution-account-live"]["healthcheck"]["retries"] == 2
    assert services["execution-account-live"]["healthcheck"]["start_interval"] == "5s"
    assert services["execution-account-live"]["stop_grace_period"] == "60s"
    assert services["live-strategy"]["healthcheck"]["start_interval"] == "5s"
    assert services["live-strategy"]["stop_grace_period"] == "60s"
    assert services["dashboard"]["healthcheck"]["interval"] == "30s"
    assert services["dashboard"]["healthcheck"]["retries"] == 4
    assert services["market-data"]["healthcheck"]["interval"] == "60s"
    assert services["market-data"]["healthcheck"]["retries"] == 2
    assert services["execution-account-live"]["healthcheck"]["test"] == [
        "CMD",
        "/usr/local/bin/cml-local-healthcheck",
        "120",
    ]
    for service in (
        "paper-orderflow-pair",
        "paper-orderflow-gainer10-pair",
        "paper-b1-gainer100",
        "paper-b1-gainer100-ema",
    ):
        assert services[service]["healthcheck"]["interval"] == "60s"
        assert services[service]["healthcheck"]["retries"] == 2
        assert services[service]["healthcheck"]["start_interval"] == "5s"
        assert services[service]["healthcheck"]["test"] == [
            "CMD",
            "/usr/local/bin/cml-local-healthcheck",
            "180",
        ]
    for service in (
        "paper-orderflow-pair",
    ):
        assert services[service]["entrypoint"] == ["cml-strategy-runner"]
        assert _option_value(
            services[service]["command"],
            "--poll-interval-seconds",
        ) == "1.0"
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
        "paper-account-16-orderflow-b8-gainer10-imbalance040-v1",
        "paper-account-17-orderflow-b1-gainer10-imbalance040-v1",
    }
    assert {
        item.strip()
        for item in services["dashboard"]["environment"][
            "CML_PAPER_ACCOUNT_RUN_IDS"
        ].split(",")
        if item.strip()
    } == configured_run_ids
    assert manifest["x-paper-account-run-ids"] == services["market-data"][
        "environment"
    ]["CML_PAPER_EXIT_RUN_IDS"]
    assert manifest["x-paper-account-run-ids"] == services["dashboard"][
        "environment"
    ]["CML_PAPER_ACCOUNT_RUN_IDS"]
    assert (
        _option_value(
            services["paper-orderflow-pair"]["command"],
            "--strategy",
        )
        == "orderflow_impulse"
    )
    orderflow = services["paper-orderflow-pair"]["command"]
    gainer10_orderflow = services["paper-orderflow-gainer10-pair"]["command"]
    assert _option_value(orderflow, "--checkpoint-phase-seconds") == "0"
    assert (
        _option_value(
            services["paper-b1-gainer100"]["command"],
            "--checkpoint-phase-seconds",
        )
        == "30"
    )
    assert _option_value(gainer10_orderflow, "--checkpoint-phase-seconds") == "15"
    assert (
        _option_value(
            services["paper-b1-gainer100-ema"]["command"],
            "--checkpoint-phase-seconds",
        )
        == "45"
    )
    assert _option_value(
        gainer10_orderflow,
        "--orderflow-min-aggressive-imbalance",
    ) == "0.40"
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
        assert _option_value(command, "--poll-interval-seconds") == "1.0"
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
    assert live_healthcheck == [
        "CMD",
        "/usr/local/bin/cml-local-healthcheck",
        "300",
    ]
    assert services["research-collector"]["healthcheck"]["interval"] == "90s"
    assert services["research-collector"]["healthcheck"]["retries"] == 2
    assert services["research-collector"]["healthcheck"]["start_interval"] == "5s"
    assert services["dashboard"]["healthcheck"]["test"] == [
        "CMD-SHELL",
        (
            "python -S -c \"import urllib.request; "
            "urllib.request.urlopen('http://127.0.0.1:8765/api/health', timeout=3)\""
        ),
    ]
    live_command = services["live-strategy"]["command"]
    assert _option_value(
        live_command,
        "--persist-exchange-operations",
    ) == "${CML_LIVE_PERSIST_EXCHANGE_OPERATIONS:-submit,cancel}"
    assert "--entry-positive-gainer-top-count" not in live_command
    assert "--entry-long-only" in live_command
    assert "--no-entry-price-above-ema5" in live_command
    assert "--no-entry-price-above-ema10" in live_command
    assert "--entry-price-above-ema5" not in live_command
    assert "--entry-price-above-ema10" not in live_command
    assert _option_value(
        live_command,
        "--candle-grace-decision-profit-pct",
    ) == "${CML_LIVE_CANDLE_GRACE_DECISION_PROFIT_PCT:-0.001}"
    for profile_option in (
        "--impulse-window-buckets",
        "--confirmation-buckets",
        "--min-return-pct",
        "--min-imbalance",
        "--min-intensity",
        "--min-notional-5m-vs-30m",
        "--cooldown-buckets",
    ):
        assert profile_option not in live_command
    for profile_env in (
        "CML_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT",
        "CML_LIVE_IMPULSE_WINDOW_BUCKETS",
        "CML_LIVE_CONFIRMATION_BUCKETS",
        "CML_LIVE_MIN_RETURN_PCT",
        "CML_LIVE_MIN_IMBALANCE",
        "CML_LIVE_MIN_INTENSITY",
        "CML_LIVE_MIN_NOTIONAL_5M_VS_30M",
        "CML_LIVE_COOLDOWN_BUCKETS",
    ):
        assert profile_env in services["live-strategy"]["environment"]
    assert services["live-strategy"]["environment"][
        "CML_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT"
    ] == "${CML_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT:-10}"
    assert services["live-strategy"]["environment"][
        "CML_LIVE_IMPULSE_WINDOW_BUCKETS"
    ] == "${CML_LIVE_IMPULSE_WINDOW_BUCKETS:-4}"
    assert services["live-strategy"]["environment"]["CML_LIVE_SESSION_ID"] == (
        "${CML_LIVE_SESSION_ID:-live-primary-v1}"
    )
    assert services["live-strategy"]["environment"]["CML_LIVE_LEASE_OWNER"] == (
        "${CML_LIVE_LEASE_OWNER:-live-worker}"
    )
    assert services["execution-account-live"]["environment"][
        "BINANCE_READ_API_KEY"
    ] == "${BINANCE_READ_API_KEY:-}"
    assert services["execution-account-live"]["command"] == manifest[
        "x-execution-account-command"
    ]
    assert services["execution-account-live"]["environment"][
        "CML_ACCOUNT_LABEL"
    ] == "${CML_LIVE_ACCOUNT_LABEL:-primary}"
    assert "--account-label" not in services["execution-account-live"]["command"]
    assert services["live-strategy"]["environment"][
        "BINANCE_TRADE_API_KEY"
    ] == "${BINANCE_TRADE_API_KEY:-}"
    assert services["live-strategy"]["environment"]["CML_LIVE_ENTRY_LEVERAGE"] == (
        "${CML_LIVE_ENTRY_LEVERAGE:-5}"
    )
    assert services["live-strategy"]["environment"]["CML_LIVE_EXIT_MODE"] == (
        "${CML_LIVE_EXIT_MODE:-candle_15m}"
    )
    assert services["live-strategy"]["environment"][
        "CML_LIVE_PERSIST_EXCHANGE_OPERATIONS"
    ] == "${CML_LIVE_PERSIST_EXCHANGE_OPERATIONS:-submit,cancel}"
    assert "BINANCE_API_KEY" not in services["execution-account-live"][
        "environment"
    ]
    assert "BINANCE_API_SECRET" not in services["execution-account-live"][
        "environment"
    ]
    assert "BINANCE_API_KEY" not in services["live-strategy"]["environment"]
    assert "BINANCE_API_SECRET" not in services["live-strategy"]["environment"]
    assert "BINANCE_API_KEY" not in str(services["market-data"])
    assert "BINANCE_API_KEY" not in str(services["paper-orderflow-pair"])


def test_multi_live_overlay_keeps_one_market_data_and_isolates_accounts() -> None:
    manifest = yaml.safe_load(
        Path("compose.live.accounts.yaml").read_text(encoding="utf-8")
    )
    services = manifest["services"]
    assert set(services) == {
        "execution-account-live-account-2",
        "execution-account-live-account-3",
        "execution-account-live-account-4",
        "live-strategy-account-2",
        "live-strategy-account-3",
        "live-strategy-account-4",
    }
    for account_number in ("2", "3", "4"):
        execution = services[f"execution-account-live-account-{account_number}"]
        strategy = services[f"live-strategy-account-{account_number}"]
        assert execution["profiles"] == ["live"]
        assert strategy["profiles"] == ["live"]
        assert execution["stop_grace_period"] == "60s"
        assert strategy["stop_grace_period"] == "60s"
        assert execution["healthcheck"]["start_interval"] == "5s"
        assert strategy["healthcheck"]["start_interval"] == "5s"
        assert execution["healthcheck"]["test"] == [
            "CMD",
            "/usr/local/bin/cml-local-healthcheck",
            "120",
        ]
        assert strategy["healthcheck"]["test"] == [
            "CMD",
            "/usr/local/bin/cml-local-healthcheck",
            "300",
        ]
        assert execution["environment"]["CML_ACCOUNT_LABEL"] == (
            f"account-{account_number}"
        )
        assert execution["command"] == manifest["x-execution-account-command"]
        assert "--account-label" not in execution["command"]
        assert f"account-{account_number}" in strategy["command"]
        assert "--entry-policy-enforce" in strategy["command"]
        for option in (
            "--entry-positive-gainer-top-count",
            "--impulse-window-buckets",
            "--confirmation-buckets",
            "--min-return-pct",
            "--min-imbalance",
            "--min-intensity",
            "--min-notional-5m-vs-30m",
            "--cooldown-buckets",
        ):
            assert option not in strategy["command"]
        for profile_env in (
            "CML_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT",
            "CML_LIVE_IMPULSE_WINDOW_BUCKETS",
            "CML_LIVE_CONFIRMATION_BUCKETS",
            "CML_LIVE_MIN_RETURN_PCT",
            "CML_LIVE_MIN_IMBALANCE",
            "CML_LIVE_MIN_INTENSITY",
            "CML_LIVE_MIN_NOTIONAL_5M_VS_30M",
            "CML_LIVE_COOLDOWN_BUCKETS",
        ):
            assert profile_env in strategy["environment"]
        for execution_env, default in (
            ("ENTRY_LEVERAGE", "5"),
            ("MARGIN_TYPE", "CROSSED"),
            ("EXIT_MODE", "candle_15m"),
            ("TAKE_PROFIT_PCT", "0.02"),
            ("STOP_LOSS_PCT", "0.01"),
            ("CANDLE_GRACE_BARS", "8"),
            ("CANDLE_GRACE_DECISION_PROFIT_PCT", "0.001"),
            ("CANDLE_GRACE_PROFIT_PCT", "0.0088"),
            ("PERSIST_EXCHANGE_OPERATIONS", "submit,cancel"),
        ):
            assert strategy["environment"][f"CML_LIVE_{execution_env}"] == (
                "${CML_LIVE_"
                f"{execution_env}_ACCOUNT_{account_number}:-{default}}}"
            )
        assert "BINANCE_API_KEY" not in execution["environment"]
        assert "BINANCE_API_SECRET" not in execution["environment"]
        assert "BINANCE_API_KEY" not in strategy["environment"]
        assert "BINANCE_API_SECRET" not in strategy["environment"]
        assert _option_value(
            strategy["command"],
            "--persist-exchange-operations",
        ) == (
            "${CML_LIVE_PERSIST_EXCHANGE_OPERATIONS_ACCOUNT_"
            f"{account_number}:-submit,cancel}}"
        )
        assert strategy["depends_on"][
            f"execution-account-live-account-{account_number}"
        ]["condition"] == "service_healthy"

    account_two_environment = services["live-strategy-account-2"]["environment"]
    assert account_two_environment["CML_LOCAL_HEALTH_DIR"] == "/run/cml/health"
    assert (
        account_two_environment["CML_LIVE_IMPULSE_WINDOW_BUCKETS"]
        == "${CML_LIVE_IMPULSE_WINDOW_BUCKETS_ACCOUNT_2:-4}"
    )
    assert (
        account_two_environment["CML_LIVE_MIN_RETURN_PCT"]
        == "${CML_LIVE_MIN_RETURN_PCT_ACCOUNT_2:-0.005}"
    )
    assert (
        account_two_environment["CML_LIVE_MIN_IMBALANCE"]
        == "${CML_LIVE_MIN_IMBALANCE_ACCOUNT_2:-0.30}"
    )
    assert (
        account_two_environment["CML_LIVE_MIN_INTENSITY"]
        == "${CML_LIVE_MIN_INTENSITY_ACCOUNT_2:-1.5}"
    )
    assert (
        account_two_environment["CML_LIVE_MIN_NOTIONAL_5M_VS_30M"]
        == "${CML_LIVE_MIN_NOTIONAL_5M_VS_30M_ACCOUNT_2:-1.50}"
    )
    assert (
        account_two_environment["CML_LIVE_SESSION_ID_ACCOUNT_2"]
        == "${CML_LIVE_SESSION_ID_ACCOUNT_2:-live-account-2-v1}"
    )
    assert (
        account_two_environment["CML_LIVE_MIGRATION_REVISION_ACCOUNT_2"]
        == "${CML_LIVE_MIGRATION_REVISION_ACCOUNT_2:-20260911_0036}"
    )

    for account_number in (3, 4):
        account_environment = services[
            f"live-strategy-account-{account_number}"
        ]["environment"]
        assert (
            account_environment["CML_LIVE_IMPULSE_WINDOW_BUCKETS"]
            == f"${{CML_LIVE_IMPULSE_WINDOW_BUCKETS_ACCOUNT_{account_number}:-2}}"
        )
        assert (
            account_environment["CML_LIVE_MIN_RETURN_PCT"]
            == f"${{CML_LIVE_MIN_RETURN_PCT_ACCOUNT_{account_number}:-0.005}}"
        )
        assert (
            account_environment["CML_LIVE_MIN_IMBALANCE"]
            == f"${{CML_LIVE_MIN_IMBALANCE_ACCOUNT_{account_number}:-0.30}}"
        )
        assert (
            account_environment["CML_LIVE_MIN_INTENSITY"]
            == f"${{CML_LIVE_MIN_INTENSITY_ACCOUNT_{account_number}:-4.0}}"
        )
        assert (
            account_environment["CML_LIVE_MIN_NOTIONAL_5M_VS_30M"]
            == f"${{CML_LIVE_MIN_NOTIONAL_5M_VS_30M_ACCOUNT_{account_number}:-1.50}}"
        )


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
