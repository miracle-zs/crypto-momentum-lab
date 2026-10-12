"""Deployment contracts independent of split or merged account processes.

This is a migration safety suite, not proof that the target single-process
architecture is implemented. Exact process-count acceptance belongs to T3.
"""

from pathlib import Path

import yaml

ACCOUNTS = {"primary", "account-2", "account-3", "account-4"}


def _services():
    base = yaml.safe_load(Path("compose.server.yaml").read_text())["services"]
    extra = yaml.safe_load(Path("compose.live.accounts.yaml").read_text())["services"]
    assert not set(base) & set(extra), "overlay must not silently redefine an account"
    return base | extra


def _account(service):
    command = service.get("command", [])
    if "--account-label" in command:
        value = _option_value(command, "--account-label")
    else:
        value = service.get("environment", {}).get("CML_ACCOUNT_LABEL")
    if value is None:
        return None
    if value.startswith("${"):
        value = value.split(":-", 1)[1].removesuffix("}")
    return value


def _trading_services():
    return {
        name: service for name, service in _services().items()
        if "BINANCE_TRADE_API_KEY" in service.get("environment", {})
    }


def test_four_accounts_keep_independent_read_and_trade_credentials():
    scopes = {role: {} for role in ("READ", "TRADE")}
    for name, service in _services().items():
        environment = service.get("environment", {})
        assert "BINANCE_API_KEY" not in environment
        assert "BINANCE_API_SECRET" not in environment
        for role in scopes:
            key = f"BINANCE_{role}_API_KEY"
            if key not in environment:
                continue
            account = _account(service)
            assert account in ACCOUNTS, name
            assert account not in scopes[role], "duplicate role owner for account"
            assert "live" in service.get("profiles", []), name
            secret = f"BINANCE_{role}_API_SECRET"
            assert environment[secret]
            scopes[role][account] = (environment[key], environment[secret])
    for accounts in scopes.values():
        assert set(accounts) == ACCOUNTS
        assert len(set(accounts.values())) == 4, "credentials shared across accounts"
    assert set(scopes["READ"].values()).isdisjoint(scopes["TRADE"].values())


def test_each_account_has_one_explicitly_enabled_order_dispatcher():
    traders = _trading_services()
    assert len(traders) == 4
    assert {_account(service) for service in traders.values()} == ACCOUNTS
    for service in traders.values():
        command = service["command"]
        assert "--i-understand-this-places-real-orders" in command
        assert _option_value(command, "--runtime-manifest") == (
            "/app/deploy/live-runtime.yaml"
        )


def test_account_merge_does_not_change_frozen_trading_defaults():
    # Freeze repository defaults, not server effective values. Runtime overrides
    # and PostgreSQL risk configuration must be compared separately at cutover.
    expected = {
        "ENTRY_LEVERAGE": "5",
        "MARGIN_TYPE": "CROSSED",
        "EXIT_MODE": "candle_15m",
        "IMPULSE_WINDOW_BUCKETS": "2",
        "CONFIRMATION_BUCKETS": "1",
        "MIN_RETURN_PCT": "0.0075",
        "MIN_IMBALANCE": "0.30",
        "MIN_INTENSITY": "3.0",
        "MIN_NOTIONAL_5M_VS_30M": "1.25",
        "CANDLE_GRACE_BARS": "8",
        "CANDLE_GRACE_DECISION_PROFIT_PCT": "0.001",
        "CANDLE_GRACE_PROFIT_PCT": "0.0088",
    }
    for service in _trading_services().values():
        environment = service["environment"]
        for suffix, literal in expected.items():
            raw = environment[f"CML_LIVE_{suffix}"]
            assert raw.rsplit(":-", 1)[-1].rstrip("}") == literal
        command = service["command"]
        assert "--entry-long-only" in command
        assert "--no-entry-price-above-ema5" in command
        assert "--no-entry-price-above-ema10" in command


def test_four_live_accounts_leave_database_capacity_for_other_work():
    total = sum(
        int(service["environment"][f"CML_DB_{plane}_{option}"])
        for service in _trading_services().values()
        for plane in ("EXECUTION", "OBSERVABILITY")
        for option in ("POOL_SIZE", "MAX_OVERFLOW")
    )
    assert total <= 24


def test_shared_services_do_not_receive_private_credentials():
    services = _services()
    assert {"postgres", "market-data", "dashboard"} <= services.keys()
    for name in ("market-data", "dashboard"):
        assert not any(
            key.startswith("BINANCE_") and ("KEY" in key or "SECRET" in key)
            for key in services[name].get("environment", {})
        ), name
    assert all(volume.endswith(":ro") for volume in services["dashboard"]["volumes"])
    assert not any(name.startswith("paper-") for name in services)


def test_healthcheck_start_interval_requires_start_period() -> None:
    """Docker rejects start_interval unless start_period is also set."""

    offenders: list[str] = []
    for compose_file in ("compose.server.yaml", "compose.live.accounts.yaml"):
        manifest = yaml.safe_load(Path(compose_file).read_text(encoding="utf-8"))
        blocks = list(manifest.get("services", {}).items())
        blocks += [
            (key, value)
            for key, value in manifest.items()
            if key.startswith("x-") and isinstance(value, dict)
        ]
        for name, body in blocks:
            if not isinstance(body, dict):
                continue
            healthcheck = body.get("healthcheck")
            if not isinstance(healthcheck, dict):
                continue
            if "start_interval" in healthcheck and "start_period" not in healthcheck:
                offenders.append(f"{compose_file}:{name}")
    assert offenders == []


def test_server_paper_capture_only_subscribes_to_strategy_required_streams() -> None:
    capture = yaml.safe_load(
        Path("configs/capture/server_paper.yaml").read_text(encoding="utf-8")
    )

    assert capture["enabled_streams"] == [
        "aggTrade",
        "bookTicker",
        "forceOrder",
        "markPrice@1s",
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
