"""Deployment contracts for the four NextGen account processes."""

from pathlib import Path

import yaml

ACCOUNTS = {"primary", "account-2", "account-3", "account-4"}


def _services():
    base = yaml.safe_load(Path("compose.server.yaml").read_text())["services"]
    extra = yaml.safe_load(Path("compose.next_gen.yaml").read_text())["services"]
    services = {name: dict(service) for name, service in base.items()}
    for name, override in extra.items():
        service = services.setdefault(name, {})
        for key, value in override.items():
            if isinstance(service.get(key), dict) and isinstance(value, dict):
                service[key] = service[key] | value
            else:
                service[key] = value
    return services


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
            assert "nextgen" in service.get("profiles", []), name
            secret = f"BINANCE_{role}_API_SECRET"
            assert environment[secret]
            scopes[role][account] = (environment[key], environment[secret])
    for accounts in scopes.values():
        assert set(accounts) == ACCOUNTS
        assert len(set(accounts.values())) == 4, "credentials shared across accounts"
    assert set(scopes["READ"].values()).isdisjoint(scopes["TRADE"].values())


def test_each_account_uses_the_nextgen_runtime_entrypoint():
    traders = _trading_services()
    assert len(traders) == 4
    assert {_account(service) for service in traders.values()} == ACCOUNTS
    for name, service in traders.items():
        assert service["entrypoint"] == ["cml-next-gen"]
        command = service["command"]
        assert command[0] == "run"
        assert _option_value(command, "--account-label") == _account(service)
        assert name.startswith("nextgen-strategy-")


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
    for compose_file in ("compose.server.yaml", "compose.next_gen.yaml"):
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
