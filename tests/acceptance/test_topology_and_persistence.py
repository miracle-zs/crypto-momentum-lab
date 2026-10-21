"""Deployment acceptance for one NextGen process per live account."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.contract

# The four live accounts are business identities, not service names; they are
# checked against the deployment descriptor so renaming a service cannot
# silently drop an account from the assertion.
LIVE_ACCOUNTS = frozenset({"primary", "account-2", "account-3", "account-4"})

CREDENTIAL_ROLES = ("READ", "TRADE")

def _merged_live_services() -> dict[str, dict[str, object]]:
    base = yaml.safe_load(Path("compose.server.yaml").read_text(encoding="utf-8"))[
        "services"
    ]
    overlay = yaml.safe_load(
        Path("compose.next_gen.yaml").read_text(encoding="utf-8")
    )["services"]
    return {
        name: body
        for name, body in (base | overlay).items()
        if "nextgen" in body.get("profiles", [])
    }


def _service_account(body: dict[str, object]) -> str | None:
    command = body.get("command", [])
    if "--account-label" in command:
        raw = command[command.index("--account-label") + 1]
    else:
        raw = body.get("environment", {}).get("CML_ACCOUNT_LABEL")
    if raw is None:
        return None
    if raw.startswith("${"):
        raw = raw.split(":-", 1)[1].removesuffix("}")
    return raw


def _service_roles(body: dict[str, object]) -> frozenset[str]:
    environment = body.get("environment", {})
    return frozenset(
        role for role in CREDENTIAL_ROLES if f"BINANCE_{role}_API_KEY" in environment
    )


def test_each_account_is_assembled_as_one_process_holding_both_roles() -> None:
    """Q5/T3: one process per account, holding the read and trade roles.

    An account is 'one process' when the merged deployment descriptor gives it
        exactly one NextGen service, and that service is the one that both
    synchronises account facts (read credentials) and executes trades (trade
    credentials).  No service name is treated as the target architecture: the
    account label and the credential roles are the only inputs.
    """
    by_account: dict[str, list[tuple[str, frozenset[str]]]] = {
        account: [] for account in LIVE_ACCOUNTS
    }
    for name, body in _merged_live_services().items():
        account = _service_account(body)
        roles = _service_roles(body)
        if account in by_account and roles:
            by_account[account].append((name, roles))

    violations: dict[str, object] = {}
    for account in sorted(LIVE_ACCOUNTS):
        processes = by_account[account]
        if not processes:
            violations[account] = "no live process carries its credentials"
            continue
        if len(processes) != 1:
            violations[account] = {
                "live_processes": len(processes),
                "roles": {name: sorted(roles) for name, roles in processes},
            }
            continue
        roles = processes[0][1]
        if roles != frozenset(CREDENTIAL_ROLES):
            violations[account] = {"live_processes": 1, "roles": sorted(roles)}

    assert not violations, (
        "the target topology is one process per account that both synchronises "
        "account facts and executes trades; observed assembly: "
        f"{violations}"
    )
