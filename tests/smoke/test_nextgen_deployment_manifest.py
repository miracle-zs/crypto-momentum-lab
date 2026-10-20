import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from crypto_momentum_lab.live_rollout.runtime_manifest import load_live_runtime_manifest


def test_compose_passes_all_account_settings_and_keeps_role_credentials_separate():
    if shutil.which("docker") is None:
        pytest.skip("Docker Compose CLI is required to verify interpolation")
    files = [
        "compose.server.yaml",
        "compose.next_gen.yaml",
    ]
    required = set()
    for name in files:
        required.update(re.findall(r"\$\{(\w+):\?", Path(name).read_text()))
    environment = dict(os.environ)
    for key in list(environment):
        if key.startswith(("BINANCE_", "CML_LIVE_")):
            del environment[key]
    environment.update({key: "test-" + key for key in required})
    environment.update(
        CML_LIVE_TARGET_NOTIONAL="150",
        CML_LIVE_MIN_RETURN_PCT_ACCOUNT_2="0.0123",
    )
    command = ["docker", "compose", "--env-file", "/dev/null"]
    for name in files:
        command.extend(["-f", name])
    result = subprocess.run(
        [*command, "--profile", "nextgen", "config", "--format", "json"],
        env=environment,
        text=True,
        capture_output=True,
        check=True,
    )
    services = json.loads(result.stdout)["services"]
    for label in ("primary", "account-2", "account-3", "account-4"):
        values = services["nextgen-strategy-" + label]["environment"]
        config = load_live_runtime_manifest(
            Path("deploy/live-runtime.yaml"), environment=values
        ).account(label)
        assert str(config.execution_inputs.target_notional) == "150"
        assert values["BINANCE_READ_API_KEY"] != values["BINANCE_TRADE_API_KEY"]
        if label == "account-2":
            assert str(config.strategy_inputs.profile.min_return_pct) == "0.0123"
    assert "CML_TRADING_ENGINE" not in services["dashboard"]["environment"]
    assert not any(
        key.startswith("BINANCE_") for key in services["dashboard"]["environment"]
    )
