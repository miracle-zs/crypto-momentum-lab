import os
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[3] / "deploy/ops/update_server.sh"


def run_guard(*, running="", live="0", changed="1", docker_status="0"):
    source = SCRIPT.read_text()
    start = source.index('if [[ "$destructive_schema_changed" == 1')
    end = source.index("if [[ ! -f .env.server ]]", start)
    harness = """
set -eu
destructive_schema_changed="$GUARD_CHANGED"
live_update="$GUARD_LIVE"
docker() { printf '%s\n' "$GUARD_RUNNING"; return "$GUARD_DOCKER_STATUS"; }
timeout() { shift 3; "$@"; }
"""
    return subprocess.run(
        ["bash", "-c", harness + source[start:end]],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "GUARD_CHANGED": changed,
            "GUARD_LIVE": live,
            "GUARD_RUNNING": running,
            "GUARD_DOCKER_STATUS": docker_status,
        },
    )


@pytest.mark.parametrize("service", ["live-strategy", "dashboard", "market-data"])
def test_non_live_deployment_cannot_migrate_under_old_consumers(service):
    result = run_guard(running=f"postgres\n{service}")
    assert result.returncode != 0


def test_stopped_consumers_allow_explicit_cutover():
    assert run_guard(running="postgres").returncode == 0


def test_container_inventory_failure_fails_closed():
    assert run_guard(docker_status="1").returncode != 0


def test_online_live_rollout_still_refuses_destructive_schema():
    assert run_guard(running="postgres", live="1").returncode != 0


def test_ordinary_rollout_is_not_blocked_by_consumer_guard():
    assert run_guard(running="live-strategy", changed="0").returncode == 0


@pytest.mark.parametrize("previous,target,expected", [(3, 4, "1"), (4, 4, "0")])
def test_checkpoint_schema_change_marks_cutover_destructive(
    tmp_path, previous, target, expected
):
    source = SCRIPT.read_text()
    start = source.index("recovery_schema_path=src/")
    end = source.index("restart_ops_monitor_if_changed()", start)
    path = tmp_path / "src/crypto_momentum_lab/domain/execution/recovery_models.py"
    path.parent.mkdir(parents=True)
    path.write_text(f"POSITION_RECOVERY_CHECKPOINT_SCHEMA_VERSION = {target}\n")
    harness = f"""
set -euo pipefail
deployment_base_commit=previous
destructive_schema_changed=0
git() {{ printf 'POSITION_RECOVERY_CHECKPOINT_SCHEMA_VERSION = {previous}\\n'; }}
"""
    result = subprocess.run(
        [
            "bash",
            "-c",
            harness
            + source[start:end]
            + 'printf "changed=%s\\n" "$destructive_schema_changed"',
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert f"changed={expected}" in result.stdout
