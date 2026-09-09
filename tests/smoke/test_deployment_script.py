import subprocess
from pathlib import Path

ROOT = Path(__file__).parents[2]
DEPLOY_SCRIPT = ROOT / "deploy/ops/update_server.sh"
DOCKERFILE = ROOT / "Dockerfile"


def test_deployment_script_is_valid_shell_and_has_recovery_guards() -> None:
    subprocess.run(["bash", "-n", str(DEPLOY_SCRIPT)], check=True)
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    remote_marker = script.index("<<'REMOTE_SCRIPT'")
    remote_start = script.index("set -Eeuo pipefail\n", remote_marker)
    remote_end = script.index("\nREMOTE_SCRIPT\n", remote_start)
    subprocess.run(
        ["bash", "-n"],
        input=script[remote_start:remote_end].encode(),
        check=True,
    )

    assert "flock -n 9" in script
    assert "git reset --keep \"$target_commit\"" in script
    assert '"$target_commit" == "$previous_commit"' in script
    assert "write_deploy_state failed" in script
    assert "phase=client-total" in script
    assert "verify_service_target" in script
    assert "dashboard_proxy_url" in script
    assert "deploy_state_phase" in script
    assert "resume_from_phase" in script
    assert "should_run_phase" in script
    assert "docker image inspect" in script
    assert "service_is_converged" in script
    assert "up_and_wait" in script
    assert "--force-recreate --no-deps" in script
    assert "runtime_commit=" in script
    assert "image_commit=" in script
    assert "CML_MARKET_DATA_WAIT_TIMEOUT_SECONDS" in script
    assert "CML_DEPLOY_OPERATION_TIMEOUT_SECONDS" in script
    assert "CML_DEPLOY_BUILD_TIMEOUT_SECONDS" in script
    assert "run_with_timeout" in script
    assert "phase=migrate" in script
    assert 'run --rm --no-deps migrate' in script
    assert "phase=volume-init" in script
    assert 'run --rm --no-deps volume-init' in script
    assert "volume-init-check" in script
    assert "ownership=correct" in script
    assert "logs --no-color --tail=200" in script


def test_live_account_services_share_the_private_request_pacer_volume() -> None:
    compose = "\n".join(
        (
            (ROOT / "compose.server.yaml").read_text(encoding="utf-8"),
            (ROOT / "compose.live.accounts.yaml").read_text(encoding="utf-8"),
        )
    )

    assert compose.count("binance-rest-pacer:/run/cml/binance-rest-pacer") >= 5
    assert "binance-rest-pacer:" in compose


def test_research_collector_stops_before_market_data_restart() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")

    assert script.index('research_stop_started_at="$(date +%s)"') < script.index(
        "if should_run_phase market-data"
    )


def test_volume_initialization_precedes_dashboard_restart() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")

    assert script.index("deploy_phase=volume-init") < script.index(
        "# Nginx exposes the dashboard"
    )


def test_release_identity_does_not_precede_dependency_layer() -> None:
    lines = DOCKERFILE.read_text(encoding="utf-8").splitlines()
    dependency_install = next(
        index for index, line in enumerate(lines)
        if "pip" in line and "install" in line
    )
    release_arg = next(
        index for index, line in enumerate(lines)
        if line == "ARG CML_CODE_COMMIT"
    )

    assert release_arg > dependency_install
