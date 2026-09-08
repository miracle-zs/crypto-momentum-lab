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
