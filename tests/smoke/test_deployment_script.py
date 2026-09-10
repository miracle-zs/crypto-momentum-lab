import os
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
    assert '"$state" != running' in script
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


def test_deployment_script_loads_extra_live_overlay_only_when_needed() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    compose_start = script.index("has_running_compose_service()")
    compose_end = script.index("dashboard_image=", compose_start)
    compose_block = script[compose_start:compose_end]

    assert "-f compose.server.yaml" in compose_block
    assert "live_overlay_required=0" in compose_block
    assert "has_running_compose_service()" in compose_block
    assert "-f compose.live.accounts.yaml" in compose_block
    assert "--profile live" in compose_block
    live_overlay_guard = compose_block.index(
        'if [[ "$live_overlay_required" == 1 ]]; then'
    )
    live_overlay = compose_block.index("-f compose.live.accounts.yaml")
    assert live_overlay_guard < live_overlay


def test_live_overlay_detection_only_reports_running_extra_accounts(
    tmp_path: Path,
) -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    start = script.index("has_running_compose_service()")
    end = script.index("compose=(\n", start)
    detection = script[start:end]
    fake_docker = tmp_path / "docker"
    fake_docker.write_text(
        "#!/usr/bin/env bash\n"
        "set -Eeuo pipefail\n"
        "service=''\n"
        "for argument in \"$@\"; do\n"
        "  case \"$argument\" in\n"
        "    label=com.docker.compose.service=*) service=\"${argument##*=}\" ;;\n"
        "  esac\n"
        "done\n"
        "case \",${RUNNING_EXTRA_SERVICES:-},\" in\n"
        "  *,${service},*) printf 'container-id\\n' ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    command = (
        "set -Eeuo pipefail\n"
        "live_update=1\n"
        f"{detection}\n"
        "if has_running_compose_service live-strategy-account-2; then\n"
        "  printf 'overlay\\n'\n"
        "else\n"
        "  printf 'base\\n'\n"
        "fi\n"
    )
    base_env = {**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}"}
    primary_only = subprocess.check_output(
        ["bash", "-c", command],
        env={**base_env, "RUNNING_EXTRA_SERVICES": ""},
        text=True,
    )
    with_extra = subprocess.check_output(
        ["bash", "-c", command],
        env={**base_env, "RUNNING_EXTRA_SERVICES": "live-strategy-account-2"},
        text=True,
    )
    assert primary_only.strip() == "base"
    assert with_extra.strip() == "overlay"


def test_ancestor_target_reaches_reset_keep_branch(tmp_path: Path) -> None:
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)

    def git(*args: str) -> str:
        return subprocess.check_output(
            ["git", "-C", str(tmp_path), *args], text=True
        ).strip()

    git("config", "user.email", "test@example.com")
    git("config", "user.name", "Test")
    git("commit", "--allow-empty", "-qm", "base")
    base = git("rev-parse", "HEAD")
    git("commit", "--allow-empty", "-qm", "newer")
    newer = git("rev-parse", "HEAD")
    git("branch", "target", base)

    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    start = script.index('previous_commit="$(git rev-parse HEAD)"')
    end = script.index("\nenv_runtime_commit=\"\"", start)
    rollback_logic = script[start:end]
    command = (
        "set -Eeuo pipefail\n"
        "target_ref=target\n"
        f"{rollback_logic}\n"
        'test "$(git rev-parse HEAD)" = "$target_commit"\n'
    )
    subprocess.run(["bash", "-c", command], cwd=tmp_path, check=True)
    assert git("rev-parse", "HEAD") == base
    assert newer != base


def test_strategy_runtime_path_is_classified_without_market_restart() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    classification_start = script.index("runtime_changed=0\n")
    classification_end = script.index(
        'if [[ "$runtime_changed" == 1 ]]; then',
        classification_start,
    )
    classification = script[classification_start:classification_end]
    assert "src/crypto_momentum_lab/strategies/*" in classification
    assert "src/crypto_momentum_lab/strategy/*" not in classification


def test_live_update_checks_position_labels_for_running_accounts() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")

    assert "position_label_is_configured()" in script
    assert "validate_live_position_labels()" in script
    assert "CML_LIVE_POSITION_ACCOUNT_LABELS" in script
    assert script.index("validate_live_position_labels()") < script.index(
        "if ! validate_live_position_labels; then"
    )


def test_live_account_services_share_the_private_request_pacer_volume() -> None:
    compose = "\n".join(
        (
            (ROOT / "compose.server.yaml").read_text(encoding="utf-8"),
            (ROOT / "compose.live.accounts.yaml").read_text(encoding="utf-8"),
        )
    )

    assert compose.count("binance-rest-pacer:/run/cml/binance-rest-pacer") >= 5
    assert "binance-rest-pacer:" in compose


def test_ops_monitor_discovers_live_accounts_from_compose_files() -> None:
    service = (ROOT / "deploy/ops/cml-ops-monitor.service").read_text(
        encoding="utf-8"
    )

    assert "CML_COMPOSE_FILE=" in service
    assert "compose.live.accounts.yaml" in service
    assert "CML_MONITOR_SERVICES=" not in service
    assert "CML_MONITOR_LIVE_ACCOUNTS=" not in service
    assert "CML_MONITOR_SERVICES" in service
    assert "CML_MONITOR_LIVE_ACCOUNTS" in service


def test_retry_classification_preserves_dashboard_only_scope(tmp_path: Path) -> None:
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    def git(*args: str) -> str:
        return subprocess.check_output(
            ["git", "-C", str(tmp_path), *args], text=True
        ).strip()
    git("config", "user.email", "test@example.com")
    git("config", "user.name", "Test")
    git("commit", "--allow-empty", "-qm", "base")
    base = git("rev-parse", "HEAD")
    dashboard = tmp_path / "src/crypto_momentum_lab/operator_dashboard"
    dashboard.mkdir(parents=True)
    (dashboard / "queries.py").write_text("# changed\n")
    git("add", ".")
    git("commit", "-qm", "dashboard")
    target = git("rev-parse", "HEAD")
    script = DEPLOY_SCRIPT.read_text()
    classification = script[
        script.index("runtime_changed=0\n"):
        script.index('if [[ "$runtime_changed" == 1 ]]; then')
    ]
    result = subprocess.check_output(
        ["bash", "-c", 'set -eu\n' + classification +
         '\nprintf "%s" "$runtime_changed:$dashboard_changed:'
         '$market_changed:$paper_changed:$live_changed"'],
        cwd=tmp_path,
        env={
            **os.environ,
            "deployment_base_commit": base,
            "target_commit": target,
            "previous_commit": target,
            "deploy_state_checkout": target,
            "deploy_state_target": target,
            "deploy_state_status": "failed",
            "deploy_state_phase": "dashboard",
            "deploy_state_base": base,
            "runtime_commit": target,
            "live_update": "1",
        }, text=True,
    )
    assert result.endswith("1:1:0:0:0")


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
