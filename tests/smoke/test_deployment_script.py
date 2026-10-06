import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
DEPLOY_SCRIPT = ROOT / "deploy/ops/update_server.sh"
DOCKERFILE = ROOT / "Dockerfile"


def test_remote_argument_decoder_preserves_timeouts_and_rollout_scope() -> None:
    script = DEPLOY_SCRIPT.read_text()
    marker = script.index("<<'REMOTE_SCRIPT'")
    start = script.index("set -Eeuo pipefail\n", marker)
    end = script.index("for timeout_name in", start)
    values = [
        "/srv/cml",
        "origin/main",
        "1",
        "2",
        "1",
        "301",
        "902",
        "303",
        "304",
        "95",
        "306",
        "907",
        "1",
        "http://localhost/health",
        "/srv/logs",
        "1",
        "1",
        "0",
    ]
    variables = (
        "remote_dir target_ref live_update live_concurrency "
        "live_canary_concurrency deploy_wait_timeout "
        "market_data_wait_timeout consumer_wait_timeout live_wait_timeout "
        "live_stop_timeout deploy_operation_timeout deploy_build_timeout "
        "dashboard_required dashboard_proxy_url crash_log_directory "
        "sync_dashboard execution_accounts_only dashboard_only "
    ).split()
    report = "printf '%s\\n' " + " ".join(f'"${name}"' for name in variables)
    result = subprocess.run(
        ["bash", "-c", script[start:end] + report, "test", *values],
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == values


@pytest.mark.parametrize(
    ("parallel", "fail_health"), [(1, False), (2, False), (1, True)]
)
def test_live_startup_concurrency_waits_for_health_before_next_batch(
    parallel, fail_health
):
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    start = script.index("  live_up_and_wait_parallel() {")
    end = script.index("\n  collect_active_live_pairs()", start)
    invocation = f"""set -Eeuo pipefail
{script[start:end]}
compose=(compose)
deploy_operation_timeout=300
record_restart_baseline() {{ :; }}
log_service_timings() {{ :; }}
stop_live_services() {{ echo "stop $*"; }}
run_with_timeout() {{ echo "$1"; }}
wait_for_services_healthy() {{ shift; echo "healthy $*"; return {17 if fail_health else 0}; }}
live_up_and_wait_parallel 300 {parallel} one two three four
"""
    result = subprocess.run(
        ["bash", "-c", invocation], capture_output=True, text=True, timeout=5
    )
    assert result.returncode == (17 if fail_health else 0), result.stderr
    expected = []
    services = ["one", "two", "three", "four"]
    for index in range(0, len(services), parallel):
        batch = " ".join(services[index : index + parallel])
        expected += [f"stop {batch}", f"compose-up:{batch}", f"healthy {batch}"]
        if fail_health:
            break
    assert result.stdout.splitlines() == expected


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
    assert 'git reset --keep "$target_commit"' in script
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
    assert "CML_LIVE_STOP_TIMEOUT_SECONDS" in script
    assert "CML_DEPLOY_OPERATION_TIMEOUT_SECONDS" in script
    assert "CML_DEPLOY_BUILD_TIMEOUT_SECONDS" in script
    assert "run_with_timeout" in script
    assert "phase=migrate" in script
    assert "run --rm --no-deps migrate" in script
    assert "phase=volume-init" in script
    assert "run --rm --no-deps volume-init" in script
    assert "volume-init-check" in script
    assert "ownership=correct" in script
    assert "logs --no-color --tail=200" in script


def test_deployment_script_reports_service_level_timings() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")

    assert "service-timing" in script
    assert "log_service_health_timings" in script
    assert 'log_service_timings "restart"' in script
    assert 'log_service_timings "graceful-stop"' in script
    assert "verify_service_target_timed" in script
    assert 'log_service_timing "health-wait"' in script
    assert 'log_service_timing "verify"' in script


def test_deployment_script_records_machine_readable_audit_events() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")

    assert "deployment-audit" in script
    assert "write_deployment_audit" in script
    assert '"elapsed_seconds":%s' in script


def test_paper_rollout_removes_retired_services() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")

    active_start = script.index("active_paper_services=(")
    active_end = script.index("compose_project_name=", active_start)
    active_config = script[active_start:active_end]
    assert "active_paper_services=()" in active_config
    for service in (
        "paper-orderflow-pair",
        "paper-b1-gainer100",
        "paper-b1-gainer100-ema",
        "paper-orderflow-gainer10-pair",
    ):
        assert service in active_config

    cleanup_start = script.index("stop_retired_paper_services()")
    cleanup_end = script.index("print_failure_context()", cleanup_start)
    cleanup = script[cleanup_start:cleanup_end]
    assert "docker stop --time 20" in cleanup
    assert 'docker rm "$container_id"' in cleanup
    assert "com.docker.compose.project" in cleanup
    assert "archive_container_logs" in cleanup

    cleanup_phase = script.index("deploy_phase=paper-retired-cleanup")
    consumers_phase = script.index("consumer_candidates=()")
    assert cleanup_phase < consumers_phase
    assert 'for paper_svc in "${active_paper_services[@]}"; do' in script
    assert (
        'if [[ "$paper_changed" == 1 ]] || ! service_is_converged "$paper_svc"; then'
        in script
    )
    assert 'consumer_candidates+=("$paper_svc")' in script


def test_health_wait_does_not_add_a_five_second_polling_gap() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    health_start = script.index("wait_for_services_healthy()")
    health_end = script.index("up_and_wait()", health_start)
    health_block = script[health_start:health_end]

    assert "sleep 1" in health_block
    assert "sleep 5" not in health_block


def test_live_readiness_validator_embedded_python_is_valid() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    marker = 'docker exec "$container_id" python -S -c \''
    live_validator = script.index("  verify_live_readiness()")
    start = script.index(marker, live_validator) + len(marker)
    end = script.index('\' "$account"', start)

    compile(script[start:end], "<live-readiness-validator>", "exec")
    assert "verify_live_readiness" in script
    assert "live-readiness" in script
    assert "/run/cml/health/readiness" in script
    assert "phase=live-readiness" in script


def test_deployment_script_loads_extra_live_overlay_only_when_needed() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    compose_start = script.index("has_running_compose_service()")
    compose_end = script.index("deploy_phase=compose", compose_start)
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
        'for argument in "$@"; do\n'
        '  case "$argument" in\n'
        '    label=com.docker.compose.service=*) service="${argument##*=}" ;;\n'
        "  esac\n"
        "done\n"
        'case ",${RUNNING_EXTRA_SERVICES:-}," in\n'
        "  *,${service},*) printf 'Up 1 second\\n' ;;\n"
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
    end = script.index('\nenv_runtime_commit=""', start)
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


def test_live_update_does_not_require_manual_position_labels() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")

    assert "CML_LIVE_POSITION_ACCOUNT_LABELS" in script
    assert "latest ready PostgreSQL" in script
    assert "position_label_is_configured()" not in script
    assert "validate_live_position_labels()" not in script


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
    service = (ROOT / "deploy/ops/cml-ops-monitor.service").read_text(encoding="utf-8")

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
        script.index("runtime_changed=0\n") : script.index(
            'if [[ "$runtime_changed" == 1 ]]; then'
        )
    ]
    result = subprocess.check_output(
        [
            "bash",
            "-c",
            "set -eu\n"
            + classification
            + '\nprintf "%s" "$runtime_changed:$schema_changed:'
            '$dashboard_changed:$market_changed:$paper_changed:$live_changed"',
        ],
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
        },
        text=True,
    )
    assert result.endswith("1:0:1:0:0:0")


def test_stale_runtime_behind_target_replays_full_rollout(tmp_path: Path) -> None:
    """A recorded runtime behind the target must not resume a stale phase.

    When the checkout is already at the target commit but the recorded runtime
    still points at the previous image, the persisted phase is not a safe
    resume point: the previous attempt never rolled the target image out, so
    the whole rollout has to be replayed from the checkout phase.
    """

    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)

    def git(*args: str) -> str:
        return subprocess.check_output(
            ["git", "-C", str(tmp_path), *args], text=True
        ).strip()

    git("config", "user.email", "test@example.com")
    git("config", "user.name", "Test")
    git("commit", "--allow-empty", "-qm", "deployed")
    stale_runtime = git("rev-parse", "HEAD")
    git("commit", "--allow-empty", "-qm", "target")
    target = git("rev-parse", "HEAD")

    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    classification = script[
        script.index("runtime_changed=0\n") : script.index(
            'if [[ "$runtime_changed" == 1 ]]; then'
        )
    ]
    result = subprocess.check_output(
        [
            "bash",
            "-c",
            "set -eu\n"
            + classification
            + '\nprintf "%s|%s" "$runtime_changed:$schema_changed:'
            '$dashboard_changed:$market_changed:$paper_changed:$live_changed" '
            '"$resume_from_phase"',
        ],
        cwd=tmp_path,
        env={
            **os.environ,
            "deployment_base_commit": target,
            "target_commit": target,
            "previous_commit": target,
            "deploy_state_checkout": target,
            "deploy_state_target": target,
            "deploy_state_status": "failed",
            "deploy_state_phase": "live-preflight",
            "deploy_state_base": target,
            "runtime_commit": stale_runtime,
            "live_update": "1",
        },
        text=True,
    )
    assert result.strip().splitlines()[-1] == "1:1:1:1:1:1|checkout"


def test_migration_phase_runs_one_shot_only_for_schema_changes() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")

    classification_start = script.index("runtime_changed=0\n")
    classification_end = script.index(
        'if [[ "$runtime_changed" == 1 ]]; then',
        classification_start,
    )
    classification = script[classification_start:classification_end]
    migration_start = script.index("deploy_phase=migrate")
    migration_end = script.index("deploy_phase=volume-init", migration_start)
    migration = script[migration_start:migration_end]

    assert "schema_changed=0" in classification
    assert "alembic.ini|alembic/*" in classification
    assert 'if [[ "$schema_changed" == 1 ]]; then' in migration
    assert "run --rm --no-deps migrate" in migration
    assert "phase=migrate skipped schema_changed=$schema_changed" in migration


def test_alembic_change_sets_schema_changed(tmp_path: Path) -> None:
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)

    def git(*args: str) -> str:
        return subprocess.check_output(
            ["git", "-C", str(tmp_path), *args], text=True
        ).strip()

    git("config", "user.email", "test@example.com")
    git("config", "user.name", "Test")
    git("commit", "--allow-empty", "-qm", "base")
    base = git("rev-parse", "HEAD")
    migration = tmp_path / "alembic/versions/20260912_0001_add_index.py"
    migration.parent.mkdir(parents=True)
    migration.write_text("# migration\n", encoding="utf-8")
    git("add", ".")
    git("commit", "-qm", "migration")
    target = git("rev-parse", "HEAD")

    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    classification_start = script.index("runtime_changed=0\n")
    classification_end = script.index(
        'if [[ "$runtime_changed" == 1 ]]; then',
        classification_start,
    )
    classification = script[classification_start:classification_end]
    result = subprocess.check_output(
        [
            "bash",
            "-c",
            "set -eu\n"
            + classification
            + '\nprintf "%s" "$runtime_changed:$schema_changed"',
        ],
        cwd=tmp_path,
        env={
            **os.environ,
            "deployment_base_commit": base,
            "target_commit": target,
            "previous_commit": target,
            "deploy_state_checkout": target,
            "deploy_state_target": target,
            "deploy_state_status": "success",
            "deploy_state_phase": "complete",
            "deploy_state_base": base,
            "runtime_commit": base,
            "live_update": "0",
        },
        text=True,
    )

    assert result.endswith("1:1")


def test_research_collector_stops_before_market_data_restart() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")

    assert script.index('research_stop_started_at="$(date +%s)"') < script.index(
        "deploy_phase=dashboard-market-data"
    )


def test_volume_initialization_precedes_dashboard_restart() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")

    assert script.index("deploy_phase=volume-init") < script.index(
        "# Nginx exposes the dashboard"
    )


def test_live_restart_waits_for_old_containers_before_recreate() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")

    helper_start = script.index("stop_live_services()")
    helper_end = script.index("live_up_and_wait_parallel()", helper_start)
    helper_block = script[helper_start:helper_end]
    restart_start = script.index("live_up_and_wait_parallel()")
    restart_end = script.index("collect_active_live_pairs()", restart_start)
    restart_block = script[restart_start:restart_end]

    assert "capture_live_container_ids" in helper_block
    assert "compose-stop:" in helper_block
    assert "wait_for_live_containers_stopped" in helper_block
    assert 'stop --timeout "$live_stop_timeout"' in helper_block
    assert helper_block.index("wait_for_live_containers_stopped") < helper_block.index(
        "sleep 1"
    )
    assert restart_block.index("stop_live_services") < restart_block.index(
        "up -d --force-recreate --no-deps"
    )
    assert script.count("live_up_and_wait_parallel") >= 3


def test_health_wait_detects_restart_loops() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    health_start = script.index("service_restart_info()")
    health_end = script.index("up_and_wait()", health_start)
    health_wait = script[health_start:health_end]

    assert "service_restart_info" in health_wait
    assert "RestartCount" in health_wait
    assert "State.Restarting" in health_wait
    assert "service restart loop detected" in health_wait
    assert "record_restart_baseline" in health_wait
    assert "declare -A" not in health_wait


def test_health_timeout_reports_the_pending_service() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    health_start = script.index("wait_for_services_healthy()")
    health_end = script.index("up_and_wait()", health_start)
    health_wait = script[health_start:health_end]

    assert "pending_service" in health_wait
    assert 'failure_service="${pending_service:-unknown}"' in health_wait
    assert 'failure_service="${service:-unknown}"' not in health_wait


def test_dashboard_and_market_data_share_a_start_wave_with_health_barriers() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    rank_start = script.index("phase_rank()")
    rank_end = script.index("should_run_phase()", rank_start)
    wave_start = script.index("deploy_phase=dashboard-market-data")
    consumer_start = script.index("consumer_candidates=()", wave_start)
    wave = script[wave_start:consumer_start]
    health_start = script.index("wait_for_dashboard_market_health()")
    health_end = script.index("verify_service_target()", health_start)
    health = script[health_start:health_end]

    assert "dashboard|research-stop) echo 6" in script[rank_start:rank_end]
    assert "dashboard-market-data|market-data) echo 7" in script[rank_start:rank_end]
    assert "compose-up:dashboard+market-data" in wave
    assert '"${dashboard_market_candidates[@]}"' in wave
    assert "wait_for_dashboard_market_health" in wave
    assert 'wait_for_services_healthy "$deploy_wait_timeout" dashboard' in health
    assert 'wait_for_services_healthy "$market_data_wait_timeout" market-data' in health
    assert script.index("if should_run_phase research-stop") < wave_start
    assert wave_start < consumer_start


def test_market_data_readiness_is_verified_without_a_timed_stability_gate() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")

    verification_start = script.index("verify_market_data_startup_readiness()")
    verification_end = script.index("verify_service_target()", verification_start)
    verification = script[verification_start:verification_end]
    health_wait_start = script.index("wait_for_dashboard_market_health()")
    health_wait_end = script.index(
        "verify_market_data_startup_readiness()",
        health_wait_start,
    )
    health_wait = script[health_wait_start:health_wait_end]
    live_restart_marker = (
        'if [[ "$live_update" == 1 && "$live_changed" == 1 ]]; then'
    )
    live_restart = script.index(live_restart_marker)
    canary = script.index("live_up_and_wait_canary", live_restart)

    assert 'Path("/run/cml/health/readiness")' in verification
    assert 'payload.get("service") != "market-data"' in verification
    assert 'payload.get("startup_ready") is not True' in verification
    assert "wait_for_market_state_stability" not in script
    assert "CML_MARKET_STATE_STABILITY_WAIT_TIMEOUT_SECONDS" not in script
    assert "verify_market_data_startup_readiness" in health_wait
    assert verification_start < live_restart < canary


def test_market_data_readiness_validator_embedded_python_is_valid() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    marker = """docker exec "$container_id" python -S -c '"""
    validator_start = script.index("verify_market_data_startup_readiness()")
    start = script.index(marker, validator_start) + len(marker)
    end = script.index("\n'\n}", start)

    compile(script[start:end], "<market-data-readiness-validator>", "exec")


def test_release_identity_does_not_precede_dependency_layer() -> None:
    lines = DOCKERFILE.read_text(encoding="utf-8").splitlines()
    dependency_install = next(
        index for index, line in enumerate(lines) if "pip" in line and "install" in line
    )
    release_arg = next(
        index for index, line in enumerate(lines) if line == "ARG CML_CODE_COMMIT"
    )

    assert release_arg > dependency_install


def test_sync_dashboard_option_and_ancestor_resolution(tmp_path: Path) -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    assert "--sync-dashboard" in script
    assert "CML_SYNC_DASHBOARD" in script
    assert "git merge-base --is-ancestor" in script

    # Test ancestor resolution behavior with a git repo
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)

    def git(*args: str) -> str:
        return subprocess.check_output(
            ["git", "-C", str(tmp_path), *args], text=True
        ).strip()

    git("config", "user.email", "test@example.com")
    git("config", "user.name", "Test")
    git("commit", "--allow-empty", "-qm", "ancestor")
    ancestor_commit = git("rev-parse", "HEAD")
    git("commit", "--allow-empty", "-qm", "target")
    target_commit = git("rev-parse", "HEAD")

    # Extract dashboard resolution block from update_server.sh
    start = script.index('current_dashboard_image="$(sed -n')
    end = script.index('export CML_CODE_COMMIT="$runtime_commit"', start)
    dashboard_block = script[start:end]

    # Test 1: ancestor image is automatically advanced
    env_file = tmp_path / ".env.server"
    env_file.write_text(
        f"CML_DASHBOARD_IMAGE=crypto-momentum-lab-app:{ancestor_commit}\n",
        encoding="utf-8",
    )
    cmd1 = (
        "set -eu\n"
        f"runtime_commit='{target_commit}'\n"
        f"target_commit='{target_commit}'\n"
        "previous_env_runtime_commit=''\n"
        "previous_runtime_commit=''\n"
        "dashboard_changed=1\n"
        "sync_dashboard=0\n"
        f"{dashboard_block}\n"
        'printf "%s" "$dashboard_image"'
    )
    res1 = subprocess.check_output(["bash", "-c", cmd1], cwd=tmp_path, text=True)
    assert res1.strip() == f"crypto-momentum-lab-app:{target_commit}"

    unaffected = subprocess.check_output(
        ["bash", "-c", cmd1.replace("dashboard_changed=1", "dashboard_changed=0")],
        cwd=tmp_path,
        text=True,
    )
    assert unaffected.strip() == f"crypto-momentum-lab-app:{ancestor_commit}"

    # Test 2: custom non-repo image is preserved when sync_dashboard=0
    env_file.write_text(
        "CML_DASHBOARD_IMAGE=custom-dashboard:latest\n",
        encoding="utf-8",
    )
    cmd2 = (
        "set -eu\n"
        f"runtime_commit='{target_commit}'\n"
        f"target_commit='{target_commit}'\n"
        "previous_env_runtime_commit=''\n"
        "previous_runtime_commit=''\n"
        "dashboard_changed=1\n"
        "sync_dashboard=0\n"
        f"{dashboard_block}\n"
        'printf "%s" "$dashboard_image"'
    )
    res2 = subprocess.check_output(["bash", "-c", cmd2], cwd=tmp_path, text=True)
    assert res2.strip().splitlines()[-1] == "custom-dashboard:latest"

    # Test 3: custom image is overwritten when sync_dashboard=1
    cmd3 = (
        "set -eu\n"
        f"runtime_commit='{target_commit}'\n"
        f"target_commit='{target_commit}'\n"
        "previous_env_runtime_commit=''\n"
        "previous_runtime_commit=''\n"
        "dashboard_changed=1\n"
        "sync_dashboard=1\n"
        f"{dashboard_block}\n"
        'printf "%s" "$dashboard_image"'
    )
    res3 = subprocess.check_output(["bash", "-c", cmd3], cwd=tmp_path, text=True)
    assert res3.strip().splitlines()[-1] == f"crypto-momentum-lab-app:{target_commit}"


def test_post_deploy_image_prune_is_configured() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    prune_idx = script.index("docker image prune -f")
    total_idx = script.index('echo "phase=total')
    deploy_commit_idx = script.index('echo "deployed_commit=')

    assert total_idx < prune_idx < deploy_commit_idx


def test_dashboard_only_deployment_mode_is_configured() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")

    assert "--dashboard-only" in script
    assert "--dashboard-only cannot be combined with --live" in script
    assert (
        "--dashboard-only cannot be combined with --execution-accounts-only" in script
    )

    phase_start = script.index('if [[ "$dashboard_only" == 1 ]]; then')
    phase_end = script.index(
        'echo "market_data_services=untouched strategy_services=untouched"', phase_start
    )
    block = script[phase_start:phase_end]

    assert "deploy_phase=dashboard-only" in block
    assert "compose-up:dashboard" in block
    assert "--force-recreate --no-deps dashboard" in block
    assert "set_env_value CML_DASHBOARD_IMAGE" in block
    assert (
        "CML_CODE_COMMIT" not in block
    )  # CML_CODE_COMMIT must not be updated in .env.server
    assert "exit 0" in script[phase_start : phase_end + 100]


@pytest.mark.parametrize(
    ("service", "execution_affected", "strategy_affected", "state", "expected"),
    [
        ("market-data", 0, 0, "running|healthy", 0),
        ("research-collector", 0, 0, "running|healthy", 0),
        ("dashboard", 0, 0, "running|healthy", 0),
        ("live-strategy", 0, 1, "running|healthy", 1),
        ("live-strategy", 1, 0, "running|healthy", 0),
        ("execution-account-live", 1, 0, "running|healthy", 1),
        ("execution-account-live", 0, 1, "running|healthy", 0),
        ("market-data", 1, 1, "running|healthy", 1),
        ("market-data", 0, 0, "running|unhealthy", 1),
        ("market-data", 0, 0, "exited|none", 1),
    ],
)
def test_restart_selection_preserves_unaffected_healthy_services(
    service: str,
    execution_affected: int,
    strategy_affected: int,
    state: str,
    expected: int,
) -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    start = script.index("service_requires_target_image() {")
    end = script.index("\nlog_service_health_timings()", start)
    invocation = (
        script[start:end]
        + """
compose=(compose_stub)
compose_stub() { printf container; }
test_state=$4
service_status() { printf '%s' "$test_state"; }
docker() { printf old-image; }
expected_image_for_service() { printf target-image; }
market_changed=$2
research_changed=$2
dashboard_changed=$2
live_changed=$2
execution_account_changed=$2
strategy_changed=$3
paper_changed=$2
sync_dashboard=0
dashboard_only=0
service_is_converged "$1"
"""
    )
    result = subprocess.run(
        [
            "bash",
            "-c",
            invocation,
            "test",
            service,
            str(execution_affected),
            str(strategy_affected),
            state,
        ],
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == expected, result.stderr


@pytest.mark.parametrize(
    (
        "path",
        "execution_affected",
        "strategy_affected",
        "all_services_affected",
        "destructive_schema",
    ),
    [
        ("src/crypto_momentum_lab/apps/execution_account/main.py", 1, 1, 0, 0),
        (
            "src/crypto_momentum_lab/execution_account/binance/rest_parser.py",
            1,
            1,
            0,
            0,
        ),
        ("src/crypto_momentum_lab/live_rollout/daemon.py", 0, 1, 0, 0),
        (
            "alembic/versions/20261005_0047_drop_unused_risk_state_age_limits.py",
            1,
            1,
            1,
            1,
        ),
    ],
)
def test_live_only_change_selects_only_the_affected_live_role(
    path: str,
    execution_affected: int,
    strategy_affected: int,
    all_services_affected: int,
    destructive_schema: int,
) -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    start = script.index("runtime_changed=0\n")
    end = script.index("\n# If the previous attempt", start)
    invocation = (
        "deployment_base_commit=base; target_commit=target; test_path=$1\n"
        'git() { printf "%s" "$test_path"; }\n'
        + script[start:end]
        + '\nprintf "%s" "$runtime_changed|$live_changed|$execution_account_changed|$strategy_changed|$market_changed|$research_changed|$dashboard_changed|$paper_changed|$destructive_schema_changed"\n'
    )
    result = subprocess.check_output(
        ["bash", "-c", invocation, "test", path], text=True
    )
    assert (
        result
        == f"1|1|{execution_affected}|{strategy_affected}|{all_services_affected}|{all_services_affected}|{all_services_affected}|{all_services_affected}|{destructive_schema}"
    )


def test_live_rollout_uses_a_single_canary_before_parallel_batches() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")

    assert "live_up_and_wait_canary()" in script
    assert '"$live_canary_concurrency" "$1"' in script
    assert '"$live_wait_timeout" "$live_concurrency"' in script


def test_destructive_live_schema_migrations_are_blocked_from_registry() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    registry = (ROOT / "deploy/ops/destructive_migrations.txt").read_text(
        encoding="utf-8"
    )

    assert "grep -Fxq" in script
    assert "destructive_schema_changed" in script
    assert "Refusing Live deployment" in script
    assert "20261005_0047_drop_unused_risk_state_age_limits.py" in registry
