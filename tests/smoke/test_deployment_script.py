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
    assert "CML_LIVE_STOP_TIMEOUT_SECONDS" in script
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


def test_deployment_script_reports_service_level_timings() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")

    assert "service-timing" in script
    assert "log_service_health_timings" in script
    assert 'log_service_timings "restart"' in script
    assert 'log_service_timings "graceful-stop"' in script
    assert "verify_service_target_timed" in script
    assert 'log_service_timing "health-wait"' in script
    assert 'log_service_timing "verify"' in script


def test_live_readiness_validator_embedded_python_is_valid() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    marker = "docker exec \"$container_id\" python -S -c '"
    start = script.index(marker) + len(marker)
    end = script.index("' \"$runtime_commit\"", start)

    compile(script[start:end], "<live-readiness-validator>", "exec")
    assert "verify_live_readiness" in script
    assert "live-readiness" in script
    assert "/run/cml/health/readiness" in script
    assert "warmup symbol counts do not reconcile" in script
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
        "for argument in \"$@\"; do\n"
        "  case \"$argument\" in\n"
        "    label=com.docker.compose.service=*) service=\"${argument##*=}\" ;;\n"
        "  esac\n"
        "done\n"
        "case \",${RUNNING_EXTRA_SERVICES:-},\" in\n"
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
         '\nprintf "%s" "$runtime_changed:$schema_changed:'
         '$dashboard_changed:$market_changed:$paper_changed:$live_changed"'],
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
    assert result.endswith("1:0:1:0:0:0")


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
    assert 'run --rm --no-deps migrate' in migration
    assert 'phase=migrate skipped schema_changed=$schema_changed' in migration


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


def test_live_recovery_always_revalidates_preflight() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    active_pairs_start = script.index("active_pairs=()")
    active_pairs_end = script.index("# market-data discovers", active_pairs_start)
    active_pairs = script[active_pairs_start:active_pairs_end]

    assert "should_run_phase live-preflight" not in script
    assert (
        '$(phase_rank "$resume_from_phase") > $(phase_rank live-preflight)'
        not in script
    )
    assert '"$recovery_run" != 1 && "$refresh_approvals" != 1' in active_pairs
    assert script.index("consumer_candidates=()") < script.index(
        "deploy_phase=live-preflight"
    )


def test_live_generation_fence_order_is_migration_preflight_restart() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")

    migration_phase = script.index("deploy_phase=migrate")
    preflight_phase = script.index("deploy_phase=live-preflight")
    dashboard_phase = script.index("deploy_phase=dashboard-market-data")
    live_restart = script.index("deploy_phase=live-restart")

    consumers_phase = script.index("consumer_candidates=()")

    assert migration_phase < dashboard_phase < consumers_phase < preflight_phase
    assert preflight_phase < live_restart
    preflight_block = script[preflight_phase:live_restart]
    assert "run --rm --no-deps migrate" in script[migration_phase:dashboard_phase]
    assert "run_parallel_pairs preflight" in preflight_block
    assert "run_parallel_pairs renew" in preflight_block
    assert preflight_block.index(
        "run_parallel_pairs preflight"
    ) < preflight_block.index("run_parallel_pairs renew")


def test_live_approval_precheck_precedes_non_live_restart() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")

    precheck = script.index("deploy_phase=live-approval-precheck")
    research_stop = script.index("deploy_phase=research-stop")
    dashboard_wave = script.index("deploy_phase=dashboard-market-data")
    consumers = script.index("consumer_candidates=()")
    final_preflight = script.index("deploy_phase=live-preflight")
    precheck_block = script[precheck:research_stop]

    assert "should_run_phase live-approval-precheck" in precheck_block
    assert "run_parallel_pairs approval-precheck" in precheck_block
    assert "run_parallel_pairs preflight" not in precheck_block
    assert "refresh_approvals=1" in precheck_block
    assert precheck < research_stop < dashboard_wave < consumers < final_preflight


def test_live_restart_waits_for_old_containers_before_recreate() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")

    helper_start = script.index("stop_live_services()")
    helper_end = script.index("run_parallel_pairs()", helper_start)
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
        'up -d --force-recreate --no-deps'
    )
    assert script.count("live_up_and_wait_parallel") >= 3


def test_live_preflight_has_no_side_effects_before_validation() -> None:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")

    preflight_phase = script.index("deploy_phase=live-preflight")
    consumers_phase = script.index("consumer_candidates=()")
    preflight_start = script.index("preflight_started_at=")
    lease_start = script.index("lease_started_at=")
    env_commit_write = script.index(
        'set_env_value CML_CODE_COMMIT "$runtime_commit"'
    )
    verify_elapsed = script.index("phase=verify elapsed_seconds=")

    assert consumers_phase < preflight_phase
    assert preflight_start < lease_start
    assert verify_elapsed < env_commit_write


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
