import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
DEPLOY_SCRIPT = ROOT / "deploy/ops/update_server.sh"


def remote_script() -> str:
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    marker = "<<'REMOTE_SCRIPT'; then\n"
    start = script.index(marker) + len(marker)
    end = script.index("\nREMOTE_SCRIPT\n  elapsed_seconds=", start)
    return script[start:end]


def test_deployment_script_and_remote_script_are_valid_shell() -> None:
    subprocess.run(["bash", "-n", str(DEPLOY_SCRIPT)], check=True)
    subprocess.run(["bash", "-n"], input=remote_script().encode(), check=True)


def test_normal_deployment_checks_live_accounts_before_stopping_services() -> None:
    script = remote_script()
    build = script.index('"${build_compose[@]}" build migrate &')
    account_check = script.index('client.fetch_positions()')
    account_orders = script.index('client.fetch_open_orders()')
    stop_services = script.index('"${compose[@]}" stop --timeout 90')
    update_code = script.index('git merge --ff-only "$target_commit"')
    start_database = script.index('"${compose[@]}" up -d postgres')
    start_services = script.index('"${compose[@]}" up -d --force-recreate')

    assert (
        build
        < account_check
        < stop_services
        < update_code
        < start_database
        < start_services
    )
    assert account_orders < stop_services
    for account, service in (
        ("primary", "live-strategy"),
        ("account-2", "live-strategy-account-2"),
        ("account-3", "live-strategy-account-3"),
        ("account-4", "live-strategy-account-4"),
    ):
        assert f'"{account}:{service}"' in script


def test_non_flat_or_unreadable_accounts_wait_and_retry() -> None:
    script = remote_script()

    assert "return 0 if not open_positions and not open_orders else 3" in script
    assert "return 4" in script
    assert "if (( status != 3 && status != 4 )); then" in script
    assert 'all_flat=0' in script
    assert "sleep 30" in script


def test_execution_account_containers_stop_in_parallel_with_app_services() -> None:
    script = remote_script()

    assert 'execution_account_containers+=("$container")' in script
    assert 'docker stop --time 90 "$container" >/dev/null &' in script
    assert 'execution_stop_pids+=("$!")' in script
    assert '"${compose[@]}" stop --timeout 90 "${services[@]}" &' in script
    assert 'wait "$stop_pid"' in script


def test_target_image_build_overlaps_account_flat_checks() -> None:
    script = remote_script()

    assert 'git worktree add --detach "$build_dir" "$target_commit"' in script
    assert '--project-directory "$build_dir"' in script
    assert 'CML_CODE_COMMIT="$target_commit"' in script
    assert 'docker image inspect "$target_image"' in script
    assert '"${build_compose[@]}" build migrate &' in script
    assert 'check_target_build' in script
    assert 'finish_target_build\n\nservices=()' in script
    assert 'trap cleanup_build_worktree EXIT' in script


def test_all_four_accounts_are_checked_even_if_a_strategy_is_stopped() -> None:
    script = remote_script()

    assert "live_running=" not in script
    assert (
        'query=(docker exec "$container" python -c "$flat_check_python" "$account")'
        in script
    )
    assert 'run --rm --no-deps --entrypoint python' in script
    assert "for item in \"${accounts[@]}\"; do" in script


def test_deployment_restarts_only_services_that_were_running() -> None:
    script = remote_script()

    assert "services+=(\"$service\")" in script
    assert '"${compose[@]}" stop --timeout 90 "${services[@]}"' in script
    assert '"${compose[@]}" up -d --force-recreate "${services[@]}"' in script
    assert "docker image prune" not in script


def test_embedded_account_reader_is_valid_python() -> None:
    script = remote_script()
    marker = "flat_check_python=$(cat <<'PY'\n"
    start = script.index(marker) + len(marker)
    end = script.index("\nPY\n)", start)

    compile(script[start:end], "<exchange-account-reader>", "exec")


@pytest.mark.parametrize(
    "argument", ["--live", "--dashboard-only", "--execution-accounts-only"]
)
def test_deployment_has_one_normal_entrypoint(argument: str) -> None:
    result = subprocess.run(
        ["bash", str(DEPLOY_SCRIPT), "local", argument],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 64
    assert "Usage:" in result.stderr
