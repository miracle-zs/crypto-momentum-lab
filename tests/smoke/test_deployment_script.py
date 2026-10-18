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
    account_check = script.index("client.fetch_positions()")
    account_orders = script.index("client.fetch_open_orders()")
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
    assert "all_flat=0" in script
    assert "sleep 30" in script


def test_execution_account_containers_stop_in_parallel_with_app_services() -> None:
    script = remote_script()

    assert 'execution_account_containers+=("$container")' in script
    assert 'docker stop --time 90 "$container" >/dev/null &' in script
    assert 'execution_stop_pids+=("$!")' in script
    assert '"${compose[@]}" stop --timeout 90 "${running_services[@]}" &' in script
    assert 'wait "$stop_pid"' in script


def test_target_image_build_overlaps_account_flat_checks() -> None:
    script = remote_script()

    assert 'git worktree add --detach "$build_dir" "$target_commit"' in script
    assert '--project-directory "$build_dir"' in script
    assert 'CML_CODE_COMMIT="$target_commit"' in script
    assert 'docker image inspect "$target_image"' in script
    assert '"${build_compose[@]}" build migrate &' in script
    assert "check_target_build" in script
    assert "finish_target_build\n\n# This timer" in script
    assert (
        script.index("finish_target_build")
        < script.index("systemctl disable --now cml-position-recovery-retention.timer")
        < script.index("running_services=()")
    )
    timer_shutdown = script[
        script.index("# This timer") : script.index("\nrunning_services=()")
    ]
    assert "|| true" not in timer_shutdown
    assert "trap cleanup_build_worktree EXIT" in script


def test_all_four_accounts_are_checked_even_if_a_strategy_is_stopped() -> None:
    script = remote_script()

    assert "live_running=" not in script
    assert (
        'query=(docker exec "$container" python -c "$flat_check_python" "$account")'
        in script
    )
    assert "run --rm --no-deps --entrypoint python" in script
    assert 'for item in "${accounts[@]}"; do' in script


def test_deployment_maps_running_accounts_to_selected_engine() -> None:
    script = remote_script()

    assert 'running_services+=("$service")' in script
    assert 'services+=("$target_service")' in script
    assert '"${compose[@]}" stop --timeout 90 "${running_services[@]}"' in script
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


def test_engine_selection_is_forwarded_and_persisted():
    full_script = DEPLOY_SCRIPT.read_text()
    script = remote_script()
    assert '"${CML_TRADING_ENGINE:-}" <<' in full_script
    assert 'requested_engine="$3"' in script
    assert 'set_env_value CML_TRADING_ENGINE "$trading_engine"' in script
    assert "-f compose.live.accounts.yaml -f compose.next_gen.yaml" in script
    assert script.index("Deployment stopped: account=$account") > script.index(
        'wait "$stop_pid"'
    )


@pytest.mark.parametrize("engine", ["legacy", "nextgen"])
def test_engine_switch_executes_correct_stop_and_start_lists(tmp_path, engine):
    import json
    import os
    import sys

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "git-meta").mkdir()
    (checkout / ".env.server").write_text("CML_CODE_COMMIT=release\n")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    log = tmp_path / "commands.jsonl"
    stopped = tmp_path / "stopped"
    source_services = (
        [
            "live-strategy",
            "live-strategy-account-2",
            "live-strategy-account-3",
            "live-strategy-account-4",
        ]
        if engine == "nextgen"
        else [
            "nextgen-strategy-primary",
            "nextgen-strategy-account-2",
            "nextgen-strategy-account-3",
            "nextgen-strategy-account-4",
        ]
    )
    stub = fake_bin / "stub"
    stub.write_text(
        "#!"
        + sys.executable
        + "\n"
        + r"""
import json, os, pathlib, re, shutil, sys
name, args = pathlib.Path(sys.argv[0]).name, sys.argv[1:]
with open(os.environ["CML_TEST_COMMAND_LOG"], "a") as output:
    output.write(json.dumps([name, *args]) + "\n")
if name == "git":
    if args[:2] == ["rev-parse", "--git-dir"]: print("git-meta")
    elif args[:2] == ["branch", "--show-current"]: print("main")
    elif args and args[0] == "rev-parse": print("release")
    elif args[:2] == ["worktree", "add"]:
        path = pathlib.Path(args[-2]); path.mkdir(); (path / ".git").touch()
    elif args[:2] == ["worktree", "remove"]: shutil.rmtree(args[-1])
elif name == "docker":
    if args[0] == "ps":
        prefix = "label=com.docker.compose.service="
        service = next(
            value[len(prefix):] for value in args if value.startswith(prefix)
        )
        if service in json.loads(os.environ["CML_TEST_RUNNING_SERVICES"]):
            print("id-" + service)
    elif args[0] == "exec": print('{"positions":[],"orders":[]}')
    elif args[0] == "inspect":
        image = "crypto-momentum-lab-app:release"
        print(image if "Config.Image" in args[2] else "running|healthy")
    elif args[0] == "compose":
        if "stop" in args: pathlib.Path(os.environ["CML_TEST_STOPPED_MARKER"]).touch()
        elif "ps" in args and "-q" in args: print("id-" + args[-1])
        elif "run" in args: print('{"positions":[],"orders":[]}')
elif name == "sed":
    assert args[0] == "-i"
    _, pattern, replacement, _ = args[1].split("#")
    path = pathlib.Path(args[-1])
    path.write_text(re.sub(pattern, replacement, path.read_text(), flags=re.MULTILINE))
elif name == "systemctl": sys.exit(1)
"""
    )
    stub.chmod(0o755)
    for name in ("git", "docker", "flock", "curl", "systemctl", "sed"):
        (fake_bin / name).symlink_to(stub)
    environment = dict(
        os.environ,
        PATH=str(fake_bin) + os.pathsep + os.environ["PATH"],
        CML_TEST_COMMAND_LOG=str(log),
        CML_TEST_STOPPED_MARKER=str(stopped),
        CML_TEST_RUNNING_SERVICES=json.dumps(
            [*source_services, "dashboard", "market-data"]
        ),
    )
    result = subprocess.run(
        ["bash", "-s", "--", str(checkout), "origin/main", engine],
        input=remote_script(),
        text=True,
        capture_output=True,
        env=environment,
        timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    stop = next(
        call for call in calls if call[:2] == ["docker", "compose"] and "stop" in call
    )
    start = next(
        call
        for call in calls
        if call[:2] == ["docker", "compose"] and "--force-recreate" in call
    )
    assert set(source_services) <= set(stop)
    targets = (
        [
            "nextgen-strategy-primary",
            "nextgen-strategy-account-2",
            "nextgen-strategy-account-3",
            "nextgen-strategy-account-4",
        ]
        if engine == "nextgen"
        else [
            "live-strategy",
            "live-strategy-account-2",
            "live-strategy-account-3",
            "live-strategy-account-4",
        ]
    )
    assert set(targets) <= set(start)
    assert not set(source_services) & set(start)
    assert f"CML_TRADING_ENGINE={engine}" in (checkout / ".env.server").read_text()
    stop_index = calls.index(stop)
    assert sum("run" in call for call in calls[stop_index + 1 :]) == 4
