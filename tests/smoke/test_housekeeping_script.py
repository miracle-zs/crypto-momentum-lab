import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def test_housekeeping_only_prunes_host_operational_artifacts() -> None:
    script = (ROOT / "deploy/ops/cml_housekeeping.sh").read_text(encoding="utf-8")

    assert "crash-logs" in script
    assert "-xdev" in script
    assert "table-archive" not in script
    assert "postgres-data" not in script
    assert "docker ps -aq" in script
    assert "--no-trunc" in script
    assert "app_image_retention_count" in script


def test_housekeeping_timer_is_daily_and_persistent() -> None:
    timer = (ROOT / "deploy/ops/cml-housekeeping.timer").read_text(encoding="utf-8")

    assert "Asia/Shanghai" in timer
    assert "Persistent=true" in timer
    assert "WantedBy=timers.target" in timer


@pytest.mark.parametrize("probe_failure", [None, "ps", "inspect"])
def test_housekeeping_preserves_stopped_container_images(tmp_path, probe_failure):
    binaries = tmp_path / "bin"
    binaries.mkdir()
    calls = tmp_path / "calls"
    docker = binaries / "docker"
    docker.write_text(
        "#!/bin/bash\n"
        'echo "$*" >> "$CML_TEST_CALLS"\n'
        'if [[ "$CML_TEST_FAILURE" == "$1" ]]; then exit 2; fi\n'
        'case "$*" in\n'
        '"ps -q") ;;\n'
        '"ps -aq") echo stopped-container ;;\n'
        "inspect*) echo sha256:stopped ;;\n"
        '"image ls "*) printf "sha256:new\\nsha256:stopped\\nsha256:unused\\n" ;;\n'
        '"image inspect "*"sha256:stopped") echo crypto-momentum-lab-app:protected ;;\n'
        '"image inspect "*) echo crypto-momentum-lab-app:unused ;;\n'
        '"image rm crypto-momentum-lab-app:protected") exit 1 ;;\n'
        "esac\n"
    )
    docker.chmod(0o755)
    journal = binaries / "journalctl"
    journal.write_text("#!/bin/bash\nexit 0\n")
    journal.chmod(0o755)
    settings = {
        "PATH": str(binaries) + ":/usr/local/bin:/usr/bin:/bin",
        "CML_TEST_CALLS": str(calls),
        "CML_APP_IMAGE_RETENTION_COUNT": "1",
        "CML_CRASH_LOG_DIRECTORY": str(tmp_path / "missing"),
        "CML_TEST_FAILURE": probe_failure or "",
    }
    script = str(ROOT / "deploy/ops/cml_housekeeping.sh")
    version = subprocess.check_output(
        ["bash", "-c", "echo ${BASH_VERSINFO[0]}"], text=True
    )
    command = ["bash", script]
    if int(version) < 4:
        image = os.environ.get("CML_HOUSEKEEPING_TEST_IMAGE")
        docker_cli = shutil.which("docker")
        if not image or not docker_cli:
            pytest.skip(
                "needs Bash 4+; set CML_HOUSEKEEPING_TEST_IMAGE for Linux validation"
            )
        command = [
            docker_cli,
            "run",
            "--rm",
            "--network",
            "none",
            "--user",
            "0:0",
            "--entrypoint",
            "bash",
            "--volume",
            f"{ROOT}:{ROOT}:ro",
            "--volume",
            f"{tmp_path}:{tmp_path}",
        ]
        for key, value in settings.items():
            command.extend(["--env", key + "=" + value])
        command.extend([image, script])
    result = subprocess.run(
        command,
        env={**os.environ, **settings},
        capture_output=True,
        text=True,
    )
    if probe_failure:
        assert result.returncode != 0
        assert "image rm" not in calls.read_text()
        assert "image prune" not in calls.read_text()
        return
    assert result.returncode == 0, result.stderr
    assert "ps -aq" in calls.read_text()
    assert "image rm crypto-momentum-lab-app:protected" not in calls.read_text()
    assert "image rm crypto-momentum-lab-app:unused" in calls.read_text()
