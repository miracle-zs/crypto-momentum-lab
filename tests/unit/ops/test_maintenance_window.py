"""Tests for the deploy/monitor maintenance-window handshake."""

import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from deploy.ops.cml_ops_monitor import _is_maintenance_noise
from deploy.ops.maintenance_window import (
    clear_maintenance_window,
    default_maintenance_path,
    is_maintenance_active,
    read_maintenance_window,
    write_maintenance_window,
)

_START = datetime(2026, 9, 14, 2, 58, tzinfo=UTC)


def test_window_round_trips_and_expires(tmp_path: Path) -> None:
    """A window is active until it expires, then it is not."""

    path = tmp_path / "maintenance.json"
    write_maintenance_window(
        path,
        started_at=_START,
        expected_seconds=900.0,
        reason="deploy",
    )

    window = read_maintenance_window(path)
    assert window is not None
    assert window.reason == "deploy"

    assert is_maintenance_active(path, now=_START + timedelta(seconds=60))
    assert not is_maintenance_active(path, now=_START + timedelta(seconds=901))


def test_window_requires_positive_ttl(tmp_path: Path) -> None:
    """A window that never expires would silence the monitor forever."""

    path = tmp_path / "maintenance.json"
    try:
        write_maintenance_window(path, started_at=_START, expected_seconds=0)
    except ValueError:
        pass
    else:  # pragma: no cover - explicit failure path
        raise AssertionError("expected ValueError for a non-positive TTL")

    assert not path.exists()


def test_missing_or_corrupt_window_fails_open(tmp_path: Path) -> None:
    """Unreadable state must alert, not stay silent."""

    path = tmp_path / "maintenance.json"
    assert read_maintenance_window(path) is None
    assert not is_maintenance_active(path, now=_START)

    path.write_text("{not json", encoding="utf-8")
    assert read_maintenance_window(path) is None
    assert not is_maintenance_active(path, now=_START)

    # Structurally wrong payloads are treated the same way.
    path.write_text('{"started_at": "not-a-time", "expected_seconds": 60}')
    assert read_maintenance_window(path) is None

    path.write_text('{"started_at": "2026-09-14T02:58:00", "expected_seconds": 60}')
    assert read_maintenance_window(path) is None, "naive timestamps are rejected"


def test_clearing_a_window_is_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "maintenance.json"
    write_maintenance_window(path, started_at=_START, expected_seconds=60.0)
    clear_maintenance_window(path)
    assert not path.exists()
    clear_maintenance_window(path)  # a deploy may never have declared one


def test_only_lifecycle_alerts_are_silenced() -> None:
    """Churn alerts go quiet during a deploy; memory and DB alerts do not."""

    # Lifecycle churn: exactly the chain one deploy produced on account-2.
    assert _is_maintenance_noise("container_unhealthy")
    assert _is_maintenance_noise("container_missing")
    assert _is_maintenance_noise("live_heartbeat_stale:account-2")
    assert _is_maintenance_noise("live_heartbeat_auto_restarted:primary")
    assert _is_maintenance_noise("live_crash_log_archive_failed:account-2")

    # A deploy can genuinely cause these, so they keep firing.
    assert not _is_maintenance_noise("container_memory_high")
    assert not _is_maintenance_noise("container_memory_growth")
    assert not _is_maintenance_noise("container_memory_pressure")
    assert not _is_maintenance_noise("database_connection_pressure")


def test_default_path_honours_the_override(monkeypatch) -> None:
    """The deploy script and the monitor must agree on the marker's location."""

    monkeypatch.setenv("CML_MAINTENANCE_WINDOW_FILE", "/tmp/probe-window.json")
    assert default_maintenance_path() == Path("/tmp/probe-window.json")

    monkeypatch.delenv("CML_MAINTENANCE_WINDOW_FILE")
    assert default_maintenance_path() == Path(
        "/var/lib/crypto-momentum-lab/maintenance.json"
    )


def test_monitor_still_starts_when_run_as_a_script() -> None:
    """The systemd unit runs the monitor as a script, not as a package.

    Importing a sibling package from a script only works if the repository root
    is restored to sys.path.  Getting this wrong crash-loops the monitor and
    silently disables every alert, so it is worth a real subprocess.
    """

    repo_root = Path(__file__).resolve().parents[3]
    script = repo_root / "deploy" / "ops" / "cml_ops_monitor.py"

    completed = subprocess.run(
        [sys.executable, str(script), "--help"],
        capture_output=True,
        text=True,
        cwd="/",  # not the repo root: exactly what systemd does
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "usage:" in completed.stdout
    assert "ModuleNotFoundError" not in completed.stderr
