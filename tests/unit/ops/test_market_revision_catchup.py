import argparse
import fcntl
import importlib.util
from pathlib import Path

import pytest

_SCRIPT = (
    Path(__file__).resolve().parents[3] / "deploy/ops/catch_up_market_revisions.py"
)


@pytest.fixture
def script():
    spec = importlib.util.spec_from_file_location("revision_catchup", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _healthy():
    return {
        "unhealthy": [],
        "other_alerts": [],
        "lock_waiters": 0,
        "old_transactions": 0,
        "free_bytes": 25 * 1024**3,
        "n_dead_tup": 0,
        "load_per_cpu": 0.3,
        "memory_full_pct": 0,
        "io_full_pct": 0,
        "oldest_pending": "2026-09-26T00:00:00+00:00",
    }


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("unhealthy", ["strategy:unhealthy"], "unhealthy"),
        ("other_alerts", ["live_market_state_delay"], "other_alerts"),
        ("lock_waiters", 1, "lock_waiters"),
        ("old_transactions", 1, "old_transactions"),
        ("free_bytes", 7 * 1024**3, "low_disk_space"),
        ("n_dead_tup", 100000, "waiting_for_autovacuum"),
        ("load_per_cpu", 0.85, "high_host_load"),
        ("memory_full_pct", 1, "memory_pressure"),
        ("io_full_pct", 5, "io_pressure"),
    ],
)
def test_pressure_gate_blocks_each_unsafe_condition(script, field, value, reason):
    state = _healthy()
    assert (
        script.blockers(
            state,
            minimum_free_bytes=8 * 1024**3,
            maximum_dead_rows=100000,
            maximum_load=0.85,
        )
        == []
    )
    state[field] = value
    assert script.blockers(
        state,
        minimum_free_bytes=8 * 1024**3,
        maximum_dead_rows=100000,
        maximum_load=0.85,
    ) == [reason]


def _setup(script, monkeypatch, tmp_path, *, apply=True):
    (tmp_path / ".git").mkdir()
    (tmp_path / ".env.server").touch()
    monkeypatch.setattr(script, "critical_containers", lambda: ["test-container"])
    monkeypatch.setattr(script, "emit", lambda *args, **kwargs: None)
    return argparse.Namespace(
        project_directory=tmp_path,
        apply=apply,
        max_runtime_seconds=30,
        pause_seconds=1,
        minimum_free_gib=8,
        maximum_dead_rows=100000,
        maximum_load=0.85,
        chunks=20,
        max_rounds=2,
    )


def test_dry_run_never_archives(script, monkeypatch, tmp_path):
    args = _setup(script, monkeypatch, tmp_path, apply=False)
    monkeypatch.setattr(script, "snapshot", lambda *args: _healthy())
    monkeypatch.setattr(script, "archive_round", lambda *args, **kwargs: pytest.fail())
    assert script.run(args) == 0


def test_probe_failure_fails_closed_in_dry_run(script, monkeypatch, tmp_path):
    args = _setup(script, monkeypatch, tmp_path, apply=False)

    def unavailable(*args):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(script, "snapshot", unavailable)
    monkeypatch.setattr(script, "archive_round", lambda *args, **kwargs: pytest.fail())
    assert script.run(args) == 1


def test_deployment_lock_prevents_any_archive(script, monkeypatch, tmp_path):
    args = _setup(script, monkeypatch, tmp_path, apply=False)
    monkeypatch.setattr(script, "snapshot", lambda *args: pytest.fail())
    with (tmp_path / ".git/cml-deploy.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert script.run(args) == 0


def test_rounds_release_deployment_lock_and_stop_when_caught_up(
    script,
    monkeypatch,
    tmp_path,
):
    args = _setup(script, monkeypatch, tmp_path)
    pending = _healthy()
    completed = {**pending, "oldest_pending": None}
    snapshots = iter([pending, completed])
    monkeypatch.setattr(script, "snapshot", lambda *args: next(snapshots))
    calls = []

    def archive(*args, **kwargs):
        calls.append(kwargs)
        with (tmp_path / ".git/cml-deploy.lock").open("a") as lock:
            with pytest.raises(BlockingIOError):
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return {"archived_rows": 100, "archived_chunks": 1}

    def cooldown(*args):
        with (tmp_path / ".git/cml-deploy.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

    monkeypatch.setattr(script, "archive_round", archive)
    monkeypatch.setattr(script, "pause", cooldown)
    assert script.run(args) == 0
    assert len(calls) == 1


def test_pressure_pauses_until_safe(script, monkeypatch, tmp_path):
    args = _setup(script, monkeypatch, tmp_path)
    args.max_rounds = 1
    snapshots = iter([{**_healthy(), "n_dead_tup": 150000}, _healthy()])
    monkeypatch.setattr(script, "snapshot", lambda *args: next(snapshots))
    pauses = []
    calls = []
    monkeypatch.setattr(script, "pause", lambda *args: pauses.append(args))

    def archive(*args, **kwargs):
        calls.append(kwargs)
        return {"archived_rows": 100, "archived_chunks": 1}

    monkeypatch.setattr(script, "archive_round", archive)
    assert script.run(args) == 0
    assert len(pauses) == len(calls) == 1


def test_no_progress_with_pending_data_is_not_completion(script, monkeypatch, tmp_path):
    args = _setup(script, monkeypatch, tmp_path)
    monkeypatch.setattr(script, "snapshot", lambda *args: _healthy())
    monkeypatch.setattr(
        script,
        "archive_round",
        lambda *args, **kwargs: {
            "archived_rows": 0,
            "archived_chunks": 0,
        },
    )
    with pytest.raises(RuntimeError, match="no progress"):
        script.run(args)
