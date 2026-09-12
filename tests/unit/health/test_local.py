import json

from crypto_momentum_lab.health import LocalHealthWriter


def test_local_health_writer_resets_and_publishes_markers(tmp_path) -> None:
    health = LocalHealthWriter.for_directory(tmp_path / "health")

    assert health.status_path.read_text() == "starting\n"
    assert not health.database_path.exists()
    assert not health.readiness_path.exists()

    health.heartbeat(database_ok=True)

    assert health.status_path.read_text() == "ready\n"
    assert health.database_path.read_text() == "ok\n"

    health.write_readiness(
        {"entry_enabled": False, "warmup_complete_symbols": 0}
    )

    assert json.loads(health.readiness_path.read_text()) == {
        "entry_enabled": False,
        "warmup_complete_symbols": 0,
    }

    health.degraded()
    assert health.status_path.read_text() == "degraded\n"

    health.stopped()
    assert health.status_path.read_text() == "stopped\n"


def test_local_health_writer_removes_stale_database_marker(tmp_path) -> None:
    directory = tmp_path / "health"
    first = LocalHealthWriter.for_directory(directory)
    first.database_ok()

    LocalHealthWriter.for_directory(directory)

    assert not first.database_path.exists()
    assert not first.readiness_path.exists()
    assert first.status_path.read_text() == "starting\n"
