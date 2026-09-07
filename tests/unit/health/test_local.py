from crypto_momentum_lab.health import LocalHealthWriter


def test_local_health_writer_resets_and_publishes_markers(tmp_path) -> None:
    health = LocalHealthWriter.for_directory(tmp_path / "health")

    assert health.status_path.read_text() == "starting\n"
    assert not health.database_path.exists()

    health.heartbeat(database_ok=True)

    assert health.status_path.read_text() == "ready\n"
    assert health.database_path.read_text() == "ok\n"

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
    assert first.status_path.read_text() == "starting\n"
