from crypto_momentum_lab.health import LocalHealthWriter


def test_local_health_writer_resets_and_publishes_markers(tmp_path) -> None:
    directory = tmp_path / "health"
    health = LocalHealthWriter(directory)

    assert (directory / "status").read_text() == "starting\n"
    assert not (directory / "database").exists()

    health.heartbeat(database_ok=True)

    assert (directory / "status").read_text() == "ready\n"
    assert (directory / "database").read_text() == "ok\n"

    health.stopped()
    assert (directory / "status").read_text() == "stopped\n"


def test_local_health_writer_removes_stale_database_marker(tmp_path) -> None:
    directory = tmp_path / "health"
    first = LocalHealthWriter(directory)
    first.database_ok()

    LocalHealthWriter(directory)

    assert not (directory / "database").exists()
    assert (directory / "status").read_text() == "starting\n"
