"""With `-v ON_ERROR_STOP=1`, statements must be terminated before the marker."""

from deploy.ops.archive_and_trim import (
    PsqlSession,
    _terminated,
    build_batch_delete_sql,
    build_freeze_targets_sql,
    build_frozen_content_digest_sql,
)
from deploy.ops.stream_digest import StreamingContentDigest


def test_terminated_adds_a_missing_semicolon() -> None:
    assert _terminated("SELECT 1") == "SELECT 1;"
    assert _terminated("SELECT 1;") == "SELECT 1;"
    assert _terminated("SELECT 1;\n") == "SELECT 1;"
    assert _terminated("  SELECT 1  ") == "  SELECT 1;"


def test_statements_built_for_the_session_are_terminated() -> None:
    for sql in (
        build_freeze_targets_sql("strategy_runtime_events", "occurred_at", "a", "b"),
        build_frozen_content_digest_sql("strategy_runtime_events"),
        build_batch_delete_sql("strategy_runtime_events", 1000),
    ):
        assert sql.rstrip().endswith(";")


def test_run_terminates_the_statement_it_writes_to_the_session() -> None:
    class FakeStdin:
        def __init__(self) -> None:
            self.written: list[str] = []

        def write(self, text: str) -> None:
            self.written.append(text)

        def flush(self) -> None:
            return None

    class FakeProcess:
        def __init__(self) -> None:
            self.stdin = FakeStdin()
            self.stdout = iter(["__cml_done_1__\n"])
            self.stderr = None

        def poll(self) -> None:
            return None

    session = PsqlSession(container="c", database="d", user="u")
    session._proc = FakeProcess()

    assert session.run("SELECT 1") == ""
    assert session._proc.stdin.written == [
        "SELECT 1;\n",
        "SELECT '__cml_done_1__';\n",
    ]


def test_stream_digest_folds_row_hashes_without_retaining_the_result_set() -> None:
    class FakeStdin:
        def write(self, text: str) -> None:
            del text

        def flush(self) -> None:
            return None

    class FakeProcess:
        def __init__(self) -> None:
            self.stdin = FakeStdin()
            self.stdout = iter(["row-hash-a\n", "row-hash-b\n", "__cml_done_1__\n"])
            self.stderr = None

        def poll(self) -> None:
            return None

    session = PsqlSession(container="c", database="d", user="u")
    session._proc = FakeProcess()

    expected = StreamingContentDigest()
    expected.update(b"row-hash-a\n")
    expected.update(b"row-hash-b\n")
    assert session.stream_digest("SELECT row_hash") == expected.hexdigest()
