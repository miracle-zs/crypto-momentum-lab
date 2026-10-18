"""The archive manifest fingerprint must stay bounded for large retention windows."""

from __future__ import annotations

import csv
import importlib.util
import io
from pathlib import Path

from deploy.ops.stream_digest import StreamingContentDigest

_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT = _ROOT / "deploy" / "ops" / "archive_table.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("archive_table", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_range_fingerprint_streams_rows_without_database_sort_or_aggregate(
    monkeypatch,
) -> None:
    mod = _load_module()
    commands: list[list[str]] = []
    rows = ['{"id":1,"path":"a\\\\b"}', '{"id":2,"value":"' + "x" * 140000 + '"}']
    encoded = io.StringIO(newline="")
    writer = csv.writer(encoded)
    writer.writerows([[row] for row in rows])

    class FakeProcess:
        def __init__(self) -> None:
            self.stdout = io.BytesIO(encoded.getvalue().encode())
            self.stderr = io.BytesIO()
            self.returncode = 0

        def wait(self) -> None:
            return None

    def fake_popen(args: list[str], **kwargs: object) -> FakeProcess:
        del kwargs
        commands.append(args)
        return FakeProcess()

    monkeypatch.setattr(mod.subprocess, "Popen", fake_popen)
    digest = mod.compute_range_fingerprint(
        [],
        container="postgres",
        user="cml",
        database="cml",
        table="demo_table",
        column="occurred_at",
        start="2026-09-01",
        end="2026-09-02",
    )

    expected = StreamingContentDigest()
    for row in rows:
        expected.update(row.encode() + b"\n")
    assert digest == expected.hexdigest()
    sql = commands[0][-1]
    assert sql.startswith("COPY (")
    assert "TO STDOUT WITH (FORMAT csv)" in sql
    assert "PGOPTIONS=-c work_mem=4MB -c temp_file_limit=256MB" in commands[0]
    assert "row_to_json(t)::text" in sql
    assert "string_agg" not in sql
    assert "ORDER BY" not in sql
