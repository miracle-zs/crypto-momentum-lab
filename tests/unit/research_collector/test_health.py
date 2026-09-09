import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from crypto_momentum_lab.research_collector import health


def _snapshot(**overrides: object) -> dict[str, object]:
    now = datetime.now(UTC).isoformat()
    return {
        "environment": "research",
        "ready": True,
        "updated_at": now,
        "capacity_updated_at": now,
        "capacity_state": "healthy",
        **overrides,
    }


def test_probe_reads_snapshot_without_scanning_data(tmp_path, monkeypatch):
    store = health.CollectorHealthStore(tmp_path, "research")
    store.save(_snapshot())

    def no_scan(*args, **kwargs):
        pytest.fail("health probe scanned the research directory")

    monkeypatch.setattr(Path, "rglob", no_scan)
    assert health.check_health(tmp_path, "research")[0]
    # Docker invokes this file with site-packages disabled.
    result = subprocess.run(
        [sys.executable, "-S", health.__file__, "--root", str(tmp_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["stale"] is False


@pytest.mark.parametrize(
    "overrides",
    [
        {"ready": False},
        {"capacity_state": "paused"},
        {"environment": "other"},
        {"updated_at": None},
        {"updated_at": "2026-01-01T00:00:00"},
        {"capacity_updated_at": "2000-01-01T00:00:00+00:00"},
        {"updated_at": "2000-01-01T00:00:00+00:00"},
        {"updated_at": (datetime.now(UTC) + timedelta(days=1)).isoformat()},
    ],
)
def test_probe_fails_closed_for_unready_or_stale_snapshot(tmp_path, overrides):
    health.CollectorHealthStore(tmp_path, "research").save(_snapshot(**overrides))
    assert not health.check_health(tmp_path, "research")[0]


def test_missing_corrupt_and_previous_process_snapshots_fail(tmp_path):
    store = health.CollectorHealthStore(tmp_path, "research")
    assert not health.check_health(tmp_path, "research")[0]
    store.save(_snapshot())
    store.path.write_text("{")
    assert not health.check_health(tmp_path, "research")[0]
    store.save(_snapshot())
    store.reset()
    assert not health.check_health(tmp_path, "research")[0]
