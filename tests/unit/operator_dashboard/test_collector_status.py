import json
import os
import shutil
from collections import namedtuple
from datetime import UTC, datetime, timedelta

import pytest

from crypto_momentum_lab.operator_dashboard.collector_status import (
    read_research_collector_status,
)
from crypto_momentum_lab.operator_dashboard.status import OperationalStatus

_Usage = namedtuple("usage", ["total", "used", "free"])
_PLENTIFUL_FREE_BYTES = 100 * 1024 * 1024 * 1024  # 100 GiB


@pytest.fixture(autouse=True)
def _stub_disk_usage(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        shutil,
        "disk_usage",
        lambda _path: _Usage(
            _PLENTIFUL_FREE_BYTES * 2,
            _PLENTIFUL_FREE_BYTES,
            _PLENTIFUL_FREE_BYTES,
        ),
    )


def _write_checkpoint(
    root,
    *,
    updated_at: datetime,
    last_bucket_start: datetime,
) -> None:
    checkpoint_path = root / "checkpoints" / "research.json"
    checkpoint_path.parent.mkdir(parents=True)
    checkpoint_path.write_text(
        json.dumps(
            {
                "environment": "research",
                "last_bucket_start": last_bucket_start.isoformat(),
                "last_sequence": 42,
                "last_symbol": "BTCUSDT",
                "schema_version": 1,
                "stream_id": "stream-id",
                "updated_at": updated_at.isoformat(),
            }
        ),
        encoding="utf-8",
    )


def _write_window(root, start: datetime) -> None:
    window_path = (
        root
        / "parquet"
        / "environment=research"
        / f"date={start.date().isoformat()}"
        / f"hour={start.hour:02d}"
        / f"window={start:%H%M%S}.parquet"
    )
    window_path.parent.mkdir(parents=True, exist_ok=True)
    window_path.write_bytes(b"parquet metadata placeholder")


def _write_spool_file(
    root,
    *,
    name: str,
    modified_at: datetime,
) -> None:
    spool_path = root / "spool" / "pending" / "hub" / name
    spool_path.parent.mkdir(parents=True, exist_ok=True)
    spool_path.write_bytes(b"spool payload placeholder")
    timestamp = modified_at.timestamp()
    os.utime(spool_path, (timestamp, timestamp))


def test_collector_status_reports_fresh_contiguous_windows(tmp_path) -> None:
    now = datetime(2026, 9, 3, 15, 17, tzinfo=UTC)
    root = tmp_path / "research-data"
    root.mkdir()
    _write_checkpoint(
        root,
        updated_at=now - timedelta(seconds=8),
        last_bucket_start=datetime(2026, 9, 3, 15, 14, 45, tzinfo=UTC),
    )
    for minute in (0, 15, 30):
        _write_window(root, datetime(2026, 9, 3, 15, minute, tzinfo=UTC))

    response = read_research_collector_status(root, now=now)

    assert response.status is OperationalStatus.FRESH
    assert response.stale is False
    assert response.parquet_file_count == 3
    assert response.parquet_gap_count == 0
    assert response.last_sequence == 42
    assert response.recent_windows[0]["window_start"] == (
        "2026-09-03T15:30:00+00:00"
    )


def test_collector_status_does_not_alert_on_current_window_spool(tmp_path) -> None:
    now = datetime(2026, 9, 3, 15, 17, tzinfo=UTC)
    root = tmp_path / "research-data"
    root.mkdir()
    _write_checkpoint(
        root,
        updated_at=now - timedelta(seconds=8),
        last_bucket_start=datetime(2026, 9, 3, 15, 14, 45, tzinfo=UTC),
    )
    _write_window(root, datetime(2026, 9, 3, 15, 0, tzinfo=UTC))
    _write_spool_file(
        root,
        name="current-window.json",
        modified_at=now - timedelta(seconds=60),
    )

    response = read_research_collector_status(root, now=now)

    assert response.status is OperationalStatus.FRESH
    assert response.pending_spool_files == 1
    assert response.pending_spool_overdue_files == 0
    assert response.alerts == []
    assert "待封存" in response.status_detail


def test_collector_status_alerts_on_overdue_spool(tmp_path) -> None:
    now = datetime(2026, 9, 3, 15, 17, tzinfo=UTC)
    root = tmp_path / "research-data"
    root.mkdir()
    _write_checkpoint(
        root,
        updated_at=now - timedelta(seconds=8),
        last_bucket_start=datetime(2026, 9, 3, 15, 14, 45, tzinfo=UTC),
    )
    _write_window(root, datetime(2026, 9, 3, 15, 0, tzinfo=UTC))
    _write_spool_file(
        root,
        name="overdue.json",
        modified_at=now - timedelta(seconds=15 * 60 + 30 + 1),
    )

    response = read_research_collector_status(root, now=now)

    assert response.status is OperationalStatus.DEGRADED
    assert response.pending_spool_files == 1
    assert response.pending_spool_overdue_files == 1
    assert any("spool" in alert and "超时" in alert for alert in response.alerts)


def test_collector_status_surfaces_window_gaps(tmp_path) -> None:
    now = datetime(2026, 9, 3, 15, 17, tzinfo=UTC)
    root = tmp_path / "research-data"
    root.mkdir()
    _write_checkpoint(
        root,
        updated_at=now - timedelta(seconds=8),
        last_bucket_start=datetime(2026, 9, 3, 15, 14, 45, tzinfo=UTC),
    )
    for minute in (0, 30):
        _write_window(root, datetime(2026, 9, 3, 15, minute, tzinfo=UTC))

    response = read_research_collector_status(root, now=now)

    assert response.parquet_gap_count == 1
    assert response.status is OperationalStatus.DEGRADED
    assert "窗口缺口" in response.status_detail
    assert response.alerts == ["已发现 1 个 15 分钟窗口缺口"]


def test_collector_status_distinguishes_missing_checkpoint(tmp_path) -> None:
    root = tmp_path / "research-data"
    root.mkdir()

    response = read_research_collector_status(
        root,
        now=datetime(2026, 9, 3, tzinfo=UTC),
    )

    assert response.status is OperationalStatus.NO_DATA
    assert response.checkpoint_at is None
    assert response.alerts == ["checkpoint 不存在，尚未确认采集状态"]


def test_collector_status_alerts_on_injected_disk_warning(tmp_path) -> None:
    now = datetime(2026, 9, 3, 15, 17, tzinfo=UTC)
    root = tmp_path / "research-data"
    root.mkdir()
    _write_checkpoint(
        root,
        updated_at=now - timedelta(seconds=8),
        last_bucket_start=datetime(2026, 9, 3, 15, 14, 45, tzinfo=UTC),
    )
    _write_window(root, datetime(2026, 9, 3, 15, 0, tzinfo=UTC))

    # Warning threshold is 15 GiB, Pause threshold is 10 GiB; inject 12 GiB free
    warning_free = 12 * 1024 * 1024 * 1024
    response = read_research_collector_status(
        root,
        now=now,
        disk_usage_fn=lambda _path: _Usage(warning_free * 2, warning_free, warning_free),
    )

    assert response.status is OperationalStatus.DEGRADED
    assert response.capacity_state == "warning"
    assert any("剩余空间进入告警区" in alert for alert in response.alerts)


def test_collector_status_alerts_on_injected_disk_paused(tmp_path) -> None:
    now = datetime(2026, 9, 3, 15, 17, tzinfo=UTC)
    root = tmp_path / "research-data"
    root.mkdir()
    _write_checkpoint(
        root,
        updated_at=now - timedelta(seconds=8),
        last_bucket_start=datetime(2026, 9, 3, 15, 14, 45, tzinfo=UTC),
    )
    _write_window(root, datetime(2026, 9, 3, 15, 0, tzinfo=UTC))

    # Pause threshold is 10 GiB; inject 8 GiB free
    pause_free = 8 * 1024 * 1024 * 1024
    response = read_research_collector_status(
        root,
        now=now,
        disk_usage_fn=lambda _path: _Usage(pause_free * 2, pause_free, pause_free),
    )

    assert response.status is OperationalStatus.HALTED
    assert response.capacity_state == "paused"
    assert any("容量保护已暂停采集" in alert for alert in response.alerts)
