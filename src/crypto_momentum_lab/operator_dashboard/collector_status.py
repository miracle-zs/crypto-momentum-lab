"""Read-only status projection for the isolated research collector."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.operator_dashboard.schemas import ResearchCollectorResponse
from crypto_momentum_lab.operator_dashboard.status import OperationalStatus
from crypto_momentum_lab.research_collector.models import (
    CollectorCheckpoint,
    CollectorConfig,
)
from crypto_momentum_lab.research_collector.storage import (
    CapacityGuard,
    CapacityState,
)

DEFAULT_RESEARCH_COLLECTOR_ROOT = Path("/app/research-data")
DEFAULT_RESEARCH_COLLECTOR_ENVIRONMENT = "research"
DEFAULT_RESEARCH_COLLECTOR_TOP_COUNT = 30
DEFAULT_STALE_AFTER_SECONDS = 120.0
_RECENT_WINDOW_LIMIT = 24


def read_research_collector_status(
    root: Path = DEFAULT_RESEARCH_COLLECTOR_ROOT,
    *,
    now: datetime | None = None,
    environment: str = DEFAULT_RESEARCH_COLLECTOR_ENVIRONMENT,
    top_count: int = DEFAULT_RESEARCH_COLLECTOR_TOP_COUNT,
    stale_after_seconds: float = DEFAULT_STALE_AFTER_SECONDS,
) -> ResearchCollectorResponse:
    """Build a bounded, filesystem-only status snapshot for the dashboard.

    The dashboard container receives the collector volume read-only. This
    projection deliberately reads only the checkpoint, file metadata, and
    capacity counters; it never opens Parquet payloads or mutates the volume.
    """

    now_utc = _as_utc(now or datetime.now(UTC))
    config = CollectorConfig(environment=environment, root=root)
    try:
        checkpoint = _load_checkpoint(
            root / "checkpoints" / f"{environment}.json",
            environment=environment,
        )
        capacity = CapacityGuard(
            root,
            soft_limit_bytes=config.soft_limit_bytes,
            hard_limit_bytes=config.hard_limit_bytes,
            global_warning_free_bytes=config.global_warning_free_bytes,
            global_pause_free_bytes=config.global_pause_free_bytes,
        ).snapshot()
        parquet_files = _parquet_files(root / "parquet")
        window_rows, window_starts, latest_written_at = _window_rows(
            parquet_files,
            window_seconds=config.window_seconds,
        )
        pending_spool_files, pending_spool_bytes = _spool_stats(
            root / "spool" / "pending"
        )
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return _unavailable_response(
            now=now_utc,
            environment=environment,
            top_count=top_count,
            config=config,
        )

    checkpoint_at = None if checkpoint is None else checkpoint.updated_at
    checkpoint_age_seconds = _age_seconds(now_utc, checkpoint_at)
    stale = (
        checkpoint_at is None
        or checkpoint_age_seconds is None
        or checkpoint_age_seconds > stale_after_seconds
    )
    gap_count = _window_gap_count(
        window_starts,
        window_seconds=config.window_seconds,
    )
    alerts: list[str] = []
    if checkpoint is None:
        alerts.append("checkpoint 不存在，尚未确认采集状态")
    elif stale:
        alerts.append(f"checkpoint 已超过 {int(stale_after_seconds)} 秒未更新")
    if capacity.state is CapacityState.PAUSED:
        alerts.append("容量保护已暂停采集")
    elif capacity.state is CapacityState.WARNING:
        alerts.append("采集卷或整机剩余空间进入告警区")
    if gap_count:
        alerts.append(f"已发现 {gap_count} 个 15 分钟窗口缺口")
    if pending_spool_files:
        alerts.append(
            f"spool 有 {pending_spool_files} 个待处理文件，合计 "
            f"{pending_spool_bytes} bytes"
        )

    status = _status_for_snapshot(
        checkpoint=checkpoint,
        stale=stale,
        capacity_state=capacity.state,
        gap_count=gap_count,
        pending_spool_files=pending_spool_files,
    )
    latest_window_start = max(window_starts) if window_starts else None
    first_window_start = min(window_starts) if window_starts else None
    return ResearchCollectorResponse(
        status=status,
        status_detail=_status_detail(
            status=status,
            checkpoint=checkpoint,
            gap_count=gap_count,
            pending_spool_files=pending_spool_files,
        ),
        generated_at=now_utc,
        environment=environment,
        checkpoint_at=checkpoint_at,
        checkpoint_age_seconds=checkpoint_age_seconds,
        last_bucket_start=(
            None if checkpoint is None else checkpoint.last_bucket_start
        ),
        last_sequence=None if checkpoint is None else checkpoint.last_sequence,
        last_symbol=None if checkpoint is None else checkpoint.last_symbol,
        stream_id=None if checkpoint is None else checkpoint.stream_id,
        stale=stale,
        capacity_state=capacity.state.value,
        collector_bytes=capacity.collector_bytes,
        collector_soft_limit_bytes=config.soft_limit_bytes,
        collector_hard_limit_bytes=config.hard_limit_bytes,
        disk_free_bytes=capacity.disk_free_bytes,
        disk_warning_free_bytes=config.global_warning_free_bytes,
        disk_pause_free_bytes=config.global_pause_free_bytes,
        pending_spool_files=pending_spool_files,
        pending_spool_bytes=pending_spool_bytes,
        parquet_file_count=len(parquet_files),
        parquet_first_window_start=first_window_start,
        parquet_latest_window_start=latest_window_start,
        parquet_latest_written_at=latest_written_at,
        parquet_latest_age_seconds=_age_seconds(now_utc, latest_written_at),
        parquet_window_seconds=config.window_seconds,
        parquet_gap_count=gap_count,
        top_count=top_count,
        late_tolerance_seconds=config.late_tolerance_seconds,
        max_spool_bytes=config.max_spool_bytes,
        alerts=alerts,
        recent_windows=window_rows[:_RECENT_WINDOW_LIMIT],
    )


def _load_checkpoint(path: Path, *, environment: str) -> CollectorCheckpoint | None:
    if not path.is_file():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("collector checkpoint must be an object")
    if payload.get("environment") != environment:
        raise ValueError("collector checkpoint environment mismatch")
    schema_version = payload.get("schema_version")
    if not isinstance(schema_version, int) or isinstance(schema_version, bool):
        raise ValueError("collector checkpoint schema version is invalid")
    return CollectorCheckpoint(
        environment=environment,
        stream_id=_optional_string(payload.get("stream_id")),
        last_sequence=_optional_int(payload.get("last_sequence")),
        last_bucket_start=_optional_datetime(payload.get("last_bucket_start")),
        last_symbol=_optional_string(payload.get("last_symbol")),
        schema_version=schema_version,
        updated_at=_optional_datetime(payload.get("updated_at")),
    )


def _parquet_files(root: Path) -> tuple[Path, ...]:
    if not root.is_dir():
        return ()
    return tuple(path for path in root.rglob("*.parquet") if path.is_file())


def _window_rows(
    paths: tuple[Path, ...],
    *,
    window_seconds: int,
) -> tuple[list[dict[str, JsonValue]], tuple[datetime, ...], datetime | None]:
    rows: list[tuple[datetime, datetime, int]] = []
    for path in paths:
        window_start = _window_start_from_path(path)
        stat = path.stat()
        written_at = datetime.fromtimestamp(stat.st_mtime, tz=UTC)
        rows.append((window_start, written_at, stat.st_size))
    rows.sort(key=lambda row: row[0], reverse=True)
    starts = tuple(sorted({row[0] for row in rows}))
    latest_written_at = max((row[1] for row in rows), default=None)
    payload: list[dict[str, JsonValue]] = []
    for window_start, written_at, size_bytes in rows:
        payload.append(
            {
                "window_start": window_start.isoformat(),
                "written_at": written_at.isoformat(),
                "size_bytes": size_bytes,
                "window_seconds": window_seconds,
            }
        )
    return payload, starts, latest_written_at


def _window_start_from_path(path: Path) -> datetime:
    date_part = next(
        part.removeprefix("date=")
        for part in path.parts
        if part.startswith("date=")
    )
    clock = path.stem.removeprefix("window=")
    if len(clock) == 4:
        clock = f"{clock}00"
    if len(clock) != 6 or not clock.isdigit():
        raise ValueError("invalid Parquet window path")
    return datetime.fromisoformat(
        f"{date_part}T{clock[:2]}:{clock[2:4]}:{clock[4:]}+00:00"
    )


def _window_gap_count(
    starts: tuple[datetime, ...],
    *,
    window_seconds: int,
) -> int:
    gap_count = 0
    for previous, current in zip(starts, starts[1:], strict=False):
        intervals = int((current - previous).total_seconds()) // window_seconds
        gap_count += max(0, intervals - 1)
    return gap_count


def _spool_stats(root: Path) -> tuple[int, int]:
    if not root.is_dir():
        return 0, 0
    files = tuple(path for path in root.rglob("*") if path.is_file())
    return len(files), sum(path.stat().st_size for path in files)


def _status_for_snapshot(
    *,
    checkpoint: CollectorCheckpoint | None,
    stale: bool,
    capacity_state: CapacityState,
    gap_count: int,
    pending_spool_files: int,
) -> OperationalStatus:
    if checkpoint is None:
        return OperationalStatus.NO_DATA
    if capacity_state is CapacityState.PAUSED:
        return OperationalStatus.HALTED
    if stale:
        return OperationalStatus.STALE
    if (
        capacity_state is CapacityState.WARNING
        or gap_count > 0
        or pending_spool_files > 0
    ):
        return OperationalStatus.DEGRADED
    return OperationalStatus.FRESH


def _status_detail(
    *,
    status: OperationalStatus,
    checkpoint: CollectorCheckpoint | None,
    gap_count: int,
    pending_spool_files: int,
) -> str:
    if status is OperationalStatus.NO_DATA or checkpoint is None:
        return "等待 checkpoint"
    if status is OperationalStatus.HALTED:
        return "容量保护已暂停写入"
    if status is OperationalStatus.STALE:
        return "checkpoint 超过新鲜度窗口"
    if gap_count:
        return f"存在 {gap_count} 个窗口缺口"
    if pending_spool_files:
        return f"spool 待处理 {pending_spool_files} 个"
    return "checkpoint 与 Parquet 窗口持续更新"


def _unavailable_response(
    *,
    now: datetime,
    environment: str,
    top_count: int,
    config: CollectorConfig,
) -> ResearchCollectorResponse:
    return ResearchCollectorResponse(
        status=OperationalStatus.UNKNOWN,
        status_detail="数据卷或 checkpoint 不可读取",
        generated_at=now,
        environment=environment,
        checkpoint_at=None,
        checkpoint_age_seconds=None,
        last_bucket_start=None,
        last_sequence=None,
        last_symbol=None,
        stream_id=None,
        stale=True,
        capacity_state="unknown",
        collector_bytes=0,
        collector_soft_limit_bytes=config.soft_limit_bytes,
        collector_hard_limit_bytes=config.hard_limit_bytes,
        disk_free_bytes=0,
        disk_warning_free_bytes=config.global_warning_free_bytes,
        disk_pause_free_bytes=config.global_pause_free_bytes,
        pending_spool_files=0,
        pending_spool_bytes=0,
        parquet_file_count=0,
        parquet_first_window_start=None,
        parquet_latest_window_start=None,
        parquet_latest_written_at=None,
        parquet_latest_age_seconds=None,
        parquet_window_seconds=config.window_seconds,
        parquet_gap_count=0,
        top_count=top_count,
        late_tolerance_seconds=config.late_tolerance_seconds,
        max_spool_bytes=config.max_spool_bytes,
        alerts=["无法读取 research-data 数据卷"],
        recent_windows=[],
    )


def _age_seconds(now: datetime, observed_at: datetime | None) -> float | None:
    if observed_at is None:
        return None
    return max(0.0, (now - observed_at).total_seconds())


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("collector status clock must be timezone-aware")
    return value.astimezone(UTC)


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("checkpoint string field is invalid")
    return value


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError("checkpoint integer field is invalid")
    return value


def _optional_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("checkpoint datetime field is invalid")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
