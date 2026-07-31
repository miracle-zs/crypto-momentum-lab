from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ArchiveFileDeletionResult:
    removable_paths: tuple[str, ...]
    deleted_bytes: int
    failed_paths: tuple[str, ...]


def retention_cutoff_date(*, now: datetime, retention_days: int) -> date:
    if retention_days <= 0:
        raise ValueError("retention_days must be positive")
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    # Keep today plus the preceding retention_days - 1 UTC calendar days.
    return now.astimezone(UTC).date() - timedelta(days=retention_days - 1)


def delete_archive_files(
    root: Path,
    relative_paths: Iterable[str],
    *,
    cutoff_date: date,
) -> ArchiveFileDeletionResult:
    resolved_root = root.resolve()
    removable_paths: list[str] = []
    failed_paths: list[str] = []
    deleted_bytes = 0

    for relative_value in dict.fromkeys(relative_paths):
        relative = Path(relative_value)
        if not _is_expired_finalized_path(relative, cutoff_date):
            continue
        raw_candidate = root / relative
        if raw_candidate.is_symlink():
            failed_paths.append(relative_value)
            continue
        candidate = raw_candidate.resolve()
        if not candidate.is_relative_to(resolved_root):
            failed_paths.append(relative_value)
            continue
        if not candidate.exists():
            removable_paths.append(relative_value)
            continue
        if not candidate.is_file():
            failed_paths.append(relative_value)
            continue
        try:
            deleted_bytes += candidate.stat().st_size
            candidate.unlink()
        except OSError:
            failed_paths.append(relative_value)
        else:
            removable_paths.append(relative_value)

    return ArchiveFileDeletionResult(
        removable_paths=tuple(removable_paths),
        deleted_bytes=deleted_bytes,
        failed_paths=tuple(failed_paths),
    )


def _is_expired_finalized_path(path: Path, cutoff_date: date) -> bool:
    if path.is_absolute() or ".." in path.parts:
        return False
    if not path.name.endswith(".jsonl.zst"):
        return False
    excluded_directories = {
        ".pending-manifests",
        ".recovery-working",
        ".recovery-quarantine",
    }
    if excluded_directories.intersection(path.parts):
        return False
    date_parts = [part[5:] for part in path.parts if part.startswith("date=")]
    if len(date_parts) != 1:
        return False
    try:
        archive_date = date.fromisoformat(date_parts[0])
    except ValueError:
        return False
    return archive_date < cutoff_date
