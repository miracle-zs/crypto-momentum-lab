"""A TTL-bounded quiet period for lifecycle alerts during planned changes.

Every deploy recreates containers.  A container that is booting -- or one that
is being stopped -- is unhealthy and has no heartbeat by definition, and Docker
exposes no "stopping" state, so from the monitor's side a deploy is
indistinguishable from a crash: it produced four alerts per deploy
(container_unhealthy, live_heartbeat_stale, live_crash_log_archive_failed,
live_heartbeat_auto_restarted) for a container whose restart count never moved.

Only the deploy knows the change is planned, so the deploy declares a
maintenance window and the monitor honours it.

The window always expires on its own: a deploy that dies mid-flight must not
silence the monitor forever, and an expired window is itself worth reporting.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

__all__ = [
    "MaintenanceWindow",
    "clear_maintenance_window",
    "is_maintenance_active",
    "read_maintenance_window",
    "write_maintenance_window",
]


@dataclass(frozen=True, slots=True)
class MaintenanceWindow:
    """A declared period during which planned container churn is expected."""

    started_at: datetime
    expected_seconds: float
    reason: str = ""

    def expires_at(self) -> datetime:
        return self.started_at + timedelta(seconds=self.expected_seconds)

    def is_active(self, *, now: datetime) -> bool:
        return now < self.expires_at()


def write_maintenance_window(
    path: Path,
    *,
    started_at: datetime,
    expected_seconds: float,
    reason: str = "",
) -> MaintenanceWindow:
    """Declare a maintenance window, replacing any previous one."""

    if expected_seconds <= 0:
        raise ValueError("expected_seconds must be positive")
    window = MaintenanceWindow(
        started_at=started_at,
        expected_seconds=expected_seconds,
        reason=reason,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    # Write then rename so a reader never observes a half-written window.
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(
            {
                "started_at": window.started_at.isoformat(),
                "expected_seconds": window.expected_seconds,
                "reason": window.reason,
            }
        ),
        encoding="utf-8",
    )
    temporary.replace(path)
    return window


def read_maintenance_window(path: Path) -> MaintenanceWindow | None:
    """Read the declared window, or None if absent or unreadable.

    An unreadable marker degrades to "no window": failing open means a corrupted
    file alerts, which is the safe direction for a monitoring gap.
    """

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    started_at = payload.get("started_at")
    expected_seconds = payload.get("expected_seconds")
    reason = payload.get("reason", "")
    if not isinstance(started_at, str) or not isinstance(
        expected_seconds, int | float
    ):
        return None
    if isinstance(expected_seconds, bool) or expected_seconds <= 0:
        return None
    try:
        parsed_started_at = datetime.fromisoformat(started_at)
    except ValueError:
        return None
    if parsed_started_at.tzinfo is None:
        return None
    return MaintenanceWindow(
        started_at=parsed_started_at,
        expected_seconds=float(expected_seconds),
        reason=reason if isinstance(reason, str) else "",
    )


def is_maintenance_active(path: Path, *, now: datetime) -> bool:
    """Report whether a declared window is still running."""

    window = read_maintenance_window(path)
    return window is not None and window.is_active(now=now)


def clear_maintenance_window(path: Path) -> None:
    """End the window.  Missing files are fine; the deploy may never have won."""

    try:
        path.unlink()
    except FileNotFoundError:
        return


def expired_maintenance_window(
    path: Path,
    *,
    now: datetime,
    grace_seconds: float = 0.0,
) -> MaintenanceWindow | None:
    """Return a window that has expired by more than ``grace_seconds``.

    A deploy that dies between writing and clearing the marker leaves it behind;
    the monitor must not stay silent forever, so an overdue window is reported.
    """

    window = read_maintenance_window(path)
    if window is None:
        return None
    overdue_by = (now - window.expires_at()).total_seconds()
    return window if overdue_by > grace_seconds else None


def default_maintenance_path() -> Path:
    """The marker location shared by the deploy script and the monitor."""

    override = _environment_path()
    if override is not None:
        return override
    return Path("/var/lib/crypto-momentum-lab/maintenance.json")


def _environment_path() -> Path | None:
    import os

    raw = os.environ.get("CML_MAINTENANCE_WINDOW_FILE")
    if raw is None or not raw.strip():
        return None
    return Path(raw.strip())


def now_utc() -> datetime:
    return datetime.now(tz=UTC)
