"""Write a tiny, process-local readiness signal.

The Docker healthcheck must not start a Python interpreter or open a database
connection for every probe. Long-running services update these files from the
same loop that already processes market/account state and performs database
writes. The shell healthcheck only reads their contents and modification times.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

_HEALTH_DIR_ENV = "CML_LOCAL_HEALTH_DIR"
_STATUS_FILE = "status"
_DATABASE_FILE = "database"


class LocalHealthWriter:
    """Publish local liveness and database-readiness markers.

    ``status`` is refreshed by the service's active work loop. ``database`` is
    refreshed only after an existing database operation succeeds. Keeping the
    two markers separate lets the probe distinguish a busy-but-disconnected
    process from a process that is still making progress through its database
    path.
    """

    def __init__(self, directory: Path) -> None:
        self._directory = directory
        self._status_path = directory / _STATUS_FILE
        self._database_path = directory / _DATABASE_FILE
        directory.mkdir(parents=True, exist_ok=True)
        self.reset()

    @classmethod
    def from_environment(cls) -> LocalHealthWriter | None:
        """Create a writer only when the container opted into local health."""

        raw_directory = os.environ.get(_HEALTH_DIR_ENV)
        if not raw_directory:
            return None
        return cls(Path(raw_directory))

    @classmethod
    def for_directory(cls, directory: Path) -> LocalHealthWriter:
        """Create a writer for tests and local process runners."""

        return cls(directory)

    @property
    def status_path(self) -> Path:
        return self._status_path

    @property
    def database_path(self) -> Path:
        return self._database_path

    def reset(self) -> None:
        """Remove stale readiness from a previous process and enter starting."""

        self._database_path.unlink(missing_ok=True)
        self._write_atomic(self._status_path, "starting\n")

    def heartbeat(self, *, database_ok: bool = False) -> None:
        """Refresh the local process heartbeat.

        This method is synchronous and intentionally tiny. It is called from
        existing service callbacks, outside the hot market-state decision path.
        """

        self._write_atomic(self._status_path, "ready\n")
        if database_ok:
            self.database_ok()

    def database_ok(self) -> None:
        """Record that an existing database operation completed successfully."""

        self._write_atomic(self._database_path, "ok\n")

    def degraded(self) -> None:
        """Stop advertising readiness while allowing the process to recover."""

        self._write_atomic(self._status_path, "degraded\n")

    def stopped(self) -> None:
        """Publish a terminal state for graceful shutdowns."""

        self._write_atomic(self._status_path, "stopped\n")

    @staticmethod
    def _write_atomic(path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.",
            dir=path.parent,
            text=True,
        )
        temporary_path = Path(temporary_name)
        try:
            with os.fdopen(fd, "w", encoding="ascii") as temporary:
                temporary.write(content)
            os.replace(temporary_path, path)
        finally:
            temporary_path.unlink(missing_ok=True)
