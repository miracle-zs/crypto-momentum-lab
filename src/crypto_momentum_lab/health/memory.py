"""Low-overhead process memory signals for controlled diagnostics."""

from __future__ import annotations

import os
import resource
import sys
import tracemalloc
from collections.abc import Mapping
from pathlib import Path

_TRACEMALLOC_ENABLED_ENV = "CML_TRACEMALLOC"
_TRACEMALLOC_FRAMES_ENV = "CML_TRACEMALLOC_FRAMES"
_DEFAULT_TRACEMALLOC_FRAMES = 1
_MAX_TRACEMALLOC_FRAMES = 25
_CGROUP_MEMORY_CURRENT_PATHS = (
    Path("/sys/fs/cgroup/memory.current"),
    Path("/sys/fs/cgroup/memory/memory.usage_in_bytes"),
)
_CGROUP_MEMORY_LIMIT_PATHS = (
    Path("/sys/fs/cgroup/memory.max"),
    Path("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
)


def current_rss_bytes() -> int | None:
    """Return the current process RSS when the platform exposes it."""
    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        with open("/proc/self/statm", encoding="ascii") as statm:
            resident_pages = int(statm.read().split()[1])
        return resident_pages * page_size
    except (FileNotFoundError, IndexError, OSError, ValueError):
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if usage <= 0:
            return None
        return usage if sys.platform == "darwin" else usage * 1024


def cgroup_memory_snapshot() -> dict[str, int | None]:
    """Return cgroup current/limit values when the container exposes them."""
    return {
        "cgroup_memory_current_bytes": _read_cgroup_bytes(
            _CGROUP_MEMORY_CURRENT_PATHS
        ),
        "cgroup_memory_limit_bytes": _read_cgroup_bytes(_CGROUP_MEMORY_LIMIT_PATHS),
    }


def configure_tracemalloc(
    environment: Mapping[str, str] | None = None,
) -> bool:
    """Enable tracemalloc only when explicitly requested by the environment.

    Tracing is deliberately opt-in because it adds allocation bookkeeping to
    every Python allocation. The frame count is kept small by default and is
    bounded so a malformed deployment setting cannot create excessive
    overhead.
    """
    env = os.environ if environment is None else environment
    if not _is_truthy(env.get(_TRACEMALLOC_ENABLED_ENV)):
        return tracemalloc.is_tracing()
    if not tracemalloc.is_tracing():
        tracemalloc.start(_tracemalloc_frames(env.get(_TRACEMALLOC_FRAMES_ENV)))
    return True


def tracemalloc_memory_snapshot() -> dict[str, bool | int | None]:
    """Return cheap current/peak Python allocation counters for structured logs."""
    enabled = tracemalloc.is_tracing()
    if not enabled:
        return {
            "tracemalloc_enabled": False,
            "tracemalloc_current_bytes": None,
            "tracemalloc_peak_bytes": None,
        }
    current, peak = tracemalloc.get_traced_memory()
    return {
        "tracemalloc_enabled": True,
        "tracemalloc_current_bytes": current,
        "tracemalloc_peak_bytes": peak,
    }


def _is_truthy(value: str | None) -> bool:
    return value is not None and value.strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _tracemalloc_frames(value: str | None) -> int:
    try:
        frames = int(value) if value is not None else _DEFAULT_TRACEMALLOC_FRAMES
    except ValueError:
        return _DEFAULT_TRACEMALLOC_FRAMES
    return max(1, min(frames, _MAX_TRACEMALLOC_FRAMES))


def _read_cgroup_bytes(paths: tuple[Path, ...]) -> int | None:
    for path in paths:
        try:
            value = path.read_text(encoding="ascii").strip()
        except (FileNotFoundError, OSError):
            continue
        if value == "max":
            return None
        try:
            parsed = int(value)
        except ValueError:
            continue
        if parsed >= 1 << 60:
            return None
        return parsed
    return None


__all__ = [
    "cgroup_memory_snapshot",
    "configure_tracemalloc",
    "current_rss_bytes",
    "tracemalloc_memory_snapshot",
]
