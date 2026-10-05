"""Low-overhead, delta-based process and cgroup resource observations."""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path

import structlog

_CGROUP_CPU_STAT = Path("/sys/fs/cgroup/cpu.stat")
_CGROUP_MEMORY_STAT = Path("/sys/fs/cgroup/memory.stat")
_CGROUP_MEMORY_EVENTS = Path("/sys/fs/cgroup/memory.events")
_PSI_PATHS = {
    "cpu": Path("/proc/pressure/cpu"),
    "memory": Path("/proc/pressure/memory"),
    "io": Path("/proc/pressure/io"),
}
_log = structlog.get_logger(__name__)


class ProcessResourceSampler:
    """Return one compact snapshot; rates are deltas since the prior sample.

    Sampling is deliberately caller-driven.  A service adds this to its
    existing 30--60 second health log rather than creating another timer,
    task, or metrics queue on the critical event loop.
    """

    def __init__(
        self,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        read_text: Callable[[Path], str] | None = None,
        list_fds: Callable[[], int | None] | None = None,
        clock_ticks: int | None = None,
    ) -> None:
        self._monotonic = monotonic
        self._read_text = read_text or _read_text
        self._list_fds = list_fds or _open_fd_count
        self._clock_ticks = clock_ticks or _clock_ticks()
        self._previous_at: float | None = None
        self._previous_process_ticks: int | None = None
        self._previous_cgroup_usage_usec: int | None = None
        self._previous_cgroup_throttled_usec: int | None = None

    def snapshot(self) -> dict[str, int | float | None]:
        now = self._monotonic()
        process_ticks = _process_cpu_ticks(self._read_text)
        cpu_stat = _key_values(self._read_text(_CGROUP_CPU_STAT))
        memory_stat = _key_values(self._read_text(_CGROUP_MEMORY_STAT))
        memory_events = _key_values(self._read_text(_CGROUP_MEMORY_EVENTS))
        usage_usec = cpu_stat.get("usage_usec")
        throttled_usec = cpu_stat.get("throttled_usec")
        elapsed = _elapsed(now, self._previous_at)
        snapshot: dict[str, int | float | None] = {
            "process_cpu_percent": _rate_percent(
                process_ticks,
                self._previous_process_ticks,
                elapsed,
                scale=1 / self._clock_ticks,
            ),
            "process_cpu_seconds_total": (
                None
                if process_ticks is None
                else round(process_ticks / self._clock_ticks, 6)
            ),
            "process_open_fd_count": self._list_fds(),
            "process_thread_count": _thread_count(self._read_text),
            "cgroup_cpu_usage_usec": usage_usec,
            "cgroup_cpu_percent": _rate_percent(
                usage_usec, self._previous_cgroup_usage_usec, elapsed, scale=0.000001
            ),
            "cgroup_cpu_throttled_usec": throttled_usec,
            "cgroup_cpu_throttled_percent": _rate_percent(
                throttled_usec,
                self._previous_cgroup_throttled_usec,
                elapsed,
                scale=0.000001,
            ),
            "cgroup_cpu_nr_throttled": cpu_stat.get("nr_throttled"),
            "cgroup_memory_anon_bytes": memory_stat.get("anon"),
            "cgroup_memory_file_bytes": memory_stat.get("file"),
            "cgroup_memory_shmem_bytes": memory_stat.get("shmem"),
            "cgroup_memory_events_high": memory_events.get("high"),
            "cgroup_memory_events_oom": memory_events.get("oom"),
            "cgroup_memory_events_oom_kill": memory_events.get("oom_kill"),
        }
        for resource, path in _PSI_PATHS.items():
            psi = _psi_averages(self._read_text(path))
            snapshot[f"psi_{resource}_some_avg10"] = psi.get("some")
            snapshot[f"psi_{resource}_full_avg10"] = psi.get("full")

        self._previous_at = now
        self._previous_process_ticks = process_ticks
        self._previous_cgroup_usage_usec = usage_usec
        self._previous_cgroup_throttled_usec = throttled_usec
        return snapshot


async def monitor_process_resources(
    *,
    service: str,
    interval_seconds: float = 60.0,
    dimensions: Mapping[str, str] | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    sampler: ProcessResourceSampler | None = None,
) -> None:
    """Emit a low-frequency process snapshot until the task is cancelled."""
    if not service.strip():
        raise ValueError("service must not be empty")
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    observe = (sampler or ProcessResourceSampler()).snapshot
    fields = {} if dimensions is None else dict(dimensions)
    while True:
        await sleep(interval_seconds)
        try:
            _log.info(
                "process_resource_snapshot",
                service=service,
                **fields,
                **observe(),
            )
        except Exception:
            _log.exception("process_resource_snapshot_failed", service=service)


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="ascii")
    except (FileNotFoundError, OSError):
        return ""


def _key_values(raw: str) -> dict[str, int]:
    values: dict[str, int] = {}
    for line in raw.splitlines():
        key, _, value = line.partition(" ")
        try:
            values[key] = int(value.strip())
        except ValueError:
            continue
    return values


def _psi_averages(raw: str) -> dict[str, float]:
    values: dict[str, float] = {}
    for line in raw.splitlines():
        kind, _, details = line.partition(" ")
        for item in details.split():
            key, _, value = item.partition("=")
            if key == "avg10":
                try:
                    values[kind] = float(value)
                except ValueError:
                    pass
    return values


def _process_cpu_ticks(read_text: Callable[[Path], str]) -> int | None:
    raw = read_text(Path("/proc/self/stat"))
    _, separator, fields = raw.rpartition(")")
    if not separator:
        return None
    parts = fields.split()
    try:
        return int(parts[11]) + int(parts[12])
    except (IndexError, ValueError):
        return None


def _thread_count(read_text: Callable[[Path], str]) -> int | None:
    for line in read_text(Path("/proc/self/status")).splitlines():
        if line.startswith("Threads:"):
            try:
                return int(line.partition(":")[2].strip())
            except ValueError:
                return None
    return None


def _open_fd_count() -> int | None:
    try:
        return sum(1 for _ in Path("/proc/self/fd").iterdir())
    except OSError:
        return None


def _clock_ticks() -> int:
    try:
        return int(os.sysconf("SC_CLK_TCK"))
    except (AttributeError, OSError, ValueError):
        return 100


def _elapsed(now: float, previous: float | None) -> float | None:
    if previous is None or now <= previous:
        return None
    return now - previous


def _rate_percent(
    current: int | None,
    previous: int | None,
    elapsed: float | None,
    *,
    scale: float,
) -> float | None:
    if current is None or previous is None or elapsed is None:
        return None
    return round(max(0, current - previous) * scale / elapsed * 100, 3)


__all__ = ["ProcessResourceSampler", "monitor_process_resources"]
