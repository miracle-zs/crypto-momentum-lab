from pathlib import Path

from crypto_momentum_lab.health.resources import ProcessResourceSampler

_CPU_PSI = "some avg10=1.25 avg60=0.0 avg300=0.0 total=1\n"
_MEMORY_PSI = (
    "some avg10=2.50 avg60=0.0 avg300=0.0 total=1\n"
    "full avg10=0.50 avg60=0.0 avg300=0.0 total=1\n"
)
_IO_PSI = (
    "some avg10=3.75 avg60=0.0 avg300=0.0 total=1\n"
    "full avg10=1.00 avg60=0.0 avg300=0.0 total=1\n"
)


def test_resource_sampler_reports_delta_rates_and_cgroup_breakdown() -> None:
    monotonic_values = iter((10.0, 12.0))
    samples = iter(
        (
            {
                "/proc/self/stat": "1 (worker) S 0 0 0 0 0 0 0 0 0 0 100 50",
                "/proc/self/status": "Name:\tworker\nThreads:\t3\n",
                "/sys/fs/cgroup/cpu.stat": (
                    "usage_usec 1000000\nthrottled_usec 100000\nnr_throttled 2\n"
                ),
                "/sys/fs/cgroup/memory.stat": "anon 100\nfile 200\nshmem 30\n",
                "/sys/fs/cgroup/memory.events": "high 1\noom 0\noom_kill 0\n",
                "/proc/pressure/cpu": _CPU_PSI,
                "/proc/pressure/memory": _MEMORY_PSI,
                "/proc/pressure/io": _IO_PSI,
            },
            {
                "/proc/self/stat": "1 (worker) S 0 0 0 0 0 0 0 0 0 0 160 90",
                "/proc/self/status": "Name:\tworker\nThreads:\t4\n",
                "/sys/fs/cgroup/cpu.stat": (
                    "usage_usec 1600000\nthrottled_usec 150000\nnr_throttled 3\n"
                ),
                "/sys/fs/cgroup/memory.stat": "anon 110\nfile 210\nshmem 31\n",
                "/sys/fs/cgroup/memory.events": "high 2\noom 0\noom_kill 0\n",
                "/proc/pressure/cpu": _CPU_PSI,
                "/proc/pressure/memory": _MEMORY_PSI,
                "/proc/pressure/io": _IO_PSI,
            },
        )
    )
    current = next(samples)

    def read_text(path: Path) -> str:
        return current.get(str(path), "")

    sampler = ProcessResourceSampler(
        monotonic=lambda: next(monotonic_values),
        read_text=read_text,
        list_fds=lambda: 7,
        clock_ticks=100,
    )
    first = sampler.snapshot()
    current = next(samples)
    second = sampler.snapshot()

    assert first["process_cpu_percent"] is None
    assert second["process_cpu_percent"] == 50.0
    assert second["cgroup_cpu_percent"] == 30.0
    assert second["cgroup_cpu_throttled_percent"] == 2.5
    assert second["process_open_fd_count"] == 7
    assert second["process_thread_count"] == 4
    assert second["cgroup_memory_anon_bytes"] == 110
    assert second["psi_memory_full_avg10"] == 0.5
