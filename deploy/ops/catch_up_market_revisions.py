"""Catch up cold market payloads in small, pressure-gated archive rounds."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import signal
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

_STOP = False
_DATABASE_PROBE = """
import json, os
from sqlalchemy import create_engine, text
url = os.environ.get('CML_MARKET_DATABASE_URL') or os.environ['CML_DATABASE_URL']
engine = create_engine(url.replace('postgresql+asyncpg://', 'postgresql+psycopg://', 1))
with engine.connect() as connection:
    connection.execute(text("SET statement_timeout='3s'"))
    state = dict(connection.execute(text('''
        SELECT n_live_tup, n_dead_tup, last_autovacuum,
               pg_total_relation_size(relid) AS relation_bytes
        FROM pg_stat_user_tables WHERE relname='market_revision_refs'
    ''')).mappings().one())
    state['oldest_pending'] = connection.execute(text('''
        SELECT bucket_start FROM market_revision_refs
        WHERE payload IS NOT NULL AND payload_archive_path IS NULL
          AND bucket_start < now() - interval '1 day'
        ORDER BY bucket_start LIMIT 1
    ''')).scalar_one_or_none()
    state['lock_waiters'] = connection.execute(text('''
        SELECT count(*) FROM pg_stat_activity
        WHERE wait_event_type='Lock' AND query_start < now()-interval '5 seconds'
    ''')).scalar_one()
    state['old_transactions'] = connection.execute(text('''
        SELECT count(*) FROM pg_stat_activity
        WHERE xact_start < now()-interval '5 minutes'
          AND backend_type='client backend'
    ''')).scalar_one()
print(json.dumps(state, default=str))
engine.dispose()
"""


def emit(event: str, **details: object) -> None:
    print(
        json.dumps({"at": datetime.now(UTC).isoformat(), "event": event, **details}),
        flush=True,
    )


def command(args: list[str], *, timeout: float = 15) -> str:
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if result.returncode:
        # Do not echo subprocess stderr: database errors can include credentials.
        raise RuntimeError(f"{args[0]} failed with exit code {result.returncode}")
    return result.stdout


def critical_containers() -> list[str]:
    names = command(["docker", "ps", "-a", "--format", "{{.Names}}"])
    exact = {
        f"crypto-momentum-lab-{service}-1"
        for service in (
            "postgres",
            "dashboard",
            "market-data",
            "research-collector",
        )
    }
    discovered = {
        name
        for name in names.splitlines()
        if name in exact
        or name.startswith(
            (
                "crypto-momentum-lab-live-strategy-",
                "crypto-momentum-lab-execution-account-live-",
            )
        )
    }
    if not exact <= discovered:
        raise RuntimeError("required base containers are missing")
    return sorted(discovered)


def pressure_average(path: Path) -> float:
    # PSI full is time when every runnable task is stalled on this resource.
    for line in path.read_text().splitlines():
        if line.startswith("full "):
            return float(dict(field.split("=") for field in line.split()[1:])["avg10"])
    raise RuntimeError(f"missing full pressure metric: {path}")


def snapshot(project: Path, containers: list[str]) -> dict[str, object]:
    statuses = command(
        [
            "docker",
            "inspect",
            "--format",
            "{{.Name}}|{{.State.Status}}|"
            "{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}",
            *containers,
        ]
    )
    unhealthy = []
    for line in statuses.splitlines():
        name, state, health = line.split("|")
        if state != "running" or health != "healthy":
            unhealthy.append(f"{name.lstrip('/')}:{state}/{health}")
    database = json.loads(
        command(
            [
                "docker",
                "exec",
                "crypto-momentum-lab-dashboard-1",
                "python",
                "-c",
                _DATABASE_PROBE,
            ]
        )
    )
    monitor = json.loads(
        Path("/var/lib/crypto-momentum-lab/ops-monitor.json").read_text()
    )
    if (
        time.time()
        - Path("/var/lib/crypto-momentum-lab/ops-monitor.json").stat().st_mtime
        > 300
    ):
        raise RuntimeError("monitor state is stale")
    alerts = set(monitor.get("active_alerts", {})) | set(
        monitor.get("pending_alerts", {})
    )
    filesystems = [
        os.statvfs(path)
        for path in (
            "/var/lib/docker",
            "/var/lib/crypto-momentum-lab/table-archive/market_revision_refs",
        )
    ]
    return {
        "unhealthy": unhealthy,
        "other_alerts": sorted(alerts - {"database_storage_growth"}),
        "free_bytes": min(space.f_bavail * space.f_frsize for space in filesystems),
        "load_per_cpu": os.getloadavg()[0] / (os.cpu_count() or 1),
        "memory_full_pct": pressure_average(Path("/proc/pressure/memory")),
        "io_full_pct": pressure_average(Path("/proc/pressure/io")),
        **database,
    }


def blockers(
    state: dict[str, object],
    *,
    minimum_free_bytes: int,
    maximum_dead_rows: int,
    maximum_load: float,
) -> list[str]:
    reasons = []
    for key in ("unhealthy", "other_alerts", "lock_waiters", "old_transactions"):
        if state[key]:
            reasons.append(key)
    if state["free_bytes"] < minimum_free_bytes:
        reasons.append("low_disk_space")
    if state["n_dead_tup"] >= maximum_dead_rows:
        reasons.append("waiting_for_autovacuum")
    if state["load_per_cpu"] >= maximum_load:
        reasons.append("high_host_load")
    if state["memory_full_pct"] >= 1:
        reasons.append("memory_pressure")
    if state["io_full_pct"] >= 5:
        reasons.append("io_pressure")
    return reasons


def archive_round(project: Path, *, chunks: int, deadline: float) -> dict[str, object]:
    name = f"cml-revision-catchup-{uuid4().hex}"
    process = subprocess.Popen(
        [
            "docker",
            "compose",
            "--project-directory",
            str(project),
            "--env-file",
            str(project / ".env.server"),
            "-f",
            str(project / "compose.server.yaml"),
            "--profile",
            "maintenance",
            "run",
            "--rm",
            "--no-deps",
            "--name",
            name,
            "market-revision-archiver",
            "--retention-days",
            "1",
            "--max-chunks",
            str(chunks),
            "--batch-size",
            "10000",
            "--apply",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    round_deadline = min(deadline, time.monotonic() + 300)
    try:
        while True:
            if _STOP:
                raise InterruptedError("stop requested")
            if time.monotonic() >= round_deadline:
                raise TimeoutError("archive round exceeded its time budget")
            try:
                stdout, _ = process.communicate(timeout=1)
                break
            except subprocess.TimeoutExpired:
                continue
        if process.returncode:
            raise RuntimeError(f"archive round failed: exit code {process.returncode}")
        result = json.loads(stdout)
        if result.get("mode") != "applied" or not isinstance(
            result.get("archived_rows"), int
        ):
            raise RuntimeError("archive round returned an invalid result")
        return result
    finally:
        if process.poll() is None:
            # Stop only this invocation's named container. Committed rounds
            # remain readable; an interrupted DB transaction rolls back.
            try:
                command(["docker", "stop", "--time", "20", name], timeout=30)
            finally:
                process.kill()
                process.communicate()


def pause(seconds: float, deadline: float) -> None:
    until = min(time.monotonic() + seconds, deadline)
    while not _STOP and time.monotonic() < until:
        time.sleep(min(1, until - time.monotonic()))


def run(args: argparse.Namespace) -> int:
    project = args.project_directory.resolve()
    if not (project / ".env.server").is_file():
        raise RuntimeError("server environment file is missing")
    containers = critical_containers()
    deadline = time.monotonic() + args.max_runtime_seconds
    rounds = rows = 0
    with (project / ".git" / "cml-deploy.lock").open("a") as deployment_lock:
        while not _STOP and time.monotonic() < deadline:
            try:
                fcntl.flock(deployment_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                emit("paused", reasons=["deployment_or_scheduled_archive_running"])
                if not args.apply:
                    return 0
                pause(args.pause_seconds, deadline)
                continue
            try:
                try:
                    state = snapshot(project, containers)
                except (
                    RuntimeError,
                    OSError,
                    ValueError,
                    subprocess.TimeoutExpired,
                ) as error:
                    emit(
                        "paused",
                        reasons=["probe_failed"],
                        error_type=type(error).__name__,
                    )
                    if not args.apply:
                        return 1
                    state = None
                if state is not None:
                    reasons = blockers(
                        state,
                        minimum_free_bytes=args.minimum_free_gib * 1024**3,
                        maximum_dead_rows=args.maximum_dead_rows,
                        maximum_load=args.maximum_load,
                    )
                    emit("probe", reasons=reasons, **state)
                    if not args.apply:
                        return 0
                    if not reasons and state["oldest_pending"] is None:
                        emit("completed", rounds=rounds, archived_rows=rows)
                        return 0
                    if not reasons:
                        started = time.monotonic()
                        result = archive_round(
                            project, chunks=args.chunks, deadline=deadline
                        )
                        rounds += 1
                        rows += result["archived_rows"]
                        emit(
                            "round_completed",
                            round=rounds,
                            seconds=round(time.monotonic() - started, 3),
                            archived_rows=result["archived_rows"],
                            archived_chunks=result["archived_chunks"],
                            total_archived_rows=rows,
                        )
                        if result["archived_rows"] == 0:
                            # A nonempty backlog that produces no records is
                            # stalled, not successful completion.
                            raise RuntimeError("pending archive data made no progress")
                        if rounds >= args.max_rounds:
                            emit(
                                "round_limit_reached", rounds=rounds, archived_rows=rows
                            )
                            return 0
            finally:
                fcntl.flock(deployment_lock, fcntl.LOCK_UN)
            # The normal hourly timer and deployments can acquire the shared
            # lock between rounds. Retain the archiver's 0.5 CPU / 512 MiB caps.
            pause(args.pause_seconds, deadline)
    emit(
        "stopped" if _STOP else "runtime_limit_reached",
        rounds=rounds,
        archived_rows=rows,
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project-directory", type=Path, default=Path("/opt/crypto-momentum-lab")
    )
    parser.add_argument(
        "--apply", action="store_true", help="Run archives; default only probes"
    )
    parser.add_argument("--chunks", type=int, default=20)
    parser.add_argument("--max-rounds", type=int, default=200)
    parser.add_argument("--max-runtime-seconds", type=int, default=21600)
    parser.add_argument("--pause-seconds", type=int, default=30)
    parser.add_argument("--minimum-free-gib", type=int, default=8)
    parser.add_argument("--maximum-dead-rows", type=int, default=100000)
    parser.add_argument("--maximum-load", type=float, default=0.85)
    args = parser.parse_args()
    if not 1 <= args.chunks <= 20:
        parser.error("--chunks must be between 1 and 20")
    if (
        min(
            args.max_rounds,
            args.max_runtime_seconds,
            args.pause_seconds,
            args.minimum_free_gib,
            args.maximum_dead_rows,
            args.maximum_load,
        )
        <= 0
    ):
        parser.error("limits and pause must be positive")

    def request_stop(_signal: int, _frame: object) -> None:
        global _STOP
        _STOP = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        with Path("/run/lock/cml-market-revision-catchup.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return run(args)
    except InterruptedError:
        emit("stopped")
        return 0
    except (RuntimeError, OSError, ValueError, subprocess.TimeoutExpired) as error:
        emit("failed", error_type=type(error).__name__, message=str(error))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
