#!/usr/bin/env python3
"""Low-dependency operational checks for the single-host deployment.

The monitor deliberately runs outside the trading processes.  It reads Docker
state, recent structured logs, and a few PostgreSQL counters, then emits a
single JSON alert stream to journald/stdout.  An HTTPS webhook is optional and
is never required for the trading stack to start.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

_DEFAULT_SERVICES = (
    "postgres",
    "market-data",
    "execution-account-live",
    "live-strategy",
)
_DEFAULT_INTERVAL_SECONDS = 60.0
_DEFAULT_LOG_WINDOW_SECONDS = 120.0
_DEFAULT_TELEMETRY_STALE_AFTER_SECONDS = 900.0
_DEFAULT_RSS_WARNING_FRACTION = 0.75
_DEFAULT_RSS_CRITICAL_FRACTION = 0.90
_DEFAULT_RSS_GROWTH_BYTES = 64 * 1024 * 1024
_DEFAULT_RSS_GROWTH_WINDOW_SECONDS = 1_800.0
_DEFAULT_ALERT_COOLDOWN_SECONDS = 900.0
_DEFAULT_COMMAND_TIMEOUT_SECONDS = 15.0


@dataclass(frozen=True, slots=True)
class Alert:
    """One condition that needs operator attention."""

    name: str
    severity: str
    summary: str
    details: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ContainerSnapshot:
    service: str
    container_id: str
    health: str | None
    oom_killed: bool
    restart_count: int
    memory_bytes: int | None
    memory_limit_bytes: int | None


@dataclass(frozen=True, slots=True)
class LogSignals:
    telemetry_persist_failures: int = 0
    dead_connection_tasks: tuple[str, ...] = ()
    latest_rss_bytes: int | None = None
    rss_observed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class DatabaseState:
    latest_event_age_seconds: float | None
    pg_stat_statements_ready: bool
    track_io_timing: bool
    track_wal_io_timing: bool
    max_parallel_maintenance_workers: int | None


def evaluate_database_state(
    *,
    now: datetime,
    latest_event_age_seconds: float | None,
    pg_stat_statements_ready: bool,
    track_io_timing: bool,
    track_wal_io_timing: bool,
    max_parallel_maintenance_workers: int | None,
    stale_after_seconds: float,
) -> tuple[Alert, ...]:
    """Return alerts for telemetry freshness and PostgreSQL observability."""

    del now
    alerts: list[Alert] = []
    if latest_event_age_seconds is None or latest_event_age_seconds < 0:
        alerts.append(
            Alert(
                "telemetry_timestamp_stale",
                "critical",
                "No live runtime telemetry event is available",
                {"age_seconds": None},
            )
        )
    elif latest_event_age_seconds > stale_after_seconds:
        alerts.append(
            Alert(
                "telemetry_timestamp_stale",
                "critical",
                "Live runtime telemetry is older than the freshness budget",
                {
                    "age_seconds": round(latest_event_age_seconds, 3),
                    "threshold_seconds": stale_after_seconds,
                },
            )
        )
    if not pg_stat_statements_ready:
        alerts.append(
            Alert(
                "database_query_stats_unavailable",
                "warning",
                "pg_stat_statements is not loaded",
            )
        )
    if not track_io_timing or not track_wal_io_timing:
        alerts.append(
            Alert(
                "database_io_timing_disabled",
                "warning",
                "PostgreSQL I/O timing is disabled",
                {
                    "track_io_timing": track_io_timing,
                    "track_wal_io_timing": track_wal_io_timing,
                },
            )
        )
    if (
        max_parallel_maintenance_workers is not None
        and max_parallel_maintenance_workers > 0
    ):
        alerts.append(
            Alert(
                "database_parallel_maintenance_enabled",
                "warning",
                "Parallel maintenance is enabled above the OOM guardrail",
                {
                    "max_parallel_maintenance_workers": (
                        max_parallel_maintenance_workers
                    )
                },
            )
        )
    return tuple(alerts)


def evaluate_log_signals(signals: LogSignals) -> tuple[Alert, ...]:
    """Return alerts represented by recent structured application logs."""

    alerts: list[Alert] = []
    if signals.telemetry_persist_failures:
        severity = "critical" if signals.telemetry_persist_failures >= 3 else "warning"
        alerts.append(
            Alert(
                "telemetry_persist_failure",
                severity,
                "Runtime telemetry batches failed to persist",
                {"failure_count": signals.telemetry_persist_failures},
            )
        )
    if signals.dead_connection_tasks:
        alerts.append(
            Alert(
                "market_task_not_alive",
                "critical",
                "A market-data connection task reported not alive",
                {"group_ids": signals.dead_connection_tasks},
            )
        )
    return tuple(alerts)


def evaluate_container(
    snapshot: ContainerSnapshot,
    *,
    rss_warning_fraction: float,
    rss_critical_fraction: float,
) -> tuple[Alert, ...]:
    """Return alerts for Docker lifecycle and memory state."""

    alerts: list[Alert] = []
    if snapshot.oom_killed:
        alerts.append(
            Alert(
                "container_oom_killed",
                "critical",
                f"Container {snapshot.service} was killed by the OOM controller",
                {
                    "service": snapshot.service,
                    "restart_count": snapshot.restart_count,
                },
            )
        )
    if snapshot.health in {"unhealthy", "dead"}:
        alerts.append(
            Alert(
                "container_unhealthy",
                "critical",
                f"Container {snapshot.service} is {snapshot.health}",
                {"service": snapshot.service, "health": snapshot.health},
            )
        )
    if (
        snapshot.memory_bytes is not None
        and snapshot.memory_limit_bytes is not None
        and snapshot.memory_limit_bytes > 0
    ):
        fraction = snapshot.memory_bytes / snapshot.memory_limit_bytes
        if fraction >= rss_critical_fraction:
            alerts.append(
                Alert(
                    "container_memory_high",
                    "critical",
                    f"Container {snapshot.service} memory is near its cgroup limit",
                    {
                        "service": snapshot.service,
                        "memory_bytes": snapshot.memory_bytes,
                        "memory_limit_bytes": snapshot.memory_limit_bytes,
                        "fraction": round(fraction, 4),
                    },
                )
            )
        elif fraction >= rss_warning_fraction:
            alerts.append(
                Alert(
                    "container_memory_high",
                    "warning",
                    (
                        f"Container {snapshot.service} memory is above the "
                        "warning threshold"
                    ),
                    {
                        "service": snapshot.service,
                        "memory_bytes": snapshot.memory_bytes,
                        "memory_limit_bytes": snapshot.memory_limit_bytes,
                        "fraction": round(fraction, 4),
                    },
                )
            )
    return tuple(alerts)


def evaluate_rss_growth(
    *,
    service: str,
    current_bytes: int | None,
    previous_bytes: int | None,
    growth_bytes: int,
) -> tuple[Alert, ...]:
    """Alert when a process RSS sample grows beyond the configured delta."""

    if (
        current_bytes is None
        or previous_bytes is None
        or growth_bytes <= 0
        or current_bytes - previous_bytes < growth_bytes
    ):
        return ()
    return (
        Alert(
            "rss_growth",
            "warning",
            f"Container {service} RSS grew beyond the configured window",
            {
                "service": service,
                "previous_bytes": previous_bytes,
                "current_bytes": current_bytes,
                "growth_bytes": current_bytes - previous_bytes,
                "threshold_bytes": growth_bytes,
            },
        ),
    )


class CommandRunner(Protocol):
    def run(self, args: Sequence[str], *, timeout_seconds: float) -> str: ...


class SubprocessRunner:
    def run(self, args: Sequence[str], *, timeout_seconds: float) -> str:
        result = subprocess.run(
            list(args),
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
        if result.returncode != 0:
            stderr = result.stderr.strip()
            raise RuntimeError(
                f"command failed ({result.returncode}): {' '.join(args)}"
                + (f": {stderr}" if stderr else "")
            )
        return result.stdout


@dataclass(frozen=True, slots=True)
class MonitorConfig:
    project_directory: Path = Path("/opt/crypto-momentum-lab")
    compose_file: Path = Path("/opt/crypto-momentum-lab/compose.server.yaml")
    compose_env_file: Path | None = Path("/opt/crypto-momentum-lab/.env.server")
    services: tuple[str, ...] = _DEFAULT_SERVICES
    live_run_id: str = "live-primary-v1"
    interval_seconds: float = _DEFAULT_INTERVAL_SECONDS
    log_window_seconds: float = _DEFAULT_LOG_WINDOW_SECONDS
    telemetry_stale_after_seconds: float = _DEFAULT_TELEMETRY_STALE_AFTER_SECONDS
    rss_warning_fraction: float = _DEFAULT_RSS_WARNING_FRACTION
    rss_critical_fraction: float = _DEFAULT_RSS_CRITICAL_FRACTION
    rss_growth_bytes: int = _DEFAULT_RSS_GROWTH_BYTES
    rss_growth_window_seconds: float = _DEFAULT_RSS_GROWTH_WINDOW_SECONDS
    alert_cooldown_seconds: float = _DEFAULT_ALERT_COOLDOWN_SECONDS
    command_timeout_seconds: float = _DEFAULT_COMMAND_TIMEOUT_SECONDS
    state_path: Path = Path("/var/lib/crypto-momentum-lab/ops-monitor.json")
    webhook_url: str | None = None


class OpsMonitor:
    def __init__(
        self,
        config: MonitorConfig,
        *,
        runner: CommandRunner | None = None,
        clock: Callable[[], float] = time.time,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if config.interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        if config.log_window_seconds <= 0:
            raise ValueError("log_window_seconds must be positive")
        if not 0 < config.rss_warning_fraction < config.rss_critical_fraction <= 1:
            raise ValueError("RSS thresholds are invalid")
        self._config = config
        self._runner = runner or SubprocessRunner()
        self._clock = clock
        self._sleeper = sleeper
        self._state = _load_state(config.state_path)

    def run_forever(self) -> None:
        while True:
            started_at = self._clock()
            try:
                self.run_once()
            except Exception as error:  # pragma: no cover - last-resort guard
                self._emit(
                    Alert(
                        "ops_monitor_failed",
                        "critical",
                        "Operational monitor iteration failed",
                        {"error_type": type(error).__name__, "error": str(error)},
                    ),
                    now=started_at,
                )
            elapsed = self._clock() - started_at
            self._sleeper(max(0.0, self._config.interval_seconds - elapsed))

    def run_once(self) -> tuple[Alert, ...]:
        now = self._clock()
        alerts: list[Alert] = []
        containers = self._container_snapshots()
        seen_services = {snapshot.service for snapshot in containers}
        for service in self._config.services:
            if service not in seen_services:
                alerts.append(
                    Alert(
                        "container_missing",
                        "critical",
                        f"Container {service} is missing from the Compose project",
                        {"service": service},
                    )
                )
        for snapshot in containers:
            alerts.extend(
                evaluate_container(
                    snapshot,
                    rss_warning_fraction=self._config.rss_warning_fraction,
                    rss_critical_fraction=self._config.rss_critical_fraction,
                )
            )
            alerts.extend(
                self._rss_alerts(snapshot.service, snapshot.memory_bytes, now)
            )

        market_id = self._container_id("market-data")
        live_id = self._container_id("live-strategy")
        combined_signals = self._log_signals(
            market_id,
            live_id,
            since_seconds=self._config.log_window_seconds,
        )
        alerts.extend(evaluate_log_signals(combined_signals))
        if combined_signals.latest_rss_bytes is not None:
            alerts.extend(
                self._rss_alerts(
                    "market-data-rss",
                    combined_signals.latest_rss_bytes,
                    now,
                )
            )

        postgres_id = self._container_id("postgres")
        if postgres_id is not None:
            try:
                database_state = self._database_state(postgres_id)
            except Exception as error:
                alerts.append(
                    Alert(
                        "database_check_failed",
                        "critical",
                        "PostgreSQL observability query failed",
                        {"error_type": type(error).__name__, "error": str(error)},
                    )
                )
            else:
                alerts.extend(
                    evaluate_database_state(
                        now=datetime.fromtimestamp(now, UTC),
                        latest_event_age_seconds=database_state.latest_event_age_seconds,
                        pg_stat_statements_ready=database_state.pg_stat_statements_ready,
                        track_io_timing=database_state.track_io_timing,
                        track_wal_io_timing=database_state.track_wal_io_timing,
                        max_parallel_maintenance_workers=(
                            database_state.max_parallel_maintenance_workers
                        ),
                        stale_after_seconds=self._config.telemetry_stale_after_seconds,
                    )
                )

        active_keys = {alert.name for alert in alerts}
        for alert in alerts:
            self._emit(alert, now=now)
        self._emit_resolutions(active_keys, now=now)
        _save_state(self._config.state_path, self._state)
        return tuple(alerts)

    def _container_id(self, service: str) -> str | None:
        command = self._compose_prefix()
        command.extend(["ps", "-q", service])
        try:
            output = self._runner.run(
                command,
                timeout_seconds=self._config.command_timeout_seconds,
            )
        except Exception:
            return None
        value = next((line.strip() for line in output.splitlines() if line.strip()), "")
        return value or None

    def _container_snapshots(self) -> tuple[ContainerSnapshot, ...]:
        snapshots: list[ContainerSnapshot] = []
        for service in self._config.services:
            container_id = self._container_id(service)
            if container_id is None:
                continue
            payload = json.loads(
                self._runner.run(
                    ["docker", "inspect", container_id],
                    timeout_seconds=self._config.command_timeout_seconds,
                )
            )[0]
            state = payload.get("State", {})
            health = state.get("Health") or {}
            memory_bytes, memory_limit_bytes = self._memory_stats(container_id)
            snapshots.append(
                ContainerSnapshot(
                    service=service,
                    container_id=container_id,
                    health=health.get("Status"),
                    oom_killed=bool(state.get("OOMKilled", False)),
                    restart_count=int(payload.get("RestartCount", 0)),
                    memory_bytes=memory_bytes,
                    memory_limit_bytes=memory_limit_bytes,
                )
            )
        return tuple(snapshots)

    def _memory_stats(self, container_id: str) -> tuple[int | None, int | None]:
        payload = json.loads(
            self._runner.run(
                ["docker", "inspect", container_id],
                timeout_seconds=self._config.command_timeout_seconds,
            )
        )[0]
        memory_limit = int(payload.get("HostConfig", {}).get("Memory", 0) or 0)
        try:
            stats = self._runner.run(
                [
                    "docker",
                    "stats",
                    "--no-stream",
                    "--format",
                    "{{.MemUsage}}",
                    container_id,
                ],
                timeout_seconds=self._config.command_timeout_seconds,
            ).strip()
            memory_text = stats.split("/", 1)[0].strip()
            memory_bytes = _parse_size(memory_text)
        except Exception:
            memory_bytes = None
        return memory_bytes, memory_limit or None

    def _log_signals(
        self,
        market_id: str | None,
        live_id: str | None,
        *,
        since_seconds: float,
    ) -> LogSignals:
        telemetry_failures = 0
        dead_tasks: list[str] = []
        latest_rss: int | None = None
        latest_rss_at: datetime | None = None
        for container_id in (market_id, live_id):
            if container_id is None:
                continue
            try:
                output = self._runner.run(
                    [
                        "docker",
                        "logs",
                        "--since",
                        f"{int(since_seconds)}s",
                        "--timestamps",
                        container_id,
                    ],
                    timeout_seconds=self._config.command_timeout_seconds,
                )
            except Exception:
                continue
            for line in output.splitlines():
                record = _parse_log_record(line)
                event = str(record.get("event", ""))
                if event == "live_runtime_telemetry_persist_failed":
                    telemetry_failures += 1
                elif event == "market_data_connection_task_not_alive":
                    values = record.get("group_ids")
                    if isinstance(values, list | tuple):
                        dead_tasks.extend(str(value) for value in values)
                    elif values:
                        dead_tasks.append(str(values))
                elif event == "market_data_health_snapshot":
                    value = record.get("rss_bytes")
                    if isinstance(value, int) and (
                        latest_rss_at is None
                        or _record_timestamp(record) >= latest_rss_at
                    ):
                        latest_rss = value
                        latest_rss_at = _record_timestamp(record)
        return LogSignals(
            telemetry_persist_failures=telemetry_failures,
            dead_connection_tasks=tuple(sorted(set(dead_tasks))),
            latest_rss_bytes=latest_rss,
            rss_observed_at=latest_rss_at,
        )

    def _database_state(self, container_id: str) -> DatabaseState:
        run_id = _sql_literal(self._config.live_run_id)
        sql = f"""
SELECT 'event_age' || E'\\t' || COALESCE(
  EXTRACT(EPOCH FROM (clock_timestamp() - max(occurred_at)))::text, '-1'
)
FROM strategy_runtime_events WHERE run_id = {run_id};
SELECT 'pg_stat_statements' || E'\\t' || (
  EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')
  AND position('pg_stat_statements' in current_setting('shared_preload_libraries')) > 0
);
SELECT 'track_io_timing' || E'\\t' || current_setting('track_io_timing');
SELECT 'track_wal_io_timing' || E'\\t' || current_setting('track_wal_io_timing');
SELECT 'parallel_maintenance' || E'\\t' || current_setting(
  'max_parallel_maintenance_workers'
);
"""
        output = self._runner.run(
            [
                "docker",
                "exec",
                container_id,
                "psql",
                "-At",
                "-q",
                "-U",
                "cml",
                "-d",
                "cml",
                "-c",
                sql,
            ],
            timeout_seconds=self._config.command_timeout_seconds,
        )
        values: dict[str, str] = {}
        for line in output.splitlines():
            key, separator, value = line.partition("\t")
            if separator:
                values[key] = value.strip()
        age = _parse_float(values.get("event_age"))
        return DatabaseState(
            latest_event_age_seconds=None if age is None or age < 0 else age,
            pg_stat_statements_ready=_parse_bool(values.get("pg_stat_statements")),
            track_io_timing=_parse_bool(values.get("track_io_timing")),
            track_wal_io_timing=_parse_bool(values.get("track_wal_io_timing")),
            max_parallel_maintenance_workers=_parse_int(
                values.get("parallel_maintenance")
            ),
        )

    def _rss_alerts(
        self,
        service: str,
        current_bytes: int | None,
        now: float,
    ) -> tuple[Alert, ...]:
        samples = self._state.setdefault("rss_samples", {}).setdefault(service, [])
        if not isinstance(samples, list):
            samples = []
            self._state.setdefault("rss_samples", {})[service] = samples
        cutoff = now - self._config.rss_growth_window_seconds
        previous_bytes: int | None = None
        retained: list[list[float | int]] = []
        for sample in samples:
            if (
                isinstance(sample, list)
                and len(sample) == 2
                and isinstance(sample[0], int | float)
                and isinstance(sample[1], int)
                and sample[0] >= cutoff
            ):
                retained.append(sample)
                previous_bytes = sample[1]
        if current_bytes is not None:
            retained.append([now, current_bytes])
        self._state.setdefault("rss_samples", {})[service] = retained[-120:]
        return evaluate_rss_growth(
            service=service,
            current_bytes=current_bytes,
            previous_bytes=previous_bytes,
            growth_bytes=self._config.rss_growth_bytes,
        )

    def _compose_prefix(self) -> list[str]:
        command = [
            "docker",
            "compose",
            "--project-directory",
            str(self._config.project_directory),
        ]
        if self._config.compose_env_file is not None:
            command.extend(["--env-file", str(self._config.compose_env_file)])
        command.extend(["-f", str(self._config.compose_file)])
        return command

    def _emit(self, alert: Alert, *, now: float) -> None:
        active = self._state.setdefault("active_alerts", {})
        previous = active.get(alert.name)
        if isinstance(previous, (int, float)) and (
            now - previous < self._config.alert_cooldown_seconds
        ):
            return
        active[alert.name] = now
        payload = {
            "event": "ops_alert",
            "observed_at": datetime.fromtimestamp(now, UTC).isoformat(),
            "alert_name": alert.name,
            "severity": alert.severity,
            "summary": alert.summary,
            "details": dict(alert.details),
        }
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)
        _deliver_webhook(self._config.webhook_url, payload)

    def _emit_resolutions(self, active_keys: set[str], *, now: float) -> None:
        active = self._state.setdefault("active_alerts", {})
        for name in tuple(active):
            if name in active_keys:
                continue
            payload = {
                "event": "ops_alert_resolved",
                "observed_at": datetime.fromtimestamp(now, UTC).isoformat(),
                "alert_name": name,
            }
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)
            _deliver_webhook(self._config.webhook_url, payload)
            active.pop(name, None)


def _parse_log_record(line: str) -> dict[str, object]:
    start = line.find("{")
    if start < 0:
        return {"event": line}
    try:
        value = json.loads(line[start:])
    except json.JSONDecodeError:
        return {"event": line}
    return value if isinstance(value, dict) else {"event": line}


def _record_timestamp(record: Mapping[str, object]) -> datetime:
    value = record.get("timestamp") or record.get("asctime")
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            pass
    return datetime.min.replace(tzinfo=UTC)


_SIZE_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)?\s*$")


def _parse_size(value: str) -> int | None:
    match = _SIZE_RE.match(value)
    if match is None:
        return None
    number = float(match.group(1))
    suffix = (match.group(2) or "B").lower()
    multipliers = {
        "b": 1,
        "kb": 1_000,
        "kib": 1_024,
        "mb": 1_000_000,
        "mib": 1_048_576,
        "gb": 1_000_000_000,
        "gib": 1_073_741_824,
        "tb": 1_000_000_000_000,
        "tib": 1_099_511_627_776,
    }
    multiplier = multipliers.get(suffix)
    return None if multiplier is None else int(number * multiplier)


def _parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _parse_bool(value: str | None) -> bool:
    """Parse the boolean spellings emitted by PostgreSQL's text output."""

    return (value or "").strip().lower() in {"1", "on", "t", "true", "yes"}


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _load_state(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _save_state(path: Path, state: Mapping[str, object]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(state, ensure_ascii=False, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    except OSError as error:
        print(
            json.dumps(
                {
                    "event": "ops_monitor_state_write_failed",
                    "error_type": type(error).__name__,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )


def _deliver_webhook(url: str | None, payload: Mapping[str, object]) -> None:
    if not url:
        return
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5):
            pass
    except Exception as error:  # pragma: no cover - external endpoint
        print(
            json.dumps(
                {
                    "event": "ops_alert_delivery_failed",
                    "error_type": type(error).__name__,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )


def _env_path(name: str, default: Path | None) -> Path | None:
    value = os.environ.get(name)
    if value is None:
        return default
    return Path(value) if value else None


def build_config(args: argparse.Namespace) -> MonitorConfig:
    services = tuple(
        item.strip()
        for item in args.services.split(",")
        if item.strip()
    )
    return MonitorConfig(
        project_directory=Path(args.project_directory),
        compose_file=Path(args.compose_file),
        compose_env_file=_env_path("CML_COMPOSE_ENV_FILE", None),
        services=services or _DEFAULT_SERVICES,
        live_run_id=args.live_run_id,
        interval_seconds=args.interval_seconds,
        log_window_seconds=args.log_window_seconds,
        telemetry_stale_after_seconds=args.telemetry_stale_after_seconds,
        rss_warning_fraction=args.rss_warning_fraction,
        rss_critical_fraction=args.rss_critical_fraction,
        rss_growth_bytes=args.rss_growth_bytes,
        rss_growth_window_seconds=args.rss_growth_window_seconds,
        alert_cooldown_seconds=args.alert_cooldown_seconds,
        command_timeout_seconds=args.command_timeout_seconds,
        state_path=Path(args.state_path),
        webhook_url=os.environ.get("CML_ALERT_WEBHOOK_URL") or None,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project-directory",
        default=os.environ.get("CML_PROJECT_DIRECTORY", "/opt/crypto-momentum-lab"),
    )
    parser.add_argument(
        "--compose-file",
        default=os.environ.get(
            "CML_COMPOSE_FILE",
            "/opt/crypto-momentum-lab/compose.server.yaml",
        ),
    )
    parser.add_argument(
        "--services",
        default=os.environ.get("CML_MONITOR_SERVICES", ",".join(_DEFAULT_SERVICES)),
    )
    parser.add_argument(
        "--live-run-id",
        default=os.environ.get("CML_LIVE_SESSION_ID", "live-primary-v1"),
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=_DEFAULT_INTERVAL_SECONDS,
    )
    parser.add_argument(
        "--log-window-seconds",
        type=float,
        default=_DEFAULT_LOG_WINDOW_SECONDS,
    )
    parser.add_argument(
        "--telemetry-stale-after-seconds",
        type=float,
        default=_DEFAULT_TELEMETRY_STALE_AFTER_SECONDS,
    )
    parser.add_argument(
        "--rss-warning-fraction",
        type=float,
        default=_DEFAULT_RSS_WARNING_FRACTION,
    )
    parser.add_argument(
        "--rss-critical-fraction",
        type=float,
        default=_DEFAULT_RSS_CRITICAL_FRACTION,
    )
    parser.add_argument(
        "--rss-growth-bytes",
        type=int,
        default=_DEFAULT_RSS_GROWTH_BYTES,
    )
    parser.add_argument(
        "--rss-growth-window-seconds",
        type=float,
        default=_DEFAULT_RSS_GROWTH_WINDOW_SECONDS,
    )
    parser.add_argument(
        "--alert-cooldown-seconds",
        type=float,
        default=float(
            os.environ.get(
                "CML_ALERT_COOLDOWN_SECONDS",
                _DEFAULT_ALERT_COOLDOWN_SECONDS,
            )
        ),
    )
    parser.add_argument(
        "--command-timeout-seconds",
        type=float,
        default=_DEFAULT_COMMAND_TIMEOUT_SECONDS,
    )
    parser.add_argument(
        "--state-path",
        default=os.environ.get(
            "CML_OPS_MONITOR_STATE_PATH",
            "/var/lib/crypto-momentum-lab/ops-monitor.json",
        ),
    )
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    monitor = OpsMonitor(build_config(args))
    if args.once:
        monitor.run_once()
    else:
        monitor.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
