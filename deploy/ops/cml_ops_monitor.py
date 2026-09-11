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
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

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
_DEFAULT_LIVE_RESTART_COOLDOWN_SECONDS = 900.0
_DEFAULT_LIVE_RESTART_MAX_ATTEMPTS = 3
_COMPOSE_SERVICE_HEADER = re.compile(
    r"^  (?P<service>[A-Za-z0-9][A-Za-z0-9_-]*):\s*$"
)


def _live_strategy_service(account_label: str) -> str:
    """Map a configured account label to its Compose strategy service."""

    return (
        "live-strategy"
        if account_label == "primary"
        else f"live-strategy-{account_label}"
    )


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
    legacy_order_identity_conflicts: int = 0
    dead_connection_tasks: tuple[str, ...] = ()
    latest_rss_bytes: int | None = None
    rss_observed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class DatabaseState:
    latest_checkpoint_age_seconds: float | None
    live_session_ready: bool
    pg_stat_statements_ready: bool
    track_io_timing: bool
    track_wal_io_timing: bool
    max_parallel_maintenance_workers: int | None


def evaluate_database_state(
    *,
    now: datetime,
    latest_checkpoint_age_seconds: float | None,
    live_session_ready: bool,
    pg_stat_statements_ready: bool,
    track_io_timing: bool,
    track_wal_io_timing: bool,
    max_parallel_maintenance_workers: int | None,
    stale_after_seconds: float,
) -> tuple[Alert, ...]:
    """Return alerts for live liveness and PostgreSQL observability."""

    del now
    alerts: list[Alert] = []
    if not live_session_ready:
        alerts.append(
            Alert(
                "live_session_not_ready",
                "critical",
                "Live session checkpoint or lease is not ready",
            )
        )
    elif (
        latest_checkpoint_age_seconds is None
        or latest_checkpoint_age_seconds < 0
        or latest_checkpoint_age_seconds > stale_after_seconds
    ):
        alerts.append(
            Alert(
                "live_checkpoint_stale",
                "critical",
                "Live strategy checkpoint is older than the freshness budget",
                {
                    "age_seconds": (
                        None
                        if latest_checkpoint_age_seconds is None
                        else round(latest_checkpoint_age_seconds, 3)
                    ),
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
    if signals.legacy_order_identity_conflicts:
        alerts.append(
            Alert(
                "live_legacy_order_identity_conflict",
                "critical",
                "Live order identity was reused across multiple exchange orders",
                {
                    "conflict_count": (
                        signals.legacy_order_identity_conflicts
                    )
                },
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
    compose_files: tuple[Path, ...] = ()
    compose_profiles: tuple[str, ...] = ()
    compose_env_file: Path | None = Path("/opt/crypto-momentum-lab/.env.server")
    services: tuple[str, ...] = _DEFAULT_SERVICES
    live_accounts: tuple[tuple[str, str, str], ...] = (
        ("primary", "live-primary-v1", "live-worker"),
    )
    live_run_id: str = "live-primary-v1"
    live_account_label: str = "primary"
    live_lease_owner: str = "live-worker"
    interval_seconds: float = _DEFAULT_INTERVAL_SECONDS
    log_window_seconds: float = _DEFAULT_LOG_WINDOW_SECONDS
    telemetry_stale_after_seconds: float = _DEFAULT_TELEMETRY_STALE_AFTER_SECONDS
    rss_warning_fraction: float = _DEFAULT_RSS_WARNING_FRACTION
    rss_critical_fraction: float = _DEFAULT_RSS_CRITICAL_FRACTION
    rss_growth_bytes: int = _DEFAULT_RSS_GROWTH_BYTES
    rss_growth_window_seconds: float = _DEFAULT_RSS_GROWTH_WINDOW_SECONDS
    alert_cooldown_seconds: float = _DEFAULT_ALERT_COOLDOWN_SECONDS
    command_timeout_seconds: float = _DEFAULT_COMMAND_TIMEOUT_SECONDS
    auto_restart_stale_live_services: bool = True
    live_restart_cooldown_seconds: float = _DEFAULT_LIVE_RESTART_COOLDOWN_SECONDS
    live_restart_max_attempts: int = _DEFAULT_LIVE_RESTART_MAX_ATTEMPTS
    state_path: Path = Path("/var/lib/crypto-momentum-lab/ops-monitor.json")
    webhook_url: str | None = None
    serverchan_sendkey: str | None = None


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
        if config.live_restart_cooldown_seconds <= 0:
            raise ValueError("live_restart_cooldown_seconds must be positive")
        if config.live_restart_max_attempts <= 0:
            raise ValueError("live_restart_max_attempts must be positive")
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
        live_strategy_accounts = {
            _live_strategy_service(account_label): account_label
            for account_label, _run_id, _lease_owner in self._config.live_accounts
        }
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
            account_label = live_strategy_accounts.get(snapshot.service)
            if account_label is not None:
                alerts.extend(
                    self._live_heartbeat_alerts(snapshot, account_label, now)
                )

        market_id = self._container_id("market-data")
        strategy_services = tuple(
            _live_strategy_service(account_label)
            for account_label, _run_id, _lease_owner in self._config.live_accounts
        )
        combined_signals = LogSignals()
        for strategy_service in strategy_services:
            live_id = self._container_id(strategy_service)
            signals = self._log_signals(
                market_id,
                live_id,
                since_seconds=self._config.log_window_seconds,
            )
            combined_signals = LogSignals(
                telemetry_persist_failures=(
                    combined_signals.telemetry_persist_failures
                    + signals.telemetry_persist_failures
                ),
                legacy_order_identity_conflicts=(
                    combined_signals.legacy_order_identity_conflicts
                    + signals.legacy_order_identity_conflicts
                ),
                dead_connection_tasks=(
                    *combined_signals.dead_connection_tasks,
                    *signals.dead_connection_tasks,
                ),
                latest_rss_bytes=(
                    signals.latest_rss_bytes
                    if signals.latest_rss_bytes is not None
                    else combined_signals.latest_rss_bytes
                ),
                rss_observed_at=(
                    signals.rss_observed_at
                    if signals.rss_observed_at is not None
                    else combined_signals.rss_observed_at
                ),
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
            for account_label, run_id, lease_owner in self._config.live_accounts:
                try:
                    database_state = self._database_state(
                        postgres_id,
                        live_run_id=run_id,
                        live_account_label=account_label,
                        live_lease_owner=lease_owner,
                    )
                except Exception as error:
                    alerts.append(
                        Alert(
                            f"database_check_failed:{account_label}",
                            "critical",
                            "PostgreSQL observability query failed",
                            {
                                "account_label": account_label,
                                "error_type": type(error).__name__,
                                "error": str(error),
                            },
                        )
                    )
                else:
                    account_alerts = evaluate_database_state(
                        now=datetime.fromtimestamp(now, UTC),
                        latest_checkpoint_age_seconds=(
                            database_state.latest_checkpoint_age_seconds
                        ),
                        live_session_ready=database_state.live_session_ready,
                        pg_stat_statements_ready=database_state.pg_stat_statements_ready,
                        track_io_timing=database_state.track_io_timing,
                        track_wal_io_timing=database_state.track_wal_io_timing,
                        max_parallel_maintenance_workers=(
                            database_state.max_parallel_maintenance_workers
                        ),
                        stale_after_seconds=self._config.telemetry_stale_after_seconds,
                    )
                    alerts.extend(
                        replace(
                            alert,
                            name=f"{alert.name}:{account_label}",
                            details={
                                **alert.details,
                                "account_label": account_label,
                            },
                        )
                        for alert in account_alerts
                    )

        active_keys = {alert.name for alert in alerts}
        for alert in alerts:
            self._emit(alert, now=now)
        self._emit_resolutions(active_keys, now=now)
        _save_state(self._config.state_path, self._state)
        return tuple(alerts)

    def _live_heartbeat_alerts(
        self,
        snapshot: ContainerSnapshot,
        account_label: str,
        now: float,
    ) -> tuple[Alert, ...]:
        """Alert on a stale live marker and restart that account's service.

        Docker's healthcheck reads the worker's local heartbeat marker, so an
        ``unhealthy`` live strategy is the host-side representation of a
        stale heartbeat.  Restart state is kept per Compose service so one
        frozen account cannot restart another account or consume its retry
        budget.
        """

        restart_states = self._state.setdefault("live_restart_state", {})
        if not isinstance(restart_states, dict):
            restart_states = {}
            self._state["live_restart_state"] = restart_states

        state = restart_states.get(snapshot.service)
        if not isinstance(state, dict):
            state = {}
            restart_states[snapshot.service] = state

        if snapshot.health == "healthy":
            restart_states.pop(snapshot.service, None)
            return ()

        last_restart_at = state.get("last_restart_at")
        if not isinstance(last_restart_at, int | float) or isinstance(
            last_restart_at, bool
        ):
            last_restart_at = None
        restart_attempts = state.get("restart_attempts", 0)
        if not isinstance(restart_attempts, int) or isinstance(
            restart_attempts, bool
        ):
            restart_attempts = 0

        details = {
            "account_label": account_label,
            "service": snapshot.service,
            "health": snapshot.health,
            "container_id": snapshot.container_id,
            "restart_count": snapshot.restart_count,
        }
        if "first_unhealthy_at" not in state:
            state["first_unhealthy_at"] = now
        stale = snapshot.health in {"unhealthy", "dead"}
        if not stale:
            if last_restart_at is None:
                restart_states.pop(snapshot.service, None)
                return ()
            details.update(
                {
                    "restart_attempts": restart_attempts,
                    "last_restart_at": last_restart_at,
                }
            )
            if state.get("last_restart_succeeded") is False:
                return (
                    Alert(
                        f"live_heartbeat_restart_failed:{account_label}",
                        "critical",
                        "Automatic live strategy restart failed",
                        {
                            **details,
                            "error_type": state.get("last_restart_error_type"),
                            "error": state.get("last_restart_error"),
                        },
                    ),
                )
            return (
                Alert(
                    f"live_heartbeat_auto_restarted:{account_label}",
                    "warning",
                    "Live strategy restart is in progress",
                    details,
                ),
            )

        stale_alert = Alert(
            f"live_heartbeat_stale:{account_label}",
            "critical",
            "Live strategy heartbeat is stale",
            {
                **details,
                "first_unhealthy_at": state["first_unhealthy_at"],
            },
        )
        alerts = [stale_alert]
        if not self._config.auto_restart_stale_live_services:
            return tuple(alerts)

        if restart_attempts >= self._config.live_restart_max_attempts:
            alerts.append(
                Alert(
                    f"live_heartbeat_restart_suppressed:{account_label}",
                    "critical",
                    "Automatic live strategy restart limit reached",
                    {
                        **details,
                        "restart_attempts": restart_attempts,
                        "max_attempts": self._config.live_restart_max_attempts,
                        "cooldown_seconds": (
                            self._config.live_restart_cooldown_seconds
                        ),
                    },
                )
            )
            return tuple(alerts)

        if (
            last_restart_at is not None
            and now - last_restart_at < self._config.live_restart_cooldown_seconds
        ):
            details.update(
                {
                    "restart_attempts": restart_attempts,
                    "last_restart_at": last_restart_at,
                    "cooldown_seconds": self._config.live_restart_cooldown_seconds,
                }
            )
            if state.get("last_restart_succeeded") is False:
                alerts.append(
                    Alert(
                        f"live_heartbeat_restart_failed:{account_label}",
                        "critical",
                        "Automatic live strategy restart failed",
                        {
                            **details,
                            "error_type": state.get("last_restart_error_type"),
                            "error": state.get("last_restart_error"),
                        },
                    )
                )
            else:
                alerts.append(
                    Alert(
                        f"live_heartbeat_auto_restarted:{account_label}",
                        "warning",
                        "Live strategy restart is awaiting health recovery",
                        details,
                    )
                )
            return tuple(alerts)

        attempt = restart_attempts + 1
        state.update(
            {
                "last_restart_at": now,
                "restart_attempts": attempt,
                "last_restart_succeeded": False,
            }
        )
        restart_command = [
            *self._compose_prefix(),
            "restart",
            snapshot.service,
        ]
        try:
            self._runner.run(
                restart_command,
                timeout_seconds=self._config.command_timeout_seconds,
            )
        except Exception as error:
            state.update(
                {
                    "last_restart_error_type": type(error).__name__,
                    "last_restart_error": str(error),
                }
            )
            alerts.append(
                Alert(
                    f"live_heartbeat_restart_failed:{account_label}",
                    "critical",
                    "Automatic live strategy restart failed",
                    {
                        **details,
                        "attempt": attempt,
                        "error_type": type(error).__name__,
                        "error": str(error),
                    },
                )
            )
        else:
            state["last_restart_succeeded"] = True
            state.pop("last_restart_error_type", None)
            state.pop("last_restart_error", None)
            alerts.append(
                Alert(
                    f"live_heartbeat_auto_restarted:{account_label}",
                    "warning",
                    "Stale live strategy heartbeat triggered an automatic restart",
                    {
                        **details,
                        "attempt": attempt,
                        "cooldown_seconds": (
                            self._config.live_restart_cooldown_seconds
                        ),
                    },
                )
            )
        return tuple(alerts)

    def _container_id(self, service: str) -> str | None:
        # Docker labels avoid re-interpolating every Compose file on each
        # monitor tick. An optional live overlay may contain required secret
        # variables for accounts that are not enabled on this host.
        command = [
            "docker",
            "ps",
            "--filter",
            f"label=com.docker.compose.service={service}",
            "--format",
            "{{.ID}}",
        ]
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
        legacy_order_identity_conflicts = 0
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
                elif event == "live_legacy_order_identity_conflict":
                    legacy_order_identity_conflicts += 1
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
            legacy_order_identity_conflicts=legacy_order_identity_conflicts,
            dead_connection_tasks=tuple(sorted(set(dead_tasks))),
            latest_rss_bytes=latest_rss,
            rss_observed_at=latest_rss_at,
        )

    def _database_state(
        self,
        container_id: str,
        *,
        live_run_id: str | None = None,
        live_account_label: str | None = None,
        live_lease_owner: str | None = None,
    ) -> DatabaseState:
        run_id = _sql_literal(live_run_id or self._config.live_run_id)
        account_label = _sql_literal(
            live_account_label or self._config.live_account_label
        )
        lease_owner = _sql_literal(
            live_lease_owner or self._config.live_lease_owner
        )
        sql = f"""
SELECT 'checkpoint_age' || E'\\t' || COALESCE(
  EXTRACT(EPOCH FROM (clock_timestamp() - max(saved_at)))::text, '-1'
)
FROM strategy_runtime_checkpoints WHERE run_id = {run_id};
SELECT 'live_ready' || E'\\t' || (
  EXISTS (
    SELECT 1 FROM live_session_transitions
    WHERE session_id = {run_id}
      AND state IN ('live_enabled', 'draining')
  )
  AND EXISTS (
    SELECT 1 FROM trading_leases
    WHERE environment = 'live'
      AND account_label = {account_label}
      AND owner = {lease_owner}
      AND state = 'active'
      AND expires_at > clock_timestamp()
  )
  AND EXISTS (
    SELECT 1 FROM strategy_runtime_checkpoints
    WHERE run_id = {run_id}
  )
);
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
        age = _parse_float(values.get("checkpoint_age"))
        return DatabaseState(
            latest_checkpoint_age_seconds=None if age is None or age < 0 else age,
            live_session_ready=_parse_bool(values.get("live_ready")),
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
        compose_files = self._config.compose_files or (self._config.compose_file,)
        for compose_file in compose_files:
            command.extend(["-f", str(compose_file)])
        for profile in self._config.compose_profiles:
            command.extend(["--profile", profile])
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
        _deliver_notification(
            self._config.webhook_url,
            self._config.serverchan_sendkey,
            payload,
        )

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
            _deliver_notification(
                self._config.webhook_url,
                self._config.serverchan_sendkey,
                payload,
            )
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


def _parse_env_bool(value: str | None, *, default: bool) -> bool:
    """Parse a monitor boolean and fail closed on an invalid override."""

    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "on", "t", "true", "yes"}:
        return True
    if normalized in {"0", "off", "f", "false", "no"}:
        return False
    raise ValueError(f"invalid boolean value: {value!r}")


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _load_state(path: Path) -> dict[str, Any]:
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


def _deliver_notification(
    webhook_url: str | None,
    serverchan_sendkey: str | None,
    payload: Mapping[str, object],
) -> None:
    if serverchan_sendkey:
        _deliver_serverchan(serverchan_sendkey, payload)
        return
    _deliver_webhook(webhook_url, payload)


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


def _deliver_serverchan(
    sendkey: str,
    payload: Mapping[str, object],
) -> None:
    try:
        request = urllib.request.Request(
            _serverchan_endpoint(sendkey),
            data=urllib.parse.urlencode(
                _serverchan_form(payload),
                doseq=False,
            ).encode("utf-8"),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            result = json.loads(response.read().decode("utf-8"))
        if not isinstance(result, dict) or result.get("code") not in {0, "0"}:
            raise RuntimeError("Server酱 returned a non-zero response")
    except Exception as error:  # pragma: no cover - external endpoint
        print(
            json.dumps(
                {
                    "event": "ops_alert_delivery_failed",
                    "error_type": type(error).__name__,
                    "provider": "serverchan",
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )


def _serverchan_endpoint(sendkey: str) -> str:
    key = sendkey.strip()
    if not key:
        raise ValueError("Server酱 SendKey must not be empty")
    sc3_match = re.match(r"^sctp([0-9]+)t", key)
    if sc3_match is not None:
        uid = sc3_match.group(1)
        return (
            f"https://{uid}.push.ft07.com/send/"
            f"{urllib.parse.quote(key, safe='')}.send"
        )
    return f"https://sctapi.ftqq.com/{urllib.parse.quote(key, safe='')}.send"


def _serverchan_form(payload: Mapping[str, object]) -> dict[str, str]:
    event = str(payload.get("event", "ops_alert"))
    alert_name = str(payload.get("alert_name", "ops_monitor"))
    if event == "ops_alert":
        severity = str(payload.get("severity", "critical")).upper()
        title = f"CML告警: {alert_name}"
        summary = str(payload.get("summary", "Operational alert"))
        details = payload.get("details", {})
        body = [
            f"## {summary}",
            f"- **级别**：`{severity}`",
            f"- **告警**：`{alert_name}`",
            f"- **时间**：`{payload.get('observed_at', '')}",
        ]
        if details:
            body.append(
                "- **详情**：\n```json\n"
                + json.dumps(details, ensure_ascii=False, sort_keys=True)
                + "\n```"
            )
    else:
        title = f"CML恢复: {alert_name}"
        body = [
            f"## 监控恢复：{alert_name}",
            f"- **时间**：{payload.get('observed_at', '')}",
        ]
    return {
        "title": " ".join(title.split())[:32],
        "desp": "\n".join(body),
    }


def _env_path(name: str, default: Path | None) -> Path | None:
    value = os.environ.get(name)
    if value is None:
        return default
    return Path(value) if value else None


def _read_env_value(path: Path | None, name: str) -> str | None:
    """Read one non-secret value from a Compose-style environment file."""

    if path is None:
        return None
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    for line in lines:
        candidate = line.strip()
        if not candidate or candidate.startswith("#"):
            continue
        if candidate.startswith("export "):
            candidate = candidate[7:].lstrip()
        key, separator, value = candidate.partition("=")
        if separator and key.strip() == name:
            value = value.strip()
            if value[:1] == value[-1:] and value[:1] in {"'", '"'}:
                value = value[1:-1]
            return value
    return None


def _parse_live_accounts(raw_value: str | None) -> tuple[tuple[str, str, str], ...]:
    if raw_value is None or not raw_value.strip():
        return (("primary", "live-primary-v1", "live-worker"),)
    accounts: list[tuple[str, str, str]] = []
    for item in raw_value.split(","):
        parts = tuple(part.strip() for part in item.split("|"))
        if len(parts) != 3 or any(not part for part in parts):
            raise ValueError(
                "CML_MONITOR_LIVE_ACCOUNTS must use "
                "label|session-id|lease-owner entries"
            )
        accounts.append((parts[0], parts[1], parts[2]))
    return tuple(accounts)


def _compose_service_names(compose_files: Sequence[Path]) -> tuple[str, ...]:
    """Read top-level service names without interpolating Compose secrets."""

    service_names: list[str] = []
    for compose_file in compose_files:
        try:
            lines = compose_file.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        in_services = False
        for line in lines:
            if line.strip() == "services:" and not line.startswith(" "):
                in_services = True
                continue
            if in_services and line and not line.startswith(" "):
                in_services = False
            if not in_services:
                continue
            match = _COMPOSE_SERVICE_HEADER.match(line)
            if match is not None:
                service = match.group("service")
                if service not in service_names:
                    service_names.append(service)
    return tuple(service_names)


def _live_account_label_for_service(service: str) -> str | None:
    for prefix in ("execution-account-live", "live-strategy"):
        if service == prefix:
            return "primary"
        prefix_with_separator = f"{prefix}-"
        if service.startswith(prefix_with_separator):
            return service[len(prefix_with_separator) :]
    return None


def _discover_live_account_labels(
    compose_files: Sequence[Path],
) -> tuple[str, ...]:
    labels: list[str] = []
    for service in _compose_service_names(compose_files):
        label = _live_account_label_for_service(service)
        if label is not None and label not in labels:
            labels.append(label)
    if "primary" in labels:
        labels.remove("primary")
        labels.insert(0, "primary")
    return tuple(labels) or ("primary",)


def _live_account_env_suffix(account_label: str) -> str:
    if account_label == "primary":
        return ""
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", account_label).strip("_")
    return f"_{normalized.upper()}"


def _configured_env_value(
    compose_env_file: Path | None,
    name: str,
) -> str | None:
    return os.environ.get(name) or _read_env_value(compose_env_file, name)


def _discover_live_accounts(
    compose_files: Sequence[Path],
    compose_env_file: Path | None,
) -> tuple[tuple[str, str, str], ...]:
    accounts: list[tuple[str, str, str]] = []
    for account_label in _discover_live_account_labels(compose_files):
        suffix = _live_account_env_suffix(account_label)
        default_session = (
            "live-primary-v1"
            if account_label == "primary"
            else f"live-{account_label}-v1"
        )
        default_lease_owner = (
            "live-worker"
            if account_label == "primary"
            else f"live-worker-{account_label}"
        )
        session_id = (
            _configured_env_value(
                compose_env_file,
                f"CML_LIVE_SESSION_ID{suffix}",
            )
            or default_session
        )
        lease_owner = (
            _configured_env_value(
                compose_env_file,
                f"CML_LIVE_LEASE_OWNER{suffix}",
            )
            or default_lease_owner
        )
        accounts.append((account_label, session_id, lease_owner))
    return tuple(accounts)


def _monitor_services_for_accounts(
    live_accounts: Sequence[tuple[str, str, str]],
) -> tuple[str, ...]:
    services = ["postgres", "market-data"]
    for account_label, _run_id, _lease_owner in live_accounts:
        suffix = "" if account_label == "primary" else f"-{account_label}"
        services.extend(
            (
                f"execution-account-live{suffix}",
                f"live-strategy{suffix}",
            )
        )
    return tuple(dict.fromkeys(services))


def build_config(args: argparse.Namespace) -> MonitorConfig:
    compose_env_file = _env_path("CML_COMPOSE_ENV_FILE", None)
    live_run_id = (
        args.live_run_id
        or os.environ.get("CML_LIVE_SESSION_ID")
        or _read_env_value(compose_env_file, "CML_LIVE_SESSION_ID")
        or "live-primary-v1"
    )
    live_account_label = (
        getattr(args, "live_account_label", None)
        or os.environ.get("CML_LIVE_ACCOUNT_LABEL")
        or _read_env_value(compose_env_file, "CML_LIVE_ACCOUNT_LABEL")
        or "primary"
    )
    live_lease_owner = (
        getattr(args, "live_lease_owner", None)
        or os.environ.get("CML_LIVE_LEASE_OWNER")
        or _read_env_value(compose_env_file, "CML_LIVE_LEASE_OWNER")
        or "live-worker"
    )
    compose_file_values = tuple(
        item.strip()
        for item in str(args.compose_file).split(",")
        if item.strip()
    )
    compose_files = tuple(Path(item) for item in compose_file_values)
    profile_values = tuple(
        item.strip()
        for item in os.environ.get("CML_COMPOSE_PROFILES", "").split(",")
        if item.strip()
    )
    configured_live_accounts = os.environ.get("CML_MONITOR_LIVE_ACCOUNTS")
    live_accounts = (
        _parse_live_accounts(configured_live_accounts)
        if configured_live_accounts is not None
        else _discover_live_accounts(compose_files, compose_env_file)
    )
    configured_services = getattr(args, "services", None)
    if configured_services is None:
        configured_services = os.environ.get("CML_MONITOR_SERVICES")
    if configured_services:
        services = tuple(
            item.strip() for item in configured_services.split(",") if item.strip()
        )
    else:
        services = _monitor_services_for_accounts(live_accounts)
    return MonitorConfig(
        project_directory=Path(args.project_directory),
        compose_file=(
            compose_files[0]
            if compose_files
            else Path(args.compose_file)
        ),
        compose_files=compose_files,
        compose_profiles=profile_values,
        compose_env_file=compose_env_file,
        services=services or _DEFAULT_SERVICES,
        live_accounts=live_accounts,
        live_run_id=live_run_id,
        live_account_label=live_account_label,
        live_lease_owner=live_lease_owner,
        interval_seconds=args.interval_seconds,
        log_window_seconds=args.log_window_seconds,
        telemetry_stale_after_seconds=args.telemetry_stale_after_seconds,
        rss_warning_fraction=args.rss_warning_fraction,
        rss_critical_fraction=args.rss_critical_fraction,
        rss_growth_bytes=args.rss_growth_bytes,
        rss_growth_window_seconds=args.rss_growth_window_seconds,
        alert_cooldown_seconds=args.alert_cooldown_seconds,
        command_timeout_seconds=args.command_timeout_seconds,
        auto_restart_stale_live_services=_parse_env_bool(
            os.environ.get("CML_AUTO_RESTART_STALE_LIVE_SERVICES"),
            default=True,
        ),
        live_restart_cooldown_seconds=float(
            os.environ.get(
                "CML_LIVE_RESTART_COOLDOWN_SECONDS",
                _DEFAULT_LIVE_RESTART_COOLDOWN_SECONDS,
            )
        ),
        live_restart_max_attempts=int(
            os.environ.get(
                "CML_LIVE_RESTART_MAX_ATTEMPTS",
                _DEFAULT_LIVE_RESTART_MAX_ATTEMPTS,
            )
        ),
        state_path=Path(args.state_path),
        webhook_url=os.environ.get("CML_ALERT_WEBHOOK_URL") or None,
        serverchan_sendkey=(
            os.environ.get("SERVERCHAN_SENDKEY")
            or os.environ.get("CML_SERVERCHAN_SENDKEY")
            or None
        ),
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
        default=None,
    )
    parser.add_argument(
        "--live-run-id",
        default=None,
    )
    parser.add_argument(
        "--live-account-label",
        default=None,
    )
    parser.add_argument(
        "--live-lease-owner",
        default=None,
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
