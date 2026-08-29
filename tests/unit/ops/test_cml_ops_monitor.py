import argparse
from datetime import UTC, datetime

from deploy.ops.cml_ops_monitor import (
    Alert,
    ContainerSnapshot,
    LogSignals,
    MonitorConfig,
    OpsMonitor,
    build_config,
    evaluate_container,
    evaluate_database_state,
    evaluate_log_signals,
)


def test_database_state_alerts_when_telemetry_is_stale() -> None:
    alerts = evaluate_database_state(
        now=datetime(2026, 8, 29, 1, 0, tzinfo=UTC),
        latest_event_age_seconds=901,
        pg_stat_statements_ready=True,
        track_io_timing=True,
        track_wal_io_timing=True,
        max_parallel_maintenance_workers=0,
        stale_after_seconds=900,
    )

    assert [alert.name for alert in alerts] == ["telemetry_timestamp_stale"]
    assert alerts[0].severity == "critical"


def test_log_signals_alert_on_persist_failure_and_dead_task() -> None:
    alerts = evaluate_log_signals(
        LogSignals(
            telemetry_persist_failures=4,
            dead_connection_tasks=("market:aggTrade:0001",),
            latest_rss_bytes=None,
            rss_observed_at=None,
        )
    )

    assert {alert.name for alert in alerts} == {
        "telemetry_persist_failure",
        "market_task_not_alive",
    }
    assert all(isinstance(alert, Alert) for alert in alerts)


def test_container_alerts_on_oom_and_rss_limit() -> None:
    alerts = evaluate_container(
        ContainerSnapshot(
            service="postgres",
            container_id="abc",
            health="healthy",
            oom_killed=True,
            restart_count=1,
            memory_bytes=950 * 1024 * 1024,
            memory_limit_bytes=1_000 * 1024 * 1024,
        ),
        rss_warning_fraction=0.75,
        rss_critical_fraction=0.90,
    )

    assert {alert.name for alert in alerts} == {
        "container_oom_killed",
        "container_memory_high",
    }
    assert all(alert.severity == "critical" for alert in alerts)


def test_database_state_parses_postgres_boolean_text(tmp_path) -> None:
    class Runner:
        def run(self, args, *, timeout_seconds):
            del args, timeout_seconds
            return (
                "event_age\t12\n"
                "pg_stat_statements\ttrue\n"
                "track_io_timing\ton\n"
                "track_wal_io_timing\tt\n"
                "parallel_maintenance\t0\n"
            )

    monitor = OpsMonitor(
        MonitorConfig(state_path=tmp_path / "state.json"),
        runner=Runner(),
    )

    state = monitor._database_state("postgres")

    assert state.latest_event_age_seconds == 12
    assert state.pg_stat_statements_ready is True
    assert state.track_io_timing is True
    assert state.track_wal_io_timing is True
    assert state.max_parallel_maintenance_workers == 0


def test_build_config_reads_live_session_from_compose_env(
    tmp_path, monkeypatch
) -> None:
    env_file = tmp_path / "compose.env"
    env_file.write_text(
        "CML_LIVE_SESSION_ID='live-b1-long-100u-5x-v1'\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("CML_COMPOSE_ENV_FILE", str(env_file))
    args = argparse.Namespace(
        project_directory=str(tmp_path),
        compose_file=str(tmp_path / "compose.yaml"),
        services="postgres",
        live_run_id=None,
        interval_seconds=60.0,
        log_window_seconds=120.0,
        telemetry_stale_after_seconds=900.0,
        rss_warning_fraction=0.75,
        rss_critical_fraction=0.90,
        rss_growth_bytes=64 * 1024 * 1024,
        rss_growth_window_seconds=1_800.0,
        alert_cooldown_seconds=900.0,
        command_timeout_seconds=15.0,
        state_path=str(tmp_path / "state.json"),
    )

    config = build_config(args)

    assert config.live_run_id == "live-b1-long-100u-5x-v1"
