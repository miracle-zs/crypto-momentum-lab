from datetime import UTC, datetime

from deploy.ops.cml_ops_monitor import (
    Alert,
    ContainerSnapshot,
    LogSignals,
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
