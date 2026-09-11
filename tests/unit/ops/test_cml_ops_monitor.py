import argparse
import json
import urllib.parse
import urllib.request
from datetime import UTC, datetime

import pytest

from deploy.ops.cml_ops_monitor import (
    Alert,
    ContainerSnapshot,
    LogSignals,
    MonitorConfig,
    OpsMonitor,
    _deliver_external_heartbeat,
    _serverchan_endpoint,
    _serverchan_form,
    build_config,
    build_deadman_heartbeat_payload,
    evaluate_container,
    evaluate_database_state,
    evaluate_log_signals,
)


def test_deadman_heartbeat_payload_is_low_sensitivity() -> None:
    payload = build_deadman_heartbeat_payload(
        now=datetime(2026, 9, 11, 1, 2, tzinfo=UTC),
        alerts=(
            Alert("container_unhealthy", "critical", "unhealthy"),
            Alert("database_io_timing_disabled", "warning", "warning"),
        ),
    )

    assert payload["event"] == "ops_heartbeat"
    assert payload["status"] == "critical"
    assert payload["critical_alerts"] == ("container_unhealthy",)
    assert payload["warning_alerts"] == ("database_io_timing_disabled",)
    assert "secret-token" not in json.dumps(payload)


def test_external_heartbeat_uses_bearer_header_not_json(monkeypatch) -> None:
    captured = {}

    class Response:
        status = 204

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

    def fake_urlopen(request, *, timeout):
        captured["request"] = request
        captured["timeout"] = timeout
        return Response()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    _deliver_external_heartbeat(
        "https://monitor.example.net/cml/heartbeat",
        "secret-token",
        {"event": "ops_heartbeat", "status": "healthy"},
        timeout_seconds=3,
    )

    request = captured["request"]
    assert request.get_header("Authorization") == "Bearer secret-token"
    assert b"secret-token" not in request.data
    assert captured["timeout"] == 3


def test_ops_monitor_requires_authenticated_external_heartbeat(tmp_path) -> None:
    with pytest.raises(ValueError, match="configured together"):
        OpsMonitor(
            MonitorConfig(
                state_path=tmp_path / "state.json",
                external_heartbeat_url="https://monitor.example.net/cml",
            )
        )


def test_database_state_alerts_when_live_checkpoint_is_stale() -> None:
    alerts = evaluate_database_state(
        now=datetime(2026, 8, 29, 1, 0, tzinfo=UTC),
        latest_checkpoint_age_seconds=901,
        live_session_ready=True,
        pg_stat_statements_ready=True,
        track_io_timing=True,
        track_wal_io_timing=True,
        max_parallel_maintenance_workers=0,
        stale_after_seconds=900,
    )

    assert [alert.name for alert in alerts] == ["live_checkpoint_stale"]
    assert alerts[0].severity == "critical"


def test_database_state_does_not_alert_when_only_order_telemetry_is_quiet() -> None:
    alerts = evaluate_database_state(
        now=datetime(2026, 8, 29, 1, 0, tzinfo=UTC),
        latest_checkpoint_age_seconds=12,
        live_session_ready=True,
        pg_stat_statements_ready=True,
        track_io_timing=True,
        track_wal_io_timing=True,
        max_parallel_maintenance_workers=0,
        stale_after_seconds=900,
    )

    assert alerts == ()


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
                "checkpoint_age\t12\n"
                "live_ready\ttrue\n"
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

    assert state.latest_checkpoint_age_seconds == 12
    assert state.live_session_ready is True
    assert state.pg_stat_statements_ready is True
    assert state.track_io_timing is True
    assert state.track_wal_io_timing is True
    assert state.max_parallel_maintenance_workers == 0


def test_database_state_uses_live_checkpoint_and_lease_not_order_events(
    tmp_path,
) -> None:
    class Runner:
        last_args = None

        def run(self, args, *, timeout_seconds):
            del timeout_seconds
            self.last_args = args
            return (
                "checkpoint_age\t12\n"
                "live_ready\ttrue\n"
                "pg_stat_statements\ttrue\n"
                "track_io_timing\ton\n"
                "track_wal_io_timing\ton\n"
                "parallel_maintenance\t0\n"
            )

    runner = Runner()
    monitor = OpsMonitor(
        MonitorConfig(
            state_path=tmp_path / "state.json",
            live_run_id="live-session",
            live_account_label="primary",
            live_lease_owner="live-worker",
        ),
        runner=runner,
    )

    monitor._database_state("postgres")
    sql = str(runner.last_args[-1])

    assert "strategy_runtime_checkpoints" in sql
    assert "trading_leases" in sql
    assert "live_session_transitions" in sql
    assert "strategy_runtime_events" not in sql


def test_build_config_reads_live_session_from_compose_env(
    tmp_path, monkeypatch
) -> None:
    env_file = tmp_path / "compose.env"
    env_file.write_text(
        "CML_LIVE_SESSION_ID='live-b1-long-100u-5x-v1'\n"
        "CML_LIVE_ACCOUNT_LABEL='primary-2'\n"
        "CML_LIVE_LEASE_OWNER='worker-2'\n",
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
    assert config.live_account_label == "primary-2"
    assert config.live_lease_owner == "worker-2"


def test_build_config_supports_multiple_compose_files_and_live_accounts(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("CML_COMPOSE_PROFILES", "live")
    monkeypatch.setenv(
        "CML_MONITOR_LIVE_ACCOUNTS",
        "primary|live-primary-v1|live-worker,"
        "account-2|live-account-2-v1|live-worker-account-2",
    )
    args = argparse.Namespace(
        project_directory=str(tmp_path),
        compose_file=(
            f"{tmp_path / 'compose.yaml'},{tmp_path / 'compose.live.yaml'}"
        ),
        services="postgres,live-strategy,live-strategy-account-2",
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
    monitor = OpsMonitor(config, runner=None)

    assert config.compose_files == (
        tmp_path / "compose.yaml",
        tmp_path / "compose.live.yaml",
    )
    assert config.compose_profiles == ("live",)
    assert config.live_accounts == (
        ("primary", "live-primary-v1", "live-worker"),
        ("account-2", "live-account-2-v1", "live-worker-account-2"),
    )
    assert monitor._compose_prefix()[-4:] == [
        "-f",
        str(tmp_path / "compose.live.yaml"),
        "--profile",
        "live",
    ]


def test_build_config_discovers_live_accounts_from_compose_files(
    tmp_path, monkeypatch
) -> None:
    base_file = tmp_path / "compose.yaml"
    overlay_file = tmp_path / "compose.live.yaml"
    env_file = tmp_path / ".env.server"
    base_file.write_text(
        "services:\n"
        "  postgres:\n"
        "  market-data:\n"
        "  execution-account-live:\n"
        "  live-strategy:\n"
        "volumes:\n"
        "  postgres-data:\n",
        encoding="utf-8",
    )
    overlay_file.write_text(
        "services:\n"
        "  execution-account-live-account-2:\n"
        "  live-strategy-account-2:\n"
        "  execution-account-live-account-4:\n"
        "  live-strategy-account-4:\n",
        encoding="utf-8",
    )
    env_file.write_text(
        "CML_LIVE_SESSION_ID_ACCOUNT_2=custom-account-2\n"
        "CML_LIVE_LEASE_OWNER_ACCOUNT_4=custom-worker-4\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("CML_MONITOR_LIVE_ACCOUNTS", raising=False)
    monkeypatch.delenv("CML_MONITOR_SERVICES", raising=False)
    monkeypatch.setenv("CML_COMPOSE_ENV_FILE", str(env_file))
    args = argparse.Namespace(
        project_directory=str(tmp_path),
        compose_file=f"{base_file},{overlay_file}",
        services=None,
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

    assert config.live_accounts == (
        ("primary", "live-primary-v1", "live-worker"),
        ("account-2", "custom-account-2", "live-worker-account-2"),
        ("account-4", "live-account-4-v1", "custom-worker-4"),
    )
    assert config.services == (
        "postgres",
        "market-data",
        "execution-account-live",
        "live-strategy",
        "execution-account-live-account-2",
        "live-strategy-account-2",
        "execution-account-live-account-4",
        "live-strategy-account-4",
    )


def test_container_id_uses_docker_service_labels(tmp_path) -> None:
    class Runner:
        last_args = None

        def run(self, args, *, timeout_seconds):
            del timeout_seconds
            self.last_args = args
            return "container-id\n"

    runner = Runner()
    monitor = OpsMonitor(
        MonitorConfig(state_path=tmp_path / "state.json"),
        runner=runner,
    )

    assert monitor._container_id("live-strategy-account-2") == "container-id"
    assert runner.last_args == [
        "docker",
        "ps",
        "--filter",
        "label=com.docker.compose.service=live-strategy-account-2",
        "--format",
        "{{.ID}}",
    ]


def test_serverchan_config_and_payload(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("SERVERCHAN_SENDKEY", "SCT-test-key")
    args = argparse.Namespace(
        project_directory=str(tmp_path),
        compose_file=str(tmp_path / "compose.yaml"),
        services="postgres",
        live_run_id="live-session",
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
    form = _serverchan_form(
        {
            "event": "ops_alert",
            "alert_name": "container_unhealthy",
            "severity": "critical",
            "summary": "Live strategy is unhealthy",
            "observed_at": "2026-09-01T12:00:00+00:00",
            "details": {"service": "live-strategy"},
        }
    )

    assert config.serverchan_sendkey == "SCT-test-key"
    assert _serverchan_endpoint(config.serverchan_sendkey).endswith(
        "/SCT-test-key.send"
    )
    assert form["title"] == "CML告警: container_unhealthy"
    assert "Live strategy is unhealthy" in form["desp"]
    assert "live-strategy" in form["desp"]


def test_serverchan_form_is_url_encoded_for_post() -> None:
    form = _serverchan_form(
        {
            "event": "ops_alert",
            "alert_name": "live_checkpoint_stale",
            "severity": "critical",
            "summary": "检查失败：checkpoint stale",
            "observed_at": "2026-09-01T12:00:00+00:00",
            "details": {},
        }
    )

    decoded = urllib.parse.parse_qs(urllib.parse.urlencode(form))

    assert decoded["title"] == [form["title"]]
    assert decoded["desp"] == [form["desp"]]
