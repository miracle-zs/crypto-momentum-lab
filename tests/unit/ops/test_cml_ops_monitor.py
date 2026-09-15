import argparse
import dataclasses
import json
import urllib.parse
import urllib.request
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from deploy.ops.cml_ops_monitor import (
    Alert,
    ContainerSnapshot,
    LogSignals,
    MonitorConfig,
    OpsMonitor,
    OrderIntentObservation,
    PositionObservation,
    SignalObservation,
    _alert_action,
    _alert_scope,
    _deliver_external_heartbeat,
    _human_seconds,
    _is_within_start_grace,
    _merge_log_signals,
    _parse_log_record,
    _parse_started_at,
    _percent,
    _serverchan_endpoint,
    _serverchan_form,
    _serverchan_title,
    build_config,
    build_deadman_heartbeat_payload,
    evaluate_container,
    evaluate_container_memory_growth,
    evaluate_database_state,
    evaluate_log_signals,
    evaluate_position_divergence,
    evaluate_position_intent_divergence,
    evaluate_signal_divergence,
    rss_warning_fraction_for,
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
        account_process_state="ready_readonly",
        account_process_age_seconds=12,
        latest_reconciliation_status="ready",
        latest_reconciliation_age_seconds=12,
        latest_market_progress_age_seconds=12,
        latest_market_delay_ms=100,
    )

    assert [alert.name for alert in alerts] == ["live_checkpoint_stale"]
    assert alerts[0].severity == "critical"




def test_database_state_reports_which_live_ready_arm_failed() -> None:
    """A not-ready live session names the failing arm, not three tables to check."""

    alerts = evaluate_database_state(
        now=datetime(2026, 8, 29, 1, 0, tzinfo=UTC),
        latest_checkpoint_age_seconds=12,
        live_session_ready=False,
        live_session_state_ready=True,
        live_lease_active=False,
        live_checkpoint_present=True,
        pg_stat_statements_ready=True,
        track_io_timing=True,
        track_wal_io_timing=True,
        max_parallel_maintenance_workers=0,
        stale_after_seconds=900,
        account_process_state="ready_readonly",
        account_process_age_seconds=12,
        latest_reconciliation_status="ready",
        latest_reconciliation_age_seconds=12,
        latest_market_progress_age_seconds=12,
        latest_market_delay_ms=100,
    )

    assert [alert.name for alert in alerts] == ["live_session_not_ready"]
    assert alerts[0].details["session_state_ready"] is True
    assert alerts[0].details["lease_active"] is False
    assert alerts[0].details["checkpoint_present"] is True
    assert alerts[0].details["checkpoint_age_seconds"] == 12
    assert alerts[0].details["stale_after_seconds"] == 900


def test_database_state_treats_absent_live_ready_arms_as_ready() -> None:
    """Older SQL output omits the three arms; absence must not read as not-ready."""

    alerts = evaluate_database_state(
        now=datetime(2026, 8, 29, 1, 0, tzinfo=UTC),
        latest_checkpoint_age_seconds=12,
        live_session_ready=True,
        pg_stat_statements_ready=True,
        track_io_timing=True,
        track_wal_io_timing=True,
        max_parallel_maintenance_workers=0,
        stale_after_seconds=900,
        account_process_state="ready_readonly",
        account_process_age_seconds=12,
        latest_reconciliation_status="ready",
        latest_reconciliation_age_seconds=12,
        latest_market_progress_age_seconds=12,
        latest_market_delay_ms=100,
    )

    assert [alert.name for alert in alerts] == []


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
        account_process_state="ready_readonly",
        account_process_age_seconds=12,
        latest_reconciliation_status="ready",
        latest_reconciliation_age_seconds=12,
        latest_market_progress_age_seconds=12,
        latest_market_delay_ms=100,
    )

    assert alerts == ()


def test_merge_log_signals_keeps_every_field() -> None:
    """Every LogSignals field must survive the per-account merge.

    The merge lists fields by hand; a field omitted there is dropped for every
    account but the last, which is how a stuck exit stayed invisible.
    """

    left = LogSignals(
        telemetry_persist_failures=1,
        legacy_order_identity_conflicts=2,
        exit_processing_degraded_symbols=("龙虾USDT",),
        dead_connection_tasks=("grp-a",),
        latest_rss_bytes=100,
        rss_observed_at=datetime(2026, 9, 15, tzinfo=UTC),
    )
    right = LogSignals(
        telemetry_persist_failures=3,
        legacy_order_identity_conflicts=4,
        exit_processing_degraded_symbols=("BTWUSDT",),
        dead_connection_tasks=("grp-b",),
    )

    merged = _merge_log_signals(left, right)

    assert merged.telemetry_persist_failures == 4
    assert merged.legacy_order_identity_conflicts == 6
    assert merged.exit_processing_degraded_symbols == ("龙虾USDT", "BTWUSDT")
    assert merged.dead_connection_tasks == ("grp-a", "grp-b")
    assert merged.latest_rss_bytes == 100
    assert merged.rss_observed_at is not None

    # Any field added to LogSignals must be merged above; keep this list honest.
    assert {f.name for f in dataclasses.fields(LogSignals)} == {
        "telemetry_persist_failures",
        "legacy_order_identity_conflicts",
        "exit_processing_degraded_symbols",
        "dead_connection_tasks",
        "latest_rss_bytes",
        "rss_observed_at",
    }


def test_console_log_record_parses_like_json() -> None:
    """The containers emit structlog's console renderer, not JSON."""

    record = _parse_log_record(
        "2026-09-15 12:20:54 [warning  ] live_grace_timeout_processing_degraded "
        "error_type=ValueError reason=order_identity_conflict "
        "retry_delay_seconds=60.0 symbol=龙虾USDT"
    )

    assert record["event"] == "live_grace_timeout_processing_degraded"
    assert record["level"] == "warning"
    assert record["symbol"] == "龙虾USDT"
    assert record["reason"] == "order_identity_conflict"
    assert record["retry_delay_seconds"] == 60.0


def test_console_log_record_parses_docker_timestamp_prefix() -> None:
    """_log_signals reads `docker logs --timestamps`, which prepends its own stamp.

    A real line therefore carries two timestamps before the level marker.
    """

    record = _parse_log_record(
        "2026-09-15T14:41:35.736643867Z 2026-09-15 14:41:35 [warning  ] "
        "live_grace_timeout_processing_degraded error_type=ValueError "
        "reason=order_identity_conflict retry_delay_seconds=60.0 symbol=龙虾USDT"
    )

    assert record["event"] == "live_grace_timeout_processing_degraded"
    assert record["symbol"] == "龙虾USDT"
    assert record["retry_delay_seconds"] == 60.0

    lane = _parse_log_record(
        "2026-09-15T14:41:35.736643867Z 2026-09-15 14:41:35 [warning  ] "
        "live_entry_lane_state_changed enabled=False "
        "run_id=live-b1-long-100u-5x-v1 state_changed=True"
    )

    assert lane["event"] == "live_entry_lane_state_changed"
    assert lane["enabled"] is False
    assert lane["run_id"] == "live-b1-long-100u-5x-v1"


def test_console_log_record_coerces_booleans_and_keeps_json_working() -> None:
    record = _parse_log_record(
        "2026-09-15 12:20:54 [warning  ] live_entry_lane_state_changed "
        "enabled=False state_changed=True"
    )
    assert record["enabled"] is False
    assert record["state_changed"] is True

    as_json = _parse_log_record('{"event": "x", "value": 3}')
    assert as_json["event"] == "x"
    assert as_json["value"] == 3


def test_log_signals_reads_console_output_and_ignores_re_enable(tmp_path) -> None:
    """A stuck exit must alert; a lane coming back up must not."""

    class Runner:
        def run(self, args, *, timeout_seconds):
            del timeout_seconds
            if args[:2] == ["docker", "logs"]:
                return (
                    "2026-09-15 12:20:54 [warning  ] "
                    "live_grace_timeout_processing_degraded "
                    "error_type=ValueError reason=order_identity_conflict "
                    "retry_delay_seconds=60.0 symbol=龙虾USDT\n"
                    "2026-09-15 12:20:54 [warning  ] live_entry_lane_state_changed "
                    "enabled=False reason=exit_failure:龙虾USDT:"
                    "order_identity_conflict "
                    "run_id=live-b1-long-100u-5x-v1\n"
                    "2026-09-15 12:21:54 [warning  ] live_entry_lane_state_changed "
                    "enabled=True run_id=live-b1-long-100u-5x-v1\n"
                )
            return ""

    monitor = OpsMonitor(
        MonitorConfig(state_path=tmp_path / "state.json"),
        runner=Runner(),
    )

    signals = monitor._log_signals(None, "live-1", since_seconds=60)

    assert signals.exit_processing_degraded_symbols == ("龙虾USDT",)

    names = [alert.name for alert in evaluate_log_signals(signals)]
    assert names == ["live_exit_processing_degraded"]


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


def test_log_signals_alert_on_legacy_order_identity_conflict() -> None:
    alerts = evaluate_log_signals(
        LogSignals(legacy_order_identity_conflicts=1)
    )

    assert [alert.name for alert in alerts] == [
        "live_legacy_order_identity_conflict"
    ]
    assert alerts[0].severity == "critical"


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


def test_postgres_gets_a_higher_memory_warning_threshold() -> None:
    """postgres is judged on a working set that includes its page cache."""

    assert rss_warning_fraction_for("postgres", 0.75) == 0.85
    # Services without an override keep the configured default.
    assert rss_warning_fraction_for("live-strategy", 0.75) == 0.75
    assert rss_warning_fraction_for("market-data", 0.75) == 0.75
    # ...including an explicitly configured one.
    assert rss_warning_fraction_for("dashboard", 0.60) == 0.60


def test_start_grace_suppresses_alerts_while_a_container_boots() -> None:
    """A freshly recreated container is booting, not frozen."""

    started = datetime(2026, 9, 14, 2, 45, tzinfo=UTC)

    assert _is_within_start_grace(
        started,
        now=started + timedelta(seconds=30),
        grace_seconds=180.0,
    )
    # Past the window -- normal rules apply.
    assert not _is_within_start_grace(
        started,
        now=started + timedelta(seconds=600),
        grace_seconds=180.0,
    )
    # Unknown start time fails open to alerting.
    assert not _is_within_start_grace(
        None,
        now=started,
        grace_seconds=180.0,
    )
    # Disabled grace.
    assert not _is_within_start_grace(started, now=started, grace_seconds=0)


def test_started_at_parses_docker_nanosecond_timestamps() -> None:
    """Docker reports nanoseconds; the parser must not choke on them."""

    assert _parse_started_at("2026-09-14T02:49:21.322155481Z") == datetime(
        2026, 9, 14, 2, 49, 21, 322155, tzinfo=UTC
    )
    assert _parse_started_at("2026-09-14T02:49:21Z") == datetime(
        2026, 9, 14, 2, 49, 21, tzinfo=UTC
    )
    assert _parse_started_at("") is None
    assert _parse_started_at("not-a-timestamp") is None
    assert _parse_started_at(None) is None


def test_postgres_warning_override_suppresses_page_cache_noise() -> None:
    """77% of the limit warns for most services but not for postgres."""

    def snapshot(service: str) -> ContainerSnapshot:
        return ContainerSnapshot(
            service=service,
            container_id="abc",
            health="healthy",
            oom_killed=False,
            restart_count=0,
            memory_bytes=790 * 1024 * 1024,
            memory_limit_bytes=1_000 * 1024 * 1024,
        )

    # The same fraction crosses the generic threshold...
    assert [
        alert.name
        for alert in evaluate_container(
            snapshot("live-strategy"),
            rss_warning_fraction=rss_warning_fraction_for("live-strategy", 0.75),
            rss_critical_fraction=0.90,
        )
    ] == ["container_memory_high"]

    # ...but not postgres's, whose page cache explains it.
    assert (
        evaluate_container(
            snapshot("postgres"),
            rss_warning_fraction=rss_warning_fraction_for("postgres", 0.75),
            rss_critical_fraction=0.90,
        )
        == ()
    )


def test_postgres_memory_high_reads_anon_not_working_set() -> None:
    """The working set counts reclaimable page cache; anon does not.

    Postgres reported 90-96% of its cgroup limit while its anonymous memory sat
    near 15%, so every deploy -- which refills that cache -- raised a memory
    alert that resolved on its own minutes later.
    """

    def snapshot(service: str, anon: int | None) -> ContainerSnapshot:
        return ContainerSnapshot(
            service=service,
            container_id="abc",
            health="healthy",
            oom_killed=False,
            restart_count=0,
            memory_bytes=950 * 1024 * 1024,
            memory_limit_bytes=1_000 * 1024 * 1024,
            memory_anon_bytes=anon,
        )

    def names(service: str, anon: int | None) -> list[str]:
        return [
            alert.name
            for alert in evaluate_container(
                snapshot(service, anon),
                rss_warning_fraction=rss_warning_fraction_for(service, 0.75),
                rss_critical_fraction=0.90,
            )
        ]

    # 95% by working set, 15% by anon: that is a cache, not memory pressure.
    assert names("postgres", 150 * 1024 * 1024) == []
    # The identical numbers still alert for a service judged on its working set.
    assert names("market-data", 150 * 1024 * 1024) == ["container_memory_high"]
    # A host where the cgroup counter is unreadable must keep alerting.
    assert names("postgres", None) == ["container_memory_high"]
    # Anonymous memory that really is near the limit still alerts.
    assert names("postgres", 950 * 1024 * 1024) == ["container_memory_high"]


def test_memory_growth_ignores_a_restart_warmup() -> None:
    """Growing from an empty container to steady state is not a leak.

    This is the real alert: live-strategy-account-3 restarted, the trend
    baseline was reset to its near-empty startup footprint, and the climb to
    ~148 MiB looked like 130 MiB of growth -- at 19% of its limit.
    """

    warmup = {
        "service": "live-strategy-account-3",
        "baseline_bytes": 18 * 1024 * 1024,
        "baseline_age_seconds": 1080.0,
        "consecutive_samples": 18,
        "required_samples": 3,
        "growth_bytes": 64 * 1024 * 1024,
        "growth_window_seconds": 1800.0,
        "metric_source": "docker_stats_working_set",
    }

    # Without a limit to compare against, the old behaviour is unchanged.
    assert evaluate_container_memory_growth(
        **warmup,
        current_bytes=148 * 1024 * 1024,
    ) != ()

    # With the real limit, 148 MiB of 768 MiB is far from trouble.
    assert (
        evaluate_container_memory_growth(
            **warmup,
            current_bytes=148 * 1024 * 1024,
            memory_limit_bytes=768 * 1024 * 1024,
            warning_fraction=0.75,
        )
        == ()
    )

    # A container genuinely approaching its limit still alerts.
    alert = evaluate_container_memory_growth(
        **{
            **warmup,
            "baseline_bytes": 600 * 1024 * 1024,
            "current_bytes": 740 * 1024 * 1024,
        },
        memory_limit_bytes=768 * 1024 * 1024,
        warning_fraction=0.75,
    )
    assert [item.name for item in alert] == ["container_memory_growth"]


def test_memory_growth_details_include_human_readable_sizes() -> None:
    """These are triaged on a phone; bytes alone are unreadable."""

    alert = evaluate_container_memory_growth(
        service="live-strategy",
        current_bytes=740 * 1024 * 1024,
        baseline_bytes=600 * 1024 * 1024,
        baseline_age_seconds=900.0,
        consecutive_samples=5,
        required_samples=3,
        growth_bytes=64 * 1024 * 1024,
        growth_window_seconds=1800.0,
        metric_source="docker_stats_working_set",
    )[0]

    assert alert.details["current_mb"] == 740.0
    assert alert.details["baseline_mb"] == 600.0
    assert alert.details["growth_mb"] == 140.0
    assert alert.details["threshold_mb"] == 64.0


def test_container_memory_growth_requires_consecutive_samples() -> None:
    common = {
        "service": "postgres",
        "current_bytes": 180,
        "baseline_bytes": 100,
        "baseline_age_seconds": 120.0,
        "required_samples": 3,
        "growth_bytes": 64,
        "growth_window_seconds": 1_800.0,
        "metric_source": "cgroup_memory_current",
    }

    assert evaluate_container_memory_growth(
        **common,
        consecutive_samples=2,
    ) == ()

    alerts = evaluate_container_memory_growth(
        **common,
        consecutive_samples=3,
    )

    assert [alert.name for alert in alerts] == ["container_memory_growth"]
    assert alerts[0].details == {
        "service": "postgres",
        "baseline_bytes": 100,
        "current_bytes": 180,
        "growth_bytes": 80,
        "threshold_bytes": 64,
        "growth_window_seconds": 1_800.0,
        "growth_window_human": "30.0 分钟",
        "baseline_age_seconds": 120.0,
        "baseline_age_human": "2.0 分钟",
        "consecutive_samples": 3,
        "required_samples": 3,
        "metric_source": "cgroup_memory_current",
        "baseline_mb": 0.0,
        "current_mb": 0.0,
        "growth_mb": 0.0,
        "threshold_mb": 0.0,
    }


def test_memory_growth_uses_oldest_window_sample_and_resets_on_drop(
    tmp_path,
) -> None:
    monitor = OpsMonitor(
        MonitorConfig(
            state_path=tmp_path / "state.json",
            rss_growth_bytes=64,
        ),
    )

    assert (
        monitor._memory_growth_alerts(
            "postgres",
            100,
            0.0,
            metric_source="cgroup_memory_current",
        )
        == ()
    )
    assert (
        monitor._memory_growth_alerts(
            "postgres",
            170,
            60.0,
            metric_source="cgroup_memory_current",
        )
        == ()
    )
    assert (
        monitor._memory_growth_alerts(
            "postgres",
            171,
            120.0,
            metric_source="cgroup_memory_current",
        )
        == ()
    )

    alerts = monitor._memory_growth_alerts(
        "postgres",
        172,
        180.0,
        metric_source="cgroup_memory_current",
    )

    assert [alert.name for alert in alerts] == ["container_memory_growth"]
    assert alerts[0].details["baseline_bytes"] == 100
    assert alerts[0].details["growth_bytes"] == 72

    assert (
        monitor._memory_growth_alerts(
            "postgres",
            105,
            240.0,
            metric_source="cgroup_memory_current",
        )
        == ()
    )
    assert monitor._state["memory_growth_breaches"]["postgres"] == 0


def test_memory_growth_starts_a_new_baseline_after_container_recreation(
    tmp_path,
) -> None:
    monitor = OpsMonitor(
        MonitorConfig(
            state_path=tmp_path / "state.json",
            rss_growth_bytes=64,
        ),
    )

    for now, value in ((0.0, 100), (60.0, 170), (120.0, 171)):
        assert (
            monitor._memory_growth_alerts(
                "market-data",
                value,
                now,
                container_id="old-container",
                metric_source="cgroup_memory_current",
            )
            == ()
        )

    # A recreated container must not inherit the old container's baseline.
    assert (
        monitor._memory_growth_alerts(
            "market-data",
            1_000,
            180.0,
            container_id="new-container",
            metric_source="cgroup_memory_current",
        )
        == ()
    )

    assert monitor._state["memory_samples"]["market-data"] == [[180.0, 1_000]]
    assert monitor._state["memory_growth_breaches"]["market-data"] == 0
    assert (
        monitor._state["memory_sample_container_ids"]["market-data"]
        == "new-container"
    )


def test_memory_pressure_alerts_on_swap_growth_not_reclaim_counter(
    tmp_path,
) -> None:
    """Reclaiming page cache is housekeeping; pushing anon into swap is cost.

    The old trigger watched ``memory.events.max``, which advances whenever the
    kernel steals cache to stay under the limit -- and a database that mostly
    caches files does that constantly.  Live it read 50394 while 40 seconds of
    sampling showed zero page scans, zero steals, an unchanged counter and a
    swap level drifting down.
    """

    monitor = OpsMonitor(
        MonitorConfig(state_path=tmp_path / "state.json"),
    )
    mib = 1024 * 1024

    def snapshot(swap: int | None, events_max: int) -> ContainerSnapshot:
        return ContainerSnapshot(
            service="postgres",
            container_id="abc",
            health="healthy",
            oom_killed=False,
            restart_count=0,
            memory_bytes=800,
            memory_limit_bytes=1_000,
            memory_source="cgroup_memory_current",
            memory_current_bytes=800,
            memory_swap_current_bytes=swap,
            memory_events_max=events_max,
        )

    # The first observation only establishes the baseline.
    assert monitor._memory_pressure_alerts(snapshot(200 * mib, 50_000)) == ()

    # The reclaim counter advancing is no longer a reason to alert.
    assert monitor._memory_pressure_alerts(snapshot(200 * mib, 50_100)) == ()

    # Swap draining back lowers the baseline instead of alerting.
    assert monitor._memory_pressure_alerts(snapshot(180 * mib, 50_100)) == ()

    # Growth below the threshold stays quiet -- and does not move the baseline,
    # so slow steady growth still accumulates.
    assert monitor._memory_pressure_alerts(snapshot(190 * mib, 50_100)) == ()

    # Real growth past the threshold alerts, measured from the 180 baseline:
    # 260 - 180 = 80, not merely the 70 since the last sample.
    alerts = monitor._memory_pressure_alerts(snapshot(260 * mib, 50_100))
    assert [alert.name for alert in alerts] == ["container_memory_pressure"]
    assert alerts[0].details["memory_swap_growth_mb"] == 80.0

    # ...and the same swap is not reported a second time.
    assert monitor._memory_pressure_alerts(snapshot(260 * mib, 50_100)) == ()

    # An unreadable swap counter stays silent rather than guessing.
    assert monitor._memory_pressure_alerts(snapshot(None, 50_100)) == ()


def test_memory_stats_prefers_working_set_and_keeps_cgroup_current(
    tmp_path,
) -> None:
    class Runner:
        def run(self, args, *, timeout_seconds):
            del timeout_seconds
            if args[:2] == ["docker", "inspect"]:
                return json.dumps(
                    [
                        {
                            "HostConfig": {"Memory": 1_000},
                        }
                    ]
                )
            if args[:2] == ["docker", "stats"]:
                return "10MiB / 1GiB\n"
            if args[:2] == ["docker", "exec"]:
                return (
                    "memory.current=700\n"
                    "memory.peak=900\n"
                    "memory.max=1000\n"
                    "memory.swap.current=128\n"
                    "memory.events.max=12\n"
                    "memory.stat.anon=150\n"
                )
            raise AssertionError(f"unexpected command: {args}")

    monitor = OpsMonitor(
        MonitorConfig(state_path=tmp_path / "state.json"),
        runner=Runner(),
    )

    stats = monitor._memory_stats("postgres")

    assert stats.observed_bytes == 10 * 1_048_576
    assert stats.memory_limit_bytes == 1_000
    assert stats.source == "docker_stats_working_set"
    assert stats.working_set_bytes == 10 * 1_048_576
    assert stats.anon_bytes == 150
    assert stats.current_bytes == 700
    assert stats.peak_bytes == 900
    assert stats.swap_current_bytes == 128
    assert stats.events_max == 12


def test_market_delay_reads_only_event_backed_buckets(tmp_path) -> None:
    """Empty buckets are a dense-clock device, not a latency measurement.

    market-data materializes zero-event buckets for quiet symbols so consumers
    see a continuous 15-second clock.  Every one of them sits at a past
    bucket_end, so received_at - bucket_end measures the bucket's age.  The
    monitor saw 18 and 65 minute "delays" that way while healthy buckets sat at
    1.3 seconds.
    """

    class Runner:
        last_args = None

        def run(self, args, *, timeout_seconds):
            del timeout_seconds
            self.last_args = args
            return ""

    runner = Runner()
    monitor = OpsMonitor(
        MonitorConfig(state_path=tmp_path / "state.json"),
        runner=runner,
    )

    monitor._database_state("postgres")
    sql = str(runner.last_args[-1])

    assert "'source_event_count'" in sql
    # The filter belongs to the delay lookup and nowhere else.
    assert sql.count("source_event_count") == 1


def test_durations_and_fractions_are_rendered_for_a_reader() -> None:
    """A raw number in a push notification makes the reader do the arithmetic.

    "1111093.444" as milliseconds, or "900.0" as seconds, or "0.9088" as a
    ratio, each asks the person triaging on a phone to know a unit and a scale
    they were never told.
    """

    assert _human_seconds(1111093.444 / 1000) == "18.5 分钟"
    assert _human_seconds(906.761) == "15.1 分钟"
    assert _human_seconds(1800) == "30.0 分钟"
    assert _human_seconds(30) == "30.0 秒"
    assert _human_seconds(0.25) == "250 毫秒"
    assert _human_seconds(7200) == "2.0 小时"
    assert _human_seconds(None) is None

    assert _percent(0.9088) == 90.88
    assert _percent(0.75) == 75.0
    assert _percent(None) is None


def test_memory_details_carry_readable_companions() -> None:
    """Every byte count gets a MiB companion next to it."""

    mib = 1024 * 1024
    alerts = evaluate_container(
        ContainerSnapshot(
            service="postgres",
            container_id="abc",
            health="healthy",
            oom_killed=False,
            restart_count=0,
            memory_bytes=951 * mib,
            memory_limit_bytes=1024 * mib,
            # postgres is judged on anon, so the ratio must come from there.
            memory_anon_bytes=951 * mib,
            memory_current_bytes=1000 * mib,
            memory_peak_bytes=1024 * mib,
            memory_swap_current_bytes=200 * mib,
        ),
        rss_warning_fraction=0.85,
        rss_critical_fraction=0.90,
    )

    assert [alert.name for alert in alerts] == ["container_memory_high"]
    details = alerts[0].details
    assert details["memory_mb"] == 951.0
    assert details["memory_anon_mb"] == 951.0
    assert details["memory_limit_mb"] == 1024.0
    assert details["memory_swap_current_mb"] == 200.0
    # 951 / 1024 = 0.9287, rendered where a reader expects a percentage.
    assert details["fraction"] == 0.9287
    assert details["fraction_percent"] == 92.87


def test_push_title_drops_the_scope_before_truncating_the_label() -> None:
    """A truncated Chinese label says nothing useful.

    "实时状态 checkpoint 已过" is not a shorter version of the real label, it
    is a different, meaningless one.  The scope is repeated on the body's
    first line, so it is the safer thing to give up.
    """

    assert _serverchan_title("严重", "account-3", "行情延迟过高") == (
        "CML | 严重 | account-3 | 行情延迟过高"
    )

    # Too long for the cap: the scope goes, the label survives intact.
    title = _serverchan_title("严重", "account-4", "实时状态 checkpoint 已过期")
    assert title == "CML | 严重 | 实时状态 checkpoint 已过期"
    assert "已过期" in title

    # Same rule on the resolution side.
    assert _serverchan_title("恢复", "account-4", "实时状态 checkpoint 已过期") == (
        "CML | 恢复 | 实时状态 checkpoint 已过期"
    )

    # No scope at all still renders.
    assert _serverchan_title("恢复", "", "行情延迟过高") == "CML | 恢复 | 行情延迟过高"


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
                "market_progress_age\t15\n"
                "market_delay_ms\t250\n"
                "account_process_state\tready_readonly\n"
                "account_process_age\t5\n"
                "reconciliation_status\tready\n"
                "reconciliation_age\t6\n"
                "unknown_order_count\t0\n"
                "oldest_unknown_order_age\t-1\n"
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
    assert state.latest_market_progress_age_seconds == 15
    assert state.latest_market_delay_ms == 250
    assert state.account_process_state == "ready_readonly"
    assert state.account_process_age_seconds == 5
    assert state.latest_reconciliation_status == "ready"
    assert state.latest_reconciliation_age_seconds == 6
    assert state.unknown_order_count == 0
    assert state.oldest_unknown_order_age_seconds is None


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
    assert "strategy_runtime_events" in sql
    assert "market_state_progress" in sql
    assert "execution_account_process_states" in sql
    assert "account_reconciliation_runs" in sql


def test_database_state_alerts_on_lifecycle_market_and_unknown_order_state() -> None:
    alerts = evaluate_database_state(
        now=datetime(2026, 8, 29, 1, 0, tzinfo=UTC),
        latest_checkpoint_age_seconds=12,
        live_session_ready=True,
        pg_stat_statements_ready=True,
        track_io_timing=True,
        track_wal_io_timing=True,
        max_parallel_maintenance_workers=0,
        stale_after_seconds=900,
        account_process_state="degraded",
        account_process_age_seconds=3,
        latest_reconciliation_status="halted",
        latest_reconciliation_age_seconds=3,
        latest_market_progress_age_seconds=901,
        latest_market_delay_ms=121_000,
        unknown_order_count=1,
        oldest_unknown_order_age_seconds=42,
    )

    assert {alert.name for alert in alerts} == {
        "live_account_lifecycle_not_ready",
        "live_account_reconciliation_stale",
        "live_market_state_stale",
        "live_market_state_delay",
        "live_unknown_orders",
    }


def test_signal_divergence_compares_only_same_strategy_configuration() -> None:
    common = {
        "symbol": "BTCUSDT",
        "bucket_start": "2026-09-01T00:00:00+00:00",
        "strategy_config_hash": "same-config",
        "signal_count": 1,
        "candidate_count": 1,
    }
    assert evaluate_signal_divergence(
        (
            SignalObservation("primary", fingerprint="same", **common),
            SignalObservation("account-2", fingerprint="same", **common),
        )
    ) == ()

    alerts = evaluate_signal_divergence(
        (
            SignalObservation("primary", fingerprint="long", **common),
            SignalObservation("account-2", fingerprint="short", **common),
            SignalObservation(
                "account-3",
                fingerprint="different-config",
                strategy_config_hash="other-config",
                **{
                    key: value
                    for key, value in common.items()
                    if key != "strategy_config_hash"
                },
            ),
        )
    )

    assert [alert.name for alert in alerts] == ["live_signal_divergence"]
    assert alerts[0].details["group_count"] == 1


def test_signal_divergence_ignores_async_candidate_count_difference() -> None:
    common = {
        "symbol": "XPINUSDT",
        "bucket_start": "2026-09-13T05:39:15+00:00",
        "strategy_config_hash": "same-config",
        "signal_count": 1,
        "fingerprint": "same-signal",
    }

    assert evaluate_signal_divergence(
        (
            SignalObservation("primary", candidate_count=1, **common),
            SignalObservation("account-2", candidate_count=0, **common),
        )
    ) == ()


def test_signal_fingerprint_covers_only_account_stable_features(tmp_path) -> None:
    """The durable fingerprint must not cover per-run rolling ratios.

    Two accounts on one config recompute ``notional_5m_vs_30m`` from their own
    local market-state window, so that value differs between them by a
    rounding-level amount; fingerprinting the whole ``features`` blob would
    report a divergence for every shared bucket.
    """

    class Runner:
        last_args = None

        def run(self, args, *, timeout_seconds):
            del timeout_seconds
            self.last_args = args
            return ""

    runner = Runner()
    monitor = OpsMonitor(
        MonitorConfig(state_path=tmp_path / "state.json"),
        runner=runner,
    )

    monitor._consistency_observations("postgres-container")
    sql = str(runner.last_args[-1])

    assert "features::text" not in sql
    assert "'notional_5m_vs_30m'" not in sql
    assert "'aggressive_imbalance'" in sql
    assert "'impulse_return_pct'" in sql
    assert "reference_prices" in sql


def test_signal_fingerprint_drops_position_derived_keys_for_reduce_only(
    tmp_path,
) -> None:
    """A close signal states a size that follows the position, not the choice.

    On 2026-09-14 05:14:45 all four accounts produced the same
    reduce_only_candidate with the same side and reason, yet their fingerprints
    differed on `quantity`, `batch_id` and `desired_notional` -- three values
    that describe what each account happened to hold.
    """

    class Runner:
        last_args = None

        def run(self, args, *, timeout_seconds):
            del timeout_seconds
            self.last_args = args
            return ""

    runner = Runner()
    monitor = OpsMonitor(
        MonitorConfig(state_path=tmp_path / "state.json"),
        runner=runner,
    )

    monitor._consistency_observations("postgres-container")
    sql = str(runner.last_args[-1])

    assert "WHEN signal_kind = 'reduce_only_candidate'" in sql
    assert "'quantity'" in sql and "'batch_id'" in sql
    assert "reference_prices - 'desired_notional'" in sql


def test_fingerprint_normalises_numeric_feature_rendering(tmp_path) -> None:
    """Equal numbers written with different trailing zeros must compare equal.

    On 2026-09-15 04:04 two accounts on the same config, same symbol and same
    bucket both reported breakout_level = 0.1469 -- one rendered as
    "0.1469000" and the other as "0.146900000000000000" (Decimal(float)
    expands to the full IEEE value).  Every other feature matched digit for
    digit, yet the raw-text fingerprint differed, so the alert fired on a
    rendering difference rather than a decision difference.
    """

    class Runner:
        last_args = None

        def run(self, args, *, timeout_seconds):
            del timeout_seconds
            self.last_args = args
            return ""

    runner = Runner()
    monitor = OpsMonitor(
        MonitorConfig(state_path=tmp_path / "state.json"),
        runner=runner,
    )

    monitor._consistency_observations("postgres-container")
    sql = str(runner.last_args[-1])

    # Numeric features are cast through trim_scale, and read with ->> so the
    # JSON string's quotes do not break the cast.
    assert "trim_scale((features->>'breakout_level')::numeric)" in sql
    assert "trim_scale((features->>'impulse_return_pct')::numeric)" in sql
    # Non-numeric features keep their raw JSON value.
    assert "trim_scale((features->>'direction')" not in sql
    assert "trim_scale((features->>'impulse_start')" not in sql


def test_signal_divergence_sql_excludes_close_candidates(tmp_path) -> None:
    """A close candidate exists because the account holds the symbol.

    Whether one appears at all follows the position, and positions diverge
    between two correct accounts as soon as their fills differ -- so requiring
    close candidates to match compares execution through the signal table.
    That the account *asked the exchange* to close is covered separately by
    ``evaluate_position_intent_divergence``, which ignores the closing
    quantity.  On 2026-09-14 the same four accounts produced matching close
    candidates whose only difference was what each happened to hold; this
    filter removes the other half of that comparison.
    """

    class Runner:
        last_args = None

        def run(self, args, *, timeout_seconds):
            del timeout_seconds
            self.last_args = args
            return ""

    runner = Runner()
    monitor = OpsMonitor(
        MonitorConfig(state_path=tmp_path / "state.json"),
        runner=runner,
    )

    monitor._consistency_observations("postgres-container")
    sql = str(runner.last_args[-1])

    assert "signal_kind <> 'reduce_only_candidate'" in sql


def test_output_event_does_not_override_the_durable_signal_count(tmp_path) -> None:
    """The observed event repeats itself; only its candidate count is used.

    The same (run, bucket) writes strategy_output_observed up to thirty times
    with a signal_count that disagrees with the durable rows, so borrowing it
    made two accounts look divergent when each had exactly one signal.
    """

    class Runner:
        def run(self, args, *, timeout_seconds):
            del timeout_seconds
            if args[:2] == ["docker", "exec"]:
                return (
                    "signal\tprimary\tMTLUSDT\t2026-09-14 05:14:45+00\tcfg\t1\tfp-a\n"
                    "output\tlive-primary-v1\tMTLUSDT\t2026-09-14 05:14:45+00"
                    "\tcfg\t7\t3\n"
                )
            raise AssertionError(f"unexpected command: {args}")

    monitor = OpsMonitor(
        MonitorConfig(state_path=tmp_path / "state.json"),
        runner=Runner(),
    )

    signals, _positions, _orders = monitor._consistency_observations("postgres")

    assert len(signals) == 1
    # The durable row's count wins; the event's own count is ignored.
    assert signals[0].signal_count == 1
    # The candidate count is the one thing the event is trusted for.
    assert signals[0].candidate_count == 3


def test_order_fingerprint_omits_closing_quantity(tmp_path) -> None:
    """Closing size follows holdings; only the decision to exit is intent.

    After the 04:26 partial fill, account-4 held 198 while its peers held 262.
    Their 05:15 exit orders therefore carried different quantities and were
    reported as divergent intent -- the same fill difference, re-raised through
    the order table.
    """

    class Runner:
        last_args = None

        def run(self, args, *, timeout_seconds):
            del timeout_seconds
            self.last_args = args
            return ""

    runner = Runner()
    monitor = OpsMonitor(
        MonitorConfig(state_path=tmp_path / "state.json"),
        runner=runner,
    )

    monitor._consistency_observations("postgres-container")
    sql = str(runner.last_args[-1])

    assert "FROM exchange_orders" in sql
    # A closing order contributes side/type only -- never its quantity.
    assert "WHEN reduce_only THEN 'close'" in sql
    # An opening order still contributes the quantity and price it chose.
    assert "quantity::text || ':' || COALESCE(price::text, '')" in sql
    # Orders are read against the signals that should have produced them, so
    # an account that never sent one still appears (with order_count = 0).
    assert "LEFT JOIN (" in sql


def test_position_intent_divergence_compares_intent_not_fills() -> None:
    """Accounts must agree on what they asked for, not on what filled.

    The live failure this encodes: four accounts sent the same 262-lot limit
    order, one of them only filled 198, and the position comparison reported
    the fill difference as a critical divergence.
    """

    def intent(
        account: str,
        count: int,
        fingerprint: str | None,
    ) -> OrderIntentObservation:
        return OrderIntentObservation(
            account_label=account,
            symbol="MTLUSDT",
            order_count=count,
            strategy_config_hash="cfg",
            fingerprint=fingerprint,
        )

    # Identical orders everywhere -- only the fills differed, so no alert.
    assert evaluate_position_intent_divergence(
        (
            intent("primary", 1, "buy-limit-262"),
            intent("account-2", 1, "buy-limit-262"),
            intent("account-3", 1, "buy-limit-262"),
            intent("account-4", 1, "buy-limit-262"),
        )
    ) == ()

    # A different quantity is a different intent.
    alerts = evaluate_position_intent_divergence(
        (
            intent("account-3", 1, "buy-limit-262"),
            intent("account-4", 1, "buy-limit-198"),
        )
    )
    assert [alert.name for alert in alerts] == ["live_position_intent_divergence"]
    assert alerts[0].severity == "critical"
    assert alerts[0].details["group_count"] == 1

    # An account that never sent anything is the worst case, not a missing row.
    assert [
        alert.name
        for alert in evaluate_position_intent_divergence(
            (intent("account-3", 1, "buy-limit-262"), intent("account-4", 0, None))
        )
    ] == ["live_position_intent_divergence"]

    # Accounts on different configs are never comparable.
    assert evaluate_position_intent_divergence(
        (
            OrderIntentObservation("primary", "MTLUSDT", 1, "cfg-a", "a"),
            OrderIntentObservation("account-3", "MTLUSDT", 1, "cfg-b", "b"),
        )
    ) == ()


def test_position_spread_is_recorded_as_state_not_raised(tmp_path) -> None:
    """Holdings that differ because of a partial fill stay out of the alerts."""

    monitor = OpsMonitor(
        MonitorConfig(state_path=tmp_path / "state.json"),
        runner=None,
    )

    monitor._record_position_spread(
        (
            PositionObservation(
                "account-3", "ready", 5.0, "MTLUSDT", "LONG", Decimal("262"), "cfg"
            ),
            PositionObservation(
                "account-4", "ready", 5.0, "MTLUSDT", "LONG", Decimal("198"), "cfg"
            ),
        )
    )

    spread = monitor._state["position_spread"]
    assert [entry["symbol"] for entry in spread] == ["MTLUSDT"]
    assert spread[0]["min_quantity"] == "198"
    assert spread[0]["max_quantity"] == "262"
    assert spread[0]["accounts"] == {
        "account-3:LONG": "262",
        "account-4:LONG": "198",
    }


def test_position_divergence_ignores_stale_reconciliation() -> None:
    observations = (
        PositionObservation(
            "primary",
            "ready",
            10,
            "BTCUSDT",
            "BOTH",
            Decimal("1"),
        ),
        PositionObservation(
            "account-2",
            "ready",
            10,
            "BTCUSDT",
            "BOTH",
            Decimal("2"),
        ),
        PositionObservation(
            "account-3",
            "ready",
            999,
            "BTCUSDT",
            "BOTH",
            Decimal("99"),
        ),
    )

    alerts = evaluate_position_divergence(
        observations,
        stale_after_seconds=120,
    )

    assert [alert.name for alert in alerts] == ["live_position_divergence"]
    assert alerts[0].details["pair_count"] == 1


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
    assert form["title"] == "CML | 严重 | primary | 服务健康检查失败"
    assert "[严重] primary：服务健康检查失败" in form["desp"]
    assert "2026-09-01 20:00:00（北京时间）" in form["desp"]
    assert "诊断结论" in form["desp"]
    assert "live-strategy" in form["desp"]


def test_serverchan_recovery_form_includes_duration_and_local_time() -> None:
    form = _serverchan_form(
        {
            "event": "ops_alert_resolved",
            "alert_name": "live_heartbeat_stale:account-2",
            "observed_at": "2026-09-01T12:03:05+00:00",
            "duration_seconds": 185.0,
            "details": {"account_label": "account-2"},
        }
    )

    assert form["title"] == "CML | 恢复 | account-2 | 实时策略心跳过期"
    assert "[恢复] account-2：实时策略心跳过期" in form["desp"]
    assert "2026-09-01 20:03:05（北京时间）" in form["desp"]
    assert "持续时间**：3 分钟 5 秒" in form["desp"]
    assert "live_heartbeat_stale:account-2" in form["desp"]


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


def test_resolution_payload_preserves_alert_context_and_duration(
    monkeypatch,
    tmp_path,
) -> None:
    delivered: list[dict[str, object]] = []

    def capture(_webhook, _sendkey, payload) -> None:
        delivered.append(dict(payload))

    monkeypatch.setattr(
        "deploy.ops.cml_ops_monitor._deliver_notification",
        capture,
    )
    monitor = OpsMonitor(
        MonitorConfig(state_path=tmp_path / "state.json"),
    )

    monitor._emit(
        Alert(
            "container_unhealthy",
            "critical",
            "service health failed",
            {"service": "live-strategy-account-2"},
        ),
        now=100.0,
    )
    monitor._emit_resolutions(set(), now=160.0)

    assert delivered[1]["event"] == "ops_alert_resolved"
    assert delivered[1]["duration_seconds"] == 60.0
    assert delivered[1]["details"] == {"service": "live-strategy-account-2"}


def test_unhealthy_live_account_is_restarted_with_cooldown_and_cap(
    tmp_path,
) -> None:
    class Runner:
        def __init__(self) -> None:
            self.calls: list[list[str]] = []

        def run(self, args, *, timeout_seconds):
            del timeout_seconds
            command = list(args)
            self.calls.append(command)
            if command[:3] == ["docker", "ps", "--filter"]:
                service = command[3].rsplit("=", 1)[1]
                if service == "live-strategy-account-2":
                    return "account-2-container\n"
                if service == "postgres":
                    return "postgres-container\n"
                return ""
            if command[:2] == ["docker", "inspect"]:
                return json.dumps(
                    [
                        {
                            "State": {
                                "Health": {"Status": "unhealthy"},
                                "OOMKilled": False,
                            },
                            "RestartCount": 0,
                            "HostConfig": {"Memory": 512 * 1024 * 1024},
                        }
                    ]
                )
            if command[:2] == ["docker", "stats"]:
                return "10MiB / 512MiB\n"
            if command[:2] == ["docker", "logs"]:
                return ""
            if command[:2] == ["docker", "exec"]:
                return (
                    "checkpoint_age\t12\n"
                    "live_ready\ttrue\n"
                    "pg_stat_statements\ttrue\n"
                    "track_io_timing\ton\n"
                    "track_wal_io_timing\ton\n"
                    "parallel_maintenance\t0\n"
                )
            if command[:2] == ["docker", "compose"]:
                return "restarted\n"
            raise AssertionError(f"unexpected command: {command}")

    now = [1000.0]
    runner = Runner()
    monitor = OpsMonitor(
        MonitorConfig(
            project_directory=tmp_path,
            compose_file=tmp_path / "compose.yaml",
            compose_files=(tmp_path / "compose.yaml",),
            compose_profiles=("live",),
            compose_env_file=None,
            services=("live-strategy-account-2",),
            live_accounts=(
                ("account-2", "live-account-2-v1", "live-worker-account-2"),
            ),
            state_path=tmp_path / "state.json",
            auto_restart_stale_live_services=True,
            live_restart_cooldown_seconds=900.0,
            live_restart_max_attempts=2,
        ),
        runner=runner,
        clock=lambda: now[0],
    )

    first_alerts = monitor.run_once()
    restart_commands = [
        call
        for call in runner.calls
        if call[:2] == ["docker", "compose"]
    ]
    assert len(restart_commands) == 1
    assert restart_commands[0][-2:] == ["restart", "live-strategy-account-2"]
    assert any(
        alert.name == "live_heartbeat_stale:account-2"
        for alert in first_alerts
    )
    archives = tuple((tmp_path / "crash-logs").glob("*.log"))
    assert len(archives) == 1
    assert "container_id=account-2-container" in archives[0].read_text(
        encoding="utf-8"
    )
    assert any(
        alert.details.get("crash_log_archive") == str(archives[0])
        for alert in first_alerts
    )

    now[0] += 10
    monitor.run_once()
    assert len(
        [call for call in runner.calls if call[:2] == ["docker", "compose"]]
    ) == 1

    now[0] += 900
    monitor.run_once()
    assert len(
        [call for call in runner.calls if call[:2] == ["docker", "compose"]]
    ) == 2

    now[0] += 900
    final_alerts = monitor.run_once()
    assert len(
        [call for call in runner.calls if call[:2] == ["docker", "compose"]]
    ) == 2
    assert any(
        alert.name == "live_heartbeat_restart_suppressed:account-2"
        for alert in final_alerts
    )


def test_position_divergence_ignores_accounts_on_different_configs() -> None:
    """Accounts running different strategy configs are not comparable."""

    left = PositionObservation(
        "primary",
        "ready",
        10,
        "BTCUSDT",
        "BOTH",
        Decimal("1"),
        "config-a",
    )
    right = PositionObservation(
        "account-2",
        "ready",
        10,
        "BTCUSDT",
        "BOTH",
        Decimal("5"),
        "config-b",
    )

    # Different configs: the quantity gap is expected, so no alert.
    assert (
        evaluate_position_divergence((left, right), stale_after_seconds=60) == ()
    )

    same_config = PositionObservation(
        "account-2",
        "ready",
        10,
        "BTCUSDT",
        "BOTH",
        Decimal("5"),
        "config-a",
    )
    alerts = evaluate_position_divergence(
        (left, same_config),
        stale_after_seconds=60,
    )
    assert [alert.name for alert in alerts] == ["live_position_divergence"]
    assert alerts[0].details["differences"][0]["strategy_config_hash"] == "config-a"


def test_position_divergence_groups_unknown_configs_apart() -> None:
    """An unknown (empty) config hash must not be compared with a known one."""

    known = PositionObservation(
        "primary",
        "ready",
        10,
        "BTCUSDT",
        "BOTH",
        Decimal("1"),
        "config-a",
    )
    unknown = PositionObservation(
        "account-2",
        "ready",
        10,
        "BTCUSDT",
        "BOTH",
        Decimal("9"),
    )

    assert (
        evaluate_position_divergence((known, unknown), stale_after_seconds=60)
        == ()
    )


def test_position_intent_divergence_scope_action_and_serverchan() -> None:
    intent_a = OrderIntentObservation(
        account_label="primary",
        symbol="BTWUSDT",
        order_count=0,
        strategy_config_hash="c223e6dbad4d588e2916b47fa303762c6241bc18346765786950e51b3cd5cbdd",
        fingerprint=None,
        intent_summary=None,
    )
    intent_b = OrderIntentObservation(
        account_label="account-2",
        symbol="BTWUSDT",
        order_count=1,
        strategy_config_hash="c223e6dbad4d588e2916b47fa303762c6241bc18346765786950e51b3cd5cbdd",
        fingerprint="a7be2db0c92b62b91f58651c26b62f27",
        intent_summary="BUY LIMIT 141@0.707",
    )

    alerts = evaluate_position_intent_divergence((intent_a, intent_b))
    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.name == "live_position_intent_divergence"

    details = alert.details
    # 1. Test scope extracts symbol
    scope = _alert_scope(alert.name, details)
    assert scope == "BTWUSDT"

    # 2. Test dynamic action detects missing order (zero count)
    action = _alert_action(alert.name, details)
    assert "检测到单边未下单" in action
    assert "primary" in action

    # 3. Test serverchan form formatting
    form = _serverchan_form(
        {
            "event": "ops_alert",
            "alert_name": alert.name,
            "severity": "critical",
            "summary": alert.summary,
            "observed_at": "2026-09-15T07:57:44+00:00",
            "details": details,
        }
    )

    assert form["title"] == "CML | 严重 | BTWUSDT | 账户下单意图发生分叉"
    assert "BTWUSDT" in form["desp"]
    assert "c223e6db" in form["desp"]
    assert "account-2`（已下单 **1** 笔）：买入 141 @ 0.707（限价）" in form["desp"]
    assert "primary`：**未下单**（0 笔）" in form["desp"]
    assert "检测到单边未下单" in form["desp"]
    assert "- **影响**" not in form["desp"]


def test_position_intent_divergence_multi_order_and_zero_trimming() -> None:
    details = {
        "differences": [
            {
                "symbol": "FFUSDT",
                "strategy_config_hash": "c223e6dbad4d588e2916b47fa303762c6241bc18346765786950e51b3cd5cbdd",
                "accounts": [
                    {
                        "account_label": "account-2",
                        "order_count": 2,
                        "intent_summary": "BUY LIMIT 684.000000000000000000@0.146040000000000000, BUY LIMIT 691.000000000000000000@0.144540000000000000",
                    },
                    {
                        "account_label": "primary",
                        "order_count": 1,
                        "intent_summary": "买入 691 @ 0.14454（限价）",
                    },
                ],
            }
        ],
        "group_count": 1,
    }
    form = _serverchan_form(
        {
            "event": "ops_alert",
            "alert_name": "live_position_intent_divergence",
            "severity": "critical",
            "summary": "Live accounts diverged on order intent for identical strategy configs",
            "observed_at": "2026-09-15T10:19:51+00:00",
            "details": details,
        }
    )

    expected_account2 = (
        "  - `account-2`（已下单 **2** 笔）：\n"
        "    - 买入 684 @ 0.14604（限价）\n"
        "    - 买入 691 @ 0.14454（限价）"
    )
    expected_primary = "  - `primary`（已下单 **1** 笔）：买入 691 @ 0.14454（限价）"

    assert expected_account2 in form["desp"]
    assert expected_primary in form["desp"]
    assert "0000000000000000" not in form["desp"]
    assert "指纹" not in form["desp"]
    assert "- **影响**" not in form["desp"]


def test_container_memory_pressure_human_formatting() -> None:
    details = {
        "service": "market-data",
        "memory_current_mb": 260.6,
        "memory_limit_mb": 640.0,
        "memory_swap_current_mb": 32.6,
        "memory_swap_growth_mb": 32.6,
        "memory_peak_mb": 262.9,
    }
    form = _serverchan_form(
        {
            "event": "ops_alert",
            "alert_name": "container_memory_pressure",
            "severity": "warning",
            "summary": "Container market-data pushed anonymous memory into swap",
            "observed_at": "2026-09-15T06:41:53+00:00",
            "details": details,
        }
    )
    assert "[警告] market-data：服务匿名内存被换出" in form["desp"]
    assert "market-data" in form["desp"]
    assert "物理内存用量**：`260.6 MB` / `640.0 MB`（占比 **40.7%**，峰值 262.9 MB）" in form["desp"]
    assert "Swap 换出情况**：当前换出 `32.6 MB` （本次新增: `+32.6 MB`）" in form["desp"]
    assert "物理内存充足" in form["desp"]


def test_position_and_signal_divergence_human_formatting() -> None:
    # 1. Position divergence
    pos_details = {
        "pair_count": 1,
        "differences": [
            {
                "accounts": ["primary", "account-2"],
                "strategy_config_hash": "c223e6dbad4d588e2916b47fa303762c6241bc18346765786950e51b3cd5cbdd",
                "quantity_differences": [
                    {
                        "symbol": "BTWUSDT",
                        "position_side": "BOTH",
                        "left_quantity": "141",
                        "right_quantity": "0",
                    }
                ],
            }
        ],
    }
    pos_form = _serverchan_form(
        {
            "event": "ops_alert",
            "alert_name": "live_position_divergence",
            "severity": "critical",
            "summary": "Comparable live account position snapshots diverged",
            "observed_at": "2026-09-15T07:57:44+00:00",
            "details": pos_details,
        }
    )
    assert pos_form["title"] == "CML | 严重 | BTWUSDT | 账户持仓发生差异"
    assert "分叉标的**：`BTWUSDT`（方向: BOTH，策略配置: `c223e6db`）" in pos_form["desp"]
    assert "`primary`：持仓 **141**" in pos_form["desp"]
    assert "`account-2`：持仓 **0**" in pos_form["desp"]

    # 2. Signal divergence
    sig_details = {
        "group_count": 1,
        "differences": [
            {
                "symbol": "BTCUSDT",
                "bucket_start": "2026-09-15 15:55:00",
                "strategy_config_hash": "c223e6dbad4d588e2916b47fa303762c6241bc18346765786950e51b3cd5cbdd",
                "accounts": [
                    {
                        "account_label": "primary",
                        "signal_count": 1,
                        "candidate_count": 3,
                    },
                    {
                        "account_label": "account-2",
                        "signal_count": 0,
                        "candidate_count": 0,
                    },
                ],
            }
        ],
    }
    sig_form = _serverchan_form(
        {
            "event": "ops_alert",
            "alert_name": "live_signal_divergence",
            "severity": "critical",
            "summary": "Comparable live accounts emitted divergent signals",
            "observed_at": "2026-09-15T07:57:44+00:00",
            "details": sig_details,
        }
    )
    assert sig_form["title"] == "CML | 严重 | BTCUSDT | 账户信号发生分叉"
    assert "分叉标的**：`BTCUSDT`（时间桶: `2026-09-15 23:55:00`，策略配置: `c223e6db`）" in sig_form["desp"]
    assert "`primary`：有效信号 **1** 个（候选: 3）" in sig_form["desp"]
    assert "`account-2`：有效信号 **0** 个（候选: 0）" in sig_form["desp"]
