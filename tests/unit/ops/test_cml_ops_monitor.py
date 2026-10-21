import argparse
import dataclasses
import json
import urllib.parse
import urllib.request
from datetime import UTC, datetime, timedelta

import pytest

from deploy.ops.cml_ops_monitor import (
    _ALERT_LABELS,
    Alert,
    ContainerSnapshot,
    LogSignals,
    MonitorConfig,
    OpsMonitor,
    _deliver_external_heartbeat,
    _deliver_serverchan,
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
    evaluate_log_signals,
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


def test_merge_log_signals_keeps_every_field() -> None:
    """Every LogSignals field must survive the per-account merge.

    The merge lists fields by hand; a field omitted there is dropped for every
    account but the last, which is how a stuck exit stayed invisible.
    """

    left = LogSignals(
        telemetry_persist_failures=1,
        exit_processing_degraded_symbols=("龙虾USDT",),
        dead_connection_tasks=("grp-a",),
        latest_rss_bytes=100,
        rss_observed_at=datetime(2026, 9, 15, tzinfo=UTC),
    )
    right = LogSignals(
        telemetry_persist_failures=3,
        exit_processing_degraded_symbols=("BTWUSDT",),
        dead_connection_tasks=("grp-b",),
        fact_inconsistencies=(("primary", "BTCUSDT", "repair_blocked"),),
        deferred_exits=(("primary", "ETHUSDT", "deferred_until_ready"),),
        expired_candidates=(("primary", "SOLUSDT"),),
    )

    merged = _merge_log_signals(left, right)

    assert merged.telemetry_persist_failures == 4
    assert merged.exit_processing_degraded_symbols == ("龙虾USDT", "BTWUSDT")
    assert merged.dead_connection_tasks == ("grp-a", "grp-b")
    assert merged.latest_rss_bytes == 100
    assert merged.rss_observed_at is not None
    assert merged.fact_inconsistencies == (("primary", "BTCUSDT", "repair_blocked"),)
    assert merged.deferred_exits == (("primary", "ETHUSDT", "deferred_until_ready"),)
    assert merged.expired_candidates == (("primary", "SOLUSDT"),)

    # Any field added to LogSignals must be merged above; keep this list honest.
    assert {f.name for f in dataclasses.fields(LogSignals)} == {
        "telemetry_persist_failures",
        "exit_processing_degraded_symbols",
        "dead_connection_tasks",
        "latest_rss_bytes",
        "rss_observed_at",
        "fact_inconsistencies",
        "deferred_exits",
        "expired_candidates",
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


def test_console_log_record_keeps_single_quoted_reason_with_spaces() -> None:
    record = _parse_log_record(
        "2026-10-09 14:20:00 [warning  ] position_repair_blocked "
        "reason='repair projection does not match actual account exposure' "
        "account_label=account-3 symbol=BTWUSDT"
    )

    assert record["event"] == "position_repair_blocked"
    assert record["reason"] == (
        "repair projection does not match actual account exposure"
    )
    assert record["account_label"] == "account-3"
    assert record["symbol"] == "BTWUSDT"


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


def test_log_signals_preserve_each_dead_market_group_from_console_output(
    tmp_path,
) -> None:
    class Runner:
        def run(self, args, *, timeout_seconds):
            del timeout_seconds
            if args[:2] == ["docker", "logs"]:
                return (
                    "2026-10-06 08:05:34 [error    ] "
                    "market_data_connection_task_not_alive "
                    "group_ids=market:aggTrade:0000,market:markPrice@1s:0000\n"
                )
            return ""

    monitor = OpsMonitor(
        MonitorConfig(state_path=tmp_path / "state.json"),
        runner=Runner(),
    )

    signals = monitor._log_signals(
        "market",
        None,
        since_seconds=60,
    )

    assert signals.dead_connection_tasks == (
        "market:aggTrade:0000",
        "market:markPrice@1s:0000",
    )


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


def test_monitor_reads_market_data_logs_once_for_all_accounts(
    tmp_path,
    monkeypatch,
) -> None:
    from deploy.ops import cml_ops_monitor

    accounts = ("primary", "account-2", "account-3", "account-4")
    container_ids = {
        "market-data": "market-container",
        **{
            f"nextgen-strategy-{account}": f"{account}-container"
            for account in accounts
        },
    }
    monitor = OpsMonitor(
        MonitorConfig(
            services=(),
            live_account_labels=accounts,
            state_path=tmp_path / "state.json",
            retention_timer_unit="",
        )
    )
    log_reads: list[tuple[str | None, str | None]] = []

    def collect_logs(market_id, live_id, *, since_seconds):
        del since_seconds
        log_reads.append((market_id, live_id))
        return LogSignals(
            dead_connection_tasks=("market:aggTrade:0000",) if market_id else ()
        )

    monkeypatch.setattr(monitor, "_container_snapshots", lambda: ())
    monkeypatch.setattr(
        monitor, "_container_id", lambda service: container_ids.get(service)
    )
    monkeypatch.setattr(monitor, "_log_signals", collect_logs)
    monkeypatch.setattr(monitor, "_retention_schedule_state", lambda: None)
    monkeypatch.setattr(monitor, "_market_revision_purge_schedule_state", lambda: None)
    monkeypatch.setattr(monitor, "_market_revision_archive_schedule_state", lambda: None)
    monkeypatch.setattr(monitor, "_disk_usage_alerts", lambda **_kwargs: ())
    monkeypatch.setattr(monitor, "_emit", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(monitor, "_emit_resolutions", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(monitor, "_send_external_heartbeat", lambda *_args: None)
    monkeypatch.setattr(cml_ops_monitor, "read_maintenance_window", lambda *_args: None)

    alerts = monitor.run_once()

    assert log_reads.count(("market-container", None)) == 1
    assert sum(market_id is not None for market_id, _live_id in log_reads) == 1
    market_alert = next(alert for alert in alerts if alert.name == "market_task_not_alive")
    assert market_alert.details["group_ids"] == ("market:aggTrade:0000",)


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
    assert rss_warning_fraction_for("nextgen-strategy-primary", 0.75) == 0.75
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
            snapshot("nextgen-strategy-primary"),
            rss_warning_fraction=rss_warning_fraction_for("nextgen-strategy-primary", 0.75),
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

    This is the real alert: nextgen-strategy-account-3 restarted, the trend
    baseline was reset to its near-empty startup footprint, and the climb to
    ~148 MiB looked like 130 MiB of growth -- at 19% of its limit.
    """

    warmup = {
        "service": "nextgen-strategy-account-3",
        "baseline_bytes": 18 * 1024 * 1024,
        "baseline_age_seconds": 1080.0,
        "consecutive_samples": 18,
        "required_samples": 3,
        "growth_bytes": 64 * 1024 * 1024,
        "growth_window_seconds": 1800.0,
        "metric_source": "docker_stats_working_set",
    }

    # Without a limit to compare against, the old behaviour is unchanged.
    assert (
        evaluate_container_memory_growth(
            **warmup,
            current_bytes=148 * 1024 * 1024,
        )
        != ()
    )

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
        service="nextgen-strategy-primary",
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

    assert (
        evaluate_container_memory_growth(
            **common,
            consecutive_samples=2,
        )
        == ()
    )

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
        monitor._state["memory_sample_container_ids"]["market-data"] == "new-container"
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
                    "memory.stat.file=420\n"
                    "memory.stat.shmem=160\n"
                    "memory.stat.slab=12\n"
                    "memory.stat.file_dirty=8\n"
                    "memory.stat.file_writeback=4\n"
                )
            if args and args[0] == "systemctl":
                # The retention-schedule check reads systemd on the host; this
                # runner stubs only the container and database probes.
                return ""
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
    assert stats.file_bytes == 420
    assert stats.shmem_bytes == 160
    assert stats.slab_bytes == 12
    assert stats.file_dirty_bytes == 8
    assert stats.file_writeback_bytes == 4


def test_postgres_memory_pressure_context_is_aggregate_and_query_text_free(
    tmp_path,
) -> None:
    class Runner:
        args: list[str] | None = None

        def run(self, args, *, timeout_seconds):
            del timeout_seconds
            self.args = args
            return (
                '{"connections":{"total":65,"active":1},'
                '"temp_heavy_queries":[{"queryid":"42","temp_blks_written":99}]}'
            )

    runner = Runner()
    monitor = OpsMonitor(
        MonitorConfig(state_path=tmp_path / "state.json"),
        runner=runner,
    )

    context = monitor._postgres_memory_pressure_context("postgres")

    assert context["connections"] == {"total": 65, "active": 1}
    assert context["temp_heavy_queries"] == [{"queryid": "42", "temp_blks_written": 99}]
    assert runner.args is not None
    sql = str(runner.args[-1])
    assert "pg_stat_statements" in sql
    assert "queryid" in sql
    assert "'query', query" not in sql


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
    """The scope is repeated in the body, so preserve the full alert label."""

    assert _serverchan_title("严重", "account-3", "行情延迟过高") == (
        "CML | 严重 | account-3 | 行情延迟过高"
    )

    # Too long for the cap: the scope goes, the label survives intact.
    title = _serverchan_title("严重", "account-4", "实时策略心跳过期")
    assert title == "CML | 严重 | 实时策略心跳过期"
    assert "过期" in title

    # Same rule on the resolution side.
    assert _serverchan_title("恢复", "account-4", "实时策略心跳过期") == (
        "CML | 恢复 | 实时策略心跳过期"
    )

    # No scope at all still renders.
    assert _serverchan_title("恢复", "", "行情延迟过高") == "CML | 恢复 | 行情延迟过高"


def test_build_config_discovers_live_account_labels_from_compose_files(
    tmp_path, monkeypatch
) -> None:
    base_file = tmp_path / "compose.server.yaml"
    overlay_file = tmp_path / "compose.next_gen.yaml"
    base_file.write_text(
        "services:\n"
        "  postgres:\n"
        "  market-data:\n"
        "volumes:\n"
        "  postgres-data:\n",
        encoding="utf-8",
    )
    overlay_file.write_text(
        "services:\n"
        "  nextgen-strategy-primary:\n"
        "  nextgen-strategy-account-2:\n"
        "  nextgen-strategy-account-4:\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("CML_MONITOR_SERVICES", raising=False)
    args = argparse.Namespace(
        project_directory=str(tmp_path),
        compose_file=str(tmp_path / "ignored-compose.yaml"),
        services=None,
        interval_seconds=60.0,
        log_window_seconds=120.0,
        rss_warning_fraction=0.75,
        rss_critical_fraction=0.90,
        rss_growth_bytes=64 * 1024 * 1024,
        rss_growth_window_seconds=1_800.0,
        alert_cooldown_seconds=900.0,
        command_timeout_seconds=15.0,
        state_path=str(tmp_path / "state.json"),
    )

    config = build_config(args)

    assert config.live_account_labels == ("primary", "account-2", "account-4")
    assert config.services == (
        "postgres",
        "market-data",
        "research-collector",
        "nextgen-strategy-primary",
        "nextgen-strategy-account-2",
        "nextgen-strategy-account-4",
    )
    monitor = OpsMonitor(config, runner=None)
    assert monitor._compose_prefix()[-4:] == [
        "-f",
        str(overlay_file),
        "--profile",
        "nextgen",
    ]


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

    assert monitor._container_id("nextgen-strategy-account-2") == "container-id"
    assert runner.last_args == [
        "docker",
        "ps",
        "--filter",
        "label=com.docker.compose.service=nextgen-strategy-account-2",
        "--format",
        "{{.ID}}",
    ]


def test_serverchan_config_and_payload(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("SERVERCHAN_SENDKEY", "SCT-test-key")
    args = argparse.Namespace(
        project_directory=str(tmp_path),
        compose_file=str(tmp_path / "compose.yaml"),
        services="postgres",
        interval_seconds=60.0,
        log_window_seconds=120.0,
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
            "details": {"service": "nextgen-strategy-primary"},
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
    assert "nextgen-strategy-primary" in form["desp"]


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
            "alert_name": "live_heartbeat_stale",
            "severity": "critical",
            "summary": "检查失败：heartbeat stale",
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
        MonitorConfig(
            state_path=tmp_path / "state.json",
            consecutive_alerts_required=1,
            consecutive_resolutions_required=1,
        ),
    )

    monitor._emit(
        Alert(
            "container_unhealthy",
            "critical",
            "service health failed",
            {"service": "nextgen-strategy-account-2"},
        ),
        now=100.0,
    )
    monitor._emit_resolutions(set(), now=160.0)

    assert delivered[1]["event"] == "ops_alert_resolved"
    assert delivered[1]["duration_seconds"] == 60.0
    assert delivered[1]["details"] == {"service": "nextgen-strategy-account-2"}


def test_alert_debounce_requires_consecutive_failures(monkeypatch, tmp_path) -> None:
    delivered: list[dict[str, object]] = []

    def capture(_webhook, _sendkey, payload) -> None:
        delivered.append(dict(payload))

    monkeypatch.setattr(
        "deploy.ops.cml_ops_monitor._deliver_notification",
        capture,
    )
    monitor = OpsMonitor(
        MonitorConfig(
            state_path=tmp_path / "state.json",
            consecutive_alerts_required=3,
            consecutive_resolutions_required=3,
        ),
    )
    alert = Alert(
        "database_check_failed:primary",
        "critical",
        "Database check failed",
        {"account_label": "primary", "error_type": "TimeoutError"},
    )

    # 1st sample: pending (1 of 3), no alert emitted
    monitor._emit(alert, now=100.0)
    assert len(delivered) == 0

    # 2nd sample: pending (2 of 3), no alert emitted
    monitor._emit(alert, now=115.0)
    assert len(delivered) == 0

    # 3rd sample: reached 3 of 3, alert emitted!
    monitor._emit(alert, now=130.0)
    assert len(delivered) == 1
    assert delivered[0]["event"] == "ops_alert"
    assert delivered[0]["alert_name"] == "database_check_failed:primary"


def test_resolution_debounce_requires_consecutive_passes(monkeypatch, tmp_path) -> None:
    delivered: list[dict[str, object]] = []

    def capture(_webhook, _sendkey, payload) -> None:
        delivered.append(dict(payload))

    monkeypatch.setattr(
        "deploy.ops.cml_ops_monitor._deliver_notification",
        capture,
    )
    monitor = OpsMonitor(
        MonitorConfig(
            state_path=tmp_path / "state.json",
            consecutive_alerts_required=1,
            consecutive_resolutions_required=3,
        ),
    )
    alert = Alert(
        "container_unhealthy",
        "critical",
        "service health failed",
        {"service": "postgres"},
    )

    # Fire alert
    monitor._emit(alert, now=100.0)
    assert len(delivered) == 1
    assert delivered[0]["event"] == "ops_alert"

    # 1st pass: healthy, but not yet 3 consecutive passes
    monitor._emit_resolutions(set(), now=115.0)
    assert len(delivered) == 1

    # 2nd pass: healthy
    monitor._emit_resolutions(set(), now=130.0)
    assert len(delivered) == 1

    # 3rd pass: reached 3, resolution emitted!
    monitor._emit_resolutions(set(), now=145.0)
    assert len(delivered) == 2
    assert delivered[1]["event"] == "ops_alert_resolved"
    assert delivered[1]["alert_name"] == "container_unhealthy"


def test_cooldown_persists_across_resolutions(monkeypatch, tmp_path) -> None:
    delivered: list[dict[str, object]] = []

    def capture(_webhook, _sendkey, payload) -> None:
        delivered.append(dict(payload))

    monkeypatch.setattr(
        "deploy.ops.cml_ops_monitor._deliver_notification",
        capture,
    )
    monitor = OpsMonitor(
        MonitorConfig(
            state_path=tmp_path / "state.json",
            consecutive_alerts_required=1,
            consecutive_resolutions_required=1,
            alert_cooldown_seconds=900.0,
        ),
    )
    alert = Alert(
        "database_check_failed:primary",
        "critical",
        "Database check failed",
        {"account_label": "primary", "error_type": "TimeoutError"},
    )

    # Fire at t=100s
    monitor._emit(alert, now=100.0)
    assert len(delivered) == 1

    # Resolves at t=160s
    monitor._emit_resolutions(set(), now=160.0)
    assert len(delivered) == 2
    assert delivered[1]["event"] == "ops_alert_resolved"

    # Re-occurs at t=200s (only 100s after previous emit, cooldown is 900s)
    monitor._emit(alert, now=200.0)
    # Must NOT send another ops_alert notification!
    assert len(delivered) == 2
    assert (
        "database_check_failed:primary"
        not in monitor._state["active_alerts"]
    )
    assert (
        "database_check_failed:primary"
        not in monitor._state["active_alert_context"]
    )
    assert (
        "database_check_failed:primary"
        not in monitor._state["alert_occurrences"]
    )


def test_orphaned_active_alert_is_repaired_without_fake_recovery(
    monkeypatch, tmp_path
) -> None:
    delivered: list[dict[str, object]] = []
    monkeypatch.setattr(
        "deploy.ops.cml_ops_monitor._deliver_notification",
        lambda _webhook, _sendkey, payload: delivered.append(dict(payload)),
    )
    monitor = OpsMonitor(
        MonitorConfig(
            state_path=tmp_path / "state.json",
            consecutive_resolutions_required=1,
        ),
    )
    monitor._state.setdefault("active_alerts", {})["database_storage_growth"] = 100.0
    monitor._state.setdefault("alert_cooldowns", {})["database_storage_growth"] = 150.0

    monitor._emit(
        Alert("database_storage_growth", "warning", "growth"),
        now=200.0,
    )

    monitor._emit_resolutions(set(), now=201.0)

    assert delivered == []
    assert monitor._state["active_alerts"] == {}
    assert monitor._state["active_alert_context"] == {}


def test_database_growth_reminder_uses_six_hour_cooldown(monkeypatch, tmp_path) -> None:
    delivered: list[dict[str, object]] = []
    monkeypatch.setattr(
        "deploy.ops.cml_ops_monitor._deliver_notification",
        lambda _webhook, _sendkey, payload: delivered.append(dict(payload)),
    )
    monitor = OpsMonitor(
        MonitorConfig(
            state_path=tmp_path / "state.json",
            consecutive_alerts_required=1,
            alert_cooldown_seconds=900.0,
            database_storage_alert_cooldown_seconds=6 * 60 * 60,
        ),
    )
    alert = Alert("database_storage_growth", "warning", "growth")

    monitor._emit(alert, now=100.0)
    monitor._emit(alert, now=100.0 + 6 * 60 * 60 - 1)
    assert len(delivered) == 1

    monitor._emit(alert, now=100.0 + 6 * 60 * 60)

    assert len(delivered) == 2
    assert delivered[-1]["event"] == "ops_alert"
    assert delivered[-1]["details"]["occurrence_count"] == 3


def test_flapping_detection_flags_frequent_state_changes(monkeypatch, tmp_path) -> None:
    delivered: list[dict[str, object]] = []

    def capture(_webhook, _sendkey, payload) -> None:
        delivered.append(dict(payload))

    monkeypatch.setattr(
        "deploy.ops.cml_ops_monitor._deliver_notification",
        capture,
    )
    monitor = OpsMonitor(
        MonitorConfig(
            state_path=tmp_path / "state.json",
            consecutive_alerts_required=1,
            consecutive_resolutions_required=1,
            alert_cooldown_seconds=10.0,
            flapping_window_seconds=600.0,
        ),
    )
    alert = Alert(
        "flapping_service",
        "critical",
        "Flapping service failed",
        {"service": "flapping"},
    )

    # 1st fire at t=100
    monitor._emit(alert, now=100.0)
    assert "[FLAPPING]" not in delivered[0]["summary"]

    # Resolves at t=115
    monitor._emit_resolutions(set(), now=115.0)

    # 2nd fire at t=130 (15s after resolution, within 600s flapping window)
    monitor._emit(alert, now=130.0)
    assert len(delivered) == 3
    assert "[FLAPPING]" in delivered[2]["summary"]
    assert delivered[2]["details"].get("flapping") is True
    assert delivered[2]["details"].get("flap_count") == 1


@pytest.mark.parametrize("maintenance", [False, True])
def test_unhealthy_nextgen_account_alerts_without_automatic_restart(
    tmp_path,
    monkeypatch,
    maintenance,
) -> None:
    expected_service = "nextgen-strategy-account-2"

    class Runner:
        def __init__(self) -> None:
            self.calls: list[list[str]] = []

        def run(self, args, *, timeout_seconds):
            del timeout_seconds
            command = list(args)
            self.calls.append(command)
            if command[:3] == ["docker", "ps", "--filter"]:
                service = command[3].rsplit("=", 1)[1]
                if service == expected_service:
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
                return ""
            if command and command[0] == "systemctl":
                return ""
            if command[:2] == ["docker", "compose"]:
                return "unexpected restart\n"
            raise AssertionError(f"unexpected command: {command}")

    runner = Runner()
    monitor = OpsMonitor(
        MonitorConfig(
            project_directory=tmp_path,
            compose_file=tmp_path / "compose.yaml",
            compose_files=(tmp_path / "compose.yaml",),
            compose_profiles=("nextgen",),
            compose_env_file=None,
            services=(expected_service,),
            live_account_labels=("account-2",),
            state_path=tmp_path / "state.json",
        ),
        runner=runner,
        clock=lambda: 1_000.0,
    )

    if maintenance:
        from deploy.ops.maintenance_window import (
            clear_maintenance_window,
            write_maintenance_window,
        )

        marker = tmp_path / "maintenance.json"
        monkeypatch.setenv("CML_MAINTENANCE_WINDOW_FILE", str(marker))
        write_maintenance_window(
            marker, started_at=datetime.now(UTC), expected_seconds=900
        )
        assert monitor.run_once() == ()
        assert not any(call[:2] == ["docker", "compose"] for call in runner.calls)
        clear_maintenance_window(marker)

    alerts = monitor.run_once()
    assert any(alert.name == "live_heartbeat_stale:account-2" for alert in alerts)
    assert not any(call[:2] == ["docker", "compose"] for call in runner.calls)
    archives = tuple((tmp_path / "crash-logs").glob("*.log"))
    assert len(archives) == 1
    assert "container_id=account-2-container" in archives[0].read_text(encoding="utf-8")
    assert any(
        alert.details.get("crash_log_archive") == str(archives[0])
        for alert in alerts
    )


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
    assert (
        "物理内存用量**：`260.6 MB` / `640.0 MB`（占比 **40.7%**，峰值 262.9 MB）"
        in form["desp"]
    )
    assert (
        "Swap 换出情况**：当前换出 `32.6 MB` （本次新增: `+32.6 MB`）" in form["desp"]
    )
    assert "物理内存充足" in form["desp"]


def test_serverchan_title_compact_preserves_long_symbol() -> None:
    # 7-char symbol fits in standard pipe format (32 chars)
    t1 = _serverchan_title("严重", "BTCUSDT", "账户信号发生分叉")
    assert t1 == "CML | 严重 | BTCUSDT | 账户信号发生分叉"
    assert len(t1) <= 32

    # 11-char symbol fits in compact bracket format (30 chars)
    t2 = _serverchan_title("严重", "USELESSUSDT", "账户信号发生分叉")
    assert t2 == "CML[严重] USELESSUSDT | 账户信号发生分叉"
    assert len(t2) <= 32

    # 13-char symbol fits in compact format (32 chars)
    t3 = _serverchan_title("严重", "1000PEPEUSDT", "账户信号发生分叉")
    assert t3 == "CML[严重] 1000PEPEUSDT | 账户信号发生分叉"
    assert len(t3) <= 32


def test_all_alert_labels_fit_in_serverchan_title() -> None:
    for name, label in _ALERT_LABELS.items():
        # Test with primary scope
        title = _serverchan_title("严重", "primary", label)
        assert len(title) <= 32, (
            f"Alert {name} label '{label}' title too long: {title} ({len(title)} chars)"
        )
        # Test with no scope
        title_no_scope = _serverchan_title("严重", None, label)
        assert len(title_no_scope) <= 32, (
            f"Alert {name} label '{label}' title without scope too long: {title_no_scope} ({len(title_no_scope)} chars)"
        )
        # Test with recovery
        title_rec = _serverchan_title("恢复", "primary", label)
        assert len(title_rec) <= 32, (
            f"Alert {name} label '{label}' recovery title too long: {title_rec} ({len(title_rec)} chars)"
        )


def test_evaluate_log_signals_fact_inconsistencies() -> None:
    signals_warn = LogSignals(
        fact_inconsistencies=(
            ("primary", "BTCUSDT", "position_repair_blocked"),
            ("primary", "BTCUSDT", "repair_rejected"),
        )
    )
    alerts = evaluate_log_signals(signals_warn)
    assert len(alerts) == 1
    assert alerts[0].name == "live_local_fact_inconsistency:primary:BTCUSDT"
    assert alerts[0].severity == "warning"
    assert alerts[0].details["count"] == 2
    assert alerts[0].details["symbol"] == "BTCUSDT"

    repeated = LogSignals(
        fact_inconsistencies=(
            ("primary", "BTCUSDT", "position_repair_blocked"),
            ("primary", "BTCUSDT", "position_repair_blocked"),
            ("primary", "BTCUSDT", "position_repair_blocked"),
        )
    )
    repeated_alert = evaluate_log_signals(repeated)[0]
    assert repeated_alert.severity == "warning"
    assert repeated_alert.details["count"] == 1

    signals_crit = LogSignals(
        fact_inconsistencies=(
            ("primary", "BTCUSDT", "position_repair_blocked"),
            ("primary", "BTCUSDT", "repair_rejected"),
            ("primary", "BTCUSDT", "live_position_repair_failed"),
        )
    )
    alerts_crit = evaluate_log_signals(signals_crit)
    assert len(alerts_crit) == 1
    assert alerts_crit[0].severity == "critical"
    assert alerts_crit[0].details["count"] == 3


def test_evaluate_log_signals_deferred_exits() -> None:
    signals_few = LogSignals(
        deferred_exits=(
            ("primary", "IMXUSDT", "sync_wait"),
            ("primary", "IMXUSDT", "sync_wait"),
        )
    )
    assert len(evaluate_log_signals(signals_few)) == 0

    signals_warn = LogSignals(deferred_exits=(("primary", "IMXUSDT", "sync_wait"),) * 3)
    alerts_warn = evaluate_log_signals(signals_warn)
    assert len(alerts_warn) == 1
    assert alerts_warn[0].name == "live_exit_evaluation_deferred:primary:IMXUSDT"
    assert alerts_warn[0].severity == "warning"

    signals_crit = LogSignals(
        deferred_exits=(("primary", "IMXUSDT", "sync_wait"),) * 10
    )
    alerts_crit = evaluate_log_signals(signals_crit)
    assert len(alerts_crit) == 1
    assert alerts_crit[0].severity == "critical"


def test_evaluate_log_signals_expired_candidates() -> None:
    signals_one = LogSignals(expired_candidates=(("primary", "MARSCOINUSDT"),))
    assert len(evaluate_log_signals(signals_one)) == 0

    signals_warn = LogSignals(expired_candidates=(("primary", "MARSCOINUSDT"),) * 2)
    alerts_warn = evaluate_log_signals(signals_warn)
    assert len(alerts_warn) == 1
    assert alerts_warn[0].name == "live_candidate_expired:primary:MARSCOINUSDT"
    assert alerts_warn[0].severity == "warning"

    signals_crit = LogSignals(expired_candidates=(("primary", "MARSCOINUSDT"),) * 5)
    alerts_crit = evaluate_log_signals(signals_crit)
    assert len(alerts_crit) == 1
    assert alerts_crit[0].severity == "critical"


def test_severity_escalation_bypasses_cooldown_and_tracks_occurrences(
    monkeypatch, tmp_path
) -> None:
    delivered: list[dict[str, object]] = []

    def capture(_webhook, _sendkey, payload) -> None:
        delivered.append(dict(payload))

    monkeypatch.setattr(
        "deploy.ops.cml_ops_monitor._deliver_notification",
        capture,
    )
    monitor = OpsMonitor(
        MonitorConfig(
            state_path=tmp_path / "state.json",
            consecutive_alerts_required=1,
            consecutive_resolutions_required=1,
            alert_cooldown_seconds=900.0,
        ),
    )

    # 1. First warning emission
    warn_alert = Alert(
        "container_memory_high:primary",
        "warning",
        "container memory high",
        {"memory_current_mb": 1024},
    )
    monitor._emit(warn_alert, now=100.0)
    assert len(delivered) == 1
    assert delivered[0]["severity"] == "warning"
    assert delivered[0]["details"]["occurrence_count"] == 1

    # 2. Second warning emission at t=200 (within 900s cooldown) - suppressed
    monitor._emit(warn_alert, now=200.0)
    assert len(delivered) == 1

    # 3. Third emission at t=300: escalated to critical - bypasses cooldown!
    crit_alert = Alert(
        "container_memory_high:primary",
        "critical",
        "container memory high",
        {"memory_current_mb": 2048},
    )
    monitor._emit(crit_alert, now=300.0)
    assert len(delivered) == 2
    assert delivered[1]["severity"] == "critical"
    assert "[ESCALATED]" in delivered[1]["summary"]
    assert delivered[1]["details"]["escalated"] is True
    assert delivered[1]["details"]["escalated_from"] == "warning"
    assert delivered[1]["details"]["occurrence_count"] == 3
    assert delivered[1]["details"]["active_duration_seconds"] == 200.0


def test_single_recovery_notification_emitted_when_stable(
    monkeypatch, tmp_path
) -> None:
    delivered: list[dict[str, object]] = []

    def capture(_webhook, _sendkey, payload) -> None:
        delivered.append(dict(payload))

    monkeypatch.setattr(
        "deploy.ops.cml_ops_monitor._deliver_notification",
        capture,
    )
    monitor = OpsMonitor(
        MonitorConfig(
            state_path=tmp_path / "state.json",
            consecutive_alerts_required=1,
            consecutive_resolutions_required=2,
            alert_cooldown_seconds=900.0,
        ),
    )

    alert = Alert(
        "database_check_failed:primary",
        "critical",
        "database check failed",
        {"account_label": "primary"},
    )
    monitor._emit(alert, now=100.0)
    monitor._emit(alert, now=160.0)
    assert len(delivered) == 1

    # Cycle 1 clean: not yet resolved (requires 2 consecutive)
    monitor._emit_resolutions(set(), now=220.0)
    assert len(delivered) == 1

    # Cycle 2 clean: threshold met, single recovery emitted
    monitor._emit_resolutions(set(), now=280.0)
    assert len(delivered) == 2
    res_payload = delivered[1]
    assert res_payload["event"] == "ops_alert_resolved"
    assert res_payload["duration_seconds"] == 180.0
    assert res_payload["occurrence_count"] == 2

    # Cycle 3 clean: no second resolution notification
    monitor._emit_resolutions(set(), now=340.0)
    assert len(delivered) == 2


def test_serverchan_formatting_new_alerts() -> None:
    # 1. Fact inconsistency
    form_fact = _serverchan_form(
        {
            "event": "ops_alert",
            "alert_name": "live_local_fact_inconsistency:primary:BTCUSDT",
            "severity": "warning",
            "summary": "Local facts inconsistent",
            "observed_at": "2026-10-03T10:00:00+00:00",
            "details": {
                "account_label": "primary",
                "symbol": "BTCUSDT",
                "reason": "repair_blocked",
                "count": 2,
            },
        }
    )
    assert "局部事实或投影不一致" in form_fact["title"]
    assert "2026-10-03 18:00:00（北京时间）" in form_fact["desp"]
    assert "BTCUSDT" in form_fact["desp"]
    assert "已做处理" in form_fact["desp"]
    assert "持续交易" in form_fact["desp"]
    assert "安全提醒" in form_fact["desp"]

    # 2. Deferred exit
    form_exit = _serverchan_form(
        {
            "event": "ops_alert",
            "alert_name": "live_exit_evaluation_deferred:primary:ETHUSDT",
            "severity": "warning",
            "summary": "Exit deferred",
            "observed_at": "2026-10-03T10:00:00+00:00",
            "details": {
                "account_label": "primary",
                "symbol": "ETHUSDT",
                "reason": "sync_wait",
                "count": 4,
            },
        }
    )
    assert "平仓评估多次延后" in form_exit["title"]
    assert "ETHUSDT" in form_exit["desp"]

    # 3. Candidate expired
    form_cand = _serverchan_form(
        {
            "event": "ops_alert",
            "alert_name": "live_candidate_expired:primary:SOLUSDT",
            "severity": "warning",
            "summary": "Candidate expired",
            "observed_at": "2026-10-03T10:00:00+00:00",
            "details": {
                "account_label": "primary",
                "symbol": "SOLUSDT",
                "count": 3,
            },
        }
    )
    assert "候选执行前已超期" in form_cand["title"]
    assert "零 POST" in form_cand["desp"]

    # 4. Recovery with cumulative count
    form_res = _serverchan_form(
        {
            "event": "ops_alert_resolved",
            "alert_name": "database_check_failed:primary",
            "severity": "critical",
            "summary": "Database check resolved",
            "observed_at": "2026-10-03T10:15:00+00:00",
            "duration_seconds": 900.0,
            "occurrence_count": 5,
            "details": {"account_label": "primary"},
        }
    )
    assert "恢复" in form_res["title"]
    assert "累计频次**：故障期间共触发 **5** 次" in form_res["desp"]


def test_deliver_serverchan_bounded_retry_and_no_leak(monkeypatch, capsys) -> None:
    attempts: list[int] = []

    class MockResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return False

        def read(self):
            return b'{"code": 0, "message": "success"}'

    def mock_urlopen(request, timeout):
        del request, timeout
        attempts.append(len(attempts) + 1)
        if len(attempts) == 1:
            raise urllib.error.URLError("connection reset")
        return MockResponse()

    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)
    monkeypatch.setattr("time.sleep", lambda _: None)

    _deliver_serverchan(
        "SCT123456TestKeySecret",
        {"event": "ops_alert", "summary": "test"},
    )
    assert len(attempts) == 2

    # Ensure SendKey is not leaked
    captured = capsys.readouterr()
    assert "SCT123456TestKeySecret" not in captured.out
    assert "SCT123456TestKeySecret" not in captured.err


def test_trading_core_has_no_serverchan_calls() -> None:
    """Ensure trading core does not import or make ServerChan network calls."""
    import subprocess

    cmd = ["git", "grep", "-i", "serverchan", "--", "src/crypto_momentum_lab/"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.stdout.strip() == "", (
        f"Found ServerChan in trading src: {result.stdout}"
    )


def test_monitor_is_pinned_to_nextgen_even_if_legacy_setting_remains(
    tmp_path, monkeypatch
):
    from pathlib import Path

    root = Path(__file__).parents[3]
    env_file = tmp_path / ".env.server"
    env_file.write_text("CML_TRADING_ENGINE=legacy\n")
    monkeypatch.setenv("CML_COMPOSE_ENV_FILE", str(env_file))
    monkeypatch.delenv("CML_MONITOR_SERVICES", raising=False)
    args = argparse.Namespace(
        project_directory=str(root),
        compose_file=str(root / "compose.server.yaml"),
        services=None,
        interval_seconds=60,
        log_window_seconds=120,
        rss_warning_fraction=0.75,
        rss_critical_fraction=0.9,
        rss_growth_bytes=64 * 1024 * 1024,
        rss_growth_window_seconds=1800,
        alert_cooldown_seconds=900,
        command_timeout_seconds=15,
        state_path=str(tmp_path / "state.json"),
    )
    config = build_config(args)
    assert config.compose_profiles == ("nextgen",)
    assert config.compose_files == (
        root / "compose.server.yaml",
        root / "compose.next_gen.yaml",
    )
    assert "nextgen-strategy-primary" in config.services
    assert "nextgen-strategy-account-4" in config.services
    assert not any(service.startswith("live-strategy") for service in config.services)
