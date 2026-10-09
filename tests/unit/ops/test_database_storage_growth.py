from pathlib import Path

from deploy.ops.cml_ops_monitor import (
    Alert,
    MonitorConfig,
    OpsMonitor,
    _alert_conclusion,
    _format_alert_human_details,
)

MIB = 1024**2
GIB = 1024**3
HOUR = 3600


def monitor(
    tmp_path: Path,
    samples: list[dict],
    *,
    minimum_window: float = HOUR,
    recovery_seconds: float = 30 * 60,
) -> OpsMonitor:
    instance = OpsMonitor(
        MonitorConfig(
            state_path=tmp_path / "monitor.json",
            storage_growth_minimum_window_seconds=minimum_window,
            storage_growth_recovery_seconds=recovery_seconds,
        )
    )
    instance._state["database_storage_samples"] = samples
    return instance


def sample(at: float, size: int) -> dict:
    return {
        "at": at,
        "database_bytes": size,
        "relations": {"market_revision_refs": size // 2},
    }


def footprint(size: int) -> dict:
    return {"database_bytes": size, "relations": {"market_revision_refs": size // 2}}


def test_old_expansion_does_not_describe_current_growth_as_critical(tmp_path):
    samples = [
        sample(0, GIB),
        sample(10 * HOUR, 3 * GIB),
        sample(11 * HOUR, 3 * GIB + 4 * MIB),
    ]
    instance = monitor(tmp_path, samples)
    assert (
        instance._database_storage_growth_alerts(
            footprint(3 * GIB + 8 * MIB), now=12 * HOUR
        )
        == ()
    )
    assert len(instance._state["database_storage_samples"]) == 4
    observation = instance._state["database_storage_growth_observation"]
    assert observation["recent"]["window_seconds"] == HOUR
    assert observation["recent"]["database_bytes_per_day"] == 96 * MIB
    assert observation["historical"]["window_seconds"] == 12 * HOUR
    assert not observation["historical"]["complete_window"]


def test_recent_expansion_alerts_even_when_long_window_is_quiet(tmp_path):
    instance = monitor(tmp_path, [sample(0, GIB), sample(11 * HOUR, GIB)])
    alerts = instance._database_storage_growth_alerts(
        footprint(GIB + 64 * MIB), now=12 * HOUR
    )
    assert len(alerts) == 1
    assert alerts[0].severity == "critical"
    assert alerts[0].details["growth_window_seconds"] == HOUR
    assert alerts[0].details["database_growth_bytes_per_day"] == 1536 * MIB
    assert alerts[0].details["historical_window_seconds"] == 12 * HOUR


def test_reclamation_keeps_signed_historical_net_change(tmp_path):
    instance = monitor(tmp_path, [sample(0, 3 * GIB), sample(23 * HOUR, GIB)])
    assert (
        instance._database_storage_growth_alerts(footprint(GIB + MIB), now=24 * HOUR)
        == ()
    )
    historical = instance._state["database_storage_growth_observation"]["historical"]
    assert historical["complete_window"]
    assert historical["database_delta_bytes"] == -2 * GIB + MIB
    assert len(instance._state["database_storage_samples"]) == 3


def test_sparse_history_is_not_mislabeled_as_recent_growth(tmp_path):
    instance = monitor(tmp_path, [sample(0, GIB)])
    alerts = instance._database_storage_growth_alerts(footprint(2 * GIB), now=12 * HOUR)
    assert [alert.name for alert in alerts] == ["database_storage_check_failed"]
    assert instance._state["database_storage_growth_observation"]["recent"] is None


def test_startup_waits_for_minimum_window(tmp_path):
    instance = monitor(tmp_path, [sample(0, GIB)])
    assert instance._database_storage_growth_alerts(footprint(2 * GIB), now=300) == ()
    assert instance._state["database_storage_growth_observation"]["recent"] is None


def test_default_recent_growth_window_smooths_single_hour_spike(tmp_path):
    instance = OpsMonitor(MonitorConfig(state_path=tmp_path / "monitor.json"))
    instance._state["database_storage_samples"] = [
        sample(0, GIB),
        sample(HOUR, GIB + 32 * MIB),
    ]

    assert instance._config.storage_growth_minimum_window_seconds == 6 * HOUR
    assert (
        instance._database_storage_growth_alerts(
            footprint(GIB + 32 * MIB), now=HOUR
        )
        == ()
    )
    assert (
        instance._database_storage_growth_alerts(
            footprint(GIB + 32 * MIB), now=6 * HOUR
        )
        == ()
    )


def test_unsorted_samples_use_nearest_eligible_baseline(tmp_path):
    instance = monitor(tmp_path, [sample(HOUR, GIB), sample(0, 0)])
    assert (
        instance._database_storage_growth_alerts(footprint(GIB + MIB), now=2 * HOUR)
        == ()
    )
    assert (
        instance._state["database_storage_growth_observation"]["recent"][
            "window_seconds"
        ]
        == HOUR
    )


def test_partial_day_is_not_claimed_as_complete(tmp_path):
    instance = monitor(tmp_path, [sample(0, GIB), sample(23 * HOUR - 60, GIB)])
    instance._database_storage_growth_alerts(footprint(GIB), now=24 * HOUR - 60)
    assert not instance._state["database_storage_growth_observation"]["historical"][
        "complete_window"
    ]


def test_one_boundary_anchor_preserves_full_day_measurement(tmp_path):
    instance = monitor(
        tmp_path, [sample(-HOUR, GIB), sample(0, GIB), sample(23 * HOUR, GIB)]
    )
    instance._database_storage_growth_alerts(footprint(GIB), now=24 * HOUR + 60)
    observation = instance._state["database_storage_growth_observation"]
    assert observation["historical"]["complete_window"]
    assert observation["historical"]["window_seconds"] == 24 * HOUR + 60
    assert [item["at"] for item in instance._state["database_storage_samples"]] == [
        0,
        23 * HOUR,
        24 * HOUR + 60,
    ]


def test_relation_growth_is_not_hidden_by_total_reclamation(tmp_path):
    instance = monitor(tmp_path, [sample(0, 2 * GIB)])
    alerts = instance._database_storage_growth_alerts(
        {"database_bytes": GIB, "relations": {"market_revision_refs": GIB + 32 * MIB}},
        now=HOUR,
    )
    assert len(alerts) == 1
    assert alerts[0].severity == "critical"
    assert alerts[0].details["database_growth_bytes_per_day"] == 0
    conclusion = _alert_conclusion(alerts[0].name, alerts[0].details)
    assert "重点表增长超预算" in conclusion
    lines = _format_alert_human_details(alerts[0].name, alerts[0].details)
    assert any("近期折算日增长" in line for line in lines)
    assert any("-1.00 GiB（未满日窗口）" in line for line in lines)


def test_checking_between_sample_intervals_does_not_duplicate_samples(tmp_path):
    instance = monitor(tmp_path, [sample(0, GIB), sample(HOUR, GIB)])
    instance._database_storage_growth_alerts(footprint(GIB), now=HOUR + 60)
    assert len(instance._state["database_storage_samples"]) == 2


def test_short_spike_with_headroom_still_exceeds_critical_growth_budget(tmp_path):
    instance = monitor(tmp_path, [sample(0, GIB), sample(23 * HOUR, GIB)])
    instance._state["storage_disk_capacity"] = {
        "at": 24 * HOUR,
        "available_bytes": 26 * GIB,
        "used_fraction": 0.56,
    }
    alerts = instance._database_storage_growth_alerts(
        footprint(GIB + 100 * MIB), now=24 * HOUR
    )
    assert alerts[0].severity == "critical"
    assert alerts[0].details["estimated_days_to_exhaustion"] > 7
    assert alerts[0].details["growth_classification"] == "recent_acceleration"
    assert any(
        "若近期数据库增速持续" in line
        for line in _format_alert_human_details(alerts[0].name, alerts[0].details)
    )


def test_historical_over_budget_is_not_mislabeled_as_recent_acceleration(tmp_path):
    instance = monitor(
        tmp_path,
        [sample(0, GIB), sample(23 * HOUR, GIB + 575 * MIB)],
    )

    alerts = instance._database_storage_growth_alerts(
        footprint(GIB + 599 * MIB), now=24 * HOUR
    )

    assert len(alerts) == 1
    assert alerts[0].details["database_growth_bytes_per_day"] < alerts[0].details[
        "historical_database_bytes_per_day"
    ]
    assert alerts[0].details["growth_classification"] == "sustained"


def test_active_growth_uses_lower_recovery_threshold_and_stable_period(
    tmp_path, monkeypatch
):
    delivered = []
    monkeypatch.setattr(
        "deploy.ops.cml_ops_monitor._deliver_notification",
        lambda _webhook, _sendkey, payload: delivered.append(dict(payload)),
    )
    instance = monitor(
        tmp_path,
        [
            sample(at, GIB + int(10 * MIB * at / HOUR))
            for at in range(0, HOUR + 1, 5 * 60)
        ],
        recovery_seconds=30 * 60,
    )
    instance._config = MonitorConfig(
        state_path=tmp_path / "monitor.json",
        consecutive_alerts_required=1,
        consecutive_resolutions_required=2,
        storage_growth_minimum_window_seconds=HOUR,
        storage_growth_recovery_seconds=30 * 60,
    )
    instance._emit(
        Alert("database_storage_growth", "warning", "growth", {"rate": 600}),
        now=100,
    )

    assert (
        instance._database_storage_growth_alerts(
            footprint(GIB + 10 * MIB), now=HOUR
        )
        == ()
    )
    assert instance._state["database_storage_growth_resolution_hold"] is True
    instance._emit_resolutions(set(), now=HOUR)
    assert len(delivered) == 1

    assert (
        instance._database_storage_growth_alerts(
            footprint(GIB + 10 * MIB), now=HOUR + 30 * 60
        )
        == ()
    )
    assert instance._state["database_storage_growth_resolution_hold"] is False
    instance._emit_resolutions(set(), now=HOUR + 30 * 60)
    assert len(delivered) == 1
    assert (
        instance._database_storage_growth_alerts(
            footprint(GIB + 10 * MIB), now=HOUR + 30 * 60 + 60
        )
        == ()
    )
    instance._emit_resolutions(set(), now=HOUR + 30 * 60 + 60)
    assert delivered[-1]["event"] == "ops_alert_resolved"


def test_capacity_exhaustion_within_week_escalates_short_growth(tmp_path):
    instance = monitor(tmp_path, [sample(0, GIB), sample(23 * HOUR, GIB)])
    instance._state["storage_disk_capacity"] = {
        "at": 24 * HOUR,
        "available_bytes": 2 * GIB,
        "used_fraction": 0.56,
    }
    alerts = instance._database_storage_growth_alerts(
        footprint(GIB + 100 * MIB), now=24 * HOUR
    )
    assert alerts[0].severity == "critical"


def test_complete_day_sustained_growth_remains_critical(tmp_path):
    instance = monitor(tmp_path, [sample(0, GIB), sample(23 * HOUR, 3 * GIB)])
    instance._state["storage_disk_capacity"] = {
        "at": 24 * HOUR,
        "available_bytes": 26 * GIB,
        "used_fraction": 0.56,
    }
    alerts = instance._database_storage_growth_alerts(
        footprint(3 * GIB + 100 * MIB), now=24 * HOUR
    )
    assert alerts[0].severity == "critical"
    assert alerts[0].details["growth_classification"] == "sustained"


def test_stale_capacity_does_not_downgrade_growth(tmp_path):
    instance = monitor(tmp_path, [sample(0, GIB), sample(23 * HOUR, GIB)])
    instance._state["storage_disk_capacity"] = {
        "at": 0,
        "available_bytes": 26 * GIB,
        "used_fraction": 0.56,
    }
    alerts = instance._database_storage_growth_alerts(
        footprint(GIB + 100 * MIB), now=24 * HOUR
    )
    assert alerts[0].severity == "critical"


def test_capacity_sample_uses_monitor_cycle_time_despite_slow_checks(
    tmp_path, monkeypatch
):
    from deploy.ops.cml_ops_monitor import DiskUsage

    instance = monitor(tmp_path, [sample(0, GIB), sample(23 * HOUR, GIB)])
    instance._config = MonitorConfig(
        state_path=tmp_path / "monitor.json",
        storage_path="/",
        storage_growth_minimum_window_seconds=HOUR,
    )
    instance._clock = lambda: 24 * HOUR + 20
    monkeypatch.setattr(
        "deploy.ops.cml_ops_monitor.read_disk_usage",
        lambda *args, **kwargs: DiskUsage(
            "/", "disk", "/", 59 * GIB, 32 * GIB, 26 * GIB
        ),
    )
    instance._disk_usage_alerts(now=24 * HOUR)
    alerts = instance._database_storage_growth_alerts(
        footprint(GIB + 100 * MIB), now=24 * HOUR
    )
    assert alerts[0].severity == "critical"
    assert alerts[0].details["available_bytes"] == 26 * GIB


def test_storage_severity_downgrade_updates_notification_once_inside_cooldown(
    tmp_path, monkeypatch
):
    from deploy.ops.cml_ops_monitor import Alert

    delivered = []
    monkeypatch.setattr(
        "deploy.ops.cml_ops_monitor._deliver_notification",
        lambda _webhook, _sendkey, payload: delivered.append(payload),
    )
    instance = OpsMonitor(
        MonitorConfig(
            state_path=tmp_path / "monitor.json",
            consecutive_alerts_required=1,
            alert_cooldown_seconds=900,
        )
    )
    instance._emit(Alert("database_storage_growth", "critical", "growth"), now=100)
    instance._emit(Alert("database_storage_growth", "warning", "growth"), now=110)
    instance._emit(Alert("database_storage_growth", "warning", "growth"), now=120)
    assert [payload["severity"] for payload in delivered] == ["critical", "warning"]
    assert delivered[-1]["details"]["deescalated_from"] == "critical"


def test_total_growth_reports_contributors_below_individual_table_threshold(tmp_path):
    base = {"strategy_runtime_events": GIB, "runtime_market_states_15s": GIB}
    instance = monitor(
        tmp_path, [{"at": 0, "database_bytes": 3 * GIB, "relations": base}]
    )
    alerts = instance._database_storage_growth_alerts(
        {
            "database_bytes": 3 * GIB + 32 * MIB,
            "relations": {
                "strategy_runtime_events": GIB + 8 * MIB,
                "runtime_market_states_15s": GIB + 8 * MIB,
            },
        },
        now=HOUR,
    )
    assert len(alerts) == 1
    details = alerts[0].details
    assert details["relation_growth_bytes_per_day"] == {}
    assert details["relation_growth_contributors_bytes_per_day"] == {
        "strategy_runtime_events": 192 * MIB,
        "runtime_market_states_15s": 192 * MIB,
    }
    assert details["unattributed_growth_bytes_per_day"] == 384 * MIB
    rendered = "\n".join(
        _format_alert_human_details("database_storage_growth", details)
    )
    assert "strategy_runtime_events" in rendered
    assert "runtime_market_states_15s" in rendered


def test_new_inventory_waits_for_baseline_without_inventing_table_growth(tmp_path):
    instance = monitor(
        tmp_path,
        [{"at": 0, "database_bytes": GIB, "relations": {"decision_traces": MIB}}],
    )
    alerts = instance._database_storage_growth_alerts(
        {
            "database_bytes": GIB + 32 * MIB,
            "relations": {"decision_traces": MIB, "newly_tracked": 500 * MIB},
        },
        now=HOUR,
    )
    details = alerts[0].details
    assert details["unbaselined_relation_count"] == 1
    assert "newly_tracked" not in details["relation_growth_contributors_bytes_per_day"]
    assert details["unattributed_growth_bytes_per_day"] == 768 * MIB
    assert "缺少窗口基线" in "\n".join(
        _format_alert_human_details("database_storage_growth", details)
    )
