from pathlib import Path

from deploy.ops.cml_ops_monitor import (
    MonitorConfig,
    OpsMonitor,
    _alert_conclusion,
    _format_alert_human_details,
)

MIB = 1024**2
GIB = 1024**3
HOUR = 3600


def monitor(tmp_path: Path, samples: list[dict]) -> OpsMonitor:
    instance = OpsMonitor(MonitorConfig(state_path=tmp_path / "monitor.json"))
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
