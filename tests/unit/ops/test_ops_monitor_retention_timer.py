"""The retention schedule itself must be observed, not only its output."""

from datetime import UTC, datetime, timedelta

import pytest

from deploy.ops.cml_ops_monitor import (
    MonitorConfig,
    OpsMonitor,
    SystemdUnitState,
    _parse_systemd_show,
    evaluate_retention_timer,
    read_systemd_unit_state,
)

NOW = datetime(2026, 9, 29, 9, 0, tzinfo=UTC)
UNIT = "cml-archive-trim.timer"
NOW_EPOCH = int(NOW.timestamp())
SHOW_OUTPUT = (
    "ActiveState=active\n"
    "UnitFileState=enabled\n"
    "Result=success\n"
    "ExecMainStatus=0\n"
    f"ExecMainStartTimestamp={NOW_EPOCH}\n"
)


class Runner:
    def __init__(self, *, output: str = "", error: Exception | None = None) -> None:
        self.calls: list[tuple[str, ...]] = []
        self._output = output
        self._error = error

    def run(self, args, *, timeout_seconds):
        del timeout_seconds
        self.calls.append(tuple(args))
        if self._error is not None:
            raise self._error
        return self._output


def _state(**overrides) -> SystemdUnitState:
    values = {
        "unit": UNIT,
        "active_state": "active",
        "unit_file_state": "enabled",
        "result": "success",
        "exec_main_status": 0,
        "last_start": NOW - timedelta(hours=20),
    }
    values.update(overrides)
    return SystemdUnitState(**values)


def test_parse_reads_unix_and_rendered_timestamps() -> None:
    parsed = _parse_systemd_show(UNIT, SHOW_OUTPUT)
    assert parsed.active_state == "active"
    assert parsed.unit_file_state == "enabled"
    assert parsed.result == "success"
    assert parsed.exec_main_status == 0
    assert parsed.last_start is not None
    assert parsed.last_start.timestamp() == pytest.approx(NOW_EPOCH)

    rendered = _parse_systemd_show(
        UNIT,
        "ActiveState=failed\nUnitFileState=enabled\nResult=exit-code\n"
        "ExecMainStatus=1\nExecMainStartTimestamp=Tue 2026-09-29 08:29:11 CST\n",
    )
    assert rendered.active_state == "failed"
    assert rendered.result == "exit-code"
    assert rendered.exec_main_status == 1
    assert rendered.last_start is not None
    assert rendered.last_start.hour == 8


def test_parse_tolerates_missing_and_unparsable_fields() -> None:
    parsed = _parse_systemd_show(UNIT, "ActiveState=active\nExecMainStartTimestamp=n/a\n")
    assert parsed.last_start is None
    assert parsed.exec_main_status is None
    assert parsed.result == ""
    assert parsed.unit_file_state == ""


def test_read_returns_none_when_systemd_is_unavailable() -> None:
    assert read_systemd_unit_state(UNIT, runner=Runner(error=FileNotFoundError())) is None
    assert read_systemd_unit_state("", runner=Runner()) is None


def test_read_reports_an_unreadable_unit_as_unknown() -> None:
    state = read_systemd_unit_state(
        UNIT, runner=Runner(error=RuntimeError("unit not found"))
    )
    assert state is not None
    assert state.active_state == "unknown"


def test_read_asks_for_unix_timestamps() -> None:
    runner = Runner(output=SHOW_OUTPUT)
    read_systemd_unit_state(UNIT, runner=runner)
    assert runner.calls[0][:3] == ("systemctl", "show", UNIT)
    assert "--timestamp=unix" in runner.calls[0]
    assert "ExecMainStartTimestamp" in runner.calls[0]


def test_healthy_schedule_is_silent() -> None:
    assert evaluate_retention_timer(_state(), now=NOW, max_age_seconds=26 * 3600) == ()


def test_unknown_state_is_silent_so_non_systemd_hosts_do_not_alert() -> None:
    assert evaluate_retention_timer(None, now=NOW, max_age_seconds=26 * 3600) == ()


@pytest.mark.parametrize("active_state", ["inactive", "failed", "activating"])
def test_inactive_timer_alerts(active_state: str) -> None:
    alerts = evaluate_retention_timer(
        _state(active_state=active_state), now=NOW, max_age_seconds=26 * 3600
    )
    assert [alert.name for alert in alerts] == ["retention_timer_inactive"]
    assert alerts[0].severity == "critical"
    assert alerts[0].details["reason"] == active_state


def test_disabled_timer_alerts_even_while_active() -> None:
    alerts = evaluate_retention_timer(
        _state(unit_file_state="disabled"), now=NOW, max_age_seconds=26 * 3600
    )
    assert [alert.name for alert in alerts] == ["retention_timer_inactive"]
    assert alerts[0].details["reason"] == "disabled"


def test_failed_last_run_alerts() -> None:
    alerts = evaluate_retention_timer(
        _state(result="exit-code", exec_main_status=1),
        now=NOW,
        max_age_seconds=26 * 3600,
    )
    assert [alert.name for alert in alerts] == ["retention_timer_failed"]
    assert alerts[0].details["exec_main_status"] == 1


def test_stale_schedule_alerts() -> None:
    alerts = evaluate_retention_timer(
        _state(last_start=NOW - timedelta(hours=30)),
        now=NOW,
        max_age_seconds=26 * 3600,
    )
    assert [alert.name for alert in alerts] == ["retention_timer_stale"]
    assert alerts[0].details["age_seconds"] == pytest.approx(30 * 3600)
    assert alerts[0].details["threshold_seconds"] == 26 * 3600


def test_never_run_schedule_alerts() -> None:
    alerts = evaluate_retention_timer(
        _state(last_start=None), now=NOW, max_age_seconds=26 * 3600
    )
    assert [alert.name for alert in alerts] == ["retention_timer_stale"]


def test_monitor_observes_the_configured_unit(tmp_path) -> None:
    runner = Runner(output=SHOW_OUTPUT)
    monitor = OpsMonitor(
        MonitorConfig(state_path=tmp_path / "state.json", retention_timer_unit=UNIT),
        runner=runner,
    )

    state = monitor._retention_timer_state()

    assert state is not None and state.active_state == "active"
    assert runner.calls[0][1:3] == ("show", UNIT)
    assert evaluate_retention_timer(
        state, now=NOW, max_age_seconds=monitor._config.retention_timer_max_age_seconds
    ) == ()


def test_empty_unit_disables_the_check(tmp_path) -> None:
    runner = Runner(output=SHOW_OUTPUT)
    monitor = OpsMonitor(
        MonitorConfig(state_path=tmp_path / "state.json", retention_timer_unit=""),
        runner=runner,
    )

    assert monitor._retention_timer_state() is None
    assert runner.calls == []


def test_max_age_must_be_positive(tmp_path) -> None:
    with pytest.raises(ValueError, match="retention_timer_max_age_seconds"):
        OpsMonitor(
            MonitorConfig(
                state_path=tmp_path / "state.json",
                retention_timer_max_age_seconds=0.0,
            )
        )
