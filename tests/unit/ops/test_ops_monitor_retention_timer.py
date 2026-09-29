"""The retention schedule itself must be observed, not only its output.

A timer unit has no last-run result of its own: Result and
ExecMainStartTimestamp live on the service it activates, so the check has to
read both units or it reports "never ran" forever.
"""

from datetime import UTC, datetime, timedelta

import pytest

from deploy.ops.cml_ops_monitor import (
    MonitorConfig,
    OpsMonitor,
    RetentionScheduleState,
    SystemdUnitState,
    _parse_systemd_show,
    _service_unit_for,
    evaluate_retention_timer,
    read_systemd_unit_state,
)

NOW_DT = datetime(2026, 9, 29, 9, 0, tzinfo=UTC)
# What OpsMonitor._clock() returns: epoch seconds, not a datetime.
NOW = NOW_DT.timestamp()
NOW_EPOCH = int(NOW)
TIMER = "cml-archive-trim.timer"
SERVICE = "cml-archive-trim.service"
TIMER_OUTPUT = (
    "ActiveState=active\n"
    "UnitFileState=enabled\n"
    "Result=success\n"
    "ExecMainStatus=0\n"
    f"ExecMainStartTimestamp=@{NOW_EPOCH}\n"
)
SERVICE_OUTPUT = (
    "ActiveState=inactive\n"
    "UnitFileState=static\n"
    "Result=success\n"
    "ExecMainStatus=0\n"
    f"ExecMainStartTimestamp=@{NOW_EPOCH - 3600}\n"
)


class Runner:
    def __init__(
        self,
        outputs: dict[str, str] | None = None,
        *,
        error: Exception | None = None,
    ) -> None:
        self.calls: list[tuple[str, ...]] = []
        self._outputs = outputs or {}
        self._error = error

    def run(self, args, *, timeout_seconds):
        del timeout_seconds
        self.calls.append(tuple(args))
        if self._error is not None:
            raise self._error
        unit = args[2] if len(args) > 2 else ""
        return self._outputs.get(unit, "")


def _timer(**overrides) -> SystemdUnitState:
    values = {
        "unit": TIMER,
        "active_state": "active",
        "unit_file_state": "enabled",
        "result": "success",
        "exec_main_status": None,
        "last_start": None,
    }
    values.update(overrides)
    return SystemdUnitState(**values)


def _service(**overrides) -> SystemdUnitState:
    values = {
        "unit": SERVICE,
        "active_state": "inactive",
        "unit_file_state": "static",
        "result": "success",
        "exec_main_status": 0,
        "last_start": NOW_DT - timedelta(hours=20),
    }
    values.update(overrides)
    return SystemdUnitState(**values)


def _schedule(
    timer: SystemdUnitState | None = None,
    service: SystemdUnitState | None = _service(),
) -> RetentionScheduleState:
    return RetentionScheduleState(timer=timer or _timer(), service=service)


def test_service_unit_is_derived_from_the_timer() -> None:
    assert _service_unit_for(TIMER) == SERVICE
    assert _service_unit_for("some-other-unit") == ""


def test_parse_reads_unix_and_rendered_timestamps() -> None:
    parsed = _parse_systemd_show(TIMER, TIMER_OUTPUT)
    assert parsed.active_state == "active"
    assert parsed.unit_file_state == "enabled"
    assert parsed.result == "success"
    assert parsed.last_start is not None
    assert parsed.last_start.timestamp() == pytest.approx(NOW_EPOCH)

    rendered = _parse_systemd_show(
        SERVICE,
        "ActiveState=failed\nUnitFileState=static\nResult=exit-code\n"
        "ExecMainStatus=1\nExecMainStartTimestamp=Tue 2026-09-29 08:29:11 CST\n",
    )
    assert rendered.active_state == "failed"
    assert rendered.result == "exit-code"
    assert rendered.exec_main_status == 1
    assert rendered.last_start is not None
    assert rendered.last_start.hour == 8


def test_parse_accepts_the_default_rendering_too() -> None:
    parsed = _parse_systemd_show(TIMER, "ExecMainStartTimestamp=Tue 2026-09-29 08:29:11 CST\n")
    assert parsed.last_start is not None
    assert parsed.last_start.hour == 8


def test_parse_tolerates_missing_and_unparsable_fields() -> None:
    parsed = _parse_systemd_show(TIMER, "ActiveState=active\nExecMainStartTimestamp=n/a\n")
    assert parsed.last_start is None
    assert parsed.exec_main_status is None
    assert parsed.result == ""
    assert parsed.unit_file_state == ""


def test_read_returns_none_when_systemd_is_unavailable() -> None:
    assert read_systemd_unit_state(TIMER, runner=Runner(error=FileNotFoundError())) is None
    assert read_systemd_unit_state("", runner=Runner()) is None


def test_read_reports_an_unreadable_unit_as_unknown() -> None:
    state = read_systemd_unit_state(
        TIMER, runner=Runner(error=RuntimeError("unit not found"))
    )
    assert state is not None
    assert state.active_state == "unknown"


def test_read_ignores_output_without_activestate() -> None:
    """A stubbed runner must not be mistaken for a dead schedule."""
    assert read_systemd_unit_state(TIMER, runner=Runner({"": "some noise"})) is None
    assert read_systemd_unit_state(TIMER, runner=Runner()) is None


def test_read_asks_for_unix_timestamps() -> None:
    runner = Runner({TIMER: TIMER_OUTPUT})
    read_systemd_unit_state(TIMER, runner=runner)
    assert runner.calls[0][:3] == ("systemctl", "show", TIMER)
    assert "--timestamp=unix" in runner.calls[0]
    assert "ExecMainStartTimestamp" in runner.calls[0]


def test_healthy_schedule_is_silent() -> None:
    assert evaluate_retention_timer(_schedule(), now=NOW, max_age_seconds=26 * 3600) == ()


def test_unknown_schedule_is_silent() -> None:
    assert evaluate_retention_timer(None, now=NOW, max_age_seconds=26 * 3600) == ()


@pytest.mark.parametrize("active_state", ["inactive", "failed", "activating"])
def test_inactive_timer_alerts(active_state: str) -> None:
    alerts = evaluate_retention_timer(
        _schedule(timer=_timer(active_state=active_state)),
        now=NOW,
        max_age_seconds=26 * 3600,
    )
    assert [alert.name for alert in alerts] == ["retention_timer_inactive"]
    assert alerts[0].severity == "critical"
    assert alerts[0].details["reason"] == active_state


def test_disabled_timer_alerts_even_while_active() -> None:
    alerts = evaluate_retention_timer(
        _schedule(timer=_timer(unit_file_state="disabled")),
        now=NOW,
        max_age_seconds=26 * 3600,
    )
    assert [alert.name for alert in alerts] == ["retention_timer_inactive"]
    assert alerts[0].details["reason"] == "disabled"


def test_failed_service_run_alerts() -> None:
    alerts = evaluate_retention_timer(
        _schedule(service=_service(result="exit-code", exec_main_status=1)),
        now=NOW,
        max_age_seconds=26 * 3600,
    )
    assert [alert.name for alert in alerts] == ["retention_timer_failed"]
    assert alerts[0].details["exec_main_status"] == 1
    assert alerts[0].details["service_unit"] == SERVICE


def test_stale_service_run_alerts() -> None:
    alerts = evaluate_retention_timer(
        _schedule(service=_service(last_start=NOW_DT - timedelta(hours=30))),
        now=NOW,
        max_age_seconds=26 * 3600,
    )
    assert [alert.name for alert in alerts] == ["retention_timer_stale"]
    assert alerts[0].details["age_seconds"] == pytest.approx(30 * 3600)
    assert alerts[0].details["threshold_seconds"] == 26 * 3600


def test_never_run_service_alerts() -> None:
    alerts = evaluate_retention_timer(
        _schedule(service=_service(last_start=None)),
        now=NOW,
        max_age_seconds=26 * 3600,
    )
    assert [alert.name for alert in alerts] == ["retention_timer_stale"]


def test_unreadable_service_alerts_as_stale() -> None:
    alerts = evaluate_retention_timer(
        _schedule(service=None), now=NOW, max_age_seconds=26 * 3600
    )
    assert [alert.name for alert in alerts] == ["retention_timer_stale"]
    assert alerts[0].details["service_unit"] == ""


def test_monitor_reads_the_timer_and_its_service(tmp_path) -> None:
    runner = Runner({TIMER: TIMER_OUTPUT, SERVICE: SERVICE_OUTPUT})
    monitor = OpsMonitor(
        MonitorConfig(state_path=tmp_path / "state.json", retention_timer_unit=TIMER),
        runner=runner,
    )

    schedule = monitor._retention_schedule_state()

    assert schedule is not None
    assert schedule.timer.active_state == "active"
    assert schedule.service is not None and schedule.service.unit == SERVICE
    assert [call[2] for call in runner.calls] == [TIMER, SERVICE]
    assert evaluate_retention_timer(
        schedule,
        now=NOW,
        max_age_seconds=monitor._config.retention_timer_max_age_seconds,
    ) == ()


def test_explicit_service_unit_overrides_the_derived_one(tmp_path) -> None:
    runner = Runner({TIMER: TIMER_OUTPUT, "custom.service": SERVICE_OUTPUT})
    monitor = OpsMonitor(
        MonitorConfig(
            state_path=tmp_path / "state.json",
            retention_timer_unit=TIMER,
            retention_service_unit="custom.service",
        ),
        runner=runner,
    )

    schedule = monitor._retention_schedule_state()

    assert schedule is not None and schedule.service_unit == "custom.service"


def test_empty_unit_disables_the_check(tmp_path) -> None:
    runner = Runner({TIMER: TIMER_OUTPUT})
    monitor = OpsMonitor(
        MonitorConfig(state_path=tmp_path / "state.json", retention_timer_unit=""),
        runner=runner,
    )

    assert monitor._retention_schedule_state() is None
    assert runner.calls == []


def test_evaluate_once_runs_with_the_monitor_clock(tmp_path) -> None:
    """The monitor clock is epoch seconds; a datetime here blinds every check."""
    runner = Runner({TIMER: TIMER_OUTPUT, SERVICE: SERVICE_OUTPUT})
    monitor = OpsMonitor(
        MonitorConfig(state_path=tmp_path / "state.json", retention_timer_unit=TIMER),
        runner=runner,
        clock=lambda: NOW,
    )

    alerts = monitor._evaluate_once()

    assert [alert.name for alert in alerts if alert.name.startswith("retention_timer")] == []


def test_max_age_must_be_positive(tmp_path) -> None:
    with pytest.raises(ValueError, match="retention_timer_max_age_seconds"):
        OpsMonitor(
            MonitorConfig(
                state_path=tmp_path / "state.json",
                retention_timer_max_age_seconds=0.0,
            )
        )
