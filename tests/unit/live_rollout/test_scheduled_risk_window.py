from datetime import UTC, datetime, time

import pytest

from crypto_momentum_lab.live_rollout.scheduled_risk_window import (
    ScheduledRiskWindowConfig,
    ScheduledRiskWindowPhase,
)

pytestmark = pytest.mark.contract


def test_default_schedule_uses_agreed_asia_shanghai_boundaries() -> None:
    schedule = ScheduledRiskWindowConfig()

    assert (
        schedule.phase(datetime(2026, 7, 3, 23, 44, 59, tzinfo=UTC))
        is ScheduledRiskWindowPhase.PRE_WINDOW
    )
    assert (
        schedule.phase(datetime(2026, 7, 3, 23, 45, tzinfo=UTC))
        is ScheduledRiskWindowPhase.FLATTENING
    )
    assert (
        schedule.phase(datetime(2026, 7, 3, 23, 55, tzinfo=UTC))
        is ScheduledRiskWindowPhase.DEADLINE
    )
    assert (
        schedule.phase(datetime(2026, 7, 3, 23, 58, tzinfo=UTC))
        is ScheduledRiskWindowPhase.VERIFYING
    )
    assert (
        schedule.phase(datetime(2026, 7, 4, 0, 59, 59, tzinfo=UTC))
        is ScheduledRiskWindowPhase.VERIFYING
    )
    assert (
        schedule.phase(datetime(2026, 7, 4, 1, 0, tzinfo=UTC))
        is ScheduledRiskWindowPhase.REOPENED
    )


def test_schedule_rejects_non_monotonic_boundaries() -> None:
    with pytest.raises(ValueError, match="flatten_deadline_at"):
        ScheduledRiskWindowConfig(
            flatten_start_at=ScheduledRiskWindowConfig().flatten_deadline_at,
        )


def test_schedule_requires_timezone_aware_input() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        ScheduledRiskWindowConfig().phase(datetime(2026, 7, 3, 23, 45))


def test_resolve_scheduled_risk_window_from_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Default is 09:00
    monkeypatch.delenv("CML_SCHEDULED_REOPEN_AT", raising=False)
    default_config = ScheduledRiskWindowConfig.from_environment()
    assert default_config.reopen_at == time(9, 0)

    # Override with env var
    monkeypatch.setenv("CML_SCHEDULED_REOPEN_AT", "09:30")
    custom_config = ScheduledRiskWindowConfig.from_environment()
    assert custom_config.reopen_at == time(9, 30)

    # Invalid format fails closed with ValueError
    monkeypatch.setenv("CML_SCHEDULED_REOPEN_AT", "invalid")
    with pytest.raises(ValueError, match="Failed to parse CML_SCHEDULED_REOPEN_AT"):
        ScheduledRiskWindowConfig.from_environment()


def test_a5_scheduled_risk_window_boundaries() -> None:
    schedule = ScheduledRiskWindowConfig(
        timezone="Asia/Shanghai",
        entry_stop_at=time(7, 45),
        flatten_start_at=time(7, 50),
        flatten_deadline_at=time(7, 55),
        verify_at=time(7, 58),
        reopen_at=time(9, 0),
    )

    # 1. Pre-window: 07:44:59 local (23:44:59 UTC)
    t_pre = datetime(2026, 7, 3, 23, 44, 59, tzinfo=UTC)
    assert schedule.phase(t_pre) is ScheduledRiskWindowPhase.PRE_WINDOW
    assert schedule.is_entry_allowed(t_pre) is True

    # 2. At entry_stop: 07:45:00 local (23:45:00 UTC) -> ENTRY_BLOCKED
    # (equal boundary uses later phase)
    t_stop = datetime(2026, 7, 3, 23, 45, 0, tzinfo=UTC)
    assert schedule.phase(t_stop) is ScheduledRiskWindowPhase.ENTRY_BLOCKED
    assert schedule.is_entry_allowed(t_stop) is False

    # 3. Inside ENTRY_BLOCKED: 07:46:00 local (23:46:00 UTC)
    t_blocked = datetime(2026, 7, 3, 23, 46, 0, tzinfo=UTC)
    assert schedule.phase(t_blocked) is ScheduledRiskWindowPhase.ENTRY_BLOCKED
    assert schedule.is_entry_allowed(t_blocked) is False

    # 4. Just before flatten_start: 07:49:59 local (23:49:59 UTC)
    t_pre_flatten = datetime(2026, 7, 3, 23, 49, 59, tzinfo=UTC)
    assert schedule.phase(t_pre_flatten) is ScheduledRiskWindowPhase.ENTRY_BLOCKED
    assert schedule.is_entry_allowed(t_pre_flatten) is False

    # 5. At flatten_start: 07:50:00 local (23:50:00 UTC) -> FLATTENING
    t_flatten = datetime(2026, 7, 3, 23, 50, 0, tzinfo=UTC)
    assert schedule.phase(t_flatten) is ScheduledRiskWindowPhase.FLATTENING
    assert schedule.is_entry_allowed(t_flatten) is False

    # 6. Just before deadline: 07:54:59 local (23:54:59 UTC)
    t_pre_deadline = datetime(2026, 7, 3, 23, 54, 59, tzinfo=UTC)
    assert schedule.phase(t_pre_deadline) is ScheduledRiskWindowPhase.FLATTENING
    assert schedule.is_entry_allowed(t_pre_deadline) is False

    # 7. At deadline: 07:55:00 local (23:55:00 UTC) -> DEADLINE
    t_deadline = datetime(2026, 7, 3, 23, 55, 0, tzinfo=UTC)
    assert schedule.phase(t_deadline) is ScheduledRiskWindowPhase.DEADLINE
    assert schedule.is_entry_allowed(t_deadline) is False

    # 8. Just before verify: 07:57:59 local (23:57:59 UTC)
    t_pre_verify = datetime(2026, 7, 3, 23, 57, 59, tzinfo=UTC)
    assert schedule.phase(t_pre_verify) is ScheduledRiskWindowPhase.DEADLINE

    # 9. At verify: 07:58:00 local (23:58:00 UTC) -> VERIFYING
    t_verify = datetime(2026, 7, 3, 23, 58, 0, tzinfo=UTC)
    assert schedule.phase(t_verify) is ScheduledRiskWindowPhase.VERIFYING
    assert schedule.is_entry_allowed(t_verify) is False

    # 10. Just before reopen: 08:59:59 local (00:59:59 UTC)
    t_pre_reopen = datetime(2026, 7, 4, 0, 59, 59, tzinfo=UTC)
    assert schedule.phase(t_pre_reopen) is ScheduledRiskWindowPhase.VERIFYING
    assert schedule.is_entry_allowed(t_pre_reopen) is False

    # 11. At reopen: 09:00:00 local (01:00:00 UTC) -> REOPENED
    t_reopen = datetime(2026, 7, 4, 1, 0, 0, tzinfo=UTC)
    assert schedule.phase(t_reopen) is ScheduledRiskWindowPhase.REOPENED
    assert schedule.is_entry_allowed(t_reopen) is True

    # 12. Default stop == flatten: no ENTRY_BLOCKED gap ("默认无额外等待")
    default_schedule = ScheduledRiskWindowConfig()
    assert default_schedule.entry_stop_at == default_schedule.flatten_start_at
    assert (
        default_schedule.phase(datetime(2026, 7, 3, 23, 45, 0, tzinfo=UTC))
        is ScheduledRiskWindowPhase.FLATTENING
    )
