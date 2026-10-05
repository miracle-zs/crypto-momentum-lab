from datetime import UTC, datetime, time

import pytest

from crypto_momentum_lab.live_rollout.scheduled_risk_window import (
    ScheduledRiskWindowConfig,
    ScheduledRiskWindowPhase,
)


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
