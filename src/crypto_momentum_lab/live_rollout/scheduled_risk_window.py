"""Wall-clock risk controls around a recurring volatility window."""

from dataclasses import dataclass
from datetime import datetime, time
from enum import StrEnum
from zoneinfo import ZoneInfo


class ScheduledRiskWindowPhase(StrEnum):
    PRE_WINDOW = "pre_window"
    FLATTENING = "flattening"
    DEADLINE = "deadline"
    VERIFYING = "verifying"
    REOPENED = "reopened"


@dataclass(frozen=True, slots=True)
class ScheduledRiskWindowConfig:
    """Daily schedule for the live account's pre-volatility controls.

    The defaults intentionally encode the agreed Asia/Shanghai operating
    times.  ``reopen_at`` is set to 10:00 so the account remains closed for
    the morning session after the 08:00 event.
    """

    timezone: str = "Asia/Shanghai"
    entry_stop_at: time = time(7, 45)
    flatten_start_at: time = time(7, 45)
    flatten_deadline_at: time = time(7, 55)
    verify_at: time = time(7, 58)
    reopen_at: time = time(10, 0)
    poll_interval_seconds: float = 1.0
    retry_interval_seconds: float = 2.0
    verify_retry_interval_seconds: float = 5.0

    def __post_init__(self) -> None:
        if not self.timezone.strip():
            raise ValueError("timezone must not be empty")
        try:
            ZoneInfo(self.timezone)
        except Exception as error:
            raise ValueError(f"unknown timezone: {self.timezone}") from error
        if self.entry_stop_at > self.flatten_start_at:
            raise ValueError("entry_stop_at must not be after flatten_start_at")
        if self.flatten_start_at >= self.flatten_deadline_at:
            raise ValueError(
                "flatten_start_at must be before flatten_deadline_at"
            )
        if self.flatten_deadline_at >= self.verify_at:
            raise ValueError("flatten_deadline_at must be before verify_at")
        if self.verify_at >= self.reopen_at:
            raise ValueError("verify_at must be before reopen_at")
        for value, field_name in (
            (self.poll_interval_seconds, "poll_interval_seconds"),
            (self.retry_interval_seconds, "retry_interval_seconds"),
            (self.verify_retry_interval_seconds, "verify_retry_interval_seconds"),
        ):
            if value <= 0:
                raise ValueError(f"{field_name} must be positive")

    @property
    def zone(self) -> ZoneInfo:
        return ZoneInfo(self.timezone)

    def localize(self, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("scheduled risk window time must be timezone-aware")
        return value.astimezone(self.zone)

    def phase(self, value: datetime) -> ScheduledRiskWindowPhase:
        local_value = self.localize(value)
        current_time = local_value.time().replace(tzinfo=None)
        if current_time < self.entry_stop_at:
            return ScheduledRiskWindowPhase.PRE_WINDOW
        if current_time < self.flatten_deadline_at:
            return ScheduledRiskWindowPhase.FLATTENING
        if current_time < self.verify_at:
            return ScheduledRiskWindowPhase.DEADLINE
        if current_time < self.reopen_at:
            return ScheduledRiskWindowPhase.VERIFYING
        return ScheduledRiskWindowPhase.REOPENED


__all__ = [
    "ScheduledRiskWindowConfig",
    "ScheduledRiskWindowPhase",
]
