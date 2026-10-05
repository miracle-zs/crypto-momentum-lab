"""Wall-clock risk controls around a recurring volatility window."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, time
from enum import StrEnum
from typing import TypedDict, cast
from zoneinfo import ZoneInfo


class _ScheduledRiskWindowOverrides(TypedDict, total=False):
    timezone: str
    entry_stop_at: time
    flatten_start_at: time
    flatten_deadline_at: time
    verify_at: time
    reopen_at: time


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
    times.  ``reopen_at`` is set to 09:00 so the account remains closed for
    the morning session after the 08:00 event.
    """

    timezone: str = "Asia/Shanghai"
    entry_stop_at: time = time(7, 45)
    flatten_start_at: time = time(7, 45)
    flatten_deadline_at: time = time(7, 55)
    verify_at: time = time(7, 58)
    reopen_at: time = time(9, 0)
    poll_interval_seconds: float = 1.0
    retry_interval_seconds: float = 2.0
    verify_retry_interval_seconds: float = 5.0

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> ScheduledRiskWindowConfig:
        """Resolve the complete schedule at its configuration seam.

        Keeping parsing beside the invariant-bearing value object prevents the
        runtime composition root from knowing individual schedule environment
        variable names or time parsing rules.
        """

        values = os.environ if environment is None else environment
        kwargs: dict[str, object] = {}
        time_env_map = {
            "CML_SCHEDULED_ENTRY_STOP_AT": "entry_stop_at",
            "CML_SCHEDULED_FLATTEN_START_AT": "flatten_start_at",
            "CML_SCHEDULED_FLATTEN_DEADLINE_AT": "flatten_deadline_at",
            "CML_SCHEDULED_VERIFY_AT": "verify_at",
            "CML_SCHEDULED_REOPEN_AT": "reopen_at",
        }
        for env_key, field_name in time_env_map.items():
            value = values.get(env_key, "").strip()
            if value:
                try:
                    parts = [int(part) for part in value.split(":")]
                    if len(parts) == 2:
                        kwargs[field_name] = time(parts[0], parts[1])
                    elif len(parts) == 3:
                        kwargs[field_name] = time(parts[0], parts[1], parts[2])
                    else:
                        raise ValueError(f"Invalid time format: {value}")
                except Exception as exc:
                    raise ValueError(
                        f"Failed to parse {env_key}={value}: expected HH:MM or HH:MM:SS"
                    ) from exc

        timezone = values.get("CML_SCHEDULED_TIMEZONE", "").strip()
        if timezone:
            kwargs["timezone"] = timezone
        return cls(**cast(_ScheduledRiskWindowOverrides, kwargs))

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
            raise ValueError("flatten_start_at must be before flatten_deadline_at")
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

    def is_entry_allowed(self, value: datetime) -> bool:
        """Check whether new order entries are allowed at the given timestamp."""
        local_value = self.localize(value)
        current_time = local_value.time().replace(tzinfo=None)
        if self.entry_stop_at < self.reopen_at:
            return not (self.entry_stop_at <= current_time < self.reopen_at)
        return not (current_time >= self.entry_stop_at or current_time < self.reopen_at)


__all__ = [
    "ScheduledRiskWindowConfig",
    "ScheduledRiskWindowPhase",
]
