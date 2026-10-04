"""Stream availability tracking and timeout budgets for real-time WebSocket clients.

Differentiates four distinct stream lifecycle states:
- CONNECTING: Initial startup and connection phase before the stream achieves its
  first ready state. Governed by ``startup_timeout_seconds``.
- READY: Fully synchronized and operational stream. No timeout is counting down.
- RECOVERING: Connected, but resynchronizing application-level state (e.g. awaiting
  a full account snapshot catch-up or rewarming market state). Governed by
  ``recovery_timeout_seconds``.
- DISRUPTED: Transport connection dropped or experienced I/O failure while attempting
  to reconnect. Governed by ``disrupted_timeout_seconds``.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum


class StreamAvailabilityState(StrEnum):
    """The four operational availability states of a client stream."""

    CONNECTING = "connecting"
    RECOVERING = "recovering"
    READY = "ready"
    DISRUPTED = "disrupted"


class StreamAvailabilityTimeoutError(RuntimeError):
    """Raised when a stream availability budget is exceeded."""


@dataclass(frozen=True, slots=True)
class StreamAvailabilityConfig:
    """Timeout budgets in seconds for stream states."""

    startup_timeout_seconds: float = 120.0
    disrupted_timeout_seconds: float = 120.0
    recovery_timeout_seconds: float = 120.0

    def __post_init__(self) -> None:
        if self.startup_timeout_seconds <= 0:
            raise ValueError("startup_timeout_seconds must be positive")
        if self.disrupted_timeout_seconds <= 0:
            raise ValueError("disrupted_timeout_seconds must be positive")
        if self.recovery_timeout_seconds <= 0:
            raise ValueError("recovery_timeout_seconds must be positive")


class StreamAvailabilityClock:
    """
    Tracks availability state and enforces distinct budgets for startup, recovery,
    and disruption.
    """

    def __init__(
        self,
        config: StreamAvailabilityConfig,
        *,
        stream_name: str = "stream",
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._config = config
        self._stream_name = stream_name
        self._clock = clock
        self._state = StreamAvailabilityState.CONNECTING
        self._has_ever_been_ready = False
        now = self._clock()
        self._startup_since: float = now
        self._disrupted_since: float = now
        self._recovering_since: float | None = None

    @property
    def state(self) -> StreamAvailabilityState:
        return self._state

    def mark_connecting(self) -> None:
        """Record attempt to connect."""
        if not self._has_ever_been_ready:
            self._state = StreamAvailabilityState.CONNECTING

    def mark_connected(
        self,
        *,
        needs_recovery: bool = False,
    ) -> None:
        """Record that transport connection was established.

        If ``needs_recovery`` is True, transitions to RECOVERING (if previously ready)
        or stays in CONNECTING (if initial startup awaiting initial ready artifact).
        Otherwise transitions directly to READY.
        """
        if not needs_recovery:
            self.mark_ready()
        elif self._has_ever_been_ready:
            self.mark_recovering()
        else:
            self._state = StreamAvailabilityState.CONNECTING

    def mark_recovering(self) -> None:
        """Record that stream is connected but resynchronizing state."""
        now = self._clock()
        self._state = StreamAvailabilityState.RECOVERING
        if self._recovering_since is None:
            self._recovering_since = now

    def mark_ready(self) -> None:
        """Record that stream is fully synchronized and ready for normal consumption."""
        self._has_ever_been_ready = True
        self._state = StreamAvailabilityState.READY
        self._recovering_since = None

    def mark_disrupted(self) -> None:
        """Record transport disconnect or network/protocol error."""
        if self._state != StreamAvailabilityState.DISRUPTED:
            self._disrupted_since = self._clock()
        self._state = StreamAvailabilityState.DISRUPTED

    def check_timeout(
        self,
        *,
        error_factory: Callable[[str], Exception] | None = None,
        custom_message: str | None = None,
    ) -> None:
        """Raise an exception if the current state has exceeded its timeout budget."""
        if self.remaining_budget() > 0:
            return
        if not self._has_ever_been_ready:
            detail = (
                f"startup timeout of {self._config.startup_timeout_seconds:.1f}s "
                f"exceeded in {self._state.value}"
            )
        elif self._state == StreamAvailabilityState.DISRUPTED:
            detail = (
                f"disruption timeout of {self._config.disrupted_timeout_seconds:.1f}s "
                "exceeded"
            )
        else:
            detail = (
                f"recovery timeout of {self._config.recovery_timeout_seconds:.1f}s "
                "exceeded"
            )
        message = custom_message or (
            f"{self._stream_name} unavailable beyond timeout ({detail})"
        )
        if error_factory is not None:
            raise error_factory(message)
        raise StreamAvailabilityTimeoutError(message)

    def remaining_budget(self) -> float:
        """
        Return the number of seconds remaining before timeout in current state, or
        inf if READY.
        """
        if self._state == StreamAvailabilityState.READY:
            return float("inf")
        now = self._clock()
        if not self._has_ever_been_ready:
            return max(
                0.0, self._config.startup_timeout_seconds - (now - self._startup_since)
            )
        if self._state == StreamAvailabilityState.DISRUPTED:
            return max(
                0.0,
                self._config.disrupted_timeout_seconds - (now - self._disrupted_since),
            )
        if self._state == StreamAvailabilityState.RECOVERING:
            recovering_since = (
                self._recovering_since if self._recovering_since is not None else now
            )
            return max(
                0.0,
                self._config.recovery_timeout_seconds - (now - recovering_since),
            )
        return float("inf")
