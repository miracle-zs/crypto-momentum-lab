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
        self._disrupted_since: float | None = None
        self._recovering_since: float | None = None
        self._last_state_change: float = now

    @property
    def state(self) -> StreamAvailabilityState:
        return self._state

    @property
    def has_ever_been_ready(self) -> bool:
        return self._has_ever_been_ready

    @property
    def config(self) -> StreamAvailabilityConfig:
        return self._config

    @property
    def stream_name(self) -> str:
        return self._stream_name

    def elapsed_in_current_state(self) -> float:
        return self._clock() - self._last_state_change

    def mark_connecting(self) -> None:
        """Record attempt to connect."""
        if not self._has_ever_been_ready:
            self._state = StreamAvailabilityState.CONNECTING

    def mark_connected(
        self,
        *,
        needs_recovery: bool = False,
        reason: str | None = None,
    ) -> None:
        """Record that transport connection was established.

        If ``needs_recovery`` is True, transitions to RECOVERING (if previously ready)
        or stays in CONNECTING (if initial startup awaiting initial ready artifact).
        Otherwise transitions directly to READY.
        """
        now = self._clock()
        self._disrupted_since = None
        if not self._has_ever_been_ready:
            if needs_recovery:
                self._state = StreamAvailabilityState.CONNECTING
                self._last_state_change = now
            else:
                self.mark_ready()
        else:
            if needs_recovery:
                self.mark_recovering(reason=reason)
            else:
                self.mark_ready()

    def mark_recovering(self, reason: str | None = None) -> None:
        """Record that stream is connected but resynchronizing state."""
        now = self._clock()
        self._state = StreamAvailabilityState.RECOVERING
        self._disrupted_since = None
        if self._recovering_since is None:
            self._recovering_since = now
        self._last_state_change = now

    def mark_ready(self) -> None:
        """Record that stream is fully synchronized and ready for normal consumption."""
        now = self._clock()
        self._has_ever_been_ready = True
        self._state = StreamAvailabilityState.READY
        self._disrupted_since = None
        self._recovering_since = None
        self._last_state_change = now

    def mark_disrupted(self, reason: str | None = None) -> None:
        """Record transport disconnect or network/protocol error."""
        now = self._clock()
        self._state = StreamAvailabilityState.DISRUPTED
        if self._disrupted_since is None:
            self._disrupted_since = now
        self._last_state_change = now

    def check_timeout(
        self,
        *,
        error_factory: Callable[[str], Exception] | None = None,
        custom_message: str | None = None,
    ) -> None:
        """Raise an exception if the current state has exceeded its timeout budget."""
        now = self._clock()
        if not self._has_ever_been_ready:
            elapsed = now - self._startup_since
            if elapsed >= self._config.startup_timeout_seconds:
                msg = (
                    custom_message
                    or f"{self._stream_name} unavailable beyond timeout "
                    f"(startup timeout of "
                    f"{self._config.startup_timeout_seconds:.1f}s exceeded in "
                    f"{self._state.value})"
                )
                if error_factory:
                    raise error_factory(msg)
                raise StreamAvailabilityTimeoutError(msg)
        else:
            if self._state == StreamAvailabilityState.DISRUPTED:
                assert self._disrupted_since is not None
                elapsed = now - self._disrupted_since
                if elapsed >= self._config.disrupted_timeout_seconds:
                    msg = (
                        custom_message
                        or f"{self._stream_name} unavailable beyond timeout "
                        f"(disruption timeout of "
                        f"{self._config.disrupted_timeout_seconds:.1f}s "
                        "exceeded)"
                    )
                    if error_factory:
                        raise error_factory(msg)
                    raise StreamAvailabilityTimeoutError(msg)
            elif self._state == StreamAvailabilityState.RECOVERING:
                assert self._recovering_since is not None
                elapsed = now - self._recovering_since
                if elapsed >= self._config.recovery_timeout_seconds:
                    msg = (
                        custom_message
                        or f"{self._stream_name} unavailable beyond timeout "
                        f"(recovery timeout of "
                        f"{self._config.recovery_timeout_seconds:.1f}s "
                        "exceeded)"
                    )
                    if error_factory:
                        raise error_factory(msg)
                    raise StreamAvailabilityTimeoutError(msg)

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
            assert self._disrupted_since is not None
            return max(
                0.0,
                self._config.disrupted_timeout_seconds - (now - self._disrupted_since),
            )
        if self._state == StreamAvailabilityState.RECOVERING:
            assert self._recovering_since is not None
            return max(
                0.0,
                self._config.recovery_timeout_seconds - (now - self._recovering_since),
            )
        return float("inf")
