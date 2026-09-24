"""Unit tests for StreamAvailabilityClock."""

import pytest

from crypto_momentum_lab.health.stream_availability import (
    StreamAvailabilityClock,
    StreamAvailabilityConfig,
    StreamAvailabilityState,
    StreamAvailabilityTimeoutError,
)


class CustomStreamError(RuntimeError):
    pass


def test_initial_state_is_connecting_and_times_out():
    current_time = 1000.0
    config = StreamAvailabilityConfig(
        startup_timeout_seconds=30.0,
        disrupted_timeout_seconds=10.0,
        recovery_timeout_seconds=20.0,
    )
    clock = StreamAvailabilityClock(
        config,
        stream_name="test-stream",
        clock=lambda: current_time,
    )

    assert clock.state == StreamAvailabilityState.CONNECTING
    assert not clock.has_ever_been_ready
    assert clock.remaining_budget() == 30.0

    current_time += 15.0
    clock.check_timeout()
    assert clock.remaining_budget() == 15.0

    current_time += 15.0
    with pytest.raises(StreamAvailabilityTimeoutError, match="startup timeout of 30.0s exceeded"):
        clock.check_timeout()


def test_startup_flapping_still_governed_by_startup_timeout():
    current_time = 1000.0
    config = StreamAvailabilityConfig(startup_timeout_seconds=30.0)
    clock = StreamAvailabilityClock(
        config,
        stream_name="test-stream",
        clock=lambda: current_time,
    )

    current_time += 10.0
    clock.mark_disrupted("tcp connection failed")
    current_time += 5.0
    clock.mark_connecting()
    clock.check_timeout()

    current_time += 15.0
    with pytest.raises(StreamAvailabilityTimeoutError):
        clock.check_timeout()


def test_transition_to_ready_clears_timers_and_idle_does_not_timeout():
    current_time = 1000.0
    config = StreamAvailabilityConfig(
        startup_timeout_seconds=30.0,
        disrupted_timeout_seconds=10.0,
        recovery_timeout_seconds=20.0,
    )
    clock = StreamAvailabilityClock(
        config,
        stream_name="test-stream",
        clock=lambda: current_time,
    )

    current_time += 5.0
    clock.mark_ready()

    assert clock.state == StreamAvailabilityState.READY
    assert clock.has_ever_been_ready
    assert clock.remaining_budget() == float("inf")

    # Idle for 3600 seconds (e.g. quiet market) - must never timeout
    current_time += 3600.0
    clock.check_timeout()
    assert clock.remaining_budget() == float("inf")


def test_disruption_after_ready_starts_timer_at_disconnect_not_process_start():
    current_time = 1000.0
    config = StreamAvailabilityConfig(
        startup_timeout_seconds=30.0,
        disrupted_timeout_seconds=10.0,
        recovery_timeout_seconds=20.0,
    )
    clock = StreamAvailabilityClock(
        config,
        stream_name="test-stream",
        clock=lambda: current_time,
    )

    # Becomes ready after 5 seconds
    current_time += 5.0
    clock.mark_ready()

    # Runs healthy for 5000 seconds
    current_time += 5000.0
    clock.check_timeout()

    # Network drops
    clock.mark_disrupted("connection closed")
    assert clock.state == StreamAvailabilityState.DISRUPTED
    assert clock.remaining_budget() == 10.0  # Fresh 10s budget, NOT 5005s elapsed!

    # 5 seconds of disruption is fine
    current_time += 5.0
    clock.check_timeout()
    assert clock.remaining_budget() == 5.0

    # Reconnects after 6 seconds total disruption
    current_time += 1.0
    clock.mark_connected(needs_recovery=False)
    assert clock.state == StreamAvailabilityState.READY
    assert clock.remaining_budget() == float("inf")


def test_disruption_timeout_triggers_when_budget_exceeded():
    current_time = 1000.0
    config = StreamAvailabilityConfig(
        startup_timeout_seconds=30.0,
        disrupted_timeout_seconds=10.0,
    )
    clock = StreamAvailabilityClock(
        config,
        stream_name="test-stream",
        clock=lambda: current_time,
    )
    clock.mark_ready()

    current_time += 100.0
    clock.mark_disrupted("connection reset")

    current_time += 10.0
    with pytest.raises(CustomStreamError, match="disruption timeout of 10.0s exceeded"):
        clock.check_timeout(error_factory=CustomStreamError)


def test_recovery_flow_and_timeout():
    current_time = 1000.0
    config = StreamAvailabilityConfig(
        startup_timeout_seconds=30.0,
        disrupted_timeout_seconds=10.0,
        recovery_timeout_seconds=20.0,
    )
    clock = StreamAvailabilityClock(
        config,
        stream_name="test-stream",
        clock=lambda: current_time,
    )
    clock.mark_ready()

    # Stream reset or full snapshot required mid-session
    current_time += 100.0
    clock.mark_connected(needs_recovery=True, reason="stream_reset")
    assert clock.state == StreamAvailabilityState.RECOVERING
    assert clock.remaining_budget() == 20.0

    # 10 seconds into recovery, connection briefly flaps for 2 seconds
    current_time += 10.0
    clock.mark_disrupted("transient disconnect")
    assert clock.state == StreamAvailabilityState.DISRUPTED

    current_time += 2.0
    clock.mark_connected(needs_recovery=True, reason="reconnected_still_recovering")
    assert clock.state == StreamAvailabilityState.RECOVERING
    # The recovery clock should preserve the overall recovery start (12s total elapsed, 8s left)
    assert clock.remaining_budget() == 8.0

    # Recovery finishes
    current_time += 5.0
    clock.mark_ready()
    assert clock.state == StreamAvailabilityState.READY
    assert clock.remaining_budget() == float("inf")


def test_recovery_timeout_triggers_if_snapshot_never_arrives():
    current_time = 1000.0
    config = StreamAvailabilityConfig(
        startup_timeout_seconds=30.0,
        disrupted_timeout_seconds=10.0,
        recovery_timeout_seconds=20.0,
    )
    clock = StreamAvailabilityClock(
        config,
        stream_name="test-stream",
        clock=lambda: current_time,
    )
    clock.mark_ready()

    current_time += 100.0
    clock.mark_connected(needs_recovery=True)
    assert clock.state == StreamAvailabilityState.RECOVERING

    current_time += 20.0
    with pytest.raises(StreamAvailabilityTimeoutError, match="recovery timeout of 20.0s exceeded"):
        clock.check_timeout()
