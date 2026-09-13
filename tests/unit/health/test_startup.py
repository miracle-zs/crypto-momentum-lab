import pytest

from crypto_momentum_lab.health import StartupPhaseTimer


class RecordingLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, object]]] = []

    def info(self, event: str, **fields: object) -> None:
        self.events.append((event, fields))


def test_startup_phase_timer_logs_phase_and_total_elapsed() -> None:
    logger = RecordingLogger()
    timer = StartupPhaseTimer(
        logger,
        event="service_startup_phase",
        service="example",
    )

    timer.mark("configuration_loaded", symbol_count=3)
    timer.mark("ready")

    assert [event for event, _fields in logger.events] == [
        "service_startup_phase",
        "service_startup_phase",
    ]
    first_fields = logger.events[0][1]
    second_fields = logger.events[1][1]
    assert first_fields["service"] == "example"
    assert first_fields["phase"] == "configuration_loaded"
    assert first_fields["symbol_count"] == 3
    assert second_fields["phase"] == "ready"
    assert first_fields["phase_elapsed_ms"] >= 0
    assert second_fields["phase_elapsed_ms"] >= 0
    assert second_fields["total_elapsed_ms"] >= first_fields["total_elapsed_ms"]


def test_startup_phase_timer_rejects_empty_names() -> None:
    logger = RecordingLogger()

    with pytest.raises(ValueError, match="event"):
        StartupPhaseTimer(logger, event=" ")

    timer = StartupPhaseTimer(logger, event="startup")
    with pytest.raises(ValueError, match="phase"):
        timer.mark(" ")
