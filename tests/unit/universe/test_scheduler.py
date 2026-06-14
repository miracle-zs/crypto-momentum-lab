from datetime import UTC, datetime

from crypto_momentum_lab.universe.scheduler import next_refresh_at


def test_next_refresh_is_first_configured_minute_after_hour() -> None:
    assert next_refresh_at(
        datetime(2026, 6, 14, 10, 30, tzinfo=UTC),
        activation_minute=1,
    ) == datetime(2026, 6, 14, 11, 1, tzinfo=UTC)


def test_exact_refresh_time_advances_to_next_hour() -> None:
    assert next_refresh_at(
        datetime(2026, 6, 14, 11, 1, tzinfo=UTC),
        activation_minute=1,
    ) == datetime(2026, 6, 14, 12, 1, tzinfo=UTC)
