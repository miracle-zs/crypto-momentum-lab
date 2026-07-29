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


def test_refresh_interval_can_run_every_fifteen_minutes() -> None:
    assert next_refresh_at(
        datetime(2026, 6, 14, 10, 30, tzinfo=UTC),
        activation_minute=1,
        refresh_interval_minutes=15,
    ) == datetime(2026, 6, 14, 10, 31, tzinfo=UTC)


def test_exact_fifteen_minute_refresh_advances_to_next_slot() -> None:
    assert next_refresh_at(
        datetime(2026, 6, 14, 10, 31, tzinfo=UTC),
        activation_minute=1,
        refresh_interval_minutes=15,
    ) == datetime(2026, 6, 14, 10, 46, tzinfo=UTC)
