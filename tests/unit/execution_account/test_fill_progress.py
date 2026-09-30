from datetime import UTC, datetime, timedelta

import pytest

from crypto_momentum_lab.domain.account.models import AccountFillReconciliationCursor
from crypto_momentum_lab.execution_account.fill_progress import (
    FillCursor,
    merge_fill_cursor,
    plan_fill_polling_ranges,
    select_fill_reconciliation_symbols,
)


@pytest.mark.parametrize(
    "current,from_id,start_time_ms,expected",
    [
        (None, 20, None, FillCursor(from_id=20)),
        (FillCursor(from_id=30), 20, None, FillCursor(from_id=30)),
        (FillCursor(from_id=30), 40, None, FillCursor(from_id=40)),
        (FillCursor(start_time_ms=100), 20, None, FillCursor(from_id=20)),
        (None, None, 100, FillCursor(start_time_ms=100)),
        (FillCursor(from_id=30), None, 200, FillCursor(from_id=30)),
        (FillCursor(start_time_ms=100), None, 80, FillCursor(start_time_ms=100)),
        (FillCursor(start_time_ms=100), None, 120, FillCursor(start_time_ms=120)),
    ],
)
def test_merge_preserves_monotonic_progress_and_id_precedence(
    current, from_id, start_time_ms, expected
):
    cursor = AccountFillReconciliationCursor(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        from_id=from_id,
        start_time_ms=start_time_ms,
        last_checked_at=datetime(2026, 10, 1, tzinfo=UTC),
    )
    assert merge_fill_cursor(current, cursor) == expected


@pytest.mark.parametrize(
    "previous,tracked,newly_active,expected",
    [
        ({}, ("BTCUSDT",), set(), ({}, {"BTCUSDT": 0})),
        ({}, ("BTCUSDT",), {"BTCUSDT"}, ({}, {"BTCUSDT": 603000000})),
        (
            {"BTCUSDT": FillCursor(from_id=42)},
            ("BTCUSDT",),
            {"BTCUSDT"},
            ({"BTCUSDT": 42}, {}),
        ),
        (
            {"BTCUSDT": FillCursor(start_time_ms=123)},
            ("BTCUSDT",),
            {"BTCUSDT"},
            ({}, {"BTCUSDT": 123}),
        ),
        (
            {"BTCUSDT": FillCursor(from_id=42, start_time_ms=123)},
            ("BTCUSDT",),
            set(),
            ({"BTCUSDT": 42}, {}),
        ),
        ({"BTCUSDT": FillCursor(from_id=42)}, (), set(), ({}, {})),
    ],
)
def test_polling_ranges_keep_id_precedence_and_existing_windows(
    previous, tracked, newly_active, expected
):
    before = dict(previous)
    assert (
        plan_fill_polling_ranges(
            previous,
            tracked,
            newly_active,
            observed_at=datetime(1970, 1, 8, tzinfo=UTC),
        )
        == expected
    )
    assert previous == before


def test_polling_ranges_keep_untracked_time_entries_for_later_request_filtering():
    previous = {"BTCUSDT": FillCursor(start_time_ms=123)}
    assert plan_fill_polling_ranges(
        previous, (), {"ETHUSDT"}, observed_at=datetime(1970, 1, 8, tzinfo=UTC)
    ) == ({}, {"BTCUSDT": 123, "ETHUSDT": 603000000})


def test_only_new_position_lookback_is_clamped_at_epoch():
    assert plan_fill_polling_ranges(
        {},
        ("BTCUSDT", "ETHUSDT"),
        {"BTCUSDT"},
        observed_at=datetime(1970, 1, 1, tzinfo=UTC),
    ) == ({}, {"BTCUSDT": 0, "ETHUSDT": -604800000})


def test_empty_polling_selection_returns_empty_mappings():
    assert plan_fill_polling_ranges(
        {}, (), set(), observed_at=datetime(1970, 1, 8, tzinfo=UTC)
    ) == ({}, {})


@pytest.mark.parametrize(
    "offset,expected",
    [(-1, ("BTCUSDT",)), (0, ("BTCUSDT",)), (1, ())],
)
def test_historical_schedule_includes_exact_due_cutoff(offset, expected):
    observed = datetime(2026, 10, 1, tzinfo=UTC)
    checked = observed - timedelta(hours=6) + timedelta(microseconds=offset)
    assert (
        select_fill_reconciliation_symbols(
            {"BTCUSDT"},
            {"BTCUSDT": checked},
            active_fill_symbols=set(),
            observed_at=observed,
            historical_interval=timedelta(hours=6),
            historical_batch_size=1,
        )
        == expected
    )


@pytest.mark.parametrize("batch_size", [1, 2])
def test_active_symbols_do_not_consume_historical_batch_limit(batch_size):
    observed = datetime(2026, 10, 1, tzinfo=UTC)
    active = {"A", "B", "C"}
    checked = {symbol: observed for symbol in active}
    assert select_fill_reconciliation_symbols(
        active | {"H1", "H2"},
        checked,
        active_fill_symbols=active,
        observed_at=observed,
        historical_interval=timedelta(hours=6),
        historical_batch_size=batch_size,
    ) == tuple(sorted(active | {"H1", "H2"} if batch_size == 2 else active | {"H1"}))


def test_unchecked_then_oldest_history_uses_symbol_tie_break_without_mutation():
    observed = datetime(2026, 10, 1, tzinfo=UTC)
    tracked = {"A", "Z", "B", "C", "D"}
    active = {"LIVE"}
    checked = {
        "B": observed - timedelta(days=2),
        "C": observed - timedelta(days=2),
        "D": observed - timedelta(days=1),
    }
    before = (set(tracked), set(active), dict(checked))
    assert select_fill_reconciliation_symbols(
        tracked,
        checked,
        active_fill_symbols=active,
        observed_at=observed,
        historical_interval=timedelta(hours=6),
        historical_batch_size=3,
    ) == ("A", "B", "LIVE", "Z")
    assert (tracked, active, checked) == before


def test_empty_historical_schedule_selects_nothing():
    assert (
        select_fill_reconciliation_symbols(
            set(),
            {},
            active_fill_symbols=set(),
            observed_at=datetime(2026, 10, 1, tzinfo=UTC),
            historical_interval=timedelta(hours=6),
            historical_batch_size=1,
        )
        == ()
    )
