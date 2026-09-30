from datetime import UTC, datetime

import pytest

from crypto_momentum_lab.domain.account.models import AccountFillReconciliationCursor
from crypto_momentum_lab.execution_account.fill_progress import (
    FillCursor,
    merge_fill_cursor,
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
