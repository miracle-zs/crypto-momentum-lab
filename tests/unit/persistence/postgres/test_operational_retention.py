from datetime import UTC, datetime
from unittest.mock import AsyncMock

from crypto_momentum_lab.persistence.postgres.operational_retention import (
    PostgresOperationalRetentionRepository,
    _account_balance_hourly_duplicate_statement,
    _account_snapshot_history_delete_statement,
)


def test_hourly_duplicate_delete_is_bounded_and_index_probe_based() -> None:
    sql = str(_account_balance_hourly_duplicate_statement()).lower()

    assert "row_number() over" not in sql
    assert "exists (" in sql
    assert "date_trunc('hour'" in sql
    assert "order by candidate.observed_at" in sql
    assert "limit :batch_size" in sql


async def test_snapshot_prune_skips_latest_scan_without_eligible_rows() -> None:
    repository = PostgresOperationalRetentionRepository(AsyncMock())
    repository._account_snapshot_has_rows_before = AsyncMock(return_value=False)
    repository._execute_account_snapshot_history_delete = AsyncMock(
        side_effect=AssertionError("delete should be skipped")
    )

    deleted = await repository._prune_account_snapshot_table(
        "account_balance_snapshots",
        ("environment", "account_label", "asset"),
        environment="live",
        account_label="primary",
        before=datetime(2026, 8, 1, tzinfo=UTC),
        batch_size=250,
        max_rows=2_000,
    )

    assert deleted == 0
    repository._account_snapshot_has_rows_before.assert_awaited_once()
    repository._execute_account_snapshot_history_delete.assert_not_awaited()


def test_snapshot_history_delete_is_bounded_and_preserves_latest_rows() -> None:
    sql = str(
        _account_snapshot_history_delete_statement(
            "account_position_snapshots",
            ("environment", "account_label", "symbol", "position_side"),
        )
    ).lower()

    assert "distinct on" not in sql
    assert "exists (" in sql
    assert "newer.symbol = candidate.symbol" in sql
    assert "newer.position_side = candidate.position_side" in sql
    assert "newer.observed_at > candidate.observed_at" in sql
    assert "order by candidate.observed_at" in sql
    assert "limit :batch_size" in sql
