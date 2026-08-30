from crypto_momentum_lab.persistence.postgres.operational_retention import (
    _account_balance_hourly_duplicate_statement,
)


def test_hourly_duplicate_delete_is_bounded_and_index_probe_based() -> None:
    sql = str(_account_balance_hourly_duplicate_statement()).lower()

    assert "row_number() over" not in sql
    assert "exists (" in sql
    assert "date_trunc('hour'" in sql
    assert "order by candidate.observed_at" in sql
    assert "limit :batch_size" in sql
