"""Keep recovery-head relevance checks efficient on retained journal history."""

from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
)
from crypto_momentum_lab.persistence.postgres.recovery_relevance import (
    relevant_recovery_head,
)


def test_trade_identity_lookup_is_uncorrelated_and_journal_probe_is_last() -> None:
    statement = select(ExecutionBookHeadRow).where(relevant_recovery_head())
    sql = str(statement.compile(dialect=postgresql.dialect()))

    assert "CASE WHEN" in sql
    assert "SELECT DISTINCT execution_trade_identities.environment" in sql
    assert (
        "execution_trade_identities.environment = execution_book_heads.environment"
        not in sql
    )
    assert sql.index("SELECT DISTINCT execution_trade_identities") < sql.index(
        "EXISTS (SELECT position_fact_journal_events"
    )
