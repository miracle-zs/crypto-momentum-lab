"""Select position facts by scoped durable identities, never global counters."""

from sqlalchemy import Numeric, case, cast, exists, select, tuple_
from sqlalchemy.sql.elements import ColumnElement

from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
    ExecutionTradeIdentityRow,
)
from crypto_momentum_lab.persistence.postgres.position_fact_journal_models import (
    PositionFactJournalEventRow,
)


def relevant_recovery_head() -> ColumnElement[bool]:
    head = ExecutionBookHeadRow
    trade = ExecutionTradeIdentityRow
    event = PositionFactJournalEventRow
    trade_scopes = select(
        trade.environment,
        trade.account_label,
        trade.symbol,
        trade.position_side,
    ).distinct()
    has_trade = tuple_(
        head.environment,
        head.account_label,
        head.symbol,
        head.position_side,
    ).in_(trade_scopes)
    has_checkpoint = (
        head.state_payload["recovery_checkpoint"]["checkpoint_id"]
        .astext.is_not(None)
    )
    has_position = exists(
        select(event.event_record_id).where(
            event.environment == head.environment,
            event.account_label == head.account_label,
            event.symbol == head.symbol,
            event.position_side == head.position_side,
            event.event_kind == "snapshot",
            cast(event.payload["position_amt"].astext, Numeric) != 0,
        )
    )
    # Keep the identity lookup uncorrelated so PostgreSQL builds it once rather
    # than rescanning the trade table for every book head.  CASE also lets the
    # cheap checkpoint and identity checks skip the journal probe for most heads.
    return case(
        (has_checkpoint, True),
        (has_trade, True),
        else_=has_position,
    )
