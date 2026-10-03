"""Select position facts by scoped durable identities, never global counters."""

from sqlalchemy import Numeric, cast, exists, or_, select
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
    has_trade = exists(
        select(trade.trade_id).where(
            trade.environment == head.environment,
            trade.account_label == head.account_label,
            trade.symbol == head.symbol,
            trade.position_side == head.position_side,
        )
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
    return or_(
        has_trade,
        has_position,
        head.state_payload["recovery_checkpoint"]["checkpoint_id"].astext.is_not(None),
    )
