"""Pure sparse position history retention rules."""

from datetime import datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.account.models import AccountPositionSnapshot

type PositionSignature = tuple[Decimal, Decimal, datetime]

_POSITION_SNAPSHOT_COALESCE = timedelta(seconds=2)


def should_persist_position(
    position: AccountPositionSnapshot,
    previous: PositionSignature | None,
    *,
    observed_at: datetime,
) -> bool:
    """Retain zero transitions and changed or sufficiently old open positions."""
    if position.position_amt == 0:
        return previous is not None and previous[0] != 0
    return not (
        previous is not None
        and previous[0] == position.position_amt
        and previous[1] == position.entry_price
        and observed_at - previous[2] < _POSITION_SNAPSHOT_COALESCE
    )
