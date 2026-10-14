"""Read a verified recovery checkpoint without depending on the execution façade."""

from __future__ import annotations

from datetime import UTC, datetime

from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_models import (
    PositionRecoveryCheckpoint,
)


async def load_recovery_checkpoint(
    *,
    books: dict[str, PositionBook],
    unit_of_work: object | None,
    scope: AccountFactStreamScope,
    as_of: datetime | None = None,
) -> PositionRecoveryCheckpoint | None:
    """Return one exact stream checkpoint, validating its identity."""
    key = PositionKey(
        environment=scope.environment,
        account_label=scope.account_label,
        symbol=scope.symbol,
        position_side=scope.position_side,
    )
    as_of = as_of or datetime.now(UTC)
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("checkpoint read as_of must be timezone-aware")
    if unit_of_work is None:
        book = books.get(key.canonical_id)
        if book is None or book.stream_scope != scope:
            return None
        checkpoint = book.current_facts.recovery_checkpoint
    else:
        state = await unit_of_work.load_position(key, as_of=as_of)
        if state is None or state.scope != scope:
            return None
        checkpoint = state.cut.checkpoint
    if checkpoint is not None and (
        checkpoint.stream_scope != scope
        or checkpoint.key.canonical_id != key.canonical_id
        or checkpoint.event_cut > as_of
    ):
        raise RuntimeError("durable recovery checkpoint identity is invalid")
    return checkpoint
