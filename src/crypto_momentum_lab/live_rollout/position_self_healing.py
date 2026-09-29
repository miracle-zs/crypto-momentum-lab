"""Automated self-healing reconciliation for unmanaged positions in live rollout.

From first principles:
When a live trading position appears on the exchange snapshot but has no managed
lot in ExecutionBook / PositionBook:
1. Verify if this position originated from orders submitted by this strategy.
2. If confirmed, query the durable account_fill_events for this symbol & account.
3. If fills exist in account_fill_events that have not yet been reflected in
   position_fact_journal_events / execution_book_heads, automatically ingest
   and project them into durable ledger facts and update the head.
4. Reload the position in ExecutionBook, converting the unmanaged position into
   an actively managed lot, clearing the unmanaged alert and preventing crash halts.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.execution_book import (
    _recovery_checkpoint_head_binding,
    _trade_payload_digest,
    _view_projection_digest,
)
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    FuturesPositionSide,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.persistence.postgres.account_journal_store import (
    PostgresAccountJournalStore,
    _fill_from_row,
    _json_digest,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
    ExecutionTradeIdentityRow,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountFillEventRow,
    ExchangeOrderRow,
    ExecutionCommandRow,
)
from crypto_momentum_lab.persistence.postgres.position_fact_journal_models import (
    PositionFactJournalEventRow,
)

log = structlog.get_logger(__name__)


async def auto_heal_unmanaged_position(
    *,
    session: AsyncSession,
    journal_store: PostgresAccountJournalStore,
    environment: str,
    account_label: str,
    symbol: str,
    position_side: FuturesPositionSide = FuturesPositionSide.LONG,
    active_stream_id: str = "account_event_hub",
    active_stream_epoch: str | None = None,
) -> bool:
    """Attempt self-healing reconciliation for an unmanaged live position.

    Returns True if the position was successfully healed and reconstructed into
    the durable ledger, or False if the position cannot/should not be healed.
    """
    # 1. Author verification: only heal positions initiated by this strategy
    has_commands = await session.scalar(
        select(func.count())
        .select_from(ExecutionCommandRow)
        .where(
            ExecutionCommandRow.details["scope"]["symbol"].astext == symbol,
            ExecutionCommandRow.details["scope"]["account_label"].astext == account_label,
        )
    )
    if not has_commands:
        has_orders = await session.scalar(
            select(func.count())
            .select_from(ExchangeOrderRow)
            .where(
                ExchangeOrderRow.symbol == symbol,
            )
        )
        if not has_orders:
            log.warning(
                "unmanaged_position_external_cannot_auto_heal",
                environment=environment,
                account_label=account_label,
                symbol=symbol,
            )
            return False

    # 2. Determine active stream epoch if not provided
    if not active_stream_epoch:
        active_stream_epoch = await session.scalar(
            select(ExecutionBookHeadRow.stream_epoch)
            .where(
                ExecutionBookHeadRow.environment == environment,
                ExecutionBookHeadRow.account_label == account_label,
            )
            .order_by(ExecutionBookHeadRow.updated_at.desc())
            .limit(1)
        )
        if not active_stream_epoch:
            active_stream_epoch = await session.scalar(
                select(PositionFactJournalEventRow.stream_epoch)
                .where(
                    PositionFactJournalEventRow.environment == environment,
                    PositionFactJournalEventRow.account_label == account_label,
                )
                .order_by(PositionFactJournalEventRow.recorded_at.desc())
                .limit(1)
            )

    if not active_stream_epoch:
        log.warning(
            "unmanaged_position_cannot_resolve_stream_epoch",
            environment=environment,
            account_label=account_label,
            symbol=symbol,
        )
        return False

    # 3. Load fills from account_fill_events
    fill_rows = (
        await session.scalars(
            select(AccountFillEventRow)
            .where(
                AccountFillEventRow.symbol == symbol,
                AccountFillEventRow.account_label == account_label,
            )
            .order_by(AccountFillEventRow.trade_at.asc())
        )
    ).all()
    if not fill_rows:
        log.warning(
            "unmanaged_position_no_fills_in_account_fill_events",
            environment=environment,
            account_label=account_label,
            symbol=symbol,
        )
        return False

    key = PositionKey(
        environment=environment,
        account_label=account_label,
        symbol=symbol,
        position_side=position_side,
    )
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id=active_stream_id, stream_epoch=active_stream_epoch
    )

    fills = [_fill_from_row(r) for r in fill_rows]
    new_facts_count = 0

    for fill in fills:
        # A. Ensure ExecutionTradeIdentityRow exists
        existing_trade = await session.scalar(
            select(ExecutionTradeIdentityRow).where(
                ExecutionTradeIdentityRow.environment == environment,
                ExecutionTradeIdentityRow.account_label == account_label,
                ExecutionTradeIdentityRow.symbol == symbol,
                ExecutionTradeIdentityRow.position_side == position_side.value,
                ExecutionTradeIdentityRow.trade_id == fill.trade_id,
            )
        )
        if existing_trade is None:
            session.add(
                ExecutionTradeIdentityRow(
                    environment=environment,
                    account_label=account_label,
                    symbol=symbol,
                    position_side=position_side.value,
                    trade_id=fill.trade_id,
                    order_id=fill.order_id,
                    quantity=fill.quantity,
                    price=fill.price,
                    side=fill.side,
                    payload_digest=_trade_payload_digest(fill),
                    first_seen_at=fill.trade_at,
                )
            )

        # B. Ensure PositionFactJournalEventRow exists
        existing_ev = await session.scalar(
            select(PositionFactJournalEventRow).where(
                PositionFactJournalEventRow.environment == environment,
                PositionFactJournalEventRow.account_label == account_label,
                PositionFactJournalEventRow.symbol == symbol,
                PositionFactJournalEventRow.position_side == position_side.value,
                PositionFactJournalEventRow.stream_id == active_stream_id,
                PositionFactJournalEventRow.stream_epoch == active_stream_epoch,
                PositionFactJournalEventRow.event_id == str(fill.trade_id),
            )
        )
        if existing_ev is None:
            payload = PositionRecoveryCodec.encode_fill(fill)
            payload_hash = _json_digest(payload)
            event_record_id = _json_digest(
                [scope.canonical_id, "fill", str(fill.trade_id), payload_hash]
            )
            session.add(
                PositionFactJournalEventRow(
                    event_record_id=event_record_id,
                    environment=environment,
                    account_label=account_label,
                    symbol=symbol,
                    position_side=position_side.value,
                    stream_id=active_stream_id,
                    stream_epoch=active_stream_epoch,
                    event_id=str(fill.trade_id),
                    event_kind="fill",
                    occurred_at=fill.trade_at,
                    recorded_at=fill.trade_at,
                    source_revision=0,
                    payload_hash=payload_hash,
                    payload=payload,
                )
            )
            new_facts_count += 1

    # 4. Clear last_error on execution_commands if any were poisoned
    cmd_rows = (
        await session.scalars(
            select(ExecutionCommandRow).where(
                ExecutionCommandRow.details["scope"]["symbol"].astext == symbol,
                ExecutionCommandRow.details["scope"]["account_label"].astext == account_label,
            )
        )
    ).all()
    for cmd in cmd_rows:
        if cmd.details and cmd.details.get("last_error"):
            dtls = dict(cmd.details)
            dtls["last_error"] = None
            cmd.details = dtls

    await session.flush()

    # 5. Reconstruct durable cut and projection view
    now_utc = datetime.now(UTC)
    cut = await journal_store.load_recovery_in_session(
        session, scope=scope, as_of=now_utc
    )
    journal = AccountJournal.from_durable_cut(cut)
    book = PositionBook(journal)
    view = book.get_view()

    if view.total_quantity <= Decimal("0"):
        log.warning(
            "unmanaged_position_auto_heal_projected_zero",
            symbol=symbol,
            account_label=account_label,
            total_quantity=str(view.total_quantity),
        )
        return False

    facts = journal.read_cut()
    facts_hash = facts.compute_facts_hash()
    proj = PositionLedger(key).project(facts)
    proj_digest = PositionRecoveryCodec.compute_projection_digest(proj)
    view_digest = _view_projection_digest(view)

    # 6. Update or insert execution_book_heads
    head = await session.scalar(
        select(ExecutionBookHeadRow).where(
            ExecutionBookHeadRow.environment == environment,
            ExecutionBookHeadRow.account_label == account_label,
            ExecutionBookHeadRow.symbol == symbol,
            ExecutionBookHeadRow.position_side == position_side.value,
        )
    )
    prev_payload = (
        dict(head.state_payload)
        if (head is not None and isinstance(head.state_payload, dict))
        else {}
    )
    raw_reservations = prev_payload.get("active_reservation_ids", [])
    active_reservations = (
        [r for r in raw_reservations if isinstance(r, str) and r]
        if isinstance(raw_reservations, list)
        else []
    )
    is_same_epoch = head is not None and head.stream_epoch == active_stream_epoch
    last_seq = prev_payload.get("last_sequence") if is_same_epoch else None
    if not isinstance(last_seq, int) or last_seq < 0:
        last_seq = None

    head_payload: dict[str, object] = {
        "schema_version": 1,
        "position_key": {
            "environment": key.environment,
            "account_label": key.account_label,
            "symbol": key.symbol,
            "position_side": key.position_side.value,
        },
        "stream_scope": {
            "stream_id": active_stream_id,
            "stream_epoch": active_stream_epoch,
        },
        "facts_hash": facts_hash,
        "projection_digest": proj_digest,
        "view_digest": view_digest,
        "recovery_checkpoint": _recovery_checkpoint_head_binding(
            facts.recovery_checkpoint
        ),
        "journal_revision": journal.revision,
        "last_sequence": last_seq,
        "seen_trade_count": len(fills),
        "active_reservation_ids": active_reservations,
    }

    if head is not None:
        head.stream_id = active_stream_id
        head.stream_epoch = active_stream_epoch
        head.projection_version = view.projection_version
        head.state_payload = head_payload
        head.revision += 1
        head.updated_at = now_utc
    else:
        session.add(
            ExecutionBookHeadRow(
                environment=environment,
                account_label=account_label,
                symbol=symbol,
                position_side=position_side.value,
                stream_id=active_stream_id,
                stream_epoch=active_stream_epoch,
                revision=1,
                projection_version=view.projection_version,
                state_payload=head_payload,
                updated_at=now_utc,
            )
        )

    await session.commit()
    log.info(
        "unmanaged_position_auto_healed_success",
        environment=environment,
        account_label=account_label,
        symbol=symbol,
        new_facts=new_facts_count,
        total_quantity=str(view.total_quantity),
        batches=len(view.batches),
        projection_version=view.projection_version,
    )
    return True
