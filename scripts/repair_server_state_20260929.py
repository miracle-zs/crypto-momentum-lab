"""Repair script for live positions state on server (2026-09-29).

Addresses:
1. AKEUSDT:LONG on account-2:
   - Removes stale integrity_issue event and removes integrity_issues from facts_state payloads.
   - Recomputes cut, view, and updates execution_book_heads to READY/CATCHING_UP.
2. NIGHTUSDT:LONG on primary, account-3, account-4:
   - Backfills missing fill events into position_fact_journal_events and execution_trade_identities.
   - Clears last_error on execution_commands and removes conflicting evidence receipts.
   - Recomputes cut, view, and updates execution_book_heads with 3572 units.
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, UTC
from decimal import Decimal

from sqlalchemy import select, text, delete
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from crypto_momentum_lab.domain.account import extract_fill_position_side
from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionKey,
    AccountFactStreamScope,
    PositionHealthStatus,
)
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.domain.execution.execution_book import (
    _view_projection_digest,
    _trade_payload_digest,
)
from crypto_momentum_lab.persistence.postgres.account_journal_store import (
    PostgresAccountJournalStore,
    _json_digest,
    _fill_from_row,
)
from crypto_momentum_lab.persistence.postgres.models import AccountFillEventRow
from crypto_momentum_lab.persistence.postgres.position_fact_journal_models import (
    PositionFactJournalEventRow,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
    ExecutionTradeIdentityRow,
    ExecutionEvidenceReceiptRow,
)


async def repair() -> None:
    db_url = os.environ.get("CML_DATABASE_URL")
    if not db_url:
        print("ERROR: CML_DATABASE_URL environment variable is required")
        sys.exit(1)

    engine = create_async_engine(db_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    store = PostgresAccountJournalStore()

    async with session_factory() as session:
        async with session.begin():
            print("==================================================")
            print("1. REPAIRING AKEUSDT:LONG on account-2")
            print("==================================================")
            # Remove integrity_issue event
            res = await session.execute(
                delete(PositionFactJournalEventRow).where(
                    PositionFactJournalEventRow.symbol == "AKEUSDT",
                    PositionFactJournalEventRow.account_label == "account-2",
                    PositionFactJournalEventRow.event_kind == "integrity_issue",
                )
            )
            print(f"Deleted {res.rowcount} integrity_issue events for AKEUSDT on account-2")

            # Update facts_state events
            facts_state_rows = (
                await session.scalars(
                    select(PositionFactJournalEventRow).where(
                        PositionFactJournalEventRow.symbol == "AKEUSDT",
                        PositionFactJournalEventRow.account_label == "account-2",
                        PositionFactJournalEventRow.event_kind == "facts_state",
                    )
                )
            ).all()

            cleaned_fs = 0
            for row in facts_state_rows:
                if row.payload and row.payload.get("integrity_issues"):
                    payload = dict(row.payload)
                    payload["integrity_issues"] = []
                    row.payload = payload
                    row.payload_hash = _json_digest(payload)
                    cleaned_fs += 1
            print(f"Cleaned {cleaned_fs} facts_state events for AKEUSDT on account-2")

            ake_key = PositionKey(
                environment="live",
                account_label="account-2",
                symbol="AKEUSDT",
                position_side=extract_fill_position_side({"ps": "LONG"}),
            )
            ake_scope = AccountFactStreamScope.for_position_key(
                ake_key,
                stream_id="account_event_hub",
                stream_epoch="cf2ff26b-be73-467c-a221-342f11d6b7be",
            )
            ake_cut = await store.load_recovery_in_session(
                session, scope=ake_scope, as_of=datetime.now(UTC)
            )
            ake_journal = AccountJournal.from_durable_cut(ake_cut)
            ake_book = PositionBook(ake_journal)
            ake_view = ake_book.get_view()
            print(f"AKEUSDT view: qty={ake_view.total_quantity}, gap={ake_view.reconciliation_gap}, "
                  f"health={ake_view.health_status}, diagnostics={ake_view.diagnostics}")

            ake_facts = ake_journal.read_cut()
            ake_facts_hash = ake_facts.compute_facts_hash()
            ake_proj = PositionLedger(ake_key).project(ake_facts)
            ake_proj_digest = PositionRecoveryCodec.compute_projection_digest(ake_proj)
            ake_view_digest = _view_projection_digest(ake_view)

            ake_head = await session.scalar(
                select(ExecutionBookHeadRow).where(
                    ExecutionBookHeadRow.environment == "live",
                    ExecutionBookHeadRow.account_label == "account-2",
                    ExecutionBookHeadRow.symbol == "AKEUSDT",
                    ExecutionBookHeadRow.position_side == "LONG",
                    ExecutionBookHeadRow.stream_id == "account_event_hub",
                    ExecutionBookHeadRow.stream_epoch == "cf2ff26b-be73-467c-a221-342f11d6b7be",
                )
            )
            if ake_head is not None:
                payload = dict(ake_head.state_payload)
                payload["facts_hash"] = ake_facts_hash
                payload["projection_digest"] = ake_proj_digest
                payload["view_digest"] = ake_view_digest
                payload["journal_revision"] = ake_journal.revision
                ake_head.projection_version = ake_view.projection_version
                ake_head.state_payload = payload
                ake_head.updated_at = datetime.now(UTC)
                print(f"Updated execution_book_heads for AKEUSDT on account-2 (pv={ake_head.projection_version})")

            print("\n==================================================")
            print("2. REPAIRING NIGHTUSDT:LONG on primary, account-3, account-4")
            print("==================================================")
            # Remove conflicting evidence receipts
            conflicting_evidence = [
                "order_7ad9b88b0c1be1b5e9e96574d1301fea9db7ee68dc5978eda731a5957afae69d",
                "order_7bbebffa140b75c0b4769a004d1c46dea23317af8f311a974ad21fea122b3b71",
                "order_9cef6c8441c7ae729d97d676ad234b4e1953ba65e98f0099fe43a6b5bb6fb1bd",
            ]
            res_ev = await session.execute(
                delete(ExecutionEvidenceReceiptRow).where(
                    ExecutionEvidenceReceiptRow.evidence_id.in_(conflicting_evidence)
                )
            )
            print(f"Deleted {res_ev.rowcount} conflicting evidence receipts")

            # Clear last_error in execution_commands
            res_cmd = await session.execute(
                text("""
                    UPDATE execution_commands
                    SET details = jsonb_set(details, '{last_error}', 'null'::jsonb)
                    WHERE command_id IN (
                        'cml_6418fa92a0132082fdf9700b60aa2479',
                        'cml_36fe7d660d743e6ebe40010a5eea99ce',
                        'cml_c47a75867222c4617b119aa72e417afe'
                    )
                """)
            )
            print(f"Updated {res_cmd.rowcount} execution_commands to clear last_error")

            accounts_config = [
                ("primary", "1523f154-a6e9-47fd-af04-0292918ef2db"),
                ("account-3", "59edaf5f-328f-4199-be3f-f1134cdbe4ea"),
                ("account-4", "2db29803-1ddc-4d2b-9cad-d4c71cfeedcf"),
            ]

            for acc, epoch in accounts_config:
                print(f"\nProcessing {acc} (epoch {epoch})...")
                key = PositionKey(
                    environment="live",
                    account_label=acc,
                    symbol="NIGHTUSDT",
                    position_side=extract_fill_position_side({"ps": "LONG"}),
                )
                scope = AccountFactStreamScope.for_position_key(
                    key, stream_id="account_event_hub", stream_epoch=epoch
                )

                fill_rows = (
                    await session.scalars(
                        select(AccountFillEventRow).where(
                            AccountFillEventRow.symbol == "NIGHTUSDT",
                            AccountFillEventRow.account_label == acc,
                        )
                    )
                ).all()
                fills = [_fill_from_row(r) for r in fill_rows]
                print(f"Found {len(fills)} fills in account_fill_events for {acc}")

                for fill in fills:
                    # 1. execution_trade_identities
                    existing_trade = await session.scalar(
                        select(ExecutionTradeIdentityRow).where(
                            ExecutionTradeIdentityRow.environment == "live",
                            ExecutionTradeIdentityRow.account_label == acc,
                            ExecutionTradeIdentityRow.symbol == "NIGHTUSDT",
                            ExecutionTradeIdentityRow.position_side == "LONG",
                            ExecutionTradeIdentityRow.trade_id == fill.trade_id,
                        )
                    )
                    if existing_trade is None:
                        session.add(
                            ExecutionTradeIdentityRow(
                                environment="live",
                                account_label=acc,
                                symbol="NIGHTUSDT",
                                position_side="LONG",
                                trade_id=fill.trade_id,
                                order_id=fill.order_id,
                                quantity=fill.quantity,
                                price=fill.price,
                                side=fill.side,
                                payload_digest=_trade_payload_digest(fill),
                                first_seen_at=fill.trade_at,
                            )
                        )
                        print(f"Added trade identity {fill.trade_id} ({fill.quantity} @ {fill.price})")

                    # 2. position_fact_journal_events (fill event)
                    existing_ev = await session.scalar(
                        select(PositionFactJournalEventRow).where(
                            PositionFactJournalEventRow.environment == "live",
                            PositionFactJournalEventRow.account_label == acc,
                            PositionFactJournalEventRow.symbol == "NIGHTUSDT",
                            PositionFactJournalEventRow.position_side == "LONG",
                            PositionFactJournalEventRow.stream_id == "account_event_hub",
                            PositionFactJournalEventRow.stream_epoch == epoch,
                            PositionFactJournalEventRow.event_id == fill.trade_id,
                        )
                    )
                    if existing_ev is None:
                        payload = PositionRecoveryCodec.encode_fill(fill)
                        payload_hash = _json_digest(payload)
                        session.add(
                            PositionFactJournalEventRow(
                                environment="live",
                                account_label=acc,
                                symbol="NIGHTUSDT",
                                position_side="LONG",
                                stream_id="account_event_hub",
                                stream_epoch=epoch,
                                event_id=fill.trade_id,
                                event_kind="fill",
                                occurred_at=fill.trade_at,
                                recorded_at=fill.trade_at,
                                source_revision=0,
                                payload_hash=payload_hash,
                                payload=payload,
                            )
                        )
                        print(f"Added journal fill event {fill.trade_id}")

                # Flush inserts to make them visible to load_recovery_in_session
                await session.flush()

                # Reconstruct cut and verify view
                cut = await store.load_recovery_in_session(
                    session, scope=scope, as_of=datetime.now(UTC)
                )
                journal = AccountJournal.from_durable_cut(cut)
                book = PositionBook(journal)
                view = book.get_view()
                print(f"Reconstructed {acc} NIGHTUSDT view:")
                print(f"  qty: {view.total_quantity}, gap: {view.reconciliation_gap}, "
                      f"health: {view.health_status}, ready: {view.is_ready_for_trade}")
                print(f"  batches: {len(view.batches)}, diagnostics: {view.diagnostics}")

                if view.total_quantity != Decimal("3572"):
                    raise ValueError(f"Expected 3572 units for {acc} NIGHTUSDT, got {view.total_quantity}")

                facts = journal.read_cut()
                facts_hash = facts.compute_facts_hash()
                proj = PositionLedger(key).project(facts)
                proj_digest = PositionRecoveryCodec.compute_projection_digest(proj)
                view_digest = _view_projection_digest(view)

                head = await session.scalar(
                    select(ExecutionBookHeadRow).where(
                        ExecutionBookHeadRow.environment == "live",
                        ExecutionBookHeadRow.account_label == acc,
                        ExecutionBookHeadRow.symbol == "NIGHTUSDT",
                        ExecutionBookHeadRow.position_side == "LONG",
                        ExecutionBookHeadRow.stream_id == "account_event_hub",
                        ExecutionBookHeadRow.stream_epoch == epoch,
                    )
                )
                if head is not None:
                    payload = dict(head.state_payload)
                    payload["facts_hash"] = facts_hash
                    payload["projection_digest"] = proj_digest
                    payload["view_digest"] = view_digest
                    payload["journal_revision"] = journal.revision
                    payload["seen_trade_count"] = len(fills)
                    head.projection_version = view.projection_version
                    head.state_payload = payload
                    head.revision += 1
                    head.updated_at = datetime.now(UTC)
                    print(f"Updated execution_book_heads for {acc} NIGHTUSDT (pv={head.projection_version}, rev={head.revision})")
                else:
                    print(f"WARNING: No execution_book_head found for {acc} with epoch {epoch}")

            print("\nDatabase repair completed successfully!")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(repair())
