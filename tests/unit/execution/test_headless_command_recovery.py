"""Legacy journals may predate the first durable execution head."""

from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.command_codec import (
    decode_active_commands,
    encode_outbox_details,
)
from crypto_momentum_lab.domain.execution.command_models import DispatchState
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.execution_book_recovery import (
    restore_durable_positions,
)
from crypto_momentum_lab.domain.execution.ports import DurableExecutionPositionState
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
)
from crypto_momentum_lab.domain.execution.recovery_models import DurableJournalCut
from crypto_momentum_lab.domain.execution.trade_command import (
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide
from tests.unit.execution.test_terminal_settlement import (
    NOW,
    SCOPE,
    ObservationUnitOfWork,
)


def legacy_command():
    seed = ExecutionBook()
    command = TradeCommand("legacy-entry", SCOPE.to_position_key(), TradeCommandType.ENTRY,
                           StrategySide.LONG, EntryType.MARKET, Decimal("1"), created_at=NOW)
    entry = seed.register_prepared_command(command, SCOPE)
    return {
        "command_id": command.command_id, "client_order_id": command.command_id,
        "command": "entry", "status": "dispatching", "requested_at": NOW,
        "details": encode_outbox_details(entry, reservation_ids=(),
                                         cumulative_quantity=Decimal(0), cumulative_quote=Decimal(0)),
    }


class Commands:
    async def load_active_execution_commands(self, **kwargs):
        return (legacy_command(),)


class LegacyUow(ObservationUnitOfWork):
    async def load_positions(self, **kwargs):
        scope = AccountFactStreamScope.for_position_key(SCOPE.to_position_key(), stream_id="hub", stream_epoch="legacy")
        journal = AccountJournal(SCOPE.to_position_key(), stream_scope=scope)
        # Production restores the historical journal even if it has no head.
        return (DurableExecutionPositionState(
            scope, DurableJournalCut(scope=scope, facts=journal.read_cut(), revision=journal.revision,
                                     as_of=NOW), None, (), (), (),
        ),)


@pytest.mark.asyncio
async def test_restore_headless_dispatch_retains_unknown_gate_and_creates_first_head():
    uow = LegacyUow()
    book = ExecutionBook(command_repository=Commands(), execution_unit_of_work=uow)
    await book.restore(account_label="primary", as_of=NOW)
    assert book.get_outbox("legacy-entry").state == DispatchState.UNKNOWN
    assert book.command_requires_recovery("legacy-entry")
    assert uow.head.revision == 1


@pytest.mark.asyncio
async def test_missing_previously_created_head_still_seals_book():
    uow = LegacyUow()
    book = ExecutionBook(command_repository=Commands(), execution_unit_of_work=uow)
    await restore_durable_positions(
        book.recovery_state, watermark_key=book._order_watermark_key,unit_of_work=uow, account_label="primary",
                                         environment="live", as_of=NOW)
    row = legacy_command()
    entry = decode_active_commands((row,), account_label="primary", restored_at=NOW)[0].entry
    book._outbox_by_command_id[entry.command_id] = entry
    book._head_revisions[SCOPE.to_position_key().canonical_id] = 2
    book._persistence_failed = False
    with pytest.raises(RuntimeError, match="durable execution head changed"):
        await book.mark_unknown(entry.command_id, "timeout", NOW)
    assert uow.head is None
