"""A committed command must survive another publication winning the local race."""

from contextlib import asynccontextmanager
from dataclasses import replace

import pytest

from crypto_momentum_lab.domain.execution.command_models import DispatchState
from tests.unit.execution.test_terminal_settlement import (
    NOW,
    SCOPE,
    ObservationTransaction,
    reserved_book,
)


@pytest.mark.parametrize("terminal_wins", [False, True])
async def test_committed_command_publishes_without_rejecting_newer_head(terminal_wins):
    book = await reserved_book()
    original = book._execution_unit_of_work
    canon = SCOPE.to_position_key().canonical_id

    class InterleavedCommit:
        @asynccontextmanager
        async def transaction(self, key):
            tx = ObservationTransaction(original.head)
            yield tx
            # The DB commit succeeds, then another committed publication is
            # observed before this call reacquires its short mutation lock.
            original.head = replace_head = replace(
                book_head(tx.head),
                revision=tx.head.revision + 1,
            )
            book._head_revisions[canon] = replace_head.revision
            if terminal_wins:
                book._outbox_by_command_id["exit"] = replace(
                    book.get_outbox("exit"),
                    state=DispatchState.TERMINAL,
                )

    book._execution_unit_of_work = InterleavedCommit()
    result = await book.mark_unknown("exit", "REST timeout", NOW)
    assert result.state == (
        DispatchState.TERMINAL if terminal_wins else DispatchState.UNKNOWN
    )
    assert book._head_revisions[canon] == original.head.revision
    assert not book._persistence_failed


def book_head(head):
    from crypto_momentum_lab.domain.execution.ports import ExecutionHeadSnapshot

    return ExecutionHeadSnapshot(
        head.revision,
        head.stream_id,
        head.stream_epoch,
        head.projection_version,
        head.state_payload,
    )


async def test_committed_evidence_is_not_replayed_after_newer_publication():
    from crypto_momentum_lab.domain.execution.observation_models import Applied
    from tests.unit.execution.test_terminal_settlement import evidence, fill

    book = await reserved_book()
    await book.mark_unknown("exit", "REST timeout", NOW)
    original = book._execution_unit_of_work
    canon = SCOPE.to_position_key().canonical_id
    transactions = []

    class InterleavedCommit:
        @asynccontextmanager
        async def transaction(self, key):
            tx = ObservationTransaction(original.head)
            transactions.append(tx)
            yield tx
            original.head = replace(book_head(tx.head), revision=tx.head.revision + 1)
            book._head_revisions[canon] = original.head.revision

    book._execution_unit_of_work = InterleavedCommit()
    result = await book.observe(evidence("exit-fill", fill=fill("close-trade", "1")))
    assert len(transactions) == 1
    assert isinstance(result, Applied)
    assert book._head_revisions[canon] == original.head.revision
    assert not book._persistence_failed
