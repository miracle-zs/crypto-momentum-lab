import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from crypto_momentum_lab.domain.execution.command_models import (
    ExecutionScope,
    OutboxEntry,
)
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.ports import (
    DurableExecutionPositionState,
    ExecutionHeadSnapshot,
)
from crypto_momentum_lab.domain.execution.recovery_models import DurableJournalCut
from crypto_momentum_lab.live_rollout.position_self_healing import (
    auto_heal_unmanaged_position,
)
from tests.unit.execution.test_terminal_settlement import ObservationTransaction
from tests.unit.live_rollout.test_position_repair_use_case import (
    NOW,
    MemoryRepairUow,
    repair_case,
)


@pytest.mark.asyncio
async def test_repair_commit_and_reload_exclude_account_observation():
    request, loaded = repair_case()
    db_lock = asyncio.Lock()
    persisted = asyncio.Event()
    observing = asyncio.Event()
    state = []

    class RepairUow(MemoryRepairUow):
        @asynccontextmanager
        async def transaction(self, key):
            async with db_lock:
                async with super().transaction(key) as tx:
                    yield tx

        async def persist_repair(self, repair):
            receipt = await super().persist_repair(repair)
            head = ExecutionHeadSnapshot(
                receipt.head_revision, "hub", "epoch",
                repair.projection_version, repair.head_payload,
            )
            state.append(DurableExecutionPositionState(
                request.scope,
                DurableJournalCut(scope=request.scope, facts=repair.facts,
                                  revision=repair.revision, as_of=NOW),
                head, ("fill-1",), (), (),
            ))
            persisted.set()
            await observing.wait()
            return receipt

    class ObserveUow:
        @asynccontextmanager
        async def transaction(self, key):
            async with db_lock:
                tx = ObservationTransaction(state[-1].head)
                yield tx

        async def load_head(self, key):
            return state[-1].head

        async def load_position(self, key, **kwargs):
            return state[-1]

    book = ExecutionBook(execution_unit_of_work=ObserveUow())
    book._persistence_failed = False
    book._stream_scopes[request.key.canonical_id] = request.scope
    book._ensure_journal(request.key)
    repair = asyncio.create_task(auto_heal_unmanaged_position(
        request=request, uow=RepairUow(loaded), book=book,
    ))
    await asyncio.wait_for(persisted.wait(), timeout=2)
    evidence = ExecutionEvidence(
        evidence_id="account-fill-during-repair",
        scope=ExecutionScope(environment=request.key.environment,
                             account_label=request.key.account_label,
                             symbol=request.key.symbol,
                             position_side=request.key.position_side),
        observed_at=NOW,
        fill=loaded.account_fills[0],
        stream_id="hub", stream_epoch="epoch", sequence=1,
    )
    async def observe():
        observing.set()
        return await book.observe(evidence)

    results = await asyncio.wait_for(asyncio.gather(
        repair, observe(), return_exceptions=True,
    ), timeout=3)
    assert results[0] is True
    assert not any(isinstance(result, Exception) for result in results), results
    assert (await book.read(evidence.scope)).total_quantity == request.expected_quantity


def test_candidate_publish_preserves_concurrent_recovery_and_stream_updates():
    book = ExecutionBook()
    request, _ = repair_case()
    key = request.key

    # 1. Book has stream s1:e1 and recovery command cmd-1
    scope = ExecutionScope(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        position_side=key.position_side,
    )
    cmd_dummy = SimpleNamespace(position_key=key)
    book._outbox_by_command_id["cmd-1"] = OutboxEntry(
        command_id="cmd-1",
        request_id="req-1",
        scope=scope,
        command=cmd_dummy,
    )
    book.register_active_stream(
        environment=key.environment,
        account_label=key.account_label,
        stream_id="s1",
        stream_epoch="e1",
    )
    book.require_command_recovery("cmd-1")

    # 2. Candidate is created
    candidate = book._staged_copy(key=key)

    # 3. While candidate is running I/O, concurrent operations occur:
    # Concurrent operation advances stream to s2:e2
    book.register_active_stream(
        environment=key.environment,
        account_label=key.account_label,
        stream_id="s2",
        stream_epoch="e2",
    )
    # Concurrent operation requires recovery for a new command cmd-2
    book._outbox_by_command_id["cmd-2"] = OutboxEntry(
        command_id="cmd-2",
        request_id="req-2",
        scope=scope,
        command=cmd_dummy,
    )
    book.require_command_recovery("cmd-2")

    # 4. Candidate resolves cmd-1 in its copy
    candidate._recovery_required_commands.discard("cmd-1")

    # 5. Candidate is published
    book._publish_candidate(candidate, key=key)

    # 6. Verify:
    # - Stream must NOT regress to s1:e1, must remain s2:e2
    assert book.get_active_stream(key.environment, key.account_label) == ("s2", "e2")
    # - cmd-1 was resolved by candidate so discarded
    assert not book.command_requires_recovery("cmd-1")
    # - cmd-2 was added concurrently so MUST BE PRESERVED!
    assert book.command_requires_recovery("cmd-2")


def test_publish_candidate_does_not_overwrite_newer_head_revision():
    book = ExecutionBook()
    request, _ = repair_case()
    key = request.key
    canon = key.canonical_id
    candidate = book._staged_copy(key=key)

    # Candidate has old revision 1
    candidate._head_revisions[canon] = 1
    # Memory book already advanced to revision 2 by concurrent transaction
    book._head_revisions[canon] = 2

    # Attempt to publish candidate
    book._publish_candidate(candidate, key=key)

    # Invariant: Head revision must NOT regress from 2 to 1!
    assert book._head_revisions[canon] == 2


@pytest.mark.asyncio
async def test_action_transaction_blocked_when_head_revision_advances_during_io():
    from decimal import Decimal

    from crypto_momentum_lab.domain.execution.execution_action_models import (
        Blocked,
        ExecutionRequest,
        TradeCommandType,
    )
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFactStreamScope,
        FuturesPositionSide,
    )
    from crypto_momentum_lab.domain.strategy.models import StrategySide

    class ConcurrentAdvanceTx:
        def __init__(self, book_ref, key_ref):
            self._book_ref = book_ref
            self._key_ref = key_ref

        async def load_head(self, key):
            return None

        async def save_reservations(self, reservations, **kwargs):
            pass

        async def persist_head(self, **kwargs):
            # Concurrent transaction advances memory revision to 5 during I/O
            self._book_ref._head_revisions[self._key_ref.canonical_id] = 5
            return 1

        async def upsert_outbox(self, **kwargs):
            pass

    class ConcurrentAdvanceUow:
        def __init__(self, book_ref, key_ref):
            self.tx = ConcurrentAdvanceTx(book_ref, key_ref)

        @asynccontextmanager
        async def transaction(self, key, account_scope=None):
            yield self.tx

    scope = ExecutionScope(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    key = scope.to_position_key()
    canon = key.canonical_id

    book = ExecutionBook()
    book._persistence_failed = False
    uow = ConcurrentAdvanceUow(book, key)
    book._execution_unit_of_work = uow
    stream_scope = AccountFactStreamScope.for_position_key(
        key, stream_id="account_event_hub", stream_epoch="epoch-1"
    )
    book._stream_scopes[canon] = stream_scope
    book._ensure_journal(key)
    book._head_revisions[canon] = 0

    view = await book.read(
        scope, stream_id="account_event_hub", stream_epoch="epoch-1"
    )

    req = ExecutionRequest(
        request_id="entry-race-1",
        scope=scope,
        strategy_name="trend_v1",
        run_id="run-1",
        decision_ref="dec-1",
        expected_view_token=view.projection_version,
        action=TradeCommandType.ENTRY,
        requested_quantity=Decimal("1.0"),
        side=StrategySide.LONG,
    )

    result = await book.act(req)

    # Invariant: Must return Blocked and must not overwrite head revision 5 with 1
    assert isinstance(result, Blocked)
    assert book._persistence_failed is True
    assert book._head_revisions[canon] == 5
