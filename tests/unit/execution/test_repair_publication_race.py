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
from crypto_momentum_lab.domain.execution.execution_head import (
    decode_execution_head_state,
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
                receipt.head_revision,
                "hub",
                "epoch",
                repair.projection_version,
                repair.head_payload,
            )
            restored = decode_execution_head_state(
                repair.head_payload,
                key=request.key,
                scope=request.scope,
            )
            facts = restored.current_facts
            state.append(
                DurableExecutionPositionState(
                    request.scope,
                    DurableJournalCut(
                        scope=request.scope,
                        facts=facts,
                        revision=receipt.head_revision,
                        as_of=NOW,
                    ),
                    head,
                    tuple(fill.trade_id for fill in repair.facts.fills),
                    (),
                    (),
                )
            )
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
    book._ensure_book(request.key)
    repair = asyncio.create_task(
        auto_heal_unmanaged_position(
            request=request,
            uow=RepairUow(loaded),
            book=book,
        )
    )
    await asyncio.wait_for(persisted.wait(), timeout=2)
    evidence = ExecutionEvidence(
        evidence_id="account-fill-during-repair",
        scope=ExecutionScope(
            environment=request.key.environment,
            account_label=request.key.account_label,
            symbol=request.key.symbol,
            position_side=request.key.position_side,
        ),
        observed_at=NOW,
        fill=loaded.account_fills[0],
        stream_id="hub",
        stream_epoch="epoch",
        sequence=1,
    )

    async def observe():
        observing.set()
        return await book.observe(evidence)

    results = await asyncio.wait_for(
        asyncio.gather(
            repair,
            observe(),
            return_exceptions=True,
        ),
        timeout=3,
    )
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
    book._ensure_book(key)
    book._head_revisions[canon] = 0

    view = await book.read(scope, stream_id="account_event_hub", stream_epoch="epoch-1")

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

    from crypto_momentum_lab.domain.execution.order_submission import (
        OrderProjectionConflictError,
    )

    with pytest.raises(OrderProjectionConflictError):
        await book.act(req)

    assert book._persistence_failed is False
    assert book._head_revisions[canon] == 5
    assert book.get_outbox("entry-race-1") is None


@pytest.mark.asyncio
async def test_action_transaction_preserves_acceptance_on_post_commit_race():
    from decimal import Decimal

    from crypto_momentum_lab.domain.execution.command_models import DispatchState
    from crypto_momentum_lab.domain.execution.execution_action_models import (
        Accepted,
        ExecutionRequest,
        TradeCommandType,
    )
    from crypto_momentum_lab.domain.execution.order_state import (
        FuturesPositionSide,
    )
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFactStreamScope,
    )
    from crypto_momentum_lab.domain.strategy.models import StrategySide

    class PostCommitAdvanceTx:
        def __init__(self, book_ref, key_ref):
            self._book_ref = book_ref
            self._key_ref = key_ref
            self.saved_outbox = []
            self.marked_rejected_orders: list[tuple[str, str]] = []

        async def load_head(self, key):
            from types import SimpleNamespace

            return SimpleNamespace(
                revision=self._book_ref._head_revisions.get(
                    self._key_ref.canonical_id, 0
                ),
                stream_id="account_event_hub",
                stream_epoch="epoch-1",
                projection_version=self._book_ref._ensure_book(self._key_ref)
                .get_view()
                .projection_version,
                state_payload={},
            )

        async def save_reservations(self, reservations, **kwargs):
            pass

        async def persist_head(self, **kwargs):
            return 1

        async def upsert_outbox(self, **kwargs):
            self.saved_outbox.append(kwargs)

        async def mark_order_rejected(
            self,
            client_order_id,
            *,
            reason="",
            occurred_at=None,
            intent_id=None,
        ):
            self.marked_rejected_orders.append((client_order_id, reason))
            return True

    class PostCommitAdvanceUow:
        def __init__(self, book_ref, key_ref):
            self.tx = PostCommitAdvanceTx(book_ref, key_ref)
            self._book_ref = book_ref
            self._key_ref = key_ref

        @asynccontextmanager
        async def transaction(self, key, account_scope=None):
            yield self.tx
            # Advance memory revision 0 -> 5 immediately after tx context commits
            self._book_ref._head_revisions[self._key_ref.canonical_id] = 5

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
    uow = PostCommitAdvanceUow(book, key)
    book._execution_unit_of_work = uow
    stream_scope = AccountFactStreamScope.for_position_key(
        key, stream_id="account_event_hub", stream_epoch="epoch-1"
    )
    book._stream_scopes[canon] = stream_scope
    book._ensure_book(key)
    book._head_revisions[canon] = 0

    view = await book.read(scope, stream_id="account_event_hub", stream_epoch="epoch-1")

    req = ExecutionRequest(
        request_id="entry-race-post-commit",
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

    assert isinstance(result, Accepted)
    assert book._persistence_failed is False
    assert book._head_revisions[canon] == 5
    outbox_entry = book.get_outbox("entry-race-post-commit")
    assert outbox_entry is not None
    assert outbox_entry.state == DispatchState.PREPARED
    assert uow.tx.marked_rejected_orders == []


@pytest.mark.asyncio
async def test_rejection_refused_when_durable_order_is_already_terminal():
    """A refused durable rejection must not publish a rejected command.

    A committed terminal order must not be rewritten by a late rejection.
    Publishing REJECTED locally would contradict that record, release exposure
    that may still be live, and (before this contract) abort the caller with a
    fatal error. The command instead stays terminal-unresolved and is gated
    behind recovery, durably and in memory.
    """
    from decimal import Decimal

    from crypto_momentum_lab.domain.execution.command_models import DispatchState
    from crypto_momentum_lab.domain.execution.order_state import (
        FuturesPositionSide,
    )
    from crypto_momentum_lab.domain.execution.order_submission import (
        OrderRejectionOutcome,
    )
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFactStreamScope,
    )
    from crypto_momentum_lab.domain.execution.trade_command import (
        TradeCommand,
        TradeCommandType,
    )
    from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

    class RefusingTx:
        def __init__(self) -> None:
            self.rejected_orders: list[str] = []
            self.persisted_payload: dict[str, object] | None = None

        async def load_head(self, key):
            return SimpleNamespace(
                revision=0,
                stream_id="account_event_hub",
                stream_epoch="epoch-1",
                projection_version="pv_initial",
                state_payload={},
            )

        async def save_reservations(self, reservations, **kwargs):
            pass

        async def persist_head(self, **kwargs):
            self.persisted_payload = kwargs["state_payload"]
            return 1

        async def upsert_outbox(self, **kwargs):
            pass

        async def mark_order_rejected(
            self, client_order_id, **kwargs
        ) -> OrderRejectionOutcome:
            self.rejected_orders.append(client_order_id)
            # The order row is already terminal: the cascade refuses.
            return OrderRejectionOutcome.TERMINAL

    class RefusingUow:
        def __init__(self) -> None:
            self.tx = RefusingTx()

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
    uow = RefusingUow()
    book._execution_unit_of_work = uow
    book._stream_scopes[canon] = AccountFactStreamScope.for_position_key(
        key, stream_id="account_event_hub", stream_epoch="epoch-1"
    )
    book._ensure_book(key)
    book._head_revisions[canon] = 0
    command = TradeCommand(
        "order-terminal-refusal",
        key,
        TradeCommandType.ENTRY,
        StrategySide.LONG,
        EntryType.MARKET,
        Decimal("1.0"),
        created_at=NOW,
    )
    book.register_prepared_command(command, scope)

    # No exception escapes: the caller keeps its own failure classification.
    entry = await book.mark_rejected(
        command.command_id, reason="pre-submission failure"
    )

    # Nothing is published as rejected; committed order facts win.
    assert uow.tx.rejected_orders == [command.command_id]
    assert entry.state == DispatchState.PREPARED
    outbox = book.get_outbox(command.command_id)
    assert outbox is not None
    assert outbox.state == DispatchState.PREPARED
    # The command is gated behind recovery, in memory and in the durable head.
    assert book.command_requires_recovery(command.command_id)
    assert uow.tx.persisted_payload is not None
    assert command.command_id in uow.tx.persisted_payload["recovery_command_ids"]
    # A later submission attempt for this identity is recovery-blocked.
    assert book.command_requires_recovery(command.command_id)


@pytest.mark.asyncio
async def test_rejection_applies_when_no_durable_order_exists():
    """A prepared-but-never-dispatched command must still be rejectable.

    ``ABSENT`` means no exchange order row exists (queued cancel before the
    POST, or a restored command that was never dispatched). Nothing can be
    contradicted, so the local rejection and its reservation release stand.
    """
    from decimal import Decimal

    from crypto_momentum_lab.domain.execution.command_models import DispatchState
    from crypto_momentum_lab.domain.execution.order_state import (
        FuturesPositionSide,
    )
    from crypto_momentum_lab.domain.execution.order_submission import (
        OrderRejectionOutcome,
    )
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFactStreamScope,
    )
    from crypto_momentum_lab.domain.execution.trade_command import (
        TradeCommand,
        TradeCommandType,
    )
    from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

    class AbsentOrderTx:
        def __init__(self) -> None:
            self.upserted: list[str] = []

        async def load_head(self, key):
            return SimpleNamespace(
                revision=0,
                stream_id="account_event_hub",
                stream_epoch="epoch-1",
                projection_version="pv_initial",
                state_payload={},
            )

        async def save_reservations(self, reservations, **kwargs):
            pass

        async def persist_head(self, **kwargs):
            return 1

        async def upsert_outbox(self, **kwargs):
            self.upserted.append(kwargs["status"])

        async def mark_order_rejected(self, client_order_id, **kwargs):
            return OrderRejectionOutcome.ABSENT

    class AbsentOrderUow:
        def __init__(self) -> None:
            self.tx = AbsentOrderTx()

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
    uow = AbsentOrderUow()
    book._execution_unit_of_work = uow
    book._stream_scopes[canon] = AccountFactStreamScope.for_position_key(
        key, stream_id="account_event_hub", stream_epoch="epoch-1"
    )
    book._ensure_book(key)
    book._head_revisions[canon] = 0
    command = TradeCommand(
        "order-never-sent",
        key,
        TradeCommandType.ENTRY,
        StrategySide.LONG,
        EntryType.MARKET,
        Decimal("1.0"),
        created_at=NOW,
    )
    book.register_prepared_command(command, scope)

    entry = await book.mark_rejected(
        command.command_id, reason="prepared_unsubmitted_before_restart"
    )

    assert entry.state == DispatchState.REJECTED
    assert book.get_outbox(command.command_id).state == DispatchState.REJECTED
    assert book.command_requires_recovery(command.command_id) is False
    assert "rejected" in uow.tx.upserted
