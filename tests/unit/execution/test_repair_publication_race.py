import asyncio
from contextlib import asynccontextmanager

import pytest

from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
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
    assert not book._persistence_failed
    assert (await book.read(evidence.scope)).total_quantity == request.expected_quantity
