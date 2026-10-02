from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.ports import (
    DecisionCommitConflict,
    ExecutionHeadSnapshot,
)
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFacts,
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.position_repair import (
    build_position_repair,
)
from crypto_momentum_lab.domain.execution.position_repair_models import (
    PositionRepairBlocked,
    PositionRepairFacts,
    PositionRepairReceipt,
    PositionRepairRequest,
)
from crypto_momentum_lab.domain.execution.recovery_models import DurableJournalCut
from crypto_momentum_lab.live_rollout.position_self_healing import (
    auto_heal_unmanaged_position,
)

NOW = datetime(2026, 9, 30, tzinfo=UTC)


def repair_case():
    key = PositionKey("live", "account-3", "TESTUSDT", "LONG")
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="hub", stream_epoch="epoch"
    )
    request = PositionRepairRequest(key, "run-3", scope, Decimal("2"), NOW)
    fill = AccountFillEvent(
        environment="live",
        account_label="account-3",
        symbol="TESTUSDT",
        trade_id="fill-1",
        order_id="order-1",
        side="BUY",
        price=Decimal("10"),
        quantity=Decimal("2"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=NOW,
        raw_payload={"positionSide": "LONG"},
    )
    cut = DurableJournalCut(
        scope=scope,
        facts=AccountFacts(position_key=key, stream_scope=scope),
        revision=0,
        as_of=NOW,
    )
    loaded = PositionRepairFacts(cut, None, (fill,), frozenset({"order-1"}))
    return request, loaded


class MemoryRepairUow:
    def __init__(self, loaded, failures=0):
        self.loaded = loaded
        self.failures = failures
        self.loads = 0
        self.commits = 0
        self.rollbacks = 0
        self.persisted = []

    @asynccontextmanager
    async def transaction(self, key):
        try:
            yield self
        except BaseException:
            self.rollbacks += 1
            raise
        else:
            self.commits += 1

    async def load_repair_facts(self, request):
        self.loads += 1
        return self.loaded

    async def persist_repair(self, repair):
        if self.failures:
            self.failures -= 1
            # A competing normal observe moves the durable head before retry.
            self.loaded = replace(
                self.loaded,
                head=ExecutionHeadSnapshot(
                    self.loads,
                    "hub",
                    "epoch",
                    "old-token",
                    {"last_sequence": self.loads},
                ),
            )
            raise DecisionCommitConflict("normal observe advanced revision")
        self.persisted.append(repair)
        return PositionRepairReceipt(
            repair.request.scope,
            repair.expected_head_revision + int(repair.needs_write),
            repair.projection_version,
            repair.needs_write,
        )


def reloaded_view(plan):
    return PositionBook(
        AccountJournal.from_durable_cut(
            DurableJournalCut(
                scope=plan.request.scope,
                facts=plan.facts,
                revision=plan.revision,
                as_of=NOW,
            )
        )
    ).get_view()


@pytest.mark.parametrize(
    "reason",
    [
        "external",
        "no_fills",
        "wrong_quantity",
        "cross_epoch",
        "wrong_scope",
        "conflicting_fill",
    ],
)
async def test_unproven_repair_rolls_back_without_reload(reason):
    request, loaded = repair_case()
    if reason == "external":
        loaded = replace(loaded, owned_order_ids=frozenset())
    if reason == "no_fills":
        loaded = replace(loaded, account_fills=())
    if reason == "wrong_quantity":
        request = replace(request, expected_quantity=Decimal("3"))
    if reason == "cross_epoch":
        loaded = replace(
            loaded, head=ExecutionHeadSnapshot(1, "hub", "old-epoch", "old", {})
        )
    if reason == "wrong_scope":
        loaded = replace(
            loaded,
            cut=replace(
                loaded.cut,
                scope=replace(request.scope, stream_epoch="other"),
                facts=replace(
                    loaded.cut.facts,
                    stream_scope=replace(request.scope, stream_epoch="other"),
                ),
            ),
        )
    if reason == "conflicting_fill":
        loaded = replace(
            loaded,
            account_fills=loaded.account_fills
            + (replace(loaded.account_fills[0], quantity=Decimal("1")),),
        )
    uow = MemoryRepairUow(loaded)
    book = AsyncMock()
    assert not await auto_heal_unmanaged_position(request=request, uow=uow, book=book)
    assert uow.commits == 0 and uow.rollbacks == 1
    assert not uow.persisted
    book.reload_position.assert_not_awaited()


async def test_repair_reloads_only_after_commit_and_is_idempotent():
    request, loaded = repair_case()
    uow = MemoryRepairUow(loaded)
    book = AsyncMock()

    async def reload(key, **kwargs):
        assert uow.commits == 1
        assert kwargs == dict(
            expected_scope=request.scope, expected_quantity=Decimal("2")
        )
        return reloaded_view(uow.persisted[-1])

    book.reload_position.side_effect = reload
    assert await auto_heal_unmanaged_position(request=request, uow=uow, book=book)
    plan = uow.persisted[-1]
    assert plan.new_facts == 1
    repeated = build_position_repair(
        request,
        replace(
            loaded,
            cut=DurableJournalCut(
                scope=request.scope, facts=plan.facts, revision=plan.revision, as_of=NOW
            ),
            head=ExecutionHeadSnapshot(
                1, "hub", "epoch", plan.projection_version, plan.head_payload
            ),
        ),
    )
    assert repeated.new_facts == 0 and not repeated.needs_write


async def test_revision_conflict_reloads_and_recomputes_before_retry():
    request, loaded = repair_case()
    uow = MemoryRepairUow(loaded, failures=2)
    book = AsyncMock()
    book.reload_position.side_effect = lambda *args, **kwargs: reloaded_view(
        uow.persisted[-1]
    )
    assert await auto_heal_unmanaged_position(request=request, uow=uow, book=book)
    assert uow.loads == 3 and uow.rollbacks == 2 and uow.commits == 1
    assert uow.persisted[-1].expected_head_revision == 2
    assert uow.persisted[-1].head_payload["last_sequence"] == 2


async def test_repeated_revision_conflict_is_bounded():
    request, loaded = repair_case()
    uow = MemoryRepairUow(loaded, failures=3)
    book = AsyncMock()
    with pytest.raises(DecisionCommitConflict):
        await auto_heal_unmanaged_position(request=request, uow=uow, book=book)
    assert uow.loads == 3 and uow.rollbacks == 3 and uow.commits == 0
    book.reload_position.assert_not_awaited()


@pytest.mark.parametrize("failure", [None, RuntimeError("bad durable head")])
async def test_post_commit_reload_failure_does_not_report_success(failure):
    request, loaded = repair_case()
    uow = MemoryRepairUow(loaded)
    book = AsyncMock()
    book.reload_position.side_effect = failure
    book.reload_position.return_value = None
    with pytest.raises((PositionRepairBlocked, RuntimeError)):
        await auto_heal_unmanaged_position(request=request, uow=uow, book=book)
    assert uow.commits == 1


@pytest.mark.parametrize("mixed", [False, True])
def test_unowned_current_entry_cannot_borrow_old_strategy_ownership(mixed):
    request, loaded = repair_case()
    external = replace(loaded.account_fills[0], order_id="manual-order")
    if mixed:
        external = replace(external, trade_id="manual-fill", quantity=Decimal("1"))
        request = replace(request, expected_quantity=Decimal("3"))
        loaded = replace(loaded, account_fills=loaded.account_fills + (external,))
    else:
        loaded = replace(loaded, account_fills=(external,))
    with pytest.raises(PositionRepairBlocked, match="not owned"):
        build_position_repair(request, loaded)


@pytest.mark.parametrize(
    "corruption", [None, "facts_hash", "epoch", "reservation", "quantity"]
)
async def test_strict_reload_validates_before_publishing_book(corruption):
    from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
    from crypto_momentum_lab.domain.execution.ports import DurableExecutionPositionState

    request, loaded = repair_case()
    plan = build_position_repair(request, loaded)
    payload = dict(plan.head_payload)
    epoch = "epoch"
    expected_quantity = request.expected_quantity
    if corruption == "facts_hash":
        payload["facts_hash"] = "corrupt"
    if corruption == "epoch":
        epoch = "other"
    if corruption == "reservation":
        payload["active_reservation_ids"] = ["missing-reservation"]
    if corruption == "quantity":
        expected_quantity = Decimal("3")
    state = DurableExecutionPositionState(
        scope=request.scope,
        cut=DurableJournalCut(
            scope=request.scope, facts=plan.facts, revision=plan.revision, as_of=NOW
        ),
        head=ExecutionHeadSnapshot(1, "hub", epoch, plan.projection_version, payload),
        trade_ids=("fill-1",),
        evidence_ids=("durable-evidence",),
        watermarks=(),
    )
    execution_uow = AsyncMock()
    execution_uow.load_positions.return_value = (state,)
    book = ExecutionBook(execution_unit_of_work=execution_uow)
    if corruption:
        with pytest.raises(PositionRepairBlocked):
            await book.reload_position(
                request.key,
                expected_scope=request.scope,
                expected_quantity=expected_quantity,
            )
        assert request.key.canonical_id not in book._books
        assert request.key.canonical_id not in book._head_revisions
        assert not book._seen_evidence_ids
    else:
        view = await book.reload_position(
            request.key,
            expected_scope=request.scope,
            expected_quantity=expected_quantity,
        )
        assert view.total_quantity == Decimal("2")
        assert book._head_revisions[request.key.canonical_id] == 1
        assert book._seen_evidence_ids == {
            f"{request.key.canonical_id}\x1fhub\x1fepoch\x1fdurable-evidence"
        }


async def test_postgres_adapter_uses_normal_trade_fact_and_head_cas_contract():
    from crypto_momentum_lab.persistence.postgres.position_repair import (
        PostgresPositionRepairTransaction,
    )

    request, loaded = repair_case()
    plan = build_position_repair(request, loaded)
    tx = AsyncMock()
    tx.persist_head.return_value = 1
    receipt = await PostgresPositionRepairTransaction(tx).persist_repair(plan)
    assert receipt.changed and receipt.head_revision == 1
    tx.record_trade.assert_awaited_once()
    tx.persist_facts.assert_awaited_once_with(
        scope=request.scope, facts=plan.facts, revision=plan.revision, delta=plan.delta
    )
    head = tx.persist_head.await_args.kwargs
    assert head["expected_revision"] == 0
    assert head["stream_epoch"] == "epoch"
    assert "allow_epoch_adoption" not in head
    tx.reset_mock()
    await PostgresPositionRepairTransaction(tx).persist_repair(
        replace(plan, needs_write=False)
    )
    tx.record_trade.assert_not_awaited()
    tx.persist_facts.assert_not_awaited()
    tx.persist_head.assert_not_awaited()


async def test_repair_adapter_delegates_transaction_and_rollback_to_normal_uow():
    from crypto_momentum_lab.persistence.postgres.position_repair import (
        PostgresPositionRepairUnitOfWork,
    )

    request, _ = repair_case()
    events = []
    transaction = AsyncMock()

    class NormalExecution:
        @asynccontextmanager
        async def transaction(self, key):
            assert key == request.key
            events.append("locked")
            try:
                yield transaction
            except RuntimeError:
                events.append("rollback")
                raise
            else:
                events.append("commit")

    adapter = object.__new__(PostgresPositionRepairUnitOfWork)
    adapter._execution = NormalExecution()
    with pytest.raises(RuntimeError):
        async with adapter.transaction(request.key) as repair_tx:
            assert repair_tx._transaction is transaction
            raise RuntimeError("failed write")
    assert events == ["locked", "rollback"]


async def test_repair_reads_exact_account_run_and_hedge_side():
    from unittest.mock import Mock

    from sqlalchemy.dialects import postgresql

    import crypto_momentum_lab.persistence.postgres.position_repair as adapter

    request, loaded = repair_case()
    other_side = replace(loaded.account_fills[0], raw_payload={"positionSide": "SHORT"})
    unknown_side = replace(loaded.account_fills[0], raw_payload={})
    results = [Mock(), Mock()]
    results[0].all.return_value = ["order-1"]
    from dataclasses import asdict

    from crypto_momentum_lab.persistence.postgres.models import AccountFillEventRow

    results[1].all.return_value = [
        AccountFillEventRow(**asdict(fill))
        for fill in (loaded.account_fills[0], other_side, unknown_side)
    ]
    tx = AsyncMock()
    tx.session.scalars.side_effect = results
    tx.load_head.return_value = None
    tx.load_recovery.return_value = loaded.cut
    actual = await adapter.PostgresPositionRepairTransaction(tx).load_repair_facts(
        request
    )
    assert actual.account_fills == loaded.account_fills
    assert actual.owned_order_ids == frozenset({"order-1"})
    queries = [
        call.args[0].compile(dialect=postgresql.dialect())
        for call in tx.session.scalars.await_args_list
    ]
    assert set(queries[0].params.values()) >= {"run-3", "TESTUSDT", "LONG"}
    assert "reduce_only IS false" in str(queries[0])
    assert set(queries[1].params.values()) == {"live", "account-3", "TESTUSDT"}
    tx.load_head.assert_awaited_once_with(request.key)
    assert tx.load_recovery.await_args.kwargs["scope"] == request.scope


async def test_repair_filters_checkpoint_fills_without_forging_cursor_coverage():
    from dataclasses import asdict
    from datetime import timedelta
    from unittest.mock import Mock

    import crypto_momentum_lab.persistence.postgres.position_repair as adapter
    from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFillLoadProvenance,
        FactCoverageInterval,
        FactCoverageStatus,
    )
    from crypto_momentum_lab.domain.execution.recovery_models import (
        PositionRecoveryCheckpoint,
    )
    from crypto_momentum_lab.persistence.postgres.models import (
        AccountFillEventRow,
        AccountFillReconciliationCursorRow,
    )

    request, loaded = repair_case()
    checkpoint_time = NOW - timedelta(minutes=10)
    fill_time = NOW - timedelta(minutes=5)
    cursor_time = NOW

    from crypto_momentum_lab.domain.account import AccountPositionSnapshot
    from crypto_momentum_lab.domain.execution.snapshot_encoding import (
        stable_snapshot_anchor_id,
    )

    zero_snap_time = checkpoint_time - timedelta(hours=1)
    zero_snapshot = AccountPositionSnapshot(
        environment=request.key.environment,
        account_label=request.key.account_label,
        symbol=request.key.symbol,
        position_side=request.key.position_side.value,
        position_amt=Decimal("0"),
        entry_price=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        mark_price=Decimal("10"),
        notional=Decimal("0"),
        leverage=5,
        margin_type="cross",
        observed_at=zero_snap_time,
        raw_payload={},
    )
    anchor_id = stable_snapshot_anchor_id(zero_snapshot)

    provenance = AccountFillLoadProvenance(
        stream_scope=request.scope,
        load_id="scan-1",
        scan_origin_from_id=None,
        scan_origin_start_time_ms=1000,
        request_from_id=None,
        next_from_id=None,
        page_count=1,
        page_exhausted=True,
        truncated=False,
        checked_through=checkpoint_time,
        observed_at=checkpoint_time,
        source_anchor_id=anchor_id,
        source_anchor_event_cut=zero_snap_time,
        source_anchor_kind="zero_snapshot",
    )
    coverage = FactCoverageInterval(
        start_at=zero_snap_time,
        end_at=checkpoint_time,
        source_cursor="scan-1",
        status=FactCoverageStatus.CONFIRMED,
        stream_scope=request.scope,
        evidence_observed_at=checkpoint_time,
        checkpoint_id="chk-1",
        checkpoint_event_cut=checkpoint_time,
        load_provenance=provenance,
        page_exhausted=True,
        not_truncated=True,
    )
    facts_at_checkpoint = AccountFacts(
        position_key=request.key,
        stream_scope=request.scope,
        coverage=coverage,
        snapshots=(zero_snapshot,),
        fill_load_provenance=provenance,
    )
    checkpoint = PositionRecoveryCheckpoint(
        key=request.key,
        stream_scope=request.scope,
        event_cut=checkpoint_time,
        projection=PositionLedger(request.key).project(facts_at_checkpoint),
        facts_hash=facts_at_checkpoint.compute_facts_hash(),
        source_revision=1,
        coverage=coverage,
        checkpoint_id="chk-1",
    )
    cut_with_checkpoint = DurableJournalCut(
        scope=request.scope,
        facts=replace(facts_at_checkpoint, recovery_checkpoint=checkpoint),
        revision=1,
        as_of=NOW,
        checkpoint=checkpoint,
    )

    new_fill = replace(
        loaded.account_fills[0],
        trade_id="fill-new",
        trade_at=fill_time,
    )

    results = [Mock(), Mock()]
    results[0].all.return_value = ["order-1"]
    # DB query returns new_fill
    results[1].all.return_value = [AccountFillEventRow(**asdict(new_fill))]

    cursor_row = AccountFillReconciliationCursorRow(
        environment=request.key.environment,
        account_label=request.key.account_label,
        symbol=request.key.symbol,
        start_time_ms=1000,
        last_checked_at=cursor_time,
    )

    tx = AsyncMock()
    tx.session.scalars.side_effect = results
    tx.session.scalar.return_value = cursor_row
    tx.load_head.return_value = None
    tx.load_recovery.return_value = cut_with_checkpoint

    actual = await adapter.PostgresPositionRepairTransaction(tx).load_repair_facts(
        request
    )

    # 1. account_fills only has the new fill
    assert actual.account_fills == (new_fill,)
    # A polling timestamp and locally stored fills do not prove that a REST
    # scan exhausted every page between the checkpoint and the new snapshot.
    assert actual.cut.facts.coverage == coverage
    assert actual.cut.facts.fill_load_provenance == provenance
