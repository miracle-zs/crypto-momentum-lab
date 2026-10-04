"""Missing stream proof must wait without claiming a fact conflict or success."""

from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from crypto_momentum_lab.domain.execution.evidence_grouping import (
    observe_evidence_group,
)
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.observation_models import (
    Applied,
    EvidenceConflict,
    EvidencePendingReason,
    WaitingForEvidence,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
)
from crypto_momentum_lab.domain.execution.snapshot_encoding import (
    stable_snapshot_anchor_id,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
)
from tests.integration.persistence.test_execution_book_epoch_adoption import _coverage
from tests.unit.execution.test_position_batch_consistency import _snapshot
from tests.unit.execution.test_terminal_settlement import (
    NOW,
    SCOPE,
    ObservationUnitOfWork,
    evidence,
    fill,
)
from tests.unit.execution_account.orders.test_coordinator import _plan, _result
from tests.unit.execution_account.orders.test_fill_scan_ingestion import (
    coordinator as scan_coordinator,
)
from tests.unit.execution_account.orders.test_fill_scan_ingestion import (
    scan,
)
from tests.unit.execution_account.orders.test_fill_scan_ingestion import (
    snapshot as scan_snapshot,
)


async def restored_book(*, flat: bool = False) -> ExecutionBook:
    book = ExecutionBook(execution_unit_of_work=ObservationUnitOfWork())
    book._persistence_failed = False
    assert isinstance(
        await book.observe(evidence("opening", fill=fill("opening", "5", entry=True))),
        Applied,
    )
    if flat:
        assert isinstance(
            await book.observe(evidence("closing", fill=fill("closing", "5"))),
            Applied,
        )
    return book


def snapshot(quantity: str):
    return replace(
        _snapshot(quantity, "59", symbol=SCOPE.symbol),
        account_label=SCOPE.account_label,
        observed_at=NOW + timedelta(seconds=2),
    )


@pytest.mark.parametrize("quantity,flat", [("5", False), ("100", False), ("0", True)])
async def test_missing_epoch_proof_waits_and_preserves_committed_book(quantity, flat):
    book = await restored_book(flat=flat)
    before = await book.read(SCOPE, stream_id="hub", stream_epoch="epoch")
    head_revision = book._head_revisions.copy()
    snap = snapshot(quantity)
    result = await book.observe(
        replace(
            evidence("new-epoch-bootstrap", snapshot=snap),
            stream_epoch="next",
            sequence=1,
        )
    )
    assert isinstance(result, WaitingForEvidence), result
    assert result.reason is EvidencePendingReason.STREAM_RECOVERY_PROOF_REQUIRED
    with pytest.raises(ValueError, match="does not match the restored position"):
        await book.read(SCOPE, stream_id="hub", stream_epoch="next")
    after = await book.read(SCOPE, stream_id="hub", stream_epoch="epoch")
    assert after.projection_version == before.projection_version
    assert after.total_quantity == before.total_quantity
    assert after.batches == before.batches
    assert book._head_revisions == head_revision
    assert not book._persistence_failed


async def test_repeated_pending_snapshots_do_not_emit_conflict_errors(monkeypatch):
    book = await restored_book(flat=True)
    logger = SimpleNamespace(error=Mock(), warning=Mock(), info=Mock())
    monkeypatch.setattr(
        "crypto_momentum_lab.execution_account.orders.coordinator.log", logger
    )
    coordinator = OrderExecutionCoordinator(
        backend=SimpleNamespace(),
        account_label="primary",
        environment="live",
        execution_book=book,
    )
    try:
        for sequence in range(1, 4):
            await coordinator.observe_account_snapshot(
                snapshot("0"),
                stream_id="hub",
                stream_epoch="next",
                sequence=sequence,
            )
        logger.error.assert_not_called()
        logger.info.assert_called_once()
        assert logger.info.call_args.args == (
            "account_snapshot_execution_book_waiting_for_evidence",
        )
        assert SCOPE.to_position_key() not in coordinator._confirmed_flat_streams
        assert (await book.read(SCOPE)).total_quantity == Decimal("0")
    finally:
        await coordinator.aclose()


@pytest.mark.parametrize("deferred_index", [0, 1, 2])
async def test_grouped_evidence_preserves_waiting_and_stops_processing(deferred_index):
    source = evidence(
        "group", fills=(fill("first", "1", entry=True), fill("second", "1", entry=True))
    )
    pending = WaitingForEvidence(
        "group" if deferred_index == 2 else "internal",
        EvidencePendingReason.STREAM_RECOVERY_PROOF_REQUIRED,
    )
    observe = AsyncMock(
        side_effect=[Applied("part", "view")] * deferred_index + [pending]
    )
    result = await observe_evidence_group(
        source,
        observe_one=observe,
        forget_identity=Mock(),
    )
    assert isinstance(result, WaitingForEvidence)
    assert result.reason is pending.reason
    assert observe.await_count == deferred_index + 1
    assert result.evidence_id == "group"


async def test_waiting_after_grouped_fill_rolls_back_the_whole_observation():
    class Book(ExecutionBook):
        async def _observe_mutating(self, item):
            if item.evidence_id == "deferred-group" and item.fill is None:
                return WaitingForEvidence(
                    item.evidence_id,
                    EvidencePendingReason.STREAM_RECOVERY_PROOF_REQUIRED,
                )
            return await super()._observe_mutating(item)

    uow = ObservationUnitOfWork()
    book = Book(execution_unit_of_work=uow)
    book._persistence_failed = False
    await book.observe(evidence("opening", fill=fill("opening", "5", entry=True)))
    before = await book.read(SCOPE)
    durable_head = uow.head
    revision = book.context_revision
    result = await book.observe(
        evidence(
            "deferred-group",
            fills=(fill("new-trade", "2", entry=True),),
        )
    )
    assert isinstance(result, WaitingForEvidence)
    assert (await book.read(SCOPE)).projection_version == before.projection_version
    assert (await book.read(SCOPE)).total_quantity == Decimal("5")
    assert book.context_revision == revision
    assert uow.head is durable_head
    assert not book._persistence_failed


async def test_waiting_transition_resolves_and_can_be_reported_again(monkeypatch):
    book = ExecutionBook()
    pending = WaitingForEvidence(
        "pending", EvidencePendingReason.STREAM_RECOVERY_PROOF_REQUIRED
    )
    book.observe = AsyncMock(
        side_effect=[pending, pending, Applied("ready", "view"), pending]
    )
    logger = SimpleNamespace(error=Mock(), warning=Mock(), info=Mock())
    monkeypatch.setattr(
        "crypto_momentum_lab.execution_account.orders.coordinator.log", logger
    )
    coordinator = OrderExecutionCoordinator(
        backend=SimpleNamespace(),
        account_label="primary",
        environment="live",
        execution_book=book,
    )
    try:
        for sequence in range(1, 5):
            await coordinator.observe_account_snapshot(
                snapshot("5"),
                stream_id="hub",
                stream_epoch="next",
                sequence=sequence,
            )
        assert [call.args[0] for call in logger.info.call_args_list] == [
            "account_snapshot_execution_book_waiting_for_evidence",
            "account_snapshot_execution_book_evidence_ready",
            "account_snapshot_execution_book_waiting_for_evidence",
        ]
        logger.error.assert_not_called()
    finally:
        await coordinator.aclose()


async def test_true_conflict_still_reports_error_and_never_confirms_flat(monkeypatch):
    book = ExecutionBook()
    book.observe = AsyncMock(
        return_value=EvidenceConflict("bad", "divergent fact identity")
    )
    logger = SimpleNamespace(error=Mock(), warning=Mock(), info=Mock())
    monkeypatch.setattr(
        "crypto_momentum_lab.execution_account.orders.coordinator.log", logger
    )
    coordinator = OrderExecutionCoordinator(
        backend=SimpleNamespace(),
        account_label="primary",
        environment="live",
        execution_book=book,
    )
    try:
        await coordinator.observe_account_snapshot(
            snapshot("0"),
            stream_id="hub",
            stream_epoch="next",
            sequence=1,
        )
        logger.error.assert_called_once()
        assert logger.error.call_args.args == (
            "account_snapshot_execution_book_conflicts",
        )
        assert logger.error.call_args.kwargs["reasons"] == {
            "divergent fact identity": 1
        }
        assert SCOPE.to_position_key() not in coordinator._confirmed_flat_streams
    finally:
        await coordinator.aclose()


@pytest.mark.parametrize("parent_id", [None, "different-parent"])
async def test_missing_parent_waits_but_mismatched_parent_remains_conflict(
    monkeypatch, parent_id
):
    coordinator, book = scan_coordinator()
    book.drain = AsyncMock()
    book.load_recovery_checkpoint.return_value = (
        None if parent_id is None else SimpleNamespace(checkpoint_id=parent_id)
    )
    source = replace(
        scan(),
        source_anchor_id="expected-parent",
        source_anchor_kind="recovery_checkpoint",
        source_stream_id="hub",
        source_stream_epoch="old",
        source_anchor_snapshot=None,
    )
    logger = SimpleNamespace(error=Mock(), warning=Mock(), info=Mock())
    monkeypatch.setattr(
        "crypto_momentum_lab.execution_account.orders.coordinator.log", logger
    )
    try:
        for sequence in (1, 2):
            await coordinator.observe_account_snapshot(
                scan_snapshot(),
                stream_id="hub",
                stream_epoch="next",
                sequence=sequence,
                fill_load_scans=(source,),
            )
        book.observe.assert_not_awaited()
        assert SCOPE.to_position_key() not in coordinator._confirmed_flat_streams
        if parent_id is None:
            logger.error.assert_not_called()
            logger.info.assert_called_once()
            assert logger.info.call_args.kwargs["reasons"] == {
                EvidencePendingReason.PARENT_CHECKPOINT_UNAVAILABLE.value: 1,
            }
        else:
            assert logger.error.call_count == 2
            assert logger.error.call_args.kwargs["reasons"] == {
                "fill scan parent checkpoint identity mismatch": 1,
            }
    finally:
        await coordinator.aclose()


async def test_waiting_projection_does_not_overwrite_exchange_result():
    book = ExecutionBook()
    book.observe = AsyncMock(
        return_value=WaitingForEvidence(
            "pending",
            EvidencePendingReason.STREAM_RECOVERY_PROOF_REQUIRED,
        )
    )
    plan = _plan("BTCUSDT", reduce_only=False)
    backend = SimpleNamespace(reconcile_order=AsyncMock(return_value=_result(plan)))
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
        environment="live",
        execution_book=book,
    )
    try:
        result = await coordinator.reconcile_order(plan)
        assert result == _result(plan)
        # A zero-fill ACK projects in the background; closing drains accepted facts.
        await coordinator.aclose()
        assert book.command_requires_recovery(plan.client_order_id)
    finally:
        await coordinator.aclose()


@pytest.mark.parametrize("complete", [False, True])
async def test_partial_scan_waits_but_complete_inconsistent_scan_is_a_conflict(
    complete,
):
    book = await restored_book()
    before = await book.read(SCOPE)
    target = snapshot("100")
    scope = AccountFactStreamScope.for_position_key(
        SCOPE.to_position_key(),
        stream_id="hub",
        stream_epoch="next",
    )
    anchor = replace(snapshot("0"), observed_at=NOW)
    proof = _coverage(
        scope,
        load_id="source-scan",
        scan_origin=NOW,
        anchor_id=stable_snapshot_anchor_id(anchor),
        anchor_cut=NOW,
        anchor_kind="zero_snapshot",
        checked_through=target.observed_at,
    )
    provenance = proof.load_provenance
    if not complete:
        provenance = replace(
            provenance,
            page_exhausted=False,
            truncated=True,
            checked_through=None,
        )
        proof = replace(
            proof,
            page_exhausted=False,
            not_truncated=False,
            fill_checked_through=None,
            load_provenance=provenance,
        )
    result = await book.observe(
        replace(
            evidence(
                "scan",
                snapshot=target,
                fills=(
                    replace(
                        fill("scanned", "1", entry=True),
                        trade_at=NOW + timedelta(seconds=1),
                    ),
                ),
            ),
            stream_epoch="next",
            sequence=1,
            coverage_evidence=proof,
            fill_load_provenance=provenance,
            source_anchor_snapshot=anchor,
        )
    )
    if complete:
        assert isinstance(result, EvidenceConflict)
        assert "validated recovery checkpoint" in result.reason
    else:
        assert isinstance(result, WaitingForEvidence)
        assert result.reason is EvidencePendingReason.STREAM_RECOVERY_PROOF_REQUIRED
    assert (await book.read(SCOPE)).projection_version == before.projection_version
    assert (await book.read(SCOPE)).total_quantity == Decimal("5")
