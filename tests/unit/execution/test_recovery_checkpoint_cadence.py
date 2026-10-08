from datetime import timedelta

from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFillLoadProvenance,
    CoverageEvidence,
    compose_fact_coverage,
)
from crypto_momentum_lab.domain.execution.position_recovery import (
    create_verified_recovery_checkpoint,
)
from crypto_momentum_lab.domain.execution.recovery_models import (
    AccountFacts,
    DurableJournalCut,
)
from tests.unit.persistence.postgres.test_checkpoint_snapshot_recovery import (
    _checkpoint,
    _snapshot,
)


def next_scan(seconds=30, quantity="2"):
    parent = _checkpoint()
    scope = parent.stream_scope
    end = parent.event_cut + timedelta(seconds=seconds)
    facts = AccountFacts(
        position_key=parent.key,
        stream_scope=scope,
        recovery_checkpoint=parent,
        prefix_facts_complete=False,
    )
    journal = AccountJournal.from_durable_cut(
        DurableJournalCut(
            scope=scope,
            facts=facts,
            revision=parent.source_revision,
            as_of=parent.event_cut,
            checkpoint=parent,
        )
    )
    provenance = AccountFillLoadProvenance(
        stream_scope=scope,
        load_id="next-scan",
        scan_origin_from_id=None,
        scan_origin_start_time_ms=int(parent.event_cut.timestamp() * 1000),
        request_from_id=None,
        next_from_id=None,
        page_count=1,
        page_exhausted=True,
        truncated=False,
        checked_through=end,
        observed_at=end,
        source_anchor_id=parent.checkpoint_id,
        source_anchor_event_cut=parent.event_cut,
        source_anchor_kind="recovery_checkpoint",
    )
    proof = CoverageEvidence(
        fill_cursor_id="next-scan",
        fill_load_start=parent.event_cut,
        fill_checked_through=end,
        checkpoint_id="next-cut",
        checkpoint_event_cut=end,
        stream_scope=scope,
        evidence_observed_at=end,
        page_exhausted=True,
        not_truncated=True,
        load_provenance=provenance,
    )
    journal.record_snapshot(_snapshot(parent.key, end, quantity, "10"))
    journal.record_fill_load_provenance(provenance)
    journal.set_coverage(
        compose_fact_coverage(
            proof,
            start=parent.event_cut,
            end=end,
            expected_scope=scope,
        )
    )
    return journal, proof, provenance


def create(journal, proof, provenance, **kwargs):
    return create_verified_recovery_checkpoint(
        key=journal.position_key,
        scope=journal.stream_scope,
        journal=journal,
        proof=proof,
        provenance=provenance,
        adoption=None,
        adopting_epoch=False,
        **kwargs,
    )


def test_unchanged_scan_reuses_parent_and_preserves_fresh_projection():
    journal, proof, provenance = next_scan()
    parent = journal.read_cut().recovery_checkpoint
    assert create(journal, proof, provenance) is None
    assert journal.read_cut().recovery_checkpoint == parent


def test_elapsed_recovery_budget_creates_checkpoint():
    journal, proof, provenance = next_scan(seconds=300)
    checkpoint = create(journal, proof, provenance)
    assert checkpoint is not None
    assert checkpoint.event_cut == proof.checkpoint_event_cut


def test_explicit_repair_forces_checkpoint_even_when_projection_is_unchanged():
    journal, proof, provenance = next_scan()
    assert create(journal, proof, provenance, force_checkpoint=True) is not None


def test_new_fill_is_folded_without_waiting_for_time_budget():
    from tests.unit.execution.test_position_recovery import _fill

    journal, proof, provenance = next_scan(quantity="3")
    parent = journal.read_cut().recovery_checkpoint
    journal.append_fill(
        _fill(
            parent.key,
            "new-entry",
            "BUY",
            "1",
            "10",
            parent.event_cut + timedelta(seconds=10),
        )
    )
    checkpoint = create(journal, proof, provenance)
    assert checkpoint is not None
    assert str(checkpoint.projection.total_active_quantity) == "3"


def test_increment_budget_bounds_dense_snapshot_replay():
    journal, proof, provenance = next_scan()
    parent = journal.read_cut().recovery_checkpoint
    for index in range(1, 65):
        journal.record_snapshot(
            _snapshot(
                parent.key, parent.event_cut + timedelta(milliseconds=index), "2", "10"
            )
        )
    assert create(journal, proof, provenance) is not None


def test_deferred_checkpoint_preserves_current_view_freshness():
    from crypto_momentum_lab.domain.execution.position_book import PositionBook
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        FreshnessRequirement,
    )

    journal, proof, provenance = next_scan()
    expected = journal.copy_for_transaction()
    checkpoint = create(expected, proof, provenance, force_checkpoint=True)
    expected.set_recovery_checkpoint(checkpoint)
    assert create(journal, proof, provenance) is None
    for requirement in (None, FreshnessRequirement()):
        actual_view = PositionBook(journal).get_view(
            now=proof.checkpoint_event_cut, requirement=requirement
        )
        expected_view = PositionBook(expected).get_view(
            now=proof.checkpoint_event_cut, requirement=requirement
        )
        assert actual_view.event_cut == expected_view.event_cut
        assert actual_view.health_status == expected_view.health_status
        assert actual_view.batches == expected_view.batches
        assert actual_view.coverage == expected_view.coverage
        assert actual_view.reconciliation_gap == expected_view.reconciliation_gap


def test_new_exit_boundary_is_folded_without_waiting_for_time_budget():
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        ExitOrderSubmissionFact,
    )

    journal, proof, provenance = next_scan()
    parent = journal.read_cut().recovery_checkpoint
    journal.record_boundary(
        ExitOrderSubmissionFact(
            order_id="new-exit",
            submitted_at=parent.event_cut + timedelta(seconds=10),
            symbol=parent.key.symbol,
            position_side=parent.key.position_side,
            target_batch_id=parent.projection.active_batches[0].batch_id,
        )
    )
    checkpoint = create(journal, proof, provenance)
    assert checkpoint is not None
    assert checkpoint.projection.active_batches[0].exit_order_submitted_at is not None


def test_epoch_adoption_forces_a_fresh_verified_checkpoint():
    journal, proof, provenance = next_scan()
    checkpoint = create_verified_recovery_checkpoint(
        key=journal.position_key,
        scope=journal.stream_scope,
        journal=journal,
        proof=proof,
        provenance=provenance,
        adoption=None,
        adopting_epoch=True,
    )
    assert checkpoint is not None
