from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select

from crypto_momentum_lab.domain.execution.recovery_models import AccountFacts
from crypto_momentum_lab.persistence.postgres.account_journal_store import (
    PostgresAccountJournalStore,
)
from crypto_momentum_lab.persistence.postgres.position_fact_journal_models import (
    PositionFactJournalEventRow,
    PositionRecoveryCheckpointRow,
)
from tests.integration.persistence.test_shared_policy_evidence import (
    evidence_sessions,  # noqa: F401
)
from tests.unit.persistence.postgres.test_checkpoint_snapshot_recovery import (
    _checkpoint,
)

pytestmark = pytest.mark.integration


async def test_repeated_checkpoint_cuts_restore_without_duplicate_state_rows(
    evidence_sessions,  # noqa: F811 - imported pytest fixture
):
    store = PostgresAccountJournalStore()
    base = replace(
        _checkpoint(), has_late_events=True, integrity_issues=("source uncertain",)
    )
    for index in range(100):
        checkpoint = replace(
            base,
            checkpoint_id=f"cut-{index}",
            source_revision=base.source_revision + index,
        )
        facts = AccountFacts(
            position_key=checkpoint.key,
            stream_scope=checkpoint.stream_scope,
            recovery_checkpoint=checkpoint,
            has_late_events=True,
            integrity_issues=checkpoint.integrity_issues,
        )
        async with evidence_sessions() as session, session.begin():
            await store.persist_facts_in_session(
                session,
                scope=checkpoint.stream_scope,
                facts=facts,
                revision=checkpoint.source_revision,
            )
    async with evidence_sessions() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(PositionFactJournalEventRow)
                .where(PositionFactJournalEventRow.event_kind == "facts_state")
            )
            == 0
        )
        for include_prefix in (False, True):
            cut = await store.load_recovery_in_session(
                session,
                scope=checkpoint.stream_scope,
                as_of=datetime.now(UTC) + timedelta(seconds=1),
                include_checkpoint_prefix=include_prefix,
            )
            assert cut.checkpoint.checkpoint_id == "cut-99"
            assert cut.facts.has_late_events is True
            assert cut.facts.integrity_issues == ("source uncertain",)
            assert cut.checkpoint.projection == base.projection


async def test_bounded_checkpoint_and_suffix_fetch_in_isolated_schema(
    evidence_sessions,  # noqa: F811
    monkeypatch,
):
    from uuid import uuid4

    import tests.unit.execution.test_position_recovery as base_test_helpers
    from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        ExitOrderSubmissionFact,
    )
    from crypto_momentum_lab.domain.execution.recovery_codec import (
        PositionRecoveryCodec,
    )
    from crypto_momentum_lab.persistence.postgres.account_journal_store import (
        JournalFactConflict,
        _json_digest,
    )
    from tests.unit.execution.test_position_recovery import _fill, _time
    from tests.unit.persistence.postgres import (
        test_checkpoint_snapshot_recovery as helper_module,
    )
    from tests.unit.persistence.postgres.test_checkpoint_snapshot_recovery import (
        _row,
        _snapshot,
    )

    unique_account = f"t2-{uuid4().hex}"
    orig_key_fn = base_test_helpers._key
    monkeypatch.setattr(
        helper_module,
        "_key",
        lambda: replace(orig_key_fn(), account_label=unique_account),
    )

    store = PostgresAccountJournalStore()
    canonical_checkpoint = helper_module._checkpoint()
    scope = canonical_checkpoint.stream_scope
    key = canonical_checkpoint.key
    base_cut = canonical_checkpoint.event_cut
    as_of = datetime.now(UTC) + timedelta(seconds=1)

    # 1. Seed 20 historical checkpoints at lower source_revisions
    # (same payload/semantics)
    async with evidence_sessions() as session, session.begin():
        for rev in range(100, 120):
            ckpt = replace(
                canonical_checkpoint,
                checkpoint_id=f"hist-ckpt-{rev}",
                source_revision=rev,
            )
            await store.save_checkpoint_in_session(session, ckpt)

    # 2. Seed latest checkpoint and duplicate checkpoint variant with different
    # ID but same semantics
    latest_rev = 406
    latest_ckpt_a = replace(
        canonical_checkpoint,
        checkpoint_id="latest-ckpt-a",
        source_revision=latest_rev,
    )
    latest_ckpt_b = replace(
        canonical_checkpoint,
        checkpoint_id="latest-ckpt-b",
        source_revision=latest_rev,
    )
    async with evidence_sessions() as session, session.begin():
        await store.save_checkpoint_in_session(session, latest_ckpt_a)
        await store.save_checkpoint_in_session(session, latest_ckpt_b)

    # 3. Seed journal rows:
    # - active target boundary matching latest checkpoint active batch
    active_batch_id = latest_ckpt_a.projection.active_batches[0].batch_id
    active_boundary = ExitOrderSubmissionFact(
        order_id="active-exit",
        submitted_at=base_cut - timedelta(seconds=2),
        symbol=key.symbol,
        position_side=key.position_side,
        target_batch_id=active_batch_id,
    )
    row_active_boundary = _row(
        latest_ckpt_a,
        "boundary",
        f"active-exit:{active_boundary.submitted_at.isoformat()}",
        active_boundary.submitted_at,
        PositionRecoveryCodec.encode_boundary(active_boundary),
    )
    row_active_boundary.source_revision = latest_rev - 1
    row_active_boundary.recorded_at = base_cut - timedelta(seconds=2)

    # - unrelated boundary
    unrelated_boundary = ExitOrderSubmissionFact(
        order_id="unrelated-exit",
        submitted_at=base_cut - timedelta(seconds=2),
        symbol=key.symbol,
        position_side=key.position_side,
        target_batch_id="unrelated-batch-closed",
    )
    row_unrelated_boundary = _row(
        latest_ckpt_a,
        "boundary",
        "unrelated-boundary-id",
        unrelated_boundary.submitted_at,
        PositionRecoveryCodec.encode_boundary(unrelated_boundary),
    )
    row_unrelated_boundary.source_revision = latest_rev - 1
    row_unrelated_boundary.recorded_at = base_cut - timedelta(seconds=2)

    # - historical prefix ordinary snapshot (revision below latest, occurred_at <= cut)
    hist_snap = _snapshot(key, base_cut - timedelta(seconds=1), "2", "10")
    row_hist_snap = _row(
        latest_ckpt_a,
        "snapshot",
        "hist-prefix-snapshot-id",
        hist_snap.observed_at,
        PositionRecoveryCodec.encode_snapshot(hist_snap),
    )
    row_hist_snap.source_revision = latest_rev - 1
    row_hist_snap.recorded_at = base_cut - timedelta(seconds=1)

    # - late divergent snapshot with newer source_revision (> latest_rev)
    # but occurred_at <= cut
    late_snap_time = base_cut - timedelta(seconds=1)
    late_snap_id = f"BOTH:{late_snap_time.isoformat()}"
    orig_late_snap = _snapshot(key, late_snap_time, "1", "10")
    row_orig_late_snap = _row(
        latest_ckpt_a,
        "snapshot",
        late_snap_id,
        late_snap_time,
        PositionRecoveryCodec.encode_snapshot(orig_late_snap),
    )
    row_orig_late_snap.source_revision = latest_rev - 1
    row_orig_late_snap.recorded_at = base_cut - timedelta(seconds=1)

    divergent_late_snap = _snapshot(key, late_snap_time, "3", "10")
    row_divergent_late_snap = _row(
        latest_ckpt_a,
        "snapshot",
        late_snap_id,
        late_snap_time,
        PositionRecoveryCodec.encode_snapshot(divergent_late_snap),
    )
    row_divergent_late_snap.source_revision = latest_rev + 1
    row_divergent_late_snap.recorded_at = base_cut + timedelta(seconds=10)

    # - future-recorded conflicting snapshot that should NOT influence earlier as_of
    future_time = as_of + timedelta(minutes=5)
    future_snap = _snapshot(key, future_time, "9", "10")
    row_future_snap = _row(
        latest_ckpt_a,
        "snapshot",
        "future-conflicting-snap",
        future_time,
        PositionRecoveryCodec.encode_snapshot(future_snap),
    )
    row_future_snap.source_revision = latest_rev + 5
    row_future_snap.recorded_at = future_time

    # - future-recorded snapshot variant with occurred_at <= cut and revision > cut,
    # sharing hist_snap ID but recorded_at > as_of. Must not trigger late subquery.
    future_same_id_snap = _snapshot(key, hist_snap.observed_at, "7", "10")
    row_future_same_id_snap = _row(
        latest_ckpt_a,
        "snapshot",
        "hist-prefix-snapshot-id",
        hist_snap.observed_at,
        PositionRecoveryCodec.encode_snapshot(future_same_id_snap),
    )
    row_future_same_id_snap.source_revision = latest_rev + 2
    row_future_same_id_snap.recorded_at = future_time

    entry = _fill(key, "entry", "BUY", "2", "10", _time(1))
    entry_row = _row(
        latest_ckpt_a,
        "fill",
        entry.trade_id,
        entry.trade_at,
        PositionRecoveryCodec.encode_fill(entry),
    )
    entry_row.source_revision = latest_rev - 1
    entry_row.recorded_at = base_cut - timedelta(seconds=1)

    # Seed rows without divergent late snapshot first to verify clean fixture projection
    async with evidence_sessions() as session, session.begin():
        for row in (
            entry_row,
            row_active_boundary,
            row_unrelated_boundary,
            row_hist_snap,
            row_orig_late_snap,
            row_future_snap,
            row_future_same_id_snap,
        ):
            session.add(row)

    # Observation wrapper to verify actual ORM query results
    class _Rows:
        def __init__(self, items):
            self._items = items

        def all(self):
            return self._items

    fetched_counts = {}
    orig_scalars = None

    def install_observer(session):
        nonlocal orig_scalars
        orig_scalars = session.scalars

        async def observe_scalars(statement, *args, **kwargs):
            result = await orig_scalars(statement, *args, **kwargs)
            items = list(result.all())
            for item in items:
                table_name = getattr(getattr(item, "__table__", None), "name", None)
                if table_name:
                    fetched_counts[table_name] = fetched_counts.get(table_name, 0) + 1
            return _Rows(items)

        session.scalars = observe_scalars

    # Test clean suffix load vs fullprefix
    async with evidence_sessions() as session:
        install_observer(session)
        try:
            cut_suffix = await store.load_recovery_in_session(
                session,
                scope=scope,
                as_of=as_of,
                include_checkpoint_prefix=False,
            )
        finally:
            session.scalars = orig_scalars

        assert fetched_counts.get("position_recovery_checkpoints", 0) == 2
        # Clean suffix query bounds SQL retrieval: only activeboundary is fetched
        assert fetched_counts.get("position_fact_journal_events", 0) == 1
        assert cut_suffix.checkpoint.checkpoint_id in ("latest-ckpt-a", "latest-ckpt-b")
        assert cut_suffix.checkpoint.source_revision == latest_rev
        assert cut_suffix.facts.exit_boundaries == (active_boundary,)
        assert not any(
            b.order_id == "unrelated-exit" for b in cut_suffix.facts.exit_boundaries
        )
        assert not any(
            s.observed_at == hist_snap.observed_at for s in cut_suffix.facts.snapshots
        )

        cut_full = await store.load_recovery_in_session(
            session,
            scope=scope,
            as_of=as_of,
            include_checkpoint_prefix=True,
        )
        assert len(cut_full.facts.snapshots) > len(cut_suffix.facts.snapshots)
        proj_suffix = PositionLedger(key).project(cut_suffix.facts)
        proj_full = PositionLedger(key).project(cut_full.facts)
        assert proj_suffix.total_active_quantity == proj_full.total_active_quantity

    # Now add divergent late snapshot and verify gap conflict retention
    async with evidence_sessions() as session, session.begin():
        session.add(row_divergent_late_snap)

    async with evidence_sessions() as session:
        cut_with_conflict = await store.load_recovery_in_session(
            session,
            scope=scope,
            as_of=as_of,
            include_checkpoint_prefix=False,
        )
        assert any(
            c.event_kind == "snapshot" for c in cut_with_conflict.facts.fact_conflicts
        )
        assert not PositionLedger(key).project(cut_with_conflict.facts).is_comparable

        # Verify future conflicting snapshot is not visible at earlier as_of
        assert not any(
            s.observed_at == future_time for s in cut_with_conflict.facts.snapshots
        )

    # Test semantic conflict in same latest checkpoint cut/revision group
    clashing_payload = PositionRecoveryCodec.encode_checkpoint(latest_ckpt_a)
    clashing_payload["schema_version"] = 999
    clashing_row = PositionRecoveryCheckpointRow(
        environment=scope.environment,
        account_label=scope.account_label,
        symbol=scope.symbol,
        position_side=scope.position_side.value,
        stream_id=scope.stream_id,
        stream_epoch=scope.stream_epoch,
        checkpoint_id="latest-ckpt-divergent",
        schema_version=999,
        event_cut=latest_ckpt_a.event_cut,
        source_revision=latest_ckpt_a.source_revision,
        facts_hash="divergent_facts_hash",
        payload_hash=_json_digest(clashing_payload),
        payload=clashing_payload,
    )
    async with evidence_sessions() as session, session.begin():
        session.add(clashing_row)

    async with evidence_sessions() as session:
        with pytest.raises(
            JournalFactConflict,
            match="multiple different checkpoints claim the same scope",
        ):
            await store.load_checkpoint_in_session(session, scope=scope, as_of=as_of)


async def test_filter_known_facts_in_session_detects_conflicts_and_idempotent_repeats(
    evidence_sessions,  # noqa: F811
):
    from uuid import uuid4

    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        ExitOrderSubmissionFact,
        JournalFactDelta,
    )
    from crypto_momentum_lab.persistence.postgres.account_journal_store import (
        JournalFactConflict,
    )
    from tests.unit.persistence.postgres import (
        test_checkpoint_snapshot_recovery as helper_module,
    )

    store = PostgresAccountJournalStore()
    canonical_checkpoint = helper_module._checkpoint()
    scope = replace(
        canonical_checkpoint.stream_scope,
        account_label=f"filter-dedup-{uuid4().hex[:12]}",
    )
    t0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

    # 1. Intra-batch conflict: same observed_at, different position_amt
    snap_a = helper_module._snapshot(canonical_checkpoint.key, t0, "10", "100")
    snap_b = helper_module._snapshot(canonical_checkpoint.key, t0, "99", "100")
    delta_snap_conflict = JournalFactDelta(snapshots=(snap_a, snap_b))
    async with evidence_sessions() as session:
        with pytest.raises(JournalFactConflict, match="duplicate snapshot identity"):
            await store.filter_known_facts_in_session(
                session, scope=scope, delta=delta_snap_conflict
            )

    # 2. Intra-batch conflict: same natural boundary identity
    # (order_id + submitted_at), different target_batch_id
    bound_a = ExitOrderSubmissionFact(
        order_id="exit-cmd-1",
        submitted_at=t0,
        symbol=scope.symbol,
        position_side=scope.position_side,
        target_batch_id="batch-1",
    )
    bound_b = ExitOrderSubmissionFact(
        order_id="exit-cmd-1",
        submitted_at=t0,
        symbol=scope.symbol,
        position_side=scope.position_side,
        target_batch_id="batch-2",
    )
    delta_bound_conflict = JournalFactDelta(exit_boundaries=(bound_a, bound_b))
    async with evidence_sessions() as session:
        with pytest.raises(JournalFactConflict, match="duplicate boundary identity"):
            await store.filter_known_facts_in_session(
                session, scope=scope, delta=delta_bound_conflict
            )

    # 3. Repeated identical payloads with different instances should not conflict
    # and filter cleanly
    snap_a_copy = helper_module._snapshot(canonical_checkpoint.key, t0, "10", "100")
    assert snap_a is not snap_a_copy
    assert snap_a == snap_a_copy

    bound_a_copy = ExitOrderSubmissionFact(
        order_id="exit-cmd-1",
        submitted_at=t0,
        symbol=scope.symbol,
        position_side=scope.position_side,
        target_batch_id="batch-1",
    )
    assert bound_a is not bound_a_copy
    assert bound_a == bound_a_copy

    delta_repeats = JournalFactDelta(
        snapshots=(snap_a, snap_a_copy),
        exit_boundaries=(bound_a, bound_a_copy),
    )
    async with evidence_sessions() as session:
        filtered = await store.filter_known_facts_in_session(
            session, scope=scope, delta=delta_repeats
        )
        assert filtered.snapshots == (snap_a, snap_a_copy)
        assert filtered.exit_boundaries == (bound_a, bound_a_copy)
