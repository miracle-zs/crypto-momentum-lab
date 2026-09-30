from crypto_momentum_lab.domain.execution.snapshot_encoding import (
    stable_snapshot_anchor_id,
)

"""Recovery checkpoints seed a real ledger and replay only their suffix."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFacts,
    AccountFactStreamScope,
    AccountFillLoadProvenance,
    CoverageEvidence,
    ExitOrderSubmissionFact,
    PositionKey,
    compose_fact_coverage,
)
from crypto_momentum_lab.domain.execution.recovery_codec import (
    PositionRecoveryCodec,
    RecoverySchemaError,
)
from crypto_momentum_lab.domain.execution.recovery_models import (
    StreamCheckpointAdoption,
)


def _time(minute: int) -> datetime:
    return datetime(2026, 9, 27, 12, minute, tzinfo=UTC)


def _key() -> PositionKey:
    return PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.BOTH)


def _scope(key: PositionKey) -> AccountFactStreamScope:
    return AccountFactStreamScope.for_position_key(
        key,
        stream_id="execution-book",
        stream_epoch="epoch-7",
    )


def _fill(
    key: PositionKey,
    trade_id: str,
    side: str,
    quantity: str,
    price: str,
    at: datetime,
    *,
    order_id: str | None = None,
) -> AccountFillEvent:
    return AccountFillEvent(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        trade_id=trade_id,
        order_id=order_id or f"order-{trade_id}",
        side=side,
        price=Decimal(price),
        quantity=Decimal(quantity),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=at,
        raw_payload={"positionSide": "BOTH"},
    )


def test_checkpoint_plus_suffix_restores_nonzero_batches_and_cost_basis() -> None:
    key = _key()
    scope = _scope(key)
    ledger = PositionLedger(key)
    fills = (
        _fill(key, "open-1", "BUY", "2", "10", _time(1)),
        _fill(key, "close-1", "SELL", "2", "12", _time(2)),
        _fill(key, "open-2", "BUY", "2", "14", _time(3)),
    )
    facts = AccountFacts(position_key=key, fills=fills, stream_scope=scope)
    checkpoint = ledger.create_recovery_checkpoint(
        facts,
        source_revision=9,
        event_cut=_time(3),
    )

    assert checkpoint.projection.total_active_quantity == Decimal("2")
    assert len(checkpoint.projection.archived_episodes) == 1
    checkpoint_episode_id = checkpoint.projection.active_episode.episode_id
    checkpoint_batch_id = checkpoint.projection.active_batches[0].batch_id
    suffix_fill = _fill(key, "reduce-2", "SELL", "0.5", "15", _time(4))
    restored_facts = AccountFacts(
        position_key=key,
        fills=(suffix_fill,),
        stream_scope=scope,
        recovery_checkpoint=checkpoint,
        prefix_facts_complete=False,
    )

    restored = ledger.project(restored_facts)

    assert restored.total_active_quantity == Decimal("1.5")
    assert restored.active_episode is not None
    assert restored.active_episode.episode_id == checkpoint_episode_id
    assert restored.active_batches[0].batch_id == checkpoint_batch_id
    assert restored.active_batches[0].entry_price == Decimal("14")
    assert restored.active_batches[0].quantity == Decimal("1.5")
    assert len(restored.archived_episodes) == 1
    assert restored.archived_episodes[0].episode_id == (
        checkpoint.projection.archived_episodes[0].episode_id
    )


def test_truncated_prefix_checkpoint_does_not_project_suffix_as_total() -> None:
    key = _key()
    scope = _scope(key)
    ledger = PositionLedger(key)
    full_facts = AccountFacts(
        position_key=key,
        fills=(_fill(key, "entry", "BUY", "5", "10", _time(1)),),
        stream_scope=scope,
    )
    checkpoint = ledger.create_recovery_checkpoint(
        full_facts,
        source_revision=4,
        event_cut=_time(1),
    )
    wrong_scope = checkpoint
    other_scope = replace(scope, stream_epoch="different-epoch")
    suffix = _fill(key, "later", "BUY", "1", "11", _time(2))
    projection = ledger.project(
        AccountFacts(
            position_key=key,
            fills=(suffix,),
            stream_scope=other_scope,
            recovery_checkpoint=wrong_scope,
            prefix_facts_complete=False,
        )
    )

    assert projection.total_active_quantity == Decimal("0")
    assert projection.health_status.value == "INCOMPLETE"
    assert not projection.is_comparable
    assert any(
        "Historical prefix is unavailable" in item for item in projection.diagnostics
    )


def test_checkpoint_codec_is_strict_and_round_trips_full_projection() -> None:
    key = _key()
    scope = _scope(key)
    ledger = PositionLedger(key)
    facts = AccountFacts(
        position_key=key,
        fills=(_fill(key, "entry", "BUY", "2", "10", _time(1)),),
        stream_scope=scope,
    )
    checkpoint = ledger.create_recovery_checkpoint(
        facts,
        source_revision=2,
        event_cut=_time(1),
    )
    encoded = PositionRecoveryCodec.encode_checkpoint(checkpoint)

    assert PositionRecoveryCodec.decode_checkpoint(encoded) == checkpoint

    malformed = dict(encoded)
    malformed["has_late_events"] = "false"
    with pytest.raises(RecoverySchemaError, match="boolean"):
        PositionRecoveryCodec.decode_checkpoint(malformed)

    unsupported = dict(encoded)
    unsupported["schema_version"] = 99
    with pytest.raises(RecoverySchemaError, match="unsupported"):
        PositionRecoveryCodec.decode_checkpoint(unsupported)


def test_checkpoint_event_cut_rejects_future_batch_and_boundary() -> None:
    key = _key()
    scope = _scope(key)
    ledger = PositionLedger(key)
    facts = AccountFacts(
        position_key=key,
        fills=(_fill(key, "entry", "BUY", "2", "10", _time(1)),),
        exit_boundaries=(
            ExitOrderSubmissionFact(
                order_id="exit",
                submitted_at=_time(2),
                symbol=key.symbol,
                position_side=key.position_side,
            ),
        ),
        stream_scope=scope,
    )
    projection = ledger.project(facts)
    future_batch = replace(projection.active_batches[0], opened_at=_time(2))
    bad_projection = replace(
        projection,
        active_batches=(future_batch,),
        active_episode=replace(
            projection.active_episode,
            batches=(future_batch,),
        ),
    )

    with pytest.raises(ValueError, match="after checkpoint cut"):
        from crypto_momentum_lab.domain.execution.recovery_models import (
            PositionRecoveryCheckpoint,
        )

        PositionRecoveryCheckpoint(
            key=key,
            stream_scope=scope,
            event_cut=_time(1),
            projection=bad_projection,
            facts_hash=facts.compute_facts_hash(),
            source_revision=1,
        )


def test_checkpoint_id_is_deterministic_for_same_cut_and_contents() -> None:
    key = _key()
    ledger = PositionLedger(key)
    facts = AccountFacts(
        position_key=key,
        fills=(_fill(key, "entry", "BUY", "2", "10", _time(1)),),
        stream_scope=_scope(key),
    )

    first = ledger.create_recovery_checkpoint(
        facts,
        source_revision=2,
        event_cut=_time(1),
    )
    second = ledger.create_recovery_checkpoint(
        facts,
        source_revision=2,
        event_cut=_time(1),
    )

    assert first.checkpoint_id == second.checkpoint_id


def test_durable_projection_token_survives_unchanged_later_cut() -> None:
    key = _key()
    scope = _scope(key)
    cut = _time(2)
    facts = AccountFacts(
        position_key=key,
        fills=(_fill(key, "entry", "BUY", "2", "10", cut),),
        stream_scope=scope,
    )
    checkpoint = PositionLedger(key).create_recovery_checkpoint(
        facts,
        source_revision=7,
        event_cut=cut,
    )
    journal = AccountJournal(key, stream_scope=scope)
    journal.set_recovery_checkpoint(checkpoint)
    book = PositionBook(journal)
    book.use_durable_projection_version("pv_durable_head", event_cut=cut)

    current = book.get_view()
    later = book.get_view(cut + timedelta(minutes=5))
    historical = book.get_view(cut - timedelta(minutes=1))

    assert current.projection_version == "pv_durable_head"
    assert later.projection_version == "pv_durable_head"
    assert historical.projection_version != "pv_durable_head"


def test_verified_flat_snapshot_can_seed_a_scoped_stream() -> None:
    key = _key()
    scope = _scope(key)
    cut = _time(2)
    snapshot = AccountPositionSnapshot(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        position_side=key.position_side.value,
        position_amt=Decimal("0"),
        entry_price=Decimal("0"),
        mark_price=Decimal("10"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("0"),
        leverage=1,
        margin_type="cross",
        observed_at=cut,
        raw_payload={"include_flat": True},
    )
    anchor_id = stable_snapshot_anchor_id(snapshot)
    provenance = AccountFillLoadProvenance(
        stream_scope=scope,
        load_id="flat-bootstrap-1",
        scan_origin_from_id=None,
        scan_origin_start_time_ms=int(cut.timestamp() * 1000),
        request_from_id=None,
        next_from_id=None,
        page_count=1,
        page_exhausted=True,
        truncated=False,
        checked_through=cut,
        observed_at=cut,
        source_anchor_id=anchor_id,
        source_anchor_event_cut=cut,
        source_anchor_kind="zero_snapshot",
    )
    coverage = compose_fact_coverage(
        CoverageEvidence(
            fill_cursor_id=provenance.load_id,
            fill_load_start=provenance.origin_start_at,
            fill_checked_through=cut,
            checkpoint_id=anchor_id,
            checkpoint_event_cut=cut,
            stream_scope=scope,
            evidence_observed_at=cut,
            page_exhausted=True,
            not_truncated=True,
            load_provenance=provenance,
        ),
        start=cut,
        end=cut,
        expected_scope=scope,
    )
    facts = AccountFacts(
        position_key=key,
        stream_scope=scope,
        snapshots=(snapshot,),
        coverage=coverage,
        fill_load_provenance=provenance,
    )

    checkpoint = PositionLedger(key).create_recovery_checkpoint(
        facts,
        source_revision=1,
        event_cut=cut,
    )

    assert checkpoint.projection.total_active_quantity == Decimal("0")
    assert checkpoint.coverage == coverage
    journal = AccountJournal(key, stream_scope=scope)
    journal.record_snapshot(snapshot)
    journal.record_fill_load_provenance(provenance)
    journal.set_coverage(coverage)
    view = PositionBook(journal).get_view()
    assert view.zero_position_snapshot_confirmed
    assert view.is_ready_for_trade


def test_nonzero_checkpoint_adopts_across_stream_epoch_with_real_suffix_proof() -> None:
    key = _key()
    old_scope = _scope(key)
    new_scope = replace(old_scope, stream_epoch="epoch-8")
    anchor_cut = _time(0)
    parent_cut = _time(1)
    target_cut = _time(2)
    flat_snapshot = AccountPositionSnapshot(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        position_side=key.position_side.value,
        position_amt=Decimal("0"),
        entry_price=Decimal("0"),
        mark_price=Decimal("10"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("0"),
        leverage=1,
        margin_type="cross",
        observed_at=anchor_cut,
        raw_payload={"include_flat": True},
    )
    parent_snapshot = replace(
        flat_snapshot,
        position_amt=Decimal("2"),
        entry_price=Decimal("10"),
        observed_at=parent_cut,
        raw_payload={},
    )
    parent_anchor = stable_snapshot_anchor_id(flat_snapshot)
    parent_provenance = AccountFillLoadProvenance(
        stream_scope=old_scope,
        load_id="old-epoch-load",
        scan_origin_from_id=None,
        scan_origin_start_time_ms=int(anchor_cut.timestamp() * 1000),
        request_from_id=None,
        next_from_id=None,
        page_count=1,
        page_exhausted=True,
        truncated=False,
        checked_through=parent_cut,
        observed_at=parent_cut,
        source_anchor_id=parent_anchor,
        source_anchor_event_cut=anchor_cut,
        source_anchor_kind="zero_snapshot",
    )
    parent_coverage = compose_fact_coverage(
        CoverageEvidence(
            fill_cursor_id="old-epoch-load",
            fill_load_start=anchor_cut,
            fill_checked_through=parent_cut,
            checkpoint_id="old-epoch-cut",
            checkpoint_event_cut=parent_cut,
            stream_scope=old_scope,
            evidence_observed_at=parent_cut,
            page_exhausted=True,
            not_truncated=True,
            load_provenance=parent_provenance,
        ),
        start=anchor_cut,
        end=parent_cut,
        expected_scope=old_scope,
    )
    opening = _fill(key, "old-open", "BUY", "2", "10", parent_cut)
    parent_facts = AccountFacts(
        position_key=key,
        stream_scope=old_scope,
        fills=(opening,),
        snapshots=(flat_snapshot, parent_snapshot),
        coverage=parent_coverage,
        fill_load_provenance=parent_provenance,
    )
    ledger = PositionLedger(key)
    parent = ledger.create_recovery_checkpoint(
        parent_facts,
        source_revision=4,
        event_cut=parent_cut,
    )

    suffix_fill = _fill(key, "new-reduce", "SELL", "0.5", "15", _time(2))
    target_snapshot = replace(
        parent_snapshot,
        position_amt=Decimal("1.5"),
        observed_at=target_cut,
    )
    target_provenance = AccountFillLoadProvenance(
        stream_scope=new_scope,
        load_id="new-epoch-load",
        scan_origin_from_id=None,
        scan_origin_start_time_ms=int(anchor_cut.timestamp() * 1000),
        request_from_id=None,
        next_from_id=None,
        page_count=1,
        page_exhausted=True,
        truncated=False,
        checked_through=target_cut,
        observed_at=target_cut,
        source_anchor_id=parent.checkpoint_id,
        source_anchor_event_cut=parent.event_cut,
        source_anchor_kind="recovery_checkpoint",
    )
    target_coverage = compose_fact_coverage(
        CoverageEvidence(
            fill_cursor_id="new-epoch-load",
            fill_load_start=target_provenance.origin_start_at,
            fill_checked_through=target_cut,
            checkpoint_id="new-epoch-cut",
            checkpoint_event_cut=target_cut,
            stream_scope=new_scope,
            evidence_observed_at=target_cut,
            page_exhausted=True,
            not_truncated=True,
            load_provenance=target_provenance,
        ),
        start=parent_cut,
        end=target_cut,
        expected_scope=new_scope,
    )
    target_facts = AccountFacts(
        position_key=key,
        stream_scope=new_scope,
        fills=(suffix_fill,),
        snapshots=(target_snapshot,),
        coverage=target_coverage,
        fill_load_provenance=target_provenance,
        prefix_facts_complete=False,
    )
    wrong_anchor = replace(target_provenance, source_anchor_id="other-parent")
    with pytest.raises(ValueError, match="does not bind the parent"):
        StreamCheckpointAdoption(
            parent_checkpoint=parent,
            target_scope=new_scope,
            target_event_cut=target_cut,
            fill_load_provenance=wrong_anchor,
        )
    adoption = StreamCheckpointAdoption(
        parent_checkpoint=parent,
        target_scope=new_scope,
        target_event_cut=target_cut,
        fill_load_provenance=target_provenance,
    )

    adopted = ledger.project(target_facts, stream_adoption=adoption)
    child = ledger.create_recovery_checkpoint(
        target_facts,
        source_revision=1,
        event_cut=target_cut,
        stream_adoption=adoption,
    )
    restored = ledger.project(
        AccountFacts(
            position_key=key,
            stream_scope=new_scope,
            recovery_checkpoint=child,
            prefix_facts_complete=False,
        )
    )

    assert parent.stream_scope == old_scope
    assert child.stream_scope == new_scope
    assert child.parent_stream_scope == old_scope
    assert child.parent_checkpoint_id == parent.checkpoint_id
    assert child.parent_facts_hash == parent.facts_hash
    assert child.parent_projection_digest == parent.projection_digest
    assert child.parent_event_cut == parent_cut
    expected = ledger.project(
        AccountFacts(
            position_key=key,
            stream_scope=new_scope,
            fills=(opening, suffix_fill),
            snapshots=(flat_snapshot, parent_snapshot, target_snapshot),
        )
    )
    assert adopted.active_batches == expected.active_batches
    assert restored.active_batches == expected.active_batches
    assert restored.total_active_quantity == Decimal("1.5")
    assert restored.active_batches[0].entry_price == Decimal("10")
    assert (
        PositionRecoveryCodec.decode_checkpoint(
            PositionRecoveryCodec.encode_checkpoint(child)
        )
        == child
    )


def test_empty_live_scope_cannot_checkpoint_an_inferred_zero() -> None:
    key = _key()
    ledger = PositionLedger(key)
    facts = AccountFacts(position_key=key, stream_scope=_scope(key))

    with pytest.raises(ValueError, match="verified flat snapshot"):
        ledger.create_recovery_checkpoint(facts, source_revision=1, event_cut=_time(2))
