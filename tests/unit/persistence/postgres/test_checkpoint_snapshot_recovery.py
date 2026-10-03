"""Queued observation snapshots must not poison a verified recovery cut."""

from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, Mock

import pytest

from crypto_momentum_lab.domain.account import AccountPositionSnapshot
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFacts,
    AccountFillLoadProvenance,
    CoverageEvidence,
    ExitOrderSubmissionFact,
    compose_fact_coverage,
)
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.domain.execution.snapshot_encoding import (
    stable_snapshot_anchor_id,
)
from crypto_momentum_lab.persistence.postgres.account_journal_store import (
    PostgresAccountJournalStore,
    _json_digest,
)
from crypto_momentum_lab.persistence.postgres.position_fact_journal_models import (
    PositionFactJournalEventRow,
)
from tests.unit.execution.test_position_recovery import _fill, _key, _scope, _time


def _snapshot(key, at, quantity, entry_price):
    return AccountPositionSnapshot(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        position_side=key.position_side.value,
        position_amt=Decimal(quantity),
        entry_price=Decimal(entry_price),
        mark_price=Decimal("10"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal(quantity) * Decimal("10"),
        leverage=2,
        margin_type="cross",
        observed_at=at,
        raw_payload={"positionSide": key.position_side.value},
    )


def _checkpoint():
    key = _key()
    scope = _scope(key)
    origin = _snapshot(key, _time(0), "0", "0")
    provenance = AccountFillLoadProvenance(
        stream_scope=scope,
        load_id="scan",
        scan_origin_from_id=None,
        scan_origin_start_time_ms=int(_time(0).timestamp() * 1000),
        request_from_id=None,
        next_from_id=None,
        page_count=1,
        page_exhausted=True,
        truncated=False,
        checked_through=_time(3),
        observed_at=_time(3),
        source_anchor_id=stable_snapshot_anchor_id(origin),
        source_anchor_event_cut=_time(0),
        source_anchor_kind="zero_snapshot",
    )
    proof = CoverageEvidence(
        fill_cursor_id="scan",
        fill_load_start=_time(0),
        fill_checked_through=_time(3),
        checkpoint_id="verified-cut",
        checkpoint_event_cut=_time(3),
        stream_scope=scope,
        evidence_observed_at=_time(3),
        page_exhausted=True,
        not_truncated=True,
        load_provenance=provenance,
    )
    facts = AccountFacts(
        position_key=key,
        stream_scope=scope,
        fills=(_fill(key, "entry", "BUY", "2", "10", _time(1)),),
        snapshots=(origin, _snapshot(key, _time(3), "2", "10")),
        coverage=compose_fact_coverage(
            proof,
            start=_time(0),
            end=_time(3),
            expected_scope=scope,
        ),
        fill_load_provenance=provenance,
    )
    return PositionLedger(key).create_recovery_checkpoint(
        facts, source_revision=406, event_cut=_time(3)
    )


def _row(checkpoint, kind, event_id, at, payload):
    scope = checkpoint.stream_scope
    return PositionFactJournalEventRow(
        event_record_id=_json_digest([kind, event_id, payload]),
        environment=scope.environment,
        account_label=scope.account_label,
        symbol=scope.symbol,
        position_side=scope.position_side.value,
        stream_id=scope.stream_id,
        stream_epoch=scope.stream_epoch,
        event_kind=kind,
        event_id=event_id,
        payload_hash=_json_digest(payload),
        source_revision=checkpoint.source_revision + 1,
        occurred_at=at,
        recorded_at=checkpoint.event_cut + timedelta(seconds=26),
        payload=payload,
    )


async def _restore(checkpoint, rows):
    store = PostgresAccountJournalStore()
    store.load_checkpoint_in_session = AsyncMock(return_value=checkpoint)
    session = AsyncMock()
    session.scalars.return_value = Mock(all=Mock(return_value=rows))
    return await store.load_recovery_in_session(
        session,
        scope=checkpoint.stream_scope,
        as_of=checkpoint.event_cut + timedelta(minutes=1),
    )


@pytest.mark.parametrize("offset", [-1, 0])
async def test_queued_snapshot_before_checkpoint_does_not_block_restored_ledger(offset):
    checkpoint = _checkpoint()
    at = checkpoint.event_cut + timedelta(seconds=offset)
    # An older observation may have a different quantity; it is not a reducer
    # and cannot replace the newer verified checkpoint's quantity/cost basis.
    snapshot = _snapshot(_key(), at, "1", "10")
    row = _row(
        checkpoint,
        "snapshot",
        f"BOTH:{at.isoformat()}",
        at,
        PositionRecoveryCodec.encode_snapshot(snapshot),
    )

    cut = await _restore(checkpoint, [row])
    projection = PositionLedger(_key()).project(cut.facts)

    assert not cut.facts.fact_conflicts
    assert not cut.integrity_issues
    assert projection.is_comparable
    assert projection.total_active_quantity == Decimal("2")
    assert projection.active_batches == checkpoint.projection.active_batches
    assert cut.facts.snapshots == (snapshot,)  # Keep the immutable evidence.


async def test_late_exit_boundary_still_requires_recovery():
    checkpoint = _checkpoint()
    at = checkpoint.event_cut - timedelta(seconds=1)
    boundary = ExitOrderSubmissionFact(
        order_id="exit",
        submitted_at=at,
        symbol=_key().symbol,
        position_side=_key().position_side,
    )
    row = _row(
        checkpoint,
        "boundary",
        f"exit:{at.isoformat()}",
        at,
        PositionRecoveryCodec.encode_boundary(boundary),
    )
    cut = await _restore(checkpoint, [row])
    assert cut.facts.fact_conflicts[0].event_kind == "boundary"
    assert not PositionLedger(_key()).project(cut.facts).is_comparable


async def test_late_fill_still_requires_checkpoint_rebuild():
    checkpoint = _checkpoint()
    at = checkpoint.event_cut - timedelta(seconds=1)
    fill = _fill(_key(), "late", "BUY", "1", "10", at)
    row = _row(
        checkpoint,
        "fill",
        fill.trade_id,
        at,
        PositionRecoveryCodec.encode_fill(fill),
    )
    cut = await _restore(checkpoint, [row])
    assert cut.facts.late_fills == (fill,)
    assert not PositionLedger(_key()).project(cut.facts).is_comparable


async def test_divergent_snapshot_identity_still_requires_recovery():
    checkpoint = _checkpoint()
    at = checkpoint.event_cut - timedelta(seconds=1)
    snapshot = _snapshot(_key(), at, "1", "10")
    rows = [
        _row(
            checkpoint,
            "snapshot",
            f"BOTH:{at.isoformat()}",
            at,
            PositionRecoveryCodec.encode_snapshot(item),
        )
        for item in (snapshot, replace(snapshot, position_amt=Decimal("3")))
    ]
    cut = await _restore(checkpoint, rows)
    assert cut.facts.fact_conflicts[0].event_kind == "snapshot"
    assert any(
        "different payloads" in conflict.details
        for conflict in cut.facts.fact_conflicts
    )
    assert not PositionLedger(_key()).project(cut.facts).is_comparable


async def test_snapshot_variant_of_checkpoint_prefix_still_requires_recovery():
    checkpoint = _checkpoint()
    at = checkpoint.event_cut - timedelta(seconds=1)
    snapshot = _snapshot(_key(), at, "1", "10")
    original = _row(
        checkpoint,
        "snapshot",
        f"BOTH:{at.isoformat()}",
        at,
        PositionRecoveryCodec.encode_snapshot(snapshot),
    )
    original.source_revision = checkpoint.source_revision - 1
    original.recorded_at = checkpoint.event_cut - timedelta(seconds=1)
    changed = _row(
        checkpoint,
        "snapshot",
        original.event_id,
        at,
        PositionRecoveryCodec.encode_snapshot(
            replace(snapshot, position_amt=Decimal("3"))
        ),
    )
    cut = await _restore(checkpoint, [original, changed])
    assert any(
        "different payloads" in conflict.details
        for conflict in cut.facts.fact_conflicts
    )
    assert not PositionLedger(_key()).project(cut.facts).is_comparable
