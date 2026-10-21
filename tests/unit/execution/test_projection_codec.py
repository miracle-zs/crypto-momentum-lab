"""Strict projection encoding with persisted opening-order identities."""

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

import crypto_momentum_lab.domain.execution.projection_codec as projection_codec
from crypto_momentum_lab.domain.execution.recovery_codec import (
    PositionRecoveryCodec,
    RecoverySchemaError,
)


@pytest.fixture
def baseline():
    return json.loads(
        (Path(__file__).parent / "fixtures" / "projection_v4.json").read_text()
    )


def test_projection_encoding_matches_order_identity_baseline(baseline):
    projection = PositionRecoveryCodec.decode_projection(baseline["payload"])
    assert projection_codec.encode_projection(projection) == baseline["payload"]
    assert projection.active_batches
    assert projection.archived_episodes[0].reductions
    assert projection.active_episode.reductions


def test_legacy_projection_without_order_count_evidence_is_rejected():
    old = json.loads(
        (Path(__file__).parent / "fixtures" / "projection_v3.json").read_text()
    )
    with pytest.raises(RecoverySchemaError, match="entry_order_ids"):
        PositionRecoveryCodec.decode_projection(old["payload"])


def test_decimal_scale_and_timezone_encoding_remain_exact(baseline):
    projection = PositionRecoveryCodec.decode_projection(baseline["payload"])
    event_cut = datetime(2026, 9, 30, 12, tzinfo=timezone(timedelta(hours=8)))
    changed = replace(
        projection, unallocated_quantity=Decimal("2.000"), event_cut=event_cut
    )
    payload = projection_codec.encode_projection(changed)
    assert payload["unallocated_quantity"] == "2.000"
    assert payload["event_cut"] == "2026-09-30T12:00:00+08:00"
    assert PositionRecoveryCodec.decode_projection(payload) == changed


def test_current_projection_encoding_omits_derived_fields(baseline):
    projection = PositionRecoveryCodec.decode_projection(baseline["payload"])
    payload = projection_codec.encode_projection(projection, include_derived=False)

    assert "active_batches" not in payload
    assert "total_active_quantity" not in payload
    assert PositionRecoveryCodec.decode_projection(payload) == projection


def test_naive_datetime_remains_rejected(baseline):
    projection = PositionRecoveryCodec.decode_projection(baseline["payload"])
    with pytest.raises(ValueError, match="timezone-aware"):
        projection_codec.encode_projection(
            replace(projection, event_cut=datetime(2026, 9, 30))
        )


def test_projection_rejects_partial_legacy_derived_fields(baseline):
    payload = dict(baseline["payload"])
    del payload["active_batches"]

    with pytest.raises(RecoverySchemaError, match="fields differ from schema"):
        PositionRecoveryCodec.decode_projection(payload)
