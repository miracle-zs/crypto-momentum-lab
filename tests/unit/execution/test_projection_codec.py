"""Compatibility against a projection captured before the codec extraction."""

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

import crypto_momentum_lab.domain.execution.projection_codec as projection_codec
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec


@pytest.fixture
def baseline():
    return json.loads(
        (Path(__file__).parent / "fixtures" / "projection_v3.json").read_text()
    )


def test_existing_projection_encoding_and_digest_are_unchanged(baseline):
    projection = PositionRecoveryCodec.decode_projection(baseline["payload"])
    assert projection_codec.encode_projection(projection) == baseline["payload"]
    assert projection_codec.compute_projection_digest(projection) == baseline["digest"]
    assert (
        PositionRecoveryCodec.compute_projection_digest(projection)
        == baseline["digest"]
    )
    assert projection.active_batches
    assert projection.archived_episodes[0].reductions
    assert projection.active_episode.reductions


def test_fact_token_is_excluded_from_digest_without_mutating_projection(baseline):
    projection = PositionRecoveryCodec.decode_projection(baseline["payload"])
    changed = replace(projection, projection_version="another-fact-token")
    assert projection_codec.compute_projection_digest(changed) == baseline["digest"]
    assert (
        projection_codec.encode_projection(changed)["projection_version"]
        == "another-fact-token"
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("total_active_quantity", Decimal("7.00")),
        ("diagnostics", ("different",)),
        ("stream_scope", None),
        ("archived_episodes", ()),
    ],
)
def test_materialized_state_changes_remain_bound_to_digest(baseline, field, value):
    projection = PositionRecoveryCodec.decode_projection(baseline["payload"])
    changed = replace(projection, **{field: value})
    assert projection_codec.compute_projection_digest(changed) != baseline["digest"]


def test_decimal_scale_and_timezone_encoding_remain_exact(baseline):
    projection = PositionRecoveryCodec.decode_projection(baseline["payload"])
    event_cut = datetime(2026, 9, 30, 12, tzinfo=timezone(timedelta(hours=8)))
    changed = replace(
        projection, total_active_quantity=Decimal("2.000"), event_cut=event_cut
    )
    payload = projection_codec.encode_projection(changed)
    assert payload["total_active_quantity"] == "2.000"
    assert payload["event_cut"] == "2026-09-30T12:00:00+08:00"
    assert PositionRecoveryCodec.decode_projection(payload) == changed


def test_naive_datetime_remains_rejected(baseline):
    projection = PositionRecoveryCodec.decode_projection(baseline["payload"])
    with pytest.raises(ValueError, match="timezone-aware"):
        projection_codec.encode_projection(
            replace(projection, event_cut=datetime(2026, 9, 30))
        )
