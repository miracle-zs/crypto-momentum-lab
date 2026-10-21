"""Serialization compatibility for retained 0062 migration payloads."""

from datetime import UTC, datetime

import pytest

from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionKey,
)
from crypto_momentum_lab.persistence.postgres.execution_head_migration_codec import (
    PositionRecoveryCodec,
    RecoverySchemaError,
)
from crypto_momentum_lab.persistence.postgres.execution_head_migration_projection import (
    AccountFactStreamScope,
    AccountFillLoadProvenance,
    FactCoverageInterval,
    FactCoverageStatus,
)


def test_coverage_codec_accepts_compact_and_legacy_fields() -> None:
    key = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.BOTH)
    scope = AccountFactStreamScope.for_position_key(
        key,
        stream_id="execution-book",
        stream_epoch="epoch-7",
    )
    cut = datetime(2026, 9, 27, 12, 2, tzinfo=UTC)
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
        source_anchor_id="flat-bootstrap-1",
        source_anchor_event_cut=cut,
    )
    coverage = FactCoverageInterval(
        start_at=cut,
        end_at=cut,
        status=FactCoverageStatus.CONFIRMED,
        stream_scope=scope,
        evidence_observed_at=cut,
        load_provenance=provenance,
    )

    encoded = PositionRecoveryCodec.encode_coverage(coverage)
    assert "page_exhausted" not in encoded
    assert "not_truncated" not in encoded
    assert "checkpoint_id" not in encoded
    assert "checkpoint_event_cut" not in encoded
    assert "source_anchor_kind" not in encoded["load_provenance"]
    assert PositionRecoveryCodec.decode_coverage(encoded) == coverage

    legacy_provenance = {
        **encoded["load_provenance"],
        "source_anchor_kind": "recovery_checkpoint",
    }
    legacy = {
        **encoded,
        "page_exhausted": True,
        "not_truncated": True,
        "checkpoint_id": "legacy-checkpoint",
        "checkpoint_event_cut": cut.isoformat(),
        "load_provenance": legacy_provenance,
    }
    decoded_legacy = PositionRecoveryCodec.decode_coverage(legacy)
    assert decoded_legacy == coverage
    assert PositionRecoveryCodec.encode_coverage(decoded_legacy) == encoded

    malformed = dict(encoded)
    malformed.pop("status")
    with pytest.raises(RecoverySchemaError, match="fields differ from schema"):
        PositionRecoveryCodec.decode_coverage(malformed)
