"""Serialization compatibility for retained execution-head migrations."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import AccountPositionSnapshot
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    AccountFillLoadProvenance,
    FactCoverageInterval,
    FactCoverageStatus,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_codec import (
    PositionRecoveryCodec,
    RecoverySchemaError,
)
from crypto_momentum_lab.domain.execution.recovery_models import AccountFacts


def test_fact_codec_round_trips_compact_and_legacy_coverage() -> None:
    key = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.BOTH)
    scope = AccountFactStreamScope.for_position_key(
        key,
        stream_id="execution-book",
        stream_epoch="epoch-7",
    )
    cut = datetime(2026, 9, 27, 12, 2, tzinfo=UTC)
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
        source_anchor_kind="zero_snapshot",
    )
    coverage = FactCoverageInterval(
        start_at=cut,
        end_at=cut,
        status=FactCoverageStatus.CONFIRMED,
        stream_scope=scope,
        evidence_observed_at=cut,
        checkpoint_id="flat-bootstrap-1",
        checkpoint_event_cut=cut,
        load_provenance=provenance,
    )
    facts = AccountFacts(
        position_key=key,
        stream_scope=scope,
        snapshots=(snapshot,),
        coverage=coverage,
        fill_load_provenance=provenance,
    )

    encoded_facts = PositionRecoveryCodec.encode_facts(facts)
    assert encoded_facts["schema_version"] == 4
    assert "fill_load_provenance" not in encoded_facts
    assert "page_exhausted" not in encoded_facts["coverage"]
    assert "not_truncated" not in encoded_facts["coverage"]
    assert PositionRecoveryCodec.decode_facts(encoded_facts) == facts

    legacy_facts = dict(encoded_facts)
    legacy_facts["schema_version"] = 3
    legacy_facts["fill_load_provenance"] = (
        PositionRecoveryCodec.encode_fill_load_provenance(provenance)
    )
    legacy_facts["coverage"] = {
        **legacy_facts["coverage"],
        "page_exhausted": True,
        "not_truncated": True,
    }
    assert PositionRecoveryCodec.decode_facts(legacy_facts) == facts

    invalid_compact_facts = dict(encoded_facts)
    invalid_compact_facts["coverage"] = {
        **encoded_facts["coverage"],
        "load_provenance": None,
    }
    with pytest.raises(RecoverySchemaError, match="coverage contains it"):
        PositionRecoveryCodec.decode_facts(invalid_compact_facts)
