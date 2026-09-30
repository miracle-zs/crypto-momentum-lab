from dataclasses import replace
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.execution.durable_evidence import (
    DurableEvidenceConflict,
    changed_order_watermarks,
    prepare_durable_evidence,
)
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    FactCoverageInterval,
    FactCoverageStatus,
)
from tests.unit.execution.test_cumulative_report import NOW, SCOPE, fill, report


def evidence(**kwargs):
    return ExecutionEvidence(
        "evidence", SCOPE, NOW, stream_id="hub", stream_epoch="epoch", **kwargs
    )


def test_durable_input_accepts_report_without_manufacturing_trade():
    source = evidence(cumulative_order=report())
    result = prepare_durable_evidence(source)
    assert result.evidence is source
    assert result.scope.stream_id == "hub" and result.scope.stream_epoch == "epoch"
    assert result.evidence.fill is None and result.evidence.fills == ()


def test_durable_input_requires_stream_identity():
    with pytest.raises(DurableEvidenceConflict, match="stream identity"):
        prepare_durable_evidence(ExecutionEvidence("evidence", SCOPE, NOW))


@pytest.mark.parametrize("raw", [{"is_cumulative": True}, {"cum_qty": "0"}])
def test_cumulative_fill_transport_cannot_enter_durable_trade_journal(raw):
    with pytest.raises(DurableEvidenceConflict, match="use cumulative_order"):
        prepare_durable_evidence(evidence(fill=replace(fill(), raw_payload=raw)))


def test_confirmed_live_coverage_requires_typed_provenance():
    scope = AccountFactStreamScope.for_position_key(
        SCOPE.to_position_key(), stream_id="hub", stream_epoch="epoch"
    )
    interval = FactCoverageInterval(NOW, NOW, stream_scope=scope)
    with pytest.raises(DurableEvidenceConflict, match="typed pagination provenance"):
        prepare_durable_evidence(evidence(coverage=interval))


def test_pending_coverage_is_not_promoted_to_confirmed():
    interval = FactCoverageInterval(NOW, NOW, status=FactCoverageStatus.PENDING)
    result = prepare_durable_evidence(evidence(coverage=interval))
    assert result.evidence.coverage.status is FactCoverageStatus.PENDING


def test_invalid_empty_stream_keeps_validation_exception_not_source_rejection():
    source = replace(evidence(), stream_id="")
    with pytest.raises(ValueError) as caught:
        prepare_durable_evidence(source)
    assert not isinstance(caught.value, DurableEvidenceConflict)


def test_changed_watermarks_filter_position_sort_orders_and_keep_inputs_unchanged():
    key = SCOPE.to_position_key()
    prefix = key.canonical_id + "\x1f"
    before = {prefix + "same": Decimal("2"), prefix + "quote": Decimal("2")}
    after = {
        prefix + "z": Decimal("3"),
        prefix + "a": Decimal("1"),
        prefix + "same": Decimal("2"),
        prefix + "quote": Decimal("2"),
        "other-position\x1forder": Decimal("99"),
    }
    before_quotes = {prefix + "quote": Decimal("20")}
    after_quotes = {prefix + "quote": Decimal("21"), prefix + "z": Decimal("30")}
    result = changed_order_watermarks(
        key,
        before_quantities=before,
        before_quotes=before_quotes,
        after_quantities=after,
        after_quotes=after_quotes,
        observed_at=NOW,
    )
    assert [item.order_id for item in result] == ["a", "quote", "z"]
    assert [item.cumulative_quote for item in result] == [
        Decimal("0"),
        Decimal("21"),
        Decimal("30"),
    ]
    assert all(item.updated_at == NOW for item in result)
    assert after[prefix + "same"] == Decimal("2") and prefix + "z" not in before


def test_unchanged_or_removed_quantity_keys_do_not_emit_writes():
    key = SCOPE.to_position_key()
    prefix = key.canonical_id + "\x1f"
    result = changed_order_watermarks(
        key,
        before_quantities={prefix + "removed": Decimal("2")},
        before_quotes={},
        after_quantities={prefix + "zero": Decimal("0")},
        after_quotes={prefix + "quote-only": Decimal("10")},
        observed_at=NOW,
    )
    assert result == ()
