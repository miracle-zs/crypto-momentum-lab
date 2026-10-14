from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
from crypto_momentum_lab.domain.execution.evidence_models import (
    ExecutionCumulativeOrderReport,
    ExecutionEvidence,
)
from crypto_momentum_lab.domain.execution.evidence_rules import (
    _canonical_evidence_payload,
    _evidence_identity,
    content_addressed_evidence_id,
)
from crypto_momentum_lab.domain.execution.evidence_settlement import (
    cumulative_order_delta,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide

NOW = datetime(2026, 9, 30, tzinfo=UTC)


@pytest.fixture
def fill():
    return AccountFillEvent(
        environment="live",
        account_label="account-3",
        symbol="TESTUSDT",
        trade_id="trade-2",
        order_id="order-1",
        side="BUY",
        quantity=Decimal("5"),
        price=Decimal("12"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=NOW,
        raw_payload={"positionSide": "LONG"},
    )


def test_order_report_advances_only_order_progress():
    report = ExecutionCumulativeOrderReport("order-1", Decimal("5"), Decimal("60"), NOW)
    delta = cumulative_order_delta(
        report,
        previous_quantity=Decimal("2"),
        previous_quote=Decimal("20"),
    )
    assert delta.quantity == Decimal("3")
    assert delta.watermark == (Decimal("5"), Decimal("60"))


def test_stale_order_report_does_not_rewind_progress():
    report = ExecutionCumulativeOrderReport("order-1", Decimal("2"), Decimal("20"), NOW)
    delta = cumulative_order_delta(
        report,
        previous_quantity=Decimal("3"),
        previous_quote=Decimal("30"),
    )
    assert delta.quantity == 0 and delta.watermark is None


def test_order_quantity_increase_requires_quote_increase():
    report = ExecutionCumulativeOrderReport("order-1", Decimal("3"), Decimal("20"), NOW)
    with pytest.raises(ValueError, match="did not increase"):
        cumulative_order_delta(
            report,
            previous_quantity=Decimal("2"),
            previous_quote=Decimal("20"),
        )


def test_same_order_quantity_cannot_change_quote():
    report = ExecutionCumulativeOrderReport("order-1", Decimal("2"), Decimal("21"), NOW)
    with pytest.raises(ValueError, match="without quantity"):
        cumulative_order_delta(
            report,
            previous_quantity=Decimal("2"),
            previous_quote=Decimal("20"),
        )


def test_identity_binds_epoch_and_canonical_digest_excludes_transport_observation(fill):
    scope = ExecutionScope("live", "account-3", "TESTUSDT", FuturesPositionSide.LONG)
    evidence = ExecutionEvidence(
        "event-1", scope, NOW, fill=fill, stream_id="hub", stream_epoch="epoch-1"
    )
    redelivery = replace(evidence, observed_at=NOW + timedelta(seconds=1))
    assert _canonical_evidence_payload(evidence) == _canonical_evidence_payload(
        redelivery
    )
    assert _evidence_identity(evidence) == _evidence_identity(redelivery)
    assert _evidence_identity(evidence) != _evidence_identity(
        replace(evidence, stream_epoch="epoch-2")
    )
    assert _canonical_evidence_payload(evidence) != _canonical_evidence_payload(
        replace(evidence, fill=replace(fill, quantity=Decimal("6")))
    )


def test_content_addressed_evidence_id_changes_for_any_canonical_fact(fill):
    scope = ExecutionScope("live", "account-3", "TESTUSDT", FuturesPositionSide.LONG)
    evidence = ExecutionEvidence(
        "temporary-order-id",
        scope,
        NOW,
        fill=fill,
        stream_id="hub",
        stream_epoch="epoch-1",
    )
    assert content_addressed_evidence_id(evidence, prefix="order") == (
        content_addressed_evidence_id(
            replace(evidence, observed_at=NOW + timedelta(seconds=1)),
            prefix="order",
        )
    )
    assert content_addressed_evidence_id(evidence, prefix="order") != (
        content_addressed_evidence_id(
            replace(evidence, fill=replace(fill, quantity=Decimal("6"))),
            prefix="order",
        )
    )
