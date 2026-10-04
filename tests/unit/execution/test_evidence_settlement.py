"""Evidence calculation acceptance without a Book or persistence."""

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
)
from crypto_momentum_lab.domain.execution.evidence_settlement import (
    cumulative_fill_delta,
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
        raw_payload={
            "positionSide": "LONG",
            "is_cumulative": True,
            "cum_qty": "5",
            "cum_quote": "60",
        },
    )


def test_cumulative_fill_computes_increment_price_without_mutating_original(fill):
    delta = cumulative_fill_delta(
        fill,
        previous_quantity=Decimal("2"),
        previous_quote=Decimal("20"),
        trade_seen=False,
    )
    assert delta.quantity == Decimal("3")
    assert delta.fill.quantity == Decimal("3") and delta.fill.price == Decimal(
        "40"
    ) / Decimal("3")
    assert delta.watermark == (Decimal("5"), Decimal("60"))
    assert fill.quantity == Decimal("5") and fill.price == Decimal("12")


@pytest.mark.parametrize("previous,quote", [("6", "70"), ("5", "60")])
def test_old_or_same_report_cannot_rewind_watermark(fill, previous, quote):
    delta = cumulative_fill_delta(
        fill,
        previous_quantity=Decimal(previous),
        previous_quote=Decimal(quote),
        trade_seen=True,
    )
    assert delta.quantity == 0 and delta.watermark is None


@pytest.mark.parametrize(
    "previous,quote,seen,reason",
    [
        ("5", "50", False, "without a quantity change"),
        ("2", "70", False, "did not increase"),
        ("2", "20", True, "was reused"),
    ],
)
def test_inconsistent_cumulative_fill_reports_conflict(
    fill, previous, quote, seen, reason
):
    with pytest.raises(ValueError, match=reason):
        cumulative_fill_delta(
            fill,
            previous_quantity=Decimal(previous),
            previous_quote=Decimal(quote),
            trade_seen=seen,
        )


def test_nonfinite_cumulative_fill_is_rejected(fill):
    malformed = replace(fill, raw_payload={"cum_qty": "NaN", "cum_quote": "60"})
    with pytest.raises(ValueError, match="invalid"):
        cumulative_fill_delta(
            malformed,
            previous_quantity=Decimal("0"),
            previous_quote=Decimal("0"),
            trade_seen=False,
        )


def test_order_report_uses_only_matching_real_fills_and_returns_settlement_watermark(
    fill,
):
    report = ExecutionCumulativeOrderReport("order-1", Decimal("2"), Decimal("20"), NOW)
    unrelated = replace(fill, order_id="other-order", quantity=Decimal("100"))
    delta = cumulative_order_delta(
        report,
        previous_quantity=Decimal("1"),
        previous_quote=Decimal("10"),
        account_fills=(fill, unrelated),
    )
    assert delta.quantity == Decimal("4")
    assert delta.watermark == (Decimal("5"), Decimal("60"))
    assert fill.quantity == Decimal("5")


def test_stale_order_report_returns_no_settlement_delta():
    report = ExecutionCumulativeOrderReport("order-1", Decimal("2"), Decimal("20"), NOW)
    delta = cumulative_order_delta(
        report,
        previous_quantity=Decimal("3"),
        previous_quote=Decimal("30"),
        account_fills=(),
    )
    assert delta.quantity == 0 and delta.watermark is None


def test_order_quantity_increase_requires_quote_increase():
    report = ExecutionCumulativeOrderReport("order-1", Decimal("3"), Decimal("20"), NOW)
    with pytest.raises(ValueError, match="did not advance"):
        cumulative_order_delta(
            report,
            previous_quantity=Decimal("2"),
            previous_quote=Decimal("20"),
            account_fills=(),
        )


def test_identity_binds_epoch_and_canonical_digest_excludes_transport_observation(fill):
    scope = ExecutionScope("live", "account-3", "TESTUSDT", FuturesPositionSide.LONG)
    evidence = ExecutionEvidence(
        "event-1", scope, NOW, fill=fill, stream_id="hub", stream_epoch="epoch-1"
    )
    redelivery = replace(evidence, observed_at=NOW + timedelta(seconds=1))
    # Adding an optional settlement contract cannot invalidate old receipt hashes.
    assert "settlement_fills" not in _canonical_evidence_payload(evidence)
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
