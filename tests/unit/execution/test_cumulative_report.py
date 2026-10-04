
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.command_models import (
    ExecutionScope,
    OutboxEntry,
)
from crypto_momentum_lab.domain.execution.cumulative_report import (
    plan_cumulative_report,
    plan_watermark_publication,
)
from crypto_momentum_lab.domain.execution.evidence_models import (
    ExecutionCumulativeOrderReport,
    ExecutionEvidence,
)
from crypto_momentum_lab.domain.execution.evidence_settlement import account_trade_delta
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    FuturesPositionSide,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

NOW = datetime(2026, 9, 30, tzinfo=UTC)
SCOPE = ExecutionScope("live", "account-3", "TESTUSDT", FuturesPositionSide.LONG)


def fill(quantity="6", *, order_id="order", cumulative=False):
    return AccountFillEvent(
        environment="live",
        account_label="account-3",
        symbol="TESTUSDT",
        trade_id="trade-" + order_id,
        order_id=order_id,
        side="SELL",
        quantity=Decimal(quantity),
        price=Decimal("10"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=NOW,
        raw_payload={"is_cumulative": cumulative},
    )


def entry(reduce_only=True):
    command = TradeCommand(
        "order",
        SCOPE.to_position_key(),
        TradeCommandType.EXIT,
        StrategySide.LONG,
        EntryType.MARKET,
        Decimal("10"),
        reduce_only=reduce_only,
        created_at=NOW,
    )
    return OutboxEntry(
        "order", "request", SCOPE, command, created_at=NOW, updated_at=NOW
    )


def report(order_id="order"):
    return ExecutionCumulativeOrderReport(order_id, Decimal("6"), Decimal("60"), NOW)


def plan(
    previous=(Decimal("0"), Decimal("0")), *, fills=(), outbox=None, reserved=False
):
    return plan_cumulative_report(
        report(),
        previous_watermark=previous,
        account_fills=fills,
        outbox=outbox,
        has_active_reservations=reserved,
    )


@pytest.mark.parametrize("report_first", [False, True])
def test_report_and_real_trade_delivery_orders_settle_total_once(report_first):
    observed = fill()
    commands = entry()
    if report_first:
        first = plan(outbox=commands)
        second = account_trade_delta(
            "order",
            previous_quantity=first.watermark[0],
            previous_quote=first.watermark[1],
            account_fills=(observed,),
        )
        total = first.settlement_quantity + second.quantity
    else:
        first = account_trade_delta(
            "order",
            previous_quantity=Decimal("0"),
            previous_quote=Decimal("0"),
            account_fills=(observed,),
        )
        second = plan(previous=first.watermark, fills=(observed,), outbox=commands)
        total = first.quantity + second.settlement_quantity
    assert total == Decimal("6")
    assert second.watermark is None
    assert observed.quantity == Decimal("6")


@pytest.mark.parametrize(
    "reduce_only,reserved,expected",
    [(True, False, "6"), (False, True, "6"), (False, False, "0")],
)
def test_only_exit_authority_settles_reservations_but_entry_report_advances_watermark(
    reduce_only, reserved, expected
):
    result = plan(outbox=entry(reduce_only), reserved=reserved)
    assert result.settlement_quantity == Decimal(expected)
    assert result.reported_quantity == Decimal("6")
    assert result.watermark == (Decimal("6"), Decimal("60"))


def test_real_trades_above_report_take_precedence_and_other_orders_are_ignored():
    result = plan(fills=(fill("8"), fill("100", order_id="other")), reserved=True)
    assert result.watermark == (Decimal("8"), Decimal("80"))
    assert result.settlement_quantity == Decimal("8")


def test_nonadvancing_report_has_no_settlement_or_watermark():
    result = plan(previous=(Decimal("7"), Decimal("70")), outbox=entry())
    assert result.settlement_quantity == 0 and result.watermark is None


def test_quote_conflict_is_not_converted_to_noop():
    with pytest.raises(ValueError, match="quote watermark did not advance"):
        plan(previous=(Decimal("2"), Decimal("100")), reserved=True)


@pytest.mark.parametrize(
    "source,expected",
    [
        ("report", "report-order"),
        ("event", "event-order"),
        ("fill", "fill-order"),
        ("fills", "fills-order"),
        ("empty", ""),
    ],
)
def test_publication_preserves_evidence_command_identity_precedence(source, expected):
    values = {}
    if source == "fills":
        values["fills"] = (fill(order_id="fills-order"),)
    if source in {"report", "event", "fill"}:
        values["fill"] = fill(order_id="fill-order")
    if source in {"report", "event"}:
        values["order_event"] = ExchangeOrderEvent(
            "event",
            "event-order",
            ExchangeOrderState.FILLED,
            NOW,
            None,
            {},
        )
    if source == "report":
        values["cumulative_order"] = report("report-order")
    evidence = ExecutionEvidence("evidence", SCOPE, NOW, **values)
    result = plan_watermark_publication(evidence, outbox_by_command_id={})
    assert result.command_id == expected
    assert result.recovery_diagnostic is None


def test_missing_outbox_for_cumulative_fill_requires_recovery():
    evidence = ExecutionEvidence("evidence", SCOPE, NOW, fill=fill(cumulative=True))
    result = plan_watermark_publication(evidence, outbox_by_command_id={})
    assert result.outbox is None
    assert (
        result.recovery_diagnostic
        == "No outbox command exists for cumulative fill order"
    )


def test_publication_returns_latest_outbox_after_lifecycle_transition():
    evidence = ExecutionEvidence("evidence", SCOPE, NOW, fill=fill(cumulative=True))
    latest = replace(entry(), last_error="latest")
    commands = {"order": latest}
    result = plan_watermark_publication(evidence, outbox_by_command_id=commands)
    assert result.outbox is latest and result.recovery_diagnostic is None
    assert commands == {"order": latest}


def test_report_alone_does_not_invent_missing_cumulative_fill():
    evidence = ExecutionEvidence("evidence", SCOPE, NOW, cumulative_order=report())
    result = plan_watermark_publication(evidence, outbox_by_command_id={})
    assert result.command_id == "order" and result.recovery_diagnostic is None
