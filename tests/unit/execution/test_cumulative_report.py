from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.execution.command_models import (
    ExecutionScope,
    OutboxEntry,
)
from crypto_momentum_lab.domain.execution.cumulative_report import (
    plan_cumulative_report,
)
from crypto_momentum_lab.domain.execution.evidence_models import (
    ExecutionCumulativeOrderReport,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.trade_command import (
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

NOW = datetime(2026, 9, 30, tzinfo=UTC)
SCOPE = ExecutionScope("live", "account-3", "TESTUSDT", FuturesPositionSide.LONG)


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


def report(quantity="6", quote="60", order_id="order"):
    return ExecutionCumulativeOrderReport(
        order_id, Decimal(quantity), Decimal(quote), NOW
    )


def plan(
    current=Decimal("0"), current_quote=Decimal("0"), *,
    observed=None, outbox=None, reserved=False,
):
    return plan_cumulative_report(
        observed or report(),
        previous_watermark=(current, current_quote),
        outbox=outbox,
        has_active_reservations=reserved,
    )


def test_cumulative_order_reports_advance_one_order_watermark():
    first = plan(observed=report("3", "30"), outbox=entry())
    repeated = plan(
        Decimal("3"), Decimal("30"), observed=report("3", "30"), outbox=entry()
    )
    later = plan(
        Decimal("3"), Decimal("30"), observed=report("5", "50"), outbox=entry()
    )
    assert first.settlement_quantity == Decimal("3")
    assert first.watermark == (Decimal("3"), Decimal("30"))
    assert repeated.settlement_quantity == 0 and repeated.watermark is None
    assert later.settlement_quantity == Decimal("2")
    assert later.watermark == (Decimal("5"), Decimal("50"))


@pytest.mark.parametrize(
    "reduce_only,reserved,expected",
    [(True, False, "6"), (False, True, "6"), (False, False, "0")],
)
def test_only_reservation_orders_consume_reservation_quantity(
    reduce_only, reserved, expected
):
    result = plan(outbox=entry(reduce_only), reserved=reserved)
    assert result.settlement_quantity == Decimal(expected)
    assert result.reported_quantity == Decimal("6")
    assert result.watermark == (Decimal("6"), Decimal("60"))


def test_report_without_local_command_can_settle_external_reservation():
    result = plan(outbox=None, reserved=True)
    assert result.settlement_quantity == Decimal("6")


def test_nonadvancing_report_does_not_rewind_watermark():
    result = plan(Decimal("7"), Decimal("70"), outbox=entry())
    assert result.settlement_quantity == 0 and result.watermark is None


def test_quote_must_increase_with_order_quantity():
    with pytest.raises(ValueError, match="did not increase"):
        plan(Decimal("2"), Decimal("100"), observed=report("3", "100"), reserved=True)
