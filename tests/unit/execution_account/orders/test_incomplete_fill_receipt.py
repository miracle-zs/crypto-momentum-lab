from crypto_momentum_lab.domain.strategy import StrategySide
from tests.fixtures.prepared_submission import submit_prepared

"""Real REST -> state machine -> Book regression for delayed execution prices."""

from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from crypto_momentum_lab.domain.account.models import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.command_models import (
    DispatchState,
    ExecutionScope,
)
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.execution_book import (
    ExecutionBook,
    ExecutionRequest,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    FactCoverageInterval,
    FactCoverageStatus,
)
from crypto_momentum_lab.domain.execution.trade_command import TradeCommandType
from crypto_momentum_lab.execution_account.binance.client import BinanceUsdMTradeClient
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionStateMachine,
)

NOW = datetime(2026, 10, 2, 12, 30, tzinfo=UTC)


class EventRepository:
    def __init__(self):
        self.events = []


    async def record_order_observation(self, event, fills=()):
        self.events.append(event)
        return True

    async def save_fill(self, fill):
        return True


@pytest.mark.parametrize(
    "order_type,limit_price", [("MARKET", None), ("LIMIT", Decimal("0.0155"))]
)
async def test_incomplete_fill_waits_for_price_without_crash_or_second_post(
    order_type, limit_price
):
    book = ExecutionBook()
    scope = ExecutionScope("live", "account-4", "TRUTHUSDT", FuturesPositionSide.LONG)
    journal = book._ensure_journal(scope.to_position_key())
    journal.set_coverage(
        FactCoverageInterval(NOW, NOW, status=FactCoverageStatus.CONFIRMED)
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="seed",
            scope=scope,
            observed_at=NOW,
            fill=AccountFillEvent(
                environment="live",
                account_label="account-4",
                symbol="TRUTHUSDT",
                trade_id="seed",
                order_id="seed-entry",
                side="BUY",
                price=Decimal("0.0154"),
                quantity=Decimal("6526"),
                realized_pnl=Decimal(0),
                fee=Decimal(0),
                fee_asset="USDT",
                trade_at=NOW,
                raw_payload={"positionSide": "LONG", "is_system": True},
            ),
            snapshot=AccountPositionSnapshot(
                environment="live",
                account_label="account-4",
                symbol="TRUTHUSDT",
                position_side="LONG",
                position_amt=Decimal("6526"),
                entry_price=Decimal("0.0154"),
                mark_price=Decimal("0.0154"),
                unrealized_pnl=Decimal(0),
                notional=Decimal("100.5"),
                leverage=5,
                margin_type="cross",
                observed_at=NOW,
                raw_payload={},
            ),
        )
    )
    view = await book.read(scope)
    plan = OrderExecutionPlan(
        intent_id="exit",
        run_id="run",
        client_order_id="cml_incomplete_fill",
        symbol="TRUTHUSDT",
        side="SELL",
        order_type=order_type,
        quantity=Decimal("6526"),
        price=limit_price,
        time_in_force="GTC" if order_type == "LIMIT" else None,
        reduce_only=True,
        position_side=FuturesPositionSide.LONG,
        created_at=NOW,
        quantized=True,
        projection_version=view.projection_version,
    )
    accepted = await book.act(
        ExecutionRequest(
            request_id=plan.client_order_id,
            scope=scope,
            strategy_name="trend_v1",
            run_id="run",
            decision_ref="exit",
            expected_view_token=view.projection_version,
            action=TradeCommandType.EXIT,
            requested_quantity=plan.quantity,
            side=StrategySide.LONG,
        )
    )
    assert book.get_outbox(plan.client_order_id) is not None, getattr(
        accepted, "diagnostics", accepted
    )
    posts = []
    price_ready = False

    def handler(request):
        if request.method == "POST":
            posts.append(request.url.path)
        return httpx.Response(
            200,
            json={
                "clientOrderId": plan.client_order_id,
                "orderId": 99999,
                "status": "FILLED",
                "executedQty": "6526",
                "avgPrice": "0.0154" if price_ready else "0",
                "cumQuote": "100.5004" if price_ready else "0",
            },
        )

    repository = EventRepository()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://fapi.binance.com"
    ) as http:
        exchange = BinanceUsdMTradeClient(
            api_key="test",
            api_secret="test",
            environment="live",
            account_label="account-4",
            live_submit_enabled=True,
            http_client=http,
            clock=lambda: NOW,
        )
        machine = OrderExecutionStateMachine(
            exchange=exchange,
            event_repository=repository,
            live_submit_enabled=True,
            clock=lambda: NOW,
        )
        coordinator = OrderExecutionCoordinator(
            backend=machine,
            execution_book=book,
            environment="live",
            account_label="account-4",
        )
        try:
            result = await submit_prepared(coordinator, plan)
            assert result.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
            assert result.executed_quantity == plan.quantity
            assert result.average_price == 0
            assert (
                repository.events[-1].details["reason"]
                == "cumulative_fill_price_pending"
            )
            assert book.get_outbox(plan.client_order_id).state is DispatchState.UNKNOWN
            assert book.get_active_reservations(scope.to_position_key())
            assert not any(
                e.state is ExchangeOrderState.FILLED for e in repository.events
            )
            result = await coordinator.reconcile_order(plan)
            assert result.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
            price_ready = True
            result = await coordinator.reconcile_order(plan)
            assert result.state is ExchangeOrderState.FILLED
            assert result.average_price == Decimal("0.0154")
            assert book.get_outbox(plan.client_order_id).state is DispatchState.TERMINAL
            assert not book.get_active_reservations(scope.to_position_key())
            assert posts == ["/fapi/v1/order"]
        finally:
            await coordinator.aclose()
            await exchange.aclose()
