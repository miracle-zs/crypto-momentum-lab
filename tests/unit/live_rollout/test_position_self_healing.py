"""Unit tests for automated unmanaged position self-healing."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.domain.execution.execution_book import (
    ExecutionBook,
)
from crypto_momentum_lab.domain.execution.observation_models import (
    Applied,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
    OrderExecutionPort,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
)

NOW = datetime(2026, 9, 29, 6, 7, 1, tzinfo=UTC)


class FakeBackend(OrderExecutionPort):
    def __init__(self) -> None:
        self.submitted_plans: list[OrderExecutionPlan] = []

    async def execute_approved_intent(
        self,
        plan: OrderExecutionPlan,
        *,
        prepared_submission=None,
    ) -> OrderExecutionResult:
        self.submitted_plans.append(plan)
        return OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.FILLED,
            exchange_order_id="ex-12345",
            executed_quantity=plan.quantity,
            average_price=Decimal("0.7248"),
            plan=plan,
        )

    async def cancel_order(self, plan: OrderExecutionPlan) -> OrderExecutionResult:
        return OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.CANCELED,
            exchange_order_id="ex-12345",
            plan=plan,
        )

    async def reconcile_order(self, plan: OrderExecutionPlan) -> OrderExecutionResult:
        return OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.FILLED,
            exchange_order_id="ex-12345",
            executed_quantity=plan.quantity,
            average_price=Decimal("0.7248"),
            plan=plan,
        )

    async def apply_observed_snapshot(self, plan, snapshot):
        raise NotImplementedError

    async def mark_absent_reconciled(self, plan, *, details):
        raise NotImplementedError


def _plan(symbol: str = "GRASSUSDT") -> OrderExecutionPlan:
    return OrderExecutionPlan(
        intent_id=f"intent-{symbol}",
        run_id="live-run-1",
        client_order_id=f"cml_{symbol.lower()}_123",
        symbol=symbol,
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("137.6"),
        price=None,
        reduce_only=False,
        position_side=FuturesPositionSide.LONG,
        created_at=NOW,
        quantized=True,
    )


@pytest.mark.asyncio
async def test_coordinator_resolves_active_stream_on_cold_start() -> None:
    backend = FakeBackend()
    book = ExecutionBook(execution_unit_of_work=AsyncMock())
    book._persistence_failed = False
    book.observe = AsyncMock(
        return_value=Applied(evidence_id="ev-mock", updated_view_token="token-1")
    )
    book.register_active_stream(
        environment="live",
        account_label="primary",
        stream_id="account_event_hub",
        stream_epoch="test-epoch-1234",
    )

    coordinator = OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
        environment="live",
        execution_book=book,
    )

    plan = _plan("GRASSUSDT")
    # Submitting an order for a new symbol with no prior restored stream identity
    # must resolve from the active stream rather than raising RuntimeError
    result = await coordinator.submit(plan)
    assert result.state is ExchangeOrderState.FILLED
    assert result.executed_quantity == Decimal("137.6")
    book.observe.assert_called_once()
    evidence = book.observe.call_args[0][0]
    assert evidence.stream_id == "account_event_hub"
    assert evidence.stream_epoch == "test-epoch-1234"
    await coordinator.aclose()


@pytest.mark.asyncio
async def test_execution_book_get_active_stream() -> None:
    book = ExecutionBook()
    book.register_active_stream(
        environment="live",
        account_label="account-2",
        stream_id="account_event_hub",
        stream_epoch="epoch-abc",
    )

    active = book.get_active_stream("live", "account-2")
    assert active == ("account_event_hub", "epoch-abc")

    missing = book.get_active_stream("live", "nonexistent")
    assert missing is None
