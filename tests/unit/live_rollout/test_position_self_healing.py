"""Unit tests for automated unmanaged position self-healing."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from crypto_momentum_lab.domain.execution.execution_book import (
    Applied,
    ExecutionBook,
    ExecutionEvidence,
    ExecutionScope,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
    OrderExecutionPort,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
)
from crypto_momentum_lab.live_rollout.position_self_healing import (
    auto_heal_unmanaged_position,
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


@pytest.mark.asyncio
async def test_auto_heal_returns_false_for_external_position() -> None:
    session = AsyncMock()
    # Scalar query for ExchangeOrderRow returns 0, ExecutionCommandRow returns 0
    session.scalar.return_value = 0

    journal_store = MagicMock()
    healed = await auto_heal_unmanaged_position(
        session=session,
        journal_store=journal_store,
        environment="live",
        account_label="primary",
        symbol="EXTERNALUSDT",
    )
    assert healed is False
    session.commit.assert_not_called()


@pytest.mark.asyncio
async def test_auto_heal_returns_false_when_no_fills_in_account_fill_events() -> None:
    session = AsyncMock()
    # Return 1 for order check (strategy initiated), then epoch, then empty fills
    session.scalar.side_effect = [
        1,  # has_orders
        "epoch-123",  # active_stream_epoch from heads
    ]

    mock_scalars = MagicMock()
    mock_scalars.all.return_value = []
    session.scalars.return_value = mock_scalars

    journal_store = MagicMock()
    healed = await auto_heal_unmanaged_position(
        session=session,
        journal_store=journal_store,
        environment="live",
        account_label="primary",
        symbol="GRASSUSDT",
    )
    assert healed is False
    session.commit.assert_not_called()


@pytest.mark.asyncio
async def test_auto_heal_success() -> None:
    session = AsyncMock()
    session.add = MagicMock()
    # 1. has_commands: 1
    # 2. active_stream_epoch: "epoch-123"
    # 3. existing_trade: None
    # 4. existing_ev: None
    # 5. head: None
    session.scalar.side_effect = [
        1,  # has_commands
        "epoch-123",  # stream epoch
        None,  # existing trade identity
        None,  # existing fact event
        None,  # existing head
    ]

    fill_row = MagicMock()
    fill_row.environment = "live"
    fill_row.account_label = "primary"
    fill_row.trade_id = "trade-1"
    fill_row.order_id = "order-1"
    fill_row.symbol = "GRASSUSDT"
    fill_row.side = "BUY"
    fill_row.price = Decimal("0.7248")
    fill_row.quantity = Decimal("137.6")
    fill_row.realized_pnl = Decimal("0")
    fill_row.fee = Decimal("0.05")
    fill_row.fee_asset = "USDT"
    fill_row.trade_at = NOW
    fill_row.raw_payload = {"mock": True, "positionSide": "LONG"}

    mock_scalars = MagicMock()
    mock_scalars.all.side_effect = [
        [fill_row],  # fill_rows
        [],  # cmd_rows
    ]
    session.scalars.return_value = mock_scalars

    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="GRASSUSDT",
        position_side=FuturesPositionSide.LONG,
    )
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="account_event_hub", stream_epoch="epoch-123"
    )
    from crypto_momentum_lab.domain.account import AccountFillEvent
    from crypto_momentum_lab.domain.execution.position_ledger_models import AccountFacts
    from crypto_momentum_lab.domain.execution.recovery_models import DurableJournalCut

    fill = AccountFillEvent(
        environment="live",
        account_label="primary",
        trade_id="trade-1",
        order_id="order-1",
        symbol="GRASSUSDT",
        side="BUY",
        price=Decimal("0.7248"),
        quantity=Decimal("137.6"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0.05"),
        fee_asset="USDT",
        trade_at=NOW,
        raw_payload={"mock": True, "positionSide": "LONG"},
    )
    facts = AccountFacts(
        position_key=key,
        stream_scope=scope,
        fills=(fill,),
    )
    cut = DurableJournalCut(
        scope=scope,
        facts=facts,
        revision=1,
        as_of=NOW,
    )

    journal_store = MagicMock()
    journal_store.load_recovery_in_session = AsyncMock(return_value=cut)

    healed = await auto_heal_unmanaged_position(
        session=session,
        journal_store=journal_store,
        environment="live",
        account_label="primary",
        symbol="GRASSUSDT",
    )
    assert healed is True
    session.commit.assert_awaited_once()
    assert session.add.call_count == 3

