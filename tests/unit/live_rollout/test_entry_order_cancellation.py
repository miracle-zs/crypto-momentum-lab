from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import AccountOpenOrderSnapshot
from crypto_momentum_lab.domain.execution import (
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
)
from crypto_momentum_lab.live_rollout.entry_order_cancellation import (
    LiveEntryOrderCanceller,
    external_open_order_cancellation_plan,
)

NOW = datetime(2026, 9, 12, 12, 0, tzinfo=UTC)


def test_external_open_order_plan_preserves_exchange_cancellation_details() -> None:
    order = _open_order(
        order_id="exchange-1",
        client_order_id="orphan-1",
        original_quantity=Decimal("0.5"),
        executed_quantity=Decimal("0.1"),
        price=Decimal("123.4"),
        raw_payload={"positionSide": "LONG", "timeInForce": "gtc"},
    )

    plan = external_open_order_cancellation_plan(order, run_id="run-1")

    assert plan.intent_id == "orphan-cancel:orphan-1"
    assert plan.quantity == Decimal("0.4")
    assert plan.price == Decimal("123.4")
    assert plan.reduce_only is False
    assert plan.position_side is FuturesPositionSide.LONG
    assert plan.time_in_force == "GTC"
    assert plan.created_at == NOW


@pytest.mark.asyncio
async def test_canceller_cancels_known_and_adopts_only_open_entry_orphans() -> None:
    known_plan = _plan("known-1")
    orphan = _open_order(
        order_id="exchange-orphan",
        client_order_id="orphan-1",
        original_quantity=Decimal("0.5"),
        executed_quantity=Decimal("0.1"),
    )
    known_exchange_order = _open_order(
        order_id="exchange-known",
        client_order_id=known_plan.client_order_id,
    )
    reduce_only = _open_order(
        order_id="exchange-exit",
        client_order_id="exit-1",
        reduce_only=True,
    )
    cancelled: list[OrderExecutionPlan] = []
    adopted: list[tuple[OrderExecutionPlan, str, datetime]] = []

    class Exchange:
        async def fetch_open_orders(
            self,
        ) -> tuple[AccountOpenOrderSnapshot, ...]:
            return (known_exchange_order, orphan, reduce_only)

    class StateMachine:
        async def cancel_order(
            self,
            plan: OrderExecutionPlan,
        ) -> OrderExecutionResult:
            cancelled.append(plan)
            return OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.CANCELED,
                exchange_order_id=f"exchange-{plan.client_order_id}",
            )

    class Repository:
        async def adopt_external_order_for_cancellation(
            self,
            plan: OrderExecutionPlan,
            *,
            exchange_order_id: str,
            observed_at: datetime,
        ) -> None:
            adopted.append((plan, exchange_order_id, observed_at))

    canceller = LiveEntryOrderCanceller(
        exchange=Exchange(),
        state_machine=StateMachine(),  # type: ignore[arg-type]
        repository=Repository(),
        run_id="run-1",
    )

    assert await canceller.cancel((known_plan,)) == 2
    assert [plan.client_order_id for plan in cancelled] == [
        "known-1",
        "orphan-1",
    ]
    assert len(adopted) == 1
    adopted_plan, exchange_order_id, observed_at = adopted[0]
    assert adopted_plan.client_order_id == "orphan-1"
    assert exchange_order_id == "exchange-orphan"
    assert observed_at == NOW


@pytest.mark.asyncio
async def test_canceller_fails_when_exchange_cancellation_is_not_confirmed() -> None:
    plan = _plan("known-1")

    class Exchange:
        async def fetch_open_orders(
            self,
        ) -> tuple[AccountOpenOrderSnapshot, ...]:
            return ()

    class StateMachine:
        async def cancel_order(
            self,
            _plan: OrderExecutionPlan,
        ) -> OrderExecutionResult:
            return OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.ACKNOWLEDGED,
                exchange_order_id="exchange-1",
            )

    class Repository:
        async def adopt_external_order_for_cancellation(
            self,
            _plan: OrderExecutionPlan,
            *,
            exchange_order_id: str,
            observed_at: datetime,
        ) -> None:
            raise AssertionError("adoption should not run for known orders")

    canceller = LiveEntryOrderCanceller(
        exchange=Exchange(),
        state_machine=StateMachine(),  # type: ignore[arg-type]
        repository=Repository(),
        run_id="run-1",
    )

    with pytest.raises(RuntimeError, match="known opening order cancellation"):
        await canceller.cancel((plan,))


def _open_order(
    *,
    order_id: str,
    client_order_id: str,
    original_quantity: Decimal = Decimal("1"),
    executed_quantity: Decimal = Decimal("0"),
    price: Decimal = Decimal("100"),
    reduce_only: bool = False,
    raw_payload: dict[str, object] | None = None,
) -> AccountOpenOrderSnapshot:
    return AccountOpenOrderSnapshot(
        environment="live",
        account_label="account-1",
        symbol="BTCUSDT",
        order_id=order_id,
        client_order_id=client_order_id,
        side="BUY",
        order_type="LIMIT",
        status="NEW",
        price=price,
        original_quantity=original_quantity,
        executed_quantity=executed_quantity,
        reduce_only=reduce_only,
        observed_at=NOW,
        raw_payload=raw_payload or {},  # type: ignore[arg-type]
    )


def _plan(client_order_id: str) -> OrderExecutionPlan:
    return OrderExecutionPlan(
        intent_id=f"intent-{client_order_id}",
        run_id="run-1",
        client_order_id=client_order_id,
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("0.1"),
        price=None,
        reduce_only=False,
        created_at=NOW,
        quantized=True,
    )
