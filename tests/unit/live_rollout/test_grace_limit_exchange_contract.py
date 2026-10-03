from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from urllib.parse import parse_qs

import httpx
import pytest

from crypto_momentum_lab.domain.execution.order_rules import SymbolTradingRules
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.execution.trade_command import (
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy.position_exit import (
    ClosedCandle15m,
    PositionExitMode,
)
from crypto_momentum_lab.execution_account.binance.client import BinanceUsdMTradeClient
from crypto_momentum_lab.execution_account.orders.quantization import (
    quantize_order_plan,
)
from crypto_momentum_lab.execution_account.orders.trade_command_planner import (
    plan_order_execution,
)
from crypto_momentum_lab.live_rollout.exits import LiveExitManager, LiveExitOrderRequest
from tests.fixtures.live_market import _state
from tests.unit.live_rollout.test_exits import FakeCandleLoader, _config, _long_position


@pytest.mark.parametrize("planner", ("command", "intent"))
async def test_grace_limit_reaches_exchange_with_explicit_gtc(planner: str) -> None:
    candle = ClosedCandle15m(
        symbol="BTCUSDT",
        candle_start=datetime(2026, 7, 4, 0, 15, tzinfo=UTC),
        candle_end=datetime(2026, 7, 4, 0, 30, tzinfo=UTC),
        open_price=Decimal("100"),
        close_price=Decimal("99"),
    )
    manager = LiveExitManager(
        config=_config(
            PositionExitMode.CANDLE_15M,
            candle_grace_bars=8,
            candle_grace_profit_pct=Decimal("0.0088"),
        ),
        candle_loader=FakeCandleLoader((candle,)),
    )
    state = replace(
        _state(),
        bucket_start=candle.candle_end,
        bucket_end=candle.candle_end.replace(second=15),
        last_bid_price=Decimal("99"),
        mark_price=Decimal("99"),
        close_price=Decimal("99"),
    )
    position = _long_position()
    requests = await manager.requests_for_state(state, (position,))
    assert len(requests) == 1
    request = requests[0]
    assert isinstance(request, LiveExitOrderRequest)
    candidate = request.candidate
    rules = SymbolTradingRules(
        symbol="BTCUSDT",
        tick_size=Decimal("0.01"),
        step_size=Decimal("0.001"),
        min_quantity=Decimal("0.001"),
        max_quantity=Decimal("100"),
        min_notional=Decimal("5"),
    )
    if planner == "command":
        command = TradeCommand(
            command_id=candidate.candidate_id,
            position_key=PositionKey(
                environment="live",
                account_label="test_account",
                symbol=candidate.symbol,
                position_side=position.position_side,
            ),
            command_type=TradeCommandType.EXIT,
            side=candidate.side,
            order_type=candidate.entry_type,
            limit_price=candidate.limit_price,
            requested_quantity=request.quantity,
            reduce_only=True,
            created_at=candidate.created_at,
        )
        plan = plan_order_execution(
            command, rules, run_id=candidate.run_id, reference_price=Decimal("99")
        ).plan
    else:
        plan = quantize_order_plan(
            candidate,
            rules,
            reference_price=Decimal("99"),
            resize_tolerance=Decimal("0.2"),
            requested_quantity=request.quantity,
        )
    assert isinstance(plan, OrderExecutionPlan)
    bodies = []

    async def handle(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/fapi/v1/order"
        body = parse_qs(request.content.decode())
        bodies.append(body)
        return httpx.Response(
            200,
            json={
                "clientOrderId": plan.client_order_id,
                "orderId": 12345,
                "status": "NEW",
                "executedQty": "0",
                "avgPrice": "0",
            },
        )

    client = BinanceUsdMTradeClient(
        api_key="test-key",
        api_secret="test-secret",
        environment="live",
        account_label="test_account",
        live_submit_enabled=True,
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handle), base_url="https://fapi.binance.com"
        ),
        clock=lambda: state.bucket_end,
    )
    try:
        receipt = await client.submit_order(plan)
    finally:
        await client.aclose()
    assert receipt.state is ExchangeOrderState.ACKNOWLEDGED
    assert len(bodies) == 1
    assert bodies[0]["timeInForce"] == ["GTC"]
    assert bodies[0]["positionSide"] == ["LONG"]
    assert bodies[0]["side"] == ["SELL"]
    assert bodies[0]["type"] == ["LIMIT"]
    assert bodies[0]["price"] == ["100.88"]
    assert "goodTillDate" not in bodies[0]
    assert plan.expires_at is None
