"""Receipt reconciliation uses real durable trade facts, without POST calls."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.account.models import AccountPositionSnapshot
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderSnapshot,
    ExchangeOrderState,
    FuturesPositionSide,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.execution.trade_command import (
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy.models import EntryType, StrategySide
from crypto_momentum_lab.live_rollout.exit_receipt_recovery import (
    LiveExitReceiptRecovery,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountFillEventRow,
    ExchangeOrderRow,
    OrderIntentExecutionRow,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)


@pytest.mark.parametrize("durable_mode", ["absent", "legacy", "different_run"])
async def test_exchange_filled_receipt_requires_exact_account_order_trade_sum(
    async_database_url,
    durable_mode,
):
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = "receipt-" + uuid4().hex[:12]
    now = datetime.now(UTC)
    command = TradeCommand(
        "exit-" + account,
        PositionKey("live", account, "BTCUSDT", FuturesPositionSide.LONG),
        TradeCommandType.EXIT,
        StrategySide.LONG,
        EntryType.MARKET,
        Decimal("2"),
        reduce_only=True,
        created_at=now - timedelta(hours=1),
    )
    position = AccountPositionSnapshot(
        "live",
        account,
        "BTCUSDT",
        "LONG",
        Decimal("0"),
        Decimal("0"),
        Decimal("10"),
        Decimal("0"),
        Decimal("0"),
        2,
        "cross",
        now,
        {},
    )
    exchange = SimpleNamespace(
        fetch_positions=AsyncMock(return_value=(position,)),
        fetch_open_orders=AsyncMock(return_value=()),
        query_order_by_client_id=AsyncMock(return_value=None),
    )
    recovery = LiveExitReceiptRecovery(
        factory,
        exchange=exchange,
        account_label=account,
        run_id="run",
        clock=lambda: now,
    )
    try:
        client_id = command.client_order_id("run")
        if durable_mode != "absent":
            client_id = command.command_id
            intent_id = "intent_exit_" + command.command_id
            async with factory() as session, session.begin():
                session.add(
                    OrderIntentExecutionRow(
                        intent_id=intent_id,
                        candidate_id=intent_id,
                        run_id="run",
                        risk_evaluation_id=intent_id,
                        strategy_name="momentum",
                        symbol="BTCUSDT",
                        state="approved",
                        approved_at=now,
                        details={},
                    )
                )
                await session.flush()
                session.add(
                    ExchangeOrderRow(
                        client_order_id=client_id,
                        intent_id=intent_id,
                        run_id="other-run"
                        if durable_mode == "different_run"
                        else "run",
                        exchange_order_id="filled-1",
                        symbol="BTCUSDT",
                        side="SELL",
                        order_type="MARKET",
                        quantity=Decimal("2"),
                        price=None,
                        time_in_force=None,
                        expires_at=None,
                        executed_quantity=Decimal("2"),
                        reduce_only=True,
                        position_side="LONG",
                        state="filled",
                        created_at=command.created_at,
                        updated_at=now,
                    )
                )
        first = await recovery(command)
        if durable_mode == "different_run":
            assert first.status == "PENDING"
            assert first.reason == "durable_order_identity_mismatch"
            exchange.query_order_by_client_id.assert_not_awaited()
            exchange.fetch_positions.assert_not_awaited()
            return
        assert first.status == ("SUPERSEDED" if durable_mode == "absent" else "PENDING")
        exchange.query_order_by_client_id.assert_awaited_once_with("BTCUSDT", client_id)
        exchange.query_order_by_client_id.return_value = ExchangeOrderSnapshot(
            client_id,
            "filled-1",
            ExchangeOrderState.FILLED,
            now,
            Decimal("2"),
            Decimal("10"),
        )
        assert (await recovery(command)).status == "PENDING"
        async with factory() as session, session.begin():
            session.add(
                AccountFillEventRow(
                    environment="live",
                    account_label=account,
                    symbol="BTCUSDT",
                    trade_id="trade-1",
                    order_id="filled-1",
                    side="SELL",
                    price=Decimal("10"),
                    quantity=Decimal("2"),
                    realized_pnl=Decimal("0"),
                    fee=Decimal("0"),
                    fee_asset="USDT",
                    trade_at=now,
                    raw_payload={"positionSide": "LONG"},
                )
            )
        disposition = await recovery(command)
        assert disposition.status == "DISPATCHED"
        assert disposition.reason == "exchange_filled_receipt_verified_filled-1"
    finally:
        await engine.dispose()
