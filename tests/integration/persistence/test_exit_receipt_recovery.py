"""Receipt reconciliation uses real durable trade facts, without POST calls."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.account.models import AccountPositionSnapshot
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderSnapshot,
    ExchangeOrderState,
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
from crypto_momentum_lab.persistence.postgres.models import AccountFillEventRow
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)


async def test_exchange_filled_receipt_requires_exact_account_order_trade_sum(
    async_database_url,
):
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = "receipt-" + uuid4().hex[:12]
    now = datetime.now(UTC)
    command = TradeCommand(
        "exit-test",
        PositionKey("live", account, "BTCUSDT", "LONG"),
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
        assert (await recovery(command)).status == "SUPERSEDED"
        exchange.query_order_by_client_id.return_value = ExchangeOrderSnapshot(
            "exit-test",
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
