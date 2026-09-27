"""Read contracts for the shared execution and decision position authority."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.execution_book import (
    ExecutionBook,
    ExecutionEvidence,
    ExecutionScope,
)


@pytest.mark.asyncio
async def test_unknown_read_does_not_establish_position_or_stream():
    book = ExecutionBook()
    scope = ExecutionScope(environment="live", account_label="reader", symbol="BTCUSDT")
    view = await book.read(scope)
    assert not view.is_ready_for_trade
    assert (
        await book.list_position_views(environment="live", account_label="reader") == ()
    )
    with pytest.raises(ValueError, match="stream"):
        await book.read(scope, stream_id="wrong", stream_epoch="one")


@pytest.mark.asyncio
async def test_reads_obey_cut_account_and_stream_identity():
    book = ExecutionBook()
    scope = ExecutionScope(environment="live", account_label="reader", symbol="BTCUSDT")
    start = datetime(2026, 9, 28, tzinfo=UTC)
    for index in (1, 2):
        fill = AccountFillEvent(
            environment=scope.environment,
            account_label=scope.account_label,
            symbol=scope.symbol,
            trade_id=str(index),
            order_id=str(index),
            side="BUY",
            price=Decimal("100"),
            quantity=Decimal("1"),
            realized_pnl=Decimal(0),
            fee=Decimal(0),
            fee_asset="USDT",
            trade_at=start + timedelta(seconds=index),
            raw_payload={"is_system": True, "positionSide": "BOTH"},
        )
        await book.observe(
            ExecutionEvidence(
                evidence_id=f"read-{index}",
                scope=scope,
                observed_at=fill.trade_at,
                fill=fill,
                stream_id="trades",
                stream_epoch="one",
                sequence=index,
            )
        )
    current = await book.read(scope, stream_id="trades", stream_epoch="one")
    historical = await book.read(
        scope,
        event_cut=start + timedelta(seconds=1),
        stream_id="trades",
        stream_epoch="one",
    )
    assert current.total_quantity == Decimal("2")
    assert historical.total_quantity == Decimal("1")
    assert historical.projection_version != current.projection_version
    assert (
        await book.list_position_views(environment="live", account_label="someone-else")
        == ()
    )
    assert (
        await book.list_position_views(
            environment="live",
            account_label="reader",
            stream_id="trades",
            stream_epoch="different",
        )
        == ()
    )
    with pytest.raises(ValueError, match="stream"):
        await book.read(scope, stream_id="trades", stream_epoch="different")
