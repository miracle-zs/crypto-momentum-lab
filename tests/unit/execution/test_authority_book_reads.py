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


@pytest.mark.asyncio
async def test_flat_position_adopts_active_stream_epoch_on_read():
    book = ExecutionBook()
    active_scope = ExecutionScope(environment="live", account_label="reader", symbol="BTCUSDT")
    start = datetime(2026, 9, 28, tzinfo=UTC)
    fill = AccountFillEvent(
        environment=active_scope.environment,
        account_label=active_scope.account_label,
        symbol=active_scope.symbol,
        trade_id="1",
        order_id="1",
        side="BUY",
        price=Decimal("100"),
        quantity=Decimal("1"),
        realized_pnl=Decimal(0),
        fee=Decimal(0),
        fee_asset="USDT",
        trade_at=start,
        raw_payload={"is_system": True, "positionSide": "BOTH"},
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="evidence-1",
            scope=active_scope,
            observed_at=fill.trade_at,
            fill=fill,
            stream_id="trades",
            stream_epoch="epoch-active",
            sequence=1,
        )
    )

    flat_scope = ExecutionScope(environment="live", account_label="reader", symbol="ALGOUSDT")
    view = await book.read(flat_scope, stream_id="trades", stream_epoch="epoch-active")
    assert view.is_ready_for_trade
    assert view.stream_scope is not None
    assert view.stream_scope.stream_epoch == "epoch-active"
    assert view.total_quantity == Decimal("0")

    # 2. A symbol with old epoch that is flat adopts the active stream on read
    old_scope = ExecutionScope(environment="live", account_label="reader", symbol="SOLUSDT")
    await book.observe(
        ExecutionEvidence(
            evidence_id="evidence-old-buy",
            scope=old_scope,
            observed_at=start,
            fill=AccountFillEvent(
                environment=old_scope.environment,
                account_label=old_scope.account_label,
                symbol=old_scope.symbol,
                trade_id="sol-1",
                order_id="sol-1",
                side="BUY",
                price=Decimal("100"),
                quantity=Decimal("1"),
                realized_pnl=Decimal(0),
                fee=Decimal(0),
                fee_asset="USDT",
                trade_at=start,
                raw_payload={"is_system": True, "positionSide": "BOTH"},
            ),
            stream_id="trades",
            stream_epoch="epoch-old",
            sequence=1,
        )
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="evidence-old-sell",
            scope=old_scope,
            observed_at=start + timedelta(seconds=1),
            fill=AccountFillEvent(
                environment=old_scope.environment,
                account_label=old_scope.account_label,
                symbol=old_scope.symbol,
                trade_id="sol-2",
                order_id="sol-2",
                side="SELL",
                price=Decimal("105"),
                quantity=Decimal("1"),
                realized_pnl=Decimal(5),
                fee=Decimal(0),
                fee_asset="USDT",
                trade_at=start + timedelta(seconds=1),
                raw_payload={"is_system": True, "positionSide": "BOTH"},
            ),
            stream_id="trades",
            stream_epoch="epoch-old",
            sequence=2,
        )
    )
    sol_view = await book.read(old_scope, stream_id="trades", stream_epoch="epoch-active")
    assert sol_view.is_ready_for_trade
    assert sol_view.stream_scope is not None
    assert sol_view.stream_scope.stream_epoch == "epoch-active"
    assert sol_view.total_quantity == Decimal("0")

    # 3. Reading with an unknown / inactive epoch must still fail closed
    with pytest.raises(ValueError, match="stream"):
        await book.read(flat_scope, stream_id="trades", stream_epoch="epoch-unknown")

