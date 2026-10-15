from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.ports import (
    DecisionCommitConflict,
    ExecutionTradeIdentity,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
    ExecutionTransaction,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionTradeIdentityRow,
)


@pytest.mark.asyncio
async def test_existing_exchange_trade_key_accepts_one_millisecond_timestamp_alias():
    session = AsyncMock()
    session.get.return_value = ExecutionTradeIdentityRow(
        environment="live",
        account_label="account-4",
        symbol="WALUSDT",
        position_side="LONG",
        trade_id="79424638",
        payload_digest="digest-with-original-timestamp",
    )
    tx = ExecutionTransaction(
        session,
        command_repository=AsyncMock(),
        reservation_repository=AsyncMock(),
    )

    inserted = await tx.record_trade(
        key=PositionKey(
            environment="live",
            account_label="account-4",
            symbol="WALUSDT",
            position_side=FuturesPositionSide.LONG,
        ),
        stream_id="account_event_hub",
        stream_epoch="epoch-new",
        trade=ExecutionTradeIdentity(
            trade_id="79424638",
            quantity=Decimal("325"),
            price=Decimal("0.06123"),
            payload_digest="digest-with-replayed-timestamp",
            payload_digest_aliases=("digest-with-original-timestamp",),
        ),
    )

    assert inserted is False
    session.add.assert_not_called()


@pytest.mark.asyncio
async def test_existing_exchange_trade_key_rejects_other_payload_changes():
    session = AsyncMock()
    session.get.return_value = ExecutionTradeIdentityRow(
        environment="live",
        account_label="account-4",
        symbol="WALUSDT",
        position_side="LONG",
        trade_id="79424638",
        payload_digest="original-payload",
    )
    tx = ExecutionTransaction(
        session,
        command_repository=AsyncMock(),
        reservation_repository=AsyncMock(),
    )

    with pytest.raises(
        DecisionCommitConflict, match="conflicts with its durable identity"
    ):
        await tx.record_trade(
            key=PositionKey(
                environment="live",
                account_label="account-4",
                symbol="WALUSDT",
                position_side=FuturesPositionSide.LONG,
            ),
            stream_id="account_event_hub",
            stream_epoch="epoch-new",
            trade=ExecutionTradeIdentity(
                trade_id="79424638",
                quantity=Decimal("325"),
                price=Decimal("0.06123"),
                payload_digest="changed-payload",
            ),
        )
