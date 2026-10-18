from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.next_gen.engine import (
    AccountSnapshot,
    AccountState,
    PositionSnapshot,
    assert_flat_baseline_or_abort,
)
from crypto_momentum_lab.next_gen.models import SystemMode


@pytest.mark.asyncio
async def test_cold_start_flat_admission_success():
    mock_db = AsyncMock()
    mock_rest = AsyncMock()

    # Empty orders and flat positions
    mock_rest.get_open_orders.return_value = []
    mock_rest.get_account_v3.return_value = AccountSnapshot(
        wallet_balances={"USDT": Decimal("10000.50"), "BNB": Decimal("2.0")},
        available_balance=Decimal("9500.25"),
        positions=[
            PositionSnapshot(
                symbol="BTCUSDT", position_side="LONG", position_amt=Decimal("0")
            ),
            PositionSnapshot(
                symbol="ETHUSDT", position_side="SHORT", position_amt=Decimal("0")
            ),
        ],
    )

    state = AccountState(
        environment="live",
        account_label="a1",
        db=mock_db,
        rest_client=mock_rest,
    )
    assert state.system_mode == SystemMode.BOOTING

    await assert_flat_baseline_or_abort(mock_rest, state)

    assert state.wallet_balance == Decimal("10000.50")
    assert state.available_balance_snapshot == Decimal("9500.25")
    assert state.wallet_balances["BNB"] == Decimal("2.0")


@pytest.mark.asyncio
async def test_cold_start_fails_closed_when_open_orders_exist():
    mock_db = AsyncMock()
    mock_rest = AsyncMock()

    mock_rest.get_open_orders.return_value = [{"orderId": 12345, "symbol": "BTCUSDT"}]
    mock_rest.get_account_v3.return_value = AccountSnapshot(
        wallet_balances={"USDT": Decimal("10000")},
        available_balance=Decimal("10000"),
        positions=[],
    )

    state = AccountState(
        environment="live",
        account_label="a1",
        db=mock_db,
        rest_client=mock_rest,
    )

    with pytest.raises(RuntimeError, match="冷启动拒绝准入：检测到交易所存在未结挂单"):
        await assert_flat_baseline_or_abort(mock_rest, state)


@pytest.mark.asyncio
async def test_cold_start_fails_closed_when_non_zero_positions_exist():
    mock_db = AsyncMock()
    mock_rest = AsyncMock()

    mock_rest.get_open_orders.return_value = []
    mock_rest.get_account_v3.return_value = AccountSnapshot(
        wallet_balances={"USDT": Decimal("10000")},
        available_balance=Decimal("10000"),
        positions=[
            PositionSnapshot(
                symbol="BTCUSDT", position_side="LONG", position_amt=Decimal("0.05")
            ),
        ],
    )

    state = AccountState(
        environment="live",
        account_label="a1",
        db=mock_db,
        rest_client=mock_rest,
    )

    with pytest.raises(RuntimeError, match="冷启动拒绝准入.*未平持仓"):
        await assert_flat_baseline_or_abort(mock_rest, state)


@pytest.mark.asyncio
async def test_cold_start_network_failure_raises():
    mock_db = AsyncMock()
    mock_rest = AsyncMock()

    mock_rest.get_open_orders.side_effect = TimeoutError("REST timeout 5.0s")
    mock_rest.get_account_v3.return_value = AccountSnapshot(
        wallet_balances={"USDT": Decimal("10000")},
        available_balance=Decimal("10000"),
        positions=[],
    )

    state = AccountState(
        environment="live",
        account_label="a1",
        db=mock_db,
        rest_client=mock_rest,
    )

    with pytest.raises(RuntimeError, match="冷启动拉取交易所快照失败"):
        await assert_flat_baseline_or_abort(mock_rest, state)
