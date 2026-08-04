from datetime import datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account.models import (
    AccountBalanceSnapshot,
    AccountPositionSnapshot,
    ExecutionAccountProcessState,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.persistence.postgres.account_repository import (
    position_snapshot_row,
)


def test_account_balance_snapshot_requires_aware_time() -> None:
    with pytest.raises(ValueError, match="observed_at must be timezone-aware"):
        AccountBalanceSnapshot(
            environment="live",
            account_label="primary",
            asset="USDT",
            wallet_balance=Decimal("100"),
            available_balance=Decimal("80"),
            unrealized_pnl=Decimal("0"),
            observed_at=datetime(2026, 7, 4, 0, 0),
            raw_payload={},
        )


def test_account_position_snapshot_rejects_empty_symbol() -> None:
    with pytest.raises(ValueError, match="symbol must not be empty"):
        AccountPositionSnapshot(
            environment="live",
            account_label="primary",
            symbol=" ",
            position_side="BOTH",
            position_amt=Decimal("0"),
            entry_price=Decimal("0"),
            mark_price=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            notional=Decimal("0"),
            leverage=None,
            margin_type=None,
            observed_at=datetime.now().astimezone(),
            raw_payload={},
        )


def test_account_sync_state_rejects_unknown_state() -> None:
    with pytest.raises(ValueError, match="state must be an ExecutionAccountStatus"):
        ExecutionAccountProcessState(
            environment="live",
            account_label="primary",
            state="READY",
            occurred_at=datetime.now().astimezone(),
            reason=None,
        )


def test_account_sync_state_accepts_ready_readonly() -> None:
    state = ExecutionAccountProcessState(
        environment="live",
        account_label="primary",
        state=ExecutionAccountStatus.READY_READONLY,
        occurred_at=datetime.now().astimezone(),
        reason=None,
    )

    assert state.state is ExecutionAccountStatus.READY_READONLY


def test_hedge_position_snapshots_have_distinct_ids() -> None:
    observed_at = datetime.now().astimezone()
    common = {
        "environment": "live",
        "account_label": "primary",
        "symbol": "BTCUSDT",
        "position_amt": Decimal("0.01"),
        "entry_price": Decimal("60000"),
        "mark_price": Decimal("60100"),
        "unrealized_pnl": Decimal("1"),
        "notional": Decimal("601"),
        "leverage": 1,
        "margin_type": "cross",
        "observed_at": observed_at,
        "raw_payload": {},
    }

    long_row = position_snapshot_row(
        AccountPositionSnapshot(position_side="LONG", **common)
    )
    short_row = position_snapshot_row(
        AccountPositionSnapshot(position_side="SHORT", **common)
    )

    assert long_row["snapshot_id"] != short_row["snapshot_id"]
