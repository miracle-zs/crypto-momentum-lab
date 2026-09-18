from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.account import (
    AccountBalanceSnapshot,
    ExecutionAccountProcessState,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.persistence.postgres.account_repository import (
    balance_snapshot_row,
    process_state_row,
)


def test_balance_snapshot_row_preserves_numeric_values() -> None:
    snapshot = AccountBalanceSnapshot(
        environment="live",
        account_label="primary",
        asset="USDT",
        wallet_balance=Decimal("100.5"),
        available_balance=Decimal("80.25"),
        unrealized_pnl=Decimal("1.5"),
        observed_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        raw_payload={"asset": "USDT"},
    )

    row = balance_snapshot_row(snapshot)

    assert row["environment"] == "live"
    assert row["account_label"] == "primary"
    assert row["asset"] == "USDT"
    assert row["wallet_balance"] == Decimal("100.5")
    assert row["available_balance"] == Decimal("80.25")
    assert row["unrealized_pnl"] == Decimal("1.5")


def test_process_state_row_uses_state_value() -> None:
    state = ExecutionAccountProcessState(
        environment="live",
        account_label="primary",
        state=ExecutionAccountStatus.READY_READONLY,
        occurred_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        reason=None,
    )

    row = process_state_row(state)

    assert row["state"] == "ready_readonly"
    assert row["reason"] is None


def test_account_reconciliation_head_validates_fields() -> None:
    from crypto_momentum_lab.domain.account import AccountReconciliationHead

    head = AccountReconciliationHead(
        environment="live",
        account_label="primary",
        reconciliation_id="test:1",
        status="ready",
        observed_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        balance_count=1,
        position_count=2,
        open_order_count=0,
        fill_count=5,
        mismatch_count=0,
        details={"ok": True},
    )
    assert head.environment == "live"
    assert head.position_count == 2
    assert head.projection_schema_version == 1
