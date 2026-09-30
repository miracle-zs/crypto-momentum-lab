"""Pure identities and durable account reconciliation record construction."""

from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

from crypto_momentum_lab.domain.account.models import (
    AccountPositionSnapshot,
    AccountReconciliationRun,
)
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.execution_account.sync_models import ExecutionAccountSyncConfig


def account_reconciliation_id(config: ExecutionAccountSyncConfig) -> str:
    return str(
        uuid5(
            NAMESPACE_URL,
            "account-reconciliation:"
            f"{config.environment}:{config.account_label}:"
            f"{config.observed_at.isoformat()}",
        )
    )


def position_state_details(
    positions: tuple[AccountPositionSnapshot, ...],
) -> dict[str, JsonValue]:
    """Persist the complete current position-key set beside each run.

    Position history remains sparse, so readers must not infer the current
    account state from rows sharing one observation timestamp.
    """
    active_keys = [
        (position.symbol.strip().upper(), position.position_side.strip().upper())
        for position in positions
        if position.position_amt != Decimal("0")
    ]
    if len(set(active_keys)) != len(active_keys):
        raise ValueError("account snapshot contains duplicate active position keys")
    return {
        "position_state_schema_version": 1,
        "position_keys": [
            {"symbol": symbol, "position_side": position_side}
            for symbol, position_side in sorted(active_keys)
        ],
    }


def user_data_reconciliation_id(
    config: ExecutionAccountSyncConfig,
    event_id: str,
) -> str:
    return str(
        uuid5(
            NAMESPACE_URL,
            "account-user-data-event:"
            f"{config.environment}:{config.account_label}:{event_id}",
        )
    )


def reconciliation_run(
    config: ExecutionAccountSyncConfig,
    *,
    reconciliation_id: str,
    status: str,
    mismatch_count: int,
    details: dict[str, JsonValue],
    balance_count: int = 0,
    position_count: int = 0,
    open_order_count: int = 0,
    fill_count: int = 0,
) -> AccountReconciliationRun:
    return AccountReconciliationRun(
        reconciliation_id=reconciliation_id,
        environment=config.environment,
        account_label=config.account_label,
        status=status,
        observed_at=config.observed_at,
        balance_count=balance_count,
        position_count=position_count,
        open_order_count=open_order_count,
        fill_count=fill_count,
        mismatch_count=mismatch_count,
        details=details,
    )
