"""Account snapshot values independent of synchronization services."""

from dataclasses import dataclass

from crypto_momentum_lab.domain.account.models import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    AccountOpenOrderSnapshot,
    AccountPositionSnapshot,
)


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    config: AccountConfigSnapshot
    balances: tuple[AccountBalanceSnapshot, ...]
    positions: tuple[AccountPositionSnapshot, ...]
    open_orders: tuple[AccountOpenOrderSnapshot, ...]
