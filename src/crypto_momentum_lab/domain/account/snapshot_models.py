"""Account snapshot values independent of synchronization services."""

from dataclasses import dataclass
from datetime import datetime

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


@dataclass(frozen=True, slots=True)
class AccountSnapshotDelta:
    """Transport-independent changes from one account snapshot to the next."""

    observed_at: datetime
    config: AccountConfigSnapshot | None = None
    balances: tuple[AccountBalanceSnapshot, ...] = ()
    removed_balance_assets: tuple[str, ...] = ()
    positions: tuple[AccountPositionSnapshot, ...] = ()
    removed_positions: tuple[tuple[str, str], ...] = ()
    open_orders: tuple[AccountOpenOrderSnapshot, ...] = ()
    removed_open_orders: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")

    @property
    def is_empty(self) -> bool:
        return not any(
            (
                self.config is not None,
                self.balances,
                self.removed_balance_assets,
                self.positions,
                self.removed_positions,
                self.open_orders,
                self.removed_open_orders,
            )
        )
