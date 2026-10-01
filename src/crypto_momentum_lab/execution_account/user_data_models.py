"""User-data merge results and failures independent of mutable merge state."""

from dataclasses import dataclass

from crypto_momentum_lab.domain.account.models import AccountFillEvent
from crypto_momentum_lab.domain.account.snapshot_models import (
    AccountSnapshot,
    AccountSnapshotDelta,
)
from crypto_momentum_lab.execution_account.binance.user_data_models import (
    BinanceUserDataEvent,
)


class UserDataStateError(ValueError):
    """An event is too incomplete to apply without a REST reconciliation."""


@dataclass(frozen=True, slots=True)
class AccountUserDataUpdate:
    event: BinanceUserDataEvent
    snapshot: AccountSnapshot
    fills: tuple[AccountFillEvent, ...]
    needs_reconciliation: bool
    reason: str | None
    changed: bool
    delta: AccountSnapshotDelta | None = None
