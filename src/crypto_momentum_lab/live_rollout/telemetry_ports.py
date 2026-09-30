"""Small telemetry capabilities consumed by independent runtime publishers."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from crypto_momentum_lab.execution_account.hub import AccountEvent


class ConsumerHealthSink(Protocol):
    def consumer_health(
        self,
        *,
        consumer: str,
        available: bool,
        occurred_at: datetime,
        reason: str | None = None,
        recovery: bool = False,
        lag: bool = False,
        sequence: int | None = None,
    ) -> None: ...



class AccountFillSink(Protocol):
    async def account_fill(
        self,
        event: AccountEvent,
        *,
        occurred_at: datetime,
    ) -> None: ...

