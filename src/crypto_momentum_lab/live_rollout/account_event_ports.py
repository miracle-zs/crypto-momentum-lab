"""Account-triggered exit processing consumed by the account event channel."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from crypto_momentum_lab.domain.market.models import MarketState15s, RealtimeMarketQuote

if TYPE_CHECKING:
    from crypto_momentum_lab.execution_account.hub import AccountEvent


class AccountEventExitProcessor(Protocol):
    async def process_account_event(
        self, state: MarketState15s, *, quote: RealtimeMarketQuote | None = None
    ) -> str | None: ...


class AccountEventOrderReconciler(Protocol):
    """Reconcile an account event before its account projection is published."""

    @property
    def run_id(self) -> str: ...

    async def reconcile_account_event(self, event: AccountEvent) -> None: ...
