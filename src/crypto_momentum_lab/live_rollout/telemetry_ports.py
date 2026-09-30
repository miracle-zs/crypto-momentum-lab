"""Small telemetry capabilities consumed by independent runtime publishers."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from crypto_momentum_lab.domain.execution.order_state import (
        ExchangeOrderEvent,
        OrderExecutionPlan,
    )
    from crypto_momentum_lab.domain.market.models import MarketState15s
    from crypto_momentum_lab.execution_account.hub import AccountEvent
    from crypto_momentum_lab.live_rollout.telemetry import SourceIngress


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



class OrderEventSink(Protocol):
    async def order_event(
        self, plan: OrderExecutionPlan, event: ExchangeOrderEvent
    ) -> None: ...


class MarketAdmissionSink(Protocol):
    async def context_ready(
        self,
        state: MarketState15s,
        *,
        occurred_at: datetime,
        prefetched: bool,
        reloaded: bool,
        ingress: SourceIngress | None = None,
    ) -> None: ...

    async def gate_evaluated(
        self,
        state: MarketState15s,
        *,
        occurred_at: datetime,
        approved: bool,
        reasons: tuple[str, ...],
    ) -> None: ...
