"""Post-processing policy for live exchange order events."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

import structlog

from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.live_rollout.telemetry_ports import OrderEventSink

log = structlog.get_logger()


class EntryOrderLifecycleObserver(Protocol):
    def observe(self, plan: OrderExecutionPlan, event: ExchangeOrderEvent) -> None: ...


class LiveOrderEventRuntime:
    """Keep telemetry best-effort while preserving local order observers."""

    def __init__(
        self,
        *,
        telemetry: OrderEventSink,
        request_recovery: Callable[[], None],
    ) -> None:
        self._telemetry = telemetry
        self._request_recovery = request_recovery
        self._entry_order_lifecycle: EntryOrderLifecycleObserver | None = None
        self._observe_entry: (
            Callable[[OrderExecutionPlan, ExchangeOrderEvent], None] | None
        ) = None

    def set_entry_order_lifecycle(
        self,
        lifecycle: EntryOrderLifecycleObserver,
    ) -> None:
        self._entry_order_lifecycle = lifecycle

    def set_entry_observer(
        self, observer: Callable[[OrderExecutionPlan, ExchangeOrderEvent], None]
    ) -> None:
        self._observe_entry = observer

    async def handle(
        self,
        plan: OrderExecutionPlan,
        event: ExchangeOrderEvent,
    ) -> None:
        try:
            await self._telemetry.order_event(plan, event)
        except Exception as error:
            log.warning(
                "live_order_telemetry_failed",
                symbol=plan.symbol,
                client_order_id=plan.client_order_id,
                error_type=type(error).__name__,
            )
        finally:
            if event.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION:
                self._request_recovery()
            if self._entry_order_lifecycle is not None:
                self._entry_order_lifecycle.observe(plan, event)
            if self._observe_entry is not None:
                self._observe_entry(plan, event)


__all__ = ["LiveOrderEventRuntime"]
