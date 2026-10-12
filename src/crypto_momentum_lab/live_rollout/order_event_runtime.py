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
from crypto_momentum_lab.live_rollout.pending_entries import (
    TerminalOrderBarrier,
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
        terminal_barrier: TerminalOrderBarrier | None = None,
    ) -> None:
        self._telemetry = telemetry
        self._request_recovery = request_recovery
        self._terminal_barrier = terminal_barrier
        self._entry_order_lifecycle: EntryOrderLifecycleObserver | None = None
        self._observe_entry: (
            Callable[[OrderExecutionPlan, ExchangeOrderEvent], None] | None
        ) = None

    def set_terminal_barrier(
        self,
        barrier: TerminalOrderBarrier,
    ) -> None:
        self._terminal_barrier = barrier

    def set_entry_order_lifecycle(
        self,
        lifecycle: EntryOrderLifecycleObserver,
    ) -> None:
        self._entry_order_lifecycle = lifecycle
        if self._terminal_barrier is None:
            self._terminal_barrier = getattr(
                lifecycle, "terminal_barrier", None
            )

    def set_entry_observer(
        self, observer: Callable[[OrderExecutionPlan, ExchangeOrderEvent], None]
    ) -> None:
        self._observe_entry = observer

    async def handle(
        self,
        plan: OrderExecutionPlan,
        event: ExchangeOrderEvent,
    ) -> None:
        if (
            self._terminal_barrier is not None
            and not self._terminal_barrier.is_terminal(plan.client_order_id)
        ):
            try:
                await self._terminal_barrier.check_durable_terminal(
                    plan.client_order_id
                )
            except Exception as error:
                log.warning(
                    "live_order_durable_terminal_check_failed",
                    symbol=plan.symbol,
                    client_order_id=plan.client_order_id,
                    error_type=type(error).__name__,
                )
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
