"""Post-processing policy for live exchange order events."""

from __future__ import annotations

import structlog

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    OrderExecutionPlan,
)
from crypto_momentum_lab.live_rollout.daemon import LiveStrategyDaemon
from crypto_momentum_lab.live_rollout.entry_orders import LiveLimitOrderLifecycle
from crypto_momentum_lab.live_rollout.telemetry import LiveTelemetrySink

log = structlog.get_logger()


class LiveOrderEventRuntime:
    """Keep telemetry best-effort while preserving local order observers."""

    def __init__(self, *, telemetry: LiveTelemetrySink) -> None:
        self._telemetry = telemetry
        self._entry_order_lifecycle: LiveLimitOrderLifecycle | None = None
        self._daemon: LiveStrategyDaemon | None = None

    def set_entry_order_lifecycle(
        self,
        lifecycle: LiveLimitOrderLifecycle,
    ) -> None:
        self._entry_order_lifecycle = lifecycle

    def set_daemon(self, daemon: LiveStrategyDaemon) -> None:
        self._daemon = daemon

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
            if self._entry_order_lifecycle is not None:
                self._entry_order_lifecycle.observe(plan, event)
            if self._daemon is not None:
                self._daemon.observe_entry_order_event(plan, event)


__all__ = ["LiveOrderEventRuntime"]
