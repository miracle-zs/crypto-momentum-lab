"""Context reload and fail-closed gate admission for one market state."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime

from crypto_momentum_lab.domain.live_rollout import LiveGateDecision
from crypto_momentum_lab.live_rollout.context import (
    LiveContextProvider,
    LiveDaemonRuntimeContext,
)
from crypto_momentum_lab.live_rollout.context_prefetch import (
    PrefetchedContext,
)
from crypto_momentum_lab.live_rollout.gates import evaluate_live_gate
from crypto_momentum_lab.live_rollout.telemetry_ports import MarketAdmissionSink


@dataclass(frozen=True, slots=True)
class MarketStateAdmission:
    """The context and gate result for one ordered market state."""

    context: LiveDaemonRuntimeContext | None
    gate: LiveGateDecision | None
    error: Exception | None


class LiveMarketStateAdmission:
    """Reload stale context, publish account state, and evaluate the gate."""

    def __init__(
        self,
        *,
        context_provider: LiveContextProvider,
        context_generation: Callable[[], int],
        apply_context: Callable[
            [LiveDaemonRuntimeContext], None
        ],
        telemetry: MarketAdmissionSink | None,
        clock: Callable[[], datetime],
        invalidate_context: Callable[[], None] | None = None,
    ) -> None:
        self._context_provider = context_provider
        self._context_generation = context_generation
        self._apply_context = apply_context
        self._telemetry = telemetry
        self._clock = clock
        self._invalidate_context = invalidate_context

    def invalidate_context_cache(self) -> None:
        """Invoke the optional invalidation capability bound at assembly."""
        if self._invalidate_context is not None:
            self._invalidate_context()

    async def prepare(
        self,
        prefetched: PrefetchedContext,
    ) -> MarketStateAdmission:
        """Prepare one state without authorizing entries on stale context."""

        context_reloaded = prefetched.generation != self._context_generation()
        try:
            if context_reloaded:
                context = await self._context_provider(prefetched.state)
            elif prefetched.error is not None:
                raise prefetched.error
            else:
                if prefetched.context is None:
                    raise RuntimeError("prefetched live context is missing")
                context = prefetched.context
        except asyncio.CancelledError:
            raise
        except Exception as error:
            return MarketStateAdmission(
                context=None,
                gate=None,
                error=error,
            )

        self._apply_context(context)
        if self._telemetry is not None:
            await self._telemetry.context_ready(
                prefetched.state,
                occurred_at=self._clock(),
                prefetched=not context_reloaded,
                reloaded=context_reloaded,
            )
        gate = evaluate_live_gate(
            replace(
                context.gate_context,
                now=context.now,
                active_lease=context.active_lease,
                account_state=context.account_state,
                active_halts=context.active_halts,
                unresolved_order_states=context.unresolved_order_states,
            )
        )
        if self._telemetry is not None:
            await self._telemetry.gate_evaluated(
                prefetched.state,
                occurred_at=self._clock(),
                approved=gate.approved,
                reasons=gate.reasons,
            )
        return MarketStateAdmission(
            context=context,
            gate=gate,
            error=None,
        )


__all__ = ["LiveMarketStateAdmission", "MarketStateAdmission"]
