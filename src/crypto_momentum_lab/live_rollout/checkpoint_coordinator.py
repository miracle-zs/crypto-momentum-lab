"""Checkpoint progress coordination for the live market-state loop."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime
from inspect import Parameter, signature
from time import perf_counter
from typing import Protocol

import structlog

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import StrategyCheckpoint
from crypto_momentum_lab.live_rollout.checkpoint_writer import CheckpointWriter

log = structlog.get_logger()


class CheckpointableStrategy(Protocol):
    def checkpoint(
        self,
        *,
        include_market_state_buffers: bool = True,
    ) -> StrategyCheckpoint: ...


class LiveCheckpointCoordinator:
    """Track market progress and schedule compact durable checkpoints.

    The coordinator owns the stateful policy around ``CheckpointWriter``:
    which symbols have been processed, when a periodic snapshot is due, and
    which last dirty timestamp must be flushed before a normal or safety halt.
    The writer remains the adapter for asynchronous persistence, retry, and
    last-write-wins coalescing.
    """

    def __init__(
        self,
        *,
        writer: CheckpointWriter,
        strategy: CheckpointableStrategy,
        checkpoint_every_states: int,
    ) -> None:
        if checkpoint_every_states <= 0:
            raise ValueError("checkpoint_every_states must be positive")
        self._writer = writer
        self._strategy = strategy
        self._checkpoint_every_states = checkpoint_every_states
        self._last_processed_at_by_symbol: dict[str, datetime] = {}
        self._processed_state_count = 0
        self._dirty = False
        self._last_saved_at: datetime | None = None
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        self._last_processed_at_by_symbol = dict(
            _checkpoint_for_persistence(self._strategy)
            .last_processed_at_by_symbol
        )
        self._processed_state_count = 0
        self._dirty = False
        self._last_saved_at = None
        await self._writer.start()
        self._started = True

    async def stop(self) -> None:
        if not self._started:
            return
        await self._writer.stop()
        self._started = False

    def last_processed_at(self, symbol: str) -> datetime | None:
        return self._last_processed_at_by_symbol.get(symbol)

    def forget_symbol(self, symbol: str) -> None:
        self._last_processed_at_by_symbol.pop(symbol, None)

    def record_processed_state(
        self,
        state: MarketState15s,
        *,
        saved_at: datetime,
    ) -> None:
        if not self._started:
            raise RuntimeError("checkpoint coordinator is not started")
        self._processed_state_count += 1
        self._last_processed_at_by_symbol[state.symbol] = state.bucket_start
        self._dirty = True
        self._last_saved_at = saved_at
        if self._processed_state_count % self._checkpoint_every_states != 0:
            return
        self._writer.submit(
            _checkpoint_for_persistence(self._strategy),
            saved_at,
        )
        self._dirty = False
        self._last_saved_at = None

    async def save_final(self) -> bool:
        if not self._dirty or self._last_saved_at is None:
            return True
        saved = await self._writer.save_now(
            _checkpoint_for_persistence(self._strategy),
            self._last_saved_at,
        )
        if saved:
            self._dirty = False
            self._last_saved_at = None
        return saved


def _checkpoint_for_persistence(
    strategy: CheckpointableStrategy,
) -> StrategyCheckpoint:
    """Build a compact checkpoint without breaking lightweight adapters."""
    started = perf_counter()
    checkpoint_method = strategy.checkpoint
    parameters: Mapping[str, Parameter] | None = None
    try:
        parameters = signature(checkpoint_method).parameters
    except (TypeError, ValueError):
        pass
    if parameters is not None and "include_market_state_buffers" in parameters and (
        parameters["include_market_state_buffers"].kind
        in {Parameter.KEYWORD_ONLY, Parameter.POSITIONAL_OR_KEYWORD}
    ):
        checkpoint = checkpoint_method(include_market_state_buffers=False)
        log.info(
            "live_checkpoint_built",
            build_ms=round((perf_counter() - started) * 1000, 3),
            payload_keys=tuple(sorted(checkpoint.payload)),
        )
        return checkpoint

    checkpoint = checkpoint_method()
    payload = {
        key: value
        for key, value in checkpoint.payload.items()
        if key not in {"market_state_buffers", "signal_buffers"}
    }
    compact = replace(checkpoint, payload=payload)
    log.info(
        "live_checkpoint_built",
        build_ms=round((perf_counter() - started) * 1000, 3),
        payload_keys=tuple(sorted(compact.payload)),
    )
    return compact


__all__ = ["LiveCheckpointCoordinator"]
