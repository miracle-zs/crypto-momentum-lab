"""Checkpoint progress coordination for the live market-state loop."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import datetime
from time import perf_counter
from typing import Protocol

import structlog

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy.models import StrategyCheckpoint
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
        checkpoint_every_states: int = 1000,
        checkpoint_every_seconds: float = 60.0,
        checkpoint_phase_seconds: float = 0.0,
        max_dirty_age_seconds: float = 90.0,
        hub_cursor_provider: Callable[[], Mapping[str, str | int] | None] | None = None,
    ) -> None:
        if checkpoint_every_states <= 0:
            raise ValueError("checkpoint_every_states must be positive")
        if checkpoint_every_seconds <= 0:
            raise ValueError("checkpoint_every_seconds must be positive")
        if not 0 <= checkpoint_phase_seconds < checkpoint_every_seconds:
            raise ValueError(
                "checkpoint_phase_seconds must be in [0, checkpoint_every_seconds)"
            )
        if max_dirty_age_seconds <= 0:
            raise ValueError("max_dirty_age_seconds must be positive")
        self._writer = writer
        self._strategy = strategy
        self._checkpoint_every_states = checkpoint_every_states
        self._checkpoint_every_seconds = checkpoint_every_seconds
        self._checkpoint_phase_seconds = checkpoint_phase_seconds
        self._max_dirty_age_seconds = max_dirty_age_seconds
        self._hub_cursor_provider = hub_cursor_provider
        self._last_processed_at_by_symbol: dict[str, datetime] = {}
        self._processed_state_count = 0
        self._dirty = False
        self._last_saved_at: datetime | None = None
        self._last_checkpoint_cycle: int | None = None
        writer.attach_clock(perf_counter)
        self._last_persisted_token: int = writer.last_persisted_token
        writer_mono = writer.last_persisted_monotonic
        self._last_persisted_monotonic: float = (
            writer_mono if writer_mono is not None else perf_counter()
        )
        self._dirty_since_monotonic: float | None = None
        self._last_submitted_token: int = 0
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        self._last_processed_at_by_symbol = dict(
            _checkpoint_for_persistence(
                self._strategy,
                hub_cursor_provider=self._hub_cursor_provider,
            ).last_processed_at_by_symbol
        )
        self._processed_state_count = 0
        self._dirty = False
        self._last_saved_at = None
        self._last_checkpoint_cycle = None
        self._writer.attach_clock(perf_counter)
        self._last_persisted_token = self._writer.last_persisted_token
        writer_mono = self._writer.last_persisted_monotonic
        self._last_persisted_monotonic = (
            writer_mono if writer_mono is not None else perf_counter()
        )
        self._dirty_since_monotonic = None
        self._last_submitted_token = 0
        await self._writer.start()
        self._started = True

    async def stop(self) -> None:
        if not self._started:
            return
        await self._writer.stop()
        self._started = False

    @property
    def checkpoint_every_states(self) -> int:
        return self._checkpoint_every_states

    @property
    def checkpoint_every_seconds(self) -> float:
        return self._checkpoint_every_seconds

    @property
    def checkpoint_phase_seconds(self) -> float:
        return self._checkpoint_phase_seconds

    @property
    def max_dirty_age_seconds(self) -> float:
        return self._max_dirty_age_seconds

    @property
    def dirty(self) -> bool:
        return self._dirty

    def _sync_persisted_progress(self) -> None:
        token = self._writer.last_persisted_token
        if token > self._last_persisted_token:
            self._last_persisted_token = token
            writer_mono = self._writer.last_persisted_monotonic
            if writer_mono is not None:
                self._last_persisted_monotonic = writer_mono

    @property
    def last_persisted_token(self) -> int:
        self._sync_persisted_progress()
        return self._last_persisted_token

    @property
    def durable_age_seconds(self) -> float:
        self._sync_persisted_progress()
        return perf_counter() - self._last_persisted_monotonic

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
        self._sync_persisted_progress()
        now_mono = perf_counter()
        self._processed_state_count += 1
        self._last_processed_at_by_symbol[state.symbol] = state.bucket_start
        self._dirty = True
        if self._dirty_since_monotonic is None:
            self._dirty_since_monotonic = now_mono
        self._last_saved_at = saved_at

        should_checkpoint = False
        if self._processed_state_count % self._checkpoint_every_states == 0:
            should_checkpoint = True

        if not should_checkpoint and self._checkpoint_every_seconds > 0:
            ts = int(round(state.bucket_end.timestamp()))
            interval = int(round(self._checkpoint_every_seconds))
            phase = int(round(self._checkpoint_phase_seconds))
            current_cycle = ts // interval
            phase_matches = (ts - phase) % interval == 0
            if phase_matches and self._last_checkpoint_cycle != current_cycle:
                self._last_checkpoint_cycle = current_cycle
                should_checkpoint = True

        # Phase E: Scheduling clock decouple - enforce max_dirty_age_seconds
        if not should_checkpoint and (
            now_mono - self._last_persisted_monotonic >= self._max_dirty_age_seconds
        ):
            should_checkpoint = True

        if not should_checkpoint:
            return

        token = self._writer.submit(
            _checkpoint_for_persistence(
                self._strategy,
                hub_cursor_provider=self._hub_cursor_provider,
            ),
            saved_at,
        )
        self._last_submitted_token = token
        self._dirty = False
        self._dirty_since_monotonic = None
        self._last_saved_at = None

    async def save_final(self, timeout_seconds: float | None = None) -> bool:
        if self._dirty and self._last_saved_at is not None:
            checkpoint = _checkpoint_for_persistence(
                self._strategy,
                hub_cursor_provider=self._hub_cursor_provider,
            )
            if timeout_seconds is not None:
                try:
                    async with asyncio.timeout(timeout_seconds):
                        saved = await self._writer.save_now(
                            checkpoint,
                            self._last_saved_at,
                        )
                except TimeoutError:
                    log.warning(
                        "live_checkpoint_final_flush_timed_out",
                        timeout_seconds=timeout_seconds,
                    )
                    return False
            else:
                saved = await self._writer.save_now(
                    checkpoint,
                    self._last_saved_at,
                )
            if saved:
                self._sync_persisted_progress()
                self._dirty = False
                self._dirty_since_monotonic = None
                writer_mono = self._writer.last_persisted_monotonic
                self._last_persisted_monotonic = (
                    writer_mono if writer_mono is not None else perf_counter()
                )
                self._last_saved_at = None
            return saved

        # If not dirty, but there is an unpersisted submitted checkpoint:
        if self._last_submitted_token > self._writer.last_persisted_token:
            if timeout_seconds is not None:
                try:
                    async with asyncio.timeout(timeout_seconds):
                        saved = await self._writer.flush()
                except TimeoutError:
                    log.warning(
                        "live_checkpoint_final_flush_timed_out",
                        timeout_seconds=timeout_seconds,
                    )
                    return False
            else:
                saved = await self._writer.flush()
            if (
                saved
                and self._writer.last_persisted_token >= self._last_submitted_token
            ):
                self._sync_persisted_progress()
                return True
            return False

        self._sync_persisted_progress()
        return True

    def record_recovered_state(
        self,
        state: MarketState15s,
        *,
        saved_at: datetime,
    ) -> None:
        """Advance the durable watermark for a state warmed during backfill.

        A recovered bucket was not evaluated as a new signal, so it must not
        count toward the normal checkpoint cadence.  It is nevertheless part
        of the strategy's contiguous rolling state and must be reflected in
        the next compact checkpoint.
        """

        if not self._started:
            raise RuntimeError("checkpoint coordinator is not started")
        previous = self._last_processed_at_by_symbol.get(state.symbol)
        if previous is not None and state.bucket_start <= previous:
            return
        self._last_processed_at_by_symbol[state.symbol] = state.bucket_start
        self._dirty = True
        if self._dirty_since_monotonic is None:
            self._dirty_since_monotonic = perf_counter()
        self._last_saved_at = saved_at


def _checkpoint_for_persistence(
    strategy: CheckpointableStrategy,
    *,
    hub_cursor_provider: Callable[[], Mapping[str, str | int] | None] | None = None,
) -> StrategyCheckpoint:
    """Request the strategy's compact checkpoint through its explicit contract."""
    started = perf_counter()
    checkpoint = strategy.checkpoint(include_market_state_buffers=False)
    log.info(
        "live_checkpoint_built",
        build_ms=round((perf_counter() - started) * 1000, 3),
        payload_keys=tuple(sorted(checkpoint.payload)),
    )
    return _with_hub_cursor(checkpoint, hub_cursor_provider)


def _with_hub_cursor(
    checkpoint: StrategyCheckpoint,
    provider: Callable[[], Mapping[str, str | int] | None] | None,
) -> StrategyCheckpoint:
    if provider is None:
        return checkpoint
    cursor = provider()
    if cursor is None:
        return checkpoint
    stream_id = cursor.get("stream_id")
    sequence = cursor.get("sequence")
    if not isinstance(stream_id, str) or not stream_id.strip():
        raise ValueError("hub cursor stream_id must be a non-empty string")
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0:
        raise ValueError("hub cursor sequence must be a non-negative integer")
    payload = dict(checkpoint.payload)
    payload["market_state_hub_cursor"] = {
        "stream_id": stream_id,
        "sequence": sequence,
    }
    return replace(checkpoint, payload=payload)


__all__ = ["LiveCheckpointCoordinator"]
