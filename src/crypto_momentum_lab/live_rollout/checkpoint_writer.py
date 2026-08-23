"""Non-blocking, last-write-wins persistence for live strategy checkpoints."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from time import perf_counter

import structlog

from crypto_momentum_lab.domain.strategy import StrategyCheckpoint

log = structlog.get_logger()

PersistCheckpoint = Callable[
    [str, StrategyCheckpoint, datetime],
    Awaitable[None],
]


@dataclass(frozen=True, slots=True)
class CheckpointWriterMetrics:
    submitted_count: int
    coalesced_count: int
    persisted_count: int
    failure_count: int
    last_duration_ms: float | None


@dataclass(frozen=True, slots=True)
class _PendingCheckpoint:
    checkpoint: StrategyCheckpoint
    saved_at: datetime


class CheckpointWriter:
    """Keep checkpoint I/O off the live market-state decision loop.

    The interface is intentionally small: callers submit the newest snapshot
    synchronously, while this module owns queue coalescing, retry, timing, and
    the explicit critical flush used when the daemon exits or enters an
    uncertain-order halt.
    """

    def __init__(
        self,
        *,
        run_id: str,
        persist: PersistCheckpoint,
        retry_delay_seconds: float = 1.0,
        flush_timeout_seconds: float = 10.0,
    ) -> None:
        if not run_id.strip():
            raise ValueError("run_id must not be empty")
        if retry_delay_seconds <= 0:
            raise ValueError("retry_delay_seconds must be positive")
        if flush_timeout_seconds <= 0:
            raise ValueError("flush_timeout_seconds must be positive")
        self._run_id = run_id
        self._persist = persist
        self._retry_delay_seconds = retry_delay_seconds
        self._flush_timeout_seconds = flush_timeout_seconds
        self._pending: _PendingCheckpoint | None = None
        self._wake = asyncio.Event()
        self._idle = asyncio.Event()
        self._idle.set()
        self._write_lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._stopping = False
        self._inflight = False
        self._submitted_count = 0
        self._coalesced_count = 0
        self._persisted_count = 0
        self._failure_count = 0
        self._last_duration_ms: float | None = None

    @property
    def metrics(self) -> CheckpointWriterMetrics:
        return CheckpointWriterMetrics(
            submitted_count=self._submitted_count,
            coalesced_count=self._coalesced_count,
            persisted_count=self._persisted_count,
            failure_count=self._failure_count,
            last_duration_ms=self._last_duration_ms,
        )

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stopping = False
        self._task = asyncio.create_task(
            self._run(),
            name=f"live-checkpoint-writer:{self._run_id}",
        )

    def submit(self, checkpoint: StrategyCheckpoint, saved_at: datetime) -> None:
        """Publish a snapshot without waiting for database work."""
        if self._task is None:
            raise RuntimeError("checkpoint writer is not started")
        if self._pending is not None:
            self._coalesced_count += 1
        self._pending = _PendingCheckpoint(checkpoint, saved_at)
        self._submitted_count += 1
        self._idle.clear()
        self._wake.set()

    async def flush(self) -> bool:
        """Wait for queued periodic writes, returning whether they became idle."""
        if self._task is None:
            return True
        try:
            await asyncio.wait_for(
                self._wait_until_idle(),
                timeout=self._flush_timeout_seconds,
            )
        except TimeoutError:
            log.error(
                "live_checkpoint_flush_timed_out",
                run_id=self._run_id,
                timeout_seconds=self._flush_timeout_seconds,
            )
            return False
        return True

    async def save_now(
        self,
        checkpoint: StrategyCheckpoint,
        saved_at: datetime,
    ) -> bool:
        """Persist a final snapshot on an explicit, bounded critical path."""
        if self._task is None:
            await self._persist(self._run_id, checkpoint, saved_at)
            return True
        self._pending = None
        self._wake.set()
        try:
            await asyncio.wait_for(
                self._persist_one(
                    _PendingCheckpoint(checkpoint, saved_at),
                ),
                timeout=self._flush_timeout_seconds,
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            log.exception(
                "live_checkpoint_critical_write_failed",
                run_id=self._run_id,
                error_type=type(error).__name__,
            )
            return False
        return True

    async def stop(self) -> None:
        task = self._task
        if task is None:
            return
        await self.flush()
        self._stopping = True
        self._wake.set()
        await asyncio.gather(task, return_exceptions=True)
        self._task = None

    async def _wait_until_idle(self) -> None:
        while self._pending is not None or self._inflight:
            await self._idle.wait()

    async def _run(self) -> None:
        while True:
            await self._wake.wait()
            self._wake.clear()
            while True:
                pending = self._pending
                if pending is None:
                    self._idle.set()
                    if self._stopping:
                        return
                    break
                self._pending = None
                self._inflight = True
                retry_pending = False
                try:
                    await self._persist_one(pending)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self._failure_count += 1
                    log.exception(
                        "live_checkpoint_persist_failed",
                        run_id=self._run_id,
                        error_type=type(error).__name__,
                        retry_seconds=self._retry_delay_seconds,
                    )
                    if not self._stopping:
                        await asyncio.sleep(self._retry_delay_seconds)
                        if self._pending is None:
                            self._pending = pending
                            self._idle.clear()
                            retry_pending = True
                finally:
                    self._inflight = False
                    if self._pending is None and not retry_pending:
                        self._idle.set()
                if retry_pending:
                    continue

    async def _persist_one(self, pending: _PendingCheckpoint) -> None:
        started = perf_counter()
        async with self._write_lock:
            await self._persist(self._run_id, pending.checkpoint, pending.saved_at)
        duration_ms = (perf_counter() - started) * 1000
        self._last_duration_ms = duration_ms
        self._persisted_count += 1
        log.info(
            "live_checkpoint_persisted",
            run_id=self._run_id,
            duration_ms=round(duration_ms, 3),
            payload_bytes=_payload_size_bytes(pending.checkpoint),
        )


def _payload_size_bytes(checkpoint: StrategyCheckpoint) -> int:
    return len(
        json.dumps(
            checkpoint.payload,
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    )


__all__ = ["CheckpointWriter", "CheckpointWriterMetrics"]
