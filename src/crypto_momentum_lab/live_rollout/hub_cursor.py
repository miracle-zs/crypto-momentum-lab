"""Hub cursor recovery and completed-batch acknowledgement ownership.

The caller processes states and persists snapshots. This module does not own
market consumption, checkpoint writes, database sessions or daemon lifecycle.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import TYPE_CHECKING

import structlog

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import StrategyCheckpoint

if TYPE_CHECKING:
    from crypto_momentum_lab.market_data.hub import MarketStateBatch

log = structlog.get_logger(__name__)


def _hub_cursor_from_checkpoint(
    checkpoint: StrategyCheckpoint,
) -> dict[str, str | int] | None:
    raw_cursor = hub_cursor_from_checkpoint_payload(checkpoint)
    if raw_cursor is None:
        return None
    stream_id = raw_cursor.get("stream_id")
    sequence = raw_cursor.get("sequence")
    if not isinstance(stream_id, str) or not stream_id.strip():
        log.warning("live_hub_cursor_checkpoint_ignored", reason="invalid_stream_id")
        return None
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0:
        log.warning("live_hub_cursor_checkpoint_ignored", reason="invalid_sequence")
        return None
    return {"stream_id": stream_id, "sequence": sequence}


def hub_cursor_from_checkpoint_payload(
    checkpoint: StrategyCheckpoint,
) -> Mapping[str, object] | None:
    raw_cursor = checkpoint.payload.get("market_state_hub_cursor")
    if not isinstance(raw_cursor, Mapping):
        return None
    return raw_cursor


def hub_cursor_for_startup(
    checkpoint: StrategyCheckpoint | None,
    *,
    requires_market_recovery: bool,
) -> dict[str, str | int] | None:
    """Only resume a Hub cursor when its epoch remains authoritative.

    A restart that requires durable rewarm must start from the current Hub
    epoch.  Reusing the old cursor would turn the expected stream reset into a
    restart loop because the old in-memory Hub history no longer exists.
    """

    if checkpoint is None or requires_market_recovery:
        return None
    return _hub_cursor_from_checkpoint(checkpoint)


class LiveHubCursorState:
    """Commit a Hub cursor only after every state in its batch is processed."""

    def __init__(self) -> None:
        self.stream_id: str | None = None
        self.sequence: int | None = None
        self._batch_by_state: dict[tuple[str, datetime], tuple[str, int]] = {}
        self._remaining_by_batch: dict[tuple[str, int], int] = {}
        # Symbols the publisher reported as newly entering the monitored pool.
        # Consumed on first report so one entry is announced exactly once.
        self._entered_symbols: frozenset[str] = frozenset()

    def consume_entered_symbol(self, symbol: str) -> bool:
        """Report, once, whether `symbol` just entered the monitored pool."""

        if symbol in self._entered_symbols:
            self._entered_symbols = self._entered_symbols - {symbol}
            return True
        return False

    @property
    def has_cursor(self) -> bool:
        return self.stream_id is not None and self.sequence is not None

    def restore(self, cursor: Mapping[str, str | int]) -> None:
        stream_id = cursor.get("stream_id")
        sequence = cursor.get("sequence")
        if not isinstance(stream_id, str) or not stream_id.strip():
            raise ValueError("hub cursor stream_id must be a non-empty string")
        if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0:
            raise ValueError("hub cursor sequence must be a non-negative integer")
        self.stream_id = stream_id
        self.sequence = sequence

    def observe_batch(self, batch: MarketStateBatch) -> None:
        if batch.stream_id is None:
            return
        if batch.entered_symbols:
            # Carried across batches so a symbol is still recognised as a fresh
            # entry even if its first bucket is not processed in this batch.
            self._entered_symbols = self._entered_symbols | batch.entered_symbols
        batch_key = (batch.stream_id, batch.sequence)
        self._remaining_by_batch[batch_key] = len(batch.states)
        for state in batch.states:
            self._batch_by_state[(state.symbol, state.bucket_start)] = batch_key

    def acknowledge_state(self, state: MarketState15s) -> None:
        batch_key = self._batch_by_state.pop(
            (state.symbol, state.bucket_start),
            None,
        )
        if batch_key is None:
            return
        remaining = self._remaining_by_batch.get(batch_key)
        if remaining is None:
            return
        if remaining > 1:
            self._remaining_by_batch[batch_key] = remaining - 1
            return
        self._remaining_by_batch.pop(batch_key, None)
        stream_id, sequence = batch_key
        if (
            self.stream_id is None
            or self.sequence is None
            or stream_id != self.stream_id
            or sequence > self.sequence
        ):
            self.stream_id = stream_id
            self.sequence = sequence

    def snapshot(self) -> dict[str, str | int] | None:
        if not self.has_cursor or self.stream_id is None or self.sequence is None:
            return None
        return {
            "stream_id": self.stream_id,
            "sequence": self.sequence,
        }
