"""Orchestration for the isolated research market-state collector.

The collector is intentionally a small deep module.  Its live seam is one
batch-level Hub subscription; its durable seams are the local write-ahead
spool, the deterministic Parquet sink, and an optional PostgreSQL recovery
adapter.  Strategy code never runs here, so a slow research write cannot block
the trading decision path.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import structlog

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.market_data.hub import (
    MarketStateHubReplayUnavailable,
)
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    RuntimeStateCursor,
)
from crypto_momentum_lab.research_collector.models import (
    CollectionBatch,
    CollectionReceipt,
    CollectionSource,
    CollectorCheckpoint,
    CollectorConfig,
    CollectorHealth,
    CollectorPaused,
    CollectorSequenceGap,
    CollectorStateConflict,
    SelectedSymbol,
    SelectionSnapshot,
    SourceKind,
    SymbolSelector,
    require_utc,
)
from crypto_momentum_lab.research_collector.source import (
    PostgresMarketStateBackfillSource,
)
from crypto_momentum_lab.research_collector.storage import (
    CapacityGuard,
    CapacitySnapshot,
    CapacityState,
    CheckpointStore,
    LocalBatchSpool,
    ParquetWindowSink,
    SinkFlushResult,
    SpoolRecord,
)

log = structlog.get_logger()

_STATE_KEY = tuple[str, str, datetime]


class ResearchStateCollector:
    """Collect selected canonical 15-second states with durable cursors."""

    def __init__(
        self,
        *,
        config: CollectorConfig,
        source: CollectionSource,
        selector: SymbolSelector,
        spool: LocalBatchSpool | None = None,
        sink: ParquetWindowSink | None = None,
        checkpoint_store: CheckpointStore | None = None,
        backfill_source: PostgresMarketStateBackfillSource | None = None,
    ) -> None:
        config.root.mkdir(parents=True, exist_ok=True)
        self._config = config
        self._source = source
        self._selector = selector
        self._spool = spool or LocalBatchSpool(
            config.root / "spool",
            max_bytes=config.max_spool_bytes,
        )
        self._sink = sink or ParquetWindowSink(
            config.root / "parquet",
            window_seconds=config.window_seconds,
            late_tolerance_seconds=config.late_tolerance_seconds,
        )
        self._checkpoint_store = checkpoint_store or CheckpointStore(
            config.root / "checkpoints" / f"{config.environment}.json"
        )
        self._capacity = CapacityGuard(
            config.root,
            soft_limit_bytes=config.soft_limit_bytes,
            hard_limit_bytes=config.hard_limit_bytes,
            global_warning_free_bytes=config.global_warning_free_bytes,
            global_pause_free_bytes=config.global_pause_free_bytes,
        )
        self._backfill_source = backfill_source
        self._checkpoint: CollectorCheckpoint | None = None
        self._pending_records: dict[Path, SpoolRecord] = {}
        self._record_committed_keys: dict[Path, set[_STATE_KEY]] = {}
        self._staged_paths: set[Path] = set()
        self._active_stream_id: str | None = None
        self._sequence_baseline: int | None = None
        self._last_seen_sequence: int | None = None
        self._last_received_bucket: datetime | None = None
        self._last_persisted_bucket: datetime | None = None
        self._last_persisted_symbol: str | None = None
        self._selected_rows = 0
        self._persisted_rows = 0
        self._duplicate_rows = 0
        self._gap_count = 0
        self._connected = False
        self._paused = False
        self._stopping = False
        self._initialized = False
        self._capacity_snapshot: CapacitySnapshot | None = None
        self._last_capacity_refresh = 0.0

    @property
    def config(self) -> CollectorConfig:
        return self._config

    async def initialize(self) -> None:
        """Load the checkpoint and make every pending spool row durable."""

        if self._initialized:
            return
        checkpoint = await asyncio.to_thread(
            self._checkpoint_store.load,
            environment=self._config.environment,
        )
        if checkpoint is None:
            checkpoint = CollectorCheckpoint(
                environment=self._config.environment,
            )
        self._checkpoint = checkpoint
        self._active_stream_id = checkpoint.stream_id
        self._last_seen_sequence = checkpoint.last_sequence
        self._last_persisted_bucket = checkpoint.last_bucket_start
        self._last_persisted_symbol = checkpoint.last_symbol

        pending = await asyncio.to_thread(self._spool.pending_records)
        pending = tuple(sorted(pending, key=_spool_sort_key))
        pending_hub_records = tuple(
            record
            for record in pending
            if record.collection_batch.source_kind is SourceKind.HUB
        )
        if (
            self._active_stream_id is None
            and pending_hub_records
            and pending_hub_records[0].collection_batch.stream_id is not None
        ):
            self._active_stream_id = pending_hub_records[0].collection_batch.stream_id
        pending_sequences = tuple(
            record.collection_batch.sequence
            for record in pending_hub_records
            if record.collection_batch.stream_id == self._active_stream_id
        )
        if pending_sequences:
            if self._last_seen_sequence is None:
                self._sequence_baseline = min(pending_sequences) - 1
                self._last_seen_sequence = max(pending_sequences)
            else:
                self._last_seen_sequence = max(
                    self._last_seen_sequence,
                    max(pending_sequences),
                )
        for record in pending:
            self._validate_collection_batch(record.collection_batch)
            self._pending_records[record.path] = record
            self._record_committed_keys[record.path] = set()
            self._observe_received_states(record.collection_batch.states)
            await asyncio.to_thread(
                self._sink.append,
                record.collection_batch,
                record.selection,
            )
            self._staged_paths.add(record.path)
        if pending:
            await self._flush_all_buffers()
        else:
            await self._save_checkpoint()
        self._set_source_cursor_from_checkpoint()
        self._initialized = True
        log.info(
            "research_collector_initialized",
            environment=self._config.environment,
            pending_records=len(pending),
            last_sequence=self._checkpoint.last_sequence,
        )

    async def ingest(self, collection_batch: CollectionBatch) -> CollectionReceipt:
        """Validate, select, spool, and durably stage one canonical batch."""

        await self.initialize()
        self._validate_collection_batch(collection_batch)
        if collection_batch.source_kind is SourceKind.HUB:
            if collection_batch.sequence <= 0:
                raise CollectorSequenceGap("Hub sequence must be positive")
            await self._prepare_hub_stream(collection_batch)
            if not self._accept_hub_cursor(collection_batch):
                self._observe_received_states(collection_batch.states)
                return _receipt_for_skipped_batch(collection_batch)

        self._observe_received_states(collection_batch.states)
        observed_at = min(state.bucket_start for state in collection_batch.states)
        selection = await self._selector.selection_at(observed_at)
        selection = _materialize_selection(
            selection,
            collection_batch.states,
        )
        selected_states = tuple(
            state
            for state in collection_batch.states
            if state.symbol in selection.by_symbol
        )
        if not selected_states:
            flush_result = await self._flush_ready(
                max(state.bucket_start for state in collection_batch.states)
            )
            if collection_batch.source_kind is SourceKind.HUB:
                self._last_seen_sequence = collection_batch.sequence
                await self._save_checkpoint()
            return _receipt_for_empty_selection(
                collection_batch,
                flush_result=flush_result,
            )

        self._ensure_capacity()
        record = await asyncio.to_thread(
            self._spool.write,
            collection_batch,
            selection,
            selected_states,
        )
        self._pending_records[record.path] = record
        self._record_committed_keys.setdefault(record.path, set())
        append_result = await asyncio.to_thread(
            self._sink.append,
            record.collection_batch,
            selection,
        )
        self._staged_paths.add(record.path)
        self._selected_rows += len(selected_states)
        self._duplicate_rows += append_result.duplicate_rows
        if collection_batch.source_kind is SourceKind.HUB:
            self._last_seen_sequence = collection_batch.sequence

        latest_bucket = max(state.bucket_start for state in selected_states)
        flush_result = await self._flush_ready(latest_bucket)
        await self._save_checkpoint()
        return _receipt_for_ingested_batch(
            collection_batch,
            selected_rows=len(selected_states),
            duplicate_rows=append_result.duplicate_rows,
            flush_result=flush_result,
        )

    async def run(self) -> None:
        """Run until stopped, recovering replay gaps from PostgreSQL."""

        await self.initialize()
        while not self._stopping:
            try:
                await self._consume_source_once()
                if not self._stopping:
                    await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                raise
            except MarketStateHubReplayUnavailable as error:
                self._connected = False
                self._gap_count += 1
                if self._backfill_source is None:
                    raise
                await self._recover_replay_gap(error)
            except CollectorPaused:
                self._connected = False
                await self._wait_for_capacity()

    async def stop(self) -> None:
        """Stop the source and flush what can be made durable."""

        if self._stopping:
            return
        self._stopping = True
        stop = getattr(self._source, "stop", None)
        if callable(stop):
            stop()
        if not self._initialized:
            return
        try:
            await self._flush_all_buffers()
        except CollectorPaused:
            log.error(
                "research_collector_stop_flush_paused",
                environment=self._config.environment,
                pending_records=len(self._pending_records),
            )
        await self._save_checkpoint()

    async def health(self) -> CollectorHealth:
        await self.initialize()
        snapshot = await asyncio.to_thread(self._capacity.snapshot)
        self._capacity_snapshot = snapshot
        self._last_capacity_refresh = time.monotonic()
        pending_bytes = await asyncio.to_thread(self._spool.pending_bytes)
        checkpoint = self._require_checkpoint()
        return CollectorHealth(
            environment=self._config.environment,
            connected=self._connected,
            last_received_bucket=self._last_received_bucket,
            last_persisted_bucket=self._last_persisted_bucket,
            last_sequence=checkpoint.last_sequence,
            selected_rows=self._selected_rows,
            persisted_rows=self._persisted_rows,
            duplicate_rows=self._duplicate_rows,
            gap_count=self._gap_count,
            pending_spool_files=len(self._pending_records),
            pending_spool_bytes=pending_bytes,
            collector_bytes=snapshot.collector_bytes,
            disk_free_bytes=snapshot.disk_free_bytes,
            warning=snapshot.state is CapacityState.WARNING,
            paused=self._paused or snapshot.state is CapacityState.PAUSED,
        )

    async def _consume_source_once(self) -> None:
        iterator = self._source.batches()
        self._connected = True
        try:
            while not self._stopping:
                try:
                    batch = await iterator.__anext__()
                except StopAsyncIteration:
                    return
                await self.ingest(CollectionBatch(batch=batch))
        finally:
            self._connected = False
            close = getattr(iterator, "aclose", None)
            if callable(close):
                await close()

    async def _recover_replay_gap(
        self,
        error: MarketStateHubReplayUnavailable,
    ) -> None:
        backfill = self._backfill_source
        if backfill is None:
            raise error
        until = await backfill.latest_bucket()
        if until is None:
            raise CollectorStateConflict(
                "Hub replay is unavailable and PostgreSQL has no recovery bucket"
            ) from error
        checkpoint = self._require_checkpoint()
        cursor = RuntimeStateCursor(
            bucket_start=checkpoint.last_bucket_start,
            symbol=checkpoint.last_symbol,
        )
        recovered_batches = 0
        recovered_rows = 0
        async for batch in backfill.batches_after(cursor, until=until):
            recovered_batches += 1
            recovered_rows += len(batch.states)
            await self.ingest(
                CollectionBatch(
                    batch=batch,
                    source_kind=SourceKind.POSTGRES_BACKFILL,
                )
            )
        await self._flush_all_buffers()
        checkpoint = self._require_checkpoint()
        stream_id = error.stream_id
        latest_sequence = error.latest_sequence
        if stream_id is None or latest_sequence is None:
            raise CollectorStateConflict(
                "Hub replay error did not contain a resumable stream cursor"
            ) from error
        if self._pending_records:
            raise CollectorStateConflict(
                "replay recovery completed with pending spool records"
            )
        self._active_stream_id = stream_id
        self._last_seen_sequence = latest_sequence
        self._checkpoint = replace(
            checkpoint,
            stream_id=stream_id,
            last_sequence=latest_sequence,
            updated_at=datetime.now(UTC),
        )
        await asyncio.to_thread(self._checkpoint_store.save, self._checkpoint)
        resume = getattr(self._source, "resume_after_recovery", None)
        if not callable(resume):
            raise CollectorStateConflict(
                "source cannot accept a recovered Hub cursor"
            ) from error
        resume(stream_id=stream_id, sequence=latest_sequence)
        log.warning(
            "research_collector_recovered_hub_gap",
            environment=self._config.environment,
            recovered_batches=recovered_batches,
            recovered_rows=recovered_rows,
            recovery_until=until.isoformat(),
            resumed_sequence=latest_sequence,
        )

    async def _wait_for_capacity(self) -> None:
        if not self._paused:
            self._paused = True
            log.error(
                "research_collector_paused_by_capacity",
                environment=self._config.environment,
            )
        while not self._stopping:
            snapshot = await asyncio.to_thread(self._capacity.snapshot)
            self._capacity_snapshot = snapshot
            self._last_capacity_refresh = time.monotonic()
            if snapshot.state is not CapacityState.PAUSED:
                try:
                    await self._flush_all_buffers()
                except CollectorPaused:
                    continue
                self._paused = False
                log.info(
                    "research_collector_capacity_recovered",
                    environment=self._config.environment,
                )
                return
            await asyncio.sleep(min(30.0, self._config.capacity_check_interval_seconds))

    async def _flush_ready(self, latest_bucket: datetime) -> SinkFlushResult:
        self._ensure_capacity()
        result = await asyncio.to_thread(
            self._sink.flush_ready,
            latest_bucket,
        )
        await self._apply_flush_result(result)
        return result

    async def _flush_all_buffers(self) -> SinkFlushResult:
        await self._restage_pending_records()
        self._ensure_capacity()
        result = await asyncio.to_thread(self._sink.flush_all)
        await self._apply_flush_result(result)
        await self._save_checkpoint()
        return result

    async def _apply_flush_result(self, result: SinkFlushResult) -> None:
        self._persisted_rows += result.committed_rows
        if result.last_bucket_start is not None:
            if (
                self._last_persisted_bucket is None
                or result.last_bucket_start > self._last_persisted_bucket
            ):
                self._last_persisted_bucket = result.last_bucket_start
                self._last_persisted_symbol = result.last_symbol
            elif result.last_bucket_start == self._last_persisted_bucket:
                self._last_persisted_symbol = (
                    max(
                        self._last_persisted_symbol or "",
                        result.last_symbol or "",
                    )
                    or None
                )
        committed = result.committed_state_keys
        for path, record in tuple(self._pending_records.items()):
            state_keys = {_state_key(state) for state in record.collection_batch.states}
            covered = self._record_committed_keys.setdefault(path, set())
            covered.update(committed.intersection(state_keys))
            if not state_keys.issubset(covered):
                continue
            await asyncio.to_thread(self._spool.remove, record)
            self._pending_records.pop(path, None)
            self._record_committed_keys.pop(path, None)
            self._staged_paths.discard(path)

    async def _restage_pending_records(self) -> None:
        for path, record in tuple(self._pending_records.items()):
            if path in self._staged_paths:
                continue
            await asyncio.to_thread(
                self._sink.append,
                record.collection_batch,
                record.selection,
            )
            self._staged_paths.add(path)

    async def _save_checkpoint(self) -> None:
        checkpoint = self._require_checkpoint()
        durable_sequence = self._durable_sequence()
        self._checkpoint = replace(
            checkpoint,
            stream_id=self._active_stream_id,
            last_sequence=durable_sequence,
            last_bucket_start=self._last_persisted_bucket
            if self._last_persisted_bucket is not None
            else checkpoint.last_bucket_start,
            last_symbol=(
                checkpoint.last_symbol
                if self._last_persisted_bucket is None
                else self._last_persisted_symbol
            ),
            updated_at=datetime.now(UTC),
        )
        await asyncio.to_thread(self._checkpoint_store.save, self._checkpoint)

    def _durable_sequence(self) -> int | None:
        checkpoint = self._require_checkpoint()
        if self._last_seen_sequence is None:
            return checkpoint.last_sequence
        if checkpoint.stream_id != self._active_stream_id:
            base: int | None = self._sequence_baseline
        else:
            base = (
                checkpoint.last_sequence
                if checkpoint.last_sequence is not None
                else self._sequence_baseline
            )
        pending = {
            record.collection_batch.sequence
            for record in self._pending_records.values()
            if (
                record.collection_batch.source_kind is SourceKind.HUB
                and record.collection_batch.stream_id == self._active_stream_id
            )
        }
        if base is None:
            return self._last_seen_sequence if not pending else None
        sequence = base
        while sequence < self._last_seen_sequence and sequence + 1 not in pending:
            sequence += 1
        return sequence

    async def _prepare_hub_stream(
        self,
        collection_batch: CollectionBatch,
    ) -> None:
        stream_id = collection_batch.stream_id
        if stream_id is None:
            return
        if self._active_stream_id is None:
            self._active_stream_id = stream_id
            return
        if stream_id == self._active_stream_id:
            return
        await self._flush_all_buffers()
        self._active_stream_id = stream_id
        self._sequence_baseline = None
        self._last_seen_sequence = None
        self._checkpoint = replace(
            self._require_checkpoint(),
            stream_id=stream_id,
            last_sequence=None,
        )
        log.warning(
            "research_collector_hub_stream_changed",
            environment=self._config.environment,
            stream_id=stream_id,
        )

    def _accept_hub_cursor(self, collection_batch: CollectionBatch) -> bool:
        if self._active_stream_id is None and collection_batch.stream_id is not None:
            self._active_stream_id = collection_batch.stream_id
        if self._last_seen_sequence is None:
            if self._sequence_baseline is None:
                self._sequence_baseline = collection_batch.sequence - 1
            return True
        if collection_batch.sequence <= self._last_seen_sequence:
            log.warning(
                "research_collector_duplicate_batch_ignored",
                environment=self._config.environment,
                sequence=collection_batch.sequence,
                last_sequence=self._last_seen_sequence,
            )
            return False
        expected = self._last_seen_sequence + 1
        if collection_batch.sequence != expected:
            self._gap_count += 1
            raise CollectorSequenceGap(
                "research collector sequence gap: "
                f"expected={expected}, received={collection_batch.sequence}"
            )
        return True

    def _validate_collection_batch(self, collection_batch: CollectionBatch) -> None:
        if collection_batch.environment != self._config.environment:
            raise CollectorStateConflict(
                "collector environment mismatch: "
                f"{collection_batch.environment} != "
                f"{self._config.environment}"
            )
        if not collection_batch.states:
            raise CollectorStateConflict("collection batch must not be empty")
        seen: set[_STATE_KEY] = set()
        for state in collection_batch.states:
            if state.environment != self._config.environment:
                raise CollectorStateConflict("market-state environment mismatch")
            _validate_canonical_state(state)
            key = _state_key(state)
            if key in seen:
                raise CollectorStateConflict(
                    f"duplicate market-state key in batch: {key!r}"
                )
            seen.add(key)

    def _observe_received_states(
        self,
        states: tuple[MarketState15s, ...],
    ) -> None:
        latest = max(state.bucket_start for state in states)
        if self._last_received_bucket is None or latest > self._last_received_bucket:
            self._last_received_bucket = latest

    def _ensure_capacity(self) -> CapacitySnapshot:
        snapshot = self._capacity.ensure_writable()
        self._capacity_snapshot = snapshot
        self._last_capacity_refresh = time.monotonic()
        if snapshot.state is CapacityState.WARNING:
            log.warning(
                "research_collector_capacity_warning",
                environment=self._config.environment,
                collector_bytes=snapshot.collector_bytes,
                disk_free_bytes=snapshot.disk_free_bytes,
            )
        return snapshot

    def _set_source_cursor_from_checkpoint(self) -> None:
        checkpoint = self._require_checkpoint()
        set_cursor = getattr(self._source, "set_resume_cursor", None)
        if callable(set_cursor):
            set_cursor(
                stream_id=checkpoint.stream_id,
                sequence=checkpoint.last_sequence,
            )

    def _require_checkpoint(self) -> CollectorCheckpoint:
        if self._checkpoint is None:
            raise RuntimeError("collector has not been initialized")
        return self._checkpoint


def _materialize_selection(
    selection: SelectionSnapshot,
    states: tuple[MarketState15s, ...],
) -> SelectionSnapshot:
    if not selection.retain_all:
        return selection
    existing = selection.by_symbol
    selected = tuple(
        existing.get(
            state.symbol,
            SelectedSymbol(
                symbol=state.symbol,
                reason="all_symbols",
                snapshot_id=selection.snapshot_id,
                snapshot_observed_at=selection.observed_at,
            ),
        )
        for state in states
    )
    unique: dict[str, SelectedSymbol] = {item.symbol: item for item in selected}
    return SelectionSnapshot(
        observed_at=selection.observed_at,
        symbols=tuple(sorted(unique.values(), key=lambda item: item.symbol)),
        snapshot_id=selection.snapshot_id,
    )


def _validate_canonical_state(state: MarketState15s) -> None:
    start = require_utc(state.bucket_start, "bucket_start")
    end = require_utc(state.bucket_end, "bucket_end")
    if state.bucket_start.utcoffset() != timedelta(0):
        raise CollectorStateConflict("bucket_start must use UTC")
    if state.bucket_end.utcoffset() != timedelta(0):
        raise CollectorStateConflict("bucket_end must use UTC")
    if start.second % 15 != 0 or start.microsecond != 0:
        raise CollectorStateConflict(
            f"bucket_start is not aligned to a 15-second boundary: {start!r}"
        )
    if end - start != timedelta(seconds=15):
        raise CollectorStateConflict(
            f"bucket duration must be 15 seconds: {start!r} -> {end!r}"
        )


def _state_key(state: MarketState15s) -> _STATE_KEY:
    return (
        state.environment,
        state.symbol,
        require_utc(state.bucket_start, "bucket_start"),
    )


def _spool_sort_key(record: SpoolRecord) -> tuple[str, str, int, datetime, str]:
    first_state = min(
        record.collection_batch.states,
        key=lambda state: (state.bucket_start, state.symbol),
    )
    return (
        record.collection_batch.source_kind.value,
        record.collection_batch.stream_id or "",
        record.collection_batch.sequence,
        first_state.bucket_start,
        first_state.symbol,
    )


def _receipt_for_skipped_batch(
    collection_batch: CollectionBatch,
) -> CollectionReceipt:
    return CollectionReceipt(
        source_kind=collection_batch.source_kind,
        sequence=(
            collection_batch.sequence
            if collection_batch.source_kind is SourceKind.HUB
            else None
        ),
        selected_rows=0,
        skipped_rows=len(collection_batch.states),
    )


def _receipt_for_empty_selection(
    collection_batch: CollectionBatch,
    *,
    flush_result: SinkFlushResult,
) -> CollectionReceipt:
    return CollectionReceipt(
        source_kind=collection_batch.source_kind,
        sequence=(
            collection_batch.sequence
            if collection_batch.source_kind is SourceKind.HUB
            else None
        ),
        selected_rows=0,
        committed_rows=flush_result.committed_rows,
        skipped_rows=len(collection_batch.states),
    )


def _receipt_for_ingested_batch(
    collection_batch: CollectionBatch,
    *,
    selected_rows: int,
    duplicate_rows: int,
    flush_result: SinkFlushResult,
) -> CollectionReceipt:
    return CollectionReceipt(
        source_kind=collection_batch.source_kind,
        sequence=(
            collection_batch.sequence
            if collection_batch.source_kind is SourceKind.HUB
            else None
        ),
        selected_rows=selected_rows,
        duplicate_rows=duplicate_rows,
        committed_rows=flush_result.committed_rows,
        committed_sequence=(
            collection_batch.sequence
            if collection_batch.source_kind is SourceKind.HUB
            and collection_batch.sequence in flush_result.committed_sequences
            else None
        ),
    )
