"""Durable single-writer Parquet materializer.

WindowMaterializer is the sole writer to Parquet window datasets:
- Batches and deduplicates incoming canonical market states across windows;
- Flushes windows atomically with fsync;
- Confirms state coverage and commits durable receipts to the journal;
- Reclaims committed journal files.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import structlog

from crypto_momentum_lab.persistence.parquet.datasets import market_state_15s_row
from crypto_momentum_lab.research_collector.journal import ArchiveJournal
from crypto_momentum_lab.research_collector.models import (
    DurableReceipt,
    JournalRecord,
)
from crypto_momentum_lab.research_collector.storage import (
    _VERSION_KEY,
    ParquetWindowSink,
    SinkAppendResult,
    SinkFlushResult,
    state_payload_digest,
)

log = structlog.get_logger()

_STATE_KEY = tuple[str, str, datetime]


@dataclass(frozen=True, slots=True)
class MaterializerFlushResult:
    committed_rows: int
    files_written: int
    bytes_written: int
    committed_receipts: tuple[DurableReceipt, ...]
    last_bucket_start: datetime | None
    last_symbol: str | None
    sink_result: SinkFlushResult

    @property
    def committed_sequences(self) -> frozenset[int]:
        return self.sink_result.committed_sequences


class WindowMaterializer:
    """Consumes journal records, buffers windows, and commits Parquet files."""

    def __init__(
        self,
        sink: ParquetWindowSink,
        journal: ArchiveJournal,
    ) -> None:
        self._sink = sink
        self._journal = journal
        self._staged_records: dict[Path, tuple[DurableReceipt, set[_VERSION_KEY]]] = {}
        self._covered_keys: dict[Path, set[_VERSION_KEY]] = {}
        self._empty_receipts: list[DurableReceipt] = []
        self._persisted_rows = 0
        self._duplicate_rows = 0

    @property
    def sink(self) -> ParquetWindowSink:
        return self._sink

    @property
    def journal(self) -> ArchiveJournal:
        return self._journal

    @property
    def persisted_rows(self) -> int:
        return self._persisted_rows

    @property
    def duplicate_rows(self) -> int:
        return self._duplicate_rows

    def stage_record(self, record: JournalRecord) -> SinkAppendResult | None:
        """Stage one journal record into the Parquet window sink."""
        if record.receipt.is_empty:
            self._empty_receipts.append(record.receipt)
            return None

        append_result = self._sink.append(
            record.collection_batch,
            record.selection,
        )
        self._duplicate_rows += append_result.duplicate_rows

        record_keys = {
            (
                s.environment,
                s.symbol,
                s.bucket_start,
                state_payload_digest(market_state_15s_row(s)),
            )
            for s in record.collection_batch.states
        }
        self._staged_records[record.path] = (record.receipt, record_keys)
        self._covered_keys.setdefault(record.path, set())
        return append_result

    def stage_records(
        self,
        records: Iterable[JournalRecord],
    ) -> None:
        """Stage multiple recovered records into the materializer buffer."""
        for record in records:
            self.stage_record(record)

    def flush_ready(self, latest_bucket_start: datetime) -> MaterializerFlushResult:
        """Flush windows older than the late tolerance and commit covered receipts."""
        sink_result = self._sink.flush_ready(latest_bucket_start)
        return self._process_flush_result(sink_result, flush_all=False)

    def flush_all(self) -> MaterializerFlushResult:
        """Flush all buffered windows unconditionally and commit covered receipts."""
        sink_result = self._sink.flush_all()
        return self._process_flush_result(sink_result, flush_all=True)

    def _process_flush_result(
        self,
        sink_result: SinkFlushResult,
        *,
        flush_all: bool,
    ) -> MaterializerFlushResult:
        self._persisted_rows += sink_result.committed_rows
        committed_keys = sink_result.committed_version_keys

        ready_receipts: list[DurableReceipt] = []

        # Check non-empty staged records
        completed_paths: list[Path] = []
        for path, (receipt, record_keys) in self._staged_records.items():
            covered = self._covered_keys.setdefault(path, set())
            covered.update(committed_keys.intersection(record_keys))
            if record_keys.issubset(covered):
                ready_receipts.append(receipt)
                completed_paths.append(path)

        for path in completed_paths:
            self._staged_records.pop(path, None)
            self._covered_keys.pop(path, None)

        # Include empty receipts whose sequence precedes all pending staged records
        if flush_all:
            ready_receipts.extend(self._empty_receipts)
            self._empty_receipts.clear()
        else:
            pending_sequences = [
                receipt.sequence
                for receipt, _ in self._staged_records.values()
                if receipt.sequence is not None
            ]
            min_pending_seq = min(pending_sequences) if pending_sequences else None

            ready_empty: list[DurableReceipt] = []
            remaining_empty: list[DurableReceipt] = []
            for empty in self._empty_receipts:
                if empty.sequence is not None:
                    if min_pending_seq is None or empty.sequence < min_pending_seq:
                        ready_empty.append(empty)
                    else:
                        remaining_empty.append(empty)
                else:
                    if not self._staged_records:
                        ready_empty.append(empty)
                    else:
                        remaining_empty.append(empty)

            ready_receipts.extend(ready_empty)
            self._empty_receipts = remaining_empty

        if ready_receipts:
            self._journal.commit_materialization(
                ready_receipts,
                last_bucket_start=sink_result.last_bucket_start,
                last_symbol=sink_result.last_symbol,
            )

        return MaterializerFlushResult(
            committed_rows=sink_result.committed_rows,
            files_written=sink_result.files_written,
            bytes_written=sink_result.bytes_written,
            committed_receipts=tuple(ready_receipts),
            last_bucket_start=sink_result.last_bucket_start,
            last_symbol=sink_result.last_symbol,
            sink_result=sink_result,
        )
