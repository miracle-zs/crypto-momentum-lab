"""Durable single-writer Parquet materializer.

WindowMaterializer is the sole writer to Parquet window datasets:
- Batches and deduplicates incoming canonical market states across windows;
- Flushes windows atomically with fsync;
- Confirms state coverage and commits durable receipts to the journal;
- Reclaims committed journal files.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

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
)


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
        self._superseded_keys: set[_VERSION_KEY] = set()
        self._empty_receipts: list[DurableReceipt] = []
        self._receipt_resolutions: dict[int | str, dict[str, Any]] = {}
        self._persisted_rows = 0

    @property
    def persisted_rows(self) -> int:
        return self._persisted_rows

    def stage_record(self, record: JournalRecord) -> SinkAppendResult | None:
        """Stage one journal record into the Parquet window sink."""
        rec_id: int | str = record.receipt.record_id or record.receipt.sequence
        if record.receipt.is_empty:
            self._empty_receipts.append(record.receipt)
            self._receipt_resolutions[rec_id] = {
                "record_id": record.receipt.record_id,
                "sequence": record.receipt.sequence,
                "stream_id": record.receipt.stream_id,
                "source_kind": record.receipt.source_kind.value,
                "status": "empty",
                "reason": "empty_batch",
                "accepted_keys_count": 0,
                "dropped_keys_count": 0,
                "superseded_keys_count": 0,
                "resolved_at": datetime.now(UTC).isoformat(),
            }
            return None

        append_result = self._sink.append(
            record.collection_batch,
            record.selection,
        )
        if append_result.superseded_version_keys:
            self._superseded_keys.update(append_result.superseded_version_keys)

        if not append_result.accepted_version_keys:
            self._empty_receipts.append(record.receipt)
            status = "rejected" if append_result.dropped_version_keys else "empty"
            reason = (
                "conflict_dropped"
                if append_result.dropped_version_keys
                else "empty_selection"
            )
            self._receipt_resolutions[rec_id] = {
                "record_id": record.receipt.record_id,
                "sequence": record.receipt.sequence,
                "stream_id": record.receipt.stream_id,
                "source_kind": record.receipt.source_kind.value,
                "status": status,
                "reason": reason,
                "accepted_keys_count": 0,
                "dropped_keys_count": len(append_result.dropped_version_keys),
                "superseded_keys_count": 0,
                "dropped_version_keys": [
                    f"{sym}:{bstart.isoformat()}:{digest}"
                    for _, sym, bstart, digest in append_result.dropped_version_keys
                ],
                "resolved_at": datetime.now(UTC).isoformat(),
            }
            return append_result

        status = (
            "partially_materialized"
            if append_result.dropped_version_keys
            else "materialized"
        )
        reason = "accepted_by_sink"
        self._receipt_resolutions[rec_id] = {
            "record_id": record.receipt.record_id,
            "sequence": record.receipt.sequence,
            "stream_id": record.receipt.stream_id,
            "source_kind": record.receipt.source_kind.value,
            "status": status,
            "reason": reason,
            "accepted_keys_count": len(append_result.accepted_version_keys),
            "dropped_keys_count": len(append_result.dropped_version_keys),
            "superseded_keys_count": len(append_result.superseded_version_keys),
            "accepted_version_keys": [
                f"{sym}:{bstart.isoformat()}:{digest}"
                for _, sym, bstart, digest in append_result.accepted_version_keys
            ],
            "resolved_at": datetime.now(UTC).isoformat(),
        }

        record_keys = set(append_result.accepted_version_keys)
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

    def flush_ready(self, latest_bucket_start: datetime) -> SinkFlushResult:
        """Flush windows older than the late tolerance and commit covered receipts."""
        sink_result = self._sink.flush_ready(latest_bucket_start)
        return self._process_flush_result(sink_result, flush_all=False)

    def flush_all(self) -> SinkFlushResult:
        """Flush all buffered windows unconditionally and commit covered receipts."""
        sink_result = self._sink.flush_all()
        return self._process_flush_result(sink_result, flush_all=True)

    def _process_flush_result(
        self,
        sink_result: SinkFlushResult,
        *,
        flush_all: bool,
    ) -> SinkFlushResult:
        self._persisted_rows += sink_result.committed_rows
        committed_keys = sink_result.committed_version_keys

        ready_receipts: list[DurableReceipt] = []

        # Check non-empty staged records
        completed_paths: list[Path] = []
        for path, (receipt, record_keys) in self._staged_records.items():
            covered = self._covered_keys.setdefault(path, set())
            covered.update(committed_keys.intersection(record_keys))
            covered.update(self._superseded_keys.intersection(record_keys))
            if record_keys.issubset(covered):
                ready_receipts.append(receipt)
                completed_paths.append(path)

        for path in completed_paths:
            self._staged_records.pop(path, None)
            self._covered_keys.pop(path, None)
        if not self._staged_records:
            self._superseded_keys.clear()

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

        ready_resolutions: list[dict[str, Any]] = []
        for r in ready_receipts:
            rec_id = r.record_id or r.sequence
            res = self._receipt_resolutions.pop(rec_id, None)
            if res is not None:
                ready_resolutions.append(res)
            else:
                ready_resolutions.append(
                    {
                        "record_id": r.record_id,
                        "sequence": r.sequence,
                        "stream_id": r.stream_id,
                        "source_kind": r.source_kind.value,
                        "status": "materialized",
                        "reason": "committed_on_flush",
                        "resolved_at": datetime.now(UTC).isoformat(),
                    }
                )

        if ready_receipts:
            self._journal.commit_materialization(
                ready_receipts,
                last_bucket_start=sink_result.last_bucket_start,
                last_symbol=sink_result.last_symbol,
                resolutions=ready_resolutions,
            )

        return sink_result
