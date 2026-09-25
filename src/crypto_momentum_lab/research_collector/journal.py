"""Durable journal for accepted market batches and recovery manifests.

ArchiveJournal provides a strict, two-stage write-ahead log:
1. ``accept()``: Ingress validates sequence and fsyncs the batch to the journal.
   Empty-selection batches write a lightweight durable receipt so continuous
   sequences are verifiable upon crash recovery.
   Incrementally tracks pending bytes and updates ``accepted_sequence``.
2. ``pending()``: Returns uncommitted journal records sorted by sequence/time.
3. ``commit_materialization()``: Materializer commits Parquet windows
   and safely removes covered journal files, decrements pending bytes,
   and advances ``materialized_sequence``.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

import structlog

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.market_data.hub import (
    MarketStateBatch,
    market_state_from_payload,
    market_state_to_payload,
)
from crypto_momentum_lab.research_collector.models import (
    CollectionBatch,
    CollectorPaused,
    CollectorStateConflict,
    DurableReceipt,
    JournalRecord,
    SelectionSnapshot,
    SourceKind,
)
from crypto_momentum_lab.research_collector.storage import (
    _atomic_write_bytes,
    _fsync_directory,
    _parse_datetime,
    _require_int,
    _require_string,
    _safe_component,
    _selection_from_payload,
    _selection_to_payload,
)

log = structlog.get_logger()

_STATE_KEY = tuple[str, str, datetime]


def _clean_temporary_files(root: Path) -> None:
    if not root.exists():
        return
    for item in root.rglob(".*.tmp"):
        try:
            item.unlink()
        except OSError:
            pass


def _require_sequence(value: object, name: str = "sequence") -> int:
    """Validate that value is strictly a non-negative integer (not float, bool, or string)."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise CollectorStateConflict(
            f"{name} must be an integer, got {type(value).__name__} ({value!r})"
        )
    if value < 0:
        raise CollectorStateConflict(f"{name} must be non-negative, got {value}")
    return value


class ArchiveJournal:
    """A bounded, atomic JSON write-ahead journal for accepted batches."""

    def __init__(
        self,
        root: Path,
        *,
        environment: str,
        max_bytes: int,
    ) -> None:
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        if not environment.strip():
            raise ValueError("environment must not be empty")
        self._root = root
        self._pending_root = root / "pending"
        self._manifest_path = root / "manifest.json"
        self._environment = environment
        self._max_bytes = max_bytes
        self._pending_bytes = 0
        self._pending_records: dict[Path, JournalRecord] = {}
        self._active_stream_id: str | None = None
        self._accepted_sequence: int | None = None
        self._materialized_sequence: int | None = None
        self._highest_committed_sequence: int | None = None
        self._last_materialized_bucket: datetime | None = None
        self._last_materialized_symbol: str | None = None

        self._pending_root.mkdir(parents=True, exist_ok=True)

    @property
    def root(self) -> Path:
        return self._root

    @property
    def pending_bytes(self) -> int:
        return self._pending_bytes

    @property
    def accepted_sequence(self) -> int | None:
        return self._accepted_sequence

    @property
    def materialized_sequence(self) -> int | None:
        return self._materialized_sequence

    @property
    def last_materialized_bucket(self) -> datetime | None:
        return self._last_materialized_bucket

    @property
    def last_materialized_symbol(self) -> str | None:
        return self._last_materialized_symbol

    @property
    def active_stream_id(self) -> str | None:
        return self._active_stream_id

    def set_active_stream_id(self, stream_id: str | None) -> None:
        self._active_stream_id = stream_id

    def set_cursors(
        self,
        *,
        accepted_sequence: int | None,
        materialized_sequence: int | None,
        last_materialized_bucket: datetime | None = None,
        last_materialized_symbol: str | None = None,
    ) -> None:
        self._accepted_sequence = accepted_sequence
        self._materialized_sequence = materialized_sequence
        self._highest_committed_sequence = materialized_sequence
        self._last_materialized_bucket = last_materialized_bucket
        self._last_materialized_symbol = last_materialized_symbol

    def accept(
        self,
        collection_batch: CollectionBatch,
        selection: SelectionSnapshot,
        selected_states: tuple[MarketState15s, ...],
    ) -> DurableReceipt:
        """Accept one batch and solidifies it into the write-ahead journal."""
        is_empty = len(selected_states) == 0
        now = datetime.now(tz=UTC)
        state_keys = tuple(
            (state.environment, state.symbol, state.bucket_start)
            for state in selected_states
        )
        content_identity: dict[str, object] = {
            "source_kind": collection_batch.source_kind.value,
            "sequence": collection_batch.sequence,
            "stream_id": collection_batch.stream_id,
            "published_at": collection_batch.batch.published_at.isoformat(),
            "environment": collection_batch.environment,
            "is_empty": is_empty,
            "state_keys": [
                f"{env}:{sym}:{dt.isoformat()}" for env, sym, dt in state_keys
            ],
        }
        identity_bytes = json.dumps(
            content_identity, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        record_id = hashlib.sha256(identity_bytes).hexdigest()

        payload: dict[str, object] = {
            "schema_version": 2,
            "record_id": record_id,
            "source_kind": collection_batch.source_kind.value,
            "sequence": collection_batch.sequence,
            "stream_id": collection_batch.stream_id,
            "published_at": collection_batch.batch.published_at.isoformat(),
            "accepted_at": now.isoformat(),
            "environment": collection_batch.environment,
            "is_empty": is_empty,
            "states": [market_state_to_payload(state) for state in selected_states],
            "selection": (
                _selection_to_payload(selection, selected_states)
                if not is_empty
                else None
            ),
        }
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        record_bytes = len(encoded)

        directory = self._pending_root / collection_batch.source_kind.value
        if collection_batch.stream_id:
            directory /= _safe_component(collection_batch.stream_id)
        directory.mkdir(parents=True, exist_ok=True)

        path = directory / f"{record_id}.json"

        if not path.exists():
            if self._pending_bytes + record_bytes > self._max_bytes:
                raise CollectorPaused(
                    "research collector journal limit reached: "
                    f"{self._pending_bytes} + {record_bytes} > {self._max_bytes}"
                )
            _atomic_write_bytes(path, encoded)
            self._pending_bytes += record_bytes

        receipt = DurableReceipt(
            sequence=collection_batch.sequence,
            stream_id=collection_batch.stream_id,
            source_kind=collection_batch.source_kind,
            state_keys=state_keys,
            accepted_at=now,
            is_empty=is_empty,
            record_bytes=record_bytes,
            record_id=record_id,
        )
        record = JournalRecord(
            receipt=receipt,
            collection_batch=CollectionBatch(
                batch=MarketStateBatch(
                    sequence=collection_batch.sequence,
                    published_at=collection_batch.batch.published_at,
                    environment=collection_batch.environment,
                    states=selected_states,
                    stream_id=collection_batch.stream_id,
                ),
                source_kind=collection_batch.source_kind,
            ),
            selection=selection,
            path=path,
        )
        self._pending_records[path] = record

        if collection_batch.source_kind is SourceKind.HUB:
            if self._accepted_sequence is None:
                self._accepted_sequence = collection_batch.sequence
            else:
                self._accepted_sequence = max(
                    self._accepted_sequence, collection_batch.sequence
                )
            if self._active_stream_id is None:
                self._active_stream_id = collection_batch.stream_id

        return receipt

    def get_record(self, receipt: DurableReceipt) -> JournalRecord | None:
        """Find the JournalRecord corresponding to a DurableReceipt."""
        if receipt.record_id:
            for record in self._pending_records.values():
                if record.receipt.record_id == receipt.record_id:
                    return record
        for record in self._pending_records.values():
            if record.receipt == receipt:
                return record
        return None

    def pending_records(self) -> tuple[JournalRecord, ...]:
        """Return all pending uncommitted journal records in order."""
        return tuple(
            sorted(
                self._pending_records.values(),
                key=lambda r: (
                    r.collection_batch.batch.published_at,
                    r.collection_batch.sequence,
                ),
            )
        )

    def commit_materialization(
        self,
        receipts: Iterable[DurableReceipt],
        *,
        last_bucket_start: datetime | None = None,
        last_symbol: str | None = None,
        resolutions: Sequence[Mapping[str, Any]] | None = None,
    ) -> None:
        """Acknowledge completed Parquet materialization.

        Cleans up committed journal records, advances materialized_sequence,
        and persists durable materialization/rejection resolution history.
        """
        committed_receipts = tuple(receipts)
        if not committed_receipts:
            return

        committed_record_ids = {
            r.record_id for r in committed_receipts if r.record_id
        }
        legacy_receipt_keys = {
            (r.source_kind, r.stream_id, r.sequence)
            for r in committed_receipts
            if not r.record_id
        }

        paths_to_remove: list[Path] = []
        for path, record in self._pending_records.items():
            r = record.receipt
            if r.record_id and r.record_id in committed_record_ids:
                paths_to_remove.append(path)
            elif (
                not r.record_id
                and (r.source_kind, r.stream_id, r.sequence) in legacy_receipt_keys
            ):
                paths_to_remove.append(path)

        # Recalculate materialized_sequence as the contiguous covered prefix of remaining records
        hub_committed = [
            r.sequence
            for r in committed_receipts
            if r.source_kind is SourceKind.HUB
            and (
                self._active_stream_id is None or r.stream_id == self._active_stream_id
            )
        ]
        if hub_committed:
            highest_committed = max(hub_committed)
            if self._highest_committed_sequence is None:
                self._highest_committed_sequence = highest_committed
            else:
                self._highest_committed_sequence = max(
                    self._highest_committed_sequence, highest_committed
                )

            remaining_pending_sequences = [
                rec.receipt.sequence
                for path, rec in self._pending_records.items()
                if path not in paths_to_remove
                and rec.receipt.source_kind is SourceKind.HUB
                and (
                    self._active_stream_id is None
                    or rec.receipt.stream_id == self._active_stream_id
                )
                and rec.receipt.sequence is not None
            ]
            if remaining_pending_sequences:
                min_pending = min(remaining_pending_sequences)
                covered_prefix = min_pending - 1
                if self._materialized_sequence is None:
                    self._materialized_sequence = covered_prefix
                else:
                    self._materialized_sequence = max(
                        self._materialized_sequence, covered_prefix
                    )
            else:
                if self._materialized_sequence is None:
                    self._materialized_sequence = self._highest_committed_sequence
                else:
                    self._materialized_sequence = max(
                        self._materialized_sequence, self._highest_committed_sequence
                    )

        if last_bucket_start is not None:
            if (
                self._last_materialized_bucket is None
                or last_bucket_start > self._last_materialized_bucket
            ):
                self._last_materialized_bucket = last_bucket_start
                self._last_materialized_symbol = last_symbol
            elif last_bucket_start == self._last_materialized_bucket:
                self._last_materialized_symbol = (
                    max(
                        self._last_materialized_symbol or "",
                        last_symbol or "",
                    )
                    or None
                )

        # 1. WRITE-AHEAD AUDIT: Persist durable resolution audit log BEFORE deleting any journal files
        if resolutions:
            res_path = self._root / "resolutions.jsonl"
            existing_rec_ids: set[str] = set()
            existing_keys: set[tuple[str, str, int]] = set()
            for item in self.read_resolutions():
                rid = item.get("record_id")
                if rid:
                    existing_rec_ids.add(str(rid))
                sk = item.get("source_kind")
                sid = item.get("stream_id")
                seq = item.get("sequence")
                if sk is not None and sid is not None and seq is not None:
                    parsed_seq = _require_sequence(seq, "existing resolution sequence")
                    existing_keys.add((str(sk), str(sid), parsed_seq))

            to_append: list[dict[str, Any]] = []
            for res in resolutions:
                res_dict = dict(res)
                rid = res_dict.get("record_id")
                sk = res_dict.get("source_kind")
                sid = res_dict.get("stream_id")
                seq = res_dict.get("sequence")
                parsed_seq = _require_sequence(seq, "resolution sequence") if seq is not None else None
                res_key = (
                    (str(sk), str(sid), parsed_seq)
                    if (sk is not None and sid is not None and parsed_seq is not None)
                    else None
                )
                if rid and str(rid) in existing_rec_ids:
                    continue
                if res_key and res_key in existing_keys:
                    continue
                to_append.append(res_dict)
                if rid:
                    existing_rec_ids.add(str(rid))
                if res_key:
                    existing_keys.add(res_key)

            if to_append:
                with res_path.open("a", encoding="utf-8") as f:
                    for item in to_append:
                        f.write(json.dumps(item, sort_keys=True) + "\n")
                    f.flush()
                    os.fsync(f.fileno())
                _fsync_directory(self._root)

        # 2. WRITE-AHEAD AUDIT: Update manifest atomically BEFORE deleting any journal files
        manifest_data = {
            "environment": self._environment,
            "accepted_sequence": self._accepted_sequence,
            "materialized_sequence": self._materialized_sequence,
            "highest_committed_sequence": self._highest_committed_sequence,
            "last_materialized_bucket": (
                self._last_materialized_bucket.isoformat()
                if self._last_materialized_bucket
                else None
            ),
            "last_materialized_symbol": self._last_materialized_symbol,
            "updated_at": datetime.now(UTC).isoformat(),
        }
        temp_manifest = self._root / "manifest.json.tmp"
        with temp_manifest.open("w", encoding="utf-8") as f:
            json.dump(manifest_data, f, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        temp_manifest.replace(self._manifest_path)
        _fsync_directory(self._root)

        # 3. ONLY AFTER audit trail and manifest are safely fsynced, remove committed journal files
        for path in paths_to_remove:
            record = self._pending_records.pop(path, None)
            if record is not None:
                try:
                    size = path.stat().st_size
                except OSError:
                    size = record.receipt.record_bytes
                try:
                    path.unlink()
                    _fsync_directory(path.parent)
                except OSError:
                    pass
                self._pending_bytes = max(0, self._pending_bytes - size)

    def read_resolutions(self) -> list[dict[str, Any]]:
        """Read all durable materialization resolutions from disk."""
        res_path = self._root / "resolutions.jsonl"
        if not res_path.exists():
            return []
        records = []
        try:
            with res_path.open("r", encoding="utf-8") as f:
                for line_no, raw_line in enumerate(f, start=1):
                    line = raw_line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError as error:
                        raise CollectorStateConflict(
                            f"corrupted materialization resolution in {res_path} at line {line_no}: {error}"
                        ) from error
                    if not isinstance(record, dict):
                        raise CollectorStateConflict(
                            f"corrupted materialization resolution in {res_path} at line {line_no}: expected dict, got {type(record).__name__}"
                        )
                    seq = record.get("sequence")
                    if seq is not None:
                        _require_sequence(seq, f"sequence in {res_path} at line {line_no}")
                    records.append(record)
        except OSError as error:
            raise CollectorStateConflict(
                f"cannot read collector materialization resolutions {res_path}: {error}"
            ) from error
        return records

    def recover(
        self,
        *,
        legacy_spool_root: Path | None = None,
    ) -> tuple[JournalRecord, ...]:
        """Recover uncommitted journal records from disk and rebuild byte accounting."""
        _clean_temporary_files(self._pending_root)
        self._pending_records.clear()
        self._pending_bytes = 0

        # Read manifest if available to restore highest_committed_sequence and materialized_sequence
        if self._manifest_path.exists():
            try:
                manifest_content = self._manifest_path.read_text(encoding="utf-8")
                mdata = json.loads(manifest_content)
            except (OSError, json.JSONDecodeError) as error:
                raise CollectorStateConflict(
                    f"cannot read collector manifest {self._manifest_path}: {error}"
                ) from error
            if not isinstance(mdata, dict):
                raise CollectorStateConflict(
                    f"corrupted collector manifest in {self._manifest_path}: expected dict, got {type(mdata).__name__}"
                )
            env = mdata.get("environment")
            if env is not None and str(env).strip() != self._environment:
                raise CollectorStateConflict(
                    f"collector manifest environment mismatch in {self._manifest_path}: expected {self._environment}, got {env}"
                )
            try:
                hcs = mdata.get("highest_committed_sequence")
                if hcs is not None:
                    parsed_hcs = _require_sequence(
                        hcs, f"highest_committed_sequence in {self._manifest_path}"
                    )
                    if self._highest_committed_sequence is None:
                        self._highest_committed_sequence = parsed_hcs
                    else:
                        self._highest_committed_sequence = max(
                            self._highest_committed_sequence, parsed_hcs
                        )
                ms = mdata.get("materialized_sequence")
                if ms is not None:
                    parsed_ms = _require_sequence(
                        ms, f"materialized_sequence in {self._manifest_path}"
                    )
                    if self._materialized_sequence is None:
                        self._materialized_sequence = parsed_ms
                    else:
                        self._materialized_sequence = max(
                            self._materialized_sequence, parsed_ms
                        )
                acs = mdata.get("accepted_sequence")
                if acs is not None:
                    parsed_acs = _require_sequence(
                        acs, f"accepted_sequence in {self._manifest_path}"
                    )
                    if self._accepted_sequence is None:
                        self._accepted_sequence = parsed_acs
                    else:
                        self._accepted_sequence = max(
                            self._accepted_sequence, parsed_acs
                        )
            except CollectorStateConflict:
                raise
            except (ValueError, TypeError) as error:
                raise CollectorStateConflict(
                    f"corrupted sequence values in manifest {self._manifest_path}: {error}"
                ) from error

        existing_resolutions = self.read_resolutions()
        existing_res_keys: set[tuple[str, str, int]] = set()
        existing_record_ids: set[str] = set()
        for res in existing_resolutions:
            rec_id = res.get("record_id")
            if rec_id:
                existing_record_ids.add(str(rec_id))
            sk = res.get("source_kind")
            sid = res.get("stream_id")
            seq = res.get("sequence")
            if sk is not None and sid is not None and seq is not None:
                parsed_seq = _require_sequence(seq, "resolution sequence")
                existing_res_keys.add((str(sk), str(sid), parsed_seq))

        paths: list[Path] = []
        if self._pending_root.exists():
            paths.extend(self._pending_root.rglob("*.json"))

        # Backward compatibility: include any pending records from
        # legacy spool directory
        if legacy_spool_root is not None and legacy_spool_root.exists():
            _clean_temporary_files(legacy_spool_root)
            paths.extend(legacy_spool_root.rglob("*.json"))

        total_bytes = 0
        for path in sorted(set(paths)):
            record = self._read_record(path)
            rec = record.receipt
            res_tuple = (
                (
                    rec.source_kind.value,
                    str(rec.stream_id),
                    _require_sequence(rec.sequence, "receipt sequence"),
                )
                if (rec.source_kind and rec.stream_id is not None and rec.sequence is not None)
                else None
            )
            is_already_committed = (
                (rec.record_id and str(rec.record_id) in existing_record_ids)
                or (res_tuple is not None and res_tuple in existing_res_keys)
                or (
                    rec.source_kind is SourceKind.HUB
                    and self._highest_committed_sequence is not None
                    and rec.sequence is not None
                    and (
                        self._active_stream_id is None
                        or rec.stream_id == self._active_stream_id
                    )
                    and rec.sequence <= self._highest_committed_sequence
                )
            )
            if is_already_committed:
                try:
                    path.unlink()
                    _fsync_directory(path.parent)
                except OSError:
                    pass
                continue

            self._pending_records[path] = record
            try:
                total_bytes += path.stat().st_size
            except OSError:
                total_bytes += record.receipt.record_bytes

        self._pending_bytes = total_bytes

        hub_records = [
            r
            for r in self._pending_records.values()
            if r.collection_batch.source_kind is SourceKind.HUB
        ]
        if hub_records:
            if self._active_stream_id is None:
                self._active_stream_id = hub_records[0].collection_batch.stream_id
            active_seqs = [
                r.collection_batch.sequence
                for r in hub_records
                if r.collection_batch.stream_id == self._active_stream_id
            ]
            if active_seqs:
                max_pending = max(active_seqs)
                if self._accepted_sequence is None:
                    self._accepted_sequence = max_pending
                else:
                    self._accepted_sequence = max(self._accepted_sequence, max_pending)

        return self.pending_records()

    def _read_record(self, path: Path) -> JournalRecord:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise CollectorStateConflict(
                f"cannot read collector journal record {path}: {error}"
            ) from error

        if not isinstance(payload, dict):
            raise CollectorStateConflict(
                f"unsupported collector journal record: {path}"
            )

        schema_version = payload.get("schema_version")
        if schema_version not in (1, 2):
            raise CollectorStateConflict(
                f"unsupported collector journal record version {schema_version}: {path}"
            )

        is_empty = bool(payload.get("is_empty", False))
        raw_states = payload.get("states", [])
        if not isinstance(raw_states, list):
            raise CollectorStateConflict(f"journal states are invalid: {path}")

        if not is_empty and not raw_states:
            raise CollectorStateConflict(
                f"non-empty journal record has no states: {path}"
            )

        states = tuple(
            market_state_from_payload(item)
            for item in raw_states
            if isinstance(item, dict)
        )
        if len(states) != len(raw_states):
            raise CollectorStateConflict(f"journal states are invalid: {path}")

        raw_selection = payload.get("selection")
        if is_empty or raw_selection is None:
            selection = SelectionSnapshot(
                observed_at=datetime.now(tz=UTC),
                symbols=(),
            )
        else:
            selection = _selection_from_payload(raw_selection)

        raw_source_kind = payload.get("source_kind")
        if not isinstance(raw_source_kind, str):
            raise CollectorStateConflict(f"journal source_kind is invalid: {path}")
        try:
            source_kind = SourceKind(raw_source_kind)
        except ValueError as error:
            raise CollectorStateConflict(
                f"journal source_kind is invalid: {path}"
            ) from error

        sequence = _require_int(payload.get("sequence"), "sequence")
        published_at = _parse_datetime(payload.get("published_at"), "published_at")
        environment = _require_string(payload.get("environment"), "environment")
        stream_id = payload.get("stream_id")
        if stream_id is not None and not isinstance(stream_id, str):
            raise CollectorStateConflict(f"journal stream_id is invalid: {path}")

        accepted_at_raw = payload.get("accepted_at")
        accepted_at = (
            _parse_datetime(accepted_at_raw, "accepted_at")
            if accepted_at_raw is not None
            else published_at
        )

        try:
            record_bytes = path.stat().st_size
        except OSError:
            record_bytes = 0

        state_keys = tuple(
            (state.environment, state.symbol, state.bucket_start) for state in states
        )
        record_id = payload.get("record_id")
        if not record_id or not isinstance(record_id, str):
            record_id = path.stem

        receipt = DurableReceipt(
            sequence=sequence,
            stream_id=stream_id,
            source_kind=source_kind,
            state_keys=state_keys,
            accepted_at=accepted_at,
            is_empty=is_empty,
            record_bytes=record_bytes,
            record_id=record_id,
        )
        return JournalRecord(
            receipt=receipt,
            collection_batch=CollectionBatch(
                batch=MarketStateBatch(
                    sequence=sequence,
                    published_at=published_at,
                    environment=environment,
                    states=states,
                    stream_id=stream_id,
                ),
                source_kind=source_kind,
            ),
            selection=selection,
            path=path,
        )
