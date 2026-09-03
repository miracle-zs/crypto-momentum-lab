"""Durable storage adapters for the research collector.

The collector uses two local seams:

* ``LocalBatchSpool`` is a small write-ahead log.  A batch is not considered
  checkpointable until its Parquet window has been atomically replaced.
* ``ParquetWindowSink`` writes one deterministic file per 15-minute window.
  Replaying a batch therefore rewrites the same window and deduplicates by the
  natural ``(environment, symbol, bucket_start)`` key instead of creating a
  second permanent copy.

The implementation is synchronous on purpose.  The public collector runs
these operations in a worker thread so a Parquet compression call never holds
the asyncio loop used by the Hub client.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import string
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from uuid import UUID, uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from crypto_momentum_lab.domain.market.models import (
    MarketState15s,
)
from crypto_momentum_lab.market_data.hub import (
    MarketStateBatch,
    market_state_from_payload,
    market_state_to_payload,
)
from crypto_momentum_lab.persistence.parquet.datasets import market_state_15s_row
from crypto_momentum_lab.research_collector.models import (
    CollectionBatch,
    CollectorCheckpoint,
    CollectorPaused,
    CollectorStateConflict,
    SelectedSymbol,
    SelectionSnapshot,
    SourceKind,
    require_utc,
)

_STATE_KEY = tuple[str, str, datetime]
_METADATA_COLUMNS = frozenset(
    {
        "selection_reason",
        "gainer_rank",
        "utc_day_return",
        "membership_status",
        "universe_snapshot_id",
        "universe_snapshot_observed_at",
        "source_kind",
        "hub_stream_id",
        "hub_sequence",
        "hub_published_at",
        "collector_received_at",
    }
)


def _timestamp_field(name: str) -> pa.Field:
    return pa.field(name, pa.timestamp("us", tz="UTC"))


_PARQUET_SCHEMA = pa.schema(
    [
        pa.field("schema_version", pa.int32()),
        pa.field("exchange", pa.string()),
        pa.field("environment", pa.string()),
        pa.field("symbol", pa.string()),
        _timestamp_field("bucket_start"),
        _timestamp_field("bucket_end"),
        pa.field("open_price", pa.string()),
        pa.field("high_price", pa.string()),
        pa.field("low_price", pa.string()),
        pa.field("close_price", pa.string()),
        pa.field("trade_count", pa.int64()),
        pa.field("trade_notional", pa.string()),
        pa.field("aggressive_buy_notional", pa.string()),
        pa.field("aggressive_sell_notional", pa.string()),
        pa.field("last_bid_price", pa.string()),
        pa.field("last_ask_price", pa.string()),
        pa.field("spread", pa.string()),
        pa.field("midpoint", pa.string()),
        pa.field("liquidation_count", pa.int64()),
        pa.field("liquidation_notional", pa.string()),
        pa.field("mark_price", pa.string()),
        pa.field("closed_kline_count", pa.int64()),
        _timestamp_field("closed_kline_1m_open_time"),
        _timestamp_field("closed_kline_1m_close_time"),
        pa.field("closed_kline_1m_open_price", pa.string()),
        pa.field("closed_kline_1m_close_price", pa.string()),
        pa.field("source_event_count", pa.int64()),
        _timestamp_field("first_received_at"),
        _timestamp_field("last_received_at"),
        pa.field("data_complete", pa.bool_()),
        pa.field("missing_agg_trade_count", pa.int64()),
        pa.field("selection_reason", pa.string()),
        pa.field("gainer_rank", pa.int32()),
        pa.field("utc_day_return", pa.string()),
        pa.field("membership_status", pa.string()),
        pa.field("universe_snapshot_id", pa.string()),
        _timestamp_field("universe_snapshot_observed_at"),
        pa.field("source_kind", pa.string()),
        pa.field("hub_stream_id", pa.string()),
        pa.field("hub_sequence", pa.int64()),
        _timestamp_field("hub_published_at"),
        _timestamp_field("collector_received_at"),
    ]
)


@dataclass(frozen=True, slots=True)
class SinkAppendResult:
    selected_rows: int
    duplicate_rows: int


@dataclass(frozen=True, slots=True)
class SinkFlushResult:
    committed_rows: int
    committed_sequences: frozenset[int]
    committed_state_keys: frozenset[_STATE_KEY]
    last_bucket_start: datetime | None
    last_symbol: str | None
    files_written: int
    bytes_written: int


@dataclass(frozen=True, slots=True)
class SpoolRecord:
    path: Path
    collection_batch: CollectionBatch
    selection: SelectionSnapshot


class LocalBatchSpool:
    """A bounded, atomic JSON write-ahead log for selected Hub batches."""

    def __init__(self, root: Path, *, max_bytes: int) -> None:
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        self._root = root
        self._pending_root = root / "pending"
        self._max_bytes = max_bytes
        self._pending_root.mkdir(parents=True, exist_ok=True)

    @property
    def root(self) -> Path:
        return self._root

    def pending_bytes(self) -> int:
        return _directory_size(self._pending_root)

    def pending_records(self) -> tuple[SpoolRecord, ...]:
        records: list[SpoolRecord] = []
        for path in sorted(self._pending_root.rglob("*.json")):
            records.append(self._read(path))
        return tuple(records)

    def write(
        self,
        collection_batch: CollectionBatch,
        selection: SelectionSnapshot,
        states: tuple[MarketState15s, ...],
    ) -> SpoolRecord:
        if not states:
            raise ValueError("states must not be empty")
        payload = {
            "schema_version": 1,
            "source_kind": collection_batch.source_kind.value,
            "sequence": collection_batch.sequence,
            "stream_id": collection_batch.stream_id,
            "published_at": collection_batch.batch.published_at.isoformat(),
            "environment": collection_batch.environment,
            "states": [market_state_to_payload(state) for state in states],
            "selection": _selection_to_payload(selection, states),
        }
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        digest = hashlib.sha256(encoded).hexdigest()
        directory = self._pending_root / collection_batch.source_kind.value
        if collection_batch.stream_id:
            directory /= _safe_component(collection_batch.stream_id)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{digest}.json"
        if not path.exists():
            if self.pending_bytes() + len(encoded) > self._max_bytes:
                raise CollectorPaused(
                    "research collector spool limit reached: "
                    f"{self.pending_bytes()} + {len(encoded)} > {self._max_bytes}"
                )
            _atomic_write_bytes(path, encoded)
        return SpoolRecord(
            path=path,
            collection_batch=CollectionBatch(
                batch=MarketStateBatch(
                    sequence=collection_batch.sequence,
                    published_at=collection_batch.batch.published_at,
                    environment=collection_batch.environment,
                    states=states,
                    stream_id=collection_batch.stream_id,
                ),
                source_kind=collection_batch.source_kind,
            ),
            selection=selection,
        )

    def remove(self, record: SpoolRecord) -> None:
        try:
            record.path.unlink()
        except FileNotFoundError:
            return
        _fsync_directory(record.path.parent)

    def _read(self, path: Path) -> SpoolRecord:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise CollectorStateConflict(
                f"cannot read collector spool record {path}: {error}"
            ) from error
        if not isinstance(payload, dict) or payload.get("schema_version") != 1:
            raise CollectorStateConflict(f"unsupported collector spool record: {path}")
        raw_states = payload.get("states")
        if not isinstance(raw_states, list) or not raw_states:
            raise CollectorStateConflict(f"spool states are invalid: {path}")
        states = tuple(
            market_state_from_payload(item)
            for item in raw_states
            if isinstance(item, dict)
        )
        if len(states) != len(raw_states):
            raise CollectorStateConflict(f"spool states are invalid: {path}")
        selection = _selection_from_payload(payload.get("selection"))
        raw_source_kind = payload.get("source_kind")
        if not isinstance(raw_source_kind, str):
            raise CollectorStateConflict(f"spool source_kind is invalid: {path}")
        try:
            source_kind = SourceKind(raw_source_kind)
        except ValueError as error:
            raise CollectorStateConflict(
                f"spool source_kind is invalid: {path}"
            ) from error
        sequence = _require_int(payload.get("sequence"), "sequence")
        published_at = _parse_datetime(payload.get("published_at"), "published_at")
        environment = _require_string(payload.get("environment"), "environment")
        stream_id = payload.get("stream_id")
        if stream_id is not None and not isinstance(stream_id, str):
            raise CollectorStateConflict(f"spool stream_id is invalid: {path}")
        return SpoolRecord(
            path=path,
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
        )


class ParquetWindowSink:
    """Write canonical states as deterministic, compressed time windows."""

    def __init__(
        self,
        root: Path,
        *,
        window_seconds: int = 900,
        late_tolerance_seconds: int = 30,
    ) -> None:
        if window_seconds < 15 or window_seconds % 15 != 0:
            raise ValueError("window_seconds must be a multiple of 15")
        if late_tolerance_seconds < 0:
            raise ValueError("late_tolerance_seconds must not be negative")
        self._root = root
        self._window_seconds = window_seconds
        self._late_tolerance = timedelta(seconds=late_tolerance_seconds)
        self._buffers: dict[Path, dict[_STATE_KEY, dict[str, object]]] = {}
        self._root.mkdir(parents=True, exist_ok=True)

    @property
    def root(self) -> Path:
        return self._root

    def append(
        self,
        collection_batch: CollectionBatch,
        selection: SelectionSnapshot,
    ) -> SinkAppendResult:
        selected_by_symbol = selection.by_symbol
        selected_rows = 0
        duplicate_rows = 0
        for state in collection_batch.states:
            selected = selected_by_symbol.get(state.symbol)
            if selected is None:
                continue
            row = _state_row(state, selected, collection_batch)
            path = self._window_path(state.bucket_start, state.environment)
            rows = self._buffers.get(path)
            if rows is None:
                rows = self._load_existing(path)
                self._buffers[path] = rows
            key = _row_key(row)
            existing = rows.get(key)
            if existing is not None:
                if _same_state_payload(existing, row):
                    duplicate_rows += 1
                    continue
                raise CollectorStateConflict(
                    f"different payloads for market-state key {key!r} in {path}"
                )
            rows[key] = row
            selected_rows += 1
        return SinkAppendResult(
            selected_rows=selected_rows,
            duplicate_rows=duplicate_rows,
        )

    def flush_ready(self, latest_bucket_start: datetime) -> SinkFlushResult:
        latest = require_utc(latest_bucket_start, "latest_bucket_start")
        cutoff = latest - self._late_tolerance
        ready = tuple(
            path
            for path in self._buffers
            if _window_start_from_path(path) + timedelta(seconds=self._window_seconds)
            <= cutoff
        )
        return self._flush_paths(ready)

    def flush_all(self) -> SinkFlushResult:
        return self._flush_paths(tuple(self._buffers))

    def _flush_paths(self, paths: Iterable[Path]) -> SinkFlushResult:
        committed_rows = 0
        committed_sequences: set[int] = set()
        committed_state_keys: set[_STATE_KEY] = set()
        last_bucket_start: datetime | None = None
        last_symbol: str | None = None
        files_written = 0
        bytes_written = 0
        for path in sorted(set(paths), key=lambda item: item.as_posix()):
            rows = self._buffers.get(path)
            if not rows:
                self._buffers.pop(path, None)
                continue
            ordered_rows = sorted(
                rows.values(),
                key=lambda row: (
                    _required_datetime(row, "bucket_start"),
                    _require_string(row.get("symbol"), "symbol"),
                ),
            )
            written = self._write_window(path, ordered_rows)
            committed_rows += len(ordered_rows)
            files_written += 1
            bytes_written += written
            for row in ordered_rows:
                committed_state_keys.add(_row_key(row))
                sequence = row.get("hub_sequence")
                if isinstance(sequence, int):
                    committed_sequences.add(sequence)
                bucket_start = _required_datetime(row, "bucket_start")
                symbol = _require_string(row.get("symbol"), "symbol")
                if last_bucket_start is None or (bucket_start, symbol) > (
                    last_bucket_start,
                    last_symbol or "",
                ):
                    last_bucket_start = bucket_start
                    last_symbol = symbol
            self._buffers.pop(path, None)
        return SinkFlushResult(
            committed_rows=committed_rows,
            committed_sequences=frozenset(committed_sequences),
            committed_state_keys=frozenset(committed_state_keys),
            last_bucket_start=last_bucket_start,
            last_symbol=last_symbol,
            files_written=files_written,
            bytes_written=bytes_written,
        )

    def _window_path(self, bucket_start: datetime, environment: str) -> Path:
        start = require_utc(bucket_start, "bucket_start")
        day_start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        seconds = int((start - day_start).total_seconds())
        window_seconds = (seconds // self._window_seconds) * self._window_seconds
        window_start = day_start + timedelta(seconds=window_seconds)
        return (
            self._root
            / f"environment={_safe_component(environment)}"
            / f"date={window_start.date().isoformat()}"
            / f"hour={window_start.hour:02d}"
            / (
                f"window={window_start.hour:02d}"
                f"{window_start.minute:02d}"
                f"{window_start.second:02d}.parquet"
            )
        )

    @staticmethod
    def _load_existing(path: Path) -> dict[_STATE_KEY, dict[str, object]]:
        if not path.exists():
            return {}
        rows = pq.ParquetFile(path).read().to_pylist()
        return {_row_key(row): row for row in rows}

    @staticmethod
    def _write_window(path: Path, rows: list[dict[str, object]]) -> int:
        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(rows, schema=_PARQUET_SCHEMA)
        temporary_path = path.parent / f".{path.name}.{uuid4().hex}.tmp"
        try:
            pq.write_table(
                table,
                temporary_path,
                compression="zstd",
                compression_level=3,
                use_dictionary=True,
                row_group_size=min(65_536, len(rows)),
            )
            with temporary_path.open("rb") as stream:
                os.fsync(stream.fileno())
            os.replace(temporary_path, path)
            _fsync_directory(path.parent)
        finally:
            try:
                temporary_path.unlink()
            except FileNotFoundError:
                pass
        return path.stat().st_size


class CheckpointStore:
    """Atomic JSON checkpoint adapter."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def load(self, *, environment: str) -> CollectorCheckpoint | None:
        if not self._path.exists():
            return None
        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise CollectorStateConflict(
                f"cannot read collector checkpoint {self._path}: {error}"
            ) from error
        if not isinstance(payload, dict):
            raise CollectorStateConflict("collector checkpoint must be an object")
        checkpoint_environment = _require_string(
            payload.get("environment"),
            "environment",
        )
        if checkpoint_environment != environment:
            raise CollectorStateConflict(
                "checkpoint environment mismatch: "
                f"{checkpoint_environment} != {environment}"
            )
        return CollectorCheckpoint(
            environment=checkpoint_environment,
            stream_id=_optional_string(payload.get("stream_id")),
            last_sequence=_optional_int(payload.get("last_sequence")),
            last_bucket_start=_optional_datetime(payload.get("last_bucket_start")),
            last_symbol=_optional_string(payload.get("last_symbol")),
            schema_version=_require_int(
                payload.get("schema_version"),
                "schema_version",
            ),
            updated_at=_optional_datetime(payload.get("updated_at")),
        )

    def save(self, checkpoint: CollectorCheckpoint) -> None:
        payload = {
            "schema_version": checkpoint.schema_version,
            "environment": checkpoint.environment,
            "stream_id": checkpoint.stream_id,
            "last_sequence": checkpoint.last_sequence,
            "last_bucket_start": (
                None
                if checkpoint.last_bucket_start is None
                else checkpoint.last_bucket_start.isoformat()
            ),
            "last_symbol": checkpoint.last_symbol,
            "updated_at": (
                None
                if checkpoint.updated_at is None
                else checkpoint.updated_at.isoformat()
            ),
        }
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        _atomic_write_bytes(self._path, encoded)


class CapacityState(StrEnum):
    HEALTHY = "healthy"
    WARNING = "warning"
    PAUSED = "paused"


@dataclass(frozen=True, slots=True)
class CapacitySnapshot:
    state: CapacityState
    collector_bytes: int
    disk_free_bytes: int


class CapacityGuard:
    """Enforce both the collector quota and the host free-space reserve."""

    def __init__(
        self,
        root: Path,
        *,
        soft_limit_bytes: int,
        hard_limit_bytes: int,
        global_warning_free_bytes: int,
        global_pause_free_bytes: int,
    ) -> None:
        if soft_limit_bytes <= 0 or hard_limit_bytes <= soft_limit_bytes:
            raise ValueError("invalid collector capacity limits")
        if (
            global_warning_free_bytes <= 0
            or global_pause_free_bytes <= 0
            or global_warning_free_bytes <= global_pause_free_bytes
        ):
            raise ValueError("invalid global capacity limits")
        self._root = root
        self._soft_limit_bytes = soft_limit_bytes
        self._hard_limit_bytes = hard_limit_bytes
        self._global_warning_free_bytes = global_warning_free_bytes
        self._global_pause_free_bytes = global_pause_free_bytes

    def snapshot(self) -> CapacitySnapshot:
        collector_bytes = _directory_size(self._root)
        disk_free_bytes = shutil.disk_usage(self._root).free
        if (
            collector_bytes >= self._hard_limit_bytes
            or disk_free_bytes <= self._global_pause_free_bytes
        ):
            state = CapacityState.PAUSED
        elif (
            collector_bytes >= self._soft_limit_bytes
            or disk_free_bytes < self._global_warning_free_bytes
        ):
            state = CapacityState.WARNING
        else:
            state = CapacityState.HEALTHY
        return CapacitySnapshot(
            state=state,
            collector_bytes=collector_bytes,
            disk_free_bytes=disk_free_bytes,
        )

    def ensure_writable(self) -> CapacitySnapshot:
        snapshot = self.snapshot()
        if snapshot.state is CapacityState.PAUSED:
            raise CollectorPaused(
                "research collector paused by storage guard: "
                f"collector_bytes={snapshot.collector_bytes}, "
                f"disk_free_bytes={snapshot.disk_free_bytes}"
            )
        return snapshot


def _state_row(
    state: MarketState15s,
    selected: SelectedSymbol,
    collection_batch: CollectionBatch,
) -> dict[str, object]:
    row = market_state_15s_row(state)
    row.update(
        {
            "symbol": state.symbol,
            "selection_reason": selected.reason,
            "gainer_rank": selected.rank,
            "utc_day_return": (
                None
                if selected.utc_day_return is None
                else str(selected.utc_day_return)
            ),
            "membership_status": selected.membership_status,
            "universe_snapshot_id": (
                None if selected.snapshot_id is None else str(selected.snapshot_id)
            ),
            "universe_snapshot_observed_at": selected.snapshot_observed_at,
            "source_kind": collection_batch.source_kind.value,
            "hub_stream_id": (
                collection_batch.stream_id
                if collection_batch.source_kind is SourceKind.HUB
                else None
            ),
            "hub_sequence": (
                collection_batch.sequence
                if collection_batch.source_kind is SourceKind.HUB
                else None
            ),
            "hub_published_at": collection_batch.batch.published_at,
            "collector_received_at": datetime.now(UTC),
        }
    )
    return row


def _selection_to_payload(
    selection: SelectionSnapshot,
    states: tuple[MarketState15s, ...],
) -> dict[str, object]:
    symbols = {state.symbol for state in states}
    return {
        "observed_at": selection.observed_at.isoformat(),
        "snapshot_id": (
            None if selection.snapshot_id is None else str(selection.snapshot_id)
        ),
        "retain_all": selection.retain_all,
        "symbols": [
            _selected_to_payload(item)
            for item in selection.symbols
            if item.symbol in symbols
        ],
    }


def _selected_to_payload(item: SelectedSymbol) -> dict[str, object]:
    return {
        "symbol": item.symbol,
        "reason": item.reason,
        "rank": item.rank,
        "utc_day_return": (
            None if item.utc_day_return is None else str(item.utc_day_return)
        ),
        "membership_status": item.membership_status,
        "snapshot_id": None if item.snapshot_id is None else str(item.snapshot_id),
        "snapshot_observed_at": (
            None
            if item.snapshot_observed_at is None
            else item.snapshot_observed_at.isoformat()
        ),
    }


def _selection_from_payload(raw: object) -> SelectionSnapshot:
    if not isinstance(raw, dict):
        raise CollectorStateConflict("spool selection is invalid")
    raw_symbols = raw.get("symbols")
    if not isinstance(raw_symbols, list):
        raise CollectorStateConflict("spool selection symbols are invalid")
    symbols: list[SelectedSymbol] = []
    for item in raw_symbols:
        if not isinstance(item, dict):
            raise CollectorStateConflict("spool selection symbol is invalid")
        snapshot_id = item.get("snapshot_id")
        symbols.append(
            SelectedSymbol(
                symbol=_require_string(item.get("symbol"), "symbol"),
                reason=_require_string(item.get("reason"), "reason"),
                rank=_optional_int(item.get("rank")),
                utc_day_return=_optional_decimal(item.get("utc_day_return")),
                membership_status=_optional_string(item.get("membership_status")),
                snapshot_id=(
                    None
                    if snapshot_id is None
                    else UUID(_require_string(snapshot_id, "snapshot_id"))
                ),
                snapshot_observed_at=_optional_datetime(
                    item.get("snapshot_observed_at")
                ),
            )
        )
    snapshot_id = raw.get("snapshot_id")
    retain_all = raw.get("retain_all", False)
    if not isinstance(retain_all, bool):
        raise CollectorStateConflict("spool selection retain_all is invalid")
    return SelectionSnapshot(
        observed_at=_parse_datetime(raw.get("observed_at"), "observed_at"),
        symbols=tuple(symbols),
        snapshot_id=(
            None
            if snapshot_id is None
            else UUID(_require_string(snapshot_id, "snapshot_id"))
        ),
        retain_all=retain_all,
    )


def _row_key(row: dict[str, object]) -> _STATE_KEY:
    return (
        _require_string(row.get("environment"), "environment"),
        _require_string(row.get("symbol"), "symbol"),
        _required_datetime(row, "bucket_start"),
    )


def _same_state_payload(
    left: dict[str, object],
    right: dict[str, object],
) -> bool:
    keys = set(left) | set(right)
    keys.difference_update(_METADATA_COLUMNS)
    return all(left.get(key) == right.get(key) for key in keys)


def _window_start_from_path(path: Path) -> datetime:
    name = path.stem
    if not name.startswith("window="):
        raise CollectorStateConflict(f"invalid Parquet window path: {path}")
    clock = name.removeprefix("window=")
    if len(clock) == 4:
        hour = int(clock[:2])
        minute = int(clock[2:])
        second = 0
    elif len(clock) == 6:
        hour = int(clock[:2])
        minute = int(clock[2:4])
        second = int(clock[4:])
    else:
        raise CollectorStateConflict(f"invalid Parquet window path: {path}")
    date_part = next(
        part.removeprefix("date=") for part in path.parts if part.startswith("date=")
    )
    return datetime.fromisoformat(
        f"{date_part}T{hour:02d}:{minute:02d}:{second:02d}+00:00"
    )


def _directory_size(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            try:
                total += item.stat().st_size
            except OSError:
                continue
    return total


def _atomic_write_bytes(path: Path, encoded: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.parent / f".{path.name}.{uuid4().hex}.tmp"
    try:
        with temporary_path.open("wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, path)
        _fsync_directory(path.parent)
    finally:
        try:
            temporary_path.unlink()
        except FileNotFoundError:
            pass


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _safe_component(value: str) -> str:
    allowed = string.ascii_letters + string.digits + "-_."
    if not value or any(character not in allowed for character in value):
        raise ValueError(f"unsafe path component: {value!r}")
    return value


def _require_string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CollectorStateConflict(f"{name} must be a non-empty string")
    return value


def _require_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise CollectorStateConflict(f"{name} must be an integer")
    return value


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    return _require_int(value, "optional integer")


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    return _require_string(value, "optional string")


def _required_datetime(row: dict[str, object], name: str) -> datetime:
    value = row.get(name)
    if not isinstance(value, datetime):
        raise CollectorStateConflict(f"{name} must be a datetime")
    return require_utc(value, name)


def _parse_datetime(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise CollectorStateConflict(f"{name} must be an ISO timestamp")
    try:
        return require_utc(datetime.fromisoformat(value), name)
    except ValueError as error:
        raise CollectorStateConflict(f"{name} is invalid") from error


def _optional_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    return _parse_datetime(value, "optional datetime")


def _optional_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise CollectorStateConflict("optional decimal is invalid")
    try:
        return Decimal(str(value))
    except (ArithmeticError, ValueError) as error:
        raise CollectorStateConflict("optional decimal is invalid") from error
