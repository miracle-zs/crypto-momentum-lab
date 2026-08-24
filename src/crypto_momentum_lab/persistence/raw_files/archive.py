import asyncio
import hashlib
import json
import os
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, BinaryIO
from uuid import NAMESPACE_URL, UUID, uuid5

import zstandard

from crypto_momentum_lab.domain.market.models import (
    ArchiveManifest,
    CaptureRoute,
    CaptureStream,
    DurableArchiveAcknowledgement,
    RawEnvelope,
)

type ArchiveManifestSink = Callable[[ArchiveManifest], Awaitable[None]]
type KnownGapCountProvider = Callable[["PartitionKey"], int]


@dataclass(frozen=True, slots=True)
class PartitionKey:
    utc_date: date
    utc_hour: int
    route: CaptureRoute
    stream: CaptureStream
    symbol: str
    connection_session_id: UUID


def partition_key(envelope: RawEnvelope) -> PartitionKey:
    received = envelope.received_at.astimezone(UTC)
    return PartitionKey(
        utc_date=received.date(),
        utc_hour=received.hour,
        route=envelope.route,
        stream=envelope.stream,
        symbol=envelope.symbol or "_global",
        connection_session_id=envelope.connection_session_id,
    )


def serialize_envelope(envelope: RawEnvelope) -> bytes:
    payload = {
        "schema_version": envelope.schema_version,
        "exchange": envelope.exchange,
        "environment": envelope.environment,
        "route": envelope.route.value,
        "stream": envelope.stream.value,
        "symbol": envelope.symbol,
        "exchange_event_at": (
            None
            if envelope.exchange_event_at is None
            else envelope.exchange_event_at.isoformat()
        ),
        "received_at": envelope.received_at.isoformat(),
        "received_monotonic_ns": envelope.received_monotonic_ns,
        "connection_session_id": str(envelope.connection_session_id),
        "local_sequence": envelope.local_sequence,
        "exchange_sequence": envelope.exchange_sequence,
        "subscription_generation": envelope.subscription_generation,
        "raw_payload": envelope.raw_payload,
        "recovered": envelope.recovered,
    }
    return (
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        + b"\n"
    )


class ZstdJsonlArchive:
    def __init__(
        self,
        *,
        root: Path,
        environment: str,
        capture_version: str,
        manifest_sink: ArchiveManifestSink,
        known_gap_count_provider: KnownGapCountProvider,
        zstd_level: int,
        rotation_uncompressed_bytes: int,
        max_open_writers: int,
        group_commit_max_events: int,
        group_commit_max_milliseconds: int,
    ) -> None:
        if rotation_uncompressed_bytes <= 0:
            raise ValueError("rotation_uncompressed_bytes must be positive")
        if max_open_writers <= 0:
            raise ValueError("max_open_writers must be positive")
        if group_commit_max_events <= 0:
            raise ValueError("group_commit_max_events must be positive")
        if group_commit_max_milliseconds <= 0:
            raise ValueError("group_commit_max_milliseconds must be positive")

        self._root = root
        self._environment = environment
        self._capture_version = capture_version
        self._manifest_sink = manifest_sink
        self._known_gap_count_provider = known_gap_count_provider
        self._zstd_level = zstd_level
        self._rotation_uncompressed_bytes = rotation_uncompressed_bytes
        self._max_open_writers = max_open_writers
        self._group_commit_max_events = group_commit_max_events
        self._group_commit_max_milliseconds = group_commit_max_milliseconds
        self._writers: OrderedDict[PartitionKey, _ArchiveWriter] = OrderedDict()
        self._lock = asyncio.Lock()
        self._closed = False

    async def append(
        self,
        envelope: RawEnvelope,
    ) -> DurableArchiveAcknowledgement:
        row = serialize_envelope(envelope)
        key = partition_key(envelope)
        async with self._lock:
            if self._closed:
                raise RuntimeError("archive is closed")

            writer = self._writers.get(key)
            if writer is not None and writer.should_rotate_for(len(row)):
                await writer.finalize()
                del self._writers[key]
                writer = None

            if writer is None:
                writer = await _ArchiveWriter.create(
                    root=self._root,
                    key=key,
                    exchange=envelope.exchange,
                    environment=self._environment,
                    capture_version=self._capture_version,
                    manifest_sink=self._manifest_sink,
                    known_gap_count_provider=self._known_gap_count_provider,
                    zstd_level=self._zstd_level,
                    rotation_uncompressed_bytes=(
                        self._rotation_uncompressed_bytes
                    ),
                    group_commit_max_events=self._group_commit_max_events,
                    group_commit_max_milliseconds=(
                        self._group_commit_max_milliseconds
                    ),
                    first_sequence=envelope.local_sequence,
                )
                self._writers[key] = writer

            self._writers.move_to_end(key)
            await self._evict_lru_writers()

        return await writer.append(envelope, row)

    async def close(self) -> None:
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            writers = tuple(self._writers.values())
            self._writers.clear()

        errors: list[Exception] = []
        for writer in writers:
            try:
                await writer.finalize()
            except Exception as error:
                errors.append(error)
                await writer.abort()
        if errors:
            raise errors[0]

    async def _evict_lru_writers(self) -> None:
        while len(self._writers) > self._max_open_writers:
            _, writer = self._writers.popitem(last=False)
            try:
                await writer.finalize()
            except Exception:
                await writer.abort()
                raise


class _ArchiveWriter:
    def __init__(
        self,
        *,
        root: Path,
        key: PartitionKey,
        exchange: str,
        environment: str,
        capture_version: str,
        manifest_sink: ArchiveManifestSink,
        known_gap_count_provider: KnownGapCountProvider,
        zstd_level: int,
        rotation_uncompressed_bytes: int,
        group_commit_max_events: int,
        group_commit_max_milliseconds: int,
        first_sequence: int,
    ) -> None:
        self._root = root
        self._key = key
        self._exchange = exchange
        self._environment = environment
        self._capture_version = capture_version
        self._manifest_sink = manifest_sink
        self._known_gap_count_provider = known_gap_count_provider
        self._zstd_level = zstd_level
        self._rotation_uncompressed_bytes = rotation_uncompressed_bytes
        self._group_commit_max_events = group_commit_max_events
        self._group_commit_delay_seconds = group_commit_max_milliseconds / 1000
        safe_exchange = _safe_path_component(exchange, "exchange")
        safe_symbol = _safe_path_component(key.symbol, "symbol")
        safe_stream = _safe_path_component(key.stream.value, "stream")
        self._relative_directory = Path(
            f"exchange={safe_exchange}",
            f"date={key.utc_date.isoformat()}",
            f"stream={safe_stream}",
            f"symbol={safe_symbol}",
            f"hour={key.utc_hour:02d}",
        )
        base = f"{key.connection_session_id}-{first_sequence:020d}"
        self._temporary_relative_path = (
            self._relative_directory / f"{base}.jsonl.zst.tmp"
        )
        self._final_relative_path = self._relative_directory / f"{base}.jsonl.zst"
        self._temporary_path = self._root / self._temporary_relative_path
        self._final_path = self._root / self._final_relative_path
        self._raw_file: BinaryIO | None = None
        self._compressor: Any | None = None
        self._lock = asyncio.Lock()
        self._closed = False
        self._timer_task: asyncio.Task[None] | None = None
        self._pending: list[
            tuple[RawEnvelope, asyncio.Future[DurableArchiveAcknowledgement]]
        ] = []
        self._uncompressed_bytes = 0
        self._row_count = 0
        self._subscription_generation_min: int | None = None
        self._subscription_generation_max: int | None = None
        self._first_exchange_event_at: datetime | None = None
        self._last_exchange_event_at: datetime | None = None
        self._first_received_at: datetime | None = None
        self._last_received_at: datetime | None = None

    @classmethod
    async def create(cls, **kwargs: Any) -> "_ArchiveWriter":
        writer = cls(**kwargs)
        await asyncio.to_thread(writer._open)
        return writer

    @property
    def uncompressed_bytes(self) -> int:
        return self._uncompressed_bytes

    def should_rotate_for(self, row_size: int) -> bool:
        return self._row_count > 0 and (
            self._uncompressed_bytes + row_size
            > self._rotation_uncompressed_bytes
        )

    async def append(
        self,
        envelope: RawEnvelope,
        row: bytes,
    ) -> DurableArchiveAcknowledgement:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[DurableArchiveAcknowledgement] = loop.create_future()
        async with self._lock:
            if self._closed:
                raise RuntimeError("archive writer is closed")
            await asyncio.to_thread(self._write, row)
            self._record_envelope(envelope, len(row))
            self._pending.append((envelope, future))
            if len(self._pending) == 1:
                self._schedule_commit_timer()
            if len(self._pending) >= self._group_commit_max_events:
                await self._commit_locked()
        return await future

    async def finalize(self) -> ArchiveManifest:
        async with self._lock:
            if self._closed:
                raise RuntimeError("archive writer already finalized")
            try:
                await self._commit_locked()
                manifest = await asyncio.to_thread(self._finish)
            except Exception:
                self._closed = True
                await asyncio.to_thread(self._abort_sync)
                raise
            self._closed = True
        await self._manifest_sink(manifest)
        return manifest

    async def abort(self) -> None:
        async with self._lock:
            self._closed = True
            self._cancel_commit_timer()
            for _, future in self._pending:
                if not future.done():
                    future.cancel()
            self._pending.clear()
            await asyncio.to_thread(self._abort_sync)

    def _open(self) -> None:
        self._temporary_path.parent.mkdir(parents=True, exist_ok=True)
        raw_file = self._temporary_path.open("wb")
        try:
            compressor = zstandard.ZstdCompressor(
                level=self._zstd_level
            ).stream_writer(raw_file, closefd=False)
        except Exception:
            raw_file.close()
            self._temporary_path.unlink(missing_ok=True)
            raise
        self._raw_file = raw_file
        self._compressor = compressor

    def _write(self, row: bytes) -> None:
        self._zstd_writer.write(row)

    def _record_envelope(self, envelope: RawEnvelope, row_size: int) -> None:
        self._uncompressed_bytes += row_size
        self._row_count += 1
        generation = envelope.subscription_generation
        if self._subscription_generation_min is None:
            self._subscription_generation_min = generation
            self._subscription_generation_max = generation
        else:
            self._subscription_generation_min = min(
                self._subscription_generation_min,
                generation,
            )
            assert self._subscription_generation_max is not None
            self._subscription_generation_max = max(
                self._subscription_generation_max,
                generation,
            )
        if envelope.exchange_event_at is not None:
            if self._first_exchange_event_at is None:
                self._first_exchange_event_at = envelope.exchange_event_at
            self._last_exchange_event_at = envelope.exchange_event_at
        if self._first_received_at is None:
            self._first_received_at = envelope.received_at
        self._last_received_at = envelope.received_at

    def _schedule_commit_timer(self) -> None:
        if self._timer_task is not None and not self._timer_task.done():
            return
        self._timer_task = asyncio.create_task(self._commit_after_delay())
        self._timer_task.add_done_callback(_consume_task_exception)

    async def _commit_after_delay(self) -> None:
        await asyncio.sleep(self._group_commit_delay_seconds)
        async with self._lock:
            await self._commit_locked()

    async def _commit_locked(self) -> None:
        if not self._pending:
            self._cancel_commit_timer()
            return
        pending = self._pending
        self._pending = []
        self._cancel_commit_timer()
        try:
            await asyncio.to_thread(self._flush_block)
        except Exception as exc:
            for _, future in pending:
                if not future.done():
                    future.set_exception(exc)
            raise

        committed_at = datetime.now(UTC)
        for envelope, future in pending:
            if not future.done():
                future.set_result(
                    DurableArchiveAcknowledgement(
                        connection_session_id=envelope.connection_session_id,
                        local_sequence=envelope.local_sequence,
                        relative_path=self._final_relative_path,
                        committed_at=committed_at,
                    )
                )

    def _cancel_commit_timer(self) -> None:
        current_task = asyncio.current_task()
        if (
            self._timer_task is not None
            and not self._timer_task.done()
            and self._timer_task is not current_task
        ):
            self._timer_task.cancel()
        self._timer_task = None

    def _flush_block(self) -> None:
        self._zstd_writer.flush(zstandard.FLUSH_BLOCK)
        self._file.flush()
        os.fsync(self._file.fileno())

    def _finish(self) -> ArchiveManifest:
        compressor = self._compressor
        raw_file = self._raw_file
        try:
            if compressor is not None:
                compressor.close()
        finally:
            self._compressor = None
            try:
                if raw_file is not None:
                    raw_file.flush()
                    os.fsync(raw_file.fileno())
                    raw_file.close()
            finally:
                self._raw_file = None

        sha256 = _sha256_file(self._temporary_path)
        compressed_bytes = self._temporary_path.stat().st_size
        self._final_path.parent.mkdir(parents=True, exist_ok=True)
        os.replace(self._temporary_path, self._final_path)
        _fsync_directory(self._final_path.parent)

        assert self._subscription_generation_min is not None
        assert self._subscription_generation_max is not None
        assert self._first_received_at is not None
        assert self._last_received_at is not None
        return ArchiveManifest(
            manifest_id=uuid5(
                NAMESPACE_URL,
                f"{self._final_relative_path.as_posix()}:{sha256}",
            ),
            schema_version=1,
            exchange=self._exchange,
            environment=self._environment,
            route=self._key.route,
            stream=self._key.stream,
            symbol=None if self._key.symbol == "_global" else self._key.symbol,
            utc_date=self._key.utc_date,
            utc_hour=self._key.utc_hour,
            relative_path=self._final_relative_path,
            connection_session_id=self._key.connection_session_id,
            subscription_generation_min=self._subscription_generation_min,
            subscription_generation_max=self._subscription_generation_max,
            row_count=self._row_count,
            compressed_bytes=compressed_bytes,
            first_exchange_event_at=self._first_exchange_event_at,
            last_exchange_event_at=self._last_exchange_event_at,
            first_received_at=self._first_received_at,
            last_received_at=self._last_received_at,
            sha256=sha256,
            capture_version=self._capture_version,
            recovery_status="complete",
            known_gap_count=self._known_gap_count_provider(self._key),
            created_at=datetime.now(UTC),
        )

    def _abort_sync(self) -> None:
        compressor = self._compressor
        raw_file = self._raw_file
        self._compressor = None
        self._raw_file = None
        try:
            if compressor is not None:
                compressor.close()
        except Exception:
            pass
        try:
            if raw_file is not None:
                raw_file.close()
        except Exception:
            pass
        try:
            self._temporary_path.unlink(missing_ok=True)
        except OSError:
            pass

    @property
    def _file(self) -> BinaryIO:
        if self._raw_file is None:
            raise RuntimeError("archive writer file is not open")
        return self._raw_file

    @property
    def _zstd_writer(self) -> Any:
        if self._compressor is None:
            raise RuntimeError("archive writer compressor is not open")
        return self._compressor


def _consume_task_exception(task: asyncio.Task[None]) -> None:
    try:
        task.result()
    except asyncio.CancelledError:
        return


def _safe_path_component(value: str, field_name: str) -> str:
    if (
        not value
        or value in {".", ".."}
        or "/" in value
        or "\\" in value
        or any(ord(char) < 32 for char in value)
    ):
        raise ValueError(f"{field_name} must be a safe path component")
    return value


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
