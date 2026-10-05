import hashlib
import json
import os
import sys
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pyarrow as pa
import pyarrow.parquet as pq

from crypto_momentum_lab.build_info import resolve_code_commit
from crypto_momentum_lab.domain.market.models import (
    MarketState15s,
    NormalizedAggTrade,
    NormalizedBookTicker,
    NormalizedKline1m,
    NormalizedLiquidation,
    NormalizedMarketEvent,
    NormalizedMarkPrice,
)

_DERIVED_DATASET_SCHEMA_VERSION = 2
_DECIMAL_TYPE = pa.decimal128(38, 18)
_TIMESTAMP_TYPE = pa.timestamp("us", tz="UTC")
_PARQUET_COMPRESSION = "zstd"
_PARQUET_ROW_GROUP_SIZE = 65_536
_WRITE_BATCH_ROWS = 4_096


class DatasetName(StrEnum):
    MARKET_EVENTS = "market_events"
    MARKET_STATES_15S = "market_states_15s"


def _field(name: str, data_type: pa.DataType, *, nullable: bool = True) -> pa.Field:
    return pa.field(name, data_type, nullable=nullable)


_MARKET_EVENTS_SCHEMA = pa.schema(
    [
        _field("schema_version", pa.int32(), nullable=False),
        _field("exchange", pa.string(), nullable=False),
        _field("environment", pa.string(), nullable=False),
        _field("event_at", _TIMESTAMP_TYPE, nullable=False),
        _field("received_at", _TIMESTAMP_TYPE, nullable=False),
        _field("source_connection_session_id", pa.string(), nullable=False),
        _field("source_local_sequence", pa.int64(), nullable=False),
        _field("source_stream", pa.string(), nullable=False),
        _field("event_type", pa.string(), nullable=False),
        _field("trade_id", pa.string()),
        _field("price", _DECIMAL_TYPE),
        _field("quantity", _DECIMAL_TYPE),
        _field("notional", _DECIMAL_TYPE),
        _field("aggressor_side", pa.string()),
        _field("update_id", pa.string()),
        _field("bid_price", _DECIMAL_TYPE),
        _field("bid_quantity", _DECIMAL_TYPE),
        _field("ask_price", _DECIMAL_TYPE),
        _field("ask_quantity", _DECIMAL_TYPE),
        _field("mark_price", _DECIMAL_TYPE),
        _field("index_price", _DECIMAL_TYPE),
        _field("estimated_settle_price", _DECIMAL_TYPE),
        _field("funding_rate", _DECIMAL_TYPE),
        _field("next_funding_at", _TIMESTAMP_TYPE),
        _field("open_time", _TIMESTAMP_TYPE),
        _field("close_time", _TIMESTAMP_TYPE),
        _field("open_price", _DECIMAL_TYPE),
        _field("high_price", _DECIMAL_TYPE),
        _field("low_price", _DECIMAL_TYPE),
        _field("close_price", _DECIMAL_TYPE),
        _field("volume", _DECIMAL_TYPE),
        _field("quote_volume", _DECIMAL_TYPE),
        _field("kline_trade_count", pa.int64()),
        _field("closed", pa.bool_()),
        _field("order_side", pa.string()),
        _field("average_price", _DECIMAL_TYPE),
        _field("trade_time", _TIMESTAMP_TYPE),
    ],
    metadata={
        b"cml.dataset": DatasetName.MARKET_EVENTS.value.encode(),
        b"cml.schema_version": str(_DERIVED_DATASET_SCHEMA_VERSION).encode(),
    },
)

_MARKET_STATES_15S_SCHEMA = pa.schema(
    [
        _field("schema_version", pa.int32(), nullable=False),
        _field("exchange", pa.string(), nullable=False),
        _field("environment", pa.string(), nullable=False),
        _field("bucket_start", _TIMESTAMP_TYPE, nullable=False),
        _field("bucket_end", _TIMESTAMP_TYPE, nullable=False),
        _field("open_price", _DECIMAL_TYPE),
        _field("high_price", _DECIMAL_TYPE),
        _field("low_price", _DECIMAL_TYPE),
        _field("close_price", _DECIMAL_TYPE),
        _field("trade_count", pa.int64(), nullable=False),
        _field("trade_notional", _DECIMAL_TYPE, nullable=False),
        _field("aggressive_buy_notional", _DECIMAL_TYPE, nullable=False),
        _field("aggressive_sell_notional", _DECIMAL_TYPE, nullable=False),
        _field("last_bid_price", _DECIMAL_TYPE),
        _field("last_ask_price", _DECIMAL_TYPE),
        _field("spread", _DECIMAL_TYPE),
        _field("midpoint", _DECIMAL_TYPE),
        _field("liquidation_count", pa.int64(), nullable=False),
        _field("liquidation_notional", _DECIMAL_TYPE, nullable=False),
        _field("mark_price", _DECIMAL_TYPE),
        _field("closed_kline_count", pa.int64(), nullable=False),
        _field("closed_kline_1m_open_time", _TIMESTAMP_TYPE),
        _field("closed_kline_1m_close_time", _TIMESTAMP_TYPE),
        _field("closed_kline_1m_open_price", _DECIMAL_TYPE),
        _field("closed_kline_1m_close_price", _DECIMAL_TYPE),
        _field("source_event_count", pa.int64(), nullable=False),
        _field("first_received_at", _TIMESTAMP_TYPE),
        _field("last_received_at", _TIMESTAMP_TYPE),
        _field("data_complete", pa.bool_(), nullable=False),
        _field("missing_agg_trade_count", pa.int64(), nullable=False),
    ],
    metadata={
        b"cml.dataset": DatasetName.MARKET_STATES_15S.value.encode(),
        b"cml.schema_version": str(_DERIVED_DATASET_SCHEMA_VERSION).encode(),
    },
)

_DECIMAL_FIELD_NAMES = frozenset(
    field.name
    for schema in (_MARKET_EVENTS_SCHEMA, _MARKET_STATES_15S_SCHEMA)
    for field in schema
    if field.type == _DECIMAL_TYPE
)


def _schema_for(dataset_name: DatasetName) -> pa.Schema:
    if dataset_name is DatasetName.MARKET_EVENTS:
        return _MARKET_EVENTS_SCHEMA
    if dataset_name is DatasetName.MARKET_STATES_15S:
        return _MARKET_STATES_15S_SCHEMA
    raise ValueError(f"unsupported dataset schema: {dataset_name}")


@dataclass(frozen=True, slots=True)
class DerivedDatasetManifest:
    manifest_id: UUID
    dataset_name: DatasetName
    schema_version: int
    relative_path: Path
    row_count: int
    input_paths: tuple[str, ...]
    input_sha256: str
    output_sha256: str
    first_event_at: datetime
    last_event_at: datetime
    created_at: datetime
    producer_code_commit: str = "unknown"
    python_version: str = "unknown"
    pyarrow_version: str = "unknown"


def market_event_row(event: NormalizedMarketEvent) -> dict[str, object]:
    row = _base_event_row(event)
    if isinstance(event, NormalizedAggTrade):
        row.update(
            {
                "event_type": "agg_trade",
                "trade_id": event.trade_id,
                "price": _decimal(event.price),
                "quantity": _decimal(event.quantity),
                "notional": _decimal(event.notional),
                "aggressor_side": event.aggressor_side.value,
            }
        )
    elif isinstance(event, NormalizedBookTicker):
        row.update(
            {
                "event_type": "book_ticker",
                "update_id": event.update_id,
                "bid_price": _decimal(event.bid_price),
                "bid_quantity": _decimal(event.bid_quantity),
                "ask_price": _decimal(event.ask_price),
                "ask_quantity": _decimal(event.ask_quantity),
            }
        )
    elif isinstance(event, NormalizedMarkPrice):
        row.update(
            {
                "event_type": "mark_price",
                "mark_price": _optional_decimal(event.mark_price),
                "index_price": _optional_decimal(event.index_price),
                "estimated_settle_price": _optional_decimal(
                    event.estimated_settle_price
                ),
                "funding_rate": _optional_decimal(event.funding_rate),
                "next_funding_at": event.next_funding_at,
            }
        )
    elif isinstance(event, NormalizedKline1m):
        row.update(
            {
                "event_type": "kline_1m",
                "open_time": event.open_time,
                "close_time": event.close_time,
                "open_price": _decimal(event.open_price),
                "high_price": _decimal(event.high_price),
                "low_price": _decimal(event.low_price),
                "close_price": _decimal(event.close_price),
                "volume": _decimal(event.volume),
                "quote_volume": _decimal(event.quote_volume),
                "kline_trade_count": event.trade_count,
                "closed": event.closed,
            }
        )
    elif isinstance(event, NormalizedLiquidation):
        row.update(
            {
                "event_type": "liquidation",
                "order_side": event.order_side.value,
                "price": _decimal(event.price),
                "average_price": _decimal(event.average_price),
                "quantity": _decimal(event.quantity),
                "notional": _decimal(event.notional),
                "trade_time": event.trade_time,
            }
        )
    else:
        raise TypeError(f"unsupported normalized event: {type(event)!r}")
    return row


def market_state_15s_row(state: MarketState15s) -> dict[str, object]:
    return {
        "schema_version": state.schema_version,
        "exchange": state.exchange,
        "environment": state.environment,
        "symbol": state.symbol,
        "bucket_start": state.bucket_start,
        "bucket_end": state.bucket_end,
        "open_price": _optional_decimal(state.open_price),
        "high_price": _optional_decimal(state.high_price),
        "low_price": _optional_decimal(state.low_price),
        "close_price": _optional_decimal(state.close_price),
        "trade_count": state.trade_count,
        "trade_notional": _decimal(state.trade_notional),
        "aggressive_buy_notional": _decimal(state.aggressive_buy_notional),
        "aggressive_sell_notional": _decimal(state.aggressive_sell_notional),
        "last_bid_price": _optional_decimal(state.last_bid_price),
        "last_ask_price": _optional_decimal(state.last_ask_price),
        "spread": _optional_decimal(state.spread),
        "midpoint": _optional_decimal(state.midpoint),
        "liquidation_count": state.liquidation_count,
        "liquidation_notional": _decimal(state.liquidation_notional),
        "mark_price": _optional_decimal(state.mark_price),
        "closed_kline_count": state.closed_kline_count,
        "closed_kline_1m_open_time": state.closed_kline_1m_open_time,
        "closed_kline_1m_close_time": state.closed_kline_1m_close_time,
        "closed_kline_1m_open_price": _optional_decimal(
            state.closed_kline_1m_open_price
        ),
        "closed_kline_1m_close_price": _optional_decimal(
            state.closed_kline_1m_close_price
        ),
        "source_event_count": state.source_event_count,
        "first_received_at": state.first_received_at,
        "last_received_at": state.last_received_at,
        "data_complete": state.data_complete,
        "missing_agg_trade_count": state.missing_agg_trade_count,
    }


def partition_for_market_event(event: NormalizedMarketEvent) -> Path:
    return Path(
        DatasetName.MARKET_EVENTS.value,
        f"date={_utc_date(event.event_at)}",
        f"stream={event.source_stream.value}",
        f"symbol={event.symbol}",
    )


def partition_for_market_state(state: MarketState15s) -> Path:
    return Path(
        DatasetName.MARKET_STATES_15S.value,
        f"date={_utc_date(state.bucket_start)}",
        f"symbol={state.symbol}",
    )


def write_market_events_dataset(
    *,
    root: Path,
    events: Iterable[NormalizedMarketEvent],
    input_paths: tuple[Path, ...],
) -> tuple[DerivedDatasetManifest, ...]:
    return _write_streaming_rows(
        root=root,
        dataset_name=DatasetName.MARKET_EVENTS,
        records=events,
        partition_for_record=partition_for_market_event,
        row_factory=market_event_row,
        input_paths=input_paths,
        event_time_key="event_at",
    )


def write_market_states_15s_dataset(
    *,
    root: Path,
    states: Iterable[MarketState15s],
    input_paths: tuple[Path, ...],
) -> tuple[DerivedDatasetManifest, ...]:
    return _write_streaming_rows(
        root=root,
        dataset_name=DatasetName.MARKET_STATES_15S,
        records=states,
        partition_for_record=partition_for_market_state,
        row_factory=market_state_15s_row,
        input_paths=input_paths,
        event_time_key="bucket_start",
    )


def _base_event_row(event: NormalizedMarketEvent) -> dict[str, object]:
    return {
        "schema_version": event.schema_version,
        "exchange": event.exchange,
        "environment": event.environment,
        "symbol": event.symbol,
        "event_at": event.event_at,
        "received_at": event.received_at,
        "source_connection_session_id": str(event.source_connection_session_id),
        "source_local_sequence": event.source_local_sequence,
        "source_stream": event.source_stream.value,
        "event_type": None,
        "trade_id": None,
        "price": None,
        "quantity": None,
        "notional": None,
        "aggressor_side": None,
        "update_id": None,
        "bid_price": None,
        "bid_quantity": None,
        "ask_price": None,
        "ask_quantity": None,
        "mark_price": None,
        "index_price": None,
        "estimated_settle_price": None,
        "funding_rate": None,
        "next_funding_at": None,
        "open_time": None,
        "close_time": None,
        "open_price": None,
        "high_price": None,
        "low_price": None,
        "close_price": None,
        "volume": None,
        "quote_volume": None,
        "kline_trade_count": None,
        "closed": None,
        "order_side": None,
        "average_price": None,
        "trade_time": None,
    }


def _decimal(value: Decimal) -> str:
    return str(value)


def _optional_decimal(value: Decimal | None) -> str | None:
    return None if value is None else _decimal(value)


def _utc_date(value: datetime) -> str:
    return value.astimezone(UTC).date().isoformat()


@dataclass(slots=True)
class _PartitionWriter:
    partition: Path
    temporary_path: Path
    writer: pq.ParquetWriter
    rows: list[dict[str, object]]
    row_count: int = 0
    first_event_at: datetime | None = None
    last_event_at: datetime | None = None


def _write_streaming_rows[DatasetRecord](
    *,
    root: Path,
    dataset_name: DatasetName,
    records: Iterable[DatasetRecord],
    partition_for_record: Callable[[DatasetRecord], Path],
    row_factory: Callable[[DatasetRecord], dict[str, object]],
    input_paths: tuple[Path, ...],
    event_time_key: str,
) -> tuple[DerivedDatasetManifest, ...]:
    input_labels = tuple(path.as_posix() for path in input_paths)
    input_sha256 = _input_sha256(input_paths)
    producer_code_commit = resolve_code_commit(required=False)
    python_version = sys.version.split()[0]
    pyarrow_version = pa.__version__
    schema = _schema_for(dataset_name)
    writers: dict[Path, _PartitionWriter] = {}
    try:
        for record in records:
            partition = partition_for_record(record)
            writer = writers.get(partition)
            if writer is None:
                writer = _new_partition_writer(root, partition, schema)
                writers[partition] = writer
            row = row_factory(record)
            event_at = _row_datetime(row, event_time_key)
            writer.rows.append(row)
            writer.row_count += 1
            writer.first_event_at = (
                event_at
                if writer.first_event_at is None or event_at < writer.first_event_at
                else writer.first_event_at
            )
            writer.last_event_at = (
                event_at
                if writer.last_event_at is None or event_at > writer.last_event_at
                else writer.last_event_at
            )
            if len(writer.rows) >= _WRITE_BATCH_ROWS:
                _flush_partition_rows(writer, schema)

        if not writers:
            raise ValueError(f"{dataset_name.value} dataset has no rows")
        manifests = [
            _finalize_partition_writer(
                root=root,
                dataset_name=dataset_name,
                writer=writers[partition],
                schema=schema,
                input_labels=input_labels,
                input_sha256=input_sha256,
                producer_code_commit=producer_code_commit,
                python_version=python_version,
                pyarrow_version=pyarrow_version,
            )
            for partition in sorted(writers, key=lambda item: item.as_posix())
        ]
    except BaseException:
        for writer in writers.values():
            try:
                writer.writer.close()
            except Exception:
                # Preserve the original write failure; this is only best-effort
                # cleanup of an unpublished temporary file.
                pass
            writer.temporary_path.unlink(missing_ok=True)
        raise
    return tuple(manifests)


def _new_partition_writer(
    root: Path,
    partition: Path,
    schema: pa.Schema,
) -> _PartitionWriter:
    partition_dir = root / partition
    partition_dir.mkdir(parents=True, exist_ok=True)
    temporary_path = partition_dir / f".part-{uuid4()}.parquet.tmp"
    return _PartitionWriter(
        partition=partition,
        temporary_path=temporary_path,
        writer=pq.ParquetWriter(
            temporary_path,
            schema,
            compression=_PARQUET_COMPRESSION,
            version="2.6",
            data_page_version="2.0",
            write_statistics=True,
        ),
        rows=[],
    )


def _flush_partition_rows(writer: _PartitionWriter, schema: pa.Schema) -> None:
    if not writer.rows:
        return
    table = pa.Table.from_pylist(
        _parquet_rows(writer.rows, schema=schema),
        schema=schema,
    )
    writer.rows.clear()
    writer.writer.write_table(table, row_group_size=_PARQUET_ROW_GROUP_SIZE)


def _finalize_partition_writer(
    *,
    root: Path,
    dataset_name: DatasetName,
    writer: _PartitionWriter,
    schema: pa.Schema,
    input_labels: tuple[str, ...],
    input_sha256: str,
    producer_code_commit: str,
    python_version: str,
    pyarrow_version: str,
) -> DerivedDatasetManifest:
    _flush_partition_rows(writer, schema)
    writer.writer.close()
    if writer.first_event_at is None or writer.last_event_at is None:
        raise RuntimeError("partition writer finalized without rows")
    output_sha256 = _sha256_file(writer.temporary_path)
    manifest_id = uuid5(
        NAMESPACE_URL,
        json.dumps(
            {
                "dataset_name": dataset_name.value,
                "partition": writer.partition.as_posix(),
                "input_paths": input_labels,
                "input_sha256": input_sha256,
                "output_sha256": output_sha256,
                "row_count": writer.row_count,
                "first_event_at": writer.first_event_at.isoformat(),
                "last_event_at": writer.last_event_at.isoformat(),
                "producer_code_commit": producer_code_commit,
                "python_version": python_version,
                "pyarrow_version": pyarrow_version,
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
    )
    relative_path = writer.partition / f"part-{manifest_id}.parquet"
    final_path = root / relative_path
    os.replace(writer.temporary_path, final_path)
    manifest = DerivedDatasetManifest(
        manifest_id=manifest_id,
        dataset_name=dataset_name,
        schema_version=_DERIVED_DATASET_SCHEMA_VERSION,
        relative_path=relative_path,
        row_count=writer.row_count,
        input_paths=input_labels,
        input_sha256=input_sha256,
        output_sha256=output_sha256,
        first_event_at=writer.first_event_at,
        last_event_at=writer.last_event_at,
        created_at=datetime.now(UTC),
        producer_code_commit=producer_code_commit,
        python_version=python_version,
        pyarrow_version=pyarrow_version,
    )
    _write_manifest(root, manifest)
    return manifest


def _write_manifest(root: Path, manifest: DerivedDatasetManifest) -> None:
    directory = root / "_manifests"
    directory.mkdir(parents=True, exist_ok=True)
    temporary_path = directory / f".{manifest.manifest_id}.json.tmp"
    final_path = directory / f"{manifest.manifest_id}.json"
    payload = {
        "manifest_id": str(manifest.manifest_id),
        "dataset_name": manifest.dataset_name.value,
        "schema_version": manifest.schema_version,
        "relative_path": manifest.relative_path.as_posix(),
        "row_count": manifest.row_count,
        "input_paths": list(manifest.input_paths),
        "input_sha256": manifest.input_sha256,
        "output_sha256": manifest.output_sha256,
        "first_event_at": manifest.first_event_at.isoformat(),
        "last_event_at": manifest.last_event_at.isoformat(),
        "created_at": manifest.created_at.isoformat(),
        "producer_code_commit": manifest.producer_code_commit,
        "python_version": manifest.python_version,
        "pyarrow_version": manifest.pyarrow_version,
    }
    temporary_path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    os.replace(temporary_path, final_path)


def _input_sha256(paths: tuple[Path, ...]) -> str:
    digest = hashlib.sha256()
    for path in sorted(paths, key=lambda item: item.as_posix()):
        digest.update(path.as_posix().encode())
        digest.update(b"\0")
        digest.update(_sha256_file(path).encode())
        digest.update(b"\0")
    return digest.hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _row_datetime(row: dict[str, object], key: str) -> datetime:
    value = row[key]
    if not isinstance(value, datetime):
        raise TypeError(f"{key} must be a datetime")
    return value


def _parquet_rows(
    rows: list[dict[str, object]],
    *,
    schema: pa.Schema,
) -> list[dict[str, object]]:
    """Project domain rows onto the immutable on-disk schema.

    Hive partition columns are deliberately absent from the file.  Rejecting
    an accidental field addition/removal here turns an implicit inference
    change into an explicit schema migration instead of silently publishing a
    data set whose physical layout varies by batch.
    """

    expected = frozenset(schema.names)
    projected_rows: list[dict[str, object]] = []
    for row in rows:
        projected = {name: value for name, value in row.items() if name != "symbol"}
        actual = frozenset(projected)
        if actual != expected:
            missing = sorted(expected - actual)
            unexpected = sorted(actual - expected)
            raise ValueError(
                "derived dataset row does not match its schema "
                f"(missing={missing}, unexpected={unexpected})"
            )
        for name in _DECIMAL_FIELD_NAMES.intersection(projected):
            value = projected[name]
            if value is not None:
                projected[name] = Decimal(str(value))
        projected_rows.append(projected)
    return projected_rows
