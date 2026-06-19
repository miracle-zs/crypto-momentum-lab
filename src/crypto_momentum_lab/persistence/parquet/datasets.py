import hashlib
import json
import os
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pyarrow as pa
import pyarrow.parquet as pq

from crypto_momentum_lab.domain.market.models import (
    MarketState15s,
    NormalizedAggTrade,
    NormalizedBookTicker,
    NormalizedKline1m,
    NormalizedLiquidation,
    NormalizedMarketEvent,
    NormalizedMarkPrice,
)


class DatasetName(StrEnum):
    MARKET_EVENTS = "market_events"
    MARKET_STATES_15S = "market_states_15s"


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
        "source_event_count": state.source_event_count,
        "first_received_at": state.first_received_at,
        "last_received_at": state.last_received_at,
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
    grouped: dict[Path, list[dict[str, object]]] = {}
    for event in events:
        grouped.setdefault(partition_for_market_event(event), []).append(
            market_event_row(event)
        )
    return _write_grouped_rows(
        root=root,
        dataset_name=DatasetName.MARKET_EVENTS,
        grouped=grouped,
        input_paths=input_paths,
        event_time_key="event_at",
    )


def write_market_states_15s_dataset(
    *,
    root: Path,
    states: Iterable[MarketState15s],
    input_paths: tuple[Path, ...],
) -> tuple[DerivedDatasetManifest, ...]:
    grouped: dict[Path, list[dict[str, object]]] = {}
    for state in states:
        grouped.setdefault(partition_for_market_state(state), []).append(
            market_state_15s_row(state)
        )
    return _write_grouped_rows(
        root=root,
        dataset_name=DatasetName.MARKET_STATES_15S,
        grouped=grouped,
        input_paths=input_paths,
        event_time_key="bucket_start",
    )


def read_market_states_15s_dataset(
    paths: Iterable[Path],
) -> tuple[MarketState15s, ...]:
    states: list[MarketState15s] = []
    for path in _iter_parquet_paths(paths):
        for row in pq.read_table(path).to_pylist():
            states.append(_market_state_from_row(row, path))
    return tuple(sorted(states, key=lambda item: (item.symbol, item.bucket_start)))


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


def _write_grouped_rows(
    *,
    root: Path,
    dataset_name: DatasetName,
    grouped: dict[Path, list[dict[str, object]]],
    input_paths: tuple[Path, ...],
    event_time_key: str,
) -> tuple[DerivedDatasetManifest, ...]:
    if not grouped:
        raise ValueError(f"{dataset_name.value} dataset has no rows")
    input_labels = tuple(path.as_posix() for path in input_paths)
    input_sha256 = _input_sha256(input_paths)
    manifests: list[DerivedDatasetManifest] = []
    for partition, rows in sorted(grouped.items(), key=lambda item: item[0].as_posix()):
        manifests.append(
            _write_partition_rows(
                root=root,
                dataset_name=dataset_name,
                partition=partition,
                rows=rows,
                input_labels=input_labels,
                input_sha256=input_sha256,
                event_time_key=event_time_key,
            )
        )
    return tuple(manifests)


def _write_partition_rows(
    *,
    root: Path,
    dataset_name: DatasetName,
    partition: Path,
    rows: list[dict[str, object]],
    input_labels: tuple[str, ...],
    input_sha256: str,
    event_time_key: str,
) -> DerivedDatasetManifest:
    partition_dir = root / partition
    partition_dir.mkdir(parents=True, exist_ok=True)
    temporary_path = partition_dir / f".part-{uuid4()}.parquet.tmp"
    table = pa.Table.from_pylist(_parquet_rows(rows))
    pq.write_table(table, temporary_path)
    output_sha256 = _sha256_file(temporary_path)
    first_event_at = min(_row_datetime(row, event_time_key) for row in rows)
    last_event_at = max(_row_datetime(row, event_time_key) for row in rows)
    manifest_id = uuid5(
        NAMESPACE_URL,
        json.dumps(
            {
                "dataset_name": dataset_name.value,
                "partition": partition.as_posix(),
                "input_paths": input_labels,
                "input_sha256": input_sha256,
                "output_sha256": output_sha256,
                "row_count": len(rows),
                "first_event_at": first_event_at.isoformat(),
                "last_event_at": last_event_at.isoformat(),
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
    )
    relative_path = partition / f"part-{manifest_id}.parquet"
    final_path = root / relative_path
    os.replace(temporary_path, final_path)
    manifest = DerivedDatasetManifest(
        manifest_id=manifest_id,
        dataset_name=dataset_name,
        schema_version=1,
        relative_path=relative_path,
        row_count=len(rows),
        input_paths=input_labels,
        input_sha256=input_sha256,
        output_sha256=output_sha256,
        first_event_at=first_event_at,
        last_event_at=last_event_at,
        created_at=datetime.now(UTC),
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


def _parquet_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    # Hive partition columns are supplied by the directory names. Writing the
    # same column inside the file makes pyarrow fail schema merging.
    return [
        {key: value for key, value in row.items() if key != "symbol"}
        for row in rows
    ]


def _iter_parquet_paths(paths: Iterable[Path]) -> tuple[Path, ...]:
    discovered: dict[str, Path] = {}
    for path in paths:
        if path.is_dir():
            for parquet_path in sorted(path.rglob("*.parquet")):
                discovered[parquet_path.as_posix()] = parquet_path
        elif path.suffix == ".parquet":
            discovered[path.as_posix()] = path
        else:
            raise ValueError(f"market state path is not parquet or directory: {path}")
    return tuple(discovered[key] for key in sorted(discovered))


def _market_state_from_row(row: dict[str, object], path: Path) -> MarketState15s:
    return MarketState15s(
        schema_version=_required_int(row, "schema_version"),
        exchange=_required_string(row, "exchange"),
        environment=_required_string(row, "environment"),
        symbol=_row_symbol(row, path),
        bucket_start=_required_datetime(row, "bucket_start"),
        bucket_end=_required_datetime(row, "bucket_end"),
        open_price=_optional_decimal_row(row, "open_price"),
        high_price=_optional_decimal_row(row, "high_price"),
        low_price=_optional_decimal_row(row, "low_price"),
        close_price=_optional_decimal_row(row, "close_price"),
        trade_count=_required_int(row, "trade_count"),
        trade_notional=_required_decimal(row, "trade_notional"),
        aggressive_buy_notional=_required_decimal(row, "aggressive_buy_notional"),
        aggressive_sell_notional=_required_decimal(row, "aggressive_sell_notional"),
        last_bid_price=_optional_decimal_row(row, "last_bid_price"),
        last_ask_price=_optional_decimal_row(row, "last_ask_price"),
        spread=_optional_decimal_row(row, "spread"),
        midpoint=_optional_decimal_row(row, "midpoint"),
        liquidation_count=_required_int(row, "liquidation_count"),
        liquidation_notional=_required_decimal(row, "liquidation_notional"),
        mark_price=_optional_decimal_row(row, "mark_price"),
        closed_kline_count=_required_int(row, "closed_kline_count"),
        source_event_count=_required_int(row, "source_event_count"),
        first_received_at=_optional_datetime(row, "first_received_at"),
        last_received_at=_optional_datetime(row, "last_received_at"),
    )


def _required_string(row: dict[str, object], key: str) -> str:
    value = row.get(key)
    if value is None:
        raise ValueError(f"{key} is required")
    return str(value)


def _required_int(row: dict[str, object], key: str) -> int:
    value = row.get(key)
    if value is None:
        raise ValueError(f"{key} is required")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value)
    raise ValueError(f"{key} must be an int")


def _required_decimal(row: dict[str, object], key: str) -> Decimal:
    value = row.get(key)
    if value is None:
        raise ValueError(f"{key} is required")
    return Decimal(str(value))


def _optional_decimal_row(row: dict[str, object], key: str) -> Decimal | None:
    value = row.get(key)
    if value is None:
        return None
    return Decimal(str(value))


def _required_datetime(row: dict[str, object], key: str) -> datetime:
    value = row.get(key)
    if not isinstance(value, datetime):
        raise ValueError(f"{key} must be a datetime")
    return value


def _optional_datetime(row: dict[str, object], key: str) -> datetime | None:
    value = row.get(key)
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise ValueError(f"{key} must be a datetime")
    return value


def _row_symbol(row: dict[str, object], path: Path) -> str:
    value = row.get("symbol")
    if value is not None:
        return str(value)
    for part in path.parts:
        if part.startswith("symbol="):
            return part.removeprefix("symbol=")
    raise ValueError(f"symbol partition missing for {path}")
