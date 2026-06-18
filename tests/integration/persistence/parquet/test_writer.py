import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pyarrow.parquet as pq

from crypto_momentum_lab.domain.market.models import (
    AggressorSide,
    CaptureStream,
    MarketState15s,
    NormalizedAggTrade,
)
from crypto_momentum_lab.persistence.parquet.datasets import (
    write_market_events_dataset,
    write_market_states_15s_dataset,
)


def test_write_market_events_dataset_creates_parquet_and_manifest(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "raw.jsonl.zst"
    input_path.write_bytes(b"raw-input")
    event = _event()

    manifests = write_market_events_dataset(
        root=tmp_path / "derived",
        events=(event,),
        input_paths=(input_path,),
    )

    assert len(manifests) == 1
    manifest = manifests[0]
    output_path = tmp_path / "derived" / manifest.relative_path
    table = pq.read_table(output_path)
    rows = table.to_pylist()
    assert rows[0]["event_type"] == "agg_trade"
    assert rows[0]["price"] == "100.25"
    assert manifest.row_count == 1
    assert manifest.output_sha256 == hashlib.sha256(
        output_path.read_bytes()
    ).hexdigest()

    manifest_path = (
        tmp_path / "derived" / "_manifests" / f"{manifest.manifest_id}.json"
    )
    payload = json.loads(manifest_path.read_text())
    assert payload["relative_path"] == manifest.relative_path.as_posix()
    assert payload["input_paths"] == [input_path.as_posix()]
    assert payload["output_sha256"] == manifest.output_sha256


def test_write_market_states_dataset_creates_parquet_and_manifest(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "raw.jsonl.zst"
    input_path.write_bytes(b"raw-input")
    state = _state()

    manifests = write_market_states_15s_dataset(
        root=tmp_path / "derived",
        states=(state,),
        input_paths=(input_path,),
    )

    assert len(manifests) == 1
    output_path = tmp_path / "derived" / manifests[0].relative_path
    rows = pq.read_table(output_path).to_pylist()
    assert rows[0]["symbol"] == "BTCUSDT"
    assert rows[0]["trade_notional"] == "100.25"
    assert manifests[0].row_count == 1


def _event() -> NormalizedAggTrade:
    return NormalizedAggTrade(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        symbol="BTCUSDT",
        event_at=datetime(2026, 6, 15, 2, 0, tzinfo=UTC),
        received_at=datetime(2026, 6, 15, 2, 0, 1, tzinfo=UTC),
        source_connection_session_id=UUID(int=1),
        source_local_sequence=7,
        source_stream=CaptureStream.AGG_TRADE,
        trade_id="42",
        price=Decimal("100.25"),
        quantity=Decimal("1"),
        notional=Decimal("100.25"),
        aggressor_side=AggressorSide.BUY,
    )


def _state() -> MarketState15s:
    return MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        symbol="BTCUSDT",
        bucket_start=datetime(2026, 6, 15, 2, 0, tzinfo=UTC),
        bucket_end=datetime(2026, 6, 15, 2, 0, 15, tzinfo=UTC),
        open_price=Decimal("100.25"),
        high_price=Decimal("100.25"),
        low_price=Decimal("100.25"),
        close_price=Decimal("100.25"),
        trade_count=1,
        trade_notional=Decimal("100.25"),
        aggressive_buy_notional=Decimal("100.25"),
        aggressive_sell_notional=Decimal("0"),
        last_bid_price=None,
        last_ask_price=None,
        spread=None,
        midpoint=None,
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=None,
        closed_kline_count=0,
        source_event_count=1,
        first_received_at=datetime(2026, 6, 15, 2, 0, 1, tzinfo=UTC),
        last_received_at=datetime(2026, 6, 15, 2, 0, 1, tzinfo=UTC),
    )
